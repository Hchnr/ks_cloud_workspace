import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.dynamic import Queue
from ks_cloud.parallel_scan import (BatchParallelScanPool, EntryBatch, ParallelScanPool,
                                    classify_entries, scan_subtree, validate_entry_result, validate_result)
from ks_cloud.scope import OUTSIDE
from ks_cloud.telemetry import scan_metrics


class ParallelSubtreeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.source = self.base / 'source'
        self.source.mkdir()
        self.shards = self.source / 'shards'
        self.shards.mkdir()
        self.outside = self.base / 'outside'
        self.outside.mkdir()
        (self.outside / 'private').write_text('outside')
        (self.source / 'rootfile').write_text('root')
        for i in range(24):
            shard = self.shards / ('gpic_train_%05d.part000' % i)
            nested = shard / 'assets'
            nested.mkdir(parents=True)
            for name in ('a', 'b', 'c', 'd'):
                (nested / name).write_text(str(i) + name)
        first = self.shards / 'gpic_train_00000.part000' / 'assets'
        (first / 'outside-link').symlink_to(self.outside / 'private')
        (first / 'outside-dir').symlink_to(self.outside, target_is_directory=True)
        (first / 'inside-link').symlink_to(self.source / 'rootfile')
        (first / 'inside-dir').symlink_to(self.shards / 'gpic_train_00001.part000', target_is_directory=True)
        (first / 'broken').symlink_to(self.source / 'missing')
        self.tune = self.base / 'parallel-tune.json'
        self.tune.write_text(json.dumps(dict(enabled=True, max_nodes=3, timeout_seconds=10,
                                             minimum_completed_files_s=0)))
        self.task = dict(id='data', source=str(self.source), destination='ks3://bucket/source/')

    def tearDown(self):
        self.tmp.cleanup()

    def config(self, name, parallel=False):
        result = dict(run_dir=str(self.base/name), hosts=['bad', 'slow', 'fast'], tasks=[self.task],
                      scope_allowed_roots=[str(self.source)], chunk_files=7, chunk_bytes=100000,
                      slots=2, pending_per_source=1000)
        if parallel:
            result.update(parallel_scan_tuning_file=str(self.tune), parallel_scan_source='data',
                          parallel_scan_base=str(self.shards))
        return result

    def wait_enabled(self, q):
        until = time.monotonic() + 3
        while not q.parallel_scan.enabled() and time.monotonic() < until:
            time.sleep(.01)
        self.assertTrue(q.parallel_scan.enabled())

    def view(self, q):
        manifests = [(r['id'], Path(r['manifest']).read_text(), r['scope_checked'])
                     for r in q.db.execute('SELECT id,manifest,scope_checked FROM tasks ORDER BY rowid')]
        audit = (q.base/'excluded-data.jsonl').read_text().splitlines()
        return manifests, audit

    def fake_remote(self, host, root, timeout):
        if host == 'bad':
            raise OSError('injected node outage')
        time.sleep(.012 if host == 'slow' else .001)
        return scan_subtree(self.config('unused', True), root)

    def test_scope_and_validation(self):
        shard = str(self.shards/'gpic_train_00000.part000')
        result = scan_subtree(self.config('unused', True), shard)
        rows = validate_result(shard, result)
        self.assertIn(['exclude', str(Path(shard)/'assets'/'outside-link'), OUTSIDE], rows)
        self.assertIn(['exclude', str(Path(shard)/'assets'/'outside-dir'), OUTSIDE], rows)
        self.assertTrue(any(r[1] == str(Path(shard)/'assets'/'inside-link') and r[0] == 'file' for r in rows))
        self.assertFalse(any(r[0] == 'file' and r[1].startswith(str(self.outside)) for r in rows))
        with self.assertRaises(ValueError):
            scan_subtree(self.config('unused', True), str(self.outside))
        with self.assertRaises(ValueError):
            validate_result(shard, dict(root=shard, records=[['file', str(self.outside/'private'), 1]],
                                        operations=1, seconds=1))

    def test_ordered_results_and_resume_after_interruption(self):
        serial = Queue(self.config('serial'))
        try:
            serial.scan(self.task)
            expected = self.view(serial)
            self.assertEqual(serial.db.execute('SELECT state FROM scans').fetchone()[0], 'done')
            for cutoff in (1, 7, 12):
                with self.subTest(cutoff=cutoff), patch.object(ParallelScanPool, '_remote', self.fake_remote):
                    q = Queue(self.config('parallel-'+str(cutoff), True))
                    try:
                        self.wait_enabled(q)
                        add = q.add
                        calls = []
                        def interrupted(*args):
                            if len(calls) == cutoff:
                                raise InterruptedError('simulated coordinator loss')
                            calls.append(1)
                            return add(*args)
                        q.add = interrupted
                        q.scan(self.task)
                        self.assertEqual(q.db.execute('SELECT state FROM scans').fetchone()[0], 'error')
                        q.add = add
                        q.scan(self.task)
                        self.assertEqual(q.db.execute('SELECT state FROM scans').fetchone()[0], 'done')
                        self.assertEqual(self.view(q), expected)
                        self.assertTrue(all(row[2] == 1 for row in self.view(q)[0]))
                    finally:
                        q.parallel_scan.stop.set()
                        q.scan_control.close()
                        q.db.close()
        finally:
            serial.scan_control.close()
            serial.db.close()

    def test_failed_node_retries_and_faster_node_claims_more(self):
        with patch.object(ParallelScanPool, '_remote', self.fake_remote):
            q = Queue(self.config('balanced', True))
            try:
                self.wait_enabled(q)
                q.scan(self.task)
                snapshot = q.parallel_scan.snapshot()
                self.assertEqual(q.db.execute('SELECT state FROM scans').fetchone()[0], 'done')
                self.assertGreater(snapshot['nodes']['bad']['errors'], 0)
                self.assertGreater(snapshot['nodes']['fast']['subtrees'], snapshot['nodes']['slow']['subtrees'])
                self.assertEqual(snapshot['fallbacks'], 0)
                metrics = scan_metrics(dict(scan_control=q.scan_control.snapshot(), parallel_scan=snapshot))
                self.assertIn('scan_fleet/fast/files', metrics)
            finally:
                q.parallel_scan.stop.set()
                q.scan_control.close()
                q.db.close()

    def test_image_buckets_match_serial_after_interruption(self):
        images = self.source/'parquet_datasets'/'understanding'/'dataset'/'_media'/'images'
        for i in range(18):
            folder = images/('%02x' % i)
            folder.mkdir(parents=True)
            for j in range(5):
                (folder/('%02d' % j)).write_text('%d-%d' % (i, j))
        for i in range(4):
            folder = images/('%010x' % (256+i))
            folder.mkdir(parents=True)
            for j in range(5):
                (folder/('%02d.jpg' % j)).write_text('%d-%d' % (i, j))
        scenes = images/'scannetv2'/'scans'
        for i in range(8):
            folder = scenes/('scene%04d' % i)/'color'
            folder.mkdir(parents=True)
            for j in range(5):
                (folder/('%02d.jpg' % j)).write_text('%d-%d' % (i, j))
        generic = self.source/'unfamiliar_layout'
        for i in range(8):
            folder = generic/('partition-%02d' % i)
            folder.mkdir(parents=True)
            for j in range(5):
                (folder/('%02d.bin' % j)).write_text('%d-%d' % (i, j))
        asset = self.source/'other'/'assets'/'0a'
        asset.mkdir(parents=True)
        (asset/'asset.bin').write_text('asset')
        first = images/'00'
        (first/'outside').symlink_to(self.outside/'private')
        (first/'inside').symlink_to(self.source/'rootfile')
        (first/'broken').symlink_to(self.source/'missing')
        (first/'inside-dir').symlink_to(images/'01', target_is_directory=True)
        # A similarly named directory outside the source cannot be submitted.
        other = self.outside/'images'/'00'
        other.mkdir(parents=True)
        cfg = self.config('image-check', True)
        with self.assertRaises(ValueError):
            scan_subtree(cfg, str(other))
        serial = Queue(self.config('image-serial'))
        try:
            serial.scan(self.task)
            expected = self.view(serial)
        finally:
            serial.scan_control.close()
            serial.db.close()
        for cutoff in (2, 15):
            visited = []
            def recording(pool, host, path, timeout):
                visited.append(path)
                return self.fake_remote(host, path, timeout)
            with self.subTest(cutoff=cutoff), patch.object(ParallelScanPool, '_remote', recording):
                cfg = self.config('image-parallel-'+str(cutoff), True)
                q = Queue(cfg)
                try:
                    self.wait_enabled(q)
                    original = q.add
                    calls = []
                    def interrupted(*args):
                        if len(calls) == cutoff:
                            raise InterruptedError('simulated coordinator loss')
                        calls.append(1)
                        return original(*args)
                    q.add = interrupted
                    q.scan(self.task)
                    self.assertEqual(q.db.execute('SELECT state FROM scans').fetchone()[0], 'error')
                    q.parallel_scan.stop.set()
                    q.scan_control.close()
                    q.db.close()
                    q = Queue(cfg)
                    self.wait_enabled(q)
                    q.scan(self.task)
                    self.assertEqual(q.db.execute('SELECT state FROM scans').fetchone()[0], 'done')
                    self.assertEqual(self.view(q), expected)
                    stats = q.parallel_scan.snapshot()
                    self.assertGreater(sum(n['subtrees'] for n in stats['nodes'].values()), 0)
                    self.assertTrue(any(str(scenes) + os.sep in path for path in visited))
                    self.assertTrue(any(path.startswith(str(images)+os.sep+'00000001') for path in visited))
                    self.assertTrue(any(str(generic) + os.sep in path for path in visited))
                finally:
                    q.parallel_scan.stop.set()
                    q.scan_control.close()
                    q.db.close()

    def test_oversized_image_bucket_falls_back_without_quarantining_node(self):
        folder = self.source/'dataset'/'images'/'aa'
        folder.mkdir(parents=True)
        for i in range(6):
            (folder/str(i)).write_text(str(i))
        with patch('ks_cloud.parallel_scan.MAX_IMAGE_RECORDS', 3):
            result = scan_subtree(self.config('unused', True), str(folder))
            self.assertTrue(result['overflow'])
        serial = Queue(self.config('image-overflow-serial'))
        try:
            serial.scan(self.task)
            expected = self.view(serial)
        finally:
            serial.scan_control.close()
            serial.db.close()
        def remote(pool, host, path, timeout):
            if path == str(folder):
                return dict(root=path, overflow=True)
            return scan_subtree(self.config('unused', True), path)
        with patch.object(ParallelScanPool, '_remote', remote):
            q = Queue(self.config('image-overflow-parallel', True))
            try:
                self.wait_enabled(q)
                q.scan(self.task)
                self.assertEqual(self.view(q), expected)
                stats = q.parallel_scan.snapshot()
                self.assertGreater(stats['fallbacks'], 0)
                self.assertEqual(sum(n['errors'] for n in stats['nodes'].values()), 0)
            finally:
                q.parallel_scan.stop.set()
                q.scan_control.close()
                q.db.close()

    def test_large_bucket_regression_uses_a_bounded_long_window(self):
        q = Queue(self.config('image-regression', True))
        try:
            self.wait_enabled(q)
            pool = q.parallel_scan
            pool.image_scan_seen = True
            pool.active = 1
            pool.settings['minimum_completed_files_s'] = 500
            nodes = {host: dict(healthy=True, oom_kill_events=0) for host in self.config('x')['hosts']}
            for now in (0, 30, 60, 90, 120, 150):
                pool.observe(nodes, 0, now, True)
                self.assertTrue(pool.enabled())
            pool.observe(nodes, 0, 180, True)
            self.assertFalse(pool.enabled())
            self.assertGreater(pool.snapshot()['cooldown_seconds'], 0)
        finally:
            q.parallel_scan.stop.set()
            q.scan_control.close()
            q.db.close()

    def test_finished_oversize_results_free_a_slot_for_child_work(self):
        from concurrent.futures import Future
        pool = ParallelScanPool(self.config('split-queue', True))
        try:
            until = time.monotonic()+3
            while not pool.enabled() and time.monotonic()<until:
                time.sleep(.01)
            root = str(self.source/'generic-parent')
            result = Future()
            result.set_result(dict(root=root, overflow=True))
            pool.futures[root] = result
            pool.prime([], None)
            self.assertNotIn(root, pool.futures)
            self.assertIn(root, pool.local_only)
            self.assertIsNone(pool.consume(root, lambda *_: None))
            self.assertNotIn(root, pool.local_only)
            self.assertEqual(pool.snapshot()['fallbacks'], 1)
        finally:
            pool.stop.set()

    def test_large_directory_batches_match_serial_after_restart(self):
        folder = self.source/'big-directory'
        folder.mkdir()
        for i in range(2600):
            (folder/('%05d' % i)).write_text(str(i))
        (folder/'nested').mkdir()
        (folder/'nested'/'inner').write_text('inner')
        (folder/'00001-dir').mkdir()
        (folder/'00001-dir'/'before-cursor').write_text('directory is not skipped')
        (folder/'outside-link').symlink_to(self.outside/'private')
        (folder/'inside-link').symlink_to(self.source/'rootfile')
        (folder/'broken-link').symlink_to(self.source/'missing')
        (folder/'inside-dir').symlink_to(self.shards/'gpic_train_00000.part000', target_is_directory=True)
        (folder/'newline\nname').write_text('invalid name')
        cfg = self.config('batch-validation', True)
        with self.assertRaises(ValueError):
            classify_entries(cfg, EntryBatch(str(folder), ('../outside',), None))
        batch = EntryBatch(str(folder), ('00000',), None)
        with self.assertRaises(ValueError):
            validate_entry_result(batch, dict(root=str(folder), records=[['skip',str(folder/'00000'),None]],
                                              operations=1, seconds=1))
        serial = Queue(self.config('batch-serial'))
        try:
            serial.scan(self.task)
            expected = self.view(serial)
        finally:
            serial.scan_control.close()
            serial.db.close()
        calls = []
        def remote(pool, host, path, timeout):
            if host == 'bad':
                raise OSError('injected node outage')
            if isinstance(path, EntryBatch):
                calls.append((host, path.root, path.skip_before, path.names[0]))
                return classify_entries(cfg, path)
            return scan_subtree(cfg, path)
        cfg = self.config('batch-parallel', True)
        with patch.object(ParallelScanPool, '_remote', remote):
            q = Queue(cfg)
            try:
                self.wait_enabled(q)
                original = q.add
                count = []
                def interrupt(*args):
                    if len(count) == 150:
                        raise InterruptedError('simulated coordinator loss')
                    count.append(1)
                    return original(*args)
                q.add = interrupt
                q.scan(self.task)
                self.assertEqual(q.db.execute('SELECT state FROM scans').fetchone()[0], 'error')
                before_restart = len(calls)
                q.parallel_scan.stop.set()
                q.scan_control.close()
                q.db.close()
                q = Queue(cfg)
                self.wait_enabled(q)
                q.scan(self.task)
                self.assertEqual(q.db.execute('SELECT state FROM scans').fetchone()[0], 'done')
                self.assertEqual(self.view(q), expected)
                self.assertTrue(any(root == str(folder) for _, root, _, _ in calls))
                resumed = [cursor for _, root, cursor, _ in calls[before_restart:] if root == str(folder)]
                self.assertTrue(resumed)
                self.assertTrue(all(cursor is not None for cursor in resumed))
                self.assertEqual(q.parallel_scan.snapshot()['buffered'], 0)
            finally:
                q.parallel_scan.stop.set()
                q.scan_control.close()
                q.db.close()

    def test_large_directory_falls_back_if_remote_pool_stops(self):
        source = self.base/'flat-source'
        source.mkdir()
        for i in range(2100):
            (source/('%05d' % i)).write_text(str(i))
        shards = source/'shards'
        shards.mkdir()
        task = dict(id='data', source=str(source), destination='ks3://bucket/flat/')
        common = dict(hosts=['bad','slow','fast'], tasks=[task], scope_allowed_roots=[str(source)],
                      chunk_files=100, chunk_bytes=100000, pending_per_source=1000, slots=2)
        serial = Queue(dict(common, run_dir=str(self.base/'flat-serial')))
        try:
            serial.scan(task)
            expected = self.view(serial)
        finally:
            serial.scan_control.close()
            serial.db.close()
        cfg = dict(common, run_dir=str(self.base/'flat-parallel'), parallel_scan_source='data',
                   parallel_scan_base=str(shards), parallel_scan_tuning_file=str(self.tune))
        def remote(pool, host, path, timeout):
            if isinstance(path, EntryBatch):
                pool.stop.set()
                raise OSError('injected scanner loss')
            return scan_subtree(cfg, path)
        with patch.object(ParallelScanPool, '_remote', remote):
            q = Queue(cfg)
            try:
                self.wait_enabled(q)
                q.scan(task)
                self.assertEqual(q.db.execute('SELECT state FROM scans').fetchone()[0], 'done')
                self.assertEqual(self.view(q), expected)
                self.assertGreater(q.parallel_scan.snapshot()['fallbacks'], 0)
                self.assertEqual(q.parallel_scan.snapshot()['buffered'], 0)
            finally:
                q.parallel_scan.stop.set()
                q.scan_control.close()
                q.db.close()

    def test_sustained_throughput_regression_and_oom_cool_down(self):
        q = Queue(self.config('fallback', True))
        try:
            self.wait_enabled(q)
            q.parallel_scan.settings['minimum_completed_files_s'] = 500
            q.parallel_scan.active = 1
            nodes = {host: dict(healthy=True, oom_kill_events=0) for host in self.config('x')['hosts']}
            q.parallel_scan.observe(nodes, 1000, 0, True)
            q.parallel_scan.observe(nodes, 2000, 30, True)
            nodes['bad']['oom_kill_events'] = 1
            q.parallel_scan.observe(nodes, 3000, 60, True)
            self.assertFalse(q.parallel_scan.enabled())
            self.assertGreater(q.parallel_scan.snapshot()['cooldown_seconds'], 0)
            self.assertGreater(q.parallel_scan.nodes['bad']['cooldown_until'], time.monotonic())
            q.parallel_scan.cooldown_until = 0
            q.parallel_scan.observe(nodes, 5000, 90, True, dict(oom_kill_events=1))
            self.assertFalse(q.parallel_scan.enabled())
            self.assertGreater(q.parallel_scan.snapshot()['cooldown_seconds'], 0)
        finally:
            q.parallel_scan.stop.set()
            q.scan_control.close()
            q.db.close()

    def test_yuanhai_coexist_guard_protects_primary_throughput(self):
        config = dict(self.config('coexist', True), parallel_yuanhai_scan_base=str(self.shards))
        pool = BatchParallelScanPool(config)
        try:
            until = time.monotonic()+3
            while not pool.enabled() and time.monotonic()<until:
                time.sleep(.01)
            self.assertTrue(pool.enabled())
            with pool.lock:
                pool.settings['minimum_completed_files_s'] = 500
                pool.settings['coexist_minimum_completed_files_s'] = 8500
                pool.active = 1
            nodes = {host: dict(healthy=True, oom_kill_events=0) for host in config['hosts']}
            pool.observe(nodes, 0, 0, True, coexist=True)
            pool.observe(nodes, 200000, 30, True, coexist=True)
            pool.observe(nodes, 400000, 60, True, coexist=True)
            self.assertFalse(pool.enabled())
            pool.cooldown_until = 0
            pool.observe(nodes, 460000, 90, True, coexist=False)
            pool.observe(nodes, 520000, 120, True, coexist=False)
            self.assertTrue(pool.enabled())
        finally:
            pool.stop.set()

    def test_resuming_prefix_is_not_a_throughput_regression(self):
        q = Queue(self.config('resume-guard', True))
        try:
            self.wait_enabled(q)
            pool = q.parallel_scan
            pool.settings['minimum_completed_files_s'] = 500
            pool.active = 1
            pool.resuming_entries = 1
            nodes = {host: dict(healthy=True, oom_kill_events=0) for host in self.config('x')['hosts']}
            for now in (0, 30, 60, 90, 120):
                pool.observe(nodes, 1000, now, True)
                self.assertTrue(pool.enabled())
            pool.resuming_entries = 0
            pool.observe(nodes, 1000, 150, True)
            self.assertTrue(pool.enabled())
            pool.observe(nodes, 1000, 180, True)
            self.assertFalse(pool.enabled())
        finally:
            q.parallel_scan.stop.set()
            q.scan_control.close()
            q.db.close()

    def test_idle_pool_does_not_cool_down_on_serial_source_work(self):
        q = Queue(self.config('idle-guard', True))
        try:
            self.wait_enabled(q)
            pool = q.parallel_scan
            pool.settings['minimum_completed_files_s'] = 500
            nodes = {host: dict(healthy=True, oom_kill_events=0) for host in self.config('x')['hosts']}
            for now in (0, 30, 60, 90):
                pool.observe(nodes, 1000, now, True)
            self.assertTrue(pool.enabled())
        finally:
            q.parallel_scan.stop.set()
            q.scan_control.close()
            q.db.close()

    def test_parallel_settings_reload_without_restart(self):
        pool = ParallelScanPool(self.config('reload', True))
        try:
            until = time.monotonic()+3
            while not pool.enabled() and time.monotonic()<until:
                time.sleep(.01)
            self.assertTrue(pool.enabled())
            process_id = id(pool)
            replacement = self.tune.with_suffix('.new')
            replacement.write_text(json.dumps(dict(enabled=False, max_nodes=1, timeout_seconds=10)))
            os.replace(replacement, self.tune)
            until = time.monotonic()+7
            while pool.enabled() and time.monotonic()<until:
                time.sleep(.05)
            self.assertFalse(pool.enabled())
            self.assertEqual(id(pool), process_id)
            self.assertEqual(pool.snapshot()['max_nodes'], 1)
        finally:
            pool.stop.set()

    def test_yuanhai_batched_order_retry_and_resume(self):
        tasks = self.source/'tasks'
        tasks.mkdir()
        batch = tasks/'egoverse_b000001'
        batch.mkdir()
        for i in range(100):
            folder = batch/('%09d' % i)/'nested'
            folder.mkdir(parents=True)
            (folder/'data').write_text(str(i))
            (folder/'data2').write_text(str(i)+'-2')
        (batch/'000000010'/'nested'/'outside').symlink_to(self.outside/'private')
        (batch/'000000011'/'nested'/'broken').symlink_to(self.source/'missing')
        yuanhai = dict(id='yuanhai', source=str(self.source), destination='ks3://bucket/yuanhai/')
        prepared = self.base/'prepared'
        prepared.mkdir()
        pre_shards = prepared/'shards'
        pre_shards.mkdir()
        other_task = dict(id='prepared', source=str(prepared), destination='ks3://bucket/prepared/')
        common = dict(hosts=['bad', 'slow', 'fast'], tasks=[yuanhai, other_task],
                      scope_allowed_roots=[str(self.source), str(prepared)],
                      chunk_files=7, chunk_bytes=100000, slots=2, pending_per_source=1000)
        serial = Queue(dict(common, run_dir=str(self.base/'yuanhai-serial')))
        serial.scan(yuanhai)
        def view(q):
            return ([(r['id'], Path(r['manifest']).read_text(), r['scope_checked'])
                     for r in q.db.execute('SELECT id,manifest,scope_checked FROM tasks ORDER BY rowid')],
                    (q.base/'excluded-yuanhai.jsonl').read_text().splitlines())
        expected = view(serial)
        serial.scan_control.close()
        serial.db.close()
        config = dict(common, run_dir=str(self.base/'yuanhai-parallel'),
                      parallel_scan_tuning_file=str(self.tune), parallel_scan_source='prepared',
                      parallel_scan_base=str(pre_shards), parallel_yuanhai_scan_base=str(tasks))
        errors = []
        def remote(pool, host, paths, timeout):
            if host == 'bad':
                errors.append(host)
                raise OSError('injected loss')
            time.sleep(.003)
            return dict(items=[scan_subtree(config, path) for path in paths])
        with patch.object(BatchParallelScanPool, '_remote', remote):
            q = Queue(config)
            try:
                until = time.monotonic()+3
                while not q.parallel_yuanhai_scan.enabled() and time.monotonic()<until:
                    time.sleep(.01)
                self.assertTrue(q.parallel_yuanhai_scan.enabled())
                original = q.add
                calls = []
                def interrupt(*args):
                    if len(calls) == 9:
                        raise InterruptedError('simulated loss')
                    calls.append(1)
                    return original(*args)
                q.add = interrupt
                q.scan(yuanhai)
                self.assertEqual(q.db.execute("SELECT state FROM scans WHERE source='yuanhai'").fetchone()[0], 'error')
                q.add = original
                q.scan(yuanhai)
                self.assertEqual(q.db.execute("SELECT state FROM scans WHERE source='yuanhai'").fetchone()[0], 'done')
                self.assertEqual(view(q), expected)
                self.assertTrue(errors, 'node failure injection was not exercised')
                self.assertEqual(q.parallel_yuanhai_scan.snapshot()['fallbacks'], 0)
            finally:
                q.parallel_scan.stop.set()
                q.parallel_yuanhai_scan.stop.set()
                q.scan_control.close()
                q.db.close()


if __name__ == '__main__':
    unittest.main()
