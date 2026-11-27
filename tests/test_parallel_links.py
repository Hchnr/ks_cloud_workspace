import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.dynamic import Queue
from ks_cloud.link_scan import scan_link
from ks_cloud.parallel_links import LinkScanPool, checked_records, scan_batch
from ks_cloud.scope import UploadScope


class ParallelLinksTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / 'source'
        self.root.mkdir()
        self.target = self.root / 'target'
        self.target.mkdir()
        (self.target / 'one').write_text('one')
        self.links = []
        for name in ('a', 'b', 'c'):
            path = self.root / name
            path.symlink_to(self.target, target_is_directory=True)
            self.links.append(str(path))
        self.tuning = self.base / 'tuning.json'
        self.tuning.write_text(json.dumps(dict(enabled=True, max_nodes=3, batch_links=1, timeout_seconds=10)))
        self.ledger = self.base / 'links.jsonl'
        self.write_ledger(self.links)
        self.config = dict(run_dir=str(self.base / 'run'), hosts=['slow', 'fast', 'other'],
                           tasks=[dict(id='source', source=str(self.root), destination='ks3://bucket/source/',
                                       link_exclusions_file=str(self.ledger))],
                           scope_allowed_roots=[str(self.root)], parallel_link_scan_tuning_file=str(self.tuning),
                           tool='/tool', config_file='/private', jobs=2, parallel=1, list_parallel=1,
                           chunk_files=1000, slots=2)

    def write_ledger(self, links):
        self.ledger.write_text(''.join(json.dumps(dict(path=p, reason='directory symlink (not followed)'))+'\n'
                                      for p in links))

    def tearDown(self):
        self.tmp.cleanup()

    def test_parallel_batch_preserves_alias_keys_scope_and_errors(self):
        outside = self.base / 'external'
        outside.mkdir()
        (outside / 'private').write_text('not in scope')
        alias = self.root / 'external'
        alias.symlink_to(outside, target_is_directory=True)
        broken = self.root / 'broken'
        broken.symlink_to(self.root / 'absent', target_is_directory=True)
        links = self.links + [str(alias), str(broken)]
        scope = UploadScope(self.config['scope_allowed_roots'])
        serial = [tuple(r) for p in links for r in scan_link(p, scope)]
        actual = scan_batch(self.config, 'source', links)
        self.assertEqual([tuple(r) for r in checked_records(links, actual)], serial)
        self.assertEqual(sum(r[0] == 'file' for r in serial), 3)
        self.assertTrue(any(r[0] == 'exclude' and 'outside' in r[2] for r in serial))

    def test_reject_foreign_source_or_forged_result_and_bound_overflow(self):
        with self.assertRaises(ValueError):
            scan_batch(self.config, 'source', [str(self.base / 'outside')])
        with self.assertRaises(ValueError):
            scan_batch(self.config, 'other', self.links)
        with self.assertRaises(ValueError):
            checked_records(self.links, dict(links=self.links, records=[['file', '/foreign', 1]]))
        with patch('ks_cloud.parallel_links.MAX_RECORDS', 1):
            result = scan_batch(self.config, 'source', self.links)
        self.assertTrue(result['overflow'])
        self.assertIsNone(checked_records(self.links, result))

    def test_idle_hosts_pull_more_work_and_results_stay_in_input_order(self):
        counts = dict.fromkeys(self.config['hosts'], 0)
        active = [0, 0]
        lock = threading.Lock()
        def remote(pool, host, source, links):
            with lock:
                counts[host] += 1
                active[0] += 1
                active[1] = max(active)
            time.sleep(.08 if host == 'slow' else .004)
            with lock:
                active[0] -= 1
            return [['file', links[0]+'/one', 3]]
        with patch.object(LinkScanPool, '_remote', remote):
            pool = LinkScanPool(self.config)
            try:
                inputs = [(i+1, self.links[i % 3]) for i in range(24)]
                results = list(pool.batches('source', inputs, lambda *args: None))
                self.assertEqual([x[0] for x in results], list(range(1, 25)))
                self.assertGreaterEqual(active[1], 2)
                self.assertGreater(counts['fast']+counts['other'], counts['slow'])
            finally:
                pool.stop.set()

    def test_failed_host_is_retried_elsewhere_and_pressure_reduces_concurrency(self):
        failed = []
        def remote(pool, host, source, links):
            if not failed:
                failed.append(host)
                raise OSError('simulated node loss')
            return [['file', links[0]+'/one', 3]]
        with patch.object(LinkScanPool, '_remote', remote):
            pool = LinkScanPool(self.config)
            try:
                results = list(pool.batches('source', list(enumerate(self.links, 1)), lambda *args: None))
                self.assertTrue(all(x[2] is not None for x in results))
                self.assertEqual(sum(x['errors'] for x in pool.nodes.values()), 1)
                pool.observe({h: dict(healthy=True) for h in self.config['hosts']},
                             dict(io_pressure_some_avg10=80), 1000)
                self.assertEqual(pool.limit, 1)
                pool.observe({h: dict(healthy=True) for h in self.config['hosts']}, {}, 1181)
                self.assertEqual(pool.limit, 2)
            finally:
                pool.stop.set()

    def make_queue(self, fail_after=None):
        config = self.config
        class FakePool:
            def __init__(self, config): self.settings = dict(batch_links=1)
            def batches(self, source, inputs, activity, batch_links=None):
                for n, (index, link) in enumerate(inputs):
                    if fail_after is not None and n == fail_after:
                        raise RuntimeError('simulated interruption')
                    records = scan_batch(config, source, [link])['records']
                    yield index, [link], records
        with patch('ks_cloud.dynamic.LinkScanPool', FakePool):
            return Queue(self.config)

    def test_cursor_resumes_after_committed_batches_and_discards_uncommitted_audit_tail(self):
        q = self.make_queue(fail_after=1)
        task = self.config['tasks'][0]
        try:
            q.scan_excluded_links(task)
            self.assertEqual(q.db.execute('select next_index from link_cursors').fetchone()[0], 1)
            self.assertEqual(q.db.execute('select sum(files) from tasks').fetchone()[0], 1)
        finally:
            q.db.close()
        audit = Path(self.config['run_dir']) / 'excluded-source.jsonl'
        audit.write_text('uncommitted corrupt tail\n')
        q = self.make_queue()
        try:
            q.scan_excluded_links(task)
            self.assertEqual(q.db.execute('select next_index from link_cursors').fetchone()[0], 3)
            self.assertEqual(q.db.execute('select sum(files) from tasks').fetchone()[0], 3)
            self.assertEqual(q.db.execute('select state from scans').fetchone()[0], 'done')
            self.assertEqual(audit.read_text(), '')
        finally:
            q.db.close()

    def test_failure_after_enqueue_before_cursor_commit_replays_without_losing_or_duplicating_files(self):
        q = self.make_queue()
        try:
            with patch('ks_cloud.dynamic.os.fsync', side_effect=OSError('simulated disk interruption')):
                q.scan_excluded_links(self.config['tasks'][0])
            self.assertEqual(q.db.execute('select next_index from link_cursors').fetchone()[0], 0)
            q.scan_excluded_links(self.config['tasks'][0])
            self.assertEqual(q.db.execute('select sum(files) from tasks').fetchone()[0], 3)
            self.assertEqual(q.db.execute('select next_index from link_cursors').fetchone()[0], 3)
        finally:
            q.db.close()

    def test_changed_ledger_fails_without_advancing_cursor(self):
        q = self.make_queue(fail_after=1)
        try:
            q.scan_excluded_links(self.config['tasks'][0])
            self.write_ledger(list(reversed(self.links)))
            q.scan_excluded_links(self.config['tasks'][0])
            state, error = q.db.execute('select state,error from scans').fetchone()
            self.assertEqual(state, 'error')
            self.assertIn('ledger changed', error)
            self.assertEqual(q.db.execute('select next_index from link_cursors').fetchone()[0], 1)
        finally:
            q.db.close()

    def test_committed_exclusions_and_counts_survive_restart(self):
        external = self.base / 'external'
        external.mkdir()
        alias = self.root / 'external-alias'
        alias.symlink_to(external, target_is_directory=True)
        self.write_ledger([str(alias)] + self.links)
        q = self.make_queue(fail_after=1)
        try:
            q.scan_excluded_links(self.config['tasks'][0])
            self.assertEqual(q.db.execute('select errors from link_cursors').fetchone()[0], 1)
        finally:
            q.db.close()
        q = self.make_queue()
        try:
            q.scan_excluded_links(self.config['tasks'][0])
            self.assertEqual(q.db.execute('select error from scans').fetchone()[0], '1 excluded entries')
            audit = Path(self.config['run_dir']) / 'excluded-source.jsonl'
            self.assertEqual(len(audit.read_text().splitlines()), 1)
            self.assertEqual(q.db.execute('select sum(files) from tasks').fetchone()[0], 3)
        finally:
            q.db.close()

    def test_online_disable_uses_bounded_local_fallback(self):
        self.tuning.write_text(json.dumps(dict(enabled=False, max_nodes=1, batch_links=2)))
        pool = LinkScanPool(self.config)
        try:
            results = list(pool.batches('source', list(enumerate(self.links, 1)), lambda *args: None))
            self.assertEqual([r[0] for r in results], [2, 3])
            self.assertTrue(all(r[2] is None for r in results))
            self.assertEqual(pool.fallbacks, 2)
        finally:
            pool.stop.set()


if __name__ == '__main__':
    unittest.main()
