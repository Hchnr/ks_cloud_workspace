import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.scan_control import ScanControl, validate_settings
from ks_cloud.scan_tuning import update
from ks_cloud.dynamic import Queue
from ks_cloud.telemetry import scan_metrics, metrics_for_snapshot


class ScanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.path = self.base/'tuning.json'
        self.now = 0.
        self.c = ScanControl(dict(scan_tuning_file=str(self.path)),clock=lambda:self.now)
        self.configure(enabled=True, auto=True, workers=2)

    def tearDown(self):
        self.c.close()
        self.tmp.cleanup()

    def configure(self, **changes):
        update(self.path, changes)
        self.c.load_once()

    def sample(self, files=120, elapsed=1., pending=0, pressure=None, completed=0):
        with self.c.lock:
            s=self.c._source('s')
            s['files']+=files; s['operations']+=files; s['elapsed']+=elapsed
        self.now+=60
        # Simulate independent config polling, avoiding an accidental stale guard.
        self.c.load_once()
        self.c.observe({'s':pending},pressure,{'s':completed})

    def test_adaptive_ramp_queue_throttle_and_upload_regression(self):
        self.assertEqual(self.c.window('s'),2)
        self.sample(completed=1000)
        self.assertEqual(self.c.window('s'),4)
        self.sample(completed=2000)
        self.assertEqual(self.c.window('s'),8)
        self.sample(pending=1,completed=2100)
        self.assertEqual(self.c.window('s'),1)
        self.assertEqual(self.c.snapshot()['sources']['s']['reason'],'upload_rate_regression')
        self.sample(completed=3100)
        self.assertEqual(self.c.window('s'),1)
        self.now+=300
        self.c.load_once()
        self.sample(pending=64,completed=4100)
        self.assertEqual(self.c.window('s'),1)
        self.assertEqual(self.c.queue_limit(256),64)
        self.sample(completed=5100)
        self.assertEqual(self.c.window('s'),2)

    def test_pressure_and_metadata_fallbacks(self):
        for pressure,reason in [({'memory_used_pct':70},'memory_pressure'),
                                ({'memory_pressure_some_avg10':26},'memory_reclaim'),
                                ({'io_pressure_some_avg10':50},'io_pressure'),
                                ({'oom_kill_events':1},'oom_observed')]:
            with self.subTest(reason=reason):
                self.configure(workers=3 if self.c.settings['workers']==2 else 2)
                self.c.window('s')
                self.sample(pressure=pressure)
                self.assertEqual(self.c.window('s'),1)
                self.assertEqual(self.c.snapshot()['sources']['s']['reason'],reason)
        self.configure(workers=4)
        self.sample(elapsed=300.)
        self.assertEqual(self.c.window('s'),1)
        self.assertEqual(self.c.snapshot()['sources']['s']['reason'],'metadata_latency')
        self.configure(workers=3)
        self.c.record('s','exclude',0.1)
        self.sample(files=1)
        self.assertEqual(self.c.snapshot()['sources']['s']['reason'],'metadata_errors')

    def test_invalid_config_staleness_and_recovery_without_restart(self):
        self.assertEqual(self.c.window('s'),2)
        original = self.path.read_text()
        for changes in [dict(workers=99),dict(workers=True),dict(unknown=1),dict(auto=1)]:
            with self.assertRaises(ValueError):update(self.path,changes)
            self.assertEqual(self.path.read_text(),original)
        self.path.write_text('{bad')
        self.c.load_once()
        self.assertEqual(self.c.window('s'),1)
        self.path.write_text(original)
        self.c.load_once()
        self.assertEqual(self.c.window('s'),2)
        self.now+=121
        self.assertEqual(self.c.window('s'),1)
        self.assertTrue(self.c.snapshot()['config_stale'])
        self.c.load_once()
        self.assertEqual(self.c.window('s'),2)
        self.configure(enabled=False)
        self.assertEqual(self.c.window('s'),1)
        self.assertEqual(self.c.queue_limit(256),256)

    def test_blocked_reload_does_not_block_control(self):
        entered=threading.Event(); release=threading.Event()
        def blocked(*args):
            entered.set(); release.wait(5)
            return '{}'
        with patch.object(Path,'read_text',blocked):
            t=threading.Thread(target=self.c.load_once)
            t.start()
            self.assertTrue(entered.wait(1))
            try:
                start=time.monotonic()
                self.c.window('s');self.c.observe({});self.c.snapshot()
                self.assertLess(time.monotonic()-start,0.2)
            finally:release.set();t.join(2)

    def test_ordered_bounded_prefetch_and_live_disable(self):
        lock=threading.Lock();counts=dict(active=0,maximum=0,visited=[])
        class Entry:
            def __init__(self,n):self.name=str(n);self.path=str(n)
            def is_dir(self,**kw):return False
            def is_symlink(self):return False
            def stat(self):
                with lock:
                    counts['active']+=1
                    counts['maximum']=max(counts['maximum'],counts['active'])
                    counts['visited'].append(self.name)
                time.sleep(0.003 if int(self.name)%3 else 0.012)
                with lock:counts['active']-=1
                return type('Stat',(),dict(st_mode=0o100644,st_size=int(self.name)))()
        self.configure(workers=8)
        def consume(source):
            result=[]
            for entry,(kind,value) in self.c.entries(source,[Entry(i) for i in range(40)]):
                result.append(value)
                if source=='a' and len(result)==10:self.configure(enabled=False)
            return result
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(consume,['a','b']))
        self.assertEqual(results,[list(range(40))]*2)
        self.assertLessEqual(counts['maximum'],8)
        self.assertGreater(counts['maximum'],1)
        self.assertEqual(len(counts['visited']),80)

    def test_background_cli_reload_keeps_the_same_scan_alive(self):
        source=self.base/'live';source.mkdir()
        for i in range(400):(source/str(i)).write_text('payload')
        config=self.base/'config.json'
        config.write_text(json.dumps(dict(scan_tuning_file=str(self.path))))
        self.c.config['scan_tuning_poll_seconds']=1
        self.c.start()
        class SlowEntry:
            def __init__(self,entry):self.entry=entry;self.path=entry.path;self.name=entry.name
            def is_dir(self,**kw):return self.entry.is_dir(**kw)
            def is_symlink(self):return self.entry.is_symlink()
            def stat(self):time.sleep(0.03);return self.entry.stat()
        with os.scandir(source) as entries:items=[SlowEntry(e) for e in entries]
        output=[]
        scanner=threading.Thread(target=lambda:output.extend(e.name for e,r in self.c.entries('s',items)))
        scanner.start()
        env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
        try:
            for settings,expected in [(['auto=false','workers=4'],4),(['enabled=false'],1)]:
                cmd=[sys.executable,'-m','ks_cloud.cli','tune-scan','--config',str(config)]
                for setting in settings:cmd.extend(['--set',setting])
                subprocess.run(cmd,env=env,check=True,capture_output=True,text=True)
                deadline=time.monotonic()+3
                while self.c.window('s')!=expected and time.monotonic()<deadline:time.sleep(0.02)
                self.assertEqual(self.c.window('s'),expected)
                self.assertTrue(scanner.is_alive())
            scanner.join(20)
            self.assertFalse(scanner.is_alive())
            self.assertEqual(output,[e.name for e in items])
        finally:scanner.join(20)

    def test_parallel_manifest_identity_and_durable_resume(self):
        source=self.base/'source';source.mkdir()
        for name in ['zz-root','00-old/a','00-old/b','10-current/a','10-current/b','10-current/sub/a','20-later/a','skip/a']:
            p=source/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(name)
        (source/'broken').symlink_to('/missing-target')
        (source/'dir-link').symlink_to(source/'00-old',target_is_directory=True)
        (source/'file-link').symlink_to(source/'zz-root')
        (source/'bad\nname').write_text('bad')
        os.mkfifo(source/'pipe')
        task=dict(id='s',source=str(source),destination='ks3://bucket/original/',exclude_child='skip')
        def queue(name,enabled):
            q=Queue(dict(run_dir=str(self.base/name),hosts=['a'],tasks=[task],chunk_files=2,chunk_bytes=30))
            q.scan_control.settings=validate_settings(dict(enabled=enabled,workers=4))
            return q
        def manifests(q):
            return [(r['id'],Path(r['manifest']).read_text()) for r in q.db.execute('SELECT id,manifest FROM tasks ORDER BY rowid')]
        serial=queue('serial',False);parallel=queue('parallel',True)
        try:
            serial.scan(task);parallel.scan(task)
            expected=manifests(serial)
            self.assertEqual(manifests(parallel),expected)
            self.assertEqual(serial.snapshot()['discovered_files'],8)
            self.assertIn('4 excluded',parallel.snapshot()['scans'][0]['error'])
            for cutoff in range(1,len(expected)):
                q=queue('resume-'+str(cutoff),True)
                try:
                    add=q.add;calls=[]
                    def interrupted(*args):
                        if len(calls)==cutoff:raise InterruptedError('simulated loss')
                        calls.append(1);return add(*args)
                    q.add=interrupted;q.scan(task)
                    q.add=add;q.scan(task)
                    self.assertEqual(manifests(q),expected)
                finally:q.scan_control.close();q.db.close()
        finally:
            for q in [serial,parallel]:q.scan_control.close();q.db.close()

    def test_scan_telemetry_obeys_snapshot_freshness(self):
        self.c.window('s')
        s=dict(scan_control=self.c.snapshot(),updated=100,state='running')
        metrics=scan_metrics(s)
        self.assertEqual(metrics['scanning/s/workers'],2)
        self.assertEqual(metrics_for_snapshot(s,{},metrics,300),
                         {'overview/snapshot_age_seconds':200,'overview/snapshot_stale':1})


if __name__=='__main__':unittest.main()
