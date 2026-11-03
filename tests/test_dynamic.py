import concurrent.futures
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.dynamic import Queue, cgroup_metrics


class DynamicTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.src = self.base / 'source'
        self.src.mkdir()
        self.config = dict(run_dir=str(self.base / 'run'), hosts=['a','b','c'], lease_seconds=240,
                           tasks=[dict(id='dataset', source=str(self.src), destination='ks3://bucket/original/')],
                           tool='/tool', config_file='/private', jobs=2, parallel=1, list_parallel=1,
                           chunk_files=2, attempts=2, slots=32)
        self.q = Queue(self.config)

    def tearDown(self):
        self.q.db.close()
        self.tmp.cleanup()

    def claim(self, host='a'):
        return self.q.rpc('claim', dict(host=host)).get('task')

    def finish(self, task, host='a', **result):
        return self.q.rpc('finish', dict(host=host,id=task['id'],token=task['token'],result=result))

    def test_concurrent_claims_unique_and_progress_not_double_counted(self):
        for i in range(24):
            self.q.add('dataset', [f'/source/{i}'], [i+1])
        with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
            tasks = list(pool.map(lambda _: self.claim(), range(24)))
        self.assertEqual(len({t['id'] for t in tasks}), 24)
        t = tasks[0]
        result = dict(exit_code=0, failed=0, succeeded=1, handled=1)
        self.assertEqual(self.finish(t, **result)['state'], 'done')
        self.assertTrue(self.finish(t, **result)['accepted'])
        self.assertEqual(self.q.snapshot()['completed_files'],1)

    def test_expiry_fences_old_worker_and_reassigns(self):
        self.q.add('dataset',['/source/one'],[100])
        old = self.claim()
        with self.q.db:
            self.q.db.execute('UPDATE tasks SET heartbeat=?', (time.time()-300,))
        new = self.claim('b')
        self.assertNotEqual(old['token'],new['token'])
        self.assertFalse(self.finish(old, exit_code=0, failed=0, succeeded=1, handled=1)['accepted'])
        heartbeat = self.q.rpc('heartbeat',dict(host='a',active=[dict(id=old['id'],token=old['token'])]))
        self.assertEqual(heartbeat['accepted'],[])
        self.assertEqual(self.finish(new,host='b',exit_code=0,failed=0,succeeded=1,handled=1)['state'],'done')

    def test_lost_claim_response_is_idempotent_and_node_cap_is_enforced(self):
        self.config['slots'] = 2
        for i in range(4):
            self.q.add('dataset',[f'/source/{i}'],[100])
        one = self.q.rpc('claim',dict(host='a',request_id='one'))['task']
        replay = self.q.rpc('claim',dict(host='a',request_id='one'))['task']
        self.assertEqual(one['id'],replay['id'])
        self.assertEqual(one['token'],replay['token'])
        self.assertEqual(replay['attempt'],1)
        self.q.rpc('claim',dict(host='a',request_id='two'))
        self.assertTrue(self.q.rpc('claim',dict(host='a',request_id='three'))['wait'])
        self.q.rpc('heartbeat',dict(host='a',active=[dict(id=one['id'],token=one['token'])]))
        s = self.q.snapshot()
        self.assertEqual(s['leased_chunks'],2)
        self.assertEqual(len(s['active']),1)
        self.assertEqual(s['nodes']['a']['active_slots'],1)

    def test_local_database_mirror_survives_loss_of_local_disk(self):
        self.q.add('dataset',['/source/one'],[100])
        c = dict(self.config,queue_db=str(self.base/'local'/'queue.sqlite'))
        local = Queue(c)
        try:
            task = local.rpc('claim',dict(host='a',request_id='persisted'))['task']
            local.rpc('finish',dict(host='a',id=task['id'],token=task['token'],result=dict(exit_code=0,failed=0,succeeded=1,handled=1)))
            local.mirror()
        finally:
            local.db.close()
        # The shared snapshot can rebuild a fresh local primary after node replacement.
        c['queue_db'] = str(self.base/'replacement'/'queue.sqlite')
        restored = Queue(c)
        try:
            self.assertEqual(restored.snapshot()['completed_files'],1)
        finally:
            restored.db.close()

    def test_drain_keeps_existing_upload_until_finish(self):
        for i in range(2):
            self.q.add('dataset',[f'/source/{i}'],[100])
        task = self.claim()
        self.config['minimum_worker_protocol'] = 2
        self.assertTrue(self.q.rpc('claim',dict(host='a'))['done'])
        r = self.q.rpc('heartbeat',dict(host='a',active=[dict(id=task['id'],token=task['token'])]))
        self.assertEqual(r['accepted'],[task['id']])
        self.assertEqual(self.finish(task,exit_code=0,failed=0,succeeded=1,handled=1)['state'],'done')
        self.assertIn('task',self.q.rpc('claim',dict(host='a',protocol_version=2,request_id='new-client')))

    def test_unanswered_claim_does_not_exhaust_upload_retry_budget(self):
        self.config['attempts'] = 1
        self.q.add('dataset',['/source/one'],[100])
        self.claim()
        with self.q.db:
            self.q.db.execute('UPDATE tasks SET heartbeat=?',(time.time()-300,))
        self.assertIsNotNone(self.claim('b'))

    def test_container_metrics_separate_cache_and_oom(self):
        base = self.base/'cgroup'
        base.mkdir()
        for name, value in {'memory.max':str(16*1024**3),'memory.current':str(15*1024**3),
                            'memory.stat':f'anon {1024**3}\nfile {14*1024**3}\n',
                            'memory.events':'oom 2\noom_kill 1\n',
                            'cpu.max':'400000 100000','cpu.stat':'usage_usec 2000000\nthrottled_usec 1000\n'}.items():
            (base/name).write_text(value)
        m = cgroup_metrics(base)
        self.assertEqual(m['memory_limit_GiB'],16)
        self.assertEqual(m['memory_file_cache_GiB'],14)
        self.assertEqual(m['memory_used_pct'],6.25)
        self.assertEqual(m['oom_kill_events'],1)
        self.assertLessEqual(m['cpus'],4)

    def test_partial_success_retries_and_caps_attempts(self):
        self.q.add('dataset',['/source/one','/source/two'],[100,200])
        t = self.claim()
        self.assertEqual(self.finish(t,exit_code=0,failed=0,succeeded=1,handled=1)['state'],'pending')
        self.assertIsNone(self.claim())
        with self.q.db:
            self.q.db.execute('UPDATE tasks SET ready=0')
        t = self.claim('b')
        self.assertEqual(self.finish(t,host='b',exit_code=1,failed=1,succeeded=1,handled=2)['state'],'failed')
        self.assertEqual(self.q.snapshot()['completed_files'],0)

    def test_scan_restart_deduplicates_and_excludes_nested_dataset(self):
        for name in ['a','b','c']:
            (self.src/name).write_bytes(b'hello')
        nested = self.src/'hand_pose'
        nested.mkdir()
        (nested/'x').write_bytes(b'ignored')
        (self.src/'broken').symlink_to('/missing-target')
        (self.src/'linked-directory').symlink_to(nested, target_is_directory=True)
        task = dict(self.config['tasks'][0],exclude_child='hand_pose')
        self.q.scan(task)
        s = self.q.snapshot()
        self.assertEqual(s['discovered_files'],3)
        self.assertIn('2 excluded',s['scans'][0]['error'])
        self.q.scan_state('dataset','scanning')
        self.q.scan(task)
        self.assertEqual(self.q.snapshot()['discovered_files'],3)
        manifests = list((self.q.base/'manifests'/'dataset').glob('*.txt'))
        self.assertTrue(all('hand_pose' not in m.read_text() for m in manifests))
        t = self.claim()
        self.assertEqual(t['config']['source'],str(self.src))
        self.assertEqual(t['config']['destination'],'ks3://bucket/original/')

    def test_http_coordinator_and_two_workers_end_to_end(self):
        fake = self.base/'fake-ks3util'
        fake.write_text('#!/usr/bin/env python3\nimport sys,time\nfrom pathlib import Path\n'
                        'n=len(Path(sys.argv[sys.argv.index("--object-file")+1]).read_text().splitlines())\n'
                        'time.sleep(0.2)\nprint(f"Succeed count: {n} Failed count: 0")\n')
        fake.chmod(0o755)
        for i in range(20):
            (self.src/str(i)).write_text('payload')
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0))
            port = sock.getsockname()[1]
        c = dict(self.config, tool=str(fake), endpoint=f'http://127.0.0.1:{port}',port=port,
                 slots=2,heartbeat_seconds=0.1,worker_disconnect_seconds=3,local_root=str(self.base/'local'))
        (self.q.base/'token').write_text('test-token')
        conf = self.base/'config.json'
        conf.write_text(json.dumps(c))
        env = dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
        coordinator = subprocess.Popen([sys.executable,'-m','ks_cloud.dynamic','coordinator','--config',str(conf)],env=env,
                                       stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        children = []
        try:
            deadline = time.monotonic()+10
            while not (self.q.base/'status.json').exists() and coordinator.poll() is None and time.monotonic()<deadline:
                time.sleep(0.1)
            self.assertIsNone(coordinator.poll())
            for host in ['a','b']:
                p = self.base/(host+'.json')
                p.write_text(json.dumps(dict(c,local_root=str(self.base/host))))
                children.append(subprocess.Popen([sys.executable,'-m','ks_cloud.dynamic','worker','--config',str(p),'--host',host],
                                                 env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE))
            for p in children:
                self.assertEqual(p.wait(timeout=20),0,p.stderr.read().decode())
            s = self.q.snapshot()
            self.assertEqual(s['completed_files'],20)
            self.assertEqual(s['state'],'finished')
            self.assertEqual(len(s['completed_by_node']),2)
        finally:
            for p in children+[coordinator]:
                if p.poll() is None:
                    p.terminate()
                    p.wait(timeout=10)
                if p.stderr:
                    p.stderr.close()

    def test_parent_death_stops_upload_child(self):
        env = dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
        script = ('import os,subprocess,sys,time\n'
                  'p=subprocess.Popen([sys.executable,"-S","-B","-m","ks_cloud.exec_child",str(os.getpid()),"/bin/sleep","60"])\n'
                  'print(p.pid,flush=True)\ntime.sleep(60)\n')
        parent = subprocess.Popen([sys.executable,'-S','-B','-c',script],env=env,stdout=subprocess.PIPE,text=True)
        child_pid = int(parent.stdout.readline())
        try:
            deadline = time.monotonic()+5
            while time.monotonic()<deadline:
                cmd = Path(f'/proc/{child_pid}/cmdline')
                if cmd.exists() and cmd.read_bytes().startswith(b'/bin/sleep'):
                    break
                time.sleep(0.05)
            else:
                self.fail('guarded child failed to exec')
            parent.kill()
            parent.wait(timeout=5)
            deadline = time.monotonic()+5
            while time.monotonic()<deadline:
                p = Path(f'/proc/{child_pid}/stat')
                if not p.exists() or p.read_text().split(') ',1)[1].split()[0]=='Z':
                    break
                time.sleep(0.05)
            else:
                self.fail('upload child survived parent death')
        finally:
            if parent.poll() is None:
                parent.kill()
                parent.wait()
            parent.stdout.close()


if __name__ == '__main__':
    unittest.main()
