import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from ks_cloud.common import save
from ks_cloud.ha import Discovery, Leadership, restore_for_leadership, locked_owner


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.config=dict(run_dir=str(self.root),queue_db=str(self.root/'local'/'queue.sqlite'),
                         endpoint='http://127.0.0.1:1',hosts=['a','b'],ha_enabled=True,
                         coordinator_endpoints={'a':'http://127.0.0.1:1','b':'http://127.0.0.1:2'})

    def tearDown(self):
        self.tmp.cleanup()

    def test_takeover_replaces_stale_local_database_with_shared_snapshot(self):
        shared=sqlite3.connect(self.root/'queue.sqlite')
        shared.execute('CREATE TABLE marker(value TEXT)');shared.execute("INSERT INTO marker VALUES ('new')");shared.commit();shared.close()
        local=Path(self.config['queue_db']);local.parent.mkdir()
        old=sqlite3.connect(local);old.execute('CREATE TABLE marker(value TEXT)');old.execute("INSERT INTO marker VALUES ('stale')");old.commit();old.close()
        save(self.root/'leader.json',dict(host='a',generation=3))
        previous,restored=restore_for_leadership(self.config,'b')
        self.assertTrue(restored)
        self.assertEqual(previous['generation'],3)
        db=sqlite3.connect(local)
        self.assertEqual(db.execute('SELECT value FROM marker').fetchone()[0],'new');db.close()
        with self.assertRaises(RuntimeError):
            (self.root/'queue.sqlite').unlink();restore_for_leadership(self.config,'b')

    def test_discovery_moves_workers_and_leadership_fails_closed(self):
        d=Discovery(self.config)
        leader=Leadership(self.config,'b',{},False)
        leader.publish()
        self.assertEqual(d.get(force=True),'http://127.0.0.1:2')
        self.assertTrue(leader.healthy())
        leader.last_success-=100
        self.assertFalse(leader.healthy())
        save(self.root/'leader.json',dict(host='a',generation=leader.generation+1,epoch='newer'))
        with self.assertRaises(RuntimeError):leader.publish()

    def test_locked_owner_survives_partial_tail_and_overrides_stale_leader_file(self):
        save(self.root/'leader.json',dict(host='a',generation=1,epoch='stale'))
        with (self.root/'coordinator.lock').open('a+') as lock:
            lock.write(json.dumps(dict(host='b',generation=4,epoch='latest'))+'\n{"partial":')
            lock.flush()
            previous=locked_owner(lock,self.config)
            self.assertEqual(previous['host'],'b')
            leader=Leadership(self.config,'a',previous,False,lock=lock)
            leader.publish()
            self.assertEqual(locked_owner(lock,self.config)['generation'],5)
        save(self.root/'leader.json',dict(host='a',generation=5,epoch=leader.epoch))
        with self.assertRaises(RuntimeError):restore_for_leadership(self.config,'a')

    def test_remote_guardian_loss_triggers_standby_takeover_and_same_node_repair(self):
        source=self.root/'source';source.mkdir()
        for i in range(80):(source/str(i)).write_text('payload')
        fake=self.root/'fake'
        fake.write_text('#!/usr/bin/env python3\nimport sys,time\nfrom pathlib import Path\n'
                        'n=len(Path(sys.argv[sys.argv.index("--object-file")+1]).read_text().splitlines())\n'
                        'time.sleep(.15)\nprint(f"Succeed count: {n} Failed count: 0")\n')
        fake.chmod(0o755)
        endpoints={}
        for host in ['a','b']:
            with socket.socket() as s:s.bind(('127.0.0.1',0));endpoints[host]=f'http://127.0.0.1:{s.getsockname()[1]}'
        shared=self.root/'run';shared.mkdir();(shared/'token').write_text('test-token')
        common=dict(self.config,run_dir=str(shared),endpoint=endpoints['a'],coordinator_endpoints=endpoints,
                    coordinator_hosts=['a','b'],swanlab_enabled=False,tool=str(fake),config_file='/private',
                    tasks=[dict(id='dataset',source=str(source),destination='ks3://bucket/source/')],
                    jobs=1,parallel=1,list_parallel=1,slots=1,chunk_files=1,pending_per_source=100,
                    lease_seconds=1,heartbeat_seconds=.1,worker_disconnect_seconds=.5,
                    queue_mirror_seconds=.2,coordinator_publish_seconds=.2,leader_heartbeat_seconds=.2,
                    leader_lease_seconds=.8,leader_refresh_seconds=.1,ha_recovery_grace_seconds=.5,
                    supervisor_interval=.1,service_restart_seconds=.2,candidate_delay_seconds=.3,
                    guardian_interval=.1,guardian_restart_seconds=.2,supervisor_stall_seconds=5)
        env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'),PYTHONDONTWRITEBYTECODE='1')
        children=[];logs=[]
        def wait_for(check,seconds=20):
            end=time.monotonic()+seconds
            while time.monotonic()<end:
                try:
                    result=check()
                    if result:return result
                except (OSError,ValueError,KeyError):pass
                time.sleep(.1)
            self.fail('recovery condition not reached; logs: '+ '\n'.join(p.read_text(errors='replace')[-2500:] for p in logs))
        def load(p):return json.loads(p.read_text())
        configs={}
        try:
            for host in ['a','b']:
                local=self.root/host
                c=dict(common,local_root=str(local),queue_db=str(local/'queue.sqlite'))
                config=self.root/(host+'.json');config.write_text(json.dumps(c));configs[host]=config
                log=self.root/(host+'.log');logs.append(log)
                with log.open('wb') as out:
                    children.append(subprocess.Popen([sys.executable,'-B','-m','ks_cloud.recovery','--config',str(config),'--host',host],env=env,stdout=out,stderr=out))
                if host=='a':wait_for(lambda:load(shared/'leader.json').get('host')=='a')
            wait_for(lambda:load(shared/'status.json')['completed_files']>=2)
            wait_for(lambda:(shared/'queue_mirror.json').exists())
            children[0].kill();children[0].wait(timeout=5)
            wait_for(lambda:load(shared/'leader.json').get('host')=='b')
            wait_for(lambda:load(shared/'status.json')['completed_files']>=10)
            old=load(self.root/'b'/'supervisor-heartbeat.json')['pid']
            os.kill(old,signal.SIGKILL)
            wait_for(lambda:load(self.root/'b'/'supervisor-heartbeat.json')['pid']!=old)
            wait_for(lambda:load(shared/'status.json')['completed_files']>=15)
            self.assertEqual(load(shared/'leader.json')['host'],'b')
        finally:
            for p in children:
                if p.poll() is None:p.terminate()
            for p in children:
                if p.poll() is None:
                    try:p.wait(timeout=8)
                    except subprocess.TimeoutExpired:p.kill();p.wait()


if __name__=='__main__':unittest.main()
