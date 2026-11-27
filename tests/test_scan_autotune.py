import sys
import json
from pathlib import Path
import subprocess
import tempfile
import time
import os
import signal
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from ks_cloud.scan_autotune import ScanAutotuner


def snapshot(now, completed, maximum=4, active=4, io=10, enabled=True, cooldown=0, oom=0):
    return dict(updated=now, completed_files=completed,
                parallel_scan=dict(max_nodes=maximum, active_nodes=active, queued=0,
                                   enabled=enabled, cooldown_seconds=cooldown),
                scan_control=dict(pressure=dict(io_pressure_some_avg10=io, memory_used_pct=10)),
                nodes={'one':dict(healthy=True, oom_kill_events=oom)})


class ScanAutotunerTests(unittest.TestCase):
    def test_probe_then_pressure_rolls_back(self):
        c=ScanAutotuner(4)
        self.assertEqual(c.observe(snapshot(1000,0,enabled=False,cooldown=200),4)[0],None)
        result=None
        for t in (1030,1060,1090,1120,1150):
            result=c.observe(snapshot(t,(t-1030)*1000),4)
        self.assertEqual(result,(8,'sustained_demand_probe'))
        self.assertEqual(c.observe(snapshot(1180,160000,maximum=8,active=8,io=66),8),
                         (4,'resource_pressure_or_cooldown'))

    def test_sustained_regression_rolls_back(self):
        c=ScanAutotuner(4)
        for t in (1000,1030,1060,1090,1120):
            result=c.observe(snapshot(t,(t-1000)*1000),4)
        self.assertEqual(result[0],8)
        result=None
        for t in (1150,1180,1210,1240,1270,1300):
            result=c.observe(snapshot(t,120000+(t-1150)*100,maximum=8,active=8),8)
            if result[0] is not None:break
        self.assertEqual(result,(4,'sustained_throughput_regression'))

    def test_idle_or_external_change_does_not_expand(self):
        c=ScanAutotuner(4)
        for t in (1000,1030,1060,1090,1120,1150):
            self.assertIsNone(c.observe(snapshot(t,(t-1000)*1000,active=0),4)[0])
        self.assertEqual(c.observe(snapshot(1180,180000),2),(None,'configuration_changed'))

    def test_restarted_controller_reduces_existing_eight_node_cooldown(self):
        c=ScanAutotuner(8)
        self.assertEqual(c.observe(snapshot(1000,0,maximum=8,active=0,enabled=False,cooldown=200),8),
                         (4,'resource_pressure_or_cooldown'))

    def test_buffered_work_counts_as_demand_only_when_workers_advance(self):
        c=ScanAutotuner(4)
        result=None
        for t in (1000,1030,1060,1090,1120):
            s=snapshot(t,(t-1000)*1000,active=0)
            s['parallel_scan'].update(buffered=4,nodes={'one':dict(files=(t-1000)*1000)})
            result=c.observe(s,4)
        self.assertEqual(result,(8,'sustained_demand_probe'))
        idle=ScanAutotuner(4)
        for t in (1000,1030,1060,1090,1120,1150):
            s=snapshot(t,(t-1000)*1000,active=0)
            s['parallel_scan'].update(buffered=4,nodes={'one':dict(files=100)})
            self.assertIsNone(idle.observe(s,4)[0])

    def test_standby_respects_an_external_tuning_change(self):
        script = Path(__file__).resolve().parents[1]/'src/ks_cloud/scan_autotune.py'
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'monitoring').mkdir()
            tuning=root/'parallel-scan-tuning.json'
            tuning.write_text(json.dumps(dict(enabled=True,max_nodes=4)))
            (root/'global-followup-state.json').write_text(json.dumps(dict(phase='checking',run_dir=str(root))))
            status=snapshot(time.time(),1000)
            status['state']='running'
            (root/'status.json').write_text(json.dumps(status))
            processes=[]
            try:
                processes.append(subprocess.Popen([sys.executable,str(script),str(root)]))
                report=root/'monitoring'/'automatic-scan-tuning.json'
                until=time.monotonic()+8
                while not report.exists() and time.monotonic()<until:time.sleep(.05)
                self.assertTrue(report.exists())
                processes.append(subprocess.Popen([sys.executable,str(script),str(root)]))
                tuning.write_text(json.dumps(dict(enabled=True,max_nodes=3)))
                status['updated']=time.time()+1
                (root/'status.json').write_text(json.dumps(status))
                for process in processes:self.assertEqual(process.wait(timeout=20),0)
                self.assertEqual(json.loads(report.read_text())['phase'],'configuration_changed')
                self.assertEqual(json.loads(tuning.read_text())['max_nodes'],3)
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=5)

    def test_supervisor_restarts_the_root_tuner(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'run';root.mkdir()
            (root/'monitoring').mkdir()
            (root/'parallel-scan-tuning.json').write_text(json.dumps(dict(enabled=True,max_nodes=4)))
            (root/'global-followup-state.json').write_text(json.dumps(dict(phase='checking',run_dir=str(root))))
            status=snapshot(time.time(),1000)
            status['state']='running'
            (root/'status.json').write_text(json.dumps(status))
            config=dict(run_dir=str(root),local_root=str(Path(directory)/'local'),hosts=['a'],
                        coordinator_hosts=['a'],ha_enabled=False,swanlab_enabled=False,
                        global_followup_enabled=True,scan_autotune_enabled=True,
                        supervisor_interval=.1,service_restart_seconds=.2)
            path=Path(directory)/'config.json';path.write_text(json.dumps(config))
            env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'),
                     PYTHONDONTWRITEBYTECODE='1')
            parent=subprocess.Popen([sys.executable,'-B','-m','ks_cloud.supervisor','--config',str(path),'--host','a'],
                                    env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            def wait_for(check):
                deadline=time.monotonic()+12
                while time.monotonic()<deadline:
                    try:
                        value=check()
                        if value:return value
                    except (OSError,ValueError,KeyError):pass
                    time.sleep(.1)
                self.fail('supervisor did not restore the tuner')
            def tuner_pid():
                return json.loads((root/'supervisors'/'a.json').read_text())['services']['scan_autotune']['pid']
            try:
                original=wait_for(tuner_pid)
                os.kill(original,signal.SIGKILL)
                replacement=wait_for(lambda: (p if (p:=tuner_pid()) != original else None))
                self.assertNotEqual(original,replacement)
            finally:
                parent.terminate()
                try:parent.wait(timeout=8)
                except subprocess.TimeoutExpired:parent.kill();parent.wait()


if __name__=='__main__':
    unittest.main()
