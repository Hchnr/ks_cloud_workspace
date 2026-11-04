"""Node-local process supervision; transfers survive SSH/chat disconnection."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request

from .common import save
from .ha import leader_record, live_leader


def run(config_path, host):
    config = json.loads(Path(config_path).read_text())
    local = Path(config['local_root'])
    local.mkdir(parents=True, exist_ok=True)
    lock = (local / 'supervisor.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    shared = Path(config['run_dir'])
    commands = dict(worker=[sys.executable,'-S','-B','-m','ks_cloud.dynamic','worker','--config',config_path,'--host',host])
    ha = config.get('ha_enabled',False)
    candidates = config.get('coordinator_hosts', [config['hosts'][0]])
    if host in candidates:
        commands['coordinator'] = [sys.executable,'-S','-B','-m','ks_cloud.dynamic','coordinator','--config',config_path,'--host',host]
        if config.get('swanlab_enabled', True):
            commands['swanlab'] = [config['swanlab_python'],'-B','-m','ks_cloud.telemetry','--config',config_path]
    children, restarts, next_start = {}, {}, {}
    stopping = False
    boot = time.monotonic()
    health_failures = 0
    last_health = 0
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def stop(*_):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopping:
        save(local/'supervisor-heartbeat.json',dict(pid=os.getpid(),updated=time.time()))
        leader = leader_record(config) if ha else {}
        leader_alive = live_leader(config,leader) if ha else False
        state_file = shared / 'status.json'
        try:
            terminal = json.loads(state_file.read_text())['state'].startswith('finished')
        except (OSError, ValueError, KeyError):
            terminal = False
        statuses = {}
        for name, cmd in commands.items():
            child = children.get(name)
            if child is not None and child.poll() is not None:
                statuses[name] = dict(exit_code=child.returncode)
                next_start[name] = max(next_start.get(name,0),time.time()+config.get('service_restart_seconds',30))
                children.pop(name)
                child = None
            should_run = name == 'coordinator' or (not terminal and not (shared/'STOP').exists())
            if ha and name == 'coordinator':
                if leader_alive and leader.get('host') != host:
                    should_run = False
                if not leader and time.monotonic()-boot < candidates.index(host)*config.get('candidate_delay_seconds',10):
                    should_run = False
            if ha and name == 'swanlab':
                owner = children.get('coordinator')
                should_run = bool(should_run and owner and owner.poll() is None
                                  and leader.get('host')==host and leader.get('pid')==owner.pid and leader_alive)
                if child and not should_run:
                    os.killpg(child.pid,signal.SIGTERM)
            if name != 'swanlab' and config.get('require_start_file') and not (shared/'START').exists():
                should_run = False
            if child is None and should_run and time.time() >= next_start.get(name,0):
                logfile = local / (name+'.log')
                if logfile.exists() and logfile.stat().st_size > 8*1024**2:
                    os.replace(logfile,logfile.with_suffix('.previous.log'))
                fd = os.open(logfile,os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
                with os.fdopen(fd,'ab',buffering=0) as out:
                    guarded = [sys.executable,'-S','-B','-m','ks_cloud.exec_child',str(os.getpid())]+cmd
                    child = subprocess.Popen(guarded,stdin=subprocess.DEVNULL,stdout=out,stderr=out,start_new_session=True)
                children[name] = child
                restarts[name] = restarts.get(name,0)+1
                next_start[name] = time.time()+min(1800,30*2**min(restarts[name]-1,6)) if name=='swanlab' else time.time()+30
            if child:
                statuses[name] = dict(pid=child.pid,alive=child.poll() is None,starts=restarts.get(name,0))
        owner = children.get('coordinator')
        if ha and owner and owner.poll() is None and leader.get('host')==host and time.time()-last_health>=15:
            last_health=time.time()
            try:
                token=(shared/'token').read_text().strip()
                request=urllib.request.Request(leader['endpoint']+'/health',json.dumps(dict(host=host)).encode(),
                                               {'Authorization':'Bearer '+token,'Content-Type':'application/json'})
                with opener.open(request,timeout=3) as response:
                    assert json.load(response)['ok']
                health_failures=0
            except Exception:
                health_failures+=1
                if health_failures>=3:
                    os.killpg(owner.pid,signal.SIGKILL)
                    health_failures=0
        save(shared / 'supervisors' / (host+'.json'), dict(host=host,pid=os.getpid(),updated=time.time(),services=statuses))
        time.sleep(config.get('supervisor_interval',5))
    for child in children.values():
        if child.poll() is None:
            os.killpg(child.pid,signal.SIGTERM)
    for child in children.values():
        try:
            child.wait(timeout=20)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid,signal.SIGKILL)
            child.wait()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--host',required=True)
    args = parser.parse_args()
    run(args.config,args.host)
