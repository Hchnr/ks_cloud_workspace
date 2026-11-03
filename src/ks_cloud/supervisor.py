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

from .common import save


def run(config_path, host):
    config = json.loads(Path(config_path).read_text())
    local = Path(config['local_root'])
    local.mkdir(parents=True, exist_ok=True)
    lock = (local / 'supervisor.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    shared = Path(config['run_dir'])
    commands = dict(worker=[sys.executable,'-S','-B','-m','ks_cloud.dynamic','worker','--config',config_path,'--host',host])
    if host == config['hosts'][0]:
        commands['coordinator'] = [sys.executable,'-S','-B','-m','ks_cloud.dynamic','coordinator','--config',config_path]
        if config.get('swanlab_enabled', True):
            commands['swanlab'] = [config['swanlab_python'],'-B','-m','ks_cloud.telemetry','--config',config_path]
    children, restarts, next_start = {}, {}, {}
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopping:
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
                next_start[name] = max(next_start.get(name,0),time.time()+30)
                children.pop(name)
                child = None
            should_run = name == 'coordinator' or (not terminal and not (shared/'STOP').exists())
            if name != 'swanlab' and config.get('require_start_file') and not (shared/'START').exists():
                should_run = False
            if child is None and should_run and time.time() >= next_start.get(name,0):
                logfile = local / (name+'.log')
                if logfile.exists() and logfile.stat().st_size > 8*1024**2:
                    os.replace(logfile,logfile.with_suffix('.previous.log'))
                fd = os.open(logfile,os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
                with os.fdopen(fd,'ab',buffering=0) as out:
                    child = subprocess.Popen(cmd,stdin=subprocess.DEVNULL,stdout=out,stderr=out,start_new_session=True)
                children[name] = child
                restarts[name] = restarts.get(name,0)+1
                next_start[name] = time.time()+min(1800,30*2**min(restarts[name]-1,6)) if name=='swanlab' else time.time()+30
            if child:
                statuses[name] = dict(pid=child.pid,alive=child.poll() is None,starts=restarts.get(name,0))
        save(shared / 'supervisors' / (host+'.json'), dict(host=host,pid=os.getpid(),updated=time.time(),services=statuses))
        time.sleep(5)
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
