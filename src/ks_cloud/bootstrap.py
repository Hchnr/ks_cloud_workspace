"""Idempotent remote bootstrap; invoked by surviving CPU guardians over SSH."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def bootstrap(config_path, host):
    config = json.loads(Path(config_path).read_text())
    assert host in config['hosts']
    if (Path(config['run_dir'])/'recovery-hold'/host).exists():
        return dict(host=host,state='held')
    local = Path(config['local_root'])
    local.mkdir(parents=True, exist_ok=True)
    with (local/'bootstrap.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = {}
        try:
            state = json.loads((local/'recovery.json').read_text())
            pid = state['pid']
            cmd = (Path('/proc')/str(pid)/'cmdline').read_bytes().split(b'\0')
            matching = b'ks_cloud.recovery' in cmd and config_path.encode() in cmd and host.encode() in cmd
            if matching and time.time()-state['updated'] < config.get('guardian_stall_seconds', 90):
                return dict(host=host, state='alive', pid=pid)
            if matching:
                os.kill(pid, signal.SIGKILL)
        except (OSError, ValueError, KeyError):
            pass
        package = local/'code'/'ks_cloud'
        package.mkdir(parents=True, exist_ok=True)
        for source in Path(config['recovery_code_dir']).glob('*.py'):
            target = package/source.name
            temporary = target.with_suffix('.py.bootstrap-tmp')
            temporary.write_bytes(source.read_bytes())
            os.replace(temporary,target)
        env = dict(os.environ, PYTHONPATH=str(package.parent), PYTHONDONTWRITEBYTECODE='1')
        log = local/'recovery.log'
        if log.exists() and log.stat().st_size > 8*1024**2:
            os.replace(log,local/'recovery.previous.log')
        with log.open('ab',buffering=0) as stream:
            child = subprocess.Popen(['python3','-S','-B','-m','ks_cloud.recovery',
                                      '--config',config_path,'--host',host],env=env,
                                     stdin=subprocess.DEVNULL,stdout=stream,stderr=stream,start_new_session=True)
        # Serial callers also see an in-flight start, before guardian initialization.
        temp=local/'recovery.json.tmp'
        temp.write_text(json.dumps(dict(pid=child.pid,updated=time.time(),host=host,state='starting')))
        os.replace(temp,local/'recovery.json')
        return dict(host=host,state='started',pid=child.pid)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    parser.add_argument('--host',required=True)
    args=parser.parse_args()
    print(json.dumps(bootstrap(args.config,args.host)))
