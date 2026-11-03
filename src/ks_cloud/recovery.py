"""Node-local guardian plus remote peer repair, independent of the control container."""
import argparse
import concurrent.futures
import fcntl
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import threading
import time

from .common import save
from .ha import read_json


def peer_command(config, host):
    target=config['recovery_ssh_targets'][host]
    remote=['python3','-S','-B',str(Path(config['recovery_code_dir'])/'bootstrap.py'),
            '--config',config['recovery_config_path'],'--host',host]
    return ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','StrictHostKeyChecking=yes',
            '-o','UserKnownHostsFile='+config['recovery_known_hosts'],
            '-i',config['recovery_ssh_identity'],'-p',str(target.get('port',22)),
            target.get('user','root')+'@'+target['hostname'],shlex.join(remote)]


def run(config_path,host):
    config=json.loads(Path(config_path).read_text())
    local=Path(config['local_root']);local.mkdir(parents=True,exist_ok=True)
    lock=(local/'recovery.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    config['recovery_config_path']=config_path
    stopping=threading.Event()
    signal.signal(signal.SIGTERM,lambda *_:stopping.set())
    signal.signal(signal.SIGINT,lambda *_:stopping.set())
    state_path=local/'recovery.json'
    previous=read_json(state_path,{})
    starts=previous.get('supervisor_starts',0)
    peers={}
    def peer_loop():
        def repair(target):
            try:
                r=subprocess.run(peer_command(config,target),stdin=subprocess.DEVNULL,
                                 capture_output=True,text=True,timeout=20)
                return target,dict(ok=r.returncode==0,updated=time.time())
            except (OSError,subprocess.TimeoutExpired):
                return target,dict(ok=False,updated=time.time())
        while not stopping.wait(config.get('peer_check_seconds',60)):
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                for target,result in pool.map(repair,[h for h in config['hosts'] if h!=host]):
                    peers[target]=result
    if host in config.get('coordinator_hosts',[]) and config.get('recovery_ssh_targets'):
        threading.Thread(target=peer_loop,daemon=True).start()
    def report_loop():
        while not stopping.is_set():
            try:
                state=read_json(state_path,{})
                save(Path(config['run_dir'])/'recovery'/(host+'.json'),state)
            except OSError:
                pass
            stopping.wait(30)
    threading.Thread(target=report_loop,daemon=True).start()
    child=None;next_start=0;child_started=0
    try:
        while not stopping.is_set():
            now=time.time()
            heartbeat=read_json(local/'supervisor-heartbeat.json',{})
            stalled=(child and child.poll() is None and now-max(child_started,heartbeat.get('updated',0))
                     >config.get('supervisor_stall_seconds',90))
            if stalled:
                os.kill(child.pid,signal.SIGKILL)
                print('Supervisor heartbeat stalled; restarting',flush=True)
            if child and child.poll() is not None:
                child=None;next_start=now+config.get('guardian_restart_seconds',5)
            if child is None and now>=next_start:
                command=[sys.executable,'-S','-B','-m','ks_cloud.exec_child',str(os.getpid()),
                         sys.executable,'-S','-B','-m','ks_cloud.supervisor','--config',config_path,'--host',host]
                with (local/'supervisor.log').open('ab',buffering=0) as stream:
                    child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=stream,
                                           stderr=stream,start_new_session=True)
                starts+=1;child_started=now
            save(state_path,dict(host=host,pid=os.getpid(),updated=now,
                                 supervisor_pid=child.pid if child else None,
                                 supervisor_starts=starts,peers=dict(peers)))
            stopping.wait(config.get('guardian_interval',5))
    finally:
        if child and child.poll() is None:
            child.terminate()
            try:child.wait(timeout=25)
            except subprocess.TimeoutExpired:child.kill();child.wait()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True);parser.add_argument('--host',required=True)
    args=parser.parse_args();run(args.config,args.host)
