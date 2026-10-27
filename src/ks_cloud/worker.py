"""Node-local runner. Resume successful tasks and retry failed/incomplete tasks."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from .common import parse_progress, save, successful, tail, upload_command


def run(directory):
    base = Path(directory).resolve()
    with (base / 'lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        plan = json.loads((base / 'plan.json').read_text())
        results = []
        stopping = False
        child = None

        def stop(signum, frame):
            nonlocal stopping
            stopping = True
            if child and child.poll() is None:
                child.terminate()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        state = dict(host=plan['host'], pid=os.getpid(), started=time.time(),
                     total_tasks=len(plan['tasks']), total_reference=sum(t['weight'] for t in plan['tasks']))

        def publish(status, task=None, progress=None):
            state.update(state=status, updated=time.time(), task=task,
                         child_pid=child.pid if child else None, progress=progress or {},
                         completed=len(results), handled_completed=sum(r.get('handled', 0) for r in results),
                         bytes_completed=sum(r.get('bytes', r.get('succeeded_bytes', 0) + r.get('skipped_bytes', 0)) for r in results),
                         failed_tasks=sum(not successful(r) for r in results))
            save(base / 'status.json', state)

        publish('starting')
        try:
            for task in plan['tasks']:
                if stopping or (base / 'STOP').exists():
                    break
                folder = base / task['id']
                folder.mkdir(exist_ok=True)
                result_file = folder / 'result.json'
                if result_file.exists():
                    prior = json.loads(result_file.read_text())
                    if successful(prior):
                        results.append(prior)
                        continue
                attempt = len(list(folder.glob('attempt-*.log'))) + 1
                log = folder / f'attempt-{attempt}.log'
                args = upload_command(plan['config'], task['manifest'], folder)
                save(folder / 'command.json', args)
                with log.open('ab', buffering=0) as out:
                    child = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=out, stderr=out)
                    while child.poll() is None:
                        publish('uploading', task['id'], parse_progress(tail(log)))
                        for _ in range(30):
                            if (base / 'STOP').exists():
                                stop(None, None)
                            if child.poll() is not None:
                                break
                            time.sleep(1)
                result = parse_progress(tail(log))
                result.update(task=task['id'], exit_code=child.returncode, finished=time.time(), log=str(log))
                save(result_file, result)
                if stopping:
                    break
                results.append(result)
            publish('stopped' if stopping or (base / 'STOP').exists() else 'finished')
        except Exception as error:
            state['error'] = str(error)
            publish('error')
            raise
        finally:
            if child and child.poll() is None:
                child.terminate()
                child.wait(timeout=30)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', required=True)
    run(parser.parse_args().run_dir)
