"""Resume directory-link presence checks after a full, non-destructive scan."""
import argparse
import concurrent.futures
import fcntl
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time

from .common import save
from .recovery import peer_command


def terminal_status(path, expected_sources=None):
    try:
        status = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None, 'status unavailable'
    if not status.get('state', '').startswith('finished'):
        return None, 'running'
    if expected_sources is not None and {s.get('source') for s in status.get('scans', [])} != set(expected_sources):
        return None, 'source scan records incomplete'
    if not status.get('scan_complete') or any(s.get('state') != 'done' for s in status.get('scans', [])):
        return None, 'scan failed or incomplete'
    if any(g.get('state') != 'done' for g in status.get('groups', [])):
        return None, 'unfinished or failed chunks'
    return status, ''


def snapshot_link_inputs(config):
    """Publish a stable, complete copy of only the original link exclusions."""
    base = Path(config['run_dir'])
    final = base / 'followup-inputs'
    if final.exists():
        return json.loads((final / 'counts.json').read_text())
    temp = Path(tempfile.mkdtemp(prefix='followup-inputs-', dir=base))
    counts = {}
    try:
        for task in config['tasks']:
            source = task['id']
            count = 0
            with (base / ('excluded-' + source + '.jsonl')).open() as inp, (temp / (source + '.jsonl')).open('w') as out:
                for line in inp:
                    item = json.loads(line)
                    if item.get('reason') == 'directory symlink (not followed)':
                        out.write(line)
                        count += 1
            counts[source] = count
        save(temp / 'counts.json', counts)
        os.rename(temp, final)
    finally:
        if temp.exists():
            shutil.rmtree(temp)
    return counts


def ssh_prefix(config, host):
    target = config['recovery_ssh_targets'][host]
    return ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', '-o', 'StrictHostKeyChecking=yes',
            '-o', 'UserKnownHostsFile=' + config['recovery_known_hosts'], '-i', config['recovery_ssh_identity'],
            '-p', str(target.get('port', 22)), target.get('user', 'root') + '@' + target['hostname']]


def find_free_port(config):
    used = set()
    for host in config['coordinator_hosts']:
        proc = subprocess.run(ssh_prefix(config, host) + ['ss', '-H', '-ltn'], capture_output=True, text=True, timeout=15)
        if proc.returncode:
            raise RuntimeError('cannot inspect coordinator port on ' + host)
        for line in proc.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 4:
                try:
                    used.add(int(parts[3].rsplit(':', 1)[1]))
                except (ValueError, IndexError):
                    pass
    for port in range(18800, 19000):
        if port not in used:
            return port
    raise RuntimeError('no free coordinator port in 18800-18999')


def prepare_round(config, counts, number, port):
    parent = Path(config['run_dir'])
    final = parent.parent / (parent.name + '_links_r' + str(number))
    if final.exists():
        return final
    temp = Path(tempfile.mkdtemp(prefix=final.name + '-preparing-', dir=parent.parent))
    try:
        child = dict(config)
        child['tasks'] = [dict(t, link_exclusions_file=str(parent / 'followup-inputs' / (t['id'] + '.jsonl')))
                          for t in config['tasks'] if counts[t['id']]]
        child['run_dir'] = str(final)
        child['port'] = port
        child['endpoint'] = config['endpoint'].rsplit(':', 1)[0] + ':' + str(port)
        child['coordinator_endpoints'] = {h: url.rsplit(':', 1)[0] + ':' + str(port)
                                          for h, url in config['coordinator_endpoints'].items()}
        child['local_root'] = '/var/tmp/ks-cloud/' + final.name
        child['queue_db'] = child['local_root'] + '/queue.sqlite'
        child['update_rule'] = 1
        with os.fdopen(os.open(temp / 'config.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as out:
            json.dump(child, out, ensure_ascii=False, indent=2)
        with os.fdopen(os.open(temp / 'token', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as out:
            out.write(secrets.token_hex(32))
        os.rename(temp, final)
    finally:
        if temp.exists():
            shutil.rmtree(temp)
    return final


def bootstrap_round(run):
    config_path = str(run / 'config.json')
    config = json.loads(Path(config_path).read_text())
    config['recovery_config_path'] = config_path
    def boot(host):
        try:
            proc = subprocess.run(peer_command(config, host), capture_output=True, text=True, timeout=25)
            return host, proc.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            return host, False
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = dict(pool.map(boot, config['hosts']))
    if all(results.values()):
        (run / 'START').touch(exist_ok=True)
    return [host for host, ok in results.items() if not ok]


def main_loop(config_path, interval=30):
    config = json.loads(Path(config_path).read_text())
    if config.get('update_rule') != 1:
        raise ValueError('follow-up requires presence-only update rule')
    base = Path(config['run_dir'])
    state_path = base / 'followup-state.json'
    lock_path = base / 'followup.lock'
    while True:
        try:
            with lock_path.open('a') as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    time.sleep(interval)
                    continue
                drive(config, state_path, interval)
                return
        except Exception as error:
            print(type(error).__name__, str(error), flush=True)
            time.sleep(interval)


def drive(config, state_path, interval):
    base = Path(config['run_dir'])
    terminal_seen = {}
    while True:
        try:
            primary, reason = terminal_status(base / 'status.json', [t['id'] for t in config['tasks']])
            if not primary:
                save(state_path, dict(updated=time.time(), phase='waiting_primary', reason=reason))
                time.sleep(interval)
                continue
            # Let the scanner close its exclusion audit after recording its final state.
            first = terminal_seen.setdefault(str(base), time.monotonic())
            if time.monotonic() - first < 60:
                time.sleep(interval)
                continue
            counts = snapshot_link_inputs(config)
            if not any(counts.values()):
                save(state_path, dict(updated=time.time(), phase='finished', link_counts=counts, rounds=0))
                return
            number = 1
            while True:
                run = base.parent / (base.name + '_links_r' + str(number))
                if not run.exists():
                    run = prepare_round(config, counts, number, find_free_port(config))
                if not (run / 'START').exists():
                    missing = bootstrap_round(run)
                    if missing:
                        save(state_path, dict(updated=time.time(), phase='starting', round=number, missing_hosts=missing))
                        break
                round_config = json.loads((run / 'config.json').read_text())
                status, reason = terminal_status(run / 'status.json', [t['id'] for t in round_config['tasks']])
                if not status:
                    save(state_path, dict(updated=time.time(), phase='checking_links', round=number, run_dir=str(run), reason=reason))
                    break
                first = terminal_seen.setdefault(str(run), time.monotonic())
                if time.monotonic() - first < 60:
                    break
                uploaded = status.get('counters', {}).get('succeeded', 0)
                if not uploaded:
                    save(state_path, dict(updated=time.time(), phase='finished', round=number, run_dir=str(run),
                                          link_counts=counts, excluded=[s for s in status['scans'] if s.get('error')]))
                    return
                number += 1
        except Exception as error:
            save(state_path, dict(updated=time.time(), phase='error_retrying', error=type(error).__name__ + ': ' + str(error)))
            print(type(error).__name__, str(error), flush=True)
        time.sleep(interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    main_loop(args.config)
