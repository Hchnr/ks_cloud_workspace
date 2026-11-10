"""Repeat complete presence checks until an entire round uploads nothing."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import secrets
import shutil
import tempfile
import time

from .common import save
from .presence_followup import bootstrap_round, find_free_port, terminal_status


def prepare_full_round(config, number, port):
    root = Path(config['run_dir'])
    final = root.parent / (root.name + '_full_r' + str(number))
    if final.exists():
        return final
    temp = Path(tempfile.mkdtemp(prefix=final.name + '-preparing-', dir=root.parent))
    try:
        child = dict(config)
        child['tasks'] = [{k: v for k, v in task.items() if k != 'link_exclusions_file'}
                          for task in config['tasks']]
        child.update(run_dir=str(final), port=port, update_rule=1,
                     local_root='/var/tmp/ks-cloud/' + final.name,
                     presence_followup_enabled=True, global_followup_enabled=False)
        child['queue_db'] = child['local_root'] + '/queue.sqlite'
        child['endpoint'] = config['endpoint'].rsplit(':', 1)[0] + ':' + str(port)
        child['coordinator_endpoints'] = {host: url.rsplit(':', 1)[0] + ':' + str(port)
                                          for host, url in config['coordinator_endpoints'].items()}
        for name, content in [('config.json', json.dumps(child, ensure_ascii=False, indent=2)),
                              ('token', secrets.token_hex(32))]:
            with os.fdopen(os.open(temp / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as output:
                output.write(content)
        os.rename(temp, final)
    finally:
        if temp.exists():
            shutil.rmtree(temp)
    return final


def completed_round(run, expected_tasks, approved_roots):
    """A round includes its normal scan and every directory-link check."""
    run = Path(run)
    primary, reason = terminal_status(run / 'status.json', [task['id'] for task in expected_tasks])
    if not primary:
        return None, 'primary: ' + reason
    try:
        followup = json.loads((run / 'followup-state.json').read_text())
    except (OSError, ValueError):
        return None, 'link follow-up unavailable'
    if followup.get('phase') != 'finished':
        return None, 'link follow-up: ' + str(followup.get('phase', 'unknown'))
    count = followup.get('round', followup.get('rounds'))
    if type(count) is not int or count < 0:
        raise ValueError('invalid completed link round count')
    statuses = [primary]
    for number in range(1, count + 1):
        link = run.parent / (run.name + '_links_r' + str(number))
        child_config = json.loads((link / 'config.json').read_text())
        by_id = {task['id']: task for task in expected_tasks}
        if child_config.get('update_rule') != 1 or child_config.get('scope_allowed_roots') != approved_roots:
            raise ValueError('link round changed the scope or update rule')
        for task in child_config['tasks']:
            original = by_id.get(task['id'])
            if not original or any(task.get(k) != original.get(k) for k in ('source', 'destination')):
                raise ValueError('link round changed a source or destination')
        expected = [task['id'] for task in child_config['tasks']]
        status, reason = terminal_status(link / 'status.json', expected)
        if not status:
            return None, 'link round ' + str(number) + ': ' + reason
        statuses.append(status)
    uploaded = 0
    exclusions = []
    for index, status in enumerate(statuses):
        counters = status.get('counters', {})
        succeeded, skipped = counters.get('succeeded', 0), counters.get('skipped', 0)
        if (type(succeeded) is not int or type(skipped) is not int or
                min(succeeded, skipped) < 0 or succeeded + skipped != status.get('completed_files')):
            raise ValueError('inconsistent completed-file counters')
        uploaded += succeeded
        for scan in status.get('scans', []):
            if scan.get('error'):
                exclusions.append(dict(stage='primary' if index == 0 else 'link-' + str(index),
                                       source=scan['source'], detail=scan['error']))
    return dict(uploaded=uploaded, link_rounds=count, exclusions=exclusions), ''


def tick(config):
    root = Path(config['run_dir'])
    if (root / 'STOP').exists():
        return dict(updated=time.time(), phase='paused', reason='STOP requested')
    if config.get('update_rule') != 1 or not config.get('scope_allowed_roots'):
        raise ValueError('global follow-up requires scoped presence-only checks')
    if {t['source'] for t in config['tasks']} != set(config['scope_allowed_roots']):
        raise ValueError('global source tasks and approved roots differ')
    summaries = []
    number = 1
    while True:
        run = root if number == 1 else root.parent / (root.name + '_full_r' + str(number))
        if number > 1 and not run.exists():
            run = prepare_full_round(config, number, find_free_port(config))
        if number > 1:
            child = json.loads((run / 'config.json').read_text())
            expected_tasks = [{k: v for k, v in task.items() if k != 'link_exclusions_file'}
                              for task in config['tasks']]
            if (child.get('tasks') != expected_tasks or child.get('scope_allowed_roots') != config['scope_allowed_roots']
                    or child.get('update_rule') != 1 or child.get('run_dir') != str(run)):
                raise ValueError('full round changed the approved scope or mapping')
        if number > 1 and not (run / 'START').exists():
            missing = bootstrap_round(run)
            if missing:
                return dict(updated=time.time(), phase='starting', round=number,
                            run_dir=str(run), missing_hosts=missing, completed_rounds=summaries)
        summary, reason = completed_round(run, config['tasks'], config['scope_allowed_roots'])
        if summary is None:
            return dict(updated=time.time(), phase='checking', round=number,
                        run_dir=str(run), reason=reason, completed_rounds=summaries)
        summary.update(round=number, run_dir=str(run))
        summaries.append(summary)
        if summary['uploaded'] == 0:
            phase = 'finished_with_exclusions' if summary['exclusions'] else 'finished'
            return dict(updated=time.time(), phase=phase, round=number, completed_rounds=summaries)
        number += 1


def run(config_path, interval=30):
    config = json.loads(Path(config_path).read_text())
    base = Path(config['run_dir'])
    with (base / 'global-followup.lock').open('a') as lock:
        while True:
            try:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    time.sleep(interval)
                    continue
                while True:
                    try:
                        state = tick(config)
                    except Exception as error:
                        state = dict(updated=time.time(), phase='error_retrying',
                                     error=type(error).__name__ + ': ' + str(error))
                        print(state['error'], flush=True)
                    save(base / 'global-followup-state.json', state)
                    if state['phase'].startswith('finished'):
                        return
                    time.sleep(interval)
            except Exception as error:
                print(type(error).__name__, str(error), flush=True)
                time.sleep(interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    run(args.config)
