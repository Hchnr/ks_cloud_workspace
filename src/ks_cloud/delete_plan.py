"""Prepare an unarmed cleanup run from a completed path-presence check."""
import json
import os
from pathlib import Path
import secrets
import shutil
import sqlite3
import stat
import tempfile

from .common import successful
from .presence_followup import terminal_status


def _completed_status(run, sources):
    status, reason = terminal_status(run / 'status.json', sources)
    if status is None:
        raise ValueError('verification not complete: ' + reason)
    if status.get('completed_files') != status.get('discovered_files'):
        raise ValueError('verification file totals differ')
    counters = status.get('counters', {})
    if counters.get('succeeded', 0) + counters.get('skipped', 0) != status.get('completed_files'):
        raise ValueError('verification result counters differ')
    return status


def _check_followup(primary, primary_config):
    cycle = json.loads((primary / 'followup-state.json').read_text())
    if cycle.get('phase') != 'finished':
        raise ValueError('directory-link follow-up is incomplete')
    counts = cycle.get('link_counts')
    primary_tasks = {task['id']: task for task in primary_config['tasks']}
    if not isinstance(counts, dict) or set(counts) != set(primary_tasks) or any(type(n) is not int or n < 0 for n in counts.values()):
        raise ValueError('link input counts are unavailable')
    stored = json.loads((primary / 'followup-inputs' / 'counts.json').read_text())
    if counts != stored:
        raise ValueError('directory-link input counts differ')
    if any(counts.values()):
        number = cycle.get('round')
        if not isinstance(number, int) or number < 1:
            raise ValueError('directory-link final round is unavailable')
        run = primary.parent / (primary.name + '_links_r' + str(number))
        if str(run) != cycle.get('run_dir'):
            raise ValueError('unexpected directory-link run')
        config = json.loads((run / 'config.json').read_text())
        expected_sources = {source for source, count in counts.items() if count}
        if config.get('update_rule') != 1 or {t['id'] for t in config['tasks']} != expected_sources:
            raise ValueError('directory-link run has unexpected scope')
        for task in config['tasks']:
            original = primary_tasks[task['id']]
            if any(task.get(k) != original.get(k) for k in ('source', 'destination', 'exclude_child')):
                raise ValueError('directory-link mapping differs from primary')
            expected_ledger = primary / 'followup-inputs' / (task['id'] + '.jsonl')
            if task.get('link_exclusions_file') != str(expected_ledger):
                raise ValueError('directory-link input differs from primary ledger')
        status = _completed_status(run, expected_sources)
        if status.get('counters', {}).get('succeeded', 0):
            raise ValueError('last directory-link round uploaded new files')
    return cycle


def _validate_roots(config, baseline, protected):
    tasks = config['tasks']
    if len({t['id'] for t in tasks}) != len(tasks) or len({t['source'] for t in tasks}) != len(tasks):
        raise ValueError('duplicate source id or root')
    if set(baseline['roots']) != {t['id'] for t in tasks} or set(baseline['hosts']) != set(config['hosts']):
        raise ValueError('root identity baseline does not cover the run')
    roots = {}
    for task in tasks:
        root = task['source']
        if not os.path.isabs(root) or os.path.normpath(root) != root or root == '/':
            raise ValueError('unsafe source root')
        info = os.lstat(root)
        if not stat.S_ISDIR(info.st_mode) or os.path.realpath(root) != root:
            raise ValueError('source root is not a physical directory: ' + task['id'])
        if baseline['roots'][task['id']] != {'ino': info.st_ino, 'real': root}:
            raise ValueError('source root identity changed: ' + task['id'])
        roots[task['id']] = info.st_ino
        for path in protected:
            absolute = os.path.realpath(path)
            if os.path.commonpath((root, absolute)) == root:
                raise ValueError('runtime or credential path is inside a source root')
    for parent in tasks:
        for child in tasks:
            if parent is child:
                continue
            if os.path.commonpath((parent['source'], child['source'])) == parent['source']:
                expected = os.path.join(parent['source'], parent.get('exclude_child', ''))
                if not parent.get('exclude_child') or child['source'] != expected:
                    raise ValueError('nested roots lack a matching excluded child')
    return roots


def _snapshot_queue(primary, destination, status):
    source = primary / 'queue.sqlite'
    with sqlite3.connect(source.resolve().as_uri() + '?mode=ro&immutable=1', uri=True) as db:
        with sqlite3.connect(destination) as copied:
            db.backup(copied)
    with sqlite3.connect(destination) as db:
        if db.execute('PRAGMA quick_check').fetchone() != ('ok',):
            raise ValueError('verified queue failed integrity check')
        groups = {(r[0], r[1]): (r[2], r[3], r[4]) for r in db.execute(
            'SELECT source,state,count(*),sum(files),sum(bytes) FROM tasks GROUP BY source,state')}
        expected_groups = {(r['source'], r['state']): (r['chunks'], r['files'], r['bytes']) for r in status['groups']}
        if groups != expected_groups or any(state != 'done' for _, state in groups):
            raise ValueError('verified queue snapshot does not match completed status')
        scans = {(r[0], r[1], r[2]) for r in db.execute('SELECT source,state,error FROM scans')}
        if scans != {(r['source'], r['state'], r['error']) for r in status['scans']}:
            raise ValueError('verified queue scan records do not match status')
        succeeded = skipped = 0
        for files, result in db.execute('SELECT files,result FROM tasks'):
            parsed = json.loads(result)
            if not successful(parsed) or parsed.get('handled') != files:
                raise ValueError('verified queue contains an incomplete task result')
            succeeded += parsed.get('succeeded', 0)
            skipped += parsed.get('skipped', 0)
        if (succeeded, skipped) != (status['counters'].get('succeeded', 0), status['counters'].get('skipped', 0)):
            raise ValueError('verified queue counters do not match status')


def prepare(primary, release, port):
    """Create a fully unarmed run; never starts workers or creates a delete gate."""
    primary, release = Path(primary).resolve(), Path(release).resolve()
    config = json.loads((primary / 'config.json').read_text())
    if config.get('update_rule') != 1 or not 1024 <= port <= 65535:
        raise ValueError('expected presence-only verification and valid port')
    mapping = [{key: task.get(key) for key in ('id', 'source', 'destination', 'exclude_child')} for task in config['tasks']]
    if mapping != json.loads((primary / 'mapping-baseline.json').read_text())['tasks']:
        raise ValueError('verification mappings changed from baseline')
    status = _completed_status(primary, [t['id'] for t in config['tasks']])
    cycle = _check_followup(primary, config)
    baseline = json.loads((primary / 'root-baseline.json').read_text())
    final = primary.parent / (primary.name + '_delete')
    if final.exists():
        raise FileExistsError(str(final))
    roots = _validate_roots(config, baseline, [primary, final, release, config['tool'], config['config_file']])
    required = ('dynamic.py', 'verified_delete.py', 'delete_telemetry.py', 'supervisor.py', 'ha.py', 'bootstrap.py')
    if any(not (release / name).is_file() for name in required):
        raise ValueError('tested deletion release is incomplete')
    temp = Path(tempfile.mkdtemp(prefix=final.name + '-preparing-', dir=primary.parent))
    try:
        _snapshot_queue(primary, temp / 'verified.sqlite', status)
        shutil.copytree(release, temp / 'release' / 'ks_cloud')
        child = dict(config)
        child.update(operation='delete_verified', run_dir=str(final), port=port,
                     local_root='/var/tmp/ks-cloud/' + final.name,
                     queue_db='/var/tmp/ks-cloud/' + final.name + '/queue.sqlite',
                     verified_db=str(final / 'verified.sqlite'), verification_run=str(primary),
                     verified_total_files=status['completed_files'], verified_total_bytes=status['completed_bytes'],
                     recovery_code_dir=str(final / 'release' / 'ks_cloud'), delete_nonce=secrets.token_hex(32),
                     delete_workers=4, stall_seconds=7200, update_rule=1)
        child['endpoint'] = config['endpoint'].rsplit(':', 1)[0] + ':' + str(port)
        child['coordinator_endpoints'] = {host: endpoint.rsplit(':', 1)[0] + ':' + str(port)
                                          for host, endpoint in config['coordinator_endpoints'].items()}
        child['tasks'] = [dict(task, root_ino=roots[task['id']]) for task in config['tasks']]
        with os.fdopen(os.open(temp / 'config.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as out:
            json.dump(child, out, ensure_ascii=False, indent=2)
        with os.fdopen(os.open(temp / 'token', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as out:
            out.write(secrets.token_hex(32))
        (temp / 'plan.json').write_text(json.dumps(dict(verification_run=str(primary),
            verified_files=status['completed_files'], verified_bytes=status['completed_bytes'],
            primary_exclusions=[s for s in status['scans'] if s.get('error')],
            link_exclusions=cycle.get('excluded', []), phase='unarmed'), ensure_ascii=False, indent=2))
        os.rename(temp, final)
    finally:
        if temp.exists():
            shutil.rmtree(temp)
    return final
