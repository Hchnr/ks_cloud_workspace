"""Low-frequency SwanLab reporting for a separate verified deletion run."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import secrets
import time

from .common import save
from .telemetry import configure_connection


def metrics(snapshot, config, previous=None):
    counters = snapshot.get('counters', {})
    total = config['verified_total_files']
    complete = snapshot['completed_files']
    result = {
        'deletion/verified_total_files': total,
        'deletion/processed_files': complete,
        'deletion/remaining_files': max(0, total - complete),
        'deletion/progress_pct': 100 * complete / max(1, total),
        'deletion/removed_confirmed_files': counters.get('succeeded', 0),
        'deletion/already_absent_files': counters.get('skipped', 0),
        'deletion/processed_logical_TiB': snapshot['completed_bytes'] / 1024**4,
        'deletion/confirmed_removed_logical_TiB': counters.get('succeeded_bytes', 0) / 1024**4,
        'deletion/failed_chunks': sum(g['chunks'] for g in snapshot['groups'] if g['state'] == 'failed'),
        'deletion/pending_chunks': sum(g['chunks'] for g in snapshot['groups'] if g['state'] == 'pending'),
        'deletion/import_complete': int(snapshot['scan_complete']),
        'deletion/terminal': int(snapshot['state'].startswith('finished')),
        'cluster/healthy_nodes': sum(bool(n.get('healthy')) for n in snapshot['nodes'].values()),
        'cluster/active_slots': len(snapshot['active']),
        'cluster/leased_chunks': snapshot.get('leased_chunks', len(snapshot['active'])),
    }
    complete_by_host = {n['owner']: n for n in snapshot.get('completed_by_node', [])}
    for host, node in snapshot['nodes'].items():
        prefix = 'nodes/' + host + '/'
        for key in ('active_slots', 'healthy', 'memory_used_pct', 'memory_current_GiB',
                    'memory_anon_GiB', 'oom_kill_events', 'io_pressure_some_avg10'):
            result[prefix + key] = float(node.get(key, 0))
        result[prefix + 'processed_files'] = complete_by_host.get(host, {}).get('files', 0)
    if previous and snapshot['updated'] > previous['updated']:
        dt = snapshot['updated'] - previous['updated']
        result['deletion/processed_files_per_second'] = max(0, complete - previous['completed_files']) / dt
    return result


def run(config):
    base = Path(config['run_dir'])
    with (base / 'swanlab.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        configure_connection(config)
        import swanlab
        swanlab.merge_settings(swanlab.Settings(interactive=False, probe=swanlab.Settings.Probe(
            hardware=False, runtime=False, requirements=False, conda=False, git=False, swanlab=False, monitor=False)))
        state_path = base / 'swanlab.json'
        state = json.loads(state_path.read_text()) if state_path.exists() else {}
        if not state.get('run_id'):
            state['run_id'] = secrets.token_hex(4)
            save(state_path, state)
        swanlab.login(api_key=os.environ['SWANLAB_API_KEY'], save=False, timeout=15)
        experiment = swanlab.init(project=config['swanlab_project'], name=base.name, id=state['run_id'],
                                  resume='allow', public=False, log_dir=str(base / 'swanlog'),
                                  config=dict(hosts=config['hosts'], verified_total_files=config['verified_total_files'],
                                              publish_seconds=max(120, config.get('swanlab_interval', 120)),
                                              semantics='processed includes already absent on retry; bytes are logical, not reclaimed'))
        state.update(url=experiment.url, run_path=experiment.path, pid=os.getpid())
        save(state_path, state)
        if hasattr(experiment, 'define_metric'):
            experiment.define_metric('deletion/*', section_name='00-清理进度')
        interval = max(120, config.get('swanlab_interval', 120))
        time.sleep(max(0, state.get('last_enqueued', 0) + interval - time.time()))
        previous, errors, last_readback = None, 0, 0
        while True:
            try:
                snap = json.loads((base / 'status.json').read_text())
                now = time.time()
                age = max(0, now - snap['updated'])
                m = {'deletion/snapshot_age_seconds': age, 'deletion/snapshot_stale': int(age > 120)}
                if age <= 120 or snap['state'].startswith('finished'):
                    m.update(metrics(snap, config, previous))
                step = max(int(now), state.get('last_step', 0) + 1)
                swanlab.log(m, step=step)
                state.update(last_step=step, last_enqueued=now, snapshot_updated=snap['updated'], errors=0)
                if now - last_readback >= 240:
                    last_readback = now
                    try:
                        summary = swanlab.Api().run(experiment.path.strip('/')).summary(keys=['deletion/processed_files'])
                        if isinstance(summary, dict) and summary.get('deletion/processed_files') is not None:
                            state.update(server_confirmed_at=now, server_summary=summary)
                    except Exception as error:
                        state.update(readback_error=type(error).__name__, readback_error_at=now)
                save(state_path, state)
                previous, errors = snap, 0
                if snap['state'].startswith('finished'):
                    swanlab.finish()
                    state['finished'] = time.time()
                    save(state_path, state)
                    return
            except Exception as error:
                errors += 1
                state.update(errors=errors, last_error=type(error).__name__, last_error_at=time.time())
                save(state_path, state)
            time.sleep(min(1800, interval * 2**min(errors, 4)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    run(json.loads(Path(args.config).read_text()))
