"""One low-frequency SwanLab publisher, isolated from transfer workers."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import time

from .common import save
from .overview import build_overview, render_guide, render_overview, snapshot_freshness


def metrics_for_snapshot(snapshot, config, metrics, now):
    freshness = snapshot_freshness(snapshot, config, now)
    if freshness['overview/snapshot_stale'] and not snapshot['state'].startswith('finished'):
        # Do not timestamp old health/progress as a fresh observation. HTML keeps
        # the last known values, with the same explicit stale-snapshot warning.
        metrics = {key: metrics[key] for key in ['overview/dashboard', 'overview/metric_guide']
                   if key in metrics}
    return dict(metrics, **freshness)


def scan_metrics(snapshot):
    control = snapshot.get('scan_control')
    if not control:
        return {}
    result = {'scanning/control_revision':control['revision'],
              'scanning/enabled':int(control['enabled']),
              'scanning/config_stale':int(control['config_stale']),
              'scanning/config_age_seconds':control['config_age_seconds']}
    for source, values in control['sources'].items():
        for key in ['workers','files_s','upload_files_s','metadata_ms','operations','errors','fallbacks','cooldown_seconds']:
            result['scanning/'+source+'/'+key] = values[key]
    return result


def configure_connection(config):
    """Load private connection settings without placing credentials in run config."""
    legacy = Path('/share-mixed/project/eai_pwm/.secrets')
    key_file = config.get('swanlab_key_file')
    if key_file:
        api_key = Path(key_file).read_text().strip()
    else:
        api_key = os.environ.get('SWANLAB_API_KEY', '').strip()
        if not api_key:
            api_key = (legacy / 'swanlab_api_key').read_text().strip()
    if not api_key:
        raise ValueError('SwanLab API key is empty')
    proxy_file = config.get('swanlab_proxy_file')
    if proxy_file is None and (legacy / 'swanlab_proxy').exists():
        proxy_file = str(legacy / 'swanlab_proxy')
    if proxy_file is not None:
        proxy = Path(proxy_file).read_text().strip() if proxy_file else ''
        for key in ['https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY', 'ALL_PROXY', 'all_proxy']:
            os.environ.pop(key, None)
        if proxy:
            for key in ['https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY']:
                os.environ[key] = proxy
    os.environ['SWANLAB_API_KEY'] = api_key
    return api_key


def run(config):
    base = Path(config['run_dir'])
    lock = (base / 'swanlab.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    configure_connection(config)
    import swanlab
    swanlab.merge_settings(swanlab.Settings(interactive=False, probe=swanlab.Settings.Probe(
        hardware=False, runtime=False, requirements=False, conda=False, git=False, swanlab=False, monitor=False)))
    state_file = base / 'swanlab.json'
    state = json.loads(state_file.read_text()) if state_file.exists() else {}
    # Persist a client-generated ID before init, so interrupted initialization cannot multiply runs.
    if not state.get('run_id'):
        import secrets
        state['run_id'] = secrets.token_hex(4)
        save(state_file, state)
    swanlab.login(api_key=os.environ['SWANLAB_API_KEY'], save=False, timeout=15)
    run = swanlab.init(project=config['swanlab_project'], name=base.name,
                       id=state['run_id'], resume='allow', public=False,
                       log_dir=str(base / 'swanlog'),
                       config=dict(hosts=config['hosts'], slots=config['slots'], jobs=config['jobs'],
                                   tasks=config['tasks'], publish_seconds=config['swanlab_interval'],
                                   progress_semantics='completed manifest bytes include skips; scan totals grow'))
    state.update(url=run.url, run_path=run.path, pid=os.getpid())
    save(state_file, state)
    inventory_path = config.get('overview_inventory_file')
    inventory = json.loads(Path(inventory_path).read_text()) if inventory_path else {}
    # Leave the SDK axis unspecified: explicit _relative_time definitions create
    # an invalid cloud descriptor. Existing view/chart presentation independently
    # uses relative_time with type=SYSTEM, class=SCALAR; epoch-second steps persist.
    if hasattr(run, 'define_metric'):
        run.define_metric('overview/*', section_name='00-总进度')
    interval = max(120, config.get('swanlab_interval', 120))
    time.sleep(max(0, state.get('last_enqueued', 0)+interval-time.time()))
    previous = None
    errors = 0
    last_confirm = 0
    while True:
        try:
            s = json.loads((base / 'status.json').read_text())
            now = time.time()
            step = max(int(now), state.get('last_step', 0)+1)
            counters = s['counters']
            m = dict(
                **{'progress/completed_files': s['completed_files'],
                   'progress/completed_TiB': s['completed_bytes']/1024**4,
                   'progress/discovered_files': s['discovered_files'],
                   'progress/discovered_TiB': s['discovered_bytes']/1024**4,
                   'progress/scan_complete': int(s['scan_complete']),
                   'progress/coverage_of_discovered_pct': 100*s['completed_files']/max(1,s['discovered_files']),
                   'progress/uploaded_files_confirmed': counters.get('succeeded',0),
                   'progress/uploaded_TiB_confirmed': counters.get('succeeded_bytes',0)/1024**4,
                   'progress/skipped_files': counters.get('skipped',0),
                   'cluster/healthy_nodes': sum(n['healthy'] for n in s['nodes'].values()),
                   'cluster/active_slots': len(s['active']),
                   'cluster/leased_chunks': s.get('leased_chunks',len(s['active'])),
                   'cluster/failed_chunks': sum(g['chunks'] for g in s['groups'] if g['state']=='failed'),
                   'cluster/pending_chunks': sum(g['chunks'] for g in s['groups'] if g['state']=='pending'),
                   'cluster/scan_errors_or_exclusions': sum(bool(x['error']) for x in s['scans']),
                   'cluster/terminal': int(s['state'].startswith('finished'))})
            m.update(scan_metrics(s))
            completed = {n['owner']: n for n in s['completed_by_node']}
            node_slots = []
            for host, n in s['nodes'].items():
                prefix = 'nodes/'+host+'/'
                for key in ['active_slots','load1','memory_used_pct','healthy','cpus',
                            'memory_limit_GiB','memory_current_GiB','memory_anon_GiB','memory_file_cache_GiB',
                            'memory_total_pct','oom_events','oom_kill_events','cpu_throttled_seconds']:
                    m[prefix+key] = float(n.get(key,0))
                node_slots.append(n['active_slots'])
                m[prefix+'completed_files'] = completed.get(host,{}).get('files',0)
                m[prefix+'completed_GiB'] = completed.get(host,{}).get('bytes',0)/1024**3
                if previous and host in previous['nodes']:
                    dt = max(1,s['updated']-previous['updated'])
                    old = previous['nodes'][host]
                    # Interface counters measure all traffic in this node's network namespace.
                    m[prefix+'network_tx_MiB_s'] = max(0,n.get('net_tx_bytes',0)-old.get('net_tx_bytes',0))/dt/1024**2
                    m[prefix+'network_rx_MiB_s'] = max(0,n.get('net_rx_bytes',0)-old.get('net_rx_bytes',0))/dt/1024**2
                    if 'cpu_usage_seconds' in n and 'cpu_usage_seconds' in old:
                        m[prefix+'cpu_used_cores'] = max(0,n['cpu_usage_seconds']-old['cpu_usage_seconds'])/dt
            m['cluster/slot_imbalance'] = max(node_slots,default=0)-min(node_slots,default=0)
            if config.get('ha_enabled'):
                m['recovery/status_stale']=int(s.get('recovery_status_stale',False))
                if s.get('recovery_status_age_seconds') is not None:
                    m['recovery/status_age_seconds']=s['recovery_status_age_seconds']+max(0,now-s['updated'])
            recovery=s.get('recovery')
            if recovery:
                leader=recovery['leader']
                m['recovery/leader_index']=config['hosts'].index(leader['host']) if leader.get('host') in config['hosts'] else -1
                m['recovery/generation']=leader.get('generation',0)
                m['recovery/leader_heartbeat_age_seconds']=max(0,now-leader.get('updated',now))
                m['recovery/guardians_alive']=sum(g['alive'] for g in recovery['guardians'].values())
                m['recovery/queue_snapshot_age_seconds']=recovery['snapshot_age_seconds']
                m['recovery/claim_grace_seconds']=max(0,leader.get('claims_after',0)-now)
            if previous:
                dt = max(1,s['updated']-previous['updated'])
                m['throughput/completed_files_s'] = max(0,s['completed_files']-previous['completed_files'])/dt
                m['throughput/completed_MiB_s_including_skips'] = max(0,s['completed_bytes']-previous['completed_bytes'])/dt/1024**2
                m['throughput/uploaded_MiB_s_confirmed'] = max(0,counters.get('succeeded_bytes',0)-previous['counters'].get('succeeded_bytes',0))/dt/1024**2
            try:
                overview = build_overview(s, config, inventory, now=now)
                html = render_overview(overview)
                m.update(overview['metrics'])
                m['overview/dashboard'] = swanlab.Html(html, caption='总进度：历史参考或本轮扫描总量，详见看板说明')
                temp = base/'overview.html.tmp'
                temp.write_text(html)
                os.replace(temp, base/'overview.html')
                if previous is None:
                    guide = render_guide()
                    m['overview/metric_guide'] = swanlab.Html(guide, caption='全部图表逐项说明；节点同名指标适用于所有 16 个节点')
                    (base/'metric_guide.html').write_text(guide)
                state.pop('overview_error', None)
            except Exception as e:
                # A presentation failure must not prevent scalar monitoring.
                state['overview_error'] = type(e).__name__+': '+str(e)
            m = metrics_for_snapshot(s, config, m, now)
            swanlab.log(m, step=step)
            if 'overview/dashboard' in m:
                state['overview_last_enqueued'] = now
                state['overview_last_step'] = step
            state.update(last_step=step, last_enqueued=now, metrics_count=len(m), errors=0,
                         snapshot_updated=s['updated'],
                         snapshot_age_seconds=m['overview/snapshot_age_seconds'],
                         snapshot_stale=bool(m['overview/snapshot_stale']))
            errors = 0
            previous = s
            # Infrequent readback distinguishes SDK enqueue from confirmed cloud ingestion.
            if now-last_confirm >= 600:
                summary = swanlab.Api().run(run.path.strip('/')).summary(keys=['progress/completed_files','cluster/healthy_nodes','overview/coverage_bytes_pct','overview/total_is_exact'])
                state['server_summary'] = summary
                state['server_checked_at'] = now
                last_confirm = now
            save(state_file, state)
            if s['state'].startswith('finished'):
                swanlab.finish()
                state['finished'] = time.time()
                save(state_file, state)
                return
        except Exception as e:
            errors += 1
            state.update(errors=errors, last_error=type(e).__name__, last_error_at=time.time())
            save(state_file, state)
        time.sleep(min(1800, interval*2**min(errors,4)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    run(json.loads(Path(args.config).read_text()))
