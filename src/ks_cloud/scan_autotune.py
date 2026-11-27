"""Conservative online adjustment of the shared scan-pool concurrency."""
from collections import deque
import fcntl
import json
import os
from pathlib import Path
import tempfile
import time


class ScanAutotuner:
    """Probe 4→8 only on sustained demand; reduce concurrency on pressure."""
    def __init__(self, initial, ceiling=8, floor=2):
        self.expected = initial
        self.ceiling = ceiling
        self.floor = floor
        self.previous_enabled = False
        self.previous_oom = None
        self.previous_work = None
        self.last_change = 0
        self.baseline = None
        self.probe_started = 0
        self.samples = deque()
        self.low_windows = 0
        self.incident = initial <= 4  # A previous 8→4 reduction is already accounted for.

    def observe(self, status, configured):
        if configured != self.expected:
            return None, 'configuration_changed'
        p = status['parallel_scan']
        now = status['updated']
        if p['max_nodes'] != configured:
            return None, 'waiting_for_reload'
        pressure = status['scan_control']['pressure']
        oom = sum(n.get('oom_kill_events', 0) for n in status['nodes'].values())
        new_oom = self.previous_oom is not None and oom > self.previous_oom
        self.previous_oom = oom
        overloaded = (pressure.get('io_pressure_some_avg10', 0) >= 65
                      or pressure.get('memory_used_pct', 0) >= 65 or new_oom)
        cooldown = not p['enabled'] and p.get('cooldown_seconds', 0) > 0
        if overloaded or (cooldown and (self.previous_enabled or not self.incident)):
            self.samples.clear()
            self.low_windows = 0
            self.baseline = None
            self.probe_started = 0
            self.previous_enabled = False
            if not self.incident and configured > self.floor:
                self.incident = True
                self.expected = max(self.floor, configured//2)
                self.last_change = now
                return self.expected, 'resource_pressure_or_cooldown'
            self.incident = True
            return None, 'cooling_down'
        if not p['enabled']:
            self.previous_enabled = False
            self.samples.clear()
            return None, 'pool_disabled'
        self.previous_enabled = True
        self.incident = False
        safe = (pressure.get('io_pressure_some_avg10', 0) < 35
                and pressure.get('memory_used_pct', 0) < 50
                and all(n.get('healthy') for n in status['nodes'].values()))
        work = sum(n.get('files', 0) for n in p.get('nodes', {}).values())
        work_advanced = self.previous_work is not None and work > self.previous_work
        self.previous_work = work
        demand = (p['active_nodes'] >= min(3, configured) or p['queued'] >= configured
                  or (p.get('buffered', 0) >= configured and work_advanced))
        if not safe:
            self.samples.clear()
            return None, 'waiting_for_low_pressure'
        self.samples.append((now, status['completed_files'], demand))
        while len(self.samples)>2 and now-self.samples[1][0]>=120:
            self.samples.popleft()
        elapsed = now-self.samples[0][0]
        if elapsed < 120:
            return None, 'collecting'
        rate = max(0, status['completed_files']-self.samples[0][1])/elapsed
        enough_demand = sum(int(x[2]) for x in self.samples) >= 2
        if self.baseline is not None:
            if enough_demand and rate < self.baseline*.75:
                self.low_windows += 1
            else:
                self.low_windows = 0
            if self.low_windows >= 2:
                self.expected = max(self.floor, configured//2)
                self.last_change = now
                self.baseline = None
                self.probe_started = 0
                self.samples.clear()
                self.low_windows = 0
                return self.expected, 'sustained_throughput_regression'
            if now-self.probe_started >= 300:
                self.baseline = None
                self.probe_started = 0
                self.samples.clear()
                return None, 'probe_stable'
            return None, 'probing'
        if (configured < self.ceiling and enough_demand and rate >= 500
                and now-self.last_change >= 300):
            self.baseline = rate
            self.probe_started = now
            self.expected = min(self.ceiling, configured*2)
            self.last_change = now
            self.samples.clear()
            return self.expected, 'sustained_demand_probe'
        return None, 'holding'


def save(path, value):
    fd, name = tempfile.mkstemp(prefix=path.name+'.', dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w') as out:
            json.dump(value, out, ensure_ascii=False, indent=2)
            out.write('\n')
            out.flush()
            os.fsync(out.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def run(root):
    root = Path(root)
    lock = (root/'monitoring'/'automatic-scan-tuning.lock').open('a+')
    while True:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            time.sleep(10)
    lock.seek(0)
    if lock.read().strip():
        return

    def mark_stop(reason):
        # The lock inode is shared across candidates. Persist the terminal
        # marker before releasing it so a standby cannot undo an operator's
        # tuning change while its report-file cache is stale.
        lock.seek(0)
        lock.truncate()
        lock.write(reason+'\n')
        lock.flush()
        os.fsync(lock.fileno())

    tuning_path = root/'parallel-scan-tuning.json'
    report_path = root/'monitoring'/'automatic-scan-tuning.json'
    if report_path.exists():
        previous = json.loads(report_path.read_text())
        if previous.get('phase') in ('configuration_changed', 'disabled', 'finished'):
            mark_stop(previous['phase'])
            return
    tuning = json.loads(tuning_path.read_text())
    controller = ScanAutotuner(tuning['max_nodes'])
    last_status = 0
    run_dir = root
    while True:
        try:
            global_state = json.loads((root/'global-followup-state.json').read_text())
            if global_state.get('phase', '').startswith('finished'):
                mark_stop('finished')
                save(report_path, dict(updated=time.time(), phase='finished', max_nodes=controller.expected))
                return
            candidate = global_state.get('run_dir')
            if candidate and Path(candidate).is_dir():
                candidate = Path(candidate)
                if candidate != run_dir:
                    controller.samples.clear()
                    controller.baseline = None
                    controller.probe_started = 0
                    controller.low_windows = 0
                    controller.previous_work = None
                    last_status = 0
                    run_dir = candidate
            status = json.loads((run_dir/'status.json').read_text())
            tuning = json.loads(tuning_path.read_text())
            if not tuning.get('enabled'):
                mark_stop('disabled')
                save(report_path, dict(updated=time.time(), phase='disabled'))
                return
            if status.get('state') != 'running' or not status.get('parallel_scan'):
                time.sleep(10)
                continue
            if time.time()-status['updated'] > 90 or status['updated'] == last_status:
                time.sleep(5)
                continue
            last_status = status['updated']
            target, reason = controller.observe(status, tuning['max_nodes'])
            if reason == 'configuration_changed':
                mark_stop(reason)
                save(report_path, dict(updated=time.time(), phase=reason, max_nodes=tuning['max_nodes']))
                return
            if target is not None:
                # Preserve all other online controls; do not overwrite a manual change.
                current = json.loads(tuning_path.read_text())
                if current.get('max_nodes') != tuning['max_nodes'] or not current.get('enabled'):
                    mark_stop('configuration_changed')
                    save(report_path, dict(updated=time.time(), phase='configuration_changed'))
                    return
                current['max_nodes'] = target
                save(tuning_path, current)
            save(report_path, dict(updated=time.time(), phase=reason, max_nodes=controller.expected,
                                   run_dir=str(run_dir), active_nodes=status['parallel_scan']['active_nodes'],
                                   io_pressure=status['scan_control']['pressure'].get('io_pressure_some_avg10')))
        except (OSError, ValueError, KeyError, TypeError) as error:
            print(type(error).__name__+': '+str(error), flush=True)
        time.sleep(5)


if __name__ == '__main__':
    import sys
    run(sys.argv[1])
