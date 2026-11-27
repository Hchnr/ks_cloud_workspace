"""Bounded, ordered directory-link scans pulled by idle CPU hosts."""
import argparse
from collections import deque
from concurrent.futures import Future, TimeoutError
import json
import os
from pathlib import Path
import queue
import shlex
import subprocess
import sys
import threading
import time

from .link_scan import scan_link
from .scope import UploadScope

MAX_LINKS = 256
MAX_RECORDS = 8192
MAX_RESULT_BYTES = 8 * 1024 * 1024


def validate_links(config, source, links):
    tasks = [t for t in config['tasks'] if t['id'] == source]
    if len(tasks) != 1 or not config.get('scope_allowed_roots'):
        raise ValueError('link scan requires one approved source and an explicit scope')
    root = tasks[0]['source']
    if not isinstance(links, list) or not 0 < len(links) <= MAX_LINKS:
        raise ValueError('invalid link batch size')
    for path in links:
        if (not isinstance(path, str) or os.path.normpath(path) != path
                or not path.startswith(root.rstrip('/') + '/') or '\n' in path or '\r' in path):
            raise ValueError('link is outside its configured source')
    return links


def scan_batch(config, source, links):
    validate_links(config, source, links)
    scope = UploadScope(config['scope_allowed_roots'])
    records = []
    size = 0
    for link in links:
        for record in scan_link(link, scope):
            size += len(json.dumps(record, ensure_ascii=False).encode())
            if len(records) >= MAX_RECORDS or size > MAX_RESULT_BYTES:
                return dict(links=links, overflow=True)
            records.append(record)
    return dict(links=links, records=records)


def checked_records(links, result):
    if result.get('links') != links or result.get('overflow'):
        return None
    records = result.get('records')
    if not isinstance(records, list) or len(records) > MAX_RECORDS:
        raise ValueError('invalid link result')
    for row in records:
        if not isinstance(row, (list, tuple)) or len(row) != 3:
            raise ValueError('invalid link record')
        kind, path, value = row
        if (not isinstance(path, str) or os.path.normpath(path) != path
                or not any(path == link or path.startswith(link + '/') for link in links)):
            raise ValueError('record escaped requested links')
        if kind == 'file':
            if type(value) is not int or value < 0 or '\n' in path or '\r' in path:
                raise ValueError('invalid file record')
        elif kind != 'exclude' or not isinstance(value, str):
            raise ValueError('invalid exclusion record')
    return records


class LinkScanPool:
    def __init__(self, config):
        self.config = config
        self.settings = dict(enabled=False, max_nodes=4, batch_links=128, timeout_seconds=90)
        self.tuning = Path(config['parallel_link_scan_tuning_file'])
        self.lock = threading.RLock()
        self.jobs = queue.Queue()
        self.stop = threading.Event()
        self.active = 0
        self.fallbacks = 0
        self.limit = 4
        self.last_pressure = 0
        self.last_config = 0
        self.last_read = -float('inf')
        self.oom = 0
        self.nodes = {h: dict(active=False, healthy=True, files=0, batches=0,
                              errors=0, blocked_until=0, io=0, memory=0) for h in config['hosts']}
        self._settings()
        for host in self.nodes:
            threading.Thread(target=self._worker, args=(host,), daemon=True).start()

    def _settings(self):
        now = time.monotonic()
        with self.lock:
            if now - self.last_read < 5:
                return
            self.last_read = now
        try:
            x = json.loads(self.tuning.read_text())
            if (not isinstance(x, dict) or type(x.get('enabled')) is not bool or type(x.get('max_nodes')) is not int
                    or not 1 <= x['max_nodes'] <= len(self.nodes)
                    or type(x.get('batch_links', 128)) is not int
                    or not 1 <= x.get('batch_links', 128) <= MAX_LINKS
                    or not 10 <= x.get('timeout_seconds', 90) <= 300):
                raise ValueError('invalid link scan tuning')
            with self.lock:
                self.settings.update(x)
                self.last_config = now
                self.limit = max(1, min(self.limit, x['max_nodes']))
        except (OSError, ValueError, TypeError):
            pass

    def enabled(self):
        return self.settings['enabled'] and time.monotonic() - self.last_config < 120

    def observe(self, nodes, pressure, now):
        self._settings()
        with self.lock:
            new_oom = pressure.get('oom_kill_events', 0) > self.oom
            self.oom = pressure.get('oom_kill_events', 0)
            if (new_oom or pressure.get('io_pressure_some_avg10', 0) >= 65
                    or pressure.get('memory_used_pct', 0) >= 65
                    or pressure.get('memory_pressure_some_avg10', 0) >= 25):
                if now - self.last_pressure >= 60:
                    self.limit = max(1, self.limit // 2)
                self.last_pressure = now
            elif (now - self.last_pressure >= 180
                  and pressure.get('io_pressure_some_avg10', 0) < 35):
                self.limit = min(self.settings['max_nodes'], self.limit + 1)
                self.last_pressure = now
            for host, state in self.nodes.items():
                metrics = nodes.get(host, {})
                state.update(healthy=bool(metrics.get('healthy')), io=metrics.get('io_pressure_some_avg10', 0),
                             memory=metrics.get('memory_used_pct', 0))

    def _remote(self, host, source, links):
        c = self.config
        t = c['recovery_ssh_targets'][host]
        ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', '-o', 'StrictHostKeyChecking=yes',
               '-o', 'UserKnownHostsFile=' + c['recovery_known_hosts'], '-i', c['recovery_ssh_identity'],
               '-p', str(t.get('port', 22)), t.get('user', 'root') + '@' + t['hostname']]
        package = c['recovery_code_dir']
        # Use -m so relative imports retain the exact staged package identity.
        command = ['env', 'PYTHONPATH=' + str(Path(package).parent), 'python3', '-S', '-B',
                   '-m', 'ks_cloud.parallel_links', '--config', str(Path(c['run_dir']) / 'config.json'),
                   '--source', source]
        result = subprocess.run(ssh + [shlex.join(command)], input=json.dumps(links), text=True,
                                capture_output=True, timeout=self.settings['timeout_seconds'])
        if result.returncode or len(result.stdout.encode()) > MAX_RESULT_BYTES + 1024 * 1024:
            raise RuntimeError('remote link scanner failed or exceeded its output limit')
        return checked_records(links, json.loads(result.stdout.strip().splitlines()[-1]))

    def _worker(self, host):
        while not self.stop.is_set():
            try:
                source, links, future, tried = self.jobs.get(timeout=1)
            except queue.Empty:
                continue
            if future.done():
                continue
            self._settings()
            with self.lock:
                node = self.nodes[host]
                allowed = (self.enabled() and host not in tried and node['healthy']
                           and time.monotonic() >= node['blocked_until'] and node['io'] < 70
                           and node['memory'] < 65 and self.active < self.limit)
                if allowed:
                    self.active += 1
                    node['active'] = True
            if not allowed:
                self.jobs.put((source, links, future, tried))
                self.stop.wait(.05)
                continue
            try:
                records = self._remote(host, source, links)
                if not future.done():
                    future.set_result(records)
                with self.lock:
                    node['batches'] += 1
                    node['files'] += sum(r[0] == 'file' for r in records or [])
            except Exception:
                with self.lock:
                    node['errors'] += 1
                    node['blocked_until'] = time.monotonic() + 60
                if not future.done():
                    if len(tried) < 1 and len(self.nodes) > 1:
                        self.jobs.put((source, links, future, tried | {host}))
                    else:
                        future.set_result(None)
            finally:
                with self.lock:
                    self.active -= 1
                    node['active'] = False

    def batches(self, source, indexed_links, activity, batch_links=None):
        # Freeze batch boundaries for a source so crash replay keeps task IDs.
        batch_links = self.settings['batch_links'] if batch_links is None else batch_links
        if type(batch_links) is not int or not 1 <= batch_links <= MAX_LINKS:
            raise ValueError('invalid durable link batch size')
        pending = deque()
        iterator = iter(indexed_links)
        exhausted = False
        try:
            while pending or not exhausted:
                self._settings()
                while not exhausted and len(pending) < max(1, 2 * self.limit):
                    batch = []
                    for _ in range(batch_links):
                        try:
                            batch.append(next(iterator))
                        except StopIteration:
                            exhausted = True
                            break
                    if not batch:
                        break
                    links = [x[1] for x in batch]
                    future = Future()
                    if self.enabled():
                        self.jobs.put((source, links, future, set()))
                    else:
                        future.set_result(None)
                    pending.append((batch[-1][0], links, future, time.monotonic()))
                if not pending:
                    break
                end, links, future, started = pending.popleft()
                records = None
                while not self.stop.is_set() and time.monotonic() - started < 2 * self.settings['timeout_seconds'] + 10:
                    if (Path(self.config['run_dir']) / 'STOP').exists():
                        raise InterruptedError('STOP requested')
                    try:
                        records = future.result(timeout=1)
                        break
                    except TimeoutError:
                        activity('parallel_link_wait', links[0])
                future.cancel()
                if records is None:
                    with self.lock:
                        self.fallbacks += 1
                yield end, links, records
        finally:
            for _, _, future, _ in pending:
                future.cancel()

    def snapshot(self):
        with self.lock:
            return dict(enabled=self.enabled(), max_nodes=self.settings['max_nodes'],
                        effective_nodes=self.limit, active_nodes=self.active, queued=self.jobs.qsize(),
                        fallbacks=self.fallbacks, nodes={h: {k: n[k] for k in
                            ('healthy', 'active', 'files', 'batches', 'errors')} for h, n in self.nodes.items()})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--source', required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    print(json.dumps(scan_batch(config, args.source, json.load(sys.stdin)), ensure_ascii=False))


if __name__ == '__main__':
    main()
