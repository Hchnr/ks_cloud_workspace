"""Bounded, dynamically balanced read-only scanning of independent subtrees.

Results are consumed by the coordinator in the original DFS order. Workers do
not publish manifests or change the durable scan cursor. Any worker failure or
timeout falls back to the coordinator's ordinary serial traversal.
"""
import argparse
from collections import Counter, deque
from concurrent.futures import Future, InvalidStateError
from dataclasses import dataclass
from itertools import islice
import json
import os
from pathlib import Path
import queue
import re
import shlex
import stat
import subprocess
import sys
import threading
import time

if __package__:
    from .scope import OUTSIDE, UploadScope
else:
    from scope import OUTSIDE, UploadScope


SHARD = re.compile(r'gpic_train_\d{5}\.part\d{3}\Z')
YUANHAI_ITEM = re.compile(r'\d{9}\Z')
YUANHAI_BATCH = re.compile(r'[A-Za-z0-9_]+\Z')
IMAGE_BUCKET = re.compile(r'[a-fA-F0-9]{2}\Z')
LARGE_SUBTREE_ANCHORS = frozenset(('images', 'assets', 'frames', 'videos'))
MAX_RECORDS = 12000
MAX_IMAGE_RECORDS = 64000
MAX_BATCH = 32
ENTRY_BATCH_SIZE = 512
ENTRY_BATCH_THRESHOLD = 2048


@dataclass(frozen=True)
class EntryBatch:
    root: str
    names: tuple
    skip_before: str | None


def classify_entries(config, batch, scope=None):
    """Classify one bounded group from a single approved physical directory."""
    root, names, skip_before = batch.root, batch.names, batch.skip_before
    sources = [t.get('source') for t in config.get('tasks', [])
               if t.get('id') == config.get('parallel_scan_source')]
    if (len(sources) != 1 or not isinstance(sources[0], str)
            or not isinstance(root, str) or os.path.normpath(root) != root
            or not (root == sources[0] or root.startswith(sources[0]+os.sep))
            or not isinstance(names, tuple) or not 0 < len(names) <= ENTRY_BATCH_SIZE
            or (skip_before is not None and not isinstance(skip_before, str))):
        raise ValueError('invalid entry batch')
    if (any(not isinstance(name, str) or name in ('', '.', '..') or '/' in name or '\0' in name for name in names)
            or len(set(names)) != len(names)):
        raise ValueError('invalid entry names')
    scope = scope or UploadScope(config['scope_allowed_roots'])
    if (not scope.contains(os.path.realpath(sources[0], strict=True))
            or os.path.islink(root) or os.path.realpath(root, strict=True) != root
            or not os.path.isdir(root)):
        raise ValueError('entry directory identity or scope changed')
    started = time.monotonic()
    records = []
    for name in names:
        path = os.path.join(root, name)
        try:
            info = os.lstat(path)
            if stat.S_ISDIR(info.st_mode):
                kind, value = 'directory', None
            elif skip_before is not None and name <= skip_before:
                kind, value = 'skip', None
            elif stat.S_ISLNK(info.st_mode) and not scope.allows(path):
                kind, value = 'exclude', OUTSIDE
            elif stat.S_ISLNK(info.st_mode) and os.path.isdir(path):
                kind, value = 'exclude', 'directory symlink (not followed)'
            else:
                if stat.S_ISLNK(info.st_mode):
                    info = os.stat(path)
                if not stat.S_ISREG(info.st_mode):
                    kind, value = 'exclude', 'special file'
                elif '\n' in path or '\r' in path:
                    kind, value = 'exclude', 'newline in filename'
                else:
                    kind, value = 'file', info.st_size
        except OSError as error:
            kind, value = 'exclude', str(error)
        records.append([kind, path, value])
    return dict(root=root, records=records, operations=len(names), seconds=time.monotonic()-started)


def validate_entry_result(batch, result):
    records = result.get('records')
    if (result.get('root') != batch.root or not isinstance(records, list)
            or len(records) != len(batch.names) or type(result.get('operations')) is not int
            or result['operations'] != len(batch.names)
            or type(result.get('seconds')) not in (int, float) or result['seconds'] < 0):
        raise ValueError('invalid entry result')
    for name, row in zip(batch.names, records):
        if not isinstance(row, list) or len(row) != 3 or row[1] != os.path.join(batch.root, name):
            raise ValueError('entry result is out of order')
        kind, _, value = row
        if kind in ('directory', 'skip'):
            if (value is not None or (kind == 'skip' and
                    (batch.skip_before is None or name > batch.skip_before))):
                raise ValueError('invalid directory or skip result')
        elif kind == 'file':
            if type(value) is not int or value < 0:
                raise ValueError('invalid file size')
        elif kind != 'exclude' or not isinstance(value, str):
            raise ValueError('invalid entry result type')
    return records


def source_subtree_base(config, root):
    """Return the approved parallel source containing this physical subtree."""
    if not isinstance(root, str) or os.path.normpath(root) != root:
        return None
    sources = [t.get('source') for t in config.get('tasks', [])
               if t.get('id') == config.get('parallel_scan_source')]
    if len(sources) != 1 or not isinstance(sources[0], str):
        return None
    source = sources[0]
    if not root.startswith(source + os.sep):
        return None
    return source


def image_bucket_base(config, root):
    """Recognize larger media subtrees inside the configured source."""
    source = source_subtree_base(config, root)
    if source is None:
        return None
    parts = root[len(source) + 1:].split(os.sep)
    anchors = [i for i, part in enumerate(parts[:-1]) if part in LARGE_SUBTREE_ANCHORS]
    if not anchors:
        return None
    return source


def scan_subtree(config, root, scope=None):
    """Return exactly the serial scanner's file/exclusion stream, or overflow."""
    base = config.get('parallel_scan_base', '')
    yuanhai = config.get('parallel_yuanhai_scan_base', '')
    shard = os.path.dirname(root) == base and SHARD.fullmatch(os.path.basename(root))
    item = (os.path.dirname(os.path.dirname(root)) == yuanhai
            and YUANHAI_BATCH.fullmatch(os.path.basename(os.path.dirname(root)))
            and YUANHAI_ITEM.fullmatch(os.path.basename(root)))
    image_base = image_bucket_base(config, root)
    source_base = source_subtree_base(config, root)
    if os.path.normpath(root) != root or not (shard or item or source_base):
        raise ValueError('subtree is not an approved shard')
    base = base if shard else yuanhai if item else source_base
    record_limit = MAX_IMAGE_RECORDS if image_base else MAX_RECORDS
    scope = scope or UploadScope(config['scope_allowed_roots'])
    if (not scope.contains(os.path.realpath(base, strict=True)) or os.path.islink(root)
            or os.path.realpath(root, strict=True) != root or not os.path.isdir(root)):
        raise ValueError('subtree identity or scope changed')
    records = []
    stack = [root]
    started = time.monotonic()
    operations = 0
    while stack:
        directory = stack.pop()
        try:
            with os.scandir(directory) as iterator:
                entries = []
                for entry in iterator:
                    entries.append(entry)
                    # Bound memory even when a single directory has millions
                    # of entries. The coordinator will split or scan it locally.
                    if len(entries) > record_limit:
                        return dict(root=root, overflow=True)
                entries.sort(key=lambda entry: entry.name)
                # Generic paths split into independent leaf directories. A
                # first-level media grouping is split as well, while known
                # hash buckets can be traversed as bounded subtrees.
                generic = not shard and not item and not image_base
                broad_media = (image_base and os.path.basename(os.path.dirname(root)) in LARGE_SUBTREE_ANCHORS
                               and not IMAGE_BUCKET.fullmatch(os.path.basename(root)))
                if directory == root and (generic or broad_media):
                    try:
                        if any(entry.is_dir(follow_symlinks=False) for entry in entries):
                            return dict(root=root, overflow=True)
                    except OSError:
                        return dict(root=root, overflow=True)
        except OSError as error:
            records.append(['exclude', directory, str(error)])
            continue
        dirs = []
        for entry in entries:
            operations += 1
            try:
                if entry.is_dir(follow_symlinks=False):
                    dirs.append(entry.path)
                elif entry.is_symlink() and not scope.allows(entry.path):
                    records.append(['exclude', entry.path, OUTSIDE])
                elif entry.is_symlink() and entry.is_dir():
                    records.append(['exclude', entry.path, 'directory symlink (not followed)'])
                else:
                    info = entry.stat()
                    if not stat.S_ISREG(info.st_mode):
                        records.append(['exclude', entry.path, 'special file'])
                    elif '\n' in entry.path or '\r' in entry.path:
                        records.append(['exclude', entry.path, 'newline in filename'])
                    else:
                        records.append(['file', entry.path, info.st_size])
            except OSError as error:
                records.append(['exclude', entry.path, str(error)])
            if len(records) > record_limit or operations > 4*record_limit:
                return dict(root=root, overflow=True)
        stack.extend(reversed(dirs))
    return dict(root=root, records=records, operations=operations,
                seconds=time.monotonic()-started)


def validate_result(root, result):
    if result.get('root') != root or result.get('overflow'):
        raise ValueError('subtree unavailable or exceeded result limit')
    records = result.get('records')
    if not isinstance(records, list) or len(records) > MAX_IMAGE_RECORDS:
        raise ValueError('invalid subtree records')
    if (type(result.get('operations')) is not int or result['operations'] < 0
            or type(result.get('seconds')) not in (int, float) or result['seconds'] < 0):
        raise ValueError('invalid subtree statistics')
    seen = set()
    for row in records:
        if not isinstance(row, list) or len(row) != 3:
            raise ValueError('invalid subtree record')
        kind, path, value = row
        if (not isinstance(path, str) or os.path.normpath(path) != path
                or not path.startswith(root + os.sep) or path in seen):
            raise ValueError('invalid or duplicate subtree path')
        if kind == 'file':
            if type(value) is not int or value < 0:
                raise ValueError('invalid file size')
        elif kind != 'exclude' or not isinstance(value, str):
            raise ValueError('invalid subtree record type')
        seen.add(path)
    return records


class ParallelScanPool:
    """A shared queue lets each healthy host claim the next available shard."""
    def __init__(self, config):
        self.config = config
        self.base = config['parallel_scan_base']
        self.tuning_file = Path(config['parallel_scan_tuning_file'])
        self.lock = threading.RLock()
        self.jobs = queue.Queue()
        self.futures = {}
        self.local_only = set()
        self.entry_buffers = 0
        self.resuming_entries = 0
        self.stop = threading.Event()
        self.settings = dict(enabled=False, max_nodes=1, timeout_seconds=60,
                             image_timeout_seconds=120,
                             io_limit_pct=70, memory_limit_pct=65, cooldown_seconds=300,
                             minimum_completed_files_s=500, coexist_minimum_completed_files_s=0)
        self.last_config = 0
        self.active = 0
        self.cooldown_until = 0
        self.regression_windows = 0
        self.image_scan_seen = False
        self.image_regression_samples = []
        self.coordinator_oom = 0
        self.previous = None
        self.fallbacks = 0
        self.nodes = {host: dict(healthy=True, active=False, files=0, subtrees=0,
                                 errors=0, cooldown_until=0, io=0, memory=0, oom=0)
                      for host in config['hosts']}
        self.reader = threading.Thread(target=self._read_settings, daemon=True)
        self.reader.start()
        self.threads = []
        for host in config['hosts']:
            thread = threading.Thread(target=self._worker, args=(host,), daemon=True,
                                      name='parallel-scan-'+host)
            thread.start()
            self.threads.append(thread)

    def _read_settings(self):
        while not self.stop.is_set():
            try:
                x = json.loads(self.tuning_file.read_text())
                if (type(x.get('enabled')) is not bool or type(x.get('max_nodes')) is not int
                        or not 1 <= x['max_nodes'] <= len(self.nodes)
                        or not 10 <= x.get('timeout_seconds', 60) <= 300
                        or not 10 <= x.get('image_timeout_seconds', 120) <= 300
                        or not 30 <= x.get('io_limit_pct', 70) <= 95
                        or not 20 <= x.get('memory_limit_pct', 65) <= 90
                        or not 60 <= x.get('cooldown_seconds', 300) <= 3600
                        or not 0 <= x.get('minimum_completed_files_s', 500) <= 100000
                        or not 0 <= x.get('coexist_minimum_completed_files_s', 0) <= 100000):
                    raise ValueError('invalid parallel scan settings')
                with self.lock:
                    self.settings.update(x)
                    self.last_config = time.monotonic()
            except (OSError, ValueError, TypeError):
                pass
            self.stop.wait(5)

    def enabled(self):
        with self.lock:
            return (self.settings['enabled'] and time.monotonic()-self.last_config < 120
                    and time.monotonic() >= self.cooldown_until)

    def eligible(self, path, cursor):
        selected = ((os.path.dirname(path) == self.base and SHARD.fullmatch(os.path.basename(path)))
                    or source_subtree_base(self.config, path))
        return (bool(selected)
                and not (cursor and (str(cursor) == path or str(cursor).startswith(path+os.sep))))

    def prime(self, stack, cursor):
        if not self.enabled():
            return
        # A speculative broad directory may already have requested splitting.
        # Release its buffer slot so its children can be scheduled in parallel.
        for path, future in list(self.futures.items()):
            if future.done() and not future.cancelled():
                try:
                    result = future.result()
                    if result.get('overflow'):
                        self.futures.pop(path)
                        self.local_only.add(path)
                except Exception:
                    pass
        with self.lock:
            limit = 2*self.settings['max_nodes']
        candidates = []
        for path, entry in reversed(stack):
            if len(candidates) >= limit:
                break
            if path in self.futures or path in self.local_only or not self.eligible(path, cursor):
                continue
            if entry is not None and not entry.is_dir(follow_symlinks=False):
                continue
            candidates.append(path)
        siblings = Counter(os.path.dirname(path) for path in candidates)
        for path in candidates:
            if len(self.futures) >= limit:
                break
            image_base = image_bucket_base(self.config, path)
            direct_hash = (os.path.basename(os.path.dirname(path)) in LARGE_SUBTREE_ANCHORS
                           and IMAGE_BUCKET.fullmatch(os.path.basename(path)))
            # Do not send a lone broad container to a worker. Let the serial
            # traversal expose its independent children as smaller jobs.
            if not direct_hash and siblings[os.path.dirname(path)] < 2:
                if not any(os.path.dirname(p) == os.path.dirname(path) for p in self.futures):
                    continue
            # Large hash buckets may contain tens of thousands of paths each.
            # Keep their completed, ordered results bounded to one per node.
            if image_base and len(self.futures) >= self.settings['max_nodes']:
                continue
            future = Future()
            self.futures[path] = future
            self.jobs.put((path, future, 0))

    def consume(self, root, activity):
        if root in self.local_only:
            self.local_only.remove(root)
            with self.lock:
                self.fallbacks += 1
            return None
        future = self.futures.pop(root, None)
        if future is None:
            return None
        with self.lock:
            timeout = (self.settings['image_timeout_seconds'] if image_bucket_base(self.config, root)
                       else self.settings['timeout_seconds']) * 2 + 10
        until = time.monotonic() + timeout
        while time.monotonic() < until and self.enabled() and not self.stop.is_set():
            try:
                result = future.result(timeout=1)
                return validate_result(root, result), result.get('seconds', 0), result.get('operations', 0)
            except TimeoutError:
                activity('parallel_wait', root)
            except Exception:
                break
        future.cancel()
        with self.lock:
            self.fallbacks += 1
        return None

    def entries(self, root, entries, skip_before, control, source, activity):
        """Dynamically classify bounded batches, yielding in original order."""
        iterator = iter(entries)
        pending = deque()
        exhausted = False
        fallback = False
        resuming = skip_before is not None
        if resuming:
            with self.lock:
                self.resuming_entries += 1
        try:
            while pending or not exhausted:
                with self.lock:
                    limit = 2*self.settings['max_nodes']
                    timeout = self.settings['timeout_seconds'] * 2 + 10
                while not exhausted and not fallback and self.enabled() and len(pending) < limit:
                    group = list(islice(iterator, ENTRY_BATCH_SIZE))
                    if not group:
                        exhausted = True
                        break
                    batch = EntryBatch(root, tuple(entry.name for entry in group), skip_before)
                    future = Future()
                    pending.append((group, batch, future))
                    with self.lock:
                        self.entry_buffers += 1
                    self.jobs.put((batch, future, 0))
                if not pending:
                    if not exhausted:
                        yield from control.entries(source, iterator, skip_before)
                    break
                group, batch, future = pending.popleft()
                records = None
                result = None
                if not fallback:
                    until = time.monotonic()+timeout
                    while time.monotonic() < until and self.enabled() and not self.stop.is_set():
                        try:
                            result = future.result(timeout=1)
                            records = validate_entry_result(batch, result)
                            break
                        except TimeoutError:
                            activity('parallel_wait', root)
                        except Exception:
                            break
                with self.lock:
                    self.entry_buffers -= 1
                if records is None:
                    future.cancel()
                    if not fallback:
                        with self.lock:
                            self.fallbacks += 1
                    fallback = True
                    yield from control.entries(source, group, skip_before)
                    continue
                mean_elapsed = result['seconds']/max(1, result['operations'])
                for entry, (kind, _, value) in zip(group, records):
                    control.record(source, kind, mean_elapsed)
                    yield entry, (kind, value)
                if resuming and group[-1].name > skip_before:
                    with self.lock:
                        self.resuming_entries -= 1
                    resuming = False
        finally:
            for _, _, future in pending:
                future.cancel()
            with self.lock:
                self.entry_buffers -= len(pending)
                if resuming:
                    self.resuming_entries -= 1

    def _remote(self, host, path, timeout):
        target = self.config['recovery_ssh_targets'][host]
        cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
               '-o', 'StrictHostKeyChecking=yes',
               '-o', 'UserKnownHostsFile='+self.config['recovery_known_hosts'],
               '-i', self.config['recovery_ssh_identity'], '-p', str(target.get('port', 22)),
               target.get('user', 'root')+'@'+target['hostname']]
        helper = str(Path(self.config['recovery_code_dir']) / 'parallel_scan.py')
        remote = ['timeout', '-k', '3s', str(timeout), 'python3', '-S', '-B', helper,
                  '--config', str(Path(self.config['run_dir']) / 'config.json')]
        if isinstance(path, EntryBatch):
            remote += ['--classify-entries']
            payload = json.dumps(dict(root=path.root, names=path.names, skip_before=path.skip_before))
        else:
            remote += ['--scan', path]
            payload = None
        process = subprocess.run(cmd+[shlex.join(remote)], input=payload, capture_output=True,
                                 text=True, timeout=timeout+8)
        if process.returncode:
            raise RuntimeError('remote scanner exit '+str(process.returncode))
        return json.loads(process.stdout.strip().splitlines()[-1])

    def _checked_records(self, path, result):
        return validate_entry_result(path, result) if isinstance(path, EntryBatch) else validate_result(path, result)

    def _worker(self, host):
        while not self.stop.is_set():
            try:
                path, future, attempts = self.jobs.get(timeout=1)
            except queue.Empty:
                continue
            if future.done():
                continue
            with self.lock:
                n = self.nodes[host]
                allowed = (self.enabled() and n['healthy'] and not n['active']
                           and time.monotonic() >= n['cooldown_until']
                           and n['io'] < self.settings['io_limit_pct']
                           and n['memory'] < self.settings['memory_limit_pct']
                           and self.active < self.settings['max_nodes'])
                if allowed:
                    self.active += 1
                    n['active'] = True
                    is_image = isinstance(path, str) and bool(image_bucket_base(self.config, path))
                    if is_image:
                        self.image_scan_seen = True
                    timeout = self.settings['image_timeout_seconds'] if is_image else self.settings['timeout_seconds']
            if not allowed:
                self.jobs.put((path, future, attempts))
                self.stop.wait(1)
                continue
            try:
                try:
                    result = self._remote(host, path, timeout)
                    # An oversized directory is a valid result and falls back
                    # locally; it is not evidence that the node is unhealthy.
                    if isinstance(path, str) and result.get('overflow'):
                        if not future.done():
                            try:
                                future.set_result(result)
                            except InvalidStateError:
                                pass
                        continue
                    records = self._checked_records(path, result)
                    with self.lock:
                        n['files'] += sum(row[0] == 'file' for row in records)
                        n['subtrees'] += len(path) if isinstance(path, tuple) else 1
                    if not future.done():
                        try:
                            future.set_result(result)
                        except InvalidStateError:
                            pass
                except Exception as error:
                    with self.lock:
                        n['errors'] += 1
                        n['cooldown_until'] = time.monotonic() + self.settings['cooldown_seconds']
                    if not future.done():
                        if attempts + 1 < min(3, len(self.nodes)):
                            self.jobs.put((path, future, attempts+1))
                        else:
                            try:
                                future.set_exception(error)
                            except InvalidStateError:
                                pass
            finally:
                with self.lock:
                    self.active -= 1
                    n['active'] = False

    def observe(self, nodes, completed, now, scanning, pressure=None, minimum_override=None):
        with self.lock:
            pressure = pressure or {}
            oom = pressure.get('oom_kill_events', 0)
            if (oom > self.coordinator_oom or pressure.get('memory_used_pct', 0) >= self.settings['memory_limit_pct']
                    or pressure.get('memory_pressure_some_avg10', 0) >= 25
                    or pressure.get('io_pressure_some_avg10', 0) >= self.settings['io_limit_pct']):
                self.cooldown_until = time.monotonic()+self.settings['cooldown_seconds']
            self.coordinator_oom = oom
            for host, n in self.nodes.items():
                metrics = nodes.get(host, {})
                n['healthy'] = bool(metrics.get('healthy'))
                n['io'] = metrics.get('io_pressure_some_avg10', 0)
                n['memory'] = metrics.get('memory_used_pct', 0)
                oom = metrics.get('oom_kill_events', 0)
                if n['oom'] is not None and oom > n['oom']:
                    n['cooldown_until'] = time.monotonic()+self.settings['cooldown_seconds']
                n['oom'] = oom
                if metrics.get('memory_pressure_some_avg10', 0) >= 25:
                    n['cooldown_until'] = time.monotonic()+self.settings['cooldown_seconds']
            if self.resuming_entries:
                # Revalidating the prefix does not advance committed-file
                # counters, so it must not be treated as a speed regression.
                self.regression_windows = 0
                self.image_regression_samples = []
            elif self.image_scan_seen and scanning:
                # Large buckets complete in bursts. Use a multi-minute window
                # before declaring a sustained regression and falling back.
                if self.active or self.entry_buffers:
                    self.image_regression_samples.append((now, completed))
                    while (len(self.image_regression_samples) > 2 and
                           now-self.image_regression_samples[1][0] >= 180):
                        self.image_regression_samples.pop(0)
                    start, count = self.image_regression_samples[0]
                    elapsed = now-start
                    if elapsed >= 180:
                        minimum = (self.settings['minimum_completed_files_s'] if minimum_override is None
                                   else minimum_override)
                        if max(0, completed-count)/elapsed < minimum:
                            self.cooldown_until = time.monotonic()+self.settings['cooldown_seconds']
                        self.image_regression_samples = [(now, completed)]
                else:
                    self.image_regression_samples = []
            elif self.previous and scanning and (self.active or self.entry_buffers):
                old_time, old_count = self.previous
                dt = now-old_time
                if dt >= 25:
                    rate = max(0, completed-old_count)/dt
                    minimum = (self.settings['minimum_completed_files_s'] if minimum_override is None
                               else minimum_override)
                    if rate < minimum:
                        self.regression_windows += 1
                    else:
                        self.regression_windows = 0
                    if self.regression_windows >= 2:
                        self.cooldown_until = time.monotonic()+self.settings['cooldown_seconds']
                        self.regression_windows = 0
            else:
                self.regression_windows = 0
            self.previous = (now, completed)

    def snapshot(self):
        with self.lock:
            return dict(enabled=self.enabled(), max_nodes=self.settings['max_nodes'], active_nodes=self.active,
                        queued=self.jobs.qsize(), buffered=len(self.futures)+self.entry_buffers,
                        fallbacks=self.fallbacks, resuming_entries=self.resuming_entries,
                        cooldown_seconds=max(0, self.cooldown_until-time.monotonic()),
                        nodes={host: dict(healthy=n['healthy'], active=n['active'], files=n['files'],
                                          subtrees=n['subtrees'], errors=n['errors'])
                               for host, n in self.nodes.items()})


class BatchParallelScanPool(ParallelScanPool):
    """Balance batches of small yuanhai task subtrees across the same nodes."""
    def __init__(self, config):
        super().__init__(config)
        self.base = config['parallel_yuanhai_scan_base']

    def eligible(self, path, cursor):
        return (os.path.dirname(os.path.dirname(path)) == self.base
                and YUANHAI_BATCH.fullmatch(os.path.basename(os.path.dirname(path)))
                and YUANHAI_ITEM.fullmatch(os.path.basename(path))
                and not (cursor and (str(cursor) == path or str(cursor).startswith(path+os.sep))))

    def observe(self, nodes, completed, now, scanning, pressure=None, coexist=False):
        with self.lock:
            minimum = self.settings['coexist_minimum_completed_files_s'] if coexist else None
        super().observe(nodes, completed, now, scanning, pressure, minimum_override=minimum or None)

    def prime(self, stack, cursor):
        if not self.enabled():
            return
        with self.lock:
            group_limit = 2*self.settings['max_nodes']
        group_count = len(set(self.futures.values()))
        if group_count >= group_limit:
            return
        current, parent = [], None
        def submit():
            nonlocal current, group_count
            if current and group_count < group_limit:
                future = Future()
                paths = tuple(current)
                for path in paths:
                    self.futures[path] = future
                self.jobs.put((paths, future, 0))
                group_count += 1
                current = []
        for path, entry in reversed(stack):
            if group_count >= group_limit:
                break
            if path in self.futures or not self.eligible(path, cursor):
                continue
            if entry is not None and not entry.is_dir(follow_symlinks=False):
                continue
            path_parent = os.path.dirname(path)
            if current and (path_parent != parent or len(current) >= MAX_BATCH):
                submit()
            if group_count >= group_limit:
                break
            current.append(path)
            parent = path_parent
        submit()

    def _remote(self, host, paths, timeout):
        target = self.config['recovery_ssh_targets'][host]
        cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
               '-o', 'StrictHostKeyChecking=yes',
               '-o', 'UserKnownHostsFile='+self.config['recovery_known_hosts'],
               '-i', self.config['recovery_ssh_identity'], '-p', str(target.get('port', 22)),
               target.get('user', 'root')+'@'+target['hostname']]
        helper = str(Path(self.config['recovery_code_dir']) / 'parallel_scan.py')
        remote = ['timeout', '-k', '3s', str(timeout), 'python3', '-S', '-B', helper,
                  '--config', str(Path(self.config['run_dir']) / 'config.json'), '--scan-batch', *paths]
        process = subprocess.run(cmd+[shlex.join(remote)], capture_output=True, text=True, timeout=timeout+8)
        if process.returncode:
            raise RuntimeError('remote scanner exit '+str(process.returncode))
        return json.loads(process.stdout.strip().splitlines()[-1])

    def _checked_records(self, paths, result):
        items = result.get('items')
        if not isinstance(items, list) or len(items) != len(paths):
            raise ValueError('batch result does not match request')
        records = []
        for root, value in zip(paths, items):
            if len(value.get('records', [])) > MAX_RECORDS:
                raise ValueError('batch subtree exceeded result limit')
            records.extend(validate_result(root, value))
        return records

    def consume(self, root, activity):
        future = self.futures.pop(root, None)
        if future is None:
            return None
        with self.lock:
            timeout = self.settings['timeout_seconds'] * 2 + 10
        until = time.monotonic()+timeout
        while time.monotonic() < until and self.enabled() and not self.stop.is_set():
            try:
                result = future.result(timeout=1)
                item = next(value for value in result['items'] if value.get('root') == root)
                return validate_result(root, item), item.get('seconds', 0), item.get('operations', 0)
            except TimeoutError:
                activity('parallel_wait', root)
            except Exception:
                break
        future.cancel()
        with self.lock:
            self.fallbacks += 1
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--scan')
    group.add_argument('--scan-batch', nargs='+')
    group.add_argument('--classify-entries', action='store_true')
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    if args.classify_entries:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict) or not isinstance(payload.get('names'), list):
            raise ValueError('invalid entry request')
        result = classify_entries(config, EntryBatch(payload.get('root'), tuple(payload['names']),
                                                     payload.get('skip_before')))
    elif args.scan_batch:
        if len(args.scan_batch) > MAX_BATCH:
            raise ValueError('batch exceeds limit')
        scope = UploadScope(config['scope_allowed_roots'])
        result = dict(items=[scan_subtree(config, path, scope=scope) for path in args.scan_batch])
    else:
        result = scan_subtree(config, args.scan)
    print(json.dumps(result, ensure_ascii=False, separators=(',', ':')))


if __name__ == '__main__':
    main()
