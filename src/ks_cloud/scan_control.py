"""Bounded metadata prefetch and conservative, hot-reloadable scan control."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import stat
import threading
import time


DEFAULTS = dict(enabled=False, auto=True, workers=2, max_workers=8,
                prefetch_entries=16, queue_high_chunks=64, control_interval_seconds=60,
                cooldown_seconds=300, config_stale_seconds=120,
                memory_anon_limit_pct=65, memory_pressure_limit_pct=25, io_pressure_limit_pct=40,
                metadata_latency_limit_ms=2000, regression_ratio=0.75)
RANGES = dict(workers=(1,8), max_workers=(1,8), prefetch_entries=(1,128),
              queue_high_chunks=(1,256), control_interval_seconds=(30,600),
              cooldown_seconds=(60,3600), config_stale_seconds=(30,600),
              memory_anon_limit_pct=(20,90), memory_pressure_limit_pct=(5,90), io_pressure_limit_pct=(5,90),
              metadata_latency_limit_ms=(50,30000), regression_ratio=(0.3,0.95))


def validate_settings(value):
    if not isinstance(value, dict) or set(value)-set(DEFAULTS):
        raise ValueError('unknown scan tuning fields')
    result = dict(DEFAULTS, **value)
    for name in ['enabled','auto']:
        if type(result[name]) is not bool:
            raise ValueError(name+' must be boolean')
    for name,(low,high) in RANGES.items():
        v = result[name]
        numeric = name in ['memory_anon_limit_pct','memory_pressure_limit_pct','io_pressure_limit_pct',
                           'metadata_latency_limit_ms','regression_ratio']
        if (type(v) not in ((int,float) if numeric else (int,)) or not low <= v <= high):
            raise ValueError('invalid '+name)
    if result['workers'] > result['max_workers']:
        raise ValueError('workers exceeds max_workers')
    return result


class ScanControl:
    def __init__(self, config, clock=time.monotonic):
        self.config = config
        self.clock = clock
        self.lock = threading.RLock()
        self.settings = dict(DEFAULTS)
        self.checked = clock()
        self.error = None
        self.revision = 0
        self.sources = {}
        self.pool = None
        self.started = False
        self.permits = threading.BoundedSemaphore(8)
        self.oom_kills = 0
        self.pressure = {}
        self.stop_event = threading.Event()
        self.reader = None

    def _source(self, source):
        if source not in self.sources:
            self.sources[source] = dict(workers=1, files=0, operations=0, errors=0,
                elapsed=0., fallbacks=0, reason='serial', last=self.clock(),
                last_files=0, last_operations=0, last_errors=0, last_elapsed=0., last_completed=None, upload_files_s=0., best_upload_rate=0.,
                rate=0., latency_ms=0., best_rate=0., cooldown_until=0.)
        return self.sources[source]

    def load_once(self):
        """May block on NFS; called only by the independent reader thread."""
        try:
            value = validate_settings(json.loads(Path(self.config['scan_tuning_file']).read_text()))
            with self.lock:
                if value != self.settings:
                    self.settings = value
                    self.revision += 1
                    for s in self.sources.values():
                        s.update(workers=value['workers'] if value['enabled'] else 1,
                                 reason='config_changed', best_rate=0., cooldown_until=0.,
                                 last=self.clock(), last_files=s['files'],
                                 last_operations=s['operations'], last_errors=s['errors'],
                                 last_elapsed=s['elapsed'], last_completed=None, best_upload_rate=0.)
                self.error = None
                self.checked = self.clock()
        except (OSError, ValueError, TypeError) as error:
            with self.lock:
                self.error = type(error).__name__+': '+str(error)
                self.checked = self.clock()

    def start(self):
        if self.started or not self.config.get('scan_tuning_file'):
            return
        self.started = True
        def reader():
            while not self.stop_event.is_set():
                self.load_once()
                self.stop_event.wait(max(1,self.config.get('scan_tuning_poll_seconds',5)))
        self.reader = threading.Thread(target=reader,daemon=True,name='scan-config')
        self.reader.start()

    def _unsafe(self):
        return bool(self.error or (self.config.get('scan_tuning_file') and
                    self.clock()-self.checked > self.settings['config_stale_seconds']))

    def window(self, source):
        with self.lock:
            s = self._source(source)
            if not self.settings['enabled'] or self._unsafe():
                return 1
            if s['reason']=='serial':
                s['workers']=self.settings['workers']
                s['reason']='initial'
            return min(s['workers'], self.settings['max_workers'], self.settings['prefetch_entries'])

    def queue_limit(self, original):
        with self.lock:
            return min(original,self.settings['queue_high_chunks']) if self.settings['enabled'] else original

    def record(self, source, kind, elapsed):
        with self.lock:
            s=self._source(source)
            s['operations']+=1
            s['files']+=int(kind=='file')
            s['errors']+=int(kind=='exclude')
            s['elapsed']+=elapsed

    def observe(self, pending, pressure=None, completed=None):
        """Uses local counters only; no filesystem reads or database operations."""
        pressure=pressure or {}
        completed=completed or {}
        with self.lock:
            self.pressure = dict(pressure)
            now=self.clock(); c=self.settings
            oom_kills = pressure.get('oom_kill_events',0)
            new_oom = oom_kills > self.oom_kills
            self.oom_kills = oom_kills
            for source,s in self.sources.items():
                dt=now-s['last']
                reason=None
                if self._unsafe(): reason='config_unavailable'
                elif pressure.get('memory_used_pct',0)>=c['memory_anon_limit_pct']: reason='memory_pressure'
                elif pressure.get('memory_pressure_some_avg10',0)>=c['memory_pressure_limit_pct']: reason='memory_reclaim'
                elif pressure.get('io_pressure_some_avg10',0)>=c['io_pressure_limit_pct']: reason='io_pressure'
                elif new_oom: reason='oom_observed'
                if reason:
                    if s['workers']>1:s['fallbacks']+=1
                    s.update(workers=1,reason=reason,cooldown_until=now+c['cooldown_seconds'],
                             best_rate=0.,best_upload_rate=0.)
                if dt<c['control_interval_seconds']:continue
                ops=s['operations']-s['last_operations']
                rate=(s['files']-s['last_files'])/max(1,dt)
                latency=1000*(s['elapsed']-s['last_elapsed'])/max(1,ops)
                errors=s['errors']-s['last_errors']
                upload_rate = ((completed[source]-s['last_completed'])/max(1,dt)
                               if source in completed and s['last_completed'] is not None else None)
                if not reason and c['enabled']:
                    if ops and (latency>c['metadata_latency_limit_ms'] or errors/max(1,ops)>0.02):
                        reason='metadata_latency' if latency>c['metadata_latency_limit_ms'] else 'metadata_errors'
                    elif (upload_rate is not None and s['best_upload_rate'] and s['workers']>1
                          and upload_rate<s['best_upload_rate']*c['regression_ratio']
                          and pending.get(source,0)>0):
                        reason='upload_rate_regression'
                    elif pending.get(source,0)<max(1,c['queue_high_chunks']//2) and s['best_rate'] and rate<s['best_rate']*c['regression_ratio']:
                        reason='scan_rate_regression'
                    if reason:
                        if s['workers']>1:s['fallbacks']+=1
                        s.update(workers=1,reason=reason,cooldown_until=now+c['cooldown_seconds'],best_rate=0.,best_upload_rate=0.)
                    elif pending.get(source,0)>=c['queue_high_chunks']:
                        s.update(workers=1,reason='queue_full',best_rate=0.)
                    elif c['auto'] and now>=s['cooldown_until'] and ops and rate>0:
                        s['best_rate']=max(s['best_rate'],rate)
                        if pending.get(source,0)<max(1,c['queue_high_chunks']//2):
                            s['workers']=min(c['max_workers'],max(2,s['workers']*2))
                            s['reason']='queue_starved_probe'
                    elif not c['auto'] and now>=s['cooldown_until']:
                        s['workers']=c['workers'];s['reason']='manual'
                if upload_rate is not None:
                    s['best_upload_rate']=max(upload_rate,s['best_upload_rate']*0.95)
                    s['upload_files_s']=upload_rate
                s['last_completed']=completed.get(source)
                s.update(rate=rate,latency_ms=latency,last=now,last_files=s['files'],
                         last_operations=s['operations'],last_elapsed=s['elapsed'],last_errors=s['errors'])

    def snapshot(self):
        with self.lock:
            return dict(revision=self.revision,enabled=self.settings['enabled'],pressure=dict(self.pressure),
                        config_age_seconds=max(0,self.clock()-self.checked),
                        config_stale=self._unsafe(),config_error=self.error,
                        settings=dict(self.settings), sources={name:dict(
                            workers=self.window(name),files_s=s['rate'],upload_files_s=s['upload_files_s'],metadata_ms=s['latency_ms'],
                            operations=s['operations'],errors=s['errors'],fallbacks=s['fallbacks'],
                            reason=s['reason'],cooldown_seconds=max(0,s['cooldown_until']-self.clock()))
                            for name,s in self.sources.items()})

    def entries(self, source, entries, skip_before=None):
        """Bounded futures, a global eight-thread ceiling, deterministic output order."""
        def inspect(entry):
            with self.permits:
                return inspect_one(entry)

        def inspect_one(entry):
            start=time.monotonic()
            try:
                if entry.is_dir(follow_symlinks=False):result=('directory',None)
                elif skip_before is not None and entry.name<=skip_before:result=('skip',None)
                elif entry.is_symlink() and entry.is_dir():result=('exclude','directory symlink (not followed)')
                else:
                    info=entry.stat()
                    if not stat.S_ISREG(info.st_mode):result=('exclude','special file')
                    elif '\n' in entry.path or '\r' in entry.path:result=('exclude','newline in filename')
                    else:result=('file',info.st_size)
            except OSError as error:result=('exclude',str(error))
            self.record(source,result[0],time.monotonic()-start)
            return result
        iterator=iter(entries);pending=deque();exhausted=False
        try:
            while pending or not exhausted:
                window=self.window(source)
                while not exhausted and len(pending)<window:
                    try:entry=next(iterator)
                    except StopIteration:exhausted=True;break
                    if window==1 and not pending:
                        yield entry,inspect(entry)
                        break
                    with self.lock:
                        if self.pool is None:self.pool=ThreadPoolExecutor(max_workers=8,thread_name_prefix='scan-stat')
                    pending.append((entry,self.pool.submit(inspect,entry)))
                if pending:
                    entry,future=pending.popleft()
                    yield entry,future.result()
        finally:
            for _,future in pending:future.cancel()

    def close(self):
        self.stop_event.set()
        if self.reader:self.reader.join(timeout=1)
        if self.pool:self.pool.shutdown(wait=True,cancel_futures=True)
