"""Remote leader discovery and fail-closed coordination on the shared NFS lock."""
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid

from .common import save


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return default


def leader_record(config, strict=False):
    path=Path(config['run_dir'])/'leader.json'
    if not strict:
        return read_json(path,{})
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return {}


def live_leader(config, record=None):
    record = leader_record(config) if record is None else record
    return bool(record and time.time()-record.get('updated', 0) < config.get('leader_lease_seconds', 45))


def locked_owner(lock, config):
    """Read owner history on the locked inode, avoiding a stale renamed NFS dentry."""
    lock.seek(0,os.SEEK_END)
    size=lock.tell()
    lock.seek(max(0,size-16384))
    lines=lock.read().splitlines()
    for line in reversed(lines):
        try:
            record=json.loads(line)
            if isinstance(record,dict) and record.get('epoch'):
                return record
        except ValueError:
            pass
    # Compatibility with the pre-HA lock, whose file was empty.
    return leader_record(config,strict=True)


def restore_for_leadership(config, host, previous=None):
    """Call only while holding coordinator.lock; never reuse another term's local DB."""
    previous = leader_record(config,strict=True) if previous is None else previous
    local = Path(config['queue_db'])
    snapshot = Path(config['run_dir'])/'queue.sqlite'
    takeover = bool(previous and previous.get('host') != host)
    if previous and not local.exists() and not snapshot.exists():
        raise RuntimeError('Queue data is unavailable; refusing an empty recovery')
    if takeover or (not local.exists() and snapshot.exists()):
        if not snapshot.exists():
            raise RuntimeError('Cannot take over without a shared queue snapshot')
        local.parent.mkdir(parents=True, exist_ok=True)
        temporary = local.with_suffix('.takeover-tmp')
        source = sqlite3.connect(f'file:{snapshot}?mode=ro', uri=True)
        target = sqlite3.connect(temporary)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()
        os.replace(temporary, local)
        for suffix in ['-journal', '-wal', '-shm']:
            Path(str(local)+suffix).unlink(missing_ok=True)
        restored = True
    else:
        restored = False
    return previous, restored


class Leadership:
    def __init__(self, config, host, previous, restored, lock=None):
        self.config = config
        self.host = host
        self.epoch = uuid.uuid4().hex
        self.last_success = 0
        self.started = time.time()
        self.generation = previous.get('generation', 0)+1
        self.claims_after = self.started + (config.get('ha_recovery_grace_seconds',config.get('worker_disconnect_seconds', 90)+20) if restored else 0)
        self.endpoint = config['coordinator_endpoints'][host]
        self.lock=lock
        self.owner_recorded=False

    def publish(self):
        current = leader_record(self.config,strict=True)
        # The NFS lock is authoritative. Never overwrite an unexpected newer term.
        if current.get('generation', 0) > self.generation or (
                current.get('generation') == self.generation and current.get('epoch') != self.epoch):
            raise RuntimeError('Leadership term superseded')
        record=dict(
            host=self.host, pid=os.getpid(), epoch=self.epoch, generation=self.generation,
            endpoint=self.endpoint, started=self.started, updated=time.time(), claims_after=self.claims_after)
        if self.lock and not self.owner_recorded:
            # Append one term record under the distributed file lock. A leading
            # newline isolates a partial record left by a crash; never replace the
            # locked inode, and don't rely on a cached leader.json to choose a DB.
            self.lock.seek(0,os.SEEK_END)
            self.lock.write('\n'+json.dumps(record)+'\n')
            self.lock.flush();os.fsync(self.lock.fileno())
            self.owner_recorded=True
        save(Path(self.config['run_dir'])/'leader.json', record)
        self.last_success = time.monotonic()

    def healthy(self):
        return time.monotonic()-self.last_success < self.config.get('leader_lease_seconds', 45)

    def start(self):
        self.publish()
        def heartbeat():
            while True:
                time.sleep(self.config.get('leader_heartbeat_seconds', 10))
                try:
                    self.publish()
                except Exception as error:
                    print('leader heartbeat:', type(error).__name__, flush=True)
        threading.Thread(target=heartbeat, daemon=True).start()


class Discovery:
    def __init__(self, config):
        self.config = config
        self.endpoint = config['endpoint']
        self.checked = 0
        self.mutex = threading.Lock()
        self.collecting = False
        self.rejected_until = {}

    def _refresh(self):
        try:
            # A hard-mounted NFS read can wait indefinitely. Never hold the
            # discovery mutex or the RPC caller while reading this file.
            current = leader_record(self.config)
            endpoint = current.get('endpoint')
            with self.mutex:
                if endpoint and time.monotonic() >= self.rejected_until.get(endpoint, 0):
                    self.endpoint = endpoint
        except Exception:
            pass
        finally:
            with self.mutex:
                self.collecting = False

    def get(self, force=False):
        if not self.config.get('ha_enabled'):
            return self.endpoint
        start = False
        with self.mutex:
            now = time.monotonic()
            if force:
                self.rejected_until[self.endpoint] = now + 10
                candidates = list(dict.fromkeys([self.config['endpoint']] +
                                                list(self.config.get('coordinator_endpoints', {}).values())))
                index = candidates.index(self.endpoint) if self.endpoint in candidates else -1
                rotated = candidates[index + 1:] + candidates[:index + 1]
                self.endpoint = next((e for e in rotated if now >= self.rejected_until.get(e, 0)), rotated[0])
            if not self.collecting and (force or now-self.checked >= self.config.get('leader_refresh_seconds',5)):
                self.collecting = True
                self.checked = now
                start = True
            endpoint = self.endpoint
        if start:
            threading.Thread(target=self._refresh, daemon=True, name='leader-discovery').start()
        return endpoint


def recovery_snapshot(config):
    base = Path(config['run_dir'])
    leader = leader_record(config)
    guardians = {}
    for host in config['hosts']:
        state = read_json(base/'recovery'/(host+'.json'), {})
        guardians[host] = dict(updated=state.get('updated', 0),
                              alive=time.time()-state.get('updated', 0) < 90,
                              restarts=state.get('supervisor_starts', 0))
    mirror = read_json(base/'queue_mirror.json', {})
    return dict(leader=leader, guardians=guardians,
                snapshot_age_seconds=max(0,time.time()-mirror.get('updated',time.time())))


class RecoveryMonitor:
    """Keep optional shared-filesystem diagnostics off the progress publication path.

    Only one collector can be in flight, even if an NFS read blocks indefinitely.
    Expired samples are omitted rather than presenting cached guardians as online.
    This cache is diagnostic only; leadership fencing always uses live state.
    """
    def __init__(self, config):
        self.config = config
        self.mutex = threading.Lock()
        self.collecting = False
        self.sample = None
        self.started = None

    def _collect(self):
        started = time.time()
        try:
            sample = recovery_snapshot(self.config)
            # Timestamp the start: sequential reads may themselves take a long time.
            with self.mutex:
                self.sample = (started, sample)
        except Exception as error:
            print('recovery diagnostics:', type(error).__name__, flush=True)
        finally:
            with self.mutex:
                self.collecting = False

    def get(self):
        now = time.time()
        with self.mutex:
            age = now-self.sample[0] if self.sample else None
            if not self.collecting and (self.started is None or
                    now-self.started >= self.config.get('recovery_monitor_seconds', 30)):
                self.collecting = True
                self.started = now
                threading.Thread(target=self._collect, daemon=True).start()
            stale = age is None or age > self.config.get('recovery_monitor_stale_seconds', 90)
            sample = dict(self.sample[1]) if self.sample and not stale else None
            if sample:
                sample['snapshot_age_seconds'] += age
                sample['guardians'] = {host: dict(g, alive=now-g.get('updated',0)<90)
                                       for host,g in sample['guardians'].items()}
            return sample, age, stale
