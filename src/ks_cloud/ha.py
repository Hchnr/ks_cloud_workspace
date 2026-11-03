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

    def get(self, force=False):
        if not self.config.get('ha_enabled'):
            return self.endpoint
        with self.mutex:
            if force or time.monotonic()-self.checked >= self.config.get('leader_refresh_seconds',5):
                current = leader_record(self.config)
                if current.get('endpoint'):
                    self.endpoint = current['endpoint']
                self.checked = time.monotonic()
        return self.endpoint


def recovery_snapshot(config):
    base = Path(config['run_dir'])
    leader = leader_record(config)
    guardians = {}
    for host in config['hosts']:
        state = read_json(base/'recovery'/(host+'.json'), {})
        guardians[host] = dict(alive=time.time()-state.get('updated', 0) < 90,
                              restarts=state.get('supervisor_starts', 0))
    mirror = read_json(base/'queue_mirror.json', {})
    return dict(leader=leader, guardians=guardians,
                snapshot_age_seconds=max(0,time.time()-mirror.get('updated',time.time())))
