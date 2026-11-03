"""Pull-based KS3 scheduling: a single durable coordinator and leased workers."""
import argparse
import concurrent.futures
import fcntl
import faulthandler
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import shutil
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.parse
import uuid

from .common import parse_progress, save, successful, tail, upload_command
from .ha import Discovery, Leadership, restore_for_leadership, RecoveryMonitor, locked_owner


class Queue:
    def __init__(self, config):
        self.config = config
        self.base = Path(config['run_dir'])
        self.base.mkdir(parents=True, exist_ok=True)
        self.mutex = threading.RLock()
        self.db_path = Path(config.get('queue_db', str(self.base / 'queue.sqlite')))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if self.db_path != self.base / 'queue.sqlite' and not self.db_path.exists() and (self.base / 'queue.sqlite').exists():
            # The coordinator lock is held before opening/migrating the production database.
            source = sqlite3.connect(self.base / 'queue.sqlite')
            restoring = self.db_path.with_suffix('.restore-tmp')
            target = sqlite3.connect(restoring)
            try:
                source.backup(target)
            finally:
                source.close()
                target.close()
            os.replace(restoring, self.db_path)
        self.db = sqlite3.connect(self.db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=DELETE;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS tasks (
              id TEXT PRIMARY KEY, source TEXT, manifest TEXT, files INTEGER, bytes INTEGER,
              state TEXT DEFAULT 'pending', owner TEXT, token TEXT, heartbeat REAL DEFAULT 0,
              attempt INTEGER DEFAULT 0, ready REAL DEFAULT 0, progress TEXT DEFAULT '{}',
              result TEXT DEFAULT '{}', finished REAL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS ready_tasks ON tasks(state,source,ready);
            CREATE TABLE IF NOT EXISTS scans (source TEXT PRIMARY KEY, state TEXT, error TEXT);
            CREATE TABLE IF NOT EXISTS nodes (host TEXT PRIMARY KEY, updated REAL, metrics TEXT);
        ''')
        if 'request_id' not in {r[1] for r in self.db.execute('PRAGMA table_info(tasks)')}:
            self.db.execute('ALTER TABLE tasks ADD COLUMN request_id TEXT')
        if 'confirmed' not in {r[1] for r in self.db.execute('PRAGMA table_info(tasks)')}:
            self.db.execute('ALTER TABLE tasks ADD COLUMN confirmed INTEGER DEFAULT 0')
        self.db.execute('CREATE UNIQUE INDEX IF NOT EXISTS claim_requests ON tasks(request_id)')
        for task in config['tasks']:
            self.db.execute('INSERT OR IGNORE INTO scans VALUES (?, ?, ?)', (task['id'], 'pending', ''))
        self.db.commit()
        self.sources = {t['id']: t for t in config['tasks']}
        self.scan_activity = {}
        self.cursor = 0
        self.recovery_monitor = RecoveryMonitor(config) if config.get('ha_enabled') else None

    def mirror(self):
        """Take a local consistent snapshot; shared disk writes never hold the RPC lock."""
        destination = self.base / 'queue.sqlite'
        if self.db_path == destination:
            return
        local = self.db_path.with_suffix('.snapshot')
        target = sqlite3.connect(local)
        try:
            with self.mutex:
                self.db.backup(target)
        finally:
            target.close()
        temp = destination.with_suffix('.sqlite.mirror-tmp')
        with local.open('rb') as source, temp.open('wb') as output:
            shutil.copyfileobj(source, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temp, destination)
        save(self.base/'queue_mirror.json', dict(updated=time.time(), primary=str(self.db_path), bytes=local.stat().st_size))

    def assignment(self, row):
        task = dict(row)
        config = dict(self.config, **self.sources[row['source']])
        task['config'] = {k: config[k] for k in ['tool','config_file','source','destination','jobs','parallel','list_parallel']}
        task['config']['enable_http2'] = self.config.get('enable_http2', True)
        if row['files'] <= 2 and row['bytes'] >= 1024**3:
            task['config']['parallel'] = self.config.get('large_file_parallel', 8)
        return dict(task=task)

    def add(self, source, paths, sizes):
        content = ''.join(p + '\n' for p in paths)
        digest = hashlib.sha256((content + repr(sizes)).encode()).hexdigest()[:32]
        tid = source + '-' + digest
        folder = self.base / 'manifests' / source
        folder.mkdir(parents=True, exist_ok=True)
        manifest = folder / (digest + '.txt')
        if not manifest.exists():
            temp = manifest.with_suffix('.tmp')
            temp.write_text(content)
            os.replace(temp, manifest)
        with self.mutex, self.db:
            self.db.execute('INSERT OR IGNORE INTO tasks(id,source,manifest,files,bytes) VALUES (?,?,?,?,?)',
                            (tid, source, str(manifest), len(paths), sum(sizes)))

    def pending(self, source):
        with self.mutex:
            return self.db.execute("SELECT count(*) FROM tasks WHERE source=? AND state='pending'", (source,)).fetchone()[0]

    def scan_state(self, source, state, error=''):
        with self.mutex, self.db:
            self.db.execute('UPDATE scans SET state=?,error=? WHERE source=?', (state, error, source))

    def scan(self, task):
        source = task['id']
        cursor = None
        resume_directory = None
        resume_name = None
        def activity(operation, path):
            self.scan_activity[source] = dict(operation=operation, path=str(path), updated=time.time(),
                                              resumed_after=str(cursor) if cursor else None)
        with self.mutex:
            if self.db.execute('SELECT state FROM scans WHERE source=?', (source,)).fetchone()[0] == 'done':
                return
            previous = self.db.execute('SELECT manifest FROM tasks WHERE source=? ORDER BY rowid DESC LIMIT 1', (source,)).fetchone()
        # Manifests commit in deterministic files-before-subdirectories DFS order.
        # The last committed file is a durable cursor, including pending/retry work.
        # Source trees must remain stable; a new full check requires a new run.
        if previous and self.config.get('resume_scans', True):
            lines = Path(previous['manifest']).read_text().splitlines()
            if lines:
                candidate = Path(lines[-1])
                try:
                    relative = candidate.relative_to(task['source'])
                except ValueError:
                    pass
                else:
                    cursor = candidate
                    resume_directory, resume_name = relative.parent.parts, relative.name
        self.scan_state(source, 'scanning')
        activity('starting', task['source'])
        paths, sizes = [], []
        batch_bytes = 0
        audit = self.base / ('excluded-' + source + '.jsonl')
        errors = 0
        if cursor and audit.exists():
            with audit.open() as history:
                errors = sum(1 for line in history if line.strip())
        with audit.open('a' if cursor else 'w') as excluded:
            def exclude(path, reason):
                nonlocal errors
                errors += 1
                activity('record_exclusion', path)
                excluded.write(json.dumps(dict(path=str(path), reason=reason), ensure_ascii=False) + '\n')

            def flush():
                nonlocal paths, sizes, batch_bytes
                if paths:
                    # Allow already committed chunks through on restart without backpressure.
                    content = ''.join(p + '\n' for p in paths)
                    tid = source + '-' + hashlib.sha256((content + repr(sizes)).encode()).hexdigest()[:32]
                    with self.mutex:
                        exists = self.db.execute('SELECT 1 FROM tasks WHERE id=?', (tid,)).fetchone()
                    while not exists and self.pending(source) >= self.config.get('pending_per_source', 256):
                        activity('queue_backpressure', paths[-1])
                        if (self.base / 'STOP').exists():
                            raise InterruptedError('STOP requested')
                        time.sleep(2)
                    activity('commit_manifest', paths[-1])
                    self.add(source, paths, sizes)
                    paths, sizes, batch_bytes = [], [], 0

            try:
                activity('check_source', task['source'])
                if not Path(task['source']).is_dir():
                    raise FileNotFoundError(task['source'])
                # DirEntry type data avoids an extra remote lstat for each directory.
                # Some roots contain over a million immediate child directories.
                stack = [(task['source'], None)]
                while stack:
                    root, pending_entry = stack.pop()
                    if pending_entry is not None:
                        activity('classify_entry', root)
                        if not pending_entry.is_dir(follow_symlinks=False):
                            # Ancestor files/exclusions precede the durable cursor.
                            # Do not follow directory symlinks while seeking it.
                            continue
                    activity('check_stop', self.base / 'STOP')
                    if (self.base / 'STOP').exists():
                        raise InterruptedError('STOP requested')
                    try:
                        activity('list_directory', root)
                        with os.scandir(root) as iterator:
                            entries = sorted(iterator, key=lambda e: e.name)
                    except OSError as e:
                        exclude(root, str(e))
                        continue
                    relative_root = Path(root).relative_to(task['source']).parts
                    ancestor = (resume_directory is not None and len(relative_root) < len(resume_directory)
                                and resume_directory[:len(relative_root)] == relative_root)
                    if ancestor:
                        # All files in this ancestor were committed before its
                        # children. Seek the cursor branch without lstat-ing every
                        # sibling up front (some roots have millions of entries).
                        branch = resume_directory[len(relative_root)]
                        stack.extend((entry.path, entry) for entry in reversed(entries)
                                     if entry.name >= branch and not
                                     (root == task['source'] and entry.name == task.get('exclude_child')))
                        continue
                    dirs = []
                    for entry in entries:
                        activity('classify_entry', entry.path)
                        if root == task['source'] and entry.name == task.get('exclude_child'):
                            continue
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                dirs.append((entry.path, None))
                                continue
                            if relative_root == resume_directory and entry.name <= resume_name:
                                continue
                            if entry.is_symlink() and entry.is_dir():
                                exclude(entry.path, 'directory symlink (not followed)')
                                continue
                        except OSError as e:
                            exclude(entry.path, str(e))
                            continue
                        p = Path(entry.path)
                        try:
                            activity('stat_file', p)
                            s = entry.stat()
                            if not stat.S_ISREG(s.st_mode):
                                exclude(p, 'special file')
                                continue
                            if '\n' in str(p) or '\r' in str(p):
                                exclude(p, 'newline in filename')
                                continue
                            if paths and batch_bytes + s.st_size > self.config.get('chunk_bytes', 2*1024**3):
                                flush()
                            paths.append(str(p))
                            sizes.append(s.st_size)
                            batch_bytes += s.st_size
                            if len(paths) >= self.config.get('chunk_files', 1000):
                                flush()
                        except InterruptedError:
                            raise
                        except OSError as e:
                            exclude(p, str(e))
                    stack.extend(reversed(dirs))
                    del entries, dirs
                flush()
                self.scan_state(source, 'done', f'{errors} excluded entries' if errors else '')
                activity('done', task['source'])
            except Exception as e:
                self.scan_state(source, 'error', str(e))
                activity('error', self.scan_activity[source]['path'])

    def rpc(self, action, data):
        now = time.time()
        host = data['host']
        if host not in self.config['hosts']:
            raise ValueError('unknown host')
        with self.mutex, self.db:
            if action == 'health':
                return dict(ok=True, queued=self.db.execute("SELECT count(*) FROM tasks WHERE state='pending'").fetchone()[0],
                            queue_db=str(self.db_path))
            if action == 'heartbeat':
                node_metrics = dict(data.get('metrics', {}), active_task_ids=[t['id'] for t in data.get('active', [])])
                self.db.execute('INSERT OR REPLACE INTO nodes VALUES (?,?,?)',
                                (host, now, json.dumps(node_metrics)))
                accepted = []
                for t in data.get('active', []):
                    c = self.db.execute("UPDATE tasks SET heartbeat=?,progress=?,confirmed=1 WHERE id=? AND token=? AND owner=? AND state='running'",
                                        (now, json.dumps(t.get('progress', {})), t['id'], t['token'], host))
                    if c.rowcount:
                        accepted.append(t['id'])
                return dict(accepted=accepted, stop=(self.base / 'STOP').exists())
            if action == 'claim':
                if (self.base / 'STOP').exists():
                    return dict(stop=True)
                if now < self.config.get('ha_claims_after', 0):
                    return dict(wait=True, recovering=True)
                # Drain old clients after their current transfers finish, without killing uploads.
                if data.get('protocol_version', 0) < self.config.get('minimum_worker_protocol', 0):
                    return dict(done=True)
                # Workers kill their process group well before this reclaim threshold.
                self.db.execute("UPDATE tasks SET state=CASE WHEN confirmed=1 AND attempt>=? THEN 'failed' ELSE 'pending' END,attempt=CASE WHEN confirmed=0 THEN max(0,attempt-1) ELSE attempt END,ready=?,progress='{}' WHERE state='running' AND heartbeat<?",
                                (self.config.get('attempts', 5), now, now-self.config.get('lease_seconds', 240)))
                request_id = data.get('request_id')
                if request_id:
                    row = self.db.execute('SELECT * FROM tasks WHERE request_id=?', (request_id,)).fetchone()
                    if row:
                        if row['owner'] != host:
                            raise ValueError('request belongs to a different host')
                        return self.assignment(row) if row['state']=='running' else dict(expired=True)
                if self.db.execute("SELECT count(*) FROM tasks WHERE owner=? AND state='running'", (host,)).fetchone()[0] >= self.config.get('slots', 2):
                    return dict(wait=True)
                ids = list(self.sources)
                for _ in ids:
                    source = ids[self.cursor % len(ids)]
                    self.cursor += 1
                    row = self.db.execute("SELECT * FROM tasks WHERE state='pending' AND source=? AND ready<=? ORDER BY bytes DESC,id LIMIT 1", (source, now)).fetchone()
                    if row:
                        token = uuid.uuid4().hex
                        self.db.execute("UPDATE tasks SET state='running',owner=?,token=?,heartbeat=?,attempt=attempt+1,progress='{}',request_id=?,confirmed=0 WHERE id=?", (host, token, now, request_id, row['id']))
                        return self.assignment(self.db.execute('SELECT * FROM tasks WHERE id=?', (row['id'],)).fetchone())
                unfinished = self.db.execute("SELECT count(*) FROM tasks WHERE state IN ('pending','running')").fetchone()[0]
                scanning = self.db.execute("SELECT count(*) FROM scans WHERE state NOT IN ('done','error')").fetchone()[0]
                return dict(done=not unfinished and not scanning)
            if action == 'finish':
                row = self.db.execute('SELECT * FROM tasks WHERE id=? AND owner=? AND token=?', (data['id'], host, data['token'])).fetchone()
                if not row:
                    return dict(accepted=False)
                if row['state'] != 'running':
                    return dict(accepted=True)
                r = data['result']
                ok = successful(r) and r.get('handled', 0) == row['files']
                state = 'done' if ok else ('failed' if row['attempt'] >= self.config.get('attempts', 5) else 'pending')
                self.db.execute('UPDATE tasks SET state=?,result=?,progress=?,finished=?,ready=? WHERE id=?',
                                (state, json.dumps(r), json.dumps(r), now, now + min(1800, 30*2**row['attempt']), row['id']))
                return dict(accepted=True, state=state)
            raise ValueError('unknown action')

    def snapshot(self):
        now = time.time()
        with self.mutex:
            groups = [dict(r) for r in self.db.execute('SELECT source,state,count(*) AS chunks,sum(files) AS files,sum(bytes) AS bytes FROM tasks GROUP BY source,state')]
            scans = [dict(r) for r in self.db.execute('SELECT * FROM scans')]
            nodes = {r['host']: dict(updated=r['updated'], **json.loads(r['metrics'])) for r in self.db.execute('SELECT * FROM nodes')}
            active = [dict(r) for r in self.db.execute("SELECT id,source,owner,heartbeat,files,bytes,progress,attempt FROM tasks WHERE state='running'")]
            completed = [dict(r) for r in self.db.execute("SELECT owner,count(*) AS chunks,sum(files) AS files,sum(bytes) AS bytes FROM tasks WHERE state='done' GROUP BY owner")]
            last_completed = self.db.execute("SELECT max(finished) FROM tasks WHERE state='done'").fetchone()[0]
            counters = {}
            for r in self.db.execute("SELECT result FROM tasks WHERE state='done'"):
                result = json.loads(r[0])
                for key in ['succeeded','skipped','succeeded_bytes','skipped_bytes']:
                    counters[key] = counters.get(key, 0) + result.get(key, 0)
        for t in active:
            t['progress'] = json.loads(t['progress'])
        leased_chunks = len(active)
        # A committed but unanswered claim is a lease, not an executing transfer.
        active = [t for t in active if t['id'] in nodes.get(t['owner'],{}).get('active_task_ids',[])
                  and now-nodes[t['owner']]['updated'] < 90]
        for host in self.config['hosts']:
            n = nodes.setdefault(host, dict(updated=0))
            n['active_slots'] = sum(t['owner'] == host for t in active)
            n['healthy'] = now-n['updated'] < 90
        unfinished = any(r['state'] in ['pending','running'] for r in groups)
        scanning = any(s['state'] not in ['done','error'] for s in scans)
        issues = any(r['state'] == 'failed' for r in groups) or any(s['error'] for s in scans)
        state = 'running' if unfinished or scanning else ('finished_with_issues' if issues else 'finished')
        if (self.base / 'STOP').exists():
            state = 'stopped'
        stalled_scanners = [s['source'] for s in scans if s['state'] == 'scanning'
                            and s['source'] in self.scan_activity
                            and self.scan_activity[s['source']]['operation'] != 'queue_backpressure'
                            and now-self.scan_activity[s['source']]['updated'] > self.config.get('scan_stall_seconds', 180)]
        recovery, recovery_age, recovery_stale = (self.recovery_monitor.get() if self.recovery_monitor
                                                   else (None, None, False))
        return dict(updated=now, state=state, scan_complete=not scanning, groups=groups, scans=scans,
                    recovery=recovery, recovery_status_age_seconds=recovery_age,
                    recovery_status_stale=recovery_stale,
                    scanner_activity=dict(self.scan_activity),
                    last_completed_at=last_completed, upload_idle_seconds=now-last_completed if last_completed else None,
                    waiting_for_scan=scanning and not unfinished, stalled_scanners=stalled_scanners,
                    nodes=nodes, active=active, leased_chunks=leased_chunks, completed_by_node=completed, counters=counters,
                    discovered_files=sum(r['files'] for r in groups), discovered_bytes=sum(r['bytes'] for r in groups),
                    completed_files=sum(r['files'] for r in groups if r['state']=='done'),
                    completed_bytes=sum(r['bytes'] for r in groups if r['state']=='done'))


def coordinator(config):
    faulthandler.register(signal.SIGUSR1, all_threads=True)
    base = Path(config['run_dir'])
    lock = (base / 'coordinator.lock').open('a+')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    leadership = None
    if config.get('ha_enabled'):
        host = config['coordinator_host']
        previous, restored = restore_for_leadership(config, host,locked_owner(lock,config))
        leadership = Leadership(config, host, previous, restored,lock=lock)
        config = dict(config, ha_claims_after=leadership.claims_after)
    queue = Queue(config)
    token = (base / 'token').read_text().strip()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            if leadership and not leadership.healthy():
                self.send_error(503, 'Leadership heartbeat expired')
                return
            if self.headers.get('Authorization') != 'Bearer ' + token:
                self.send_error(403)
                return
            try:
                length = int(self.headers.get('Content-Length', 0))
                if not 0 < length <= 1024*1024:
                    raise ValueError('invalid body')
                data = json.loads(self.rfile.read(length))
                result = queue.rpc(self.path.strip('/'), data)
                payload = json.dumps(result).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:
                print('RPC error:', type(e).__name__, str(e), flush=True)
                self.send_error(500)

    port = urllib.parse.urlsplit(leadership.endpoint).port if leadership else config['port']
    server = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    if leadership:
        leadership.start()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    def mirror_loop():
        while True:
            try:
                queue.mirror()
            except Exception as e:
                print('queue mirror:', type(e).__name__, str(e), flush=True)
            time.sleep(config.get('queue_mirror_seconds', 30))
    if queue.db_path != base/'queue.sqlite':
        threading.Thread(target=mirror_loop, daemon=True).start()
    for task in config['tasks']:
        threading.Thread(target=queue.scan, args=(task,), daemon=True).start()
    while True:
        s = queue.snapshot()
        save(base / 'status.json', s)
        lines = ['# 16 节点 KS3 动态上传', '', time.strftime('%F %T %z'), '',
                 f"状态：{s['state']}；扫描{'完成' if s['scan_complete'] else '中（总量仍增长）'}。",
                 f"已完成文件：{s['completed_files']:,} / 已发现 {s['discovered_files']:,}。",
                 f"已完成检查及补传：{s['completed_bytes']/1024**4:.3f} TiB / 已发现 {s['discovered_bytes']/1024**4:.3f} TiB。",
                 '完成量含增量跳过；活动分片单独列出，失败尝试不累加。扫描结束前覆盖率仅针对已发现范围。', '',
                 '| 节点 | 在线 | 活动槽位 | 宿主机负载 1m | 容器匿名内存 % |', '|---|---|---|---|---|']
        if s['waiting_for_scan']:
            lines.insert(5, f"注意：上传队列为空，正在等待扫描供给；距最后完成约 {(s['upload_idle_seconds'] or 0)/60:.1f} 分钟。")
        for host, n in s['nodes'].items():
            lines.append(f"| {host} | {n['healthy']} | {n['active_slots']}/{config['slots']} | {n.get('load1',0):.2f} | {n.get('memory_used_pct',0):.1f} |")
        lines += ['', '## 扫描与排除', '']
        lines += [f"- {x['source']}: {x['state']} {x['error']}" for x in s['scans']]
        lines += ['', '## 扫描器当前位置', '']
        lines += [f"- {name}: {v['operation']}，{max(0,s['updated']-v['updated']):.0f} 秒前更新，`{v['path']}`"
                  for name,v in s['scanner_activity'].items()]
        lines += ['', '## 路径映射', '']
        lines += [f"- `{t['source']}` → `{t['destination']}`" for t in config['tasks']]
        temp = base / 'DASHBOARD.md.tmp'
        temp.write_text('\n'.join(lines)+'\n')
        os.replace(temp, base / 'DASHBOARD.md')
        time.sleep(config.get('coordinator_publish_seconds',30))


def cgroup_metrics(base=Path('/sys/fs/cgroup')):
    """Container limits and counters, without mistaking host RAM/CPUs for pod resources."""
    result = {}
    try:
        limit = (base/'memory.max').read_text().strip()
        current = int((base/'memory.current').read_text())
        stats = dict(line.split() for line in (base/'memory.stat').read_text().splitlines())
        events = dict(line.split() for line in (base/'memory.events').read_text().splitlines())
        result.update(memory_current_GiB=current/1024**3, memory_anon_GiB=int(stats.get('anon',0))/1024**3,
                      memory_file_cache_GiB=int(stats.get('file',0))/1024**3,
                      oom_events=int(events.get('oom',0)), oom_kill_events=int(events.get('oom_kill',0)))
        if limit != 'max':
            result.update(memory_limit_GiB=int(limit)/1024**3,
                          memory_used_pct=100*int(stats.get('anon',0))/max(1,int(limit)),
                          memory_total_pct=100*current/max(1,int(limit)))
        quota, period = (base/'cpu.max').read_text().split()
        if quota != 'max':
            result['cpus'] = min(len(os.sched_getaffinity(0)),int(quota)/int(period))
        cpu = dict(line.split() for line in (base/'cpu.stat').read_text().splitlines())
        result['cpu_usage_seconds'] = int(cpu.get('usage_usec',0))/1e6
        result['cpu_throttled_seconds'] = int(cpu.get('throttled_usec',0))/1e6
    except (OSError, ValueError):
        pass
    return result


def metrics():
    mem = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        fields = line.split()
        mem[fields[0].rstrip(':')] = int(fields[1])
    net_rx = net_tx = 0
    for line in Path('/proc/net/dev').read_text().splitlines()[2:]:
        name, values = line.split(':')
        if name.strip() != 'lo':
            v = values.split()
            net_rx += int(v[0]); net_tx += int(v[8])
    result = dict(load1=os.getloadavg()[0], cpus=len(os.sched_getaffinity(0)),
                  memory_used_pct=100*(1-mem['MemAvailable']/mem['MemTotal']), net_rx_bytes=net_rx, net_tx_bytes=net_tx)
    result.update(cgroup_metrics())
    return result


def worker(config, host):
    base = Path(config['local_root'])
    base.mkdir(parents=True, exist_ok=True)
    lock = (base / 'worker.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    shared = Path(config['run_dir'])
    token = (shared / 'token').read_text().strip()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    discovery = Discovery(config)
    active = {}
    mutex = threading.RLock()
    stop = threading.Event()
    last_contact = [time.monotonic()]

    def rpc(action, data):
        data = dict(data, host=host, protocol_version=3 if config.get('ha_enabled') else 2)
        req = urllib.request.Request(discovery.get()+'/'+action, json.dumps(data).encode(),
                                     {'Authorization': 'Bearer '+token, 'Content-Type': 'application/json'})
        try:
            with opener.open(req, timeout=10) as response:
                return json.load(response)
        except Exception:
            discovery.get(force=True)
            raise

    def terminate(child):
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()

    def heartbeat():
        while not stop.is_set():
            try:
                with mutex:
                    entries = [dict(id=k, token=v['token'], progress=parse_progress(tail(v['log']))) for k,v in active.items()]
                r = rpc('heartbeat', dict(active=entries, metrics=metrics()))
                last_contact[0] = time.monotonic()
                if r.get('stop'):
                    stop.set()
                accepted = set(r['accepted'])
                with mutex:
                    rejected = [v['child'] for k,v in active.items() if k not in accepted and k in {t['id'] for t in entries}]
                for child in rejected:
                    terminate(child)
            except Exception as e:
                print('heartbeat:', type(e).__name__, flush=True)
            stop.wait(config.get('heartbeat_seconds', 15))

    def slot(index):
        request_id = uuid.uuid4().hex
        while not stop.is_set():
            task = None
            try:
                r = rpc('claim', dict(request_id=request_id))
                if r.get('expired'):
                    request_id = uuid.uuid4().hex
                    continue
                if r.get('stop') or r.get('done'):
                    return
                task = r.get('task')
                if not task:
                    stop.wait(5)
                    continue
                folder = base / task['id']
                folder.mkdir(exist_ok=True)
                log = folder / 'stdout.log'
                args = upload_command(task['config'], task['manifest'], folder)
                save(folder / 'command.json', args)
                with log.open('wb', buffering=0) as output:
                    guarded = [sys.executable, '-S', '-B', '-m', 'ks_cloud.exec_child', str(os.getpid())] + args
                    child = subprocess.Popen(guarded, stdin=subprocess.DEVNULL, stdout=output, stderr=output, start_new_session=True)
                    with mutex:
                        active[task['id']] = dict(token=task['token'], child=child, log=log)
                    previous = None
                    last_change = time.monotonic()
                    stalled = False
                    while child.poll() is None:
                        if stop.is_set() or (shared/'STOP').exists() or time.monotonic()-last_contact[0] > config.get('worker_disconnect_seconds', 90):
                            terminate(child)
                            break
                        p = parse_progress(tail(log))
                        key = (p.get('handled'), p.get('bytes'))
                        if key != previous:
                            last_change = time.monotonic()
                            previous = key
                        if time.monotonic()-last_change > config.get('stall_seconds', 1800):
                            stalled = True
                            terminate(child)
                            break
                        stop.wait(1)
                    result = parse_progress(tail(log))
                    result.update(exit_code=child.returncode, log=str(log), host=host, stalled=stalled)
                    save(folder / 'result.json', result)
                # Keep heartbeating this lease until its result has been acknowledged.
                while not stop.is_set():
                    try:
                        rpc('finish', dict(id=task['id'], token=task['token'], result=result))
                        break
                    except Exception:
                        if time.monotonic()-last_contact[0] > config.get('worker_disconnect_seconds', 90):
                            break
                        stop.wait(5)
            except Exception as e:
                print('slot', index, type(e).__name__, str(e), flush=True)
                stop.wait(5)
            finally:
                if task:
                    with mutex:
                        entry = active.pop(task['id'], None)
                    if entry:
                        terminate(entry['child'])
                    request_id = uuid.uuid4().hex

    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    threading.Thread(target=heartbeat, daemon=True).start()
    with concurrent.futures.ThreadPoolExecutor(max_workers=config['slots']) as pool:
        list(pool.map(slot, range(config['slots'])))
    stop.set()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['coordinator','worker'])
    parser.add_argument('--config', required=True)
    parser.add_argument('--host')
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    if args.mode == 'coordinator':
        if config.get('ha_enabled'):
            config = dict(config, coordinator_host=args.host or config['hosts'][0])
        coordinator(config)
    else:
        if args.host not in config['hosts']:
            parser.error('host must be a configured node')
        worker(config, args.host)


if __name__ == '__main__':
    main()
