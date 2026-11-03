"""Single-node batch uploads with bounded logs and an overwritten Markdown dashboard."""
import argparse
from collections import deque
import fcntl
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import signal
import subprocess
import threading
import time

from .common import parse_progress, save, upload_command


def human(n):
    for u in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if n < 1024 or u == 'PiB':
            return f'{n:.2f} {u}'
        n /= 1024


def duration(seconds):
    if seconds is None:
        return '估算中'
    return f'{seconds / 3600:.1f} h' if seconds >= 3600 else f'{seconds / 60:.0f} min'


def run(config_path):
    config = json.loads(Path(config_path).read_text())
    base = Path(config['local_state_dir'])
    base.mkdir(parents=True, exist_ok=True)
    lock = (base / 'lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    workspace = Path(config['workspace'])
    stop = threading.Event()
    children = {}
    states = {}
    previous = base / 'status.json'
    prior = json.loads(previous.read_text()).get('tasks', {}) if previous.exists() else {}
    for task in config['tasks']:
        old = prior.get(task['id'], {})
        states[task['id']] = dict(old, state='finished' if old.get('state') == 'finished' and old.get('failed', 0) == 0 else 'pending')

    def on_stop(signum, frame):
        stop.set()
        for child in list(children.values()):
            if child.poll() is None:
                child.terminate()
    signal.signal(signal.SIGTERM, on_stop)
    signal.signal(signal.SIGINT, on_stop)

    def inventory():
        # Metadata only; one thread, throttled to avoid aggressive parallel source scans.
        for task in config['tasks']:
            if stop.is_set():
                return
            state = states[task['id']]
            if state.get('inventory_complete'):
                continue
            files = dirs = size = links = 0
            errors = []
            def error(e):
                if len(errors) < 10:
                    errors.append(str(e))
            for root, names, filenames in os.walk(task['source'], followlinks=False, onerror=error):
                if stop.is_set():
                    return
                if task.get('exclude_child') and Path(root) == Path(task['source']):
                    names[:] = [n for n in names if n != task['exclude_child']]
                for name in names[:]:
                    p = Path(root) / name
                    if p.is_symlink():
                        names.remove(name)
                        links += 1
                    else:
                        dirs += 1
                for name in filenames:
                    try:
                        p = Path(root) / name
                        if p.is_file():
                            size += p.stat().st_size
                            files += 1
                        else:
                            links += 1
                    except OSError as exc:
                        error(exc)
                state.update(inventory_files=files, inventory_dirs=dirs, inventory_bytes=size, inventory_errors=errors, inventory_links=links)
                time.sleep(0.002)
            state['inventory_complete'] = True
    threading.Thread(target=inventory, daemon=True).start()

    def execute(task):
        state = states[task['id']]
        folder = base / task['id']
        folder.mkdir(exist_ok=True)
        logger = logging.getLogger(task['id'])
        logger.setLevel(logging.INFO)
        log_folder = Path(config.get('local_log_dir', str(base))) / task['id']
        log_folder.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(log_folder / 'stdout.log', maxBytes=8*1024**2, backupCount=2)
        handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(handler)
        merged = dict(config, source=task['source'], destination=task['destination'])
        manifest = task.get('manifest')
        args = upload_command(merged, manifest or '', folder)
        if not manifest:
            pos = args.index('--object-file')
            del args[pos:pos+2]
        save(folder / 'command.json', args)
        state.update(state='uploading', started=time.time())
        recent = deque(maxlen=80)
        try:
            child = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            children[task['id']] = child
            state['pid'] = child.pid
            while True:
                raw = child.stdout.readline()
                if not raw:
                    break
                line = raw.decode(errors='replace').replace('\r', '')
                logger.info(line.rstrip())
                recent.append(line[-8192:])
                progress = parse_progress(''.join(recent))
                state.update(progress)
                speeds = re.findall(r'速度:([\d.]+)([KMGTP]?B)/s', line)
                if speeds:
                    from .common import UNITS
                    state['speed_Bps'] = float(speeds[-1][0]) * UNITS[speeds[-1][1]]
                state['heartbeat'] = time.time()
            rc = child.wait()
            state.update(exit_code=rc, finished=time.time(), state='stopped' if stop.is_set() else ('finished' if rc == 0 and state.get('failed') == 0 else 'failed'), speed_Bps=0)
            save(folder / 'result.json', state)
        except Exception as exc:
            state.update(state='failed', error=str(exc))
        finally:
            child = children.pop(task['id'], None)
            if child and child.poll() is None:
                child.terminate()
                child.wait(timeout=30)
            logger.removeHandler(handler)
            handler.close()

    threads = {}
    history = {}
    last_publish = 0
    while True:
        if (base / 'STOP').exists() or (workspace / 'STOP').exists():
            on_stop(None, None)
        for task in config['tasks']:
            if stop.is_set() or sum(t.is_alive() for t in threads.values()) >= config.get('max_active', 2):
                break
            if states[task['id']]['state'] == 'pending':
                thread = threading.Thread(target=execute, args=(task,), daemon=True)
                threads[task['id']] = thread
                states[task['id']]['state'] = 'starting'
                thread.start()
        now = time.time()
        all_terminal = all(s['state'] in ['finished', 'failed', 'stopped'] for s in states.values())
        if now-last_publish >= 30 or all_terminal or stop.is_set():
            last_publish = now
            rows = []
            total_bytes = handled_bytes = total_items = handled_items = 0
            for task in config['tasks']:
                s = states[task['id']]
                count = s.get('handled', 0)
                size = s.get('bytes', 0)
                # Terminal skip-only commands may omit a final progress bar.
                if s.get('state') == 'finished':
                    size = s.get('succeeded_bytes', 0) + s.get('skipped_bytes', 0)
                denom = max(s.get('reference_total', 0), s.get('total', 0), s.get('inventory_files', 0)+s.get('inventory_dirs', 0))
                byte_denom = max(s.get('reference_total_bytes', 0), s.get('total_bytes', 0), s.get('inventory_bytes', 0))
                total_bytes += byte_denom
                handled_bytes += size
                total_items += denom
                handled_items += count
                h = history.setdefault(task['id'], deque(maxlen=300))
                h.append((now, count, size))
                prior_sample = next((x for x in reversed(h) if now-x[0] >= 300), None)
                rate_b = max(0, (size-prior_sample[2])/(now-prior_sample[0])) if prior_sample else s.get('speed_Bps', 0)
                rate_f = max(0, (count-prior_sample[1])/(now-prior_sample[0])) if prior_sample else 0
                eta = max(0, byte_denom-size)/rate_b if rate_b > 0 else None
                if s['state'] == 'finished':
                    eta = 0
                s.update(recent_Bps=rate_b, recent_items_s=rate_f, eta_seconds=eta)
                pct = lambda x, y: f'{min(100,x/y*100):.2f}%' if y else '待扫描'
                file_label = f'{count:,}/{denom:,} ({pct(count,denom)})' if denom else '待扫描'
                byte_label = f'{human(size)}/{human(byte_denom)} ({pct(size,byte_denom)})' if byte_denom else '待扫描'
                rows.append(f'| {task["id"]} | {s["state"]} | {file_label} | {byte_label} | {human(rate_b)}/s; {rate_f:.1f} 项/s | {duration(eta)} | {s.get("failed",0)} |')
            inventory_done = all(s.get('inventory_complete') for s in states.values())
            active_speed = sum(s.get('recent_Bps',0) for s in states.values() if s['state']=='uploading')
            overall_eta = max(0,total_bytes-handled_bytes)/active_speed if inventory_done and active_speed>0 else None
            if all_terminal:
                overall_eta = 0
            report = ['# 单节点上传进度看板', '', time.strftime('更新：%Y-%m-%d %H:%M:%S %z'), '',
                      f'总处理项数：{handled_items:,}/{total_items:,}；数据量：{human(handled_bytes)}/{human(total_bytes)}。',
                      f'总量状态：{"清点完成" if inventory_done else "清点中，目前总量仅为已发现下限"}；近期合计速度：{human(active_speed)}/s；预计剩余：{duration(overall_eta)}。', '',
                      '文件维度使用工具处理项数（可能含目录）。分母随扫描增长；增量跳过也计入已处理，非内容哈希校验。',
                      '近期速度优先使用近5分钟差分，启动不足5分钟时为工具即时速度；ETA仅为趋势估计，待运行任务的单项ETA暂未知。',
                      '| 任务 | 状态 | 文件/目录项进度 | 数据量进度 | 近期速度 | 剩余时间 | 失败项 |',
                      '|---|---|---|---|---|---|---|', *rows, '', '## 路径映射', '']
            if config.get('recovery_baseline'):
                baseline = config['recovery_baseline']
                report += ['## 原节点最后进度（历史参考，不与本轮累加）', '', '已完成云端对象保留；本轮从0重新统计检查覆盖率，符合更新规则的对象跳过。旧节点未完成文件的分片断点尚未找回。', '', '| 任务 | 历史处理项数 | 历史处理数据量 | 原状态 |', '|---|---|---|---|']
                for name, old in baseline.items():
                    report.append(f'| {name} | {old.get("handled",0):,} | {human(old.get("bytes",0))} | {old.get("state","未知")} |')
                report += ['', '## 路径映射', '']
            for task in config['tasks']:
                report += [f'- **{task["id"]}**：`{task["source"]}` → `{task["destination"]}`' + (f'（清点排除子目录：{task["exclude_child"]}；上传范围以 manifest 为准）' if task.get('exclude_child') else '')]
            report += ['', '## 运行说明', '', f'- 单节点，同一时间最多 {config.get("max_active",2)} 个目录任务，每任务 jobs={config["jobs"]}、parallel={config["parallel"]}。', f'- 持久化状态、断点及失败清单：`{base}`；stdout本地目录：`{config.get("local_log_dir", str(base))}`，每任务 stdout 最多约24 MiB。', '- 本文及 status.json 均覆盖更新，不追加历史。停止：在本 run 下创建 STOP，恢复前移除 STOP 并重启控制器。']
            text = '\n'.join(report)+'\n'
            snapshot = dict(updated=now, tasks={k:dict(v) for k,v in states.items()}, all_terminal=all_terminal, inventory_complete=inventory_done)
            save(base/'status.json', snapshot)
            (base/'DASHBOARD.md').write_text(text)
            try:
                save(workspace/'status.json',snapshot)
                temp = workspace/'DASHBOARD.md.tmp'
                temp.write_text(text)
                os.replace(temp,workspace/'DASHBOARD.md')
            except OSError as exc:
                print('Dashboard mirror failed; local copy remains available:',exc,flush=True)
        if all_terminal or stop.is_set():
            for thread in threads.values():
                thread.join(timeout=35)
            break
        time.sleep(2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    run(parser.parse_args().config)
