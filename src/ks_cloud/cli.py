"""Prepare, deploy and monitor resumable KS3 upload plans."""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from .common import save


def ssh(host, code, timeout=30):
    return subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', host,
                           'python3 -'], input=code, capture_output=True, text=True, timeout=timeout)


def distribute(config, tasks):
    hosts = config['hosts']
    if not hosts or len(set(hosts)) != len(hosts):
        raise ValueError('hosts must be nonempty and unique')
    if len({t['id'] for t in tasks}) != len(tasks):
        raise ValueError('duplicate task IDs')
    bins = [[] for _ in hosts]
    loads = [0] * len(hosts)
    for task in sorted(tasks, key=lambda t: t['weight'], reverse=True):
        index = min(range(len(hosts)), key=lambda i: loads[i])
        bins[index].append(task)
        loads[index] += task['weight']
    return {'config': config, 'nodes': [dict(host=h, tasks=t, reference=w) for h, t, w in zip(hosts, bins, loads)]}


def prepare(args):
    config = json.loads(Path(args.config).read_text())
    for key in ['source', 'tool', 'config_file', 'remote_run_dir']:
        if not Path(config[key]).is_absolute():
            raise ValueError(key + ' must be absolute')
    if not config['destination'].startswith('ks3://'):
        raise ValueError('destination must start with ks3://')
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    tasks = []
    if args.legacy:
        old = Path(args.legacy).resolve()
        records = {}
        for bucket in ('pending', 'running', 'done', 'failed'):
            for path in (old / 'queue' / bucket).glob('*.json'):
                record = json.loads(path.read_text())
                records[record['id']] = record
        expected = json.loads((old / 'plan.json').read_text())['tasks']
        if len(records) != expected:
            raise ValueError('legacy task inventory is incomplete')
        for record in records.values():
            manifest = Path(record['manifest'])
            if not manifest.is_file():
                raise ValueError('missing manifest: ' + str(manifest))
            tasks.append(dict(id=record['id'], manifest=str(manifest), weight=max(1, record.get('progress', {}).get('handled', record.get('entries', 1)))))
    else:
        source = Path(config['source'])
        if not source.is_dir():
            raise ValueError('source directory does not exist')
        if args.chunk_size < 1:
            raise ValueError('chunk size must be positive')
        batch = []
        excluded = (output / 'excluded.jsonl').open('w')

        def flush():
            if not batch:
                return
            tid = f'task-{len(tasks):05d}'
            manifest = output / (tid + '.txt')
            manifest.write_text(''.join(str(p) + '\n' for p in batch))
            tasks.append(dict(id=tid, manifest=str(manifest), weight=len(batch)))
            batch.clear()

        try:
            for root, directories, files in os.walk(source, followlinks=False, onerror=lambda e: (_ for _ in ()).throw(e)):
                for name in directories[:]:
                    p = Path(root) / name
                    if p.is_symlink():
                        excluded.write(json.dumps({'path': str(p), 'reason': 'directory symlink'}) + '\n')
                        directories.remove(name)
                for name in files:
                    p = Path(root) / name
                    if '\n' in str(p) or '\r' in str(p):
                        raise ValueError('newline in source path: ' + repr(str(p)))
                    if not p.is_file():
                        excluded.write(json.dumps({'path': str(p), 'reason': 'missing target or special file'}) + '\n')
                        continue
                    batch.append(p)
                    if len(batch) >= args.chunk_size:
                        flush()
            flush()
        finally:
            excluded.close()
    save(output / 'plan.json', distribute(config, tasks))
    print(f'Prepared {len(tasks)} tasks; review {output / "plan.json"} before launch')


def launch(args):
    path = Path(args.plan).resolve()
    plan = json.loads(path.read_text())
    config = plan['config']
    package = {p.name: p.read_text() for p in Path(__file__).parent.glob('*.py')}

    def deploy(node):
        node_plan = dict(node, config=config)
        code = '''import fcntl,json,pathlib,subprocess
b=pathlib.Path(CONFIG['remote_run_dir']);b.mkdir(parents=True,exist_ok=True)
l=(b/'lock').open('a');fcntl.flock(l,fcntl.LOCK_EX|fcntl.LOCK_NB)
existing=b/'plan.json'
if existing.exists() and json.loads(existing.read_text())!=PLAN:
    raise RuntimeError('Run directory contains a different plan; use a new run directory')
p=b/'ks_cloud';p.mkdir(exist_ok=True)
for name,text in PACKAGE.items():(p/name).write_text(text)
existing.write_text(json.dumps(PLAN))
f=(b/'worker.log').open('ab',buffering=0)
l.close()
child=subprocess.Popen(['python3','-m','ks_cloud.worker','--run-dir',str(b)],cwd=b,stdin=subprocess.DEVNULL,stdout=f,stderr=f,start_new_session=True)
print(child.pid)
'''
        code = 'CONFIG=' + repr(config) + '\nPLAN=' + repr(node_plan) + '\nPACKAGE=' + repr(package) + '\n' + code
        try:
            result = ssh(node['host'], code)
            return dict(host=node['host'], returncode=result.returncode, pid=result.stdout.strip(), error=result.stderr)
        except (subprocess.TimeoutExpired, OSError) as error:
            return dict(host=node['host'], returncode=-1, pid='', error=str(error))

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(deploy, plan['nodes']))
    save(path.parent / 'launch.json', results)
    print(json.dumps(results, indent=2))
    if any(r['returncode'] for r in results):
        raise SystemExit(1)


def monitor(args):
    plan_path = Path(args.plan).resolve()
    plan = json.loads(plan_path.read_text())
    base = plan['config']['remote_run_dir']
    cache = {}
    history = []
    total = sum(n['reference'] for n in plan['nodes'])

    def read(node):
        try:
            r = ssh(node['host'], 'from pathlib import Path; print((Path(' + repr(base) + ')/"status.json").read_text())', timeout=12)
            if r.returncode == 0:
                return node['host'], json.loads(r.stdout)
        except (subprocess.TimeoutExpired, ValueError):
            pass
        return node['host'], cache.get(node['host'])

    try:
        while True:
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
                for host, status in pool.map(read, plan['nodes']):
                    if status:
                        cache[host] = status
            now = time.time()
            handled = sum(n.get('handled_completed', 0) + n.get('progress', {}).get('handled', 0) for n in cache.values())
            finished = len(cache) == len(plan['nodes']) and all(n['state'] == 'finished' for n in cache.values())
            failed = sum(n.get('failed_tasks', 0) for n in cache.values())
            history.append((now, handled))
            history = history[-120:]
            prior = next((p for p in reversed(history) if now - p[0] >= 300), None)
            rate = max(0, (handled - prior[1]) / (now - prior[0])) if prior else 0
            eta = '已结束' if finished else (f'{max(0, total-handled)/rate/3600:.1f}小时' if rate else '采样中')
            lines = [f'KS3 检查+补传 | {time.strftime("%F %T %z")}',
                     f'已处理 {handled:,}/{total:,} ({handled/total:.2%})' if total else '空任务计划',
                     f'近5分钟 {rate:.0f}项/s | ETA {eta} | 失败分片 {failed}',
                     '总量为计划参考项数；含跳过检查，非内容校验。节点百分比为当前分片。']
            for node in plan['nodes']:
                n = cache.get(node['host'])
                if not n:
                    lines.append(node['host'] + ' 状态不可读')
                    continue
                p = n.get('progress', {})
                pct = lambda a, b: f'{a/b:.1%}' if b else '待扫描'
                age = int(now - n['updated'])
                state = n['state'] if age < 120 or n['state'] == 'finished' else '心跳过期'
                lines.append(f'{node["host"]:<14} {state:<12} 分片{n.get("completed",0)}/{n["total_tasks"]} 文件{pct(p.get("handled",0),p.get("total",0))} 数据量{pct(p.get("bytes",0),p.get("total_bytes",0))} 失败{n.get("failed_tasks",0)}')
            text = '\n'.join(lines)
            save(plan_path.parent / 'progress.json', dict(updated=now, nodes=cache, handled=handled, total=total, finished=finished, failed_tasks=failed))
            (plan_path.parent / 'progress.txt').write_text(text + '\n')
            if sys.stdout.isatty() and not args.once:
                print('\033[2J\033[H', end='')
            print(text, flush=True)
            if args.once:
                return
            time.sleep(30)
    except KeyboardInterrupt:
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    p = commands.add_parser('prepare', help='Create manifests and a static assignment; no uploads')
    p.add_argument('--config', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--legacy', help='Reuse a legacy distributed directory and its manifests')
    p.add_argument('--chunk-size', type=int, default=5000)
    p.set_defaults(func=prepare)
    p = commands.add_parser('launch', help='Start or resume detached workers via SSH')
    p.add_argument('--plan', required=True)
    p.set_defaults(func=launch)
    p = commands.add_parser('monitor', help='Read node statuses without starting uploads')
    p.add_argument('--plan', required=True)
    p.add_argument('--once', action='store_true')
    p.set_defaults(func=monitor)
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
