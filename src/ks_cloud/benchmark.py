"""A bounded per-node read/PUT probe; emit JSON, never count failed bytes as throughput."""
import argparse
import concurrent.futures
import datetime
import json
import mmap
import os
from pathlib import Path
import random
import re
import signal
import socket
import subprocess
import time

from .common import save


def disk(args):
    block = 1024 * 1024 if args.mode == 'disk-throughput' else 4096
    if not hasattr(os, 'O_DIRECT'):
        raise RuntimeError('O_DIRECT unavailable; refusing to silently benchmark page cache')
    files = [Path(p).resolve() for p in args.read_file]
    if not files or any(not p.is_file() or p.stat().st_size < 64 * block for p in files):
        raise ValueError('Supply existing regular files of at least 64 blocks; preferably multiple large files')
    start = time.monotonic()
    deadline = start + args.seconds

    def reader(index):
        count = errors = 0
        first_error = None
        rng = random.Random(index + args.seed)
        path = files[index % len(files)]
        fd = os.open(path, os.O_RDONLY | os.O_DIRECT)
        buf = mmap.mmap(-1, block)
        slots = os.fstat(fd).st_size // block
        offset = (args.region * 1024 + index * 64) % slots
        try:
            while time.monotonic() < deadline:
                if args.mode == 'disk-iops':
                    offset = rng.randrange(slots)
                try:
                    read = os.preadv(fd, [buf], offset * block)
                    if read != block:
                        raise OSError('short read')
                    count += 1
                except OSError as exc:
                    errors += 1
                    first_error = str(exc)
                    break
                offset = (offset + 1) % slots
        finally:
            buf.close()
            os.close(fd)
        return dict(operations=count, bytes=count * block, errors=errors, first_error=first_error)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        records = list(pool.map(reader, range(args.concurrency)))
    elapsed = time.monotonic() - start
    return dict(seconds=elapsed, block_bytes=block, operations=sum(r['operations'] for r in records),
                completed_bytes=sum(r['bytes'] for r in records), errors=sum(r['errors'] for r in records), readers=records,
                cache_policy='O_DIRECT bypasses client page cache; server-side cache is uncontrolled')


def cloud(args):
    if not args.tool or not args.config_file or not args.cloud_prefix:
        raise ValueError('Cloud probes require --tool, --config-file and --cloud-prefix')
    prefix = args.cloud_prefix.rstrip('/')
    if not prefix.startswith('ks3://') or '/_diagnostics/' not in prefix:
        raise ValueError('Test destination must be a dedicated KS3 /_diagnostics/ prefix')
    folder = Path(args.output).resolve().parent / (Path(args.output).stem + '-files')
    folder.mkdir(parents=True, exist_ok=False)
    source = folder / 'source'
    source.mkdir()
    large = args.mode == 'ks3-throughput'
    size = args.object_mib * 1024**2 if large else 4096
    count = args.objects if args.objects is not None else (4 if large else 512)
    payload = os.urandom(min(size, 8 * 1024**2))
    for i in range(count):
        with (source / f'object-{i:06d}.bin').open('wb') as stream:
            left = size
            while left:
                block = payload[:min(left, len(payload))]
                stream.write(block)
                left -= len(block)
    # Unique destination on every trial: no --update and no reused objects.
    destination = prefix + '/' + args.label + '/' + socket.gethostname() + '/'
    output = folder / 'output'
    output.mkdir()
    for d in ['checkpoint', 'logs']:
        (folder / d).mkdir()
    command = [args.tool, 'cp', str(source) + '/', destination, '--config-file', args.config_file,
               '--recursive', '--force', '--jobs', str(args.concurrency), '--parallel', str(args.part_parallel),
               '--part-size', '32MB', '--retry-times', '1', '--connect-timeout', '5', '--read-timeout', '15',
               '--language', 'CH', '--pb-output-mode', 'newline', '--pb-output-interval', '5',
               '--output-item', 'succeeded,failed,warned', '--output-dir', str(output),
               '--checkpoint-dir', str(folder / 'checkpoint'), '--log-path', str(folder / 'logs'), '--loglevel', 'error']
    save(folder / 'command.json', command)
    start = time.monotonic()
    timed_out = False
    with (folder / 'stdout.log').open('wb') as log:
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        try:
            code = child.wait(timeout=args.cloud_timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            child.terminate()
            try:
                code = child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                code = child.wait()
    elapsed = time.monotonic() - start
    successful = set()
    for path in output.glob('*succeed*'):
        for line in path.open(errors='replace'):
            match = re.search(r'(object-\d{6}\.bin) --> ks3://', line)
            if match:
                successful.add(match[1])
    # Terminal Succeed bytes can contain bytes from incomplete failed objects; never use it here.
    return dict(seconds=elapsed, operations=len(successful), completed_bytes=len(successful) * size,
                attempted_objects=count, object_bytes=size, failed_or_unconfirmed_objects=count-len(successful),
                returncode=code, timeout=timed_out, destination=destination,
                errors=count-len(successful), confirmation='unique file entries in cp_succeed manifest',
                local_source=str(source), log=str(folder / 'stdout.log'))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=['disk-throughput', 'disk-iops', 'ks3-throughput', 'ks3-iops'], required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--label', required=True, help='Unique trial label, e.g. rep1-single-c4')
    p.add_argument('--concurrency', type=int, default=4)
    p.add_argument('--seconds', type=int, default=20)
    p.add_argument('--read-file', action='append', default=[])
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--region', type=int, default=0, help='Different sequential read starting region per node')
    p.add_argument('--tool')
    p.add_argument('--config-file')
    p.add_argument('--cloud-prefix')
    p.add_argument('--objects', type=int)
    p.add_argument('--object-mib', type=int, default=256)
    p.add_argument('--part-parallel', type=int, default=2)
    p.add_argument('--cloud-timeout', type=int, default=90)
    args = p.parse_args()
    if not 1 <= args.concurrency <= 64 or not 1 <= args.seconds <= 60:
        p.error('Concurrency must be 1..64, duration 1..60 seconds')
    if not 1 <= args.object_mib <= 1024 or (args.objects is not None and not 1 <= args.objects <= 4096):
        p.error('Invalid object size/count')
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    if Path(args.output).exists():
        p.error('Output already exists: choose a fresh trial label/path')
    # Terminate even if a filesystem operation hangs; caller treats missing JSON as a failed trial.
    signal.alarm(args.seconds + 45 if args.mode.startswith('disk') else args.cloud_timeout + 30)
    result = dict(hostname=socket.gethostname(), mode=args.mode, label=args.label,
                  concurrency=args.concurrency, started=datetime.datetime.now(datetime.timezone.utc).isoformat())
    try:
        result.update(disk(args) if args.mode.startswith('disk') else cloud(args))
        result.update(MiB_s=result['completed_bytes']/result['seconds']/1024**2,
                      IOPS=result['operations']/result['seconds'])
    except Exception as exc:
        result['error'] = str(exc)
    finally:
        signal.alarm(0)
    save(args.output, result)
    print(json.dumps(result, ensure_ascii=False))
    if result.get('error'):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
