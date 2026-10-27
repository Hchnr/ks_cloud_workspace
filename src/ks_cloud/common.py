import json
import os
from pathlib import Path
import re
import tempfile

UNITS = {u: 1024 ** i for i, u in enumerate(('B', 'KB', 'MB', 'GB', 'TB', 'PB'))}


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as out:
            json.dump(value, out, ensure_ascii=False, indent=2)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def tail(path):
    with Path(path).open('rb') as stream:
        stream.seek(0, 2)
        stream.seek(max(0, stream.tell() - 65536))
        return stream.read().decode(errors='replace')


def parse_progress(text):
    result = {}
    matches = list(re.finditer(
        r'完成数:(\d+)/(\d+).*?完成大小:([\d.]+)([KMGTP]?B)/([\d.]+)([KMGTP]?B)', text))
    if matches:
        count, total, size, unit, total_size, total_unit = matches[-1].groups()
        result.update(handled=int(count), total=int(total),
                      bytes=float(size) * UNITS[unit], total_bytes=float(total_size) * UNITS[total_unit])
    for label, key in [('Succeed', 'succeeded'), ('Skip', 'skipped'), ('Failed', 'failed')]:
        values = re.findall(label + r' count:\s*(\d+)', text)
        if values:
            result[key] = int(values[-1])
        sizes = re.findall(label + r' bytes:\s*([\d.]+)([KMGTP]?B)', text)
        if sizes:
            size, unit = sizes[-1]
            result[key + '_bytes'] = float(size) * UNITS[unit]
    if 'succeeded' in result and 'failed' in result:
        result.setdefault('skipped', 0)
        result['handled'] = sum(result.get(k, 0) for k in ('succeeded', 'skipped', 'failed'))
    return result


def successful(result):
    return result.get('exit_code') == 0 and result.get('failed') == 0 and 'succeeded' in result


def upload_command(config, manifest, folder):
    folder = Path(folder)
    args = [config['tool'], 'cp', config['source'].rstrip('/') + '/', config['destination'],
            '--config-file', config['config_file'], '--language', 'CH', '--object-file', str(manifest),
            '--recursive', '--update', '--update-rule', '4', '--force',
            '--jobs', str(config.get('jobs', 8)), '--parallel', str(config.get('parallel', 1)),
            '--list-parallel', str(config.get('list_parallel', 2)), '--part-size', '32MB',
            '--retry-times', '10', '--connect-timeout', '15', '--read-timeout', '60',
            '--pb-output-mode', 'newline', '--pb-output-interval', '30',
            '--loglevel', 'error', '--output-item', 'failed,warned']
    for flag, name in [('--checkpoint-dir', 'checkpoint'), ('--output-dir', 'output'), ('--log-path', 'logs')]:
        path = folder / name
        path.mkdir(parents=True, exist_ok=True)
        args += [flag, str(path)]
    for pattern in config.get('exclude', []):
        args += ['--exclude', pattern]
    return args
