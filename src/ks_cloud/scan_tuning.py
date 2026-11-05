"""Validate and atomically edit the coordinator's hot scan settings."""
import argparse
import fcntl
import json
from pathlib import Path

from .common import save
from .scan_control import validate_settings


def update(path, changes):
    path = Path(path)
    # Never replace this lock inode; multiple operators serialize on the same lock.
    with path.with_suffix(path.suffix+'.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        current = json.loads(path.read_text()) if path.exists() else {}
        settings = validate_settings(dict(current, **changes))
        save(path, settings)
        return settings


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, help='Active coordinator config (not credentials)')
    parser.add_argument('--set', action='append', default=[], metavar='KEY=JSON',
                        help='For example enabled=true, workers=2, auto=false; repeatable')
    args = parser.parse_args(argv)
    try:
        config = json.loads(Path(args.config).read_text())
        path = Path(config['scan_tuning_file'])
        changes = {}
        for expression in args.set:
            key, value = expression.split('=',1)
            if key in changes:
                raise ValueError('duplicate field: '+key)
            changes[key] = json.loads(value)
        settings = update(path, changes) if changes else validate_settings(json.loads(path.read_text()))
    except (OSError, ValueError, TypeError, KeyError) as error:
        parser.error(str(error))
    print(json.dumps(dict(path=str(path), settings=settings), indent=2))


if __name__ == '__main__':
    main()
