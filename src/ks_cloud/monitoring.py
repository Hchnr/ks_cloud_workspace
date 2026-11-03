"""Read a dynamic run's snapshots without contacting upload nodes."""
import argparse
import json
from pathlib import Path
import sys
import time


def monitor(directory, once=False, interval=30):
    base = Path(directory)
    while True:
        try:
            report = (base / 'DASHBOARD.md').read_text()
            status = json.loads((base / 'status.json').read_text())
            if sys.stdout.isatty() and not once:
                print('\033[2J\033[H', end='')
            print(report, flush=True)
            if once or status['state'].startswith('finished'):
                return
        except FileNotFoundError:
            print('Waiting for coordinator snapshot:', base, flush=True)
            if once:
                return
        time.sleep(interval)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument('--run-dir', help='Dynamic run directory containing status.json')
    target.add_argument('--active', help='JSON pointer containing the active run_dir')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args(argv)
    directory = args.run_dir
    if args.active:
        try:
            directory = json.loads(Path(args.active).read_text())['run_dir']
            if not isinstance(directory, str) or not directory:
                raise ValueError('run_dir must be a nonempty path')
        except (OSError, ValueError, KeyError, TypeError) as error:
            parser.error(f'Invalid active-run pointer: {error}')
    try:
        monitor(directory, once=args.once)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
