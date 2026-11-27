"""Filter a legacy queued manifest before invoking the normal upload tool."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

from .common import upload_command
from .scope import UploadScope, filter_manifest


def run(task_file, folder):
    task = json.loads(Path(task_file).read_text())
    folder = Path(folder)
    scope = UploadScope(task['scope_allowed_roots'])
    accepted, excluded, excluded_bytes = filter_manifest(
        scope, task['upload']['source'], task['manifest'], folder / 'scope-allowed.txt',
        folder / 'scope-excluded.jsonl')
    if accepted + excluded != task['expected']:
        raise ValueError('legacy manifest count differs from its leased task')

    def publish_scope():
        print('ScopeExcluded count:', excluded, 'ScopeExcluded bytes:', str(excluded_bytes) + 'B', flush=True)

    publish_scope()
    if not accepted:
        print('Succeed count: 0 Skip count: 0 Failed count: 0', flush=True)
        print('Succeed bytes: 0B Skip bytes: 0B Failed bytes: 0B', flush=True)
        return 0
    args = upload_command(task['upload'], folder / 'scope-allowed.txt', folder)
    result = subprocess.run(args, stdin=subprocess.DEVNULL)
    # Repeat after the tool output, so the worker's bounded log tail includes it.
    publish_scope()
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--task', required=True)
    parser.add_argument('--folder', required=True)
    args = parser.parse_args()
    try:
        return run(args.task, args.folder)
    except Exception as error:
        print('scoped_upload:', type(error).__name__, str(error), file=sys.stderr, flush=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
