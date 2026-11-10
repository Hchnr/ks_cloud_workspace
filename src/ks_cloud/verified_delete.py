"""Delete only manifest entries after rechecking their KS3 paths.

This module is inert unless a separate, validated deletion run supplies its
explicit gate. It never traverses a directory symlink or crosses a mount.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys

from .common import parse_progress, successful, tail, upload_command


def checked_parts(root, path, excluded_child=None):
    root = os.path.abspath(root)
    if root == '/' or not os.path.isabs(path) or os.path.normpath(path) != path:
        raise ValueError('invalid root or non-normalized path')
    if os.path.commonpath((root, path)) != root or path == root:
        raise ValueError('manifest path outside source root')
    parts = Path(os.path.relpath(path, root)).parts
    if not parts or any(part in ('', '.', '..') for part in parts):
        raise ValueError('invalid relative path')
    if excluded_child and parts[0] == excluded_child:
        raise ValueError('manifest enters separately scheduled child')
    return parts


def entry_state(root, parts, root_ino, remove=False):
    """Inspect/unlink one entry using dirfds; symlink directories are rejected."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    if os.path.realpath(root) != os.path.abspath(root):
        raise ValueError('source root or an ancestor is a symlink')
    fd = os.open(root, flags)
    try:
        root_stat = os.fstat(fd)
        if root_stat.st_ino != root_ino:
            raise ValueError('source root identity changed')
        for part in parts[:-1]:
            next_fd = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = next_fd
            if os.fstat(fd).st_dev != root_stat.st_dev:
                raise ValueError('nested mount boundary')
        try:
            info = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            return 'absent', 0
        if stat.S_ISLNK(info.st_mode):
            try:
                target = os.stat(parts[-1], dir_fd=fd, follow_symlinks=True)
            except FileNotFoundError:
                raise ValueError('file symlink target is missing') from None
            if not stat.S_ISREG(target.st_mode):
                raise ValueError('symlink does not refer to a regular file')
            size = target.st_size
        elif stat.S_ISREG(info.st_mode):
            size = info.st_size
        else:
            raise ValueError('entry is not a regular file or file symlink')
        if remove:
            try:
                os.unlink(parts[-1], dir_fd=fd)
            except FileNotFoundError:
                return 'absent', 0
            return 'removed', size
        return 'present', size
    except FileNotFoundError:
        return 'absent', 0
    finally:
        os.close(fd)


def validate_gate(config):
    run = Path(config['run_dir'])
    if config.get('operation') != 'delete_verified' or (run / 'STOP').exists():
        raise ValueError('deletion disabled or stopped')
    gate = json.loads((run / 'DELETE_ENABLED').read_text())
    if gate.get('nonce') != config.get('delete_nonce') or not gate.get('nonce'):
        raise ValueError('deletion gate does not match run')
    cycle = json.loads((Path(config['verification_run']) / 'followup-state.json').read_text())
    if cycle.get('phase') != 'finished':
        raise ValueError('presence verification and link follow-up not finished')


def validate_manifest(config, source, task_id, manifest, expected):
    task = next((t for t in config['tasks'] if t['id'] == source), None)
    if not task:
        raise ValueError('unknown source')
    verify_config = json.loads((Path(config['verification_run']) / 'config.json').read_text())
    original = next((t for t in verify_config['tasks'] if t['id'] == source), None)
    if not original or verify_config.get('update_rule') != 1 or any(task[k] != original[k] for k in ('source', 'destination')):
        raise ValueError('source or destination differs from verified run')
    db_path = Path(config['verified_db']).resolve()
    with sqlite3.connect(db_path.as_uri() + '?mode=ro&immutable=1', uri=True) as db:
        row = db.execute('SELECT source,manifest,files,state FROM tasks WHERE id=?', (task_id,)).fetchone()
    if not row or row[0] != source or os.path.abspath(row[1]) != os.path.abspath(manifest) or row[2] != expected or row[3] != 'done':
        raise ValueError('manifest is not a completed verified task')
    manifest_root = Path(config['verification_run']) / 'manifests' / source
    if os.path.commonpath((str(manifest_root.resolve()), str(Path(manifest).resolve()))) != str(manifest_root.resolve()):
        raise ValueError('manifest outside verified run')
    paths = Path(manifest).read_text().splitlines()
    if len(paths) != expected:
        raise ValueError('manifest count mismatch')
    entries = [(p, checked_parts(task['source'], p, task.get('exclude_child'))) for p in paths]
    return task, entries


def run(config, source, task_id, manifest, expected, folder):
    validate_gate(config)
    task, entries = validate_manifest(config, source, task_id, manifest, expected)
    root, root_ino = task['source'], task['root_ino']
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    present, absent, failures = [], 0, []
    for path, parts in entries:
        try:
            state, _ = entry_state(root, parts, root_ino)
            if state == 'absent':
                absent += 1
            else:
                present.append((path, parts))
        except (OSError, ValueError) as error:
            failures.append(dict(path=path, error=type(error).__name__ + ': ' + str(error)))
    if failures:
        (folder / 'delete-failures.json').write_text(json.dumps(failures, ensure_ascii=False))
        raise RuntimeError('source path inspection failed; no files deleted')
    if present:
        check_manifest = folder / 'present.txt'
        check_manifest.write_text(''.join(path + '\n' for path, _ in present))
        upload_config = dict(config, **task, update_rule=1)
        args = upload_command(upload_config, check_manifest, folder / 'presence-check')
        log = folder / 'presence-check.log'
        with log.open('wb') as out:
            result = subprocess.run(args, stdin=subprocess.DEVNULL, stdout=out, stderr=out,
                                    timeout=config.get('presence_check_timeout', 21600))
        check = parse_progress(tail(log))
        check['exit_code'] = result.returncode
        if not successful(check) or check.get('handled') != len(present):
            raise RuntimeError('cloud path recheck failed; no files deleted')
    validate_gate(config)
    removed, removed_bytes = 0, 0
    def unlink(entry):
        path, parts = entry
        try:
            state, size = entry_state(root, parts, root_ino, remove=True)
            return path, state, size, ''
        except (OSError, ValueError) as error:
            return path, 'failed', 0, type(error).__name__ + ': ' + str(error)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(16, int(config.get('delete_workers', 4))))) as pool:
        for path, state, size, error in pool.map(unlink, present):
            if state == 'removed':
                removed += 1
                removed_bytes += size
            elif state == 'absent':
                absent += 1
            else:
                failures.append(dict(path=path, error=error))
    if failures:
        (folder / 'delete-failures.json').write_text(json.dumps(failures, ensure_ascii=False))
    print('Succeed count:', removed, 'Skip count:', absent, 'Failed count:', len(failures), flush=True)
    print('Succeed bytes:', str(removed_bytes) + 'B', 'Skip bytes: 0B', flush=True)
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--expected', type=int, required=True)
    parser.add_argument('--folder', required=True)
    args = parser.parse_args()
    try:
        config = json.loads(Path(args.config).read_text())
        return run(config, args.source, args.task_id, args.manifest, args.expected, args.folder)
    except Exception as error:
        print('verified_delete:', type(error).__name__, str(error), file=sys.stderr, flush=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
