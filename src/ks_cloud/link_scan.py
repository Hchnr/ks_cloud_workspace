"""Enumerate a directory alias without losing its logical object paths.

Errors are yielded to the caller, so an unreadable subtree cannot silently
turn into a successful backup. Cycle detection is per ancestry, not global:
different aliases of the same target must each retain their own object keys.
"""
import os
import stat


def scan_link(path):
    """Yield (kind, path, value): file/size or exclusion/reason.

    The supplied path must be a directory symlink. Directory entries are
    traversed in sorted order, with files before child directories. The
    caller is responsible for durable batching and restart checkpoints.
    """
    if not os.path.islink(path):
        yield 'exclude', path, 'expected directory symlink'
        return
    stack = [(path, frozenset())]
    while stack:
        directory, ancestors = stack.pop()
        try:
            info = os.stat(directory)
            if not stat.S_ISDIR(info.st_mode):
                raise NotADirectoryError(directory)
            identity = (info.st_dev, info.st_ino)
            if identity in ancestors:
                yield 'exclude', directory, 'directory symlink cycle'
                continue
            with os.scandir(directory) as entries:
                entries = sorted(entries, key=lambda entry: entry.name)
        except OSError as error:
            yield 'exclude', directory, str(error)
            continue
        ancestry = ancestors | {identity}
        directories = []
        for entry in entries:
            try:
                info = entry.stat(follow_symlinks=True)
                if stat.S_ISDIR(info.st_mode):
                    directories.append(entry.path)
                elif stat.S_ISREG(info.st_mode):
                    if '\n' in entry.path or '\r' in entry.path:
                        yield 'exclude', entry.path, 'newline in filename'
                    else:
                        yield 'file', entry.path, info.st_size
                else:
                    yield 'exclude', entry.path, 'special file'
            except OSError as error:
                yield 'exclude', entry.path, str(error)
        stack.extend((child, ancestry) for child in reversed(directories))
