"""Keep upload paths and symlink targets within explicitly approved roots."""
import json
import os
from pathlib import Path
import stat


OUTSIDE = 'symlink target outside selected source roots'


class UploadScope:
    def __init__(self, roots):
        if not isinstance(roots, list) or not roots:
            raise ValueError('scope roots must be a non-empty list')
        canonical = []
        for root in roots:
            if not isinstance(root, str) or not os.path.isabs(root) or os.path.normpath(root) != root or root == '/':
                raise ValueError('invalid scope root')
            resolved = os.path.realpath(root, strict=True)
            if not os.path.isdir(resolved) or resolved == '/' or resolved != root:
                raise ValueError('scope root is not a directory')
            canonical.append(resolved)
        self.roots = tuple(dict.fromkeys(canonical))

    def contains(self, resolved):
        return any(resolved == root or resolved.startswith(root + os.sep) for root in self.roots)

    def resolve(self, path):
        return os.path.realpath(path, strict=True)

    def allows(self, path):
        return self.contains(self.resolve(path))


def filter_manifest(scope, source, manifest, destination, audit):
    """Filter a pre-policy manifest; return allowed count, excluded count and bytes.

    This path is used only for already queued work created before scope checking
    was deployed. New manifests are checked by the scanner before publication.
    Missing or uninspectable entries fail the task rather than being claimed as
    backed up. The caller keeps the original immutable manifest untouched.
    """
    source = os.path.normpath(source)
    if source not in scope.roots:
        raise ValueError('manifest source is not an approved root')
    parents = {source: (source, True)}

    def checked_parent(parent):
        # A legacy link manifest may enter an external directory and later
        # point back inside the approved roots. Every link on the way must be
        # in scope, not just the final resolved file.
        current = source
        relative = os.path.relpath(parent, source)
        for part in (() if relative == '.' else relative.split(os.sep)):
            child = os.path.join(current, part)
            if child not in parents:
                resolved, allowed = parents[current]
                info = os.lstat(child)
                if stat.S_ISLNK(info.st_mode):
                    resolved = scope.resolve(child)
                    allowed = allowed and scope.contains(resolved)
                else:
                    if not stat.S_ISDIR(info.st_mode):
                        raise ValueError('manifest parent is not a directory')
                    resolved = os.path.join(resolved, part)
                parents[child] = (resolved, allowed)
            current = child
        return parents[parent]

    accepted = excluded = excluded_bytes = 0
    with open(manifest) as inp, open(destination, 'w') as out, open(audit, 'w') as log:
        for line in inp:
            path = line.rstrip('\n')
            if (not os.path.isabs(path) or os.path.normpath(path) != path or
                    os.path.commonpath((source, path)) != source or path == source):
                raise ValueError('manifest path is outside its logical source')
            parent = os.path.dirname(path)
            resolved_parent, parent_allowed = checked_parent(parent)
            info = os.lstat(path)
            if stat.S_ISLNK(info.st_mode):
                resolved = scope.resolve(path)
                target = os.stat(path)
            else:
                resolved = os.path.join(resolved_parent, os.path.basename(path))
                target = info
            if not stat.S_ISREG(target.st_mode):
                raise ValueError('manifest entry is not a regular file')
            if not parent_allowed or not scope.contains(resolved):
                excluded += 1
                excluded_bytes += target.st_size
                log.write(json.dumps(dict(path=path, target=resolved, reason=OUTSIDE), ensure_ascii=False) + '\n')
            else:
                out.write(path + '\n')
                accepted += 1
    return accepted, excluded, excluded_bytes
