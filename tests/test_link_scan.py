import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.link_scan import scan_link


class LinkScanTest(unittest.TestCase):
    def test_alias_paths_cycles_and_dangling_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / 'target'
            target.mkdir()
            (target / 'data').write_bytes(b'abc')
            (target / 'child').mkdir()
            (target / 'child' / 'part').write_bytes(b'ab')
            (target / 'child' / 'cycle').symlink_to(target, target_is_directory=True)
            (target / 'missing').symlink_to(root / 'not-there')
            (target / 'alias').symlink_to(target / 'child', target_is_directory=True)
            link = root / 'logical'
            link.symlink_to(target, target_is_directory=True)
            items = list(scan_link(str(link)))
            files = {(Path(path).relative_to(link).as_posix(), size)
                     for kind, path, size in items if kind == 'file'}
            self.assertEqual(files, {('data', 3), ('alias/part', 2), ('child/part', 2)})
            excluded = [(Path(path).relative_to(link).as_posix(), reason)
                        for kind, path, reason in items if kind == 'exclude']
            self.assertEqual(sum('cycle' in reason for _, reason in excluded), 2)
            self.assertTrue(any(path == 'missing' for path, _ in excluded))

    def test_requires_directory_link(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(list(scan_link(tmp)), [('exclude', tmp, 'expected directory symlink')])


if __name__ == '__main__':
    unittest.main()
