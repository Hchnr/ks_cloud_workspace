import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.common import parse_progress
from ks_cloud.scope import OUTSIDE, UploadScope, filter_manifest


class ScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.source, self.other, self.outside = [self.base / name for name in ('source', 'other', 'outside')]
        for path in (self.source, self.other, self.outside):
            path.mkdir()
        (self.source / 'normal').write_bytes(b'abc')
        (self.other / 'inside').write_bytes(b'inner')
        (self.outside / 'secret').write_bytes(b'outside')
        (self.source / 'internal').symlink_to(self.other / 'inside')
        (self.source / 'external').symlink_to(self.outside / 'secret')
        (self.source / 'external-dir').symlink_to(self.outside, target_is_directory=True)
        self.scope = UploadScope([str(self.source), str(self.other)])

    def tearDown(self):
        self.temp.cleanup()

    def test_legacy_manifest_filters_files_and_directory_aliases_outside_scope(self):
        paths = [self.source / 'normal', self.source / 'internal', self.source / 'external',
                 self.source / 'external-dir' / 'secret']
        manifest, allowed, audit = [self.base / name for name in ('manifest', 'allowed', 'audit')]
        manifest.write_text(''.join(str(path) + '\n' for path in paths))
        self.assertEqual(filter_manifest(self.scope, str(self.source), manifest, allowed, audit), (2, 2, 14))
        self.assertEqual(allowed.read_text().splitlines(), [str(paths[0]), str(paths[1])])
        excluded = [json.loads(line) for line in audit.read_text().splitlines()]
        self.assertEqual([item['path'] for item in excluded], [str(paths[2]), str(paths[3])])
        self.assertTrue(all(item['reason'] == OUTSIDE for item in excluded))

    def test_unresolvable_or_outside_logical_path_fails_closed(self):
        link = self.source / 'dangling'
        link.symlink_to(self.outside / 'missing')
        manifest = self.base / 'manifest'
        manifest.write_text(str(link) + '\n')
        with self.assertRaises(FileNotFoundError):
            filter_manifest(self.scope, str(self.source), manifest, self.base / 'allowed', self.base / 'audit')
        manifest.write_text(str(self.outside / 'secret') + '\n')
        with self.assertRaisesRegex(ValueError, 'outside its logical source'):
            filter_manifest(self.scope, str(self.source), manifest, self.base / 'allowed', self.base / 'audit')

    def test_legacy_path_cannot_leave_scope_and_then_point_back_inside(self):
        (self.outside / 'back-file').symlink_to(self.other / 'inside')
        (self.outside / 'back-dir').symlink_to(self.other, target_is_directory=True)
        paths = [self.source / 'external-dir' / 'back-file',
                 self.source / 'external-dir' / 'back-dir' / 'inside']
        manifest, allowed, audit = [self.base / name for name in ('manifest', 'allowed', 'audit')]
        manifest.write_text(''.join(str(path) + '\n' for path in paths))
        self.assertEqual(filter_manifest(self.scope, str(self.source), manifest, allowed, audit), (0, 2, 10))
        self.assertEqual(allowed.read_text(), '')
        self.assertEqual([json.loads(line)['path'] for line in audit.read_text().splitlines()], list(map(str, paths)))

    def test_a_root_replaced_by_a_symlink_is_not_accepted(self):
        alias = self.base / 'root-alias'
        alias.symlink_to(self.outside, target_is_directory=True)
        with self.assertRaises(ValueError):
            UploadScope([str(alias)])

    def test_scoped_upload_never_passes_external_target_to_tool(self):
        fake = self.base / 'fake-tool'
        fake.write_text("""#!/usr/bin/env python3
import os, sys
from pathlib import Path
manifest = Path(sys.argv[sys.argv.index('--object-file') + 1])
paths = manifest.read_text().splitlines()
Path(os.environ['TEST_RECEIVED']).write_text('\\n'.join(paths))
print('Succeed count: 0 Skip count:', len(paths), 'Failed count: 0')
print('Succeed bytes: 0B Skip bytes: 8B Failed bytes: 0B')
""")
        fake.chmod(0o755)
        manifest = self.base / 'manifest'
        manifest.write_text(''.join(str(self.source / name) + '\n' for name in ('normal', 'internal', 'external')))
        folder = self.base / 'worker'
        folder.mkdir()
        task = dict(scope_allowed_roots=[str(self.source), str(self.other)], manifest=str(manifest), expected=3,
                    upload=dict(tool=str(fake), config_file='/dev/null', source=str(self.source),
                                destination='ks3://bucket/path/', jobs=2, parallel=1, list_parallel=1))
        task_file = folder / 'task.json'
        task_file.write_text(json.dumps(task))
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'),
                   TEST_RECEIVED=str(self.base / 'received'))
        proc = subprocess.run([sys.executable, '-m', 'ks_cloud.scoped_upload', '--task', str(task_file),
                               '--folder', str(folder)], capture_output=True, text=True, env=env, timeout=10)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual((self.base / 'received').read_text().splitlines(),
                         [str(self.source / 'normal'), str(self.source / 'internal')])
        result = parse_progress(proc.stdout)
        self.assertEqual((result['handled'], result['skipped'], result['scope_excluded_files'],
                          result['scope_excluded_bytes']), (3, 2, 1, 7))


if __name__ == '__main__':
    unittest.main()
