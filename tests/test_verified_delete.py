import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.verified_delete import run


class VerifiedDeleteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.source = self.base / 'source'
        self.source.mkdir()
        self.outside = self.base / 'outside'
        self.outside.mkdir()
        self.verify = self.base / 'verify'
        self.verify.mkdir()
        self.manifests = self.verify / 'manifests' / 'data'
        self.manifests.mkdir(parents=True)
        self.delete = self.base / 'delete'
        self.delete.mkdir()
        task = dict(id='data', source=str(self.source), destination='ks3://bucket/data/')
        (self.verify / 'config.json').write_text(json.dumps(dict(tasks=[task], update_rule=1)))
        (self.verify / 'followup-state.json').write_text(json.dumps(dict(phase='finished')))
        (self.delete / 'DELETE_ENABLED').write_text(json.dumps(dict(nonce='test-nonce')))
        self.db = self.base / 'verified.sqlite'
        with sqlite3.connect(self.db) as db:
            db.execute('CREATE TABLE tasks(id TEXT PRIMARY KEY, source TEXT, manifest TEXT, files INTEGER, state TEXT)')
        self.config = dict(operation='delete_verified', run_dir=str(self.delete), verification_run=str(self.verify),
                           verified_db=str(self.db), delete_nonce='test-nonce',
                           tasks=[dict(task, root_ino=os.lstat(self.source).st_ino)],
                           tool='/fake/ks3util', config_file='/private', jobs=2, parallel=1, list_parallel=1,
                           delete_workers=2)

    def tearDown(self):
        self.tmp.cleanup()

    def add_manifest(self, paths):
        manifest = self.manifests / 'task.txt'
        manifest.write_text(''.join(str(path) + '\n' for path in paths))
        with sqlite3.connect(self.db) as db:
            db.execute('INSERT INTO tasks VALUES (?,?,?,?,?)', ('data-task', 'data', str(manifest), len(paths), 'done'))
        return manifest

    def good_check(self, args, stdout, **kwargs):
        manifest = args[args.index('--object-file') + 1]
        count = len(Path(manifest).read_text().splitlines())
        stdout.write(('Succeed count: 0 Failed count: 0 Skip count: %d\n' % count).encode())
        class Result:
            returncode = 0
        return Result()

    def execute(self, manifest, count):
        return run(self.config, 'data', 'data-task', str(manifest), count, self.base / 'worker')

    def test_deletes_verified_files_and_only_unlinks_file_symlink(self):
        normal = self.source / 'file'
        normal.write_text('content')
        target = self.outside / 'target'
        target.write_text('outside')
        link = self.source / 'link'
        link.symlink_to(target)
        manifest = self.add_manifest([normal, link, self.source / 'already-absent'])
        with patch('ks_cloud.verified_delete.subprocess.run', self.good_check):
            self.assertEqual(self.execute(manifest, 3), 0)
        self.assertFalse(normal.exists())
        self.assertFalse(link.is_symlink())
        self.assertEqual(target.read_text(), 'outside')

    def test_symlink_directory_escape_prevents_all_deletion(self):
        normal = self.source / 'file'
        normal.write_text('content')
        target = self.outside / 'target'
        target.write_text('outside')
        (self.source / 'alias').symlink_to(self.outside, target_is_directory=True)
        manifest = self.add_manifest([normal, self.source / 'alias' / 'target'])
        with patch('ks_cloud.verified_delete.subprocess.run') as check:
            with self.assertRaises(RuntimeError):
                self.execute(manifest, 2)
            check.assert_not_called()
        self.assertTrue(normal.exists())
        self.assertTrue(target.exists())

    def test_failed_cloud_recheck_deletes_nothing(self):
        normal = self.source / 'file'
        normal.write_text('content')
        manifest = self.add_manifest([normal])
        def failed(args, stdout, **kwargs):
            stdout.write(b'Succeed count: 0 Failed count: 1 Skip count: 0\n')
            class Result:
                returncode = 1
            return Result()
        with patch('ks_cloud.verified_delete.subprocess.run', failed):
            with self.assertRaises(RuntimeError):
                self.execute(manifest, 1)
        self.assertTrue(normal.exists())

    def test_stale_root_and_missing_gate_prevent_deletion(self):
        normal = self.source / 'file'
        normal.write_text('content')
        manifest = self.add_manifest([normal])
        (self.delete / 'DELETE_ENABLED').unlink()
        with self.assertRaises(FileNotFoundError):
            self.execute(manifest, 1)
        (self.delete / 'DELETE_ENABLED').write_text(json.dumps(dict(nonce='test-nonce')))
        self.config['tasks'][0]['root_ino'] += 1
        with self.assertRaises(RuntimeError):
            self.execute(manifest, 1)
        self.assertTrue(normal.exists())


if __name__ == '__main__':
    unittest.main()
