import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.openalex_increment import make_plan, download, atomic_json, verify_deletions, verify_manifest
from app.openalex_snapshot import SnapshotError


def manifest(date, files):
    rows = [{'url': 's3://openalex/data/jsonl/works/' + p,
             'meta': {'record_count': n, 'content_length': size}} for p, n, size in files]
    total = sum(r['meta']['record_count'] for r in rows)
    size = sum(r['meta']['content_length'] for r in rows)
    return json.dumps({'date': date, 'meta': {'record_count': total, 'content_length': size},
                      'entities': [{'entity': 'works', 'record_count': total, 'content_length': size,
                                    'files': rows}]}).encode()


class IncrementTests(unittest.TestCase):
    def test_select_new_partitions_and_previously_missing_paths(self):
        old = manifest('2026-09-23', [('updated_date=2026-09-01/part_0.gz', 2, 40)])
        new = manifest('2026-10-14', [('updated_date=2026-09-01/part_0.gz', 1, 20),
                                    ('updated_date=2026-10-01/part_0.gz', 2, 40),
                                    ('updated_date=2026-09-02/part_0.gz', 1, 20)])
        plan = make_plan(old, new)
        self.assertEqual(len(plan['files']), 2)
        self.assertEqual(plan['record_count'], 3)
        with self.assertRaises(SnapshotError):
            make_plan(new, old)

    def test_resume_completed_receipts_without_downloading_again(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = gzip.compress(b'{"id":"https://openalex.org/W1"}\n')
            ledger = gzip.compress(b'work_id,deleted_date\nhttps://openalex.org/W2,2026-10-01\n')
            old = manifest('2026-09-23', [('updated_date=2026-09-01/part_0.gz', 1, 20)])
            new = manifest('2026-10-14', [('updated_date=2026-10-01/part_0.gz', 1, len(raw))])
            plan = make_plan(old, new)
            plan['deleted_ids'] = {'url': 's3://openalex/data/jsonl/works/deleted_ids.csv.gz',
                                   'path': 'works/deleted_ids.csv.gz', 'content_length': len(ledger), 'etag': 'fixed'}
            (root / 'base-manifest.json').write_bytes(old)
            (root / 'manifest.json').write_bytes(new)
            atomic_json(root / 'plan.json', plan)
            def fake(root, item, reserve, deletion=False):
                path = root / item['path'];path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(ledger if deletion else raw)
                return {'path': item['path'], 'bytes': item['content_length'], 'records': 1, 'sha256': 'test'}
            with patch('app.openalex_increment._read_manifest', return_value=new):
                with patch('app.openalex_increment.download_file', side_effect=fake) as worker:
                    self.assertEqual(download(root, 4, 0)['state'], 'raw_complete')
                    self.assertEqual(worker.call_count, 2)
                with patch('app.openalex_increment.download_file', side_effect=AssertionError('must resume')):
                    self.assertEqual(download(root, 4, 0)['state'], 'raw_complete')
            with patch('app.openalex_increment._read_manifest', return_value=old):
                with self.assertRaises(SnapshotError):
                    download(root, 4, 0)
            plan['files'][0]['path'] = '../escape'
            with self.assertRaises(SnapshotError):
                verify_manifest(root, plan)

    def test_deletions_are_validated(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'deleted.gz'
            path.write_bytes(gzip.compress(b'work_id,deleted_date\nhttps://openalex.org/W2,2026-10-01\n'))
            self.assertEqual(verify_deletions(path), 1)
            path.write_bytes(gzip.compress(b'wrong,header\n'))
            with self.assertRaises(SnapshotError):verify_deletions(path)
