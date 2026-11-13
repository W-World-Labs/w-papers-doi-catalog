"""Freeze and download a resumable OpenAlex increment without changing live indexes."""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import fcntl
import gzip
import hashlib
import json
import re
import shutil
import sqlite3
import time
import urllib.request
from pathlib import Path

from app.openalex_snapshot import (
    SnapshotError, _download_urllib, _opener, _read_manifest,
    _record_count, _url, _validate_manifest,
)


def read_manifest() -> bytes:
    for attempt in range(3):
        try:
            return _read_manifest()
        except OSError:
            if attempt == 2:
                raise
            time.sleep(attempt + 1)
    raise AssertionError('unreachable')


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, data: dict) -> None:
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, sort_keys=True, indent=2) + '\n')
    temporary.replace(path)


def make_plan(base_bytes: bytes, target_bytes: bytes) -> dict:
    base = _validate_manifest(base_bytes)
    target = _validate_manifest(target_bytes)
    if target['date'] <= base['date']:
        raise SnapshotError('target snapshot must be newer than the verified base')
    base_urls = {f['url'] for e in base['entities'] for f in e['files']}
    files = []
    for entity in target['entities']:
        for item in entity['files']:
            path = item['url'].removeprefix('s3://openalex/data/jsonl/')
            if path == item['url'] or '..' in Path(path).parts or Path(path).is_absolute():
                raise SnapshotError('unsafe increment path')
            match = re.search(r'/updated_date=(\d{4}-\d{2}-\d{2})/', '/' + path)
            if not match:
                raise SnapshotError('increment requires updated_date partitions')
            if match[1] > base['date'] or item['url'] not in base_urls:
                files.append({'entity': entity['entity'], 'url': item['url'], 'path': path,
                              **item['meta']})
    return {
        'schema_version': 1, 'base_date': base['date'], 'target_date': target['date'],
        'base_manifest_sha256': hashlib.sha256(base_bytes).hexdigest(),
        'manifest_sha256': hashlib.sha256(target_bytes).hexdigest(),
        'files': files,
        'content_length': sum(f['content_length'] for f in files),
        'record_count': sum(f['record_count'] for f in files),
        'note': 'Raw increment only. Does not publish SQLite, ES, or vector projections.',
    }


def verify_manifest(root: Path, plan: dict, *, remote: bool = False) -> None:
    for filename, field in [('base-manifest.json', 'base_manifest_sha256'),
                            ('manifest.json', 'manifest_sha256')]:
        if hashlib.sha256((root / filename).read_bytes()).hexdigest() != plan[field]:
            raise SnapshotError('frozen manifest changed: ' + filename)
    expected = make_plan((root / 'base-manifest.json').read_bytes(), (root / 'manifest.json').read_bytes())
    if any(expected[key] != plan[key] for key in expected):
        raise SnapshotError('increment file list differs from frozen manifest')
    if remote and hashlib.sha256(read_manifest()).hexdigest() != plan['manifest_sha256']:
        raise SnapshotError('official release changed; refusing mixed-snapshot download')


def prepare(base_manifest: Path, root: Path) -> dict:
    if not root.is_absolute():
        raise SnapshotError('increment directory must be absolute')
    root.mkdir(parents=True, exist_ok=True)
    if (root / 'plan.json').exists():
        plan = json.loads((root / 'plan.json').read_text())
        verify_manifest(root, plan, remote=True)
        return plan
    base_bytes = base_manifest.read_bytes()
    target_bytes = read_manifest()
    plan = make_plan(base_bytes, target_bytes)
    url = 's3://openalex/data/jsonl/works/deleted_ids.csv.gz'
    request = urllib.request.Request(_url(url), method='HEAD')
    with _opener().open(request, timeout=30) as response:
        size = int(response.headers['Content-Length'])
        etag = response.headers.get('ETag')
    if size <= 0 or not etag:
        raise SnapshotError('deletion ledger lacks size or ETag')
    plan['deleted_ids'] = {'url': url, 'path': 'works/deleted_ids.csv.gz',
                           'content_length': size, 'etag': etag}
    # Immutable metadata; do not allow a failed preparation to replace a prior freeze.
    for name, data in [('base-manifest.json', base_bytes), ('manifest.json', target_bytes)]:
        path = root / name
        if path.exists() and path.read_bytes() != data:
            raise SnapshotError('existing frozen manifest differs: ' + name)
        path.write_bytes(data)
    atomic_json(root / 'plan.json', plan)
    return plan


def verify_deletions(path: Path) -> int:
    with gzip.open(path, 'rt', encoding='utf-8', newline='') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ['work_id', 'deleted_date']:
            raise SnapshotError('unexpected deletion ledger columns')
        count = 0
        for row in reader:
            if not re.fullmatch(r'https://openalex.org/W\d+', row['work_id'] or ''):
                raise SnapshotError('invalid deletion work ID')
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', row['deleted_date'] or ''):
                raise SnapshotError('invalid deletion date')
            count += 1
        return count


def download_file(root: Path, item: dict, reserve: int, deletion: bool = False) -> dict:
    target = root / item['path']
    target.parent.mkdir(parents=True, exist_ok=True)
    # Bounded retries and Range resume are supplied by the existing downloader.
    if not target.exists():
        _download_urllib(item['url'], target, item['content_length'], reserve)
    if target.stat().st_size != item['content_length']:
        raise SnapshotError('increment shard size mismatch: ' + item['path'])
    if deletion:
        request = urllib.request.Request(_url(item['url']), method='HEAD')
        with _opener().open(request, timeout=30) as response:
            if response.headers.get('ETag') != item['etag']:
                raise SnapshotError('deletion ledger changed during download')
        count = verify_deletions(target)
    else:
        count = _record_count(target)
        if count != item['record_count']:
            raise SnapshotError('increment shard record count mismatch: ' + item['path'])
    return {'path': item['path'], 'bytes': target.stat().st_size, 'records': count,
            'sha256': _sha256_file(target)}


def download(root: Path, workers: int, reserve: int) -> dict:
    if not 1 <= workers <= 8 or reserve < 0:
        raise SnapshotError('workers must be 1..8 and reserve nonnegative')
    plan = json.loads((root / 'plan.json').read_text())
    verify_manifest(root, plan, remote=True)
    with (root / '.download.lock').open('a+b') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SnapshotError('increment downloader already running') from exc
        with sqlite3.connect(root / 'download.sqlite') as db:
            db.execute('CREATE TABLE IF NOT EXISTS receipts(path TEXT PRIMARY KEY, bytes INTEGER, records INTEGER, sha256 TEXT)')
            receipts = {r[0]: r for r in db.execute('SELECT path,bytes,records,sha256 FROM receipts')}
            files = [(plan['deleted_ids'], True)] + [(f, False) for f in plan['files']]
            pending = []
            for item, deletion in files:
                saved = receipts.get(item['path'])
                target = root / item['path']
                if saved and target.is_file() and target.stat().st_size == saved[1] == item['content_length']:
                    continue
                pending.append((item, deletion))
            remaining = sum(item['content_length'] for item, _ in pending)
            if shutil.disk_usage(root).free < remaining + reserve:
                raise SnapshotError('insufficient space for remaining increment and reserve')

            def report(state: str, error: str | None = None) -> dict:
                rows = db.execute('SELECT count(*),coalesce(sum(bytes),0) FROM receipts').fetchone()
                result = {'state': state, 'base_date': plan['base_date'], 'target_date': plan['target_date'],
                          'expected_files': len(files), 'verified_files': rows[0],
                          'expected_bytes': plan['content_length'] + plan['deleted_ids']['content_length'],
                          'verified_bytes': rows[1], 'error': error, 'projections_updated': False}
                atomic_json(root / 'status.json', result)
                print(json.dumps(result), flush=True)
                return result
            report('downloading')
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                    iterator = iter(pending)
                    active = set()
                    for _ in range(workers):
                        pair = next(iterator, None)
                        if pair:
                            active.add(pool.submit(download_file, root, pair[0], reserve, pair[1]))
                    while active:
                        done, active = concurrent.futures.wait(active, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            receipt = future.result()
                            db.execute('INSERT OR REPLACE INTO receipts VALUES(?,?,?,?)',
                                       (receipt['path'], receipt['bytes'], receipt['records'], receipt['sha256']))
                            db.commit()
                            report('downloading')
                            pair = next(iterator, None)
                            if pair:
                                active.add(pool.submit(download_file, root, pair[0], reserve, pair[1]))
                verify_manifest(root, plan, remote=True)
                return report('raw_complete')
            except Exception as exc:
                report('failed', type(exc).__name__ + ': ' + str(exc))
                raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'download', 'status'])
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--base-manifest', type=Path)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--reserve-bytes', type=int, default=1024**4)
    args = parser.parse_args()
    if args.command == 'prepare':
        if not args.base_manifest:
            parser.error('--base-manifest required')
        plan = prepare(args.base_manifest, args.root)
        print(json.dumps({k: v for k, v in plan.items() if k != 'files'}))
    elif args.command == 'download':
        download(args.root, args.workers, args.reserve_bytes)
    else:
        print((args.root / 'status.json').read_text())


if __name__ == '__main__':
    main()
