"""Insert a bounded article sample without replacing existing snapshot records."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from pathlib import Path

from app.openalex_snapshot import _UPSERT, _work_values, SnapshotError


def apply(database: Path, sample: Path, start: str, end: str) -> dict:
    raw = sample.read_bytes()
    if len(raw) > 8 * 1024 * 1024:
        raise SnapshotError('trial sample exceeds 8 MiB')
    records = json.loads(raw)['results']
    if not isinstance(records, list) or not 1 <= len(records) <= 100:
        raise SnapshotError('trial requires 1..100 articles')
    for record in records:
        if (not re.fullmatch(r'https://openalex.org/W\d+', record.get('id', ''))
                or record.get('type') != 'article'
                or not start <= record.get('publication_date', '') <= end):
            raise SnapshotError('article outside trial scope')
    receipt = hashlib.sha256(raw).hexdigest()
    with sqlite3.connect(f'file:{database}?mode=rw', uri=True, timeout=5) as db:
        db.execute('BEGIN IMMEDIATE')
        state = db.execute('SELECT dataset_version,complete FROM openalex_snapshot_state WHERE singleton=1').fetchone()
        if not state or not state[1]:
            raise SnapshotError('base snapshot must be complete')
        columns = {r[1] for r in db.execute('PRAGMA table_info(openalex_snapshot_state)')}
        version = state[0]
        if 'storage_version' in columns:
            version = db.execute('SELECT coalesce(storage_version,dataset_version) FROM openalex_snapshot_state WHERE singleton=1').fetchone()[0]
        db.execute('''CREATE TABLE IF NOT EXISTS openalex_article_supplements(
            openalex_id TEXT PRIMARY KEY, sample_sha256 TEXT NOT NULL,
            publication_from TEXT NOT NULL, publication_to TEXT NOT NULL,
            raw_json TEXT NOT NULL, inserted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)''')
        added = []; existing = 0
        for record in records:
            values = _work_values(record, version)
            if values is None:
                raise SnapshotError('invalid article')
            if db.execute('SELECT 1 FROM openalex_works WHERE openalex_id=?', (values[1],)).fetchone():
                existing += 1
                continue
            db.execute(_UPSERT.split('ON CONFLICT')[0], values)
            db.execute('INSERT INTO openalex_article_supplements(openalex_id,sample_sha256,publication_from,publication_to,raw_json) VALUES(?,?,?,?,?)',
                       (values[1], receipt, start, end, json.dumps(record, ensure_ascii=False)))
            added.append(values[1])
        db.execute('UPDATE openalex_snapshot_state SET imported_works=imported_works+?,updated_at=CURRENT_TIMESTAMP WHERE singleton=1', (len(added),))
        db.commit()
    return {'inserted': len(added), 'existing': existing, 'ids': added,
            'base_snapshot': state[0], 'sample_sha256': receipt,
            'coverage': 'publication-date article trial only', 'full_sync': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--sample', type=Path, required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    args = parser.parse_args()
    print(json.dumps(apply(args.database, args.sample, args.start, args.end)))
