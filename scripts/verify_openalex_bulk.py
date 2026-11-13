#!/usr/bin/env python3
"""Bounded, read-only bulk-file experiment. Requires an existing DuckDB install.

Only S3 Parquet byte ranges are requested; no article API or database writes.
Sparse artifacts are INCOMPLETE and must never be used as an import source.
"""
from __future__ import annotations
import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import struct
import time
import urllib.error
import urllib.request


class Compact:
    """Read only the Thrift Compact types used by Parquet metadata."""
    def __init__(self, data): self.data, self.pos = data, 0
    def take(self, n):
        if n < 0 or self.pos + n > len(self.data): raise ValueError('truncated metadata')
        b = self.data[self.pos:self.pos+n]; self.pos += n
        return b
    def byte(self): return self.take(1)[0]
    def uint(self):
        value = 0
        for shift in range(0, 70, 7):
            b = self.byte(); value |= (b & 127) << shift
            if b < 128: return value
        raise ValueError('oversized varint')
    def integer(self):
        n = self.uint(); return (n >> 1) ^ -(n & 1)
    def read(self, kind=12, depth=0):
        if depth > 64: raise ValueError('metadata nesting limit')
        sub = lambda k: self.read(k, depth+1)
        if kind in (1, 2): return kind == 1
        if kind == 3: return self.byte()
        if kind in (4, 5, 6): return self.integer()
        if kind == 7: return struct.unpack('<d', self.take(8))[0]
        if kind == 8: return self.take(self.uint())
        if kind in (9, 10):
            h = self.byte(); count = h >> 4
            if count == 15: count = self.uint()
            if count > len(self.data): raise ValueError('oversized collection')
            return [sub(h & 15) for _ in range(count)]
        if kind == 11:
            count = self.uint()
            if count > len(self.data): raise ValueError('oversized map')
            if not count: return []
            h = self.byte(); return [(sub(h >> 4), sub(h & 15)) for _ in range(count)]
        if kind == 12:
            result = {}; field = 0
            while True:
                h = self.byte()
                if not h: return result
                field = field + (h >> 4) if h >> 4 else self.integer()
                if field in result: raise ValueError('duplicate metadata field')
                result[field] = sub(h & 15)
        raise ValueError('unsupported compact type')


def merge_ranges(ranges):
    result = []
    for start, end in sorted((a,b) for a,b in ranges):
        if start < 0 or end <= start: raise ValueError('invalid byte range')
        if result and start <= result[-1][1]: result[-1][1] = max(end, result[-1][1])
        else: result.append([start, end])
    return result


class Source:
    def __init__(self, url, root, budget):
        if not re.fullmatch(r'https://openalex\.s3\.amazonaws\.com/data/parquet/works/updated_date=\d{4}-\d{2}-\d{2}/part_\d+\.parquet', url):
            raise ValueError('only official public works Parquet files are accepted')
        self.url, self.root, self.budget = url, root, budget
        self.bytes = 0; self.requests = 0; self.ranges = []
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        root.mkdir(parents=True, exist_ok=True)
        for attempt in range(3):
            try:
                with self.opener.open(urllib.request.Request(url, method='HEAD'), timeout=30) as r:
                    self.size = int(r.headers['Content-Length']); self.etag = r.headers['ETag']
                break
            except (OSError, urllib.error.URLError):
                if attempt == 2: raise
                time.sleep(attempt+1)
        identity = {'url': url, 'etag': self.etag, 'size': self.size}
        path = root / 'source.json'
        if path.exists() and json.loads(path.read_text()) != identity:
            raise ValueError('source changed; use a new work directory')
        path.write_text(json.dumps(identity, indent=2))
    def get(self, start, end):
        if not 0 <= start < end <= self.size: raise ValueError('range outside source')
        n = end-start; path = self.root / f'{start}-{end}.bin'
        checksum = path.with_suffix('.sha256')
        if path.exists() and checksum.exists():
            data = path.read_bytes()
            if len(data) == n and hashlib.sha256(data).hexdigest() == checksum.read_text():
                self.ranges.append((start,end)); return data
            raise ValueError('cached range corrupted')
        for attempt in range(3):
            if self.bytes+n > self.budget: raise ValueError('download byte budget exceeded')
            req = urllib.request.Request(self.url, headers={'Range': f'bytes={start}-{end-1}', 'If-Match': self.etag, 'Accept-Encoding':'identity'})
            self.requests += 1
            try:
                with self.opener.open(req, timeout=30) as r:
                    if r.status != 206 or r.headers.get('Content-Range') != f'bytes {start}-{end-1}/{self.size}' or r.headers.get('ETag') != self.etag:
                        raise ValueError('server did not honor pinned byte range')
                    chunks=[]; received=0
                    while received < n:
                        chunk=r.read(min(1024*1024,n-received))
                        if not chunk: raise OSError('short response')
                        received+=len(chunk);self.bytes+=len(chunk);chunks.append(chunk)
                    data=b''.join(chunks)
                path.write_bytes(data); checksum.write_text(hashlib.sha256(data).hexdigest())
                self.ranges.append((start,end)); return data
            except (OSError, urllib.error.URLError):
                if attempt == 2: raise
                time.sleep(attempt+1)
        raise AssertionError('unreachable')


def cached_range(source, start, end):
    path = source.root / f'{start}-{end}.bin'
    checksum = path.with_suffix('.sha256')
    if not path.exists() or not checksum.exists():
        return False
    data = path.read_bytes()
    if len(data) != end-start or hashlib.sha256(data).hexdigest() != checksum.read_text():
        raise ValueError('cached range corrupted')
    return True


def new_network_bytes(source, ranges):
    return sum(end-start for start,end in ranges if not cached_range(source,start,end))


def chunk_range(column):
    m=column[3]; start=min(m.get(offset,m[9]) for offset in (9,10,11))
    return start,start+m[7]


def selected_rowgroups(metadata, positions):
    groups=metadata[4]
    total=sum(group[3] for group in groups)
    selected=set()
    for position in positions:
        if isinstance(position,bool) or not isinstance(position,int) or not 0<=position<total:
            raise ValueError('row position outside Parquet file')
        start=0
        for index,group in enumerate(groups):
            if position < start+group[3]:
                selected.add(index)
                break
            start+=group[3]
    return selected


def verification_ranges(metadata, positions):
    groups=metadata[4]
    selected=selected_rowgroups(metadata,positions)
    if not selected:
        return []
    ranges=[]
    for index,group in enumerate(groups):
        for column in group[1]:
            name=b'.'.join(column[3][3])
            if index in selected or name==b'id':
                ranges.append(chunk_range(column))
    if not any(b'.'.join(column[3][3])==b'id' for group in groups for column in group[1]):
        raise ValueError('Parquet file has no ID column')
    return merge_ranges(ranges)


def sparse(source, path, ranges, footer_start):
    ranges=merge_ranges([(0,4),(footer_start,source.size)]+list(ranges))
    # Include earlier range reads in this run and account only for validated cache hits.
    if source.bytes+new_network_bytes(source,ranges)>source.budget:
        raise ValueError('planned sparse transfer exceeds remaining byte budget')
    with path.open('wb') as stream:
        stream.truncate(source.size)
        for a,b in ranges:
            stream.seek(a);stream.write(source.get(a,b))


def run(args):
    import duckdb
    report={'scope':'one bulk file only','production_writes':False,'article_api_requests':0,'decoded_complete_records':False,'full_reference_comparison':False}
    source=None
    args.work_dir.mkdir(parents=True,exist_ok=True)
    try:
        source=Source(args.url,args.work_dir,args.max_bytes)
        tail=source.get(source.size-8,source.size)
        length=struct.unpack('<I',tail[:4])[0]
        if tail[4:]!=b'PAR1' or length>4*1024*1024:raise ValueError('invalid Parquet footer')
        footer_start=source.size-8-length
        metadata=Compact(source.get(footer_start,source.size-8)).read()
        scan_ranges=[chunk_range(c) for g in metadata[4] for c in g[1] if c[3][3][0] in (b'id',b'created_date')]
        if not scan_ranges:raise ValueError('required columns absent')
        scan=args.work_dir/'INCOMPLETE-created-columns.parquet'
        sparse(source,scan,scan_ranges,footer_start)
        con=duckdb.connect();con.execute("SET memory_limit='1GB'");con.execute('SET threads=2')
        rows=con.execute('SELECT id,file_row_number FROM read_parquet(?,file_row_number=true) WHERE created_date >= CAST(? AS TIMESTAMP)',[str(scan),args.since]).fetchall()
        missing=rows
        if args.database:
            with sqlite3.connect(args.database.resolve().as_uri()+'?mode=ro',uri=True,timeout=5) as db:
                missing=[r for r in rows if not db.execute('SELECT 1 FROM openalex_works WHERE openalex_id=?',(r[0].rsplit('/',1)[-1],)).fetchone()]
        report.update(source_bytes=source.size,source_rows=metadata[3],created_since=len(rows),missing_locally=len(missing) if args.database else None,scan_network_bytes=source.bytes)
        all_ranges=verification_ranges(metadata,[r[1] for r in missing])
        report['estimated_rowgroup_decode_bytes']=sum(b-a for a,b in all_ranges)
        (args.work_dir/'missing.json').write_text(json.dumps(missing))
        if args.verify_records and missing:
            chosen=missing[:args.verify_records]
            ranges=verification_ranges(metadata,[r[1] for r in chosen])
            ranges=merge_ranges([(0,4),(footer_start,source.size)]+ranges)
            planned=sum(b-a for a,b in ranges)
            required=new_network_bytes(source,ranges)
            report['verification_records']=len(chosen)
            report['verification_rowgroups']=len(selected_rowgroups(metadata,[row[1] for row in chosen]))
            report['verification_granularity']='complete row groups plus ID column chunks'
            report['verification_planned_bytes']=planned
            report['verification_network_bytes_required']=required
            path=args.work_dir/'INCOMPLETE-selected-rowgroups.parquet'
            if source.bytes+required>source.budget:
                report['status']='budget_exceeded'
                report['decode_error']=(f'complete row groups require {required} more response bytes; '
                                        f'{max(0,source.budget-source.bytes)} remain in the run budget')
            else:
                sparse(source,path,ranges,footer_start)
                marks=','.join('?' for r in chosen)
                try:
                    decoded=con.execute(f'SELECT * FROM read_parquet(?,file_row_number=true) WHERE id IN ({marks})',[str(path)]+[r[0] for r in chosen]).fetchall()
                    columns=[c[0] for c in con.description]
                    identities={(r[columns.index('id')],r[columns.index('file_row_number')]) for r in decoded}
                    report['decoded_complete_records']=len(decoded)==len(chosen) and identities==set(chosen)
                    report['decoded_rows']=len(decoded)
                    report['decoded_columns']=len(columns)-1
                    (args.work_dir/'decoded-sample.json').write_text(json.dumps([dict(zip(columns,r)) for r in decoded],default=str))
                    report['full_reference_comparison']=False
                except Exception as exc:
                    report['decode_error']=str(exc)[:2000]
        report.setdefault('status','measured')
    except Exception as exc:
        report.update(status='failed',error=type(exc).__name__+': '+str(exc))
    finally:
        report.update(network_bytes=source.bytes if source else 0,range_requests=source.requests if source else 0,
                      unique_requested_bytes=sum(b-a for a,b in merge_ranges(source.ranges)) if source else 0)
        (args.work_dir/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
    if report['status']=='failed': return 1
    return 2 if report.get('verification_records') and not report['decoded_complete_records'] else 0


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--url',required=True)
    p.add_argument('--work-dir',type=Path,required=True)
    p.add_argument('--database',type=Path,help='optional existing catalog, opened read-only')
    p.add_argument('--since',default='2026-09-24')
    p.add_argument('--max-bytes',type=int,default=64*1024*1024)
    p.add_argument('--verify-records',type=int,default=1)
    a=p.parse_args();date.fromisoformat(a.since)
    if not 0<=a.verify_records<=100 or a.max_bytes<=0:p.error('invalid limits')
    raise SystemExit(run(a))
