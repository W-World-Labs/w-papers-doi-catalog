import tempfile
import unittest
import io
import hashlib
from pathlib import Path
from scripts.verify_openalex_bulk import (
    Compact, Source, merge_ranges, new_network_bytes, verification_ranges,
    chunk_range,
)


class BulkProbeTests(unittest.TestCase):
    def test_compact_metadata_and_malformed_input(self):
        self.assertEqual(Compact(bytes([0x15, 2, 0])).read(), {1: 1})
        self.assertEqual(Compact(bytes([0x19, 0x25, 2, 4, 0])).read(), {1: [1, 2]})
        for raw in [b'', bytes([0x18, 100, 1]), bytes([0x15])+b'\xff'*10]:
            with self.assertRaises(ValueError):Compact(raw).read()

    def test_byte_ranges_merge_without_double_counting(self):
        self.assertEqual(merge_ranges([(20,30),(0,10),(5,20),(40,45)]),[[0,30],[40,45]])
        self.assertEqual(merge_ranges([(0,4),[4,8]]),[[0,8]])
        with self.assertRaises(ValueError):merge_ranges([(5,5)])

    def test_verification_uses_full_selected_rowgroups_and_all_id_chunks(self):
        def column(name, start, size):
            return {3: {3: [name.encode()], 9: start, 7: size}}
        metadata = {4: [
            {3: 2, 1: [column('id', 0, 2), column('title', 10, 10)]},
            {3: 3, 1: [column('id', 40, 2), column('title', 20, 10)]},
        ]}
        self.assertEqual(verification_ranges(metadata, [2]), [[0, 2], [20, 30], [40, 42]])
        self.assertEqual(verification_ranges(metadata, [1, 2]), [[0, 2], [10, 30], [40, 42]])
        with self.assertRaisesRegex(ValueError, 'outside Parquet file'):
            verification_ranges(metadata, [5])

    def test_column_chunk_range_includes_any_index_page_before_data(self):
        self.assertEqual(chunk_range({3: {7: 20, 9: 100}}), (100, 120))
        self.assertEqual(chunk_range({3: {7: 20, 9: 100, 10: 90, 11: 95}}), (90, 110))

    def test_network_budget_counts_only_verified_cached_ranges(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Source.__new__(Source)
            source.root = Path(tmp)
            data = b'abc'
            (source.root / '1-4.bin').write_bytes(data)
            (source.root / '1-4.sha256').write_text(hashlib.sha256(data).hexdigest())
            self.assertEqual(new_network_bytes(source, [[1, 4], [5, 7]]), 2)
            (source.root / '1-4.sha256').write_text('invalid')
            with self.assertRaisesRegex(ValueError, 'corrupted'):
                new_network_bytes(source, [[1, 4]])

    def test_budget_rejects_before_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            s=Source.__new__(Source)
            s.root=Path(tmp);s.size=100;s.bytes=0;s.budget=9;s.requests=0;s.ranges=[]
            with self.assertRaisesRegex(ValueError,'budget'):s.get(0,10)
            self.assertEqual(s.requests,0)

    def test_invalid_source_rejected_before_network(self):
        with self.assertRaises(ValueError):
            Source('https://api.openalex.org/works',Path('/tmp/not-created'),100)

    def test_pinned_range_and_cache_integrity(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Source.__new__(Source)
            source.root=Path(tmp);source.size=100;source.bytes=0;source.budget=8
            source.requests=0;source.ranges=[];source.etag='fixed';source.url='https://example.invalid/file'
            class Response(io.BytesIO):
                status=206
                headers={'Content-Range':'bytes 10-17/100','ETag':'fixed'}
            class Opener:
                def open(inner,request,timeout):
                    self.assertEqual(request.get_header('If-match'),'fixed')
                    return Response(b'12345678')
            source.opener=Opener()
            self.assertEqual(source.get(10,18),b'12345678')
            self.assertEqual(source.get(10,18),b'12345678')
            self.assertEqual((source.bytes,source.requests),(8,1))
            (Path(tmp)/'10-18.bin').write_bytes(b'87654321')
            with self.assertRaisesRegex(ValueError,'corrupted'):source.get(10,18)
