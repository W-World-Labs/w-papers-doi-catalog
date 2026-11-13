# Operator profiling and bounded range verification

profile_openalex_index.py reuses app.openalex_catalog and the existing
app.openalex_snapshot._index_works implementation. Supply --shard and
--dataset-version explicitly. It profiles a local gzip JSONL works shard into
a fresh TemporaryDirectory database and removes only that scratch state.
It never downloads input or operates an existing catalog.

verify_openalex_bulk.py is the existing bounded S3 range experiment. DuckDB is
an optional operator-installed decoder, not a runtime service dependency.
Receipts distinguish ETag/length/range hashes, actual network bytes, estimates,
budget refusals and decoding outcomes; selected-row estimates are not a
snapshot-completeness claim. Optional catalog connections are read-only.
No real data/network experiment runs in unittests or source CI.
