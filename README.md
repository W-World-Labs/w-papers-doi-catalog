# W Papers DOI Catalog

Complete independent Python source for a read-only DOI metadata cache and a
separate local OpenAlex snapshot/index/query service. No product workspace,
user, private library, arXiv or UI dependency. No dataset is bundled. See
[scope](PUBLIC_SOURCE.md), [rights/data terms](NOTICE.md), [authors](AUTHORS.md)
and the new [MIT software grant](LICENSE).

## DOI cache contract

`GET /v1/doi?doi=10.1234/example` preserves the existing work shape: DOI, title,
authors, journal, publisher, date, volume, issue, page, type, ISSN, source and
from_local_catalog. Misses return 404, invalid input 422, unavailable providers
without stale data 503. X-DOI-Cache remains hit/miss/negative/stale. Fresh
positive/negative results avoid upstream calls; expired positives are stale
only on provider failure. Expired misses are not authoritative.

Normalization accepts DOIs/doi.org URLs, not arbitrary resolver URLs. Providers
use fixed HTTPS hosts, no redirects/environment proxies, bounded streamed
responses and finite timeouts. All routes including health/metrics enforce
socket-peer IP allowlists; forwarded headers are not trusted. Example IPs in
deploy templates are documentation addresses, not a runtime inventory.

## OpenAlex snapshot service

The importer freezes a release manifest, preserves entity manifests, license/
release assets and deleted IDs, resumes interrupted transfers, checks size,
gzip CRC and row counts, and indexes works into SQLite/FTS. Progress distinguishes
verified raw objects from indexed works. complete=false remains until the frozen
manifest and index count agree. Missing works never silently fall back to a
public API; the DOI cache is independent.

app.openalex_snapshot owns sync/status and guarded download-mode controls.
app.openalex_api:app owns read queries, DOI/ID lookup, title search and status.
The split-mount systemd examples require both data mounts and bind the API to
loopback. Operators choose paths, private ingress and allowlists; examples are
not production instructions and are never applied by CI.

Axel is required for new segmented downloads; existing urllib partials retain
resume behavior. Retries are bounded, ETags pin resumed objects, and a free-space
reserve/import lock protect downloads/indexing. Completed snapshots stay intact.
Optional proxies must be credential-free loopback HTTP; ambient proxies are
cleared. Root-only mode changes require protected control files and a scrubbed
environment. The wrapper retains its bounded direct/proxy benchmark as an
explicit operator action, never an import or test side effect.

Increment tools stage raw updated-date partitions and verify receipts. They do
not claim full-snapshot parity; SQLite candidate application stays disabled
because updated_date can miss field reloads. The retained article-trial helper
applies an explicitly supplied sample insert-only and idempotently; it is not
automatic API-based snapshot synchronization.

## Development and original verification

Python 3.12 is the CI runtime. In an explicitly chosen development environment:

```sh
python -m pip install -r requirements.txt
python -m unittest discover -s tests -q
```

Public CI runs the existing suite on synthetic metadata/shards/databases and
temporary loopback receivers. Cache isolation, validation, privilege/mount
checks, heartbeat-after-bind, corrupt-file refusal and disabled unsafe increment
paths remain covered. No real snapshot or production endpoint is used.

DOI entrypoint: `python -m app.server --host 127.0.0.1 --port 55221 --no-proxy-headers`.
OpenAlex entrypoint: `app.openalex_api:app`. Configuration is environment-only,
never an automatically read .env. Replace .env.example values and keep real
credentials outside source. Without a tracking code xradar does not submit
heartbeats. requirements.txt pins the existing direct packages; CI records its
actual resolved dependency declarations, not a portable lock or binary license
acceptance.

## Operator tools and data

Maintenance prune/checkpoint/backup is explicit (`python -m app.maintenance`).
Use a verified SQLite online-backup/recovery procedure for the actual writer;
source CI never operates a live catalog.

See [profiling](scripts/README.md). The profiler takes a supplied local shard
and a new temporary scratch database. The range verifier uses bounded public S3
byte ranges and optional read-only local ID checks; DuckDB 1.5.5 is an explicitly
installed optional decoder. Its receipts distinguish actual bytes/ETag/hash,
estimates, budget refusals and decoded rows. One-file/row verification is not
full-release equality or completeness. No data download or optional decoder
runs during import or the original unittest suite.

Users independently obtain data under the exact release's terms. OpenAlex
metadata, Crossref facts, copyrighted abstracts and linked full text are not
relicensed by this software. Upstream access, data freshness/coverage, capacity,
optional tools, private ingress and production activation remain separate.
