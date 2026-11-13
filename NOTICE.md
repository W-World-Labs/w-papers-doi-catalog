# Software and external-data boundaries

W-authored DOI cache, adapters, OpenAlex snapshot/index/query implementation,
operator tools and synthetic tests receive the new root MIT grant. AUTHORS.md
preserves contributor names; no earlier public grant is implied.

The existing xradar 0.2.2 SDK is W-Radar code under the same project MIT license.
profiling.py matches the public SDK at
https://github.com/W-World-Labs/w-radar/tree/57314c8f904927740962ae178ef4b7ab6e239389/sdk/python/xradar .
The older client is retained rather than silently adopting the later SDK's
trace-session changes. Only its default endpoint is a non-routable example.

FastAPI, httpx, uvicorn and their dependencies retain their own terms. Optional
DuckDB, Axel, Caddy, certbot and system tools are separately installed, not
redistributed. requirements.txt pins the three existing runtime requirements;
CI records actual resolved Python distributions, licenses and archive hashes.
That report is not a cross-platform lockfile or binary acceptance. No installed
dependencies, databases, snapshots, paper/abstract corpora or binaries are bundled.

OpenAlex metadata is described as CC0, with an ODC-BY exception for its legacy
MAG-format snapshot. Independently obtained releases retain their original
LICENSE/RELEASE_NOTES and manifests; this program uses JSONL/Parquet and never
relabels data as MIT:
https://github.com/ourresearch/openalex-docs/blob/main/license.md
https://help.openalex.org/access/snapshot/

Crossref distinguishes factual metadata and CC0-generated data from abstracts
whose publisher/author copyright remains. Linked PDFs, full text and content
licenses are separate. A DOI, OA flag or readable URL is not a redistribution
grant:
https://www.crossref.org/documentation/retrieve-metadata/
https://help.openalex.org/data/works/open-access/

Tests generate small synthetic records, manifests, compressed shards and SQLite
files in temporary directories; no real catalog, article text or private library
is bundled. Users supply lawful data and upstream access. Software MIT terms
do not alter data or hosted-service terms.
