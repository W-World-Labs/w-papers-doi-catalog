# W Papers DOI Catalog

Independent Python DOI cache/provider service and separate OpenAlex snapshot
read API. app/domain owns DOI normalization and the legacy DoiWork contract;
app/application owns TTL/stale policy; adapters own SQLite and fixed-origin
HTTP providers. app/openalex_* owns validation, indexing, queries and existing
bounded experiments. xradar is optional and inactive without W_RADAR_CODE.
No browser UI or webpage parser belongs here.

Read README.md, NOTICE.md and PUBLIC_SOURCE.md before changing scope. Preserve
/v1/doi contracts, read-only API behavior, socket-peer allowlists, finite upstream
limits, import locks, frozen snapshots and complete=false semantics. Keep DOI
cache and snapshot databases independent. OA flags do not grant content rights.

Python 3.12; requirements.txt pins FastAPI, httpx and uvicorn. Existing tests:
python -m unittest discover -s tests -q. Product install/test/packaging runs in
public CI. Optional tools/data are supplied explicitly, never downloaded by
an import or synthetic test.

Deploy files are generic examples, not auto-applied. Source review must not
start services, renew certificates, change proxy mode or operate existing data.
Never overwrite an operator database to repair a test. The profiler uses fresh
temporary scratch state. Configuration is environment-only; .env.example is
inert. Keep secrets and real operating addresses out of source and evidence.

Preserve original assertions. tests/test_deploy.py checks literal examples;
change corresponding constants together without dropping mount, privilege,
certificate or privacy assertions. Source archives contain tracked software and
dependency declarations, not databases, datasets or binaries.
