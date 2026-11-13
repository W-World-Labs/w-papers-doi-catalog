# Reviewed public source scope

Complete current independent Python DOI metadata service source: legacy DOI
cache/providers/TTL policy, OpenAlex snapshot validation/download, SQLite/FTS
indexing/read API, bounded raw increment and article-trial tools, range verifier,
profiler, original tests and generic self-hosting examples. No product users,
private Papers library backend or webpage extraction algorithm is included.
Shared webpage extraction remains owned by W Commons; no second parser is copied.

No private Git history, deployment inventory, real service addresses, credentials,
databases, snapshots, correction payloads, paper/abstract corpora, user data,
dependencies or generated binaries are distributed. Example IPs are documentation
addresses and require replacement. Software/data rights stay distinct (NOTICE.md).

Original test methods and assertions remain; only documentation IP constants
change consistently in templates/tests. The profiler takes an explicit local
shard and a new temporary scratch database, retaining its indexing/profiling
implementation. The historical increment unit has a generic example name.
The original source checkout and live services are untouched.

Standard public Ubuntu CI runs existing synthetic/loopback unittests, records
resolved Python license/archive declarations, scans source/history/all Git-object
payloads and uploads exact Git source plus metadata. It never invokes real
download/import/maintenance/benchmark/certificate/deployment entrypoints.

Data coverage/equality, upstream access, optional DuckDB decoding, large-data
capacity, network/region/configuration, recovery and binary distribution remain
separate acceptance. The article-trial helper is explicit and opt-in, not a
snapshot synchronization policy.
