from __future__ import annotations

import gzip
import json
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib.request import ProxyHandler, Request
from unittest.mock import patch

from app.openalex_catalog import OpenAlexCatalog, connect, initialize
from app.openalex_snapshot import (
    SnapshotError,
    _UPSERT,
    _asset_path,
    _download,
    _download_proxy,
    _ensure_state,
    _index_works,
    _opener,
    _validate_manifest,
    _assert_secure_control_path,
    benchmark_download_modes,
    download_mode_status,
    set_download_mode,
    sync,
)


class _Response:
    def __init__(self, body: bytes, status: int = 206, headers=None):
        self.body = body
        self.status = status
        self.headers = headers or {"Content-Range": "bytes 3-7/8"}
        self.offset = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self.body)
        chunk = self.body[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk


class _Opener:
    def __init__(self, response: _Response):
        self.response = response
        self.request = None

    def open(self, request, timeout):
        self.request = request
        return self.response


class OpenAlexCatalogTest(unittest.TestCase):
    def test_local_work_lookup_search_and_external_fts_update(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "catalog.sqlite"
            catalog = OpenAlexCatalog(database)
            self.assertFalse(catalog.status()["complete"])
            with connect(database) as db:
                db.execute(
                    "INSERT INTO openalex_snapshot_state"
                    "(singleton,dataset_version,state,complete,expected_records,expected_files) "
                    "VALUES(1,'2026-09-23','syncing',0,1,1)"
                )
                db.execute(
                    _UPSERT,
                    (
                        "2026-09-23", "W123", "10.1234/example", "A local science paper",
                        "An abstract searchable locally", '[{"name":"Ada Author","orcid":null}]',
                        "2026-09-23", 2026, 7, "article", None, None, "2026-09-23",
                    ),
                )
            self.assertEqual(catalog.by_id("W123")["work"]["abstract"], "An abstract searchable locally")
            self.assertIs(catalog.status()["complete"], False)
            self.assertEqual(catalog.by_doi("https://doi.org/10.1234/example")["work"]["id"], "W123")
            self.assertEqual(catalog.search("science abstract", None, 20)["results"][0]["id"], "W123")
            with connect(database) as db:
                db.execute("UPDATE openalex_works SET title='Changed indexing title' WHERE openalex_id='W123'")
            result = catalog.search("changed indexing", None, 20)
            self.assertEqual(result["results"][0]["title"], "Changed indexing title")
            self.assertEqual(catalog.search("local science", None, 20)["results"], [])

    def test_search_pages_filter_snapshot_and_preserve_all_term_matching(self):
        with tempfile.TemporaryDirectory() as directory:
            catalog = OpenAlexCatalog(Path(directory) / "catalog.sqlite")
            with connect(catalog.database) as db:
                db.execute(
                    "INSERT INTO openalex_snapshot_state"
                    "(singleton,dataset_version,state,complete,expected_records,expected_files) "
                    "VALUES(1,'current','complete',1,3,1)"
                )
                db.executemany(
                    "INSERT INTO openalex_works(dataset_version,openalex_id,title,abstract) "
                    "VALUES(?,?,?,?)",
                    [("old", "W1", "attention", "science"),
                     ("current", "W2", "attention", "science"),
                     ("current", "W3", "attention", "other"),
                     ("current", "W4", "science", "attention")],
                )
            first = catalog.search("attention science", None, 1)
            second = catalog.search("attention science", first["next_cursor"], 1)
            self.assertEqual({row["id"] for row in first["results"] + second["results"]}, {"W2", "W4"})
            self.assertNotEqual(first["results"][0]["id"], second["results"][0]["id"])
            self.assertIsNone(second["next_cursor"])
            self.assertEqual(catalog.search("absentword", None, 20)["results"], [])

    def test_works_shard_streams_into_index_and_checks_row_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "catalog.sqlite"
            initialize(database)
            shard = root / "works.jsonl.gz"
            record = {
                "id": "https://openalex.org/W456",
                "doi": "https://doi.org/10.5555/LOCAL",
                "title": "Snapshot science work",
                "abstract_inverted_index": {"Local": [0], "abstract": [1]},
                "authorships": [{"author": {"display_name": "Lin Author", "orcid": "orcid:1"}}],
                "publication_year": 2025,
                "cited_by_count": 3,
            }
            with gzip.open(shard, "wt", encoding="utf-8") as stream:
                stream.write(json.dumps(record) + "\n")
            with connect(database) as db:
                db.execute(
                    "INSERT INTO openalex_snapshot_state"
                    "(singleton,dataset_version,state,complete,expected_records,expected_files) "
                    "VALUES(1,'2026-09-23','syncing',0,1,1)"
                )
                count = _index_works(shard, db, "2026-09-23", 1, database, 0)
            self.assertEqual(count, 1)
            work = OpenAlexCatalog(database).by_id("W456")["work"]
            self.assertEqual(work["abstract"], "Local abstract")
            self.assertEqual(work["authors"], [{"name": "Lin Author", "orcid": "orcid:1"}])

    def test_download_resumes_partial_and_promotes_complete_part(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "shard.gz"
            partial = target.with_name(target.name + ".part")
            partial.write_bytes(b"abc")
            opener = _Opener(_Response(b"defgh"))
            with patch("app.openalex_snapshot._opener", return_value=opener):
                _download("s3://openalex/data/jsonl/works/part.gz", target, 8, 0)
            self.assertEqual(opener.request.get_header("Range"), "bytes=3-")
            self.assertEqual(target.read_bytes(), b"abcdefgh")
            target.unlink()
            partial.write_bytes(b"abcdefgh")
            with patch("app.openalex_snapshot._opener", side_effect=AssertionError("should not redownload")):
                _download("s3://openalex/data/jsonl/works/part.gz", target, 8, 0)
            self.assertEqual(target.read_bytes(), b"abcdefgh")

    def test_axel_direct_mode_ignores_ambient_proxy_and_uses_configured_connections(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "fresh.gz"
            calls = []

            def run(command, **kwargs):
                calls.append((command, kwargs))
                output = Path(command[command.index("-o") + 1])
                output.write_bytes(b"12345678")
                return SimpleNamespace(returncode=0)

            with patch.dict(os.environ, {
                "OPENALEX_DOWNLOAD_CONNECTIONS": "16",
                "OPENALEX_DOWNLOAD_PROXY": "",
                "HTTPS_PROXY": "http://127.0.0.1:7891",
                "http_proxy": "http://127.0.0.1:7891",
                "NO_PROXY": "openalex.s3.amazonaws.com",
            }):
                with patch("app.openalex_snapshot.shutil.which", return_value="/usr/bin/axel"):
                    with patch("app.openalex_snapshot.subprocess.run", side_effect=run):
                        _download("s3://openalex/data/jsonl/works/fresh.gz", target, 8, 0)
            command, kwargs = calls[0]
            self.assertEqual(command[:5], ["/usr/bin/axel", "-N", "-n", "16", "-T"])
            self.assertEqual(command[5], "90")
            self.assertIn("-o", command)
            self.assertEqual(kwargs["timeout"], 300)
            self.assertNotIn("HTTPS_PROXY", kwargs["env"])
            self.assertNotIn("http_proxy", kwargs["env"])
            self.assertNotIn("NO_PROXY", kwargs["env"])
            self.assertEqual(target.read_bytes(), b"12345678")

    def test_axel_explicit_loopback_proxy_is_passed_to_child(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "fresh-proxied.gz"
            captured = []

            def run(command, **kwargs):
                captured.append((command, kwargs))
                Path(command[command.index("-o") + 1]).write_bytes(b"12345678")
                return SimpleNamespace(returncode=0)

            with patch.dict(os.environ, {
                "OPENALEX_DOWNLOAD_CONNECTIONS": "16",
                "OPENALEX_DOWNLOAD_PROXY": "http://127.0.0.1:7891",
                "HTTPS_PROXY": "http://203.0.113.8:7777",
                "NO_PROXY": "openalex.s3.amazonaws.com",
            }):
                with patch("app.openalex_snapshot.shutil.which", return_value="/usr/bin/axel"):
                    with patch("app.openalex_snapshot.subprocess.run", side_effect=run):
                        _download("s3://openalex/data/jsonl/works/fresh.gz", target, 8, 0)
            command, kwargs = captured[0]
            self.assertNotIn("-N", command)
            for variable in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
                self.assertEqual(kwargs["env"][variable], "http://127.0.0.1:7891")
            self.assertEqual(kwargs["env"]["NO_PROXY"], "")
            self.assertEqual(kwargs["env"]["no_proxy"], "")
            self.assertEqual(target.read_bytes(), b"12345678")

    def test_urllib_opener_uses_only_validated_loopback_proxy(self):
        with patch.dict(os.environ, {
            "OPENALEX_DOWNLOAD_PROXY": "http://127.0.0.1:7891",
            "HTTPS_PROXY": "http://203.0.113.8:7777",
            "NO_PROXY": "*",
        }):
            opener = _opener()
        proxy_handler = next(handler for handler in opener.handlers if isinstance(handler, ProxyHandler))
        self.assertEqual(proxy_handler.proxies, {
            "http": "http://127.0.0.1:7891",
            "https": "http://127.0.0.1:7891",
        })
        request = Request("https://openalex.s3.amazonaws.com/data.jsonl")
        proxy_handler.proxy_open(request, "http://127.0.0.1:7891", "https")
        self.assertEqual(request.host, "127.0.0.1:7891")
        self.assertEqual(request._tunnel_host, "openalex.s3.amazonaws.com")
        for proxy in ("http://203.0.113.8:7891", "http://user:secret@127.0.0.1:7891"):
            with patch.dict(os.environ, {"OPENALEX_DOWNLOAD_PROXY": proxy}):
                with self.assertRaises(SnapshotError) as raised:
                    _opener()
                self.assertNotIn("secret", str(raised.exception))

    def test_download_mode_override_is_atomic_and_takes_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "catalog.sqlite"
            control = root / "openalex-download-control.json"
            with patch.dict(os.environ, {
                "OPENALEX_CATALOG_DB": str(database),
                "OPENALEX_DOWNLOAD_PROXY": "http://127.0.0.1:7891",
                "OPENALEX_DOWNLOAD_CONTROL_FILE": str(control),
            }):
                with patch("app.openalex_snapshot.os.geteuid", return_value=0):
                    with patch("app.openalex_snapshot._assert_secure_control_path", side_effect=lambda path: path.exists()):
                        self.assertEqual(download_mode_status(), {
                            "effective_mode": "proxy",
                            "source": "startup_environment",
                            "proxy_configured": True,
                        })
                        direct = set_download_mode("direct")
                        self.assertEqual(direct["effective_mode"], "direct")
                        self.assertEqual(direct["source"], "runtime_override")
                        self.assertEqual(control.read_text(), '{"mode":"direct"}\n')
                        self.assertEqual(control.stat().st_mode & 0o777, 0o644)
                        self.assertIsNone(_download_proxy())
                        proxy = set_download_mode("proxy")
                        self.assertEqual(proxy["effective_mode"], "proxy")
                        self.assertEqual(_download_proxy(), "http://127.0.0.1:7891")

    def test_download_mode_mutation_requires_root_before_path_access(self):
        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory) / "control.json"
            with patch.dict(os.environ, {"OPENALEX_DOWNLOAD_CONTROL_FILE": str(control)}):
                with patch("app.openalex_snapshot.os.geteuid", return_value=1000):
                    with self.assertRaisesRegex(SnapshotError, "requires root"):
                        set_download_mode("direct")
            self.assertFalse(control.exists())

    def test_download_mode_status_remains_read_only_for_service_account(self):
        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory) / "control.json"
            control.write_text('{"mode":"direct"}\n')
            with patch.dict(os.environ, {"OPENALEX_DOWNLOAD_CONTROL_FILE": str(control)}):
                with patch("app.openalex_snapshot.os.geteuid", return_value=1000):
                    with patch("app.openalex_snapshot._assert_secure_control_path", return_value=True):
                        status = download_mode_status()
            self.assertEqual(status["effective_mode"], "direct")
            self.assertEqual(status["source"], "runtime_override")
            self.assertEqual(control.read_text(), '{"mode":"direct"}\n')

    def test_download_control_path_requires_root_owned_nonwritable_directory_and_file(self):
        path = Path("/etc/w/control.json")
        safe_directory = SimpleNamespace(st_uid=0, st_mode=stat.S_IFDIR | 0o755)
        safe_file = SimpleNamespace(st_uid=0, st_mode=stat.S_IFREG | 0o644)
        safe_directories = [safe_directory] * 3
        with patch.object(Path, "lstat", side_effect=[*safe_directories, safe_file]):
            self.assertTrue(_assert_secure_control_path(path))
        with patch.object(Path, "lstat", side_effect=[*safe_directories, SimpleNamespace(
            st_uid=1000, st_mode=stat.S_IFREG | 0o644,
        )]):
            with self.assertRaisesRegex(SnapshotError, "file must be root-owned"):
                _assert_secure_control_path(path)
        with patch.object(Path, "lstat", side_effect=[*safe_directories, SimpleNamespace(
            st_uid=0, st_mode=stat.S_IFREG | 0o664,
        )]):
            with self.assertRaisesRegex(SnapshotError, "file must be root-owned"):
                _assert_secure_control_path(path)
        unsafe_directory = SimpleNamespace(st_uid=0, st_mode=stat.S_IFDIR | 0o775)
        with patch.object(Path, "lstat", return_value=unsafe_directory):
            with self.assertRaisesRegex(SnapshotError, "directories must be root-owned"):
                _assert_secure_control_path(path)

    def test_download_mode_rejects_corrupt_control_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            control = root / "control.json"
            control.write_text("not-json")
            with patch.dict(os.environ, {"OPENALEX_DOWNLOAD_CONTROL_FILE": str(control)}):
                with patch("app.openalex_snapshot._assert_secure_control_path", return_value=True):
                    with self.assertRaisesRegex(SnapshotError, "control file"):
                        _download_proxy()

    def test_benchmark_downloads_identical_verified_shard_in_balanced_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "catalog.sqlite"
            raw = root / "raw"
            raw.mkdir()
            initialize(database)
            source = "s3://openalex/data/jsonl/works/part_0000.gz"
            payload = gzip.compress(b'{"id":"https://openalex.org/W1"}\n')
            shard = raw / "2026-09-23" / "works" / "part_0000.gz"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(payload)
            with connect(database) as db:
                db.execute(
                    "INSERT INTO openalex_snapshot_files"
                    "(dataset_version,source_url,entity,expected_bytes,expected_records,verified,indexed,actual_records) "
                    "VALUES('2026-09-23',?,'works',?,1,1,1,1)",
                    (source, len(payload)),
                )
            calls = []

            def fake_download(source_url, destination, expected_bytes, reserve, **kwargs):
                calls.append((source_url, kwargs))
                destination.write_bytes(payload)

            with patch.dict(os.environ, {
                "OPENALEX_DOWNLOAD_PROXY": "http://127.0.0.1:7891",
                "OPENALEX_DOWNLOAD_CONNECTIONS": "16",
            }):
                with patch("app.openalex_snapshot.shutil.which", return_value="/usr/bin/axel"):
                    with patch("app.openalex_snapshot._download_axel", side_effect=fake_download):
                        result = benchmark_download_modes(database, raw, 0)
            self.assertEqual([call[1]["mode_override"] for call in calls], [
                "direct", "proxy", "proxy", "direct",
            ])
            self.assertTrue(result["all_runs_valid"])
            self.assertEqual(result["connections"], 16)
            self.assertEqual(result["summary"]["direct"]["successful_runs"], 2)
            self.assertEqual(result["summary"]["proxy"]["successful_runs"], 2)

    def test_axel_preallocated_part_is_not_promoted_until_success(self):
        for has_marker in (False, True):
            with self.subTest(has_marker=has_marker), tempfile.TemporaryDirectory() as directory:
                target = Path(directory) / "preallocated.gz"
                part = target.with_name(target.name + ".part")
                part.write_bytes(b"12345678")
                if has_marker:
                    target.with_name(target.name + ".part.axel").touch()
                part.with_name(part.name + ".st").write_text("axelstate")
                calls = []
                with patch.dict(os.environ, {"OPENALEX_DOWNLOAD_CONNECTIONS": "16"}):
                    with patch("app.openalex_snapshot.shutil.which", return_value="/usr/bin/axel"):
                        with patch("app.openalex_snapshot.time.sleep"):
                            with patch("app.openalex_snapshot.subprocess.run", side_effect=lambda cmd, **kw: calls.append(cmd) or SimpleNamespace(returncode=1)):
                                with self.assertRaises(SnapshotError):
                                    _download("s3://openalex/data/jsonl/works/preallocated.gz", target, 8, 0)
                self.assertEqual(len(calls), 3)
                self.assertFalse(target.exists())
                self.assertTrue(part.exists())

    def test_axel_missing_binary_fails_without_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "fresh.gz"
            with patch("app.openalex_snapshot.shutil.which", return_value=None):
                with self.assertRaisesRegex(SnapshotError, "Axel is required"):
                    _download("s3://openalex/data/jsonl/works/fresh.gz", target, 8, 0)

    def test_manifest_rejects_outside_bucket_and_inconsistent_totals(self):
        manifest = {
            "date": "2026-09-23",
            "meta": {"record_count": 1, "content_length": 8},
            "entities": [{
                "entity": "works", "files": [{
                    "url": "s3://openalex/data/jsonl/works/part.gz",
                    "meta": {"record_count": 1, "content_length": 8},
                }],
            }],
        }
        self.assertEqual(_validate_manifest(json.dumps(manifest).encode())["date"], "2026-09-23")
        manifest["entities"][0]["files"][0]["url"] = "s3://other/data/jsonl/works/part.gz"
        with self.assertRaises(SnapshotError):
            _validate_manifest(json.dumps(manifest).encode())

    def test_complete_dataset_resumes_frozen_manifest_without_refetch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            snapshot = raw / "2026-09-23"
            snapshot.mkdir(parents=True)
            manifest = {
                "date": "2026-09-23",
                "meta": {"record_count": 1, "content_length": 8},
                "entities": [{
                    "entity": "works", "record_count": 1, "content_length": 8,
                    "files": [{
                        "url": "s3://openalex/data/jsonl/works/part.gz",
                        "meta": {"record_count": 1, "content_length": 8},
                    }],
                }],
            }
            (snapshot / "manifest.json").write_text(json.dumps(manifest))
            database = root / "catalog.sqlite"
            initialize(database)
            with connect(database) as db:
                db.execute(
                    "INSERT INTO openalex_snapshot_state"
                    "(singleton,dataset_version,state,complete,expected_records,expected_files) "
                    "VALUES(1,'2026-09-23','complete',1,1,1)"
                )
            with patch("app.openalex_snapshot._read_manifest", side_effect=AssertionError("must use frozen manifest")):
                result = sync(database, raw, 0)
            self.assertTrue(result["complete"])
            self.assertEqual(result["dataset_version"], "2026-09-23")

    def test_failed_partial_dataset_resumes_its_frozen_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            snapshot = raw / "2026-09-23"
            snapshot.mkdir(parents=True)
            source = "s3://openalex/data/jsonl/works/part.gz"
            record = {"id": "https://openalex.org/W789", "title": "Resumed work"}
            archive = gzip.compress((json.dumps(record) + "\n").encode())
            manifest = {
                "date": "2026-09-23",
                "meta": {"record_count": 1, "content_length": len(archive)},
                "entities": [{
                    "entity": "works", "record_count": 1, "content_length": len(archive),
                    "files": [{
                        "url": source,
                        "meta": {"record_count": 1, "content_length": len(archive)},
                    }],
                }],
            }
            (snapshot / "manifest.json").write_text(json.dumps(manifest))
            database = root / "catalog.sqlite"
            initialize(database)
            with connect(database) as db:
                db.execute(
                    "INSERT INTO openalex_snapshot_state"
                    "(singleton,dataset_version,state,complete,expected_records,expected_files) "
                    "VALUES(1,'2026-09-23','failed',0,1,1)"
                )

            def restore_shard(_url, target, expected_bytes, _reserve):
                self.assertEqual(expected_bytes, len(archive))
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive)

            with patch("app.openalex_snapshot._read_manifest", side_effect=AssertionError("must resume frozen manifest")):
                with patch("app.openalex_snapshot._download", side_effect=restore_shard):
                    with patch("app.openalex_snapshot._support_assets", return_value=[]):
                        result = sync(database, raw, 0)
            self.assertTrue(result["complete"])
            self.assertEqual(OpenAlexCatalog(database).by_id("W789")["work"]["title"], "Resumed work")

    def test_complete_snapshot_support_assets_use_frozen_paths_and_gate_readiness(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            database = root / "catalog.sqlite"
            version = "2026-09-23"
            sources = [
                "s3://openalex/data/jsonl/works/part_0000.gz",
                "s3://openalex/data/jsonl/works/part_0001.gz",
            ]
            records = [
                {"id": "https://openalex.org/W987", "title": "Local snapshot row"},
                {"id": "https://openalex.org/W988", "title": "Second indexed row"},
            ]
            works_archives = [gzip.compress((json.dumps(record) + "\n").encode()) for record in records]
            authors_source = "s3://openalex/data/jsonl/authors/part_0000.gz"
            authors_archive = gzip.compress(b'{"id":"https://openalex.org/A1"}\n')
            works_manifest = json.dumps({
                "date": version, "entity": "works", "record_count": 2,
                "content_length": sum(map(len, works_archives)),
            }).encode()
            authors_manifest = json.dumps({
                "date": version, "entity": "authors", "record_count": 1,
                "content_length": len(authors_archive),
            }).encode()
            deleted_ids = gzip.compress(b"openalex_id\nW999\n")
            assets = [
                {"url": "s3://openalex/data/jsonl/manifest.json", "bytes": 100, "records": 0},
                {"url": "s3://openalex/LICENSE.txt", "bytes": 7, "records": 0},
                {"url": "s3://openalex/README.txt", "bytes": 7, "records": 0},
                {"url": "s3://openalex/RELEASE_NOTES.txt", "bytes": 7, "records": 0},
                {"url": "s3://openalex/data/jsonl/works/manifest.json", "bytes": len(works_manifest), "records": 0},
                {"url": "s3://openalex/data/jsonl/authors/manifest.json", "bytes": len(authors_manifest), "records": 0},
                {"url": "s3://openalex/data/jsonl/works/deleted_ids.csv.gz", "bytes": len(deleted_ids), "records": 0},
            ]
            manifest = {
                "date": version,
                "meta": {"record_count": 3, "content_length": sum(map(len, works_archives)) + len(authors_archive)},
                "entities": [{
                    "entity": "works",
                    "record_count": 2,
                    "content_length": sum(map(len, works_archives)),
                    "files": [
                        {"url": source, "meta": {"record_count": 1, "content_length": len(archive)}}
                        for source, archive in zip(sources, works_archives)
                    ],
                }, {
                    "entity": "authors",
                    "record_count": 1,
                    "content_length": len(authors_archive),
                    "files": [{
                        "url": authors_source,
                        "meta": {"record_count": 1, "content_length": len(authors_archive)},
                    }],
                }],
            }
            manifest_bytes = json.dumps(manifest).encode()
            assets[0]["bytes"] = len(manifest_bytes)
            events = []

            def download(source_url, target, expected_bytes, _reserve):
                events.append(("download", source_url))
                if source_url in sources:
                    payload = works_archives[sources.index(source_url)]
                elif source_url == authors_source:
                    payload = authors_archive
                elif source_url.endswith("/works/manifest.json"):
                    payload = works_manifest
                elif source_url.endswith("/authors/manifest.json"):
                    payload = authors_manifest
                elif source_url.endswith("/deleted_ids.csv.gz"):
                    payload = deleted_ids
                else:
                    payload = b"asset!\n"
                self.assertEqual(len(payload), expected_bytes)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(payload)

            real_index = _index_works

            def index_after_raw_ready(path, db, dataset_version, expected, db_path, reserve):
                events.append(("index", path.name))
                status = OpenAlexCatalog(database).status()
                self.assertIs(status["complete"], False)
                self.assertEqual(status["verified_files"], status["expected_files"])
                self.assertEqual(status["verified_records"], 3)
                indexed_before = db.execute(
                    "SELECT coalesce(sum(actual_records),0) FROM openalex_snapshot_files "
                    "WHERE dataset_version=? AND entity='works' AND indexed=1",
                    (version,),
                ).fetchone()[0]
                self.assertEqual(status["imported_works"], indexed_before)
                return real_index(path, db, dataset_version, expected, db_path, reserve)

            with patch("app.openalex_snapshot._read_manifest", return_value=manifest_bytes):
                with patch("app.openalex_snapshot._support_assets", return_value=assets):
                    with patch("app.openalex_snapshot._download", side_effect=download):
                        with patch("app.openalex_snapshot._index_works", side_effect=index_after_raw_ready):
                            result = sync(database, raw, 0)
            self.assertTrue(result["complete"])
            self.assertEqual(result["verified_files"], 10)
            first_index = next(index for index, (kind, _) in enumerate(events) if kind == "index")
            self.assertTrue(all(kind == "download" for kind, _ in events[:first_index]))
            self.assertEqual(OpenAlexCatalog(database).by_id("W987")["work"]["title"], "Local snapshot row")
            self.assertEqual(
                _asset_path(raw, version, "s3://openalex/data/jsonl/manifest.json"),
                raw / version / "manifest.json",
            )
            self.assertTrue((raw / version / "assets" / "LICENSE.txt").is_file())
            self.assertTrue((raw / version / "works" / "manifest.json").is_file())
            self.assertTrue((raw / version / "works" / "deleted_ids.csv.gz").is_file())

            # Simulate a restart after the first works shard committed its index.
            # Raw verification remains complete, and the first shard must not be indexed again.
            with connect(database) as db:
                db.execute("UPDATE openalex_snapshot_state SET state='failed',complete=0")
                db.execute(
                    "UPDATE openalex_snapshot_files SET indexed=0 WHERE dataset_version=? AND source_url=?",
                    (version, sources[1]),
                )
            events.clear()
            with patch("app.openalex_snapshot._read_manifest", side_effect=AssertionError("must use frozen manifest")):
                with patch("app.openalex_snapshot._support_assets", return_value=assets):
                    with patch("app.openalex_snapshot._download", side_effect=AssertionError("raw archive was already verified")):
                        with patch("app.openalex_snapshot._index_works", side_effect=index_after_raw_ready):
                            resumed = sync(database, raw, 0)
            self.assertTrue(resumed["complete"])
            self.assertEqual([event for event in events if event[0] == "index"], [("index", "part_0001.gz")])

    def test_snapshot_state_progress_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "catalog.sqlite"
            initialize(database)
            manifest = {
                "date": "2026-09-23",
                "meta": {"record_count": 2, "content_length": 12},
                "entities": [{
                    "entity": "works", "record_count": 2, "content_length": 12,
                    "files": [{
                        "url": "s3://openalex/data/jsonl/works/part.gz",
                        "meta": {"record_count": 2, "content_length": 12},
                    }],
                }],
            }
            with connect(database) as db:
                _ensure_state(db, manifest)
            status = OpenAlexCatalog(database).status()
            self.assertEqual(status["state"], "syncing")
            self.assertIs(status["complete"], False)
            self.assertEqual(status["expected_records"], 2)


if __name__ == "__main__":
    unittest.main()
