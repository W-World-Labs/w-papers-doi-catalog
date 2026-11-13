"""Resumable full-snapshot downloader and local works indexer.

The public OpenAlex API is never queried by the runtime catalog. This command
stores every manifest shard as its original compressed JSONL and builds a local
title/abstract/DOI index from the works entity.
"""

from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import ipaddress
import json
import os
import re
import shutil
import sqlite3
import statistics
import stat
import subprocess
import tempfile
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path, PurePosixPath
from typing import Any

from app.openalex_catalog import abstract_from_index, connect, initialize, normalize_doi


MANIFEST_URL = "https://openalex.s3.amazonaws.com/data/jsonl/manifest.json"
MANIFEST_MAX_BYTES = 32 * 1024 * 1024
CHUNK_BYTES = 1024 * 1024
COMMIT_RECORDS = 5000
DOWNLOAD_CONTROL_FILE = Path("/etc/w/w-papers-openalex-download-mode.json")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ENTITY = re.compile(r"^[a-z][a-z0-9-]{0,63}$")


class SnapshotError(RuntimeError):
    pass


class S3Redirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parsed = urllib.parse.urlsplit(newurl)
        host = (parsed.hostname or "").lower()
        if parsed.scheme != "https" or not (
            host == "openalex.s3.amazonaws.com"
            or (host.startswith("openalex.s3.") and host.endswith(".amazonaws.com"))
        ):
            raise SnapshotError("snapshot redirect left the OpenAlex S3 host")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class ExplicitLoopbackProxyHandler(urllib.request.ProxyHandler):
    def proxy_open(self, req, proxy, _type):
        parsed = urllib.parse.urlsplit(proxy)
        req.set_proxy(parsed.netloc, "http")
        return None


def _configured_download_proxy() -> str | None:
    proxy = os.environ.get("OPENALEX_DOWNLOAD_PROXY", "").strip()
    if not proxy:
        return None
    try:
        parsed = urllib.parse.urlsplit(proxy)
        host = ipaddress.ip_address(parsed.hostname or "")
        port = parsed.port
    except ValueError as exc:
        raise SnapshotError("OPENALEX_DOWNLOAD_PROXY must be a loopback HTTP URL with a valid port") from exc
    if (
        parsed.scheme != "http"
        or not host.is_loopback
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise SnapshotError("OPENALEX_DOWNLOAD_PROXY must be a credential-free HTTP proxy on loopback")
    return proxy


def _download_control_path() -> Path | None:
    configured = os.environ.get("OPENALEX_DOWNLOAD_CONTROL_FILE", "").strip()
    if not configured and not DOWNLOAD_CONTROL_FILE.parent.is_dir():
        return None
    path = Path(configured) if configured else DOWNLOAD_CONTROL_FILE
    if not path.is_absolute():
        raise SnapshotError("OPENALEX_DOWNLOAD_CONTROL_FILE must be an absolute path")
    return path


def _assert_secure_control_path(path: Path) -> bool:
    try:
        directories = (path.parent, *path.parent.parents)
        for directory in directories:
            parent = directory.lstat()
            if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != 0 or parent.st_mode & 0o022:
                raise SnapshotError("download control directories must be root-owned and not group/world-writable")
    except OSError as exc:
        raise SnapshotError("download control directory is unavailable") from exc
    try:
        control = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise SnapshotError("download control file is unavailable") from exc
    if (
        not stat.S_ISREG(control.st_mode)
        or control.st_uid != 0
        or control.st_mode & 0o022
        or not control.st_mode & 0o044
    ):
        raise SnapshotError("download control file must be root-owned, readable, and not group/world-writable")
    return True


def _read_download_mode() -> tuple[str | None, str]:
    path = _download_control_path()
    if path is None or not _assert_secure_control_path(path):
        return None, "startup_environment"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotError("download mode control file is unreadable or invalid") from exc
    mode = value.get("mode") if isinstance(value, dict) else None
    if mode not in ("direct", "proxy"):
        raise SnapshotError("download mode control file must contain mode direct or proxy")
    return mode, "runtime_override"


def _download_proxy(mode_override: str | None = None) -> str | None:
    mode, _ = _read_download_mode() if mode_override is None else (mode_override, "benchmark")
    if mode == "direct":
        return None
    if mode not in (None, "proxy"):
        raise SnapshotError("download mode must be direct or proxy")
    proxy = _configured_download_proxy()
    if mode == "proxy" and proxy is None:
        raise SnapshotError("proxy mode requires OPENALEX_DOWNLOAD_PROXY to be configured")
    return proxy


def set_download_mode(mode: str) -> dict[str, str]:
    if os.geteuid() != 0:
        raise SnapshotError("changing the OpenAlex download mode requires root")
    if mode not in ("direct", "proxy"):
        raise SnapshotError("download mode must be direct or proxy")
    if mode == "proxy" and _configured_download_proxy() is None:
        raise SnapshotError("proxy mode requires OPENALEX_DOWNLOAD_PROXY to be configured")
    path = _download_control_path()
    if path is None:
        raise SnapshotError("root-owned download control directory is not available")
    _assert_secure_control_path(path)
    payload = json.dumps({"mode": mode}, separators=(",", ":")) + "\n"
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=path.name + ".", suffix=".tmp", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            os.chmod(temporary_path, 0o644)
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
    except OSError as exc:
        raise SnapshotError(f"could not update download mode: {type(exc).__name__}") from exc
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return download_mode_status()


def download_mode_status() -> dict[str, str | bool]:
    override, source = _read_download_mode()
    proxy_configured = _configured_download_proxy() is not None
    mode = override or ("proxy" if proxy_configured else "direct")
    if override == "proxy" and not proxy_configured:
        raise SnapshotError("proxy mode requires OPENALEX_DOWNLOAD_PROXY to be configured")
    return {
        "effective_mode": mode,
        "source": source,
        "proxy_configured": proxy_configured,
    }


def _opener() -> urllib.request.OpenerDirector:
    proxy = _download_proxy()
    proxies = {"http": proxy, "https": proxy} if proxy else {}
    handler = ExplicitLoopbackProxyHandler(proxies) if proxy else urllib.request.ProxyHandler({})
    return urllib.request.build_opener(handler, S3Redirects())


def _read_manifest() -> bytes:
    request = urllib.request.Request(
        MANIFEST_URL,
        headers={"User-Agent": "W-Papers-OpenAlex-Snapshot/1.0", "Accept": "application/json"},
    )
    with _opener().open(request, timeout=30) as response:
        if response.status != 200:
            raise SnapshotError(f"manifest request returned HTTP {response.status}")
        body = response.read(MANIFEST_MAX_BYTES + 1)
    if len(body) > MANIFEST_MAX_BYTES:
        raise SnapshotError("snapshot manifest exceeded its size limit")
    return body


def _s3_object_url(key: str) -> str:
    if not key or key.startswith("/") or ".." in PurePosixPath(key).parts:
        raise SnapshotError("unsafe OpenAlex S3 object key")
    return "s3://openalex/" + key


def _head_length(source_url: str, optional: bool = False) -> int | None:
    request = urllib.request.Request(
        _url(source_url), method="HEAD",
        headers={"User-Agent": "W-Papers-OpenAlex-Snapshot/1.0"},
    )
    for attempt in range(3):
        try:
            with _opener().open(request, timeout=30) as response:
                if response.status != 200:
                    raise SnapshotError(f"OpenAlex asset returned HTTP {response.status}")
                length = response.headers.get("Content-Length")
                if not length or not length.isdigit() or int(length) <= 0:
                    raise SnapshotError("OpenAlex asset has no valid Content-Length")
                return int(length)
        except urllib.error.HTTPError as exc:
            if optional and exc.code == 404:
                return None
            if attempt >= 2:
                raise SnapshotError(f"OpenAlex asset HEAD failed: HTTP {exc.code}") from exc
        except (OSError, urllib.error.URLError, TimeoutError) as exc:
            if attempt >= 2:
                raise SnapshotError(f"OpenAlex asset HEAD failed: {type(exc).__name__}") from exc
        time.sleep(1 + attempt)
    return None


def _validate_manifest(body: bytes) -> dict[str, Any]:
    try:
        manifest = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotError("snapshot manifest is not valid JSON") from exc
    date = manifest.get("date") if isinstance(manifest, dict) else None
    if not isinstance(date, str) or not _DATE.fullmatch(date):
        raise SnapshotError("snapshot manifest has an invalid date")
    entities = manifest.get("entities")
    if not isinstance(entities, list) or not entities:
        raise SnapshotError("snapshot manifest has no entities")
    urls: set[str] = set()
    files_count = records_count = bytes_count = 0
    for entity in entities:
        if not isinstance(entity, dict) or not isinstance(entity.get("entity"), str):
            raise SnapshotError("snapshot manifest contains an invalid entity")
        if not _ENTITY.fullmatch(entity["entity"]):
            raise SnapshotError("snapshot manifest contains an unsafe entity name")
        files = entity.get("files")
        if not isinstance(files, list):
            raise SnapshotError("snapshot manifest entity is missing files")
        for item in files:
            url = item.get("url") if isinstance(item, dict) else None
            meta = item.get("meta") if isinstance(item, dict) else None
            if not isinstance(url, str) or not isinstance(meta, dict):
                raise SnapshotError("snapshot manifest contains an invalid shard")
            parsed = urllib.parse.urlsplit(url)
            path = PurePosixPath(parsed.path)
            if (
                parsed.scheme != "s3"
                or parsed.netloc != "openalex"
                or not parsed.path.startswith("/data/jsonl/")
                or ".." in path.parts
                or not path.name.endswith(".gz")
            ):
                raise SnapshotError("snapshot shard is outside the official JSONL bucket path")
            if url in urls:
                raise SnapshotError("snapshot manifest contains a duplicate shard")
            size = meta.get("content_length")
            records = meta.get("record_count")
            if not isinstance(size, int) or size <= 0 or not isinstance(records, int) or records < 0:
                raise SnapshotError("snapshot shard has invalid size or row count")
            urls.add(url)
            files_count += 1
            records_count += records
            bytes_count += size
    if not files_count or not any(e["entity"] == "works" for e in entities):
        raise SnapshotError("snapshot manifest is missing works shards")
    meta = manifest.get("meta") or {}
    if meta.get("record_count") != records_count or meta.get("content_length") != bytes_count:
        raise SnapshotError("snapshot manifest totals do not match the shard list")
    return manifest


def _save_manifest(raw_root: Path, manifest_bytes: bytes, date: str) -> dict[str, Any]:
    directory = raw_root / date
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "manifest.json"
    if path.exists():
        existing = path.read_bytes()
        try:
            frozen = _validate_manifest(existing)
        except SnapshotError:
            raise SnapshotError(f"frozen manifest is invalid: {path}")
        if frozen["date"] != date:
            raise SnapshotError("frozen manifest directory does not match its date")
        return frozen
    temp = path.with_suffix(".json.part")
    with temp.open("wb") as stream:
        stream.write(manifest_bytes)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    return _validate_manifest(manifest_bytes)


def _shard_path(raw_root: Path, date: str, entity: str, source_url: str) -> Path:
    path = PurePosixPath(urllib.parse.urlsplit(source_url).path)
    relative = path.relative_to(PurePosixPath("/data/jsonl") / entity)
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise SnapshotError("unsafe shard path")
    return raw_root / date / entity / Path(*relative.parts)


def _asset_path(raw_root: Path, date: str, source_url: str) -> Path:
    key = PurePosixPath(urllib.parse.urlsplit(source_url).path)
    key_parts = key.parts[1:] if key.is_absolute() else key.parts
    if not key_parts or ".." in key_parts:
        raise SnapshotError("unsafe OpenAlex asset path")
    if key_parts[:2] == ("data", "jsonl"):
        relative = key_parts[2:]
    else:
        relative = ("assets",) + key_parts
    if not relative:
        raise SnapshotError("empty OpenAlex asset path")
    return raw_root / date / Path(*relative)


def _support_assets(raw_root: Path, version: str, manifest: dict[str, Any], db: sqlite3.Connection) -> list[dict[str, Any]]:
    frozen_manifest = raw_root / version / "manifest.json"
    assets: list[dict[str, Any]] = [{
        "url": MANIFEST_URL.replace("https://openalex.s3.amazonaws.com/", "s3://openalex/"),
        "entity": "assets", "bytes": frozen_manifest.stat().st_size, "records": 0,
    }]
    keys = ["LICENSE.txt", "README.txt", "RELEASE_NOTES.txt"]
    keys.extend(f"data/jsonl/{entity['entity']}/manifest.json" for entity in manifest["entities"])
    keys.append("data/jsonl/works/deleted_ids.csv.gz")
    existing = {
        row["source_url"]: row["expected_bytes"]
        for row in db.execute(
            "SELECT source_url,expected_bytes FROM openalex_snapshot_files "
            "WHERE dataset_version=? AND entity='assets'", (version,),
        )
    }
    for key in keys:
        source = _s3_object_url(key)
        if source in existing:
            length = existing[source]
        else:
            length = _head_length(source, optional=(key.endswith("/deleted_ids.csv.gz")))
            if length is None:
                continue
        assets.append({"url": source, "entity": "assets", "bytes": length, "records": 0})
    return assets


def _validate_asset(path: Path, source_url: str, manifest: dict[str, Any]) -> None:
    key = PurePosixPath(urllib.parse.urlsplit(source_url).path).as_posix()
    if key.endswith("/deleted_ids.csv.gz"):
        if _record_count(path) < 1:
            raise SnapshotError("OpenAlex deleted-ID archive is empty")
        return
    if key.endswith("/manifest.json"):
        if key == "/data/jsonl/manifest.json":
            frozen = _validate_manifest(path.read_bytes())
            if frozen["date"] != manifest["date"]:
                raise SnapshotError("OpenAlex frozen top-level manifest date changed")
            return
        try:
            entity_manifest = json.loads(path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SnapshotError("OpenAlex entity manifest is invalid JSON") from exc
        entity = key.split("/")[-2]
        expected = next((item for item in manifest["entities"] if item["entity"] == entity), None)
        if (
            expected is None
            or entity_manifest.get("date") != manifest["date"]
            or entity_manifest.get("entity") != entity
            or entity_manifest.get("record_count") != expected.get("record_count")
            or entity_manifest.get("content_length") != expected.get("content_length")
        ):
            raise SnapshotError("OpenAlex entity manifest does not match the frozen snapshot")
        return
    if not path.is_file() or path.stat().st_size == 0:
        raise SnapshotError("OpenAlex snapshot documentation asset is empty")


def _url(source_url: str) -> str:
    parsed = urllib.parse.urlsplit(source_url)
    key = parsed.path.lstrip("/")
    return "https://openalex.s3.amazonaws.com/" + urllib.parse.quote(key, safe="/=-_")


def _download_urllib(source_url: str, destination: Path, expected_bytes: int, min_free_bytes: int) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".part")
    etag_path = destination.with_name(destination.name + ".part.etag")
    for attempt in range(3):
        free = shutil.disk_usage(destination.parent).free
        offset = part.stat().st_size if part.exists() else 0
        if offset == expected_bytes:
            os.replace(part, destination)
            return
        remaining_bytes = max(0, expected_bytes - offset)
        if free - remaining_bytes < min_free_bytes:
            raise SnapshotError(
                f"disk free-space reserve would fall below {min_free_bytes} bytes"
            )
        if offset > expected_bytes:
            part.unlink()
            offset = 0
        headers = {"User-Agent": "W-Papers-OpenAlex-Snapshot/1.0", "Accept-Encoding": "identity"}
        if offset:
            headers["Range"] = f"bytes={offset}-"
            if etag_path.exists():
                headers["If-Range"] = etag_path.read_text(encoding="ascii")
        request = urllib.request.Request(_url(source_url), headers=headers)
        try:
            with _opener().open(request, timeout=90) as response:
                if offset and response.status == 206:
                    content_range = response.headers.get("Content-Range", "")
                    if not content_range.startswith(f"bytes {offset}-"):
                        raise SnapshotError("S3 returned an invalid byte-range response")
                    response_etag = response.headers.get("ETag")
                    previous_etag = etag_path.read_text(encoding="ascii") if etag_path.exists() else None
                    if previous_etag and response_etag and response_etag != previous_etag:
                        part.unlink(missing_ok=True)
                        etag_path.unlink(missing_ok=True)
                        raise SnapshotError("S3 object changed during a resumed download")
                    mode = "ab"
                elif response.status == 200:
                    mode = "wb"
                    offset = 0
                else:
                    raise SnapshotError(f"S3 shard returned HTTP {response.status}")
                response_etag = response.headers.get("ETag")
                if response_etag:
                    etag_path.write_text(response_etag, encoding="ascii")
                with part.open(mode) as stream:
                    received = offset
                    while True:
                        chunk = response.read(CHUNK_BYTES)
                        if not chunk:
                            break
                        if received + len(chunk) > expected_bytes:
                            raise SnapshotError("S3 shard exceeded the manifest byte limit")
                        stream.write(chunk)
                        received += len(chunk)
                    stream.flush()
                    os.fsync(stream.fileno())
            if part.stat().st_size != expected_bytes:
                raise SnapshotError("downloaded shard length does not match manifest")
            os.replace(part, destination)
            etag_path.unlink(missing_ok=True)
            return
        except SnapshotError:
            if attempt >= 2:
                raise
        except (OSError, urllib.error.URLError, TimeoutError) as exc:
            if attempt >= 2:
                raise SnapshotError(f"shard download failed after bounded retries: {type(exc).__name__}") from exc
        time.sleep(1 + attempt)
    raise SnapshotError("shard download failed")


def _download_axel(
    source_url: str,
    destination: Path,
    expected_bytes: int,
    min_free_bytes: int,
    *,
    mode_override: str | None = None,
    max_attempts: int = 3,
    timeout_seconds: int | None = None,
) -> None:
    part = destination.with_name(destination.name + ".part")
    marker = destination.with_name(destination.name + ".part.axel")
    state_path = part.with_name(part.name + ".st")
    free = shutil.disk_usage(destination.parent).free
    offset = part.stat().st_size if part.exists() else 0
    if offset > expected_bytes:
        part.unlink()
        marker.unlink(missing_ok=True)
        state_path.unlink(missing_ok=True)
        offset = 0
    axel_incomplete = marker.exists() or state_path.exists()
    if offset == expected_bytes and not axel_incomplete:
        os.replace(part, destination)
        return
    allocated = min(expected_bytes, part.stat().st_blocks * 512) if part.exists() else 0
    if free - (expected_bytes - allocated) < min_free_bytes:
        raise SnapshotError(f"disk free-space reserve would fall below {min_free_bytes} bytes")

    # Older versions used urllib byte-range partials. Keep resuming those with
    # the same downloader; Axel's own sidecar state is only used for new parts.
    if part.exists() and not axel_incomplete:
        _download_urllib(source_url, destination, expected_bytes, min_free_bytes)
        return

    executable = shutil.which("axel")
    if executable is None:
        raise SnapshotError("Axel is required for snapshot downloads but is not installed")
    connections_raw = os.environ.get("OPENALEX_DOWNLOAD_CONNECTIONS", "16")
    try:
        connections = int(connections_raw)
    except ValueError as exc:
        raise SnapshotError("OPENALEX_DOWNLOAD_CONNECTIONS must be an integer from 1 to 32") from exc
    if not 1 <= connections <= 32:
        raise SnapshotError("OPENALEX_DOWNLOAD_CONNECTIONS must be an integer from 1 to 32")

    if not 1 <= max_attempts <= 3:
        raise SnapshotError("Axel attempts must be from 1 to 3")
    marker.touch(exist_ok=True)
    timeout = timeout_seconds or min(6 * 60 * 60, max(300, 120 + expected_bytes // (256 * 1024)))
    proxy_variables = (
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "FTP_PROXY", "http_proxy", "https_proxy",
        "all_proxy", "ftp_proxy", "NO_PROXY", "no_proxy",
    )
    for attempt in range(max_attempts):
        proxy = _download_proxy(mode_override)
        child_env = {key: value for key, value in os.environ.items() if key not in proxy_variables}
        command = [executable]
        if proxy is None:
            command.append("-N")
        else:
            child_env.update({
                "HTTP_PROXY": proxy,
                "HTTPS_PROXY": proxy,
                "http_proxy": proxy,
                "https_proxy": proxy,
                "NO_PROXY": "",
                "no_proxy": "",
            })
        command.extend(["-n", str(connections), "-T", "90", "-o", str(part), _url(source_url)])
        try:
            result = subprocess.run(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=timeout,
                env=child_env,
            )
        except subprocess.TimeoutExpired:
            if attempt >= max_attempts - 1:
                raise SnapshotError("Axel download exceeded its bounded timeout")
        except OSError as exc:
            raise SnapshotError(f"Axel download could not run: {type(exc).__name__}") from exc
        else:
            if result.returncode == 0:
                if not part.is_file() or part.stat().st_size != expected_bytes:
                    if attempt >= max_attempts - 1:
                        raise SnapshotError("Axel output length does not match the frozen manifest")
                else:
                    os.replace(part, destination)
                    marker.unlink(missing_ok=True)
                    state_path.unlink(missing_ok=True)
                    return
            elif attempt >= max_attempts - 1:
                raise SnapshotError(f"Axel download failed with exit status {result.returncode}")
        if attempt < max_attempts - 1:
            time.sleep(1 + attempt)
    raise SnapshotError("Axel download failed after bounded retries")


def _download(source_url: str, destination: Path, expected_bytes: int, min_free_bytes: int) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    _download_axel(source_url, destination, expected_bytes, min_free_bytes)


def _record_count(path: Path) -> int:
    count = 0
    try:
        with gzip.open(path, "rb") as stream:
            while True:
                block = stream.readline()
                if not block:
                    break
                if block.strip():
                    count += 1
    except (OSError, EOFError) as exc:
        raise SnapshotError(f"gzip shard failed CRC/decompression validation: {path.name}") from exc
    return count


def _authors(record: dict[str, Any]) -> str:
    result = []
    for authorship in record.get("authorships") or []:
        if not isinstance(authorship, dict):
            continue
        author = authorship.get("author") or {}
        if not isinstance(author, dict):
            continue
        name = author.get("display_name") or author.get("name")
        if isinstance(name, str) and name:
            result.append({"name": name, "orcid": author.get("orcid")})
    return json.dumps(result, ensure_ascii=False, separators=(",", ":"))


def _work_values(record: dict[str, Any], version: str) -> tuple[Any, ...] | None:
    raw_id = record.get("id")
    if not isinstance(raw_id, str):
        return None
    work_id = raw_id.rstrip("/").rsplit("/", 1)[-1]
    if not re.fullmatch(r"W[0-9]+", work_id):
        return None
    raw_doi = record.get("doi")
    try:
        doi = normalize_doi(raw_doi) if isinstance(raw_doi, str) else None
    except ValueError:
        doi = None
    title = record.get("title")
    if not isinstance(title, str):
        title = ""
    abstract = record.get("abstract")
    if not isinstance(abstract, str):
        abstract = abstract_from_index(record.get("abstract_inverted_index"))
    year = record.get("publication_year")
    if not isinstance(year, int):
        year = None
    cited = record.get("cited_by_count")
    if not isinstance(cited, int) or cited < 0:
        cited = 0
    def json_or_none(value: Any) -> str | None:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")) if isinstance(value, dict) else None
    return (
        version, work_id, doi, title, abstract, _authors(record),
        record.get("publication_date") if isinstance(record.get("publication_date"), str) else None,
        year, cited, record.get("type") if isinstance(record.get("type"), str) else None,
        json_or_none(record.get("primary_location")), json_or_none(record.get("best_oa_location")),
        record.get("updated_date") if isinstance(record.get("updated_date"), str) else None,
    )


_UPSERT = """
INSERT INTO openalex_works (
 dataset_version, openalex_id, doi, title, abstract, authors_json,
 publication_date, publication_year, cited_by_count, work_type,
 primary_location_json, best_oa_location_json, updated_date
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(openalex_id) DO UPDATE SET
 dataset_version=excluded.dataset_version, doi=excluded.doi, title=excluded.title,
 abstract=excluded.abstract, authors_json=excluded.authors_json,
 publication_date=excluded.publication_date, publication_year=excluded.publication_year,
 cited_by_count=excluded.cited_by_count, work_type=excluded.work_type,
 primary_location_json=excluded.primary_location_json,
 best_oa_location_json=excluded.best_oa_location_json, updated_date=excluded.updated_date
WHERE coalesce(excluded.updated_date, '') >= coalesce(openalex_works.updated_date, '')
"""


def _index_works(
    path: Path,
    connection: sqlite3.Connection,
    version: str,
    expected: int,
    database_path: Path,
    min_free_bytes: int,
) -> int:
    count = 0
    pending: list[tuple[Any, ...]] = []
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="strict") as stream:
            for line in stream:
                if not line.strip():
                    continue
                record = json.loads(line)
                if not isinstance(record, dict):
                    path.unlink(missing_ok=True)
                    raise SnapshotError("works shard contains a non-object JSON record")
                count += 1
                values = _work_values(record, version)
                if values is not None:
                    pending.append(values)
                if count % COMMIT_RECORDS == 0:
                    if shutil.disk_usage(database_path.parent).free < min_free_bytes + 64 * 1024 * 1024:
                        raise SnapshotError("database free-space reserve reached during work indexing")
                    connection.executemany(_UPSERT, pending)
                    pending.clear()
                    connection.commit()
            if pending:
                if shutil.disk_usage(database_path.parent).free < min_free_bytes + 64 * 1024 * 1024:
                    raise SnapshotError("database free-space reserve reached during work indexing")
                connection.executemany(_UPSERT, pending)
        connection.commit()
    except (OSError, EOFError, UnicodeDecodeError, json.JSONDecodeError, sqlite3.Error) as exc:
        connection.rollback()
        if not isinstance(exc, sqlite3.Error):
            path.unlink(missing_ok=True)
        raise SnapshotError(f"works shard failed validation: {path.name}") from exc
    except SnapshotError as exc:
        connection.rollback()
        if "free-space reserve" not in str(exc):
            path.unlink(missing_ok=True)
        raise
    if count != expected:
        path.unlink(missing_ok=True)
        raise SnapshotError(f"works shard had {count} records; manifest expects {expected}")
    return count


def _ensure_state(
    connection: sqlite3.Connection,
    manifest: dict[str, Any],
    assets: list[dict[str, Any]] | None = None,
) -> None:
    version = manifest["date"]
    files = [file for entity in manifest["entities"] for file in entity["files"]]
    assets = assets or []
    expected_bytes = manifest["meta"]["content_length"] + sum(asset["bytes"] for asset in assets)
    connection.execute(
        "INSERT INTO openalex_snapshot_state(singleton,dataset_version,state,complete,"
        "expected_records,expected_files,expected_bytes,verified_records,imported_works,"
        "verified_files,verified_bytes,error,updated_at) VALUES(1,?,'syncing',0,?,?,?,0,0,0,0,NULL,CURRENT_TIMESTAMP) "
        "ON CONFLICT(singleton) DO UPDATE SET dataset_version=excluded.dataset_version,"
        "state=CASE WHEN openalex_snapshot_state.dataset_version=excluded.dataset_version "
        "AND openalex_snapshot_state.complete=1 THEN 'complete' ELSE 'syncing' END,"
        "complete=CASE WHEN openalex_snapshot_state.dataset_version=excluded.dataset_version "
        "AND openalex_snapshot_state.complete=1 THEN 1 ELSE 0 END,"
        "expected_records=excluded.expected_records,expected_files=excluded.expected_files,"
        "expected_bytes=excluded.expected_bytes,"
        "verified_records=CASE WHEN openalex_snapshot_state.dataset_version=excluded.dataset_version "
        "THEN openalex_snapshot_state.verified_records ELSE 0 END,"
        "imported_works=CASE WHEN openalex_snapshot_state.dataset_version=excluded.dataset_version "
        "THEN openalex_snapshot_state.imported_works ELSE 0 END,"
        "verified_files=CASE WHEN openalex_snapshot_state.dataset_version=excluded.dataset_version "
        "THEN openalex_snapshot_state.verified_files ELSE 0 END,"
        "verified_bytes=CASE WHEN openalex_snapshot_state.dataset_version=excluded.dataset_version "
        "THEN openalex_snapshot_state.verified_bytes ELSE 0 END,error=NULL,updated_at=CURRENT_TIMESTAMP",
        (version, manifest["meta"]["record_count"], len(files) + len(assets), expected_bytes),
    )
    for entity in manifest["entities"]:
        for file in entity["files"]:
            connection.execute(
                "INSERT OR IGNORE INTO openalex_snapshot_files"
                "(dataset_version,source_url,entity,expected_bytes,expected_records) VALUES(?,?,?,?,?)",
                (version, file["url"], entity["entity"], file["meta"]["content_length"], file["meta"]["record_count"]),
            )
    for asset in assets:
        connection.execute(
            "INSERT OR IGNORE INTO openalex_snapshot_files"
            "(dataset_version,source_url,entity,expected_bytes,expected_records) VALUES(?,?,'assets',?,0)",
            (version, asset["url"], asset["bytes"]),
        )
    connection.commit()


def _refresh_progress(connection: sqlite3.Connection, version: str) -> None:
    connection.execute(
        "UPDATE openalex_snapshot_state SET verified_files=(SELECT count(*) FROM openalex_snapshot_files "
        "WHERE dataset_version=? AND verified=1),verified_records=(SELECT coalesce(sum(actual_records),0) "
        "FROM openalex_snapshot_files WHERE dataset_version=? AND verified=1),verified_bytes=(SELECT "
        "coalesce(sum(expected_bytes),0) FROM openalex_snapshot_files WHERE dataset_version=? AND verified=1),"
        "imported_works=(SELECT coalesce(sum(actual_records),0) FROM openalex_snapshot_files "
        "WHERE dataset_version=? AND entity='works' AND indexed=1),updated_at=CURRENT_TIMESTAMP "
        "WHERE singleton=1",
        (version, version, version, version),
    )


def _load_frozen_manifest(raw_root: Path, version: str) -> dict[str, Any]:
    path = raw_root / version / "manifest.json"
    if not path.exists():
        raise SnapshotError(f"resume manifest is missing: {path}")
    manifest = _validate_manifest(path.read_bytes())
    if manifest["date"] != version:
        raise SnapshotError("resume manifest does not match the active dataset version")
    return manifest


def _sync_locked(database: Path, raw_root: Path, min_free_bytes: int, refresh: bool = False) -> dict[str, Any]:
    if not database.is_absolute() or not raw_root.is_absolute():
        raise SnapshotError("database and snapshot paths must be absolute")
    raw_root.mkdir(parents=True, exist_ok=True)
    initialize(database)
    state_connection = connect(database)
    try:
        current = state_connection.execute(
            "SELECT dataset_version,state,complete FROM openalex_snapshot_state WHERE singleton=1"
        ).fetchone()
    finally:
        state_connection.close()
    if current is not None and not refresh:
        manifest = _load_frozen_manifest(raw_root, current["dataset_version"])
        if current["complete"]:
            summary_db = connect(database)
            try:
                verified_files = summary_db.execute(
                    "SELECT count(*) FROM openalex_snapshot_files WHERE dataset_version=? AND verified=1",
                    (current["dataset_version"],),
                ).fetchone()[0]
                imported_works = summary_db.execute(
                    "SELECT count(*) FROM openalex_works WHERE dataset_version=?",
                    (current["dataset_version"],),
                ).fetchone()[0]
            finally:
                summary_db.close()
            return {
                "dataset_version": current["dataset_version"],
                "state": "complete",
                "complete": True,
                "verified_files": verified_files,
                "imported_works": imported_works,
            }
    else:
        manifest_bytes = _read_manifest()
        latest = _validate_manifest(manifest_bytes)
        manifest = _save_manifest(raw_root, manifest_bytes, latest["date"])
    version = manifest["date"]
    db = connect(database)
    try:
        support_assets = _support_assets(raw_root, version, manifest, db)
        _ensure_state(db, manifest, support_assets)
        _refresh_progress(db, version)
        db.commit()
        for asset in support_assets:
            source_url = asset["url"]
            row = db.execute(
                "SELECT verified FROM openalex_snapshot_files WHERE dataset_version=? AND source_url=?",
                (version, source_url),
            ).fetchone()
            if row["verified"]:
                continue
            target = _asset_path(raw_root, version, source_url)
            if source_url != _s3_object_url("data/jsonl/manifest.json"):
                if not target.exists() or target.stat().st_size != asset["bytes"]:
                    _download(source_url, target, asset["bytes"], min_free_bytes)
            try:
                _validate_asset(target, source_url, manifest)
            except SnapshotError:
                if target != raw_root / version / "manifest.json":
                    target.unlink(missing_ok=True)
                raise
            db.execute(
                "UPDATE openalex_snapshot_files SET downloaded=1,verified=1,indexed=1,actual_records=0 "
                "WHERE dataset_version=? AND source_url=?",
                (version, source_url),
            )
            _refresh_progress(db, version)
            db.commit()
        entities = sorted(manifest["entities"], key=lambda entity: (entity["entity"] != "works", entity["entity"]))
        for entity in entities:
            for item in entity["files"]:
                source_url = item["url"]
                expected_bytes = item["meta"]["content_length"]
                expected_records = item["meta"]["record_count"]
                row = db.execute(
                    "SELECT downloaded,verified,indexed FROM openalex_snapshot_files "
                    "WHERE dataset_version=? AND source_url=?", (version, source_url),
                ).fetchone()
                target = _shard_path(raw_root, version, entity["entity"], source_url)
                if row["verified"] and target.is_file() and target.stat().st_size == expected_bytes:
                    if entity["entity"] != "works" and not row["indexed"]:
                        db.execute(
                            "UPDATE openalex_snapshot_files SET indexed=1 WHERE dataset_version=? AND source_url=?",
                            (version, source_url),
                        )
                        db.commit()
                    continue
                if not target.exists() or target.stat().st_size != expected_bytes:
                    if row["verified"]:
                        db.execute(
                            "UPDATE openalex_snapshot_files SET downloaded=0,verified=0,actual_records=0 "
                            "WHERE dataset_version=? AND source_url=?", (version, source_url),
                        )
                        _refresh_progress(db, version)
                        db.commit()
                    _download(source_url, target, expected_bytes, min_free_bytes)
                    db.execute(
                        "UPDATE openalex_snapshot_files SET downloaded=1 WHERE dataset_version=? AND source_url=?",
                        (version, source_url),
                    )
                    db.commit()
                try:
                    actual = _record_count(target)
                except SnapshotError:
                    target.unlink(missing_ok=True)
                    raise
                if actual != expected_records:
                    target.unlink(missing_ok=True)
                    raise SnapshotError(
                        f"{entity['entity']} shard had {actual} rows; manifest expects {expected_records}"
                    )
                db.execute(
                    "UPDATE openalex_snapshot_files SET downloaded=1,verified=1,"
                    "indexed=CASE WHEN entity='works' THEN indexed ELSE 1 END,actual_records=? "
                    "WHERE dataset_version=? AND source_url=?",
                    (actual, version, source_url),
                )
                _refresh_progress(db, version)
                db.commit()
        pending = db.execute(
            "SELECT count(*) FROM openalex_snapshot_files WHERE dataset_version=? AND verified=0", (version,)
        ).fetchone()[0]
        if pending:
            raise SnapshotError(f"snapshot has {pending} unverified raw shard/asset(s)")
        verified_records = db.execute(
            "SELECT coalesce(sum(actual_records),0) FROM openalex_snapshot_files "
            "WHERE dataset_version=? AND verified=1", (version,),
        ).fetchone()[0]
        if verified_records != manifest["meta"]["record_count"]:
            raise SnapshotError(
                f"verified raw snapshot has {verified_records} records; manifest expects "
                f"{manifest['meta']['record_count']}"
            )
        # The full raw corpus is now downloaded and CRC/count-verified. Only now
        # begin the slower full-text works projection.
        works_entity = next(entity for entity in entities if entity["entity"] == "works")
        for item in works_entity["files"]:
            source_url = item["url"]
            row = db.execute(
                "SELECT indexed FROM openalex_snapshot_files WHERE dataset_version=? AND source_url=?",
                (version, source_url),
            ).fetchone()
            if row["indexed"]:
                continue
            target = _shard_path(raw_root, version, "works", source_url)
            count = _index_works(
                target, db, version, item["meta"]["record_count"], database, min_free_bytes
            )
            if count != item["meta"]["record_count"]:
                raise SnapshotError("indexed works shard count does not match its frozen manifest")
            db.execute(
                "UPDATE openalex_snapshot_files SET indexed=1 WHERE dataset_version=? AND source_url=?",
                (version, source_url),
            )
            _refresh_progress(db, version)
            db.commit()
        expected_works = next(entity["record_count"] for entity in manifest["entities"] if entity["entity"] == "works")
        imported = db.execute(
            "SELECT count(*) FROM openalex_works WHERE dataset_version=?", (version,)
        ).fetchone()[0]
        if imported != expected_works:
            raise SnapshotError(f"works index contains {imported} records; manifest expects {expected_works}")
        db.execute(
            "UPDATE openalex_snapshot_state SET state='complete',complete=1,error=NULL,updated_at=CURRENT_TIMESTAMP "
            "WHERE singleton=1"
        )
        db.commit()
        return {
            "dataset_version": version,
            "state": "complete",
            "complete": True,
            "verified_files": db.execute(
                "SELECT count(*) FROM openalex_snapshot_files WHERE dataset_version=? AND verified=1", (version,)
            ).fetchone()[0],
            "imported_works": imported,
        }
    except Exception as exc:
        db.execute(
            "UPDATE openalex_snapshot_state SET state='failed',complete=0,error=?,updated_at=CURRENT_TIMESTAMP "
            "WHERE singleton=1",
            (type(exc).__name__,),
        )
        db.commit()
        raise
    finally:
        db.close()


def sync(database: Path, raw_root: Path, min_free_bytes: int, refresh: bool = False) -> dict[str, Any]:
    if not raw_root.is_absolute():
        raise SnapshotError("snapshot paths must be absolute")
    raw_root.mkdir(parents=True, exist_ok=True)
    lock_path = raw_root / ".sync.lock"
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SnapshotError("another OpenAlex snapshot sync is already running") from exc
        try:
            return _sync_locked(database, raw_root, min_free_bytes, refresh)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def benchmark_download_modes(database: Path, raw_root: Path, min_free_bytes: int) -> dict[str, Any]:
    """Compare direct/proxy Axel transfers of the same verified works shard."""
    if not database.is_absolute() or not raw_root.is_absolute():
        raise SnapshotError("database and snapshot paths must be absolute")
    if _configured_download_proxy() is None:
        raise SnapshotError("benchmark requires OPENALEX_DOWNLOAD_PROXY to be configured")
    executable = shutil.which("axel")
    if executable is None:
        raise SnapshotError("Axel is required for the download benchmark")
    try:
        connections = int(os.environ.get("OPENALEX_DOWNLOAD_CONNECTIONS", "16"))
    except ValueError as exc:
        raise SnapshotError("OPENALEX_DOWNLOAD_CONNECTIONS must be an integer from 1 to 32") from exc
    if not 1 <= connections <= 32:
        raise SnapshotError("OPENALEX_DOWNLOAD_CONNECTIONS must be an integer from 1 to 32")
    if not raw_root.is_dir():
        raise SnapshotError("snapshot directory is missing")

    lock_path = raw_root / ".sync.lock"
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SnapshotError("stop the snapshot sync service before benchmarking") from exc
        try:
            db = connect(database)
            try:
                row = db.execute(
                    "SELECT dataset_version,source_url,expected_bytes,actual_records "
                    "FROM openalex_snapshot_files WHERE entity='works' AND verified=1 "
                    "ORDER BY expected_bytes DESC LIMIT 1"
                ).fetchone()
            finally:
                db.close()
            if row is None:
                raise SnapshotError("benchmark requires at least one verified works shard")
            if row["expected_bytes"] > 2 * 1024**3:
                raise SnapshotError("largest verified works shard exceeds the 2 GiB benchmark limit")
            source = row["source_url"]
            source_path = _shard_path(raw_root, row["dataset_version"], "works", source)
            if not source_path.is_file() or source_path.stat().st_size != row["expected_bytes"]:
                raise SnapshotError("largest verified works shard is not present at its expected size")
            expected_hash = _sha256_file(source_path)
            if _record_count(source_path) != row["actual_records"]:
                raise SnapshotError("largest verified works shard failed its recorded row count")
            if shutil.disk_usage(raw_root).free - row["expected_bytes"] < min_free_bytes:
                raise SnapshotError("benchmark output would violate the snapshot disk reserve")

            runs = []
            for index, mode in enumerate(("direct", "proxy", "proxy", "direct"), start=1):
                with tempfile.TemporaryDirectory(prefix=".openalex-benchmark-", dir=raw_root) as temp_dir:
                    output = Path(temp_dir) / "benchmark.gz"
                    started = time.monotonic()
                    result: dict[str, Any] = {
                        "run": index,
                        "mode": mode,
                        "ok": False,
                        "bytes": row["expected_bytes"],
                    }
                    try:
                        _download_axel(
                            source, output, row["expected_bytes"], min_free_bytes,
                            mode_override=mode, max_attempts=1, timeout_seconds=180,
                        )
                        result["download_seconds"] = round(time.monotonic() - started, 3)
                        validation_started = time.monotonic()
                        actual_hash = _sha256_file(output)
                        actual_records = _record_count(output)
                        result["validation_seconds"] = round(time.monotonic() - validation_started, 3)
                        if actual_records != row["actual_records"]:
                            raise SnapshotError("benchmark row count differs from the verified shard")
                        if actual_hash != expected_hash:
                            raise SnapshotError("benchmark SHA-256 differs from the verified shard")
                        result.update({
                            "ok": True,
                            "sha256": actual_hash,
                            "records": actual_records,
                            "mb_per_second": round(
                                row["expected_bytes"] / max(result["download_seconds"], 0.001) / 1_000_000, 3
                            ),
                        })
                    except Exception as exc:
                        result["error"] = f"{type(exc).__name__}: {exc}"
                        result.setdefault("download_seconds", round(time.monotonic() - started, 3))
                    runs.append(result)
                    print(json.dumps(result, sort_keys=True), file=sys.stderr, flush=True)

            summary = {}
            for mode in ("direct", "proxy"):
                successful = [run for run in runs if run["mode"] == mode and run["ok"]]
                speeds = [run["mb_per_second"] for run in successful]
                summary[mode] = {
                    "successful_runs": len(successful),
                    "median_mb_per_second": round(statistics.median(speeds), 3) if speeds else None,
                }
            hashes = {run.get("sha256") for run in runs if run["ok"]}
            return {
                "source": "/".join(PurePosixPath(urllib.parse.urlsplit(source).path).parts[-2:]),
                "bytes": row["expected_bytes"],
                "records": row["actual_records"],
                "connections": connections,
                "sequence": ["direct", "proxy", "proxy", "direct"],
                "runs": runs,
                "summary": summary,
                "all_runs_valid": len(runs) == 4 and all(run["ok"] for run in runs) and len(hashes) == 1,
            }
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def main() -> int:
    parser = argparse.ArgumentParser(description="Download and index a complete OpenAlex snapshot")
    parser.add_argument("command", choices=("sync", "mode", "benchmark"))
    parser.add_argument("mode", nargs="?", choices=("direct", "proxy", "status"))
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="explicitly fetch latest manifest instead of resuming the frozen active snapshot",
    )
    args = parser.parse_args()
    if args.command == "mode":
        if args.mode is None:
            parser.error("mode command requires direct, proxy, or status")
        try:
            result = download_mode_status() if args.mode == "status" else set_download_mode(args.mode)
            database_for_status = os.environ.get("OPENALEX_CATALOG_DB", "")
            if args.mode == "status" and database_for_status and Path(database_for_status).is_file():
                snapshot_status = OpenAlexCatalog(Path(database_for_status)).status()
                result["snapshot"] = {
                    key: snapshot_status[key]
                    for key in ("state", "dataset_version", "complete", "verified_files", "expected_files",
                                "verified_records", "expected_records", "imported_works")
                }
        except Exception as exc:
            print(f"OpenAlex download mode failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.mode is not None:
        parser.error("mode argument is only valid with the mode command")
    database_raw = os.environ.get("OPENALEX_CATALOG_DB", "")
    raw_raw = os.environ.get("OPENALEX_RAW_DIR", "")
    if not database_raw:
        parser.error("OPENALEX_CATALOG_DB must be configured")
    if args.command in ("sync", "benchmark") and not raw_raw:
        parser.error("OPENALEX_RAW_DIR must be configured")
    try:
        min_free = int(os.environ.get("OPENALEX_MIN_FREE_BYTES", str(1024**4)))
        if min_free < 0:
            raise ValueError
    except ValueError:
        parser.error("OPENALEX_MIN_FREE_BYTES must be a non-negative integer")
    try:
        if args.command == "benchmark":
            result = benchmark_download_modes(Path(database_raw), Path(raw_raw), min_free)
        else:
            result = sync(Path(database_raw), Path(raw_raw), min_free, refresh=args.refresh)
    except Exception as exc:
        print(f"OpenAlex snapshot sync failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0 if args.command != "benchmark" or result["all_runs_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
