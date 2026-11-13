"""Project-owned SQLite WAL cache adapter."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from app.application.ports import CacheEntry
from app.domain import DoiWork


class SqliteDoiCache:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    def open(self) -> None:
        with self._lock:
            if self._connection is not None:
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(
                self.path,
                timeout=5,
                check_same_thread=False,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS doi_cache (
                    doi TEXT PRIMARY KEY,
                    status TEXT NOT NULL CHECK(status IN ('found', 'missing')),
                    payload_json TEXT,
                    provider TEXT,
                    fetched_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    CHECK (
                        (status = 'found' AND payload_json IS NOT NULL)
                        OR (status = 'missing' AND payload_json IS NULL)
                    )
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS doi_cache_expiry_idx ON doi_cache(expires_at)"
            )
            connection.commit()
            self._connection = connection

    def _conn(self) -> sqlite3.Connection:
        self.open()
        assert self._connection is not None
        return self._connection

    def get(self, doi: str) -> CacheEntry | None:
        with self._lock:
            row = self._conn().execute(
                "SELECT status, payload_json, expires_at FROM doi_cache WHERE doi = ?",
                (doi,),
            ).fetchone()
        if row is None:
            return None
        work = None
        if row["status"] == "found":
            try:
                payload = json.loads(row["payload_json"])
                work = DoiWork.from_mapping(payload)
            except (TypeError, ValueError, json.JSONDecodeError):
                self.delete(doi)
                return None
        return CacheEntry(str(row["status"]), work, float(row["expires_at"]))

    def put_found(self, work: DoiWork, expires_at: float) -> None:
        self._upsert(
            work.doi,
            "found",
            json.dumps(work.to_contract(), ensure_ascii=False, separators=(",", ":")),
            work.source,
            expires_at,
        )

    def put_missing(self, doi: str, expires_at: float) -> None:
        self._upsert(doi, "missing", None, None, expires_at)

    def _upsert(
        self,
        doi: str,
        status: str,
        payload: str | None,
        provider: str | None,
        expires_at: float,
    ) -> None:
        with self._lock:
            self._conn().execute(
                """
                INSERT INTO doi_cache (
                    doi, status, payload_json, provider, fetched_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(doi) DO UPDATE SET
                    status=excluded.status,
                    payload_json=excluded.payload_json,
                    provider=excluded.provider,
                    fetched_at=excluded.fetched_at,
                    expires_at=excluded.expires_at
                """,
                (doi, status, payload, provider, time.time(), expires_at),
            )
            self._conn().commit()

    def delete(self, doi: str) -> None:
        with self._lock:
            self._conn().execute("DELETE FROM doi_cache WHERE doi = ?", (doi,))
            self._conn().commit()

    def health(self) -> dict[str, Any]:
        started = time.monotonic()
        try:
            with self._lock:
                count = int(
                    self._conn().execute("SELECT count(*) FROM doi_cache").fetchone()[0]
                )
            reachable = True
            error = None
        except Exception as exc:
            count = 0
            reachable = False
            error = type(exc).__name__
        wal = Path(str(self.path) + "-wal")
        return {
            "reachable": reachable,
            "latency_ms": round((time.monotonic() - started) * 1000, 3),
            "entries": count,
            "bytes": self.path.stat().st_size if self.path.is_file() else 0,
            "wal_bytes": wal.stat().st_size if wal.is_file() else 0,
            "error": error,
        }

    def prune_expired(self, now: float | None = None) -> int:
        cutoff = time.time() if now is None else now
        with self._lock:
            cursor = self._conn().execute(
                "DELETE FROM doi_cache WHERE expires_at < ?", (cutoff,)
            )
            self._conn().commit()
            return max(0, int(cursor.rowcount))

    def checkpoint(self) -> None:
        with self._lock:
            self._conn().execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()

    def backup(self, destination: Path) -> None:
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".partial")
        temporary.unlink(missing_ok=True)
        with self._lock:
            target = sqlite3.connect(temporary)
            try:
                self._conn().backup(target)
            finally:
                target.close()
        temporary.replace(destination)

    def close(self) -> None:
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
