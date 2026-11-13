"""Read-only local OpenAlex works catalog backed by an imported snapshot."""

from __future__ import annotations

import base64
import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any


CONTRACT_VERSION = "openalex-catalog.v1"
_WORK_ID = re.compile(r"^W[0-9]+$")
_TERMS = re.compile(r"[\w-]{2,64}", re.UNICODE)


def connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def initialize(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect(path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS openalex_snapshot_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                dataset_version TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'syncing',
                complete INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0, 1)),
                expected_records INTEGER NOT NULL,
                expected_files INTEGER NOT NULL,
                expected_bytes INTEGER NOT NULL DEFAULT 0,
                verified_records INTEGER NOT NULL DEFAULT 0,
                imported_works INTEGER NOT NULL DEFAULT 0,
                verified_files INTEGER NOT NULL DEFAULT 0,
                verified_bytes INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS openalex_snapshot_files (
                dataset_version TEXT NOT NULL,
                source_url TEXT NOT NULL,
                entity TEXT NOT NULL,
                expected_bytes INTEGER NOT NULL,
                expected_records INTEGER NOT NULL,
                downloaded INTEGER NOT NULL DEFAULT 0,
                verified INTEGER NOT NULL DEFAULT 0,
                indexed INTEGER NOT NULL DEFAULT 0,
                actual_records INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (dataset_version, source_url)
            );
            CREATE TABLE IF NOT EXISTS openalex_works (
                rowid INTEGER PRIMARY KEY,
                dataset_version TEXT NOT NULL,
                openalex_id TEXT NOT NULL UNIQUE,
                doi TEXT,
                title TEXT NOT NULL DEFAULT '',
                abstract TEXT,
                authors_json TEXT NOT NULL DEFAULT '[]',
                publication_date TEXT,
                publication_year INTEGER,
                cited_by_count INTEGER NOT NULL DEFAULT 0,
                work_type TEXT,
                primary_location_json TEXT,
                best_oa_location_json TEXT,
                updated_date TEXT,
                UNIQUE (openalex_id)
            );
            CREATE INDEX IF NOT EXISTS idx_openalex_works_doi
                ON openalex_works(dataset_version, doi);
            CREATE INDEX IF NOT EXISTS idx_openalex_works_date
                ON openalex_works(dataset_version, publication_date DESC, openalex_id);
            CREATE VIRTUAL TABLE IF NOT EXISTS openalex_works_fts USING fts5(
                dataset_version UNINDEXED, title, abstract,
                content='openalex_works', content_rowid='rowid',
                tokenize='unicode61 remove_diacritics 2'
            );
            CREATE TRIGGER IF NOT EXISTS openalex_works_ai AFTER INSERT ON openalex_works BEGIN
                INSERT INTO openalex_works_fts(rowid, dataset_version, title, abstract)
                VALUES (new.rowid, new.dataset_version, new.title, coalesce(new.abstract, ''));
            END;
            CREATE TRIGGER IF NOT EXISTS openalex_works_ad AFTER DELETE ON openalex_works BEGIN
                INSERT INTO openalex_works_fts(openalex_works_fts,rowid,dataset_version,title,abstract)
                VALUES ('delete',old.rowid,old.dataset_version,old.title,coalesce(old.abstract,''));
            END;
            CREATE TRIGGER IF NOT EXISTS openalex_works_au AFTER UPDATE ON openalex_works BEGIN
                INSERT INTO openalex_works_fts(openalex_works_fts,rowid,dataset_version,title,abstract)
                VALUES ('delete',old.rowid,old.dataset_version,old.title,coalesce(old.abstract,''));
                INSERT INTO openalex_works_fts(rowid, dataset_version, title, abstract)
                VALUES (new.rowid, new.dataset_version, new.title, coalesce(new.abstract, ''));
            END;
            """
        )
        columns = {row[1] for row in connection.execute("PRAGMA table_info(openalex_snapshot_state)")}
        if "storage_version" not in columns:
            connection.execute("ALTER TABLE openalex_snapshot_state ADD COLUMN storage_version TEXT")
        connection.commit()
    finally:
        connection.close()


def decode_cursor(cursor: str | None) -> tuple[int, int]:
    if not cursor:
        return 0, 0
    if len(cursor) > 64:
        raise ValueError("cursor is invalid")
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        offset, separator, pinned = raw.decode("ascii").partition(":")
        value = int(offset)
        pinned_rowid = int(pinned) if separator else 0
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("cursor is invalid") from exc
    if not 0 <= value <= 5_000 or not 0 <= pinned_rowid < 2**63:
        raise ValueError("cursor is outside the supported range")
    return value, pinned_rowid


def encode_cursor(offset: int, pinned_rowid: int) -> str:
    position = f"{offset}:{pinned_rowid}" if pinned_rowid else str(offset)
    return base64.urlsafe_b64encode(position.encode("ascii")).decode("ascii").rstrip("=")


def search_expression(search: str) -> str:
    terms = _TERMS.findall(search[:256])
    if not terms or len(terms) > 12:
        raise ValueError("search must contain 1 to 12 words")
    return " AND ".join('"' + term.replace('"', '""') + '"' for term in terms)


def normalize_doi(value: str) -> str:
    doi = re.sub(r"^doi:\s*", "", value.strip(), flags=re.I)
    for prefix in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "http://dx.doi.org/"):
        if doi.lower().startswith(prefix):
            doi = doi[len(prefix) :]
            break
    doi = doi.strip().lower()
    if not re.fullmatch(r"10\.[0-9]{4,9}/[^\s<>\"{}|^`\\]+", doi):
        raise ValueError("invalid DOI")
    return doi


def abstract_from_index(index: Any) -> str | None:
    if not isinstance(index, dict) or not index:
        return None
    positions: dict[int, str] = {}
    for word, offsets in index.items():
        if not isinstance(word, str) or not isinstance(offsets, list):
            continue
        for offset in offsets:
            if isinstance(offset, int) and 0 <= offset < 100_000:
                positions[offset] = word
    if not positions:
        return None
    return " ".join(positions.get(i, "") for i in range(max(positions) + 1)).strip() or None


def location_dto(location: Any) -> dict[str, Any] | None:
    if not isinstance(location, dict):
        return None
    source = location.get("source") if isinstance(location.get("source"), dict) else {}
    return {
        "landing_page_url": location.get("landing_page_url"),
        "pdf_url": location.get("pdf_url"),
        "source": {
            "id": source.get("id"),
            "display_name": source.get("display_name"),
            "type": source.get("type"),
        },
    }


def work_dto(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["openalex_id"],
        "doi": row["doi"],
        "title": row["title"],
        "abstract": row["abstract"],
        "authors": json.loads(row["authors_json"]),
        "publication_date": row["publication_date"],
        "publication_year": row["publication_year"],
        "cited_by_count": row["cited_by_count"],
        "type": row["work_type"],
        "primary_location": location_dto(
            json.loads(row["primary_location_json"]) if row["primary_location_json"] else None
        ),
        "best_oa_location": location_dto(
            json.loads(row["best_oa_location_json"]) if row["best_oa_location_json"] else None
        ),
    }


def envelope(state: sqlite3.Row | None) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "dataset_version": state["dataset_version"] if state else None,
        "complete": bool(state["complete"]) if state else False,
        "state": state["state"] if state and "state" in state.keys() else "not_started",
    }


class OpenAlexCatalog:
    def __init__(self, database: Path) -> None:
        self.database = database
        initialize(database)

    def _state(self, db: sqlite3.Connection) -> sqlite3.Row | None:
        return db.execute(
            "SELECT dataset_version, state, complete, coalesce(storage_version,dataset_version) AS storage_version "
            "FROM openalex_snapshot_state WHERE singleton=1"
        ).fetchone()

    def status(self) -> dict[str, Any]:
        with connect(self.database) as db:
            state = db.execute(
                "SELECT dataset_version, state, complete, expected_records, verified_records, "
                "expected_files, imported_works, verified_files, expected_bytes, verified_bytes, "
                "error, updated_at FROM openalex_snapshot_state "
                "WHERE singleton=1"
            ).fetchone()
            if state is None:
                return {
                    **envelope(None), "expected_records": 0, "expected_files": 0,
                    "state": "not_started", "verified_records": 0,
                    "imported_works": 0, "verified_files": 0, "expected_bytes": 0,
                    "verified_bytes": 0, "error": None, "updated_at": None,
                }
            return {**dict(state), **envelope(state), "complete": bool(state["complete"])}

    def by_id(self, work_id: str) -> dict[str, Any] | None:
        if not _WORK_ID.fullmatch(work_id):
            raise ValueError("work ID must use the W followed by digits format")
        with connect(self.database) as db:
            state = self._state(db)
            if state is None:
                return {**envelope(None), "work": None}
            row = db.execute(
                "SELECT * FROM openalex_works WHERE dataset_version=? AND openalex_id=?",
                (state["storage_version"], work_id),
            ).fetchone()
            return {**envelope(state), "work": work_dto(row) if row else None}

    def by_doi(self, doi_value: str) -> dict[str, Any]:
        doi = normalize_doi(doi_value)
        with connect(self.database) as db:
            state = self._state(db)
            if state is None:
                return {**envelope(None), "work": None}
            row = db.execute(
                "SELECT * FROM openalex_works WHERE dataset_version=? AND doi=? "
                "ORDER BY cited_by_count DESC, openalex_id LIMIT 1",
                (state["storage_version"], doi),
            ).fetchone()
            return {**envelope(state), "work": work_dto(row) if row else None}

    def search(self, query: str, cursor: str | None, per_page: int) -> dict[str, Any]:
        offset, pinned_rowid = decode_cursor(cursor)
        try:
            doi = normalize_doi(query)
        except ValueError:
            doi = None
        if doi:
            result = self.by_doi(doi)
            work = result.pop("work")
            return {**result, "results": [work] if work and not offset else [], "next_cursor": None}
        query = query.strip()
        limit = min(max(per_page, 1), 50)
        with connect(self.database) as db:
            state = self._state(db)
            if state is None:
                return {**envelope(None), "results": [], "next_cursor": None}
            rows = []
            if not cursor and "\x00" not in query and _TERMS.search(query):
                # Limit the optional title probe independently of the keyword search.
                title_deadline = time.monotonic() + 1.0
                db.set_progress_handler(lambda: int(time.monotonic() >= title_deadline), 1000)
                phrase = 'title : ^"' + query.replace('"', '""') + '"'
                try:
                    title = db.execute(
                        "SELECT w.* FROM openalex_works_fts CROSS JOIN openalex_works w "
                        "ON w.rowid=openalex_works_fts.rowid WHERE openalex_works_fts MATCH ? "
                        "AND w.dataset_version=? AND w.title=? COLLATE NOCASE "
                        "ORDER BY openalex_works_fts.rowid DESC LIMIT 1",
                        (phrase, state["storage_version"], query),
                    ).fetchone()
                except sqlite3.OperationalError as exc:
                    if str(exc) != "interrupted":
                        raise
                    title = None
                if title:
                    rows.append(title)
                    pinned_rowid = title["rowid"]
            try:
                expression = search_expression(query)
            except ValueError:
                if rows:
                    return {**envelope(state), "results": [work_dto(rows[0])], "next_cursor": None}
                raise
            remaining = limit - len(rows)
            deadline = time.monotonic() + 4.0
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            keyword_rows = db.execute(
                "SELECT w.* FROM openalex_works_fts JOIN openalex_works w "
                "ON w.rowid=openalex_works_fts.rowid "
                "WHERE openalex_works_fts MATCH ? AND openalex_works_fts.dataset_version=? "
                "AND w.dataset_version=? AND w.rowid != ? "
                "ORDER BY rank LIMIT ? OFFSET ?",
                (expression, state["storage_version"], state["storage_version"], pinned_rowid, remaining + 1, offset),
            ).fetchall()
            rows.extend(keyword_rows)
            more = len(rows) > limit and offset + remaining <= 5_000
            results = [work_dto(row) for row in rows[:limit]]
            return {
                **envelope(state),
                "results": results,
                "next_cursor": encode_cursor(offset + remaining, pinned_rowid) if more else None,
            }
