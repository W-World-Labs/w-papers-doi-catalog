"""Private LAN API for the locally imported OpenAlex snapshot."""

from __future__ import annotations

import os
import ipaddress
import sqlite3
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Path as PathParam, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.openalex_catalog import OpenAlexCatalog
from app.web.middleware import TrustedClientMiddleware


class OpenAlexAuthor(BaseModel):
    name: str
    orcid: str | None = None


class OpenAlexLocationSource(BaseModel):
    id: str | None = None
    display_name: str | None = None
    type: str | None = None


class OpenAlexLocation(BaseModel):
    landing_page_url: str | None = None
    pdf_url: str | None = None
    source: OpenAlexLocationSource | None = None


class OpenAlexWork(BaseModel):
    id: str
    doi: str | None = None
    title: str
    abstract: str | None = None
    authors: list[OpenAlexAuthor]
    publication_date: str | None = None
    publication_year: int | None = None
    cited_by_count: int
    type: str | None = None
    primary_location: OpenAlexLocation | None = None
    best_oa_location: OpenAlexLocation | None = None


class WorkResponse(BaseModel):
    contract_version: Literal["openalex-catalog.v1"]
    dataset_version: str
    complete: bool
    state: Literal["syncing", "complete", "failed"]
    work: OpenAlexWork


class SearchResponse(BaseModel):
    contract_version: Literal["openalex-catalog.v1"]
    dataset_version: str
    complete: bool
    state: Literal["syncing", "complete", "failed"]
    results: list[OpenAlexWork]
    next_cursor: str | None


class StatusResponse(BaseModel):
    contract_version: Literal["openalex-catalog.v1"]
    dataset_version: str | None
    state: Literal["not_started", "syncing", "complete", "failed"]
    complete: bool
    expected_records: int
    verified_records: int
    expected_files: int
    imported_works: int
    verified_files: int
    expected_bytes: int
    verified_bytes: int
    error: str | None
    updated_at: str | None


database_value = os.environ.get("OPENALEX_CATALOG_DB", "")
if not database_value:
    raise RuntimeError("OPENALEX_CATALOG_DB must be configured")
database_path = Path(database_value)
if not database_path.is_absolute():
    raise RuntimeError("OPENALEX_CATALOG_DB must be an absolute path")

catalog = OpenAlexCatalog(database_path)
trusted_ips_raw = os.environ.get("OPENALEX_TRUSTED_CLIENT_IPS", "127.0.0.1,::1")
trusted_ips = frozenset(
    str(ipaddress.ip_address(value.strip()))
    for value in trusted_ips_raw.split(",")
    if value.strip()
)
if not trusted_ips:
    raise RuntimeError("OPENALEX_TRUSTED_CLIENT_IPS must contain exact IP addresses")

app = FastAPI(
    title="W Papers Local OpenAlex Catalog",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
)
app.add_middleware(TrustedClientMiddleware, trusted_ips=trusted_ips)


@app.exception_handler(sqlite3.OperationalError)
def sqlite_unavailable(_: Any, exc: sqlite3.OperationalError) -> JSONResponse:
    # The catalog can be busy while the long snapshot importer commits a shard.
    # Return a bounded degraded response instead of tying up API workers.
    return JSONResponse(
        {"detail": "local OpenAlex catalog is busy; retry shortly"},
        status_code=503,
        headers={"Retry-After": "2"},
    )


@app.get("/v1/openalex/status", response_model=StatusResponse)
def status() -> dict[str, Any]:
    return catalog.status()


@app.get(
    "/v1/openalex/works/{work_id}",
    response_model=WorkResponse,
    responses={404: {"description": "Work absent from this complete snapshot or not imported yet"},
                503: {"description": "No local snapshot available or local SQLite index is busy"}},
)
def get_work(work_id: str = PathParam(min_length=2, max_length=32)):
    try:
        result = catalog.by_id(work_id)
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=422)
    if result["dataset_version"] is None:
        return JSONResponse(
            {"detail": "OpenAlex snapshot has not been imported", "state": result["state"]},
            status_code=503,
            headers={"Retry-After": "30"},
        )
    if result["work"] is None:
        return JSONResponse(
            {
                "detail": "work is not present in the imported portion of the local snapshot",
                "dataset_version": result["dataset_version"],
                "complete": result["complete"],
            },
            status_code=404,
            headers={"X-OpenAlex-Catalog-Complete": str(result["complete"]).lower()},
        )
    return result


@app.get(
    "/v1/openalex/doi/{doi:path}",
    response_model=WorkResponse,
    responses={404: {"description": "DOI absent from this complete snapshot or not imported yet"},
                503: {"description": "No local snapshot available or local SQLite index is busy"}},
)
def get_work_by_doi(doi: str = PathParam(min_length=7, max_length=300)):
    try:
        result = catalog.by_doi(doi)
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=422)
    if result["dataset_version"] is None:
        return JSONResponse(
            {"detail": "OpenAlex snapshot has not been imported", "state": result["state"]},
            status_code=503,
            headers={"Retry-After": "30"},
        )
    if result["work"] is None:
        return JSONResponse(
            {
                "detail": "DOI is not present in the imported portion of the local snapshot",
                "dataset_version": result["dataset_version"],
                "complete": result["complete"],
            },
            status_code=404,
            headers={"X-OpenAlex-Catalog-Complete": str(result["complete"]).lower()},
        )
    return result


@app.get(
    "/v1/openalex/works",
    response_model=SearchResponse,
    description=(
        "A DOI queries the local identifier index. Text search first attempts a bounded "
        "exact-title lookup, then returns matching keyword results. Pass next_cursor unchanged."
    ),
    responses={503: {"description": "No local snapshot available or local SQLite search exceeded its time budget"}},
)
def search_works(
    search: str = Query(min_length=2, max_length=256),
    cursor: str | None = Query(default=None, max_length=64),
    per_page: int = Query(default=20, ge=1, le=50),
):
    try:
        result = catalog.search(search, cursor, per_page)
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=422)
    if result["dataset_version"] is None:
        return JSONResponse(
            {"detail": "OpenAlex snapshot has not been imported", "state": result["state"]},
            status_code=503,
            headers={"Retry-After": "30"},
        )
    return result
