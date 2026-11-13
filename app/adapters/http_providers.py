"""Fixed-origin Crossref and OpenAlex metadata adapters."""

from __future__ import annotations

import json
import re
from datetime import date
from typing import Any
from urllib.parse import quote

import httpx

from app.application.errors import ProviderUnavailable
from app.domain import DoiWork


_TYPE_MAP = {
    "journal-article": "article",
    "posted-content": "preprint",
    "proceedings-article": "proceedings",
    "book-chapter": "chapter",
}


def _text(value: Any, maximum: int = 1000) -> str | None:
    text = " ".join(str(value or "").split())
    return text[:maximum] or None


def _author(given: Any, family: Any, display: Any = None) -> str | None:
    name = " ".join(part for part in (_text(given, 150), _text(family, 150)) if part)
    return name or _text(display, 300)


def _crossref_date(message: dict[str, Any]) -> str | None:
    parts = ((message.get("issued") or {}).get("date-parts") or [[]])[0]
    try:
        numbers = [int(value) for value in parts[:3]]
    except (TypeError, ValueError):
        return None
    if not numbers:
        return None
    try:
        if len(numbers) == 1:
            return f"{numbers[0]:04d}"
        if len(numbers) == 2:
            return f"{numbers[0]:04d}-{numbers[1]:02d}"
        return date(numbers[0], numbers[1], numbers[2]).isoformat()
    except ValueError:
        return None


class _JsonProvider:
    name = "provider"

    def __init__(
        self,
        base_url: str,
        *,
        user_agent: str,
        connect_timeout: float,
        read_timeout: float,
        max_response_bytes: int,
        client: httpx.Client | None = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._max_response_bytes = max_response_bytes
        self._owns_client = client is None
        self._client = client or httpx.Client(
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=httpx.Timeout(read_timeout, connect=connect_timeout),
            follow_redirects=False,
            trust_env=False,
        )

    def _get(self, url: str) -> dict[str, Any] | None:
        try:
            with self._client.stream("GET", url) as response:
                if response.status_code == 404:
                    return None
                if response.status_code != 200:
                    raise ProviderUnavailable(
                        f"{self.name} returned HTTP {response.status_code}"
                    )
                length = response.headers.get("content-length")
                if length and length.isdigit() and int(length) > self._max_response_bytes:
                    raise ProviderUnavailable(f"{self.name} response is too large")
                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > self._max_response_bytes:
                        raise ProviderUnavailable(f"{self.name} response is too large")
                    chunks.append(chunk)
        except httpx.HTTPError as exc:
            raise ProviderUnavailable(f"{self.name} request failed") from exc
        try:
            payload = json.loads(b"".join(chunks))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProviderUnavailable(f"{self.name} returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise ProviderUnavailable(f"{self.name} returned an invalid object")
        return payload

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


class CrossrefProvider(_JsonProvider):
    name = "crossref"

    def lookup(self, doi: str) -> DoiWork | None:
        payload = self._get(f"{self._base_url}/{quote(doi, safe='')}")
        if payload is None:
            return None
        message = payload.get("message")
        if not isinstance(message, dict):
            raise ProviderUnavailable("crossref response has no message")
        response_doi = str(message.get("DOI") or doi).strip().lower()
        authors = tuple(
            name
            for item in (message.get("author") or [])
            if isinstance(item, dict)
            and (name := _author(item.get("given"), item.get("family")))
        )[:100]
        containers = message.get("container-title") or message.get(
            "short-container-title"
        ) or []
        title_values = message.get("title") or []
        work_type = str(message.get("type") or "")
        try:
            return DoiWork(
                doi=response_doi,
                title=_text(title_values[0] if title_values else "", 2000) or "",
                authors=authors,
                journal=_text(containers[0] if containers else "", 500),
                publisher=_text(message.get("publisher"), 500),
                published_at=_crossref_date(message),
                volume=_text(message.get("volume"), 100),
                issue=_text(message.get("issue"), 100),
                page=_text(message.get("page"), 100),
                article_type=_TYPE_MAP.get(work_type, work_type or None),
                issn=tuple(str(item).strip() for item in (message.get("ISSN") or []) if item)[:8],
                source=self.name,
            )
        except ValueError as exc:
            raise ProviderUnavailable("crossref returned an invalid DOI record") from exc


class OpenAlexProvider(_JsonProvider):
    name = "openalex"

    def lookup(self, doi: str) -> DoiWork | None:
        identity = quote(f"https://doi.org/{doi}", safe="")
        payload = self._get(f"{self._base_url}/{identity}")
        if payload is None or payload.get("error"):
            return None
        ids = payload.get("ids") or {}
        response_doi = str(ids.get("doi") or payload.get("doi") or doi)
        response_doi = re.sub(r"^https?://doi\.org/", "", response_doi, flags=re.I).lower()
        authors = tuple(
            name
            for item in (payload.get("authorships") or [])
            if isinstance(item, dict)
            and (
                name := _author(
                    None,
                    None,
                    (item.get("author") or {}).get("display_name"),
                )
            )
        )[:100]
        location = payload.get("primary_location") or {}
        source = location.get("source") or {}
        biblio = payload.get("biblio") or {}
        first_page = _text(biblio.get("first_page"), 100)
        last_page = _text(biblio.get("last_page"), 100)
        page = "-".join(value for value in (first_page, last_page) if value) or None
        work_type = str(payload.get("type") or "")
        try:
            return DoiWork(
                doi=response_doi,
                title=_text(payload.get("display_name") or payload.get("title"), 2000)
                or "",
                authors=authors,
                journal=_text(source.get("display_name"), 500),
                publisher=_text(source.get("host_organization_name"), 500),
                published_at=_text(
                    payload.get("publication_date") or payload.get("publication_year"), 100
                ),
                volume=_text(biblio.get("volume"), 100),
                issue=_text(biblio.get("issue"), 100),
                page=page,
                article_type=_TYPE_MAP.get(work_type, work_type or None),
                issn=tuple(str(item).strip() for item in (source.get("issn") or []) if item)[:8],
                source=self.name,
            )
        except ValueError as exc:
            raise ProviderUnavailable("openalex returned an invalid DOI record") from exc
