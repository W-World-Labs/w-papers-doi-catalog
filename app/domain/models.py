"""Provider-independent DOI identities and bibliography records."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote


_DOI_PREFIX = re.compile(r"^(?:doi:\s*|https?://(?:dx\.)?doi\.org/)", re.I)
_DOI_RE = re.compile(r"^10\.\d{4,9}/[-._;()/:a-z0-9]+$", re.I)


def normalize_doi(value: str | None) -> str | None:
    """Return a canonical lowercase DOI, rejecting URLs other than doi.org."""
    if not isinstance(value, str):
        return None
    candidate = unquote(value.strip())
    candidate = _DOI_PREFIX.sub("", candidate).strip().lower()
    if not candidate or len(candidate) > 255:
        return None
    if any(ord(char) < 0x20 or char.isspace() for char in candidate):
        return None
    return candidate if _DOI_RE.fullmatch(candidate) else None


def _optional(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


@dataclass(frozen=True, slots=True)
class DoiWork:
    doi: str
    title: str = ""
    authors: tuple[str, ...] = ()
    journal: str | None = None
    publisher: str | None = None
    published_at: str | None = None
    volume: str | None = None
    issue: str | None = None
    page: str | None = None
    article_type: str | None = None
    issn: tuple[str, ...] = ()
    source: str = "unknown"

    def __post_init__(self) -> None:
        canonical = normalize_doi(self.doi)
        if canonical is None or canonical != self.doi:
            raise ValueError("doi must be canonical")
        if not self.source.strip():
            raise ValueError("source is required")

    def to_contract(self) -> dict[str, Any]:
        """Keep the legacy W Papers `/v1/doi` work shape stable."""
        return {
            "doi": self.doi,
            "title": self.title,
            "authors": list(self.authors),
            "journal": self.journal,
            "publisher": self.publisher,
            "published_at": self.published_at,
            "volume": self.volume,
            "issue": self.issue,
            "page": self.page,
            "article_type": self.article_type,
            "issn": list(self.issn),
            "source": self.source,
            "from_local_catalog": True,
        }

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "DoiWork":
        doi = normalize_doi(str(value.get("doi") or ""))
        if doi is None:
            raise ValueError("cached work has an invalid doi")
        authors = tuple(
            text[:300]
            for item in (value.get("authors") or [])
            if (text := str(item).strip())
        )[:100]
        issn = tuple(
            text[:32]
            for item in (value.get("issn") or [])
            if (text := str(item).strip())
        )[:8]
        return cls(
            doi=doi,
            title=str(value.get("title") or "").strip()[:2000],
            authors=authors,
            journal=_optional(value.get("journal")),
            publisher=_optional(value.get("publisher")),
            published_at=_optional(value.get("published_at")),
            volume=_optional(value.get("volume")),
            issue=_optional(value.get("issue")),
            page=_optional(value.get("page")),
            article_type=_optional(value.get("article_type")),
            issn=issn,
            source=str(value.get("source") or "unknown").strip(),
        )
