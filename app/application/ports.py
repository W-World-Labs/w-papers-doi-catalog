from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from app.domain import DoiWork


@dataclass(frozen=True, slots=True)
class CacheEntry:
    status: str
    work: DoiWork | None
    expires_at: float


class DoiCache(Protocol):
    def get(self, doi: str) -> CacheEntry | None: ...

    def put_found(self, work: DoiWork, expires_at: float) -> None: ...

    def put_missing(self, doi: str, expires_at: float) -> None: ...


class MetadataProvider(Protocol):
    @property
    def name(self) -> str: ...

    def lookup(self, doi: str) -> DoiWork | None: ...

    def close(self) -> None: ...


class Clock(Protocol):
    def __call__(self) -> float: ...
