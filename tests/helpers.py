from __future__ import annotations

from pathlib import Path

from app.application.ports import CacheEntry
from app.domain import DoiWork


class MemoryCache:
    def __init__(self):
        self.entries: dict[str, CacheEntry] = {}

    def get(self, doi: str):
        return self.entries.get(doi)

    def put_found(self, work: DoiWork, expires_at: float):
        self.entries[work.doi] = CacheEntry("found", work, expires_at)

    def put_missing(self, doi: str, expires_at: float):
        self.entries[doi] = CacheEntry("missing", None, expires_at)

    def health(self):
        return {"reachable": True, "entries": len(self.entries)}

    def close(self):
        pass


class Provider:
    def __init__(self, name="test", result=None, error=None):
        self.name = name
        self.result = result
        self.error = error
        self.calls: list[str] = []

    def lookup(self, doi: str):
        self.calls.append(doi)
        if self.error:
            raise self.error
        return self.result

    def close(self):
        pass


def work(doi: str = "10.1234/example", source: str = "test") -> DoiWork:
    return DoiWork(doi=doi, title="Example", authors=("A. Author",), source=source)


def env(tmp: Path, **overrides: str) -> dict[str, str]:
    values = {
        "DOI_CACHE_PATH": str(tmp / "catalog.sqlite"),
        "DOI_BACKUP_DIR": str(tmp / "backups"),
        "TRUSTED_CLIENT_IPS": "127.0.0.1,::1",
    }
    values.update(overrides)
    return values
