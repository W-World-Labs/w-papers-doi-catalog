from __future__ import annotations

import time
from dataclasses import dataclass

from app.domain import DoiWork, normalize_doi

from .errors import DoiNotFound, InvalidDoi, ProviderUnavailable, UpstreamUnavailable
from .ports import Clock, DoiCache, MetadataProvider


@dataclass(frozen=True, slots=True)
class LookupResult:
    work: DoiWork
    cache_status: str


class DoiLookup:
    def __init__(
        self,
        cache: DoiCache,
        providers: tuple[MetadataProvider, ...],
        *,
        positive_ttl_seconds: int,
        negative_ttl_seconds: int,
        clock: Clock = time.time,
    ):
        if positive_ttl_seconds < 1 or negative_ttl_seconds < 1:
            raise ValueError("cache TTLs must be positive")
        self._cache = cache
        self._providers = providers
        self._positive_ttl = positive_ttl_seconds
        self._negative_ttl = negative_ttl_seconds
        self._clock = clock

    @property
    def provider_names(self) -> tuple[str, ...]:
        return tuple(provider.name for provider in self._providers)

    def execute(self, raw_doi: str) -> LookupResult:
        doi = normalize_doi(raw_doi)
        if doi is None:
            raise InvalidDoi("invalid DOI")
        now = self._clock()
        cached = self._cache.get(doi)
        if cached is not None and cached.expires_at > now:
            if cached.status == "found" and cached.work is not None:
                return LookupResult(cached.work, "hit")
            if cached.status == "missing":
                raise DoiNotFound("negative")

        stale = cached.work if cached and cached.status == "found" else None
        failures = 0
        successful_misses = 0
        for provider in self._providers:
            try:
                work = provider.lookup(doi)
            except ProviderUnavailable:
                failures += 1
                continue
            if work is None:
                successful_misses += 1
                continue
            if work.doi != doi:
                failures += 1
                continue
            self._cache.put_found(work, now + self._positive_ttl)
            return LookupResult(work, "miss")

        if failures:
            if stale is not None:
                return LookupResult(stale, "stale")
            raise UpstreamUnavailable("DOI metadata providers are unavailable")
        if not self._providers or successful_misses != len(self._providers):
            raise UpstreamUnavailable("no DOI metadata provider is available")
        self._cache.put_missing(doi, now + self._negative_ttl)
        raise DoiNotFound("miss")
