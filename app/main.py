from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import Any, Mapping

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from app.adapters.http_providers import CrossrefProvider, OpenAlexProvider
from app.adapters.sqlite_cache import SqliteDoiCache
from app.application.errors import DoiNotFound, InvalidDoi, UpstreamUnavailable
from app.application.use_cases import DoiLookup
from app.config import Settings
from app.web.metrics import Metrics
from app.web.middleware import TrustedClientMiddleware
from xradar import (
    XRadarAsgiMiddleware,
    config_from_env,
)

VERSION = "0.1.0"
PROJECT_ID = "10-15-w-papers-doi-catalog"

_ROUTE_LABELS = {
    "/health": "/health",
    "/api/health": "/api/health",
    "/metrics": "/metrics",
    "/v1/doi": "/v1/doi",
}


def _build_providers(settings: Settings) -> tuple[Any, ...]:
    common = {
        "user_agent": settings.user_agent,
        "connect_timeout": settings.connect_timeout_seconds,
        "read_timeout": settings.read_timeout_seconds,
        "max_response_bytes": settings.max_response_bytes,
    }
    available = {
        "crossref": CrossrefProvider(settings.crossref_api_base, **common),
        "openalex": OpenAlexProvider(settings.openalex_api_base, **common),
    }
    return tuple(available[name] for name in settings.provider_order)


def create_app(
    settings: Settings | None = None,
    *,
    cache: Any | None = None,
    providers: tuple[Any, ...] | None = None,
    radar_environ: Mapping[str, str] | None = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    cache = cache or SqliteDoiCache(settings.cache_path)
    providers = providers if providers is not None else _build_providers(settings)
    lookup = DoiLookup(
        cache,
        providers,
        positive_ttl_seconds=settings.positive_ttl_seconds,
        negative_ttl_seconds=settings.negative_ttl_seconds,
    )
    metrics = Metrics()
    radar_values = radar_environ if radar_environ is not None else os.environ
    radar_config = config_from_env(radar_values)
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        for provider in providers:
            close = getattr(provider, "close", None)
            if close:
                close()
        close_cache = getattr(cache, "close", None)
        if close_cache:
            close_cache()

    app = FastAPI(
        title="W Papers DOI Catalog",
        version=VERSION,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.cache = cache
    app.state.lookup = lookup
    app.state.metrics = metrics

    @app.middleware("http")
    async def observe(request: Request, call_next):
        route = _ROUTE_LABELS.get(request.url.path, "unmatched")
        response = await call_next(request)
        metrics.record_request(request.method, route, response.status_code)
        return response

    @app.get("/health")
    @app.get("/api/health")
    def health() -> dict[str, Any]:
        try:
            cache_health = cache.health()
        except Exception as exc:
            cache_health = {"reachable": False, "error": type(exc).__name__}
        return {
            "status": "ok",
            "service": PROJECT_ID,
            "version": VERSION,
            "providers": list(lookup.provider_names),
            "cache": cache_health,
            "uptime_seconds": round(time.monotonic() - metrics.started_at, 3),
        }

    @app.get("/metrics", response_class=PlainTextResponse)
    def prometheus_metrics() -> str:
        return metrics.render()

    @app.get("/v1/doi")
    def get_doi(doi: str = Query(min_length=7, max_length=255)):
        try:
            result = lookup.execute(doi)
        except InvalidDoi:
            metrics.record_lookup("invalid")
            return JSONResponse({"detail": "invalid DOI"}, status_code=422)
        except DoiNotFound as exc:
            metrics.record_lookup(exc.cache_status)
            return JSONResponse(
                {"detail": "doi not in local catalog"},
                status_code=404,
                headers={"X-DOI-Cache": exc.cache_status},
            )
        except UpstreamUnavailable:
            metrics.record_lookup("upstream_error")
            return JSONResponse(
                {"detail": "DOI metadata providers unavailable"}, status_code=503
            )
        metrics.record_lookup(result.cache_status)
        return JSONResponse(
            {"work": result.work.to_contract()},
            headers={
                "X-DOI-Cache": result.cache_status,
                "Cache-Control": "private, max-age=300",
            },
        )

    if radar_config is not None:
        app.add_middleware(XRadarAsgiMiddleware, config=radar_config)
    app.add_middleware(
        TrustedClientMiddleware, trusted_ips=settings.trusted_client_ips
    )
    return app


app = create_app()
