"""Environment-only configuration with fail-closed validation."""

from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit


def _int(values: Mapping[str, str], key: str, default: int, low: int, high: int) -> int:
    try:
        value = int(values.get(key, str(default)))
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if not low <= value <= high:
        raise ValueError(f"{key} must be between {low} and {high}")
    return value


def _float(
    values: Mapping[str, str], key: str, default: float, low: float, high: float
) -> float:
    try:
        value = float(values.get(key, str(default)))
    except ValueError as exc:
        raise ValueError(f"{key} must be numeric") from exc
    if not low <= value <= high:
        raise ValueError(f"{key} must be between {low} and {high}")
    return value


def _csv(raw: str) -> tuple[str, ...]:
    return tuple(value.strip() for value in raw.split(",") if value.strip())


def _base_url(raw: str, allowed_hosts: frozenset[str]) -> str:
    parsed = urlsplit(raw.strip())
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme != "https"
        or not host
        or host not in allowed_hosts
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("upstream base URLs must be allowlisted HTTPS origins")
    return raw.rstrip("/")


@dataclass(frozen=True, slots=True)
class Settings:
    bind_host: str
    bind_port: int
    trusted_client_ips: frozenset[str]
    cache_path: Path
    backup_dir: Path
    positive_ttl_seconds: int
    negative_ttl_seconds: int
    provider_order: tuple[str, ...]
    crossref_api_base: str
    openalex_api_base: str
    upstream_allowed_hosts: frozenset[str]
    connect_timeout_seconds: float
    read_timeout_seconds: float
    max_response_bytes: int
    user_agent: str
    backup_retain: int

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "Settings":
        values = environ if environ is not None else os.environ
        allowed_hosts = frozenset(
            host.lower()
            for host in _csv(
                values.get(
                    "UPSTREAM_ALLOWED_HOSTS",
                    "api.crossref.org,api.openalex.org",
                )
            )
        )
        if not allowed_hosts:
            raise ValueError("UPSTREAM_ALLOWED_HOSTS cannot be empty")
        trusted: set[str] = set()
        for raw in _csv(values.get("TRUSTED_CLIENT_IPS", "127.0.0.1,::1")):
            try:
                trusted.add(str(ipaddress.ip_address(raw)))
            except ValueError as exc:
                raise ValueError("TRUSTED_CLIENT_IPS must contain exact IP addresses") from exc
        if not trusted:
            raise ValueError("TRUSTED_CLIENT_IPS cannot be empty")
        providers = _csv(values.get("DOI_PROVIDER_ORDER", "crossref,openalex"))
        if not providers or len(set(providers)) != len(providers) or any(
            provider not in {"crossref", "openalex"} for provider in providers
        ):
            raise ValueError("DOI_PROVIDER_ORDER must contain unique known providers")
        cache_path = Path(
            values.get(
                "DOI_CACHE_PATH",
                "/home/w-data/w-papers-doi-catalog/catalog.sqlite",
            )
        )
        backup_dir = Path(
            values.get(
                "DOI_BACKUP_DIR",
                "/home/w-data/w-papers-doi-catalog/backups",
            )
        )
        if not cache_path.is_absolute() or not backup_dir.is_absolute():
            raise ValueError("DOI cache and backup paths must be absolute")
        user_agent = values.get(
            "DOI_CATALOG_USER_AGENT",
            "W-Papers-DOI-Catalog/1.0 (+https://papers.example.invalid; mailto:operator@example.invalid)",
        ).strip()
        if not user_agent or "mailto:" not in user_agent:
            raise ValueError("DOI_CATALOG_USER_AGENT must include an operator contact")
        return cls(
            bind_host=values.get("BIND_HOST", "0.0.0.0").strip(),
            bind_port=_int(values, "BIND_PORT", 55221, 1, 65535),
            trusted_client_ips=frozenset(trusted),
            cache_path=cache_path,
            backup_dir=backup_dir,
            positive_ttl_seconds=_int(
                values, "DOI_POSITIVE_TTL_SECONDS", 2_592_000, 60, 31_536_000
            ),
            negative_ttl_seconds=_int(
                values, "DOI_NEGATIVE_TTL_SECONDS", 21_600, 60, 2_592_000
            ),
            provider_order=providers,
            crossref_api_base=_base_url(
                values.get("CROSSREF_API_BASE", "https://api.crossref.org/works"),
                allowed_hosts,
            ),
            openalex_api_base=_base_url(
                values.get("OPENALEX_API_BASE", "https://api.openalex.org/works"),
                allowed_hosts,
            ),
            upstream_allowed_hosts=allowed_hosts,
            connect_timeout_seconds=_float(
                values, "UPSTREAM_CONNECT_TIMEOUT_SECONDS", 2.0, 0.1, 30.0
            ),
            read_timeout_seconds=_float(
                values, "UPSTREAM_READ_TIMEOUT_SECONDS", 6.0, 0.1, 60.0
            ),
            max_response_bytes=_int(
                values, "UPSTREAM_MAX_RESPONSE_BYTES", 2_097_152, 1024, 8_388_608
            ),
            user_agent=user_agent,
            backup_retain=_int(values, "DOI_BACKUP_RETAIN", 14, 1, 365),
        )
