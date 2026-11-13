from __future__ import annotations

import json as _json
import math
import os
import asyncio
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Sequence

DEFAULT_ENDPOINT = "https://radar.region-a.example.invalid"
TRACEPARENT_HEADER = b"traceparent"

# Carried verbatim in the Capabilities wire payload's `sdk_version` field so
# the Radar receiver can spot SDK skew across deployed binaries.
SDK_VERSION = "0.2.2"

# 15 min default for the background capability heartbeat — long enough to
# stay under per-project rate limits across all 48 workspace projects but
# short enough to keep `last_push_at` inside the scanner's 1h freshness
# window.
CAPABILITIES_HEARTBEAT_SECS = 15 * 60


@dataclass(frozen=True)
class ClientConfig:
    code: str
    endpoint: str = DEFAULT_ENDPOINT
    service_name: str = "xradar"
    release: str | None = None
    environment: str | None = None
    client_kind: str = "server"
    runtime: str = "python"
    platform: str = "server"
    slow_trace_ms: int = 1000

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ClientConfig | None":
        return config_from_env(env)


@dataclass(frozen=True)
class Span:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    span_name: str
    span_kind: str
    started_at_ms: int
    started_at: float
    duration_ms: int | None = None

    @classmethod
    def root(cls, span_name: str, span_kind: str = "internal") -> "Span":
        return cls(
            trace_id=uuid.uuid4().hex,
            span_id=uuid.uuid4().hex[:16],
            parent_span_id=None,
            span_name=span_name,
            span_kind=span_kind,
            started_at_ms=current_time_millis(),
            started_at=time.perf_counter(),
        )

    @classmethod
    def server_root(cls, span_name: str, parent: Mapping[str, str] | None = None) -> "Span":
        return cls(
            trace_id=parent["trace_id"] if parent else uuid.uuid4().hex,
            span_id=uuid.uuid4().hex[:16],
            parent_span_id=parent.get("parent_span_id") if parent else None,
            span_name=span_name,
            span_kind="server",
            started_at_ms=current_time_millis(),
            started_at=time.perf_counter(),
        )

    @classmethod
    def fixed(
        cls,
        span_name: str,
        span_kind: str,
        trace_id: str,
        span_id: str,
        started_at_ms: int,
        duration_ms: int,
        parent_span_id: str | None = None,
    ) -> "Span":
        return cls(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            span_name=span_name,
            span_kind=span_kind,
            started_at_ms=started_at_ms,
            started_at=time.perf_counter(),
            duration_ms=duration_ms,
        )

    def traceparent(self) -> str:
        return f"00-{self.trace_id}-{self.span_id}-01"

    def child(self, span_name: str, span_kind: str = "internal") -> "Span":
        return Span(
            trace_id=self.trace_id,
            span_id=uuid.uuid4().hex[:16],
            parent_span_id=self.span_id,
            span_name=span_name,
            span_kind=span_kind,
            started_at_ms=current_time_millis(),
            started_at=time.perf_counter(),
        )

    def elapsed_ms(self) -> int:
        if self.duration_ms is not None:
            return self.duration_ms
        return round((time.perf_counter() - self.started_at) * 1000)


@dataclass(frozen=True)
class SpanBatchItem:
    span: Span
    status: str = "ok"
    data: Mapping[str, Any] | None = None


def config_from_env(env: Mapping[str, str] | None = None) -> ClientConfig | None:
    source = env if env is not None else os.environ
    code = env_value(source, "W_RADAR_CODE")
    if code is None:
        return None
    return ClientConfig(
        code=code,
        endpoint=(
            env_value(source, "W_RADAR_ENDPOINT")
            or env_value(source, "W_RADAR_URL")
            or env_value(source, "W_RADAR_ORIGIN")
            or DEFAULT_ENDPOINT
        ).rstrip("/"),
        service_name=env_value(source, "SERVICE_NAME") or "python",
        release=env_value(source, "APP_RELEASE") or env_value(source, "RELEASE"),
        environment=env_value(source, "APP_ENV") or env_value(source, "ENVIRONMENT"),
        client_kind=env_value(source, "XRADAR_CLIENT_KIND") or "server",
        runtime=env_value(source, "XRADAR_RUNTIME") or "python",
        platform=env_value(source, "XRADAR_PLATFORM") or "server",
        slow_trace_ms=parse_int(
            env_value(source, "W_RADAR_SLOW_TRACE_MS") or env_value(source, "SLOW_TRACE_MS"),
            1000,
        ),
    )


def build_span_payload(
    config: ClientConfig,
    span: Span,
    status: str = "ok",
    extra_data: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return build_span_batch_payload(config, [SpanBatchItem(span, status, extra_data)])


def build_span_batch_payload(
    config: ClientConfig,
    span_items: Sequence[Span | SpanBatchItem],
) -> dict[str, Any]:
    items = [normalize_span_item(item) for item in span_items]
    service_name = config.service_name or "python"
    first_span = items[0].span if items else None
    return {
        "code": config.code,
        "session_id": f"trace_{first_span.trace_id}" if first_span else "trace_unknown",
        "visitor_id": f"service:{service_name}",
        "events": [
            build_span_event(config, item.span, item.status, item.data)
            for item in items
        ],
        "session_info": {
            "client_kind": config.client_kind or "server",
            "client_version": config.release,
        },
    }


def build_span_event(
    config: ClientConfig,
    span: Span,
    status: str = "ok",
    extra_data: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    duration_ms = span.elapsed_ms()
    service_name = config.service_name or "python"
    data: dict[str, Any] = {
        "trace_id": span.trace_id,
        "span_id": span.span_id,
        "parent_span_id": span.parent_span_id,
        "traceparent": span.traceparent(),
        "span_name": span.span_name,
        "span_kind": span.span_kind,
        "duration_ms": duration_ms,
        "status": status or "ok",
        "service_name": service_name,
        "runtime": config.runtime or "python",
        "platform": config.platform or "server",
        "release": config.release,
        "environment": config.environment,
        "slow_trace": duration_ms >= config.slow_trace_ms,
        "telemetry.sdk.name": "xradar",
        "telemetry.sdk.language": "python",
        "telemetry.sdk.version": SDK_VERSION,
        "service.name": service_name,
        "service.version": config.release,
        "deployment.environment": config.environment,
    }
    if extra_data:
        data.update(dict(extra_data))

    return {
        "type": "performance_span",
        "name": span.span_name,
        "url": data.get("http.route") or data.get("url"),
        "data": data,
        "ts": span.started_at_ms,
    }


def normalize_span_item(item: Span | SpanBatchItem) -> SpanBatchItem:
    if isinstance(item, SpanBatchItem):
        return item
    return SpanBatchItem(item)


def build_error_payload(
    config: ClientConfig,
    message: str,
    exception_type: str,
    handled: bool,
    severity: str,
    context: Mapping[str, Any] | None = None,
    timestamp_ms: int | None = None,
    stack: str | None = None,
    url: str | None = None,
    screen: str | None = None,
    breadcrumbs: Sequence[Any] | None = None,
    tags: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    merged_tags: dict[str, Any] = {"service_name": config.service_name}
    if tags:
        merged_tags.update(dict(tags))
    payload: dict[str, Any] = {
        "code": config.code,
        "platform": config.platform,
        "runtime": config.runtime,
        "release": config.release,
        "environment": config.environment,
        "message": message,
        "exception_type": exception_type,
        "handled": handled,
        "severity": severity,
        "url": url,
        "screen": screen,
        "breadcrumbs": list(breadcrumbs or []),
        "tags": merged_tags,
        "context": dict(context or {}),
        "timestamp": timestamp_ms if timestamp_ms is not None else current_time_millis(),
    }
    if stack is not None:
        payload["stack"] = stack
    return payload


def record_span(
    config: ClientConfig,
    span: Span,
    status: str = "ok",
    extra_data: Mapping[str, Any] | None = None,
    timeout_seconds: float = 2.0,
) -> None:
    record_spans(config, [SpanBatchItem(span, status, extra_data)], timeout_seconds)


def record_spans(
    config: ClientConfig,
    span_items: Sequence[Span | SpanBatchItem],
    timeout_seconds: float = 2.0,
) -> None:
    if not config.code:
        return
    payload = build_span_batch_payload(config, span_items)
    if not payload["events"]:
        return
    post_json(f"{config.endpoint.rstrip('/')}/collect", payload, timeout_seconds)


def capture_error(
    config: ClientConfig,
    message: str,
    exception_type: str,
    handled: bool = False,
    severity: str = "error",
    context: Mapping[str, Any] | None = None,
    stack: str | None = None,
    url: str | None = None,
    screen: str | None = None,
    breadcrumbs: Sequence[Any] | None = None,
    tags: Mapping[str, Any] | None = None,
    timeout_seconds: float = 2.0,
) -> None:
    if not config.code:
        return
    post_json(
        f"{config.endpoint.rstrip('/')}/errors/capture",
        build_error_payload(
            config,
            message,
            exception_type,
            handled,
            severity,
            context,
            stack=stack,
            url=url,
            screen=screen,
            breadcrumbs=breadcrumbs,
            tags=tags,
        ),
        timeout_seconds,
    )


class XRadarAsgiMiddleware:
    def __init__(
        self,
        app: Callable[..., Awaitable[None]],
        *,
        config: ClientConfig | None = None,
        background: bool = True,
        route_name: Callable[[Mapping[str, Any]], str] | None = None,
    ) -> None:
        self.app = app
        self.config = config
        self.background = background
        self.route_name = route_name

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        config = self.config or config_from_env()
        path = scope.get("path") or "/"
        if config is None or should_skip_path(path):
            await self.app(scope, receive, send)
            return

        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        parent = parse_traceparent(headers.get(TRACEPARENT_HEADER, b"").decode("latin1"))
        route = self.route_name(scope) if self.route_name else normalize_path(path)
        span = Span.server_root(f"http.server {scope.get('method')} {route}", parent)
        status_code = 500

        async def wrapped_send(message):
            nonlocal status_code
            if message.get("type") == "http.response.start":
                status_code = int(message.get("status", 500))
                response_headers = list(message.get("headers", []))
                response_headers.append((TRACEPARENT_HEADER, span.traceparent().encode("ascii")))
                message = {**message, "headers": response_headers}
            await send(message)

        try:
            await self.app(scope, receive, wrapped_send)
        except BaseException as error:
            self._record_span(config, span, "error", scope, route, status_code)
            self._capture_error(config, error, scope, span)
            raise
        self._record_span(
            config,
            span,
            "error" if status_code >= 500 else "ok",
            scope,
            route,
            status_code,
        )

    def _record_span(
        self,
        config: ClientConfig,
        span: Span,
        status: str,
        scope: Mapping[str, Any],
        route: str,
        status_code: int,
    ) -> None:
        path = scope.get("path") or "/"
        query_string = scope.get("query_string") or b""
        target = path
        if query_string:
            target = f"{target}?{query_string.decode('latin1')}"
        data = {
            "http.method": scope.get("method"),
            "http.route": route,
            "http.status_code": status_code,
            "http.target": target,
        }

        def emit() -> None:
            record_span(config, span, status, data)

        if self.background:
            asyncio.create_task(asyncio.to_thread(emit))
        else:
            emit()

    def _capture_error(
        self,
        config: ClientConfig,
        error: BaseException,
        scope: Mapping[str, Any],
        span: Span,
    ) -> None:
        context = {
            "trace_id": span.trace_id,
            "span_id": span.span_id,
            "method": scope.get("method"),
            "path": scope.get("path"),
        }

        def emit() -> None:
            capture_error(
                config,
                str(error) or error.__class__.__name__,
                error.__class__.__name__,
                handled=False,
                severity="fatal",
                context=context,
                breadcrumbs=[{"type": "http", "name": span.span_name}],
            )

        if self.background:
            asyncio.create_task(asyncio.to_thread(emit))
        else:
            emit()


def parse_traceparent(value: str | None) -> dict[str, str] | None:
    if not value:
        return None
    parts = value.split("-")
    if len(parts) != 4:
        return None
    version, trace_id, span_id, flags = parts
    if (
        version != "00"
        or len(trace_id) != 32
        or len(span_id) != 16
        or len(flags) != 2
        or not all(ch in "0123456789abcdefABCDEF" for ch in trace_id)
        or not all(ch in "0123456789abcdefABCDEF" for ch in span_id)
        or not all(ch in "0123456789abcdefABCDEF" for ch in flags)
    ):
        return None
    parent_span_id = span_id.lower()
    return {
        "trace_id": trace_id.lower(),
        "parent_span_id": parent_span_id,
        "span_id": parent_span_id,
    }


def normalize_path(path: str) -> str:
    clean_path = (path or "/").split("?", 1)[0]
    if clean_path == "/":
        return "/"
    return "/".join(
        ":id" if high_cardinality_segment(segment) else segment
        for segment in clean_path.split("/")
    )


def should_skip_path(path: str) -> bool:
    clean_path = (path or "/").split("?", 1)[0].lower()
    suffixes = (
        ".js",
        ".css",
        ".ico",
        ".svg",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".woff",
        ".woff2",
    )
    return (
        clean_path in {"/health", "/healthz", "/ready", "/metrics", "/collect", "/errors/capture"}
        or clean_path.startswith("/health/")
        or clean_path.startswith("/collect/")
        or clean_path.startswith("/errors/capture/")
        or clean_path.startswith("/pixel/")
        or clean_path.startswith("/assets/")
        or clean_path.startswith("/static/")
        or clean_path.endswith(suffixes)
    )


def env_value(env: Mapping[str, str], key: str) -> str | None:
    value = env.get(key)
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def parse_int(value: str | None, fallback: int) -> int:
    if value is None:
        return fallback
    try:
        return int(value)
    except ValueError:
        return fallback


def current_time_millis() -> int:
    return int(time.time() * 1000)


def high_cardinality_segment(segment: str) -> bool:
    if not segment:
        return False
    if segment.startswith(":"):
        return True
    if segment.startswith("{") and segment.endswith("}"):
        return True
    if segment.isdigit():
        return True
    if len(segment) < 16:
        return False
    allowed = all(ch.isalnum() or ch in "-_" for ch in segment)
    has_digit = any(ch.isdigit() for ch in segment)
    return allowed and has_digit


def post_json(url: str, payload: dict[str, Any], timeout_seconds: float) -> None:
    try:
        import json

        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=body,
            headers={"content-type": "application/json", "user-agent": f"xradar/{SDK_VERSION}"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout_seconds):
            return
    except (OSError, urllib.error.URLError, ValueError):
        return


# ============================================================
# Phase-2 capability self-report
# ============================================================
#
# Mirrors sdk/rust/xradar (canonical) and sdk/js/xradar. The wire payload
# MUST stay byte-identical across all three SDKs — the Radar receiver
# decodes any of them into the same struct.


@dataclass
class TenancyState:
    mirror_installed: bool = False
    auth_callback_wires_mirror: bool = False
    principal_can_access_uses: int = 0
    user_owned_tables: list[str] = field(default_factory=list)


@dataclass
class GitState:
    branch: str | None = None
    sha: str | None = None
    dirty: bool = False


@dataclass
class ProcessInfo:
    pid: int = 0
    started_at: int = 0
    uptime_seconds: int = 0

    @classmethod
    def now(cls) -> "ProcessInfo":
        started_at = int(time.time())
        return cls(pid=os.getpid(), started_at=started_at, uptime_seconds=0)


@dataclass
class Capabilities:
    project_id: str | None = None
    instance_id: str | None = None
    host: str | None = None
    runtime: str = "python"
    sdks: dict[str, str] = field(default_factory=dict)
    tenancy: TenancyState | None = None
    git: GitState | None = None
    process: ProcessInfo = field(default_factory=ProcessInfo.now)
    reported_at: int = 0
    sdk_version: str = SDK_VERSION

    @classmethod
    def host_introspect(cls, project_id: str) -> "Capabilities":
        """Build a sensible-defaults Capabilities. Host layers extra detail
        on via the with_* builders."""
        proc = ProcessInfo.now()
        host = os.environ.get("HOSTNAME") or socket.gethostname() or None
        return cls(
            project_id=project_id,
            instance_id=None,
            host=host or None,
            runtime="python",
            sdks={"xradar": SDK_VERSION},
            tenancy=None,
            git=None,
            process=proc,
            reported_at=proc.started_at,
            sdk_version=SDK_VERSION,
        )

    def with_project_id(self, project_id: str) -> "Capabilities":
        self.project_id = project_id
        return self

    def with_instance_id(self, instance_id: str) -> "Capabilities":
        self.instance_id = instance_id
        return self

    def with_host(self, host: str) -> "Capabilities":
        self.host = host
        return self

    def with_runtime(self, runtime: str) -> "Capabilities":
        self.runtime = runtime
        return self

    def with_sdk(self, name: str, version: str) -> "Capabilities":
        self.sdks[name] = version
        return self

    def with_tenancy(
        self,
        *,
        mirror_installed: bool = False,
        auth_callback_wires_mirror: bool = False,
        principal_can_access_uses: int = 0,
        user_owned_tables: Sequence[str] | None = None,
    ) -> "Capabilities":
        self.tenancy = TenancyState(
            mirror_installed=mirror_installed,
            auth_callback_wires_mirror=auth_callback_wires_mirror,
            principal_can_access_uses=int(principal_can_access_uses),
            user_owned_tables=list(user_owned_tables or []),
        )
        return self

    def with_git(
        self,
        *,
        branch: str | None = None,
        sha: str | None = None,
        dirty: bool = False,
    ) -> "Capabilities":
        self.git = GitState(branch=branch, sha=sha, dirty=bool(dirty))
        return self

    def with_git_from_env(self, env: Mapping[str, str] | None = None) -> "Capabilities":
        source = env if env is not None else os.environ
        sha = env_value(source, "XRADAR_HOST_GIT_SHA")
        branch = env_value(source, "XRADAR_HOST_GIT_BRANCH")
        dirty_raw = env_value(source, "XRADAR_HOST_GIT_DIRTY")
        dirty = dirty_raw in {"1", "true", "yes"} if dirty_raw is not None else False
        self.git = GitState(branch=branch, sha=sha, dirty=dirty)
        return self

    def with_process(
        self,
        *,
        pid: int,
        started_at: int,
        uptime_seconds: int = 0,
    ) -> "Capabilities":
        self.process = ProcessInfo(
            pid=int(pid),
            started_at=int(started_at),
            uptime_seconds=int(uptime_seconds),
        )
        return self

    def with_reported_at(self, reported_at: int) -> "Capabilities":
        self.reported_at = int(reported_at)
        return self


def build_capabilities_payload(caps: Capabilities) -> dict[str, Any]:
    """Serialize a Capabilities into the canonical wire dict. Optional
    branches (tenancy/git) appear only when set so Radar's deserializer can
    tell "host didn't provide" from "host said false"."""
    payload: dict[str, Any] = {
        "project_id": caps.project_id,
        "instance_id": caps.instance_id,
        "host": caps.host,
        "runtime": caps.runtime,
        "sdks": dict(caps.sdks),
        "process": {
            "pid": int(caps.process.pid),
            "started_at": int(caps.process.started_at),
            "uptime_seconds": int(caps.process.uptime_seconds),
        },
        "reported_at": int(caps.reported_at) if caps.reported_at else int(time.time()),
        "sdk_version": caps.sdk_version,
    }
    if caps.tenancy is not None:
        payload["tenancy"] = {
            "mirror_installed": bool(caps.tenancy.mirror_installed),
            "auth_callback_wires_mirror": bool(caps.tenancy.auth_callback_wires_mirror),
            "principal_can_access_uses": int(caps.tenancy.principal_can_access_uses),
            "user_owned_tables": list(caps.tenancy.user_owned_tables),
        }
    if caps.git is not None:
        payload["git"] = {
            "branch": caps.git.branch,
            "sha": caps.git.sha,
            "dirty": bool(caps.git.dirty),
        }
    return payload


def report_capabilities(
    client: "ClientConfig | XRadarClient",
    caps: Capabilities,
    timeout_seconds: float = 5.0,
) -> bool:
    """POST a capability snapshot to Radar's /api/platform/runtime/capabilities.
    Returns True on 2xx, False on any error (transport, non-2xx, disabled).
    Honors `XRADAR_DISABLE_CAPABILITIES=1` as a kill switch."""
    if os.environ.get("XRADAR_DISABLE_CAPABILITIES") == "1":
        return False
    config: ClientConfig = client if isinstance(client, ClientConfig) else client.config
    endpoint = config.endpoint.rstrip("/")
    url = f"{endpoint}/api/platform/runtime/capabilities"
    payload = build_capabilities_payload(caps)
    body = _json.dumps(payload).encode("utf-8")
    headers = {"content-type": "application/json", "user-agent": f"xradar/{SDK_VERSION}"}
    token = os.environ.get("W_RADAR_API_KEY")
    if token:
        headers["authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as resp:
            code = resp.getcode() or 0
            return 200 <= code < 300
    except urllib.error.HTTPError as exc:
        return 200 <= exc.code < 300
    except (OSError, urllib.error.URLError, ValueError):
        return False


def spawn_capabilities_heartbeat(
    client: "ClientConfig | XRadarClient",
    caps: Capabilities,
    interval_seconds: float = CAPABILITIES_HEARTBEAT_SECS,
    stop_event: "threading.Event | None" = None,
) -> "Future[None]":
    """Spawn a background heartbeat that posts once on start, then every
    `interval_seconds`. Set the supplied stop_event, then call Future.result()
    to wait for shutdown (an in-flight request has a five-second timeout).
    Future.cancel() only cancels before the thread starts. The daemon thread
    never keeps the host process alive on its own.
    No-op (immediate return) when `XRADAR_DISABLE_CAPABILITIES=1`."""

    if os.environ.get("XRADAR_DISABLE_CAPABILITIES") == "1":
        f: Future = Future()
        f.set_result(None)
        return f

    stop = stop_event or threading.Event()
    if not math.isfinite(interval_seconds) or not 0 < interval_seconds <= threading.TIMEOUT_MAX:
        interval_seconds = CAPABILITIES_HEARTBEAT_SECS
    task: Future[None] = Future()

    def _loop() -> None:
        # Push immediately, then sleep and repeat. The sleep is interruptible
        # via `stop_event.wait(interval)` so graceful shutdown is instant.
        config = client if isinstance(client, ClientConfig) else client.config
        while not stop.is_set():
            snapshot = Capabilities(
                project_id=caps.project_id,
                instance_id=caps.instance_id,
                host=caps.host,
                runtime=caps.runtime,
                sdks=dict(caps.sdks),
                tenancy=caps.tenancy,
                git=caps.git,
                process=ProcessInfo(
                    pid=caps.process.pid,
                    started_at=caps.process.started_at,
                    uptime_seconds=max(
                        0,
                        int(time.time()) - int(caps.process.started_at),
                    ),
                ),
                reported_at=int(time.time()),
                sdk_version=caps.sdk_version,
            )
            try:
                report_capabilities(config, snapshot)
            except Exception:
                # Telemetry must never crash the host.
                pass
            if stop.wait(interval_seconds):
                break

    def _run() -> None:
        if not task.set_running_or_notify_cancel():
            return
        try:
            _loop()
        except Exception as exc:
            task.set_exception(exc)
        else:
            task.set_result(None)

    threading.Thread(target=_run, name="xradar-cap-heartbeat", daemon=True).start()
    return task


# Forward-declared for the heartbeat type hint above; XRadarClient does not
# exist in this stdlib-only SDK so we alias it onto ClientConfig — callers
# can pass either.
XRadarClient = ClientConfig
