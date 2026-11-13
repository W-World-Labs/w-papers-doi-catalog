"""Typed profiling span builders for the xRadar Python SDK.

Each builder is a frozen dataclass with a ``to_event_data()`` method that
returns a plain ``dict`` suitable for passing as ``extra_data`` to
``record_span()``.  The returned dict always includes a ``span_kind``
discriminator field and omits any fields whose value is ``None``.

Usage::

    from xradar.profiling import LlmInferenceSpan
    from xradar import record_span, Span, ClientConfig

    config = ClientConfig(code="OBS-XXX")
    span   = Span.root("llm.generate")
    extra  = LlmInferenceSpan(
        model="deepseek-r1",
        input_tokens=512,
        output_tokens=256,
        ttft_ms=180.5,
    ).to_event_data()
    record_span(config, span, extra_data=extra)
"""

from __future__ import annotations

import dataclasses
from typing import Any


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _to_dict(obj: Any, span_kind: str) -> dict[str, Any]:
    """Serialize a frozen dataclass to a dict, injecting ``span_kind`` and
    omitting any field whose value is ``None``."""
    result: dict[str, Any] = {"span_kind": span_kind}
    for f in dataclasses.fields(obj):
        value = getattr(obj, f.name)
        if value is not None:
            result[f.name] = value
    return result


# ---------------------------------------------------------------------------
# 1. LlmInferenceSpan
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class LlmInferenceSpan:
    """Profiling data for a single LLM inference call."""

    model: str

    input_tokens: int | None = None
    output_tokens: int | None = None
    ttft_ms: float | None = None
    total_latency_ms: float | None = None
    tokens_per_second: float | None = None
    supplier: str | None = None
    batch_size: int | None = None
    kv_cache_hit_ratio: float | None = None
    cost_usd: float | None = None
    gpu_id: str | None = None
    stop_reason: str | None = None

    def to_event_data(self) -> dict[str, Any]:
        return _to_dict(self, "llm.inference")


# ---------------------------------------------------------------------------
# 2. NetworkRequestSpan
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class NetworkRequestSpan:
    """Profiling data for an outbound network request."""

    target_host: str
    method: str

    total_latency_ms: float | None = None
    dns_ms: float | None = None
    tcp_connect_ms: float | None = None
    tls_handshake_ms: float | None = None
    ttfb_ms: float | None = None
    transfer_ms: float | None = None
    response_status: int | None = None
    bytes_sent: int | None = None
    bytes_received: int | None = None
    tls_version: str | None = None
    tls_session_reused: bool | None = None
    cache_status: str | None = None
    protocol: str | None = None

    def to_event_data(self) -> dict[str, Any]:
        return _to_dict(self, "network.request")


# ---------------------------------------------------------------------------
# 3. VideoPipelineSpan
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class VideoPipelineSpan:
    """Profiling data for a single stage in a video-processing pipeline."""

    camera_id: str
    stage: str

    stage_latency_ms: float | None = None
    algorithm_id: str | None = None
    provider: str | None = None
    fps: float | None = None
    frame_width: int | None = None
    frame_height: int | None = None
    frames_dropped: int | None = None
    gpu_memory_mb: float | None = None
    npu_core_id: int | None = None

    def to_event_data(self) -> dict[str, Any]:
        return _to_dict(self, "video.pipeline")


# ---------------------------------------------------------------------------
# 4. PipelineStageSpan
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class PipelineStageSpan:
    """Profiling data for a generic data-pipeline stage."""

    pipeline_id: str
    stage_name: str

    stage_latency_ms: float | None = None
    records_in: int | None = None
    records_out: int | None = None
    error_count: int | None = None
    quality_score: float | None = None
    storage_backend: str | None = None
    bytes_processed: int | None = None

    def to_event_data(self) -> dict[str, Any]:
        return _to_dict(self, "pipeline.stage")


# ---------------------------------------------------------------------------
# 5. InfraHealthSpan
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class InfraHealthSpan:
    """Profiling data for an infrastructure component health check."""

    component: str
    status: str

    pool_total: int | None = None
    pool_idle: int | None = None
    pool_waiting: int | None = None
    buffer_depth: int | None = None
    flush_rate_per_sec: float | None = None
    uptime_seconds: int | None = None
    panic_count: int | None = None
    detection_latency_ms: float | None = None
    rules_matched: int | None = None
    cache_hit_ratio: float | None = None

    def to_event_data(self) -> dict[str, Any]:
        return _to_dict(self, "infra.health")
