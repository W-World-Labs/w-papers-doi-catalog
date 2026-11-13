from __future__ import annotations

import ipaddress
from collections.abc import Awaitable, Callable
from typing import Any

from starlette.responses import JSONResponse


class TrustedClientMiddleware:
    """Allow exact socket peer IPs; forwarded headers are intentionally ignored."""

    def __init__(self, app: Callable[..., Awaitable[None]], trusted_ips: frozenset[str]):
        self.app = app
        self.trusted_ips = trusted_ips

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        client = scope.get("client")
        try:
            peer = str(ipaddress.ip_address(client[0])) if client else ""
        except (ValueError, TypeError):
            peer = ""
        if peer not in self.trusted_ips:
            response = JSONResponse({"detail": "client IP is not allowed"}, status_code=403)
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)
