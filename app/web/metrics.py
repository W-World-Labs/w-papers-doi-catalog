from __future__ import annotations

import threading
import time
from collections import Counter


class Metrics:
    def __init__(self) -> None:
        self.started_at = time.monotonic()
        self._lock = threading.Lock()
        self._requests: Counter[tuple[str, str, int]] = Counter()
        self._lookups: Counter[str] = Counter()

    def record_request(self, method: str, route: str, status: int) -> None:
        with self._lock:
            self._requests[(method, route, status)] += 1

    def record_lookup(self, result: str) -> None:
        with self._lock:
            self._lookups[result] += 1

    def render(self) -> str:
        with self._lock:
            requests = sorted(self._requests.items())
            lookups = sorted(self._lookups.items())
        lines = [
            "# HELP doi_catalog_uptime_seconds Process uptime in seconds.",
            "# TYPE doi_catalog_uptime_seconds gauge",
            f"doi_catalog_uptime_seconds {time.monotonic() - self.started_at:.3f}",
            "# HELP doi_catalog_http_requests_total HTTP requests by stable route.",
            "# TYPE doi_catalog_http_requests_total counter",
        ]
        for (method, route, status), value in requests:
            lines.append(
                'doi_catalog_http_requests_total{method="%s",route="%s",status="%s"} %d'
                % (method, route, status, value)
            )
        lines.extend(
            [
                "# HELP doi_catalog_lookups_total DOI lookup outcomes.",
                "# TYPE doi_catalog_lookups_total counter",
            ]
        )
        for result, value in lookups:
            lines.append(f'doi_catalog_lookups_total{{result="{result}"}} {value}')
        return "\n".join(lines) + "\n"
