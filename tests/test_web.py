import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from tests.helpers import MemoryCache, Provider, env, work


class WebTests(unittest.TestCase):
    def make_app(self, root: Path, cache=None, providers=None, **overrides):
        settings = Settings.from_env(env(root, **overrides))
        return create_app(
            settings,
            cache=cache or MemoryCache(),
            providers=providers if providers is not None else (Provider(result=work()),),
            radar_environ={},
        )

    def test_contract_health_metrics_and_no_extra_product_routes(self):
        with tempfile.TemporaryDirectory() as raw:
            app = self.make_app(Path(raw))
            with TestClient(app, client=("127.0.0.1", 12345)) as web:
                self.assertEqual(web.get("/health").status_code, 200)
                self.assertEqual(web.get("/api/health").status_code, 200)
                response = web.get("/v1/doi", params={"doi": "10.1234/example"})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["work"]["doi"], "10.1234/example")
                self.assertEqual(response.headers["x-doi-cache"], "miss")
                metrics = web.get("/metrics")
                self.assertEqual(metrics.status_code, 200)
                self.assertIn("doi_catalog_http_requests_total", metrics.text)
                for path in ("/v1/papers", "/paper", "/library", "/docs", "/openapi.json"):
                    self.assertEqual(web.get(path).status_code, 404)

    def test_untrusted_socket_peer_is_403_even_with_spoofed_forwarded_header(self):
        with tempfile.TemporaryDirectory() as raw:
            app = self.make_app(Path(raw))
            with TestClient(app, client=("203.0.113.8", 12345)) as web:
                response = web.get(
                    "/health", headers={"X-Forwarded-For": "127.0.0.1"}
                )
            self.assertEqual(response.status_code, 403)

    def test_explicit_remote_peer_can_be_allowlisted(self):
        with tempfile.TemporaryDirectory() as raw:
            app = self.make_app(
                Path(raw), TRUSTED_CLIENT_IPS="127.0.0.1,198.51.100.20"
            )
            with TestClient(app, client=("198.51.100.20", 12345)) as web:
                self.assertEqual(web.get("/health").status_code, 200)

    def test_missing_radar_environment_never_attempts_network(self):
        with tempfile.TemporaryDirectory() as raw:
            with patch("xradar.record_span") as record:
                app = self.make_app(Path(raw))
                with TestClient(app, client=("127.0.0.1", 12345)) as web:
                    self.assertEqual(web.get("/health").status_code, 200)
                record.assert_not_called()


if __name__ == "__main__":
    unittest.main()
