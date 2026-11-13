import tempfile
import unittest
from pathlib import Path

from app.config import Settings
from tests.helpers import env


class SettingsTests(unittest.TestCase):
    def test_defaults_are_loopback_only_and_final_bind(self):
        with tempfile.TemporaryDirectory() as raw:
            settings = Settings.from_env(env(Path(raw)))
        self.assertEqual(settings.bind_host, "0.0.0.0")
        self.assertEqual(settings.bind_port, 55221)
        self.assertEqual(settings.trusted_client_ips, frozenset({"127.0.0.1", "::1"}))

    def test_rejects_network_ranges_in_trusted_clients(self):
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaises(ValueError):
                Settings.from_env(env(Path(raw), TRUSTED_CLIENT_IPS="10.0.0.0/8"))

    def test_rejects_unallowlisted_or_insecure_upstream(self):
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaises(ValueError):
                Settings.from_env(env(Path(raw), CROSSREF_API_BASE="http://evil.invalid"))
            with self.assertRaises(ValueError):
                Settings.from_env(
                    env(
                        Path(raw),
                        CROSSREF_API_BASE="https://evil.invalid/works",
                    )
                )
            with self.assertRaises(ValueError):
                Settings.from_env(
                    env(
                        Path(raw),
                        CROSSREF_API_BASE="https://api.crossref.org:444/works",
                    )
                )


if __name__ == "__main__":
    unittest.main()
