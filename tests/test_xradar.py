import os
import unittest
from unittest.mock import patch

from xradar import (
    SDK_VERSION,
    Capabilities,
    ClientConfig,
    post_json,
    report_capabilities,
)


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def getcode(self):
        return 200


class XRadarHttpTests(unittest.TestCase):
    def test_collect_request_has_stable_user_agent(self):
        with patch("urllib.request.urlopen", return_value=_Response()) as urlopen:
            post_json("https://radar.example/collect", {"code": "test"}, 1)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_header("User-agent"), f"xradar/{SDK_VERSION}")
        self.assertEqual(f"xradar/{SDK_VERSION}", "xradar/0.2.2")

    def test_capabilities_request_has_same_user_agent(self):
        config = ClientConfig(code="test", endpoint="https://radar.example")
        caps = Capabilities.host_introspect("test-project")
        with patch.dict(os.environ, {"W_RADAR_API_KEY": "test-key"}, clear=True):
            with patch("urllib.request.urlopen", return_value=_Response()) as urlopen:
                self.assertTrue(report_capabilities(config, caps, 1))
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_header("User-agent"), f"xradar/{SDK_VERSION}")


if __name__ == "__main__":
    unittest.main()
