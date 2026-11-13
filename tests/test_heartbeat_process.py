import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ROOT = Path(__file__).resolve().parents[1]


class HeartbeatProcessTests(unittest.TestCase):
    def test_bound_process_recovers_and_stops_and_failed_bind_reports_nothing(self):
        reports = []

        class Receiver(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path.endswith("/capabilities"):
                    reports.append((body, self.headers["User-Agent"]))
                self.send_response(503 if len(reports) == 1 else 200)
                self.end_headers()

            def log_message(self, *_):
                pass

        with tempfile.TemporaryDirectory() as directory:
            receiver = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
            thread = threading.Thread(target=receiver.serve_forever, daemon=True)
            thread.start()
            with socket.socket() as available:
                available.bind(("127.0.0.1", 0))
                port = available.getsockname()[1]
            env = dict(os.environ)
            env.update(W_RADAR_CODE="test", W_RADAR_PROJECT_ID="10-15-w-papers-doi-catalog",
                       W_RADAR_ENDPOINT=f"http://127.0.0.1:{receiver.server_port}",
                       W_RADAR_INSTANCE_ID="doi-process-test", DOI_CACHE_PATH=f"{directory}/cache.sqlite")
            env.pop("W_RADAR_API_KEY", None)
            env.pop("XRADAR_DISABLE_CAPABILITIES", None)
            command = [sys.executable, "-c",
                       "from app.server import Server, main; Server.heartbeat_interval=0.2; main()",
                       "--host", "127.0.0.1", "--port", str(port), "--no-proxy-headers"]
            process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 10
                while len(reports) < 3 and time.monotonic() < deadline:
                    self.assertIsNone(process.poll())
                    time.sleep(0.05)
                self.assertGreaterEqual(len(reports), 3)
                for body, agent in reports:
                    self.assertEqual(body["process"]["pid"], process.pid)
                    self.assertEqual(body["instance_id"], "doi-process-test")
                    self.assertEqual(body["sdk_version"], "0.2.2")
                    self.assertEqual(agent, "xradar/0.2.2")
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as response:
                    self.assertEqual(json.load(response)["status"], "ok")
                process.terminate()
                self.assertIn(process.wait(timeout=8), (0, -15))
                count = len(reports)
                time.sleep(0.4)
                self.assertEqual(len(reports), count)
                with socket.socket() as occupied:
                    occupied.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    occupied.bind(("127.0.0.1", port))
                    occupied.listen()
                    failed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, timeout=8)
                self.assertNotEqual(failed.returncode, 0)
                self.assertEqual(len(reports), count)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                receiver.shutdown()
                receiver.server_close()
                thread.join()


if __name__ == "__main__":
    unittest.main()
