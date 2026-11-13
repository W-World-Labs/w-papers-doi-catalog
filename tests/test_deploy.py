import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeployTests(unittest.TestCase):
    def test_service_uses_final_bind_without_proxy_headers(self):
        unit = (ROOT / "deploy/w-papers-doi-catalog.service").read_text()
        self.assertIn("--host ${BIND_HOST} --port ${BIND_PORT}", unit)
        self.assertIn("--no-proxy-headers", unit)
        self.assertIn("/etc/w/w-papers-doi-catalog.service.env", unit)
        example = (ROOT / ".env.example").read_text()
        self.assertIn("BIND_HOST=0.0.0.0", example)
        self.assertIn("BIND_PORT=55221", example)
        self.assertIn("TRUSTED_CLIENT_IPS=127.0.0.1,::1,198.51.100.20", example)

    def test_units_use_expected_persistent_path_and_timer(self):
        unit = (ROOT / "deploy/w-papers-doi-catalog.service").read_text()
        self.assertIn("ReadWritePaths=/home/w-data/w-papers-doi-catalog", unit)
        timer = (ROOT / "deploy/w-papers-doi-catalog-maintenance.timer").read_text()
        self.assertIn("Persistent=true", timer)

    def test_openalex_units_guard_split_mounts_and_keep_api_private_by_default(self):
        api = (ROOT / "deploy/w-papers-openalex.service").read_text()
        sync = (ROOT / "deploy/w-papers-openalex-sync.service").read_text()
        for unit in (api, sync):
            self.assertIn("User=w-openalex-catalog", unit)
            self.assertIn("RequiresMountsFor=/mnt/md0/openalex /mnt/main/w-papers-openalex", unit)
            self.assertIn("AssertPathIsMountPoint=/mnt/md0", unit)
            self.assertIn("AssertPathIsMountPoint=/mnt/main", unit)
            self.assertIn("/usr/bin/mountpoint -q /mnt/md0", unit)
            self.assertIn("/usr/bin/mountpoint -q /mnt/main", unit)
        self.assertIn("--host 127.0.0.1 --port 55222", api)
        self.assertIn("app.openalex_api:app", api)
        self.assertIn("python -m app.openalex_snapshot sync", sync)
        self.assertIn("ReadWritePaths=/mnt/md0/openalex /mnt/main/w-papers-openalex", sync)
        self.assertIn("TimeoutStartSec=infinity", sync)
        self.assertIn("Restart=on-failure", sync)

    def test_openalex_proxy_mode_mutation_stays_root_and_uses_scrubbed_environment(self):
        command = (ROOT / "deploy/w-openalex-download").read_text()
        self.assertIn("if [[ ${EUID} -ne 0 ]]", command)
        self.assertIn("run_catalog_root mode \"$1\"", command)
        self.assertIn("/usr/bin/env -i", command)
        self.assertIn("run_catalog mode status", command)

    def test_ip_tls_keeps_query_private_and_renews_short_certificate(self):
        bootstrap = (ROOT / "deploy/caddy-acme-bootstrap.Caddyfile").read_text()
        self.assertIn("http://198.51.100.10", bootstrap)
        self.assertIn("/.well-known/acme-challenge/*", bootstrap)
        caddy = (ROOT / "deploy/caddy-ip-tls.Caddyfile").read_text()
        self.assertIn("http://198.51.100.10", caddy)
        self.assertIn("https://198.51.100.10", caddy)
        self.assertIn("/.well-known/acme-challenge/*", caddy)
        self.assertGreaterEqual(caddy.count("/.well-known/acme-challenge/*"), 2)
        self.assertIn("@health path /health /api/health", caddy)
        self.assertIn("@production_client remote_ip 198.51.100.20", caddy)
        self.assertIn('respond "client IP is not allowed" 403', caddy)

        renew = (ROOT / "deploy/renew-ip-certificate.sh").read_text()
        self.assertIn("--cert-name 198.51.100.10", renew)
        self.assertIn('systemctl restart caddy', renew)
        verify = (ROOT / "deploy/verify-ip-certificate.sh").read_text()
        self.assertIn("-checkend 172800", verify)
        self.assertIn("https://198.51.100.10/health", verify)

        timer = (ROOT / "deploy/w-papers-doi-cert-renew.timer").read_text()
        self.assertIn("OnCalendar=*-*-* 00,12:17:00 UTC", timer)
        self.assertIn("Persistent=true", timer)
        service = (ROOT / "deploy/w-papers-doi-cert-renew.service").read_text()
        self.assertIn("ProtectHome=read-only", service)
        self.assertIn("/root/snap", service)
        self.assertIn("NoNewPrivileges=false", service)


if __name__ == "__main__":
    unittest.main()
