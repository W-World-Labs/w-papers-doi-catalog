import tempfile
import unittest
from pathlib import Path

from app.adapters.sqlite_cache import SqliteDoiCache
from tests.helpers import work


class SqliteCacheTests(unittest.TestCase):
    def test_wal_positive_negative_prune_and_backup(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cache = SqliteDoiCache(root / "catalog.sqlite")
            cache.put_found(work(), 200)
            cache.put_missing("10.1234/missing", 50)
            self.assertEqual(cache.get("10.1234/example").work.title, "Example")
            self.assertEqual(cache.get("10.1234/missing").status, "missing")
            self.assertEqual(cache.prune_expired(now=100), 1)
            backup = root / "backups" / "catalog.sqlite"
            cache.backup(backup)
            self.assertTrue(backup.is_file())
            mode = cache._conn().execute("PRAGMA journal_mode").fetchone()[0]
            self.assertEqual(mode.lower(), "wal")
            cache.close()
            restored = SqliteDoiCache(backup)
            self.assertIsNotNone(restored.get("10.1234/example"))
            restored.close()

    def test_health_reports_open_failure_in_body(self):
        with tempfile.TemporaryDirectory() as raw:
            parent_file = Path(raw) / "file"
            parent_file.write_text("not a directory")
            health = SqliteDoiCache(parent_file / "catalog.sqlite").health()
            self.assertFalse(health["reachable"])
            self.assertEqual(health["error"], "FileExistsError")


if __name__ == "__main__":
    unittest.main()
