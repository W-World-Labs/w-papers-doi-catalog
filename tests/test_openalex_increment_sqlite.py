import tempfile
import unittest
from pathlib import Path

from app.openalex_increment_sqlite import apply
from app.openalex_snapshot import SnapshotError


class SqliteIncrementTests(unittest.TestCase):
    def test_candidate_application_fails_closed_without_touching_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'increment'
            candidate = Path(tmp) / 'candidate.sqlite'
            with self.assertRaisesRegex(SnapshotError, 'cannot guarantee full-snapshot field parity'):
                apply(root, candidate, 0)
            self.assertFalse(root.exists())
            self.assertFalse(candidate.exists())
