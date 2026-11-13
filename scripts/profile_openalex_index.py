"""Profile indexing of an explicitly supplied local works shard."""
import argparse
import cProfile
import gzip
import pstats
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.openalex_catalog import connect, initialize
from app.openalex_snapshot import _index_works


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard", type=Path, required=True)
    parser.add_argument("--dataset-version", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="openalex-profile-") as directory:
        tmp = Path(directory) / "profile.sqlite"
        initialize(tmp)
        db = connect(tmp)
        try:
            expected = sum(1 for line in gzip.open(args.shard, "rt") if line.strip())
            print("records in shard:", expected, flush=True)
            start = time.time()
            prof = cProfile.Profile()
            prof.enable()
            count = _index_works(args.shard, db, args.dataset_version, expected, tmp, 0)
            prof.disable()
            print(f"indexed {count} in {time.time() - start:.1f}s")
            pstats.Stats(prof).sort_stats("tottime").print_stats(18)
        finally:
            db.close()


if __name__ == "__main__":
    main()
