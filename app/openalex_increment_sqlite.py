"""Reject candidate updates until an increment can prove snapshot parity."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.openalex_snapshot import SnapshotError


def apply(root: Path, candidate: Path, reserve: int) -> dict:
    raise SnapshotError(
        'candidate application is disabled: updated_date increments cannot '
        'guarantee full-snapshot field parity'
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--reserve-bytes', type=int, default=512 * 1024**3)
    args = parser.parse_args()
    print(json.dumps(apply(args.root, args.candidate, args.reserve_bytes)))


if __name__ == '__main__':
    main()
