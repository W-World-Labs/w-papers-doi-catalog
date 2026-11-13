from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from app.adapters.sqlite_cache import SqliteDoiCache
from app.config import Settings


def rotate_backups(directory: Path, retain: int) -> list[str]:
    backups = sorted(directory.glob("catalog-*.sqlite"), reverse=True)
    removed: list[str] = []
    for path in backups[retain:]:
        path.unlink()
        removed.append(path.name)
    return removed


def run() -> int:
    parser = argparse.ArgumentParser(description="Maintain the DOI catalog cache")
    parser.add_argument("--prune", action="store_true")
    parser.add_argument("--checkpoint", action="store_true")
    parser.add_argument("--backup", action="store_true")
    args = parser.parse_args()
    if not (args.prune or args.checkpoint or args.backup):
        args.prune = args.checkpoint = args.backup = True

    settings = Settings.from_env()
    cache = SqliteDoiCache(settings.cache_path)
    result: dict[str, object] = {"status": "ok"}
    try:
        if args.prune:
            result["pruned"] = cache.prune_expired()
        if args.checkpoint:
            cache.checkpoint()
            result["checkpointed"] = True
        if args.backup:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            destination = settings.backup_dir / f"catalog-{stamp}.sqlite"
            cache.backup(destination)
            result["backup"] = str(destination)
            result["removed_backups"] = rotate_backups(
                settings.backup_dir, settings.backup_retain
            )
    finally:
        cache.close()
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
