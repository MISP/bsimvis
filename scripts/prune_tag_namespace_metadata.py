#!/usr/bin/env python3
"""Remove non-vocabulary namespace rows from a collection tag metadata hash.

Index buckets are untouched; rerun this pass whenever policy changes.
"""

import argparse

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.tag_service import prune_non_vocabulary_tags


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    r = get_redis()
    if args.dry_run:
        raw = r.hgetall(f"{args.collection}:tags_metadata") or {}
        from bsimvis.app.services.tag_taxonomy import tag_policy

        stale = [
            (k.decode() if isinstance(k, bytes) else str(k))
            for k in raw
            if not tag_policy(
                k.decode() if isinstance(k, bytes) else str(k), "user"
            ).vocabulary
        ]
        print(f"would remove {len(stale)} metadata rows")
    else:
        print(f"removed {prune_non_vocabulary_tags(r, args.collection)} metadata rows")


if __name__ == "__main__":
    main()
