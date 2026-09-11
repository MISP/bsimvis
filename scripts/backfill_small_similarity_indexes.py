"""Restore search indexes for saved similarities below the feature floor.

Preview (default):
    uv run python scripts/backfill_small_similarity_indexes.py --collection main
Apply:
    uv run python scripts/backfill_small_similarity_indexes.py --collection main --apply

Uses KVROCKS_HOST / KVROCKS_PORT from the environment or .env. No scores are
recomputed or deleted. Re-running is safe. Use --below-features to match the
historical floor, or --pool for a pool namespace. SCAN may revisit an edge.
"""

import argparse
from itertools import islice

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.similarity_service import SimilarityService


def backfill(r, namespace, algo, below_features=10, apply=False, pool_id=None):
    prefix = f"{namespace}:sim:" if pool_id else f"{namespace}:sim:{algo}:"
    candidates = (
        sid
        for sid, count in r.zscan_iter(f"{namespace}:sim:min_features", count=200)
        if count < below_features and sid.startswith(prefix)
    )
    service = SimilarityService(r) if apply else None
    total = 0
    while batch := list(islice(candidates, 200)):
        if apply:
            service._func_meta_cache = {}
            service._file_meta_cache = {}
            service._sim_registry_seen = set()
            service._index_similarity_ids_batch(
                r, batch, namespace, pool_id, None, None
            )
        total += len(batch)
        print(f"{'Indexed' if apply else 'Would index'} {total} edges", flush=True)
    return total


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--collection")
    target.add_argument("--pool", help="Pool ID")
    parser.add_argument("--algo", default="unweighted_cosine")
    parser.add_argument("--below-features", type=int, default=10)
    parser.add_argument(
        "--apply", action="store_true", help="Write indexes (default: preview)"
    )
    args = parser.parse_args()
    if args.below_features <= 0:
        parser.error("--below-features must be positive")
    namespace = f"global:pool:{args.pool}" if args.pool else args.collection
    count = backfill(
        get_redis(), namespace, args.algo, args.below_features, args.apply, args.pool
    )
    print(
        f"Done: {count} candidate edges; {'applied' if args.apply else 'preview only'}."
    )


if __name__ == "__main__":
    main()
