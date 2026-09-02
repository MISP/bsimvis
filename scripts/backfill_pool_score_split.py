"""Backfill Code/Library scores onto pool bin_sim pairs built before the split
existed.

Pools built by the old `build_pool_bin_sim` stored only `score`: the pool
builder was a second copy of the collection builder and never got
`code_library_split`, so `sort=score_code` on a pool answered 0 results while
the same query on a collection answered thousands. Both builders now share
`bin_sim_tags.score_pair`, but pairs already on disk keep the doc they were
written with.

No rebuild needed -- the split is derived from the diff each doc already
carries, which is what `resplit_bin_sim` does for collections. Its keys are
prefix-relative, so it runs on `global:pool:{id}` unchanged; this just points
it at pools and reports what moved.

Idempotent -- re-running rewrites the same values.

    uv run python scripts/backfill_pool_score_split.py --pool <uuid>
    uv run python scripts/backfill_pool_score_split.py --all --dry-run
"""

import argparse
import json
import logging

from bsimvis.app.services.bin_sim_service import bin_sim_service
from bsimvis.app.services.pool_service import pool_service
from bsimvis.app.services.redis_client import get_redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


def pair_state(r, pool_id, algo):
    """(total pairs, pairs already carrying a code score) for one pool."""
    sids = [_s(s) for s in r.smembers(f"global:pool:{pool_id}:bin_sim:built:{algo}")]
    if not sids:
        return 0, 0
    with_code = 0
    for start in range(0, len(sids), 500):
        pipe = r.pipeline(transaction=False)
        for sid in sids[start : start + 500]:
            pipe.get(sid)
        for raw in pipe.execute():
            if not raw:
                continue
            doc = json.loads(_s(raw))
            if isinstance(doc, str):
                doc = json.loads(doc)
            if isinstance(doc, dict) and doc.get("score_code") is not None:
                with_code += 1
    return len(sids), with_code


def backfill(pool_id, algo, dry_run=False):
    r = get_redis()
    total, before = pair_state(r, pool_id, algo)
    if not total:
        logging.info("pool %s: no bin_sim pairs for algo %s, skipping", pool_id, algo)
        return
    if dry_run:
        logging.info(
            "pool %s: %d pairs, %d already split, %d would be backfilled",
            pool_id,
            total,
            before,
            total - before,
        )
        return

    logging.info("pool %s: resplitting %d pairs...", pool_id, total)
    bin_sim_service.resplit_bin_sim(f"global:pool:{pool_id}", algo=algo)

    _, after = pair_state(r, pool_id, algo)
    # The sort ZSET is what the UI's Code pill counts, so check the index too
    # and not just the docs -- a doc-only write would still show 0 in the view.
    indexed = r.zcard(f"global:pool:{pool_id}:idx:bin_sim:score_code")
    logging.info(
        "pool %s: %d/%d pairs split (was %d), %d in the score_code sort index",
        pool_id,
        after,
        total,
        before,
        indexed,
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pool", help="pool id to backfill")
    g.add_argument("--all", action="store_true", help="every pool")
    ap.add_argument("--algo", default="unweighted_cosine")
    ap.add_argument(
        "--dry-run", action="store_true", help="report what is missing, write nothing"
    )
    args = ap.parse_args()

    pool_ids = (
        [p["id"] for p in pool_service.list_pools()] if args.all else [args.pool]
    )
    for pool_id in pool_ids:
        backfill(pool_id, args.algo, args.dry_run)


if __name__ == "__main__":
    main()
