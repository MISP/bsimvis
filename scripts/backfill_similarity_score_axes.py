"""Backfill Code/Library score indexes for function similarity pairs.

Preview: uv run python scripts/backfill_similarity_score_axes.py --collection main
Apply:   uv run python scripts/backfill_similarity_score_axes.py --collection main --apply

Pause similarity builds, clears, and library-tag changes for the namespace during --apply.
The old query path remains active until the final readiness marker is written.
"""

import argparse
import json
from itertools import islice

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.similarity_axis_index import (
    pair_axis,
    ready_key,
    score_axis_key,
)
from bsimvis.app.services.pool_service import pool_service


def _text(value):
    return value.decode() if isinstance(value, bytes) else value


def backfill(r, namespace, algo=None, pool=False, apply=False):
    source = f"{namespace}:sim:score" if pool else f"{namespace}:sim:score:{algo}"
    code_key = score_axis_key(namespace, "code", None if pool else algo)
    library_key = score_axis_key(namespace, "library", None if pool else algo)
    marker = ready_key(namespace, None if pool else algo)

    if apply:
        r.delete(marker, code_key, library_key)

    rows = r.zscan_iter(source, count=500)
    total = code_count = library_count = 0
    while batch := list(islice(rows, 200)):
        sids = [_text(sid) for sid, _ in batch]
        docs_raw = r.mget(sids)
        docs = []
        fids = set()
        for sid, raw in zip(sids, docs_raw):
            if not raw:
                continue
            doc = json.loads(raw) if not isinstance(raw, dict) else raw
            docs.append((sid, doc))
            fids.update((doc.get("id1"), doc.get("id2")))
        fids.discard(None)
        fids = list(fids)
        meta_raw = r.mget([f"{fid}:meta" for fid in fids]) if fids else []
        metas = {
            fid: (json.loads(raw) if raw else {}) for fid, raw in zip(fids, meta_raw)
        }
        pipe = r.pipeline(transaction=False)
        score_by_sid = {_text(sid): float(score) for sid, score in batch}
        for sid, doc in docs:
            axis = pair_axis(metas.get(doc.get("id1")), metas.get(doc.get("id2")))
            score = score_by_sid[sid]
            if axis == "library":
                library_count += 1
                pipe.zadd(library_key, {sid: score})
            else:
                code_count += 1
                pipe.zadd(code_key, {sid: score})
        if apply:
            pipe.execute()
        total += len(docs)
        if total % 100_000 == 0:
            print(f"{'Indexed' if apply else 'Would index'} {total} pairs", flush=True)

    if apply:
        base_count = r.zcard(source)
        indexed_count = r.zcard(code_key) + r.zcard(library_key)
        if indexed_count != base_count:
            raise RuntimeError(
                f"Index count mismatch: {indexed_count} axis entries, {base_count} source pairs"
            )
        r.set(marker, "1")
    return total, code_count, library_count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--collection")
    target.add_argument("--pool", help="Pool ID")
    parser.add_argument("--algo", help="Collection similarity algorithm")
    parser.add_argument("--apply", action="store_true", help="Write indexes")
    args = parser.parse_args()

    r = get_redis()
    if args.pool:
        pool = pool_service.get_pool(args.pool)
        if not pool:
            parser.error(f"Pool {args.pool!r} not found")
        namespace, algo, is_pool = f"global:pool:{args.pool}", None, True
    else:
        if not args.algo:
            parser.error("--algo is required with --collection")
        namespace, algo, is_pool = args.collection, args.algo, False

    total, code, library = backfill(r, namespace, algo, is_pool, args.apply)
    print(
        f"Done: {total} pairs ({code} Code, {library} Library); "
        f"{'applied' if args.apply else 'preview only'}."
    )


if __name__ == "__main__":
    main()
