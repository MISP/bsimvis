"""Backfill per-function caller_count / callee_count onto already-indexed functions.

The call graph itself has always been stored -- as the `{fid}:callers` /
`{fid}:callees` SETs -- but the degree was never kept as a field, so functions
indexed before caller_count/callee_count existed cannot be sorted or filtered
by it and show blank columns.

This re-derives both counts with SCARD, writes them into the function meta blob
and re-runs save_function so the numeric ZSETs (`{coll}:idx:func:caller_count`,
`:callee_count`) get populated. Pure Kvrocks pass: no Ghidra, no re-ingest.

Idempotent -- re-running overwrites the same values, and ZADD replaces.

    uv run python scripts/backfill_call_counts.py --collection main
    uv run python scripts/backfill_call_counts.py --all --dry-run
"""

import argparse
import json
import logging

from tqdm import tqdm

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import save_function

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Function metas are fat (call graph, parameters), so stream them in slices
# rather than pulling a whole corpus into memory.
BATCH = 500


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


def _counts(r, func_ids):
    """(caller_count, callee_count) per function id, in the order given."""
    pipe = r.pipeline(transaction=False)
    for base in func_ids:
        pipe.scard(f"{base}:callers")
        pipe.scard(f"{base}:callees")
    flat = pipe.execute()
    return [(flat[i], flat[i + 1]) for i in range(0, len(flat), 2)]


def backfill(collection, dry_run=False):
    r = get_redis()

    # all_functions holds base keys; tolerate a stored ":meta" suffix either way.
    bases = sorted(
        _s(f)[: -len(":meta")] if _s(f).endswith(":meta") else _s(f)
        for f in r.smembers(f"{collection}:all_functions")
    )
    if not bases:
        logging.warning(f"[!] No functions in collection {collection}")
        return 0

    updated = 0
    for start in tqdm(range(0, len(bases), BATCH), desc=collection, unit="batch"):
        chunk = bases[start : start + BATCH]

        meta_pipe = r.pipeline(transaction=False)
        for base in chunk:
            meta_pipe.get(f"{base}:meta")
        metas = meta_pipe.execute()

        counts = _counts(r, chunk)

        write_pipe = r.pipeline(transaction=False)
        pending = 0
        for base, raw, (n_callers, n_callees) in zip(chunk, metas, counts):
            if not raw:
                continue
            try:
                meta = json.loads(_s(raw))
            except ValueError:
                logging.warning(f"    {base}: unreadable meta, skipped")
                continue

            if (
                meta.get("caller_count") == n_callers
                and meta.get("callee_count") == n_callees
            ):
                continue

            if dry_run:
                updated += 1
                continue

            meta["caller_count"] = n_callers
            meta["callee_count"] = n_callees

            # base is "{coll}:func:{md5}:{addr}" -- save_function wants the tail parts.
            parts = base.split(":")
            addr, md5 = parts[-1], parts[-2]

            write_pipe.set(f"{base}:meta", json.dumps(meta))
            save_function(write_pipe, collection, md5, addr, meta)
            updated += 1
            pending += 1

        if pending:
            write_pipe.execute()

    verb = "would update" if dry_run else "updated"
    logging.info(f"[+] {collection}: {verb} {updated}/{len(bases)} functions")
    return updated


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--collection", action="append", help="repeatable")
    ap.add_argument("--all", action="store_true", help="every known collection")
    ap.add_argument("--dry-run", action="store_true", help="report, write nothing")
    args = ap.parse_args()

    if args.all:
        collections = sorted(_s(c) for c in get_redis().smembers("global:collections"))
    elif args.collection:
        collections = args.collection
    else:
        ap.error("pass --collection NAME (repeatable) or --all")

    for c in collections:
        backfill(c, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
