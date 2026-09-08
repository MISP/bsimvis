"""Backfill file-level library tags from already-indexed function tags.

Files indexed before the rollup existed carry no `lib:name:version` tag even
when their functions do. This walks a collection's files, re-derives the tags
from the function metadata already in Kvrocks and folds them into the file doc
via the same ProcessingService.rollup_lib_tags the pipeline uses.

Idempotent -- re-running only adds what is missing.

    uv run python scripts/backfill_lib_file_tags.py --collection main
    uv run python scripts/backfill_lib_file_tags.py --all --dry-run
"""

import argparse
import json
import logging

from tqdm import tqdm

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.processing_service import ProcessingService, lib_parents

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Function metas are fat (call graph, parameters). Read them a slice at a time
# so a 20k-function binary does not land in memory all at once.
META_BATCH = 500


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


def file_lib_tags(r, collection, md5):
    """Library tags implied by every indexed function of one file."""
    func_ids = [_s(f) for f in r.smembers(f"{collection}:idx:file:functions:{md5}")]
    derived = set()
    for start in range(0, len(func_ids), META_BATCH):
        pipe = r.pipeline(transaction=False)
        for fid in func_ids[start : start + META_BATCH]:
            pipe.get(fid if fid.endswith(":meta") else f"{fid}:meta")
        for raw in pipe.execute():
            if not raw:
                continue
            try:
                derived |= lib_parents(json.loads(_s(raw)).get("tags"))
            except (ValueError, AttributeError):
                continue
    return derived


def backfill(collection, dry_run=False):
    r = get_redis()
    svc = ProcessingService(r)

    md5s = sorted(_s(f).split(":")[-1] for f in r.smembers(f"{collection}:all_files"))
    if not md5s:
        logging.warning(f"[!] No files in collection {collection}")
        return 0

    tagged = 0
    for md5 in tqdm(md5s, desc=collection, unit="file"):
        derived = file_lib_tags(r, collection, md5)
        if not derived:
            continue
        if dry_run:
            doc = r.get(f"{collection}:file:{md5}:meta")
            have = set(json.loads(_s(doc)).get("tags") or []) if doc else set()
            missing = derived - have
            if missing:
                tagged += 1
                logging.info(f"    {md5}: would add {', '.join(sorted(missing))}")
            continue
        r.sadd(f"{collection}:file:{md5}:lib_tags", *derived)
        svc.rollup_lib_tags(collection, md5)
        tagged += 1

    verb = "would tag" if dry_run else "tagged"
    logging.info(f"[+] {collection}: {verb} {tagged}/{len(md5s)} files")
    return tagged


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
