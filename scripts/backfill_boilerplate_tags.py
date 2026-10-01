"""Retag stored functions with the current boilerplate name rules.

The rules run at analysis time, so collections analysed before a rule change
carry the old `boilerplate:*` tags. Function names are already stored, so this
replays `boilerplate_tags_for_names` per file over Kvrocks -- no Ghidra -- and
rewrites only the functions whose boilerplate tag changed: function meta, the
func tag index and the file-level library rollup.

Idempotent -- a second run changes nothing.

    uv run python scripts/backfill_boilerplate_tags.py --collection main --dry-run
    uv run python scripts/backfill_boilerplate_tags.py --collection main
    uv run python scripts/backfill_boilerplate_tags.py --all

Limits:
- Sim-level `func_tags` and cluster `tag_distribution` are not refreshed; a
  similarity/cluster rebuild does that. Function search, the file view and the
  boilerplate axis read the func tag index and are right immediately.
- A file `boilerplate:runtime` tag is not removed when its only runtime
  function lost the tag (the rollup is additive).
- Read-modify-write: run with no upload or analysis job active for the
  collection.
"""

import argparse
import json
import logging
from collections import Counter

from tqdm import tqdm

from bsimvis.app.services.bin_sim_tags import bump_tags_rev
from bsimvis.app.services.boilerplate_tag_service import boilerplate_tags_for_names
from bsimvis.app.services.index_service import _index_tag, _unindex_tag
from bsimvis.app.services.processing_service import ProcessingService, lib_parents
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.tag_taxonomy import filter_tags, tag_namespace

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Function metas are fat; read them a slice at a time.
META_BATCH = 500
EXAMPLES = 8


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


def _family(tag):
    return tag.split("#", 1)[0]


def _metas(r, fids):
    pipe = r.pipeline(transaction=False)
    for fid in fids:
        pipe.get(f"{fid}:meta")
    for raw in pipe.execute():
        try:
            yield json.loads(_s(raw)) if raw else None
        except ValueError:
            yield None


def backfill_file(r, coll, md5, dry_run, stats):
    fids = sorted(_s(f) for f in r.smembers(f"{coll}:idx:file:functions:{md5}"))
    stats["files"] += 1

    # Pass 1: names only, the gates need the whole file.
    names = {}
    for start in range(0, len(fids), META_BATCH):
        chunk = fids[start : start + META_BATCH]
        for fid, m in zip(chunk, _metas(r, chunk)):
            if m and m.get("function_name") is not None:
                names[fid] = m["function_name"]
    if not names:
        return

    want_by_name = boilerplate_tags_for_names(set(names.values()))

    # Pass 2: diff and write.
    lib_new = set()
    touched = False
    ordered = list(names)
    for start in range(0, len(ordered), META_BATCH):
        chunk = ordered[start : start + META_BATCH]
        pipe = r.pipeline(transaction=False)
        for fid, m in zip(chunk, _metas(r, chunk)):
            if not m:
                continue
            tags = list(m.get("tags") or [])
            old = [t for t in tags if tag_namespace(t) == "boilerplate"]
            want = want_by_name.get(names[fid])
            if old == ([want] if want else []):
                continue
            new_tags = filter_tags(
                [t for t in tags if t not in old] + ([want] if want else []),
                "analysis",
            )
            for t in old:
                stats["removed"] += 1
                stats["removed_fam"][_family(t)] += 1
            if want:
                stats["added"] += 1
                stats["added_fam"][_family(want)] += 1
                if len(stats["examples"]) < EXAMPLES:
                    stats["examples"].append(f"{names[fid]} -> {want}")
            touched = True
            lib_new |= lib_parents(new_tags)
            if dry_run:
                continue
            m["tags"] = new_tags
            pipe.set(f"{fid}:meta", json.dumps(m))
            if old:
                _unindex_tag(pipe, coll, "func", "tags", old, fid, remaining=new_tags)
            if want:
                _index_tag(pipe, coll, "func", "tags", [want], fid)
        if not dry_run:
            pipe.execute()

    if touched:
        stats["files_touched"] += 1
        if lib_new and not dry_run:
            r.sadd(f"{coll}:file:{md5}:lib_tags", *lib_new)
            ProcessingService(r).rollup_lib_tags(coll, md5)


def backfill(collection, dry_run=False):
    r = get_redis()
    md5s = sorted(_s(f).split(":")[-1] for f in r.smembers(f"{collection}:all_files"))
    if not md5s:
        logging.warning(f"[!] No files in collection {collection}")
        return None

    stats = {
        "files": 0,
        "files_touched": 0,
        "added": 0,
        "removed": 0,
        "added_fam": Counter(),
        "removed_fam": Counter(),
        "examples": [],
    }
    for md5 in tqdm(md5s, desc=collection, unit="file"):
        backfill_file(r, collection, md5, dry_run, stats)

    if stats["files_touched"] and not dry_run:
        bump_tags_rev(r, collection)
        for p in r.smembers(f"{collection}:pools"):
            bump_tags_rev(r, f"global:pool:{_s(p)}")

    verb = "would change" if dry_run else "changed"
    logging.info(
        f"[+] {collection}: {verb} {stats['files_touched']}/{stats['files']} files, "
        f"+{stats['added']} / -{stats['removed']} function tags"
    )
    for label, fam in (("added", "added_fam"), ("removed", "removed_fam")):
        for name, n in stats[fam].most_common():
            logging.info(f"    {label:8} {n:7}  {name}")
    if dry_run:
        for e in stats["examples"]:
            logging.info(f"    e.g. {e}")
    return stats


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
