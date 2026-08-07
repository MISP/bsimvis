"""Backfill containment lineage from the scalar parent fields already indexed.

Files ingested before lineage edges existed carry `parent_md5` /
`parent_file_name` on their document but no edge, so nothing can answer "what
came out of this container". This rebuilds the edges from those fields, and
rolls the function counts up into the containers.

Containers ingested before they had documents of their own are the awkward
case: their children point at an md5 with nothing behind it. Their name and
format are recoverable from the children (`parent_file_name`, the `container:*`
tag), so a minimal identity document is reconstructed for them -- enough for
the lineage links to resolve. Pass --no-create-missing to leave them dangling.

Idempotent -- edges are a set, counts are stored per contributing descendant.

    uv run python scripts/backfill_lineage.py --collection main --dry-run
    uv run python scripts/backfill_lineage.py --all
"""

import argparse
import json
import logging
import time

from tqdm import tqdm

from bsimvis.app.services import lineage_service
from bsimvis.app.services.processing_service import ProcessingService
from bsimvis.app.services.redis_client import get_redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


def _container_tag(tags):
    """The container:* / packer:* tag a child inherited from its parent."""
    for t in tags or []:
        if str(t).startswith(("container:", "packer:")):
            return str(t)
    return ""


def backfill(collection, dry_run=False, create_missing=True):
    r = get_redis()
    md5s = sorted(_s(f).split(":")[-1] for f in r.smembers(f"{collection}:all_files"))
    if not md5s:
        logging.warning(f"[!] No files in collection {collection}")
        return 0

    edges = 0
    all_parents = set()
    # parent md5 -> what its children could tell us about it
    orphan_parents = {}

    for md5 in tqdm(md5s, desc=f"{collection}: edges", unit="file"):
        raw = r.get(f"{collection}:file:{md5}:meta")
        if not raw:
            continue
        try:
            meta = json.loads(_s(raw))
        except (ValueError, TypeError):
            logging.warning(f"[-] {md5}: unreadable meta, skipped")
            continue

        parent_md5 = meta.get("parent_md5")
        if not parent_md5 or parent_md5 == md5:
            continue

        path = meta.get("file_name") or ""
        edges += 1
        all_parents.add(parent_md5)
        if not dry_run:
            lineage_service.record(collection, parent_md5, md5, path, r)

        if not r.exists(f"{collection}:file:{parent_md5}:meta"):
            known = orphan_parents.setdefault(
                parent_md5,
                {
                    "file_name": meta.get("parent_file_name") or parent_md5,
                    "tag": "",
                    "batch_uuid": meta.get("batch_uuid"),
                    "batch_name": meta.get("batch_name"),
                    "children": 0,
                },
            )
            known["children"] += 1
            known["tag"] = known["tag"] or _container_tag(meta.get("tags"))

    # A parent that does have a document is a container only if it holds no
    # code of its own; a UPX-packed executable is a real binary and must not be
    # marked. Zero indexed functions is exactly that distinction.
    marked = 0
    for parent_md5 in sorted(all_parents - set(orphan_parents)):
        if r.scard(f"{collection}:idx:file:functions:{parent_md5}"):
            continue
        marked += 1
        if not dry_run:
            lineage_service.mark_container(collection, parent_md5, r)

    created = 0
    for parent_md5, known in sorted(orphan_parents.items()):
        if not create_missing:
            logging.info(
                f"    {parent_md5}: {known['children']} children, no document (left dangling)"
            )
            continue
        created += 1
        if dry_run:
            logging.info(
                f"    {parent_md5}: would create container doc "
                f"'{known['file_name']}' ({known['children']} children)"
            )
            continue
        now_unix = int(time.time() * 1000)
        ProcessingService(r).index_metadata(
            collection,
            None,
            file_meta={
                "entry_date": now_unix,
                "file_date": now_unix,
                "file_md5": parent_md5,
                "file_name": known["file_name"],
                "batch_uuid": known["batch_uuid"] or "backfilled",
                "batch_name": known["batch_name"] or "Lineage backfill",
                "tags": [t for t in [known["tag"]] if t],
                "filetype": (known["tag"].split(":")[-1] if known["tag"] else ""),
                "is_container": True,
                "recovered": True,
                "language_id": "",
            },
            num_functions=0,
            total_features=0,
        )
        lineage_service.mark_container(collection, parent_md5, r)

    # Counts last: a container only restates once it is marked and has a doc.
    counted = 0
    if not dry_run:
        for md5 in tqdm(md5s, desc=f"{collection}: counts", unit="file"):
            n = r.scard(f"{collection}:idx:file:functions:{md5}")
            if n:
                counted += 1
                lineage_service.record_function_count(collection, md5, n, r)

    verb = "would record" if dry_run else "recorded"
    logging.info(
        f"[+] {collection}: {verb} {edges} edges, marked {marked} containers, "
        f"{'would create' if dry_run else 'created'} {created} missing container docs, "
        f"rolled up {counted} files"
    )
    return edges


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--collection", action="append", help="repeatable")
    ap.add_argument("--all", action="store_true", help="every known collection")
    ap.add_argument("--dry-run", action="store_true", help="report, write nothing")
    ap.add_argument(
        "--no-create-missing",
        action="store_true",
        help="do not reconstruct documents for containers that have none",
    )
    args = ap.parse_args()

    if args.all:
        collections = sorted(_s(c) for c in get_redis().smembers("global:collections"))
    elif args.collection:
        collections = args.collection
    else:
        ap.error("pass --collection NAME (repeatable) or --all")

    for c in collections:
        backfill(
            c, dry_run=args.dry_run, create_missing=not args.no_create_missing
        )


if __name__ == "__main__":
    main()
