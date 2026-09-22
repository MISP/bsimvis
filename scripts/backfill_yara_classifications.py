#!/usr/bin/env python3
"""Backfill classification tags for already-indexed Rulezet YARA matches.

Only rewrites unknown YARA tags when the rule name has a recognised prefix:

    yara:unknown:unknown#SUSP_ELF_... -> yara:classification:suspicious#SUSP_ELF_...
    yara:anomally#ANOM_ELF_...        -> yara:classification:anomaly#ANOM_ELF_...

The storage and index rebuild is delegated to migrate_tag_taxonomy. It rewrites
file/function metadata and their indexes, but never edits data/rulezet or
contacts Rulezet.

Preview:
    uv run python scripts/backfill_yara_classifications.py --collection main --dry-run
Apply:
    uv run python scripts/backfill_yara_classifications.py --collection main
"""

import argparse

from bsimvis.app.services import tag_taxonomy
import migrate_tag_taxonomy as storage

OLD_UNKNOWN = "yara:unknown:unknown"
OLD_ANOMALY = {"yara:anomally", "yara:anomaly"}


def rewrite_tag(tag):
    """Rewrite one old YARA tag, preserving every unrelated tag unchanged."""
    raw = str(tag)
    body, detail = tag_taxonomy.tag_body(raw)
    if not detail or (body.lower() != OLD_UNKNOWN and body.lower() not in OLD_ANOMALY):
        return [raw]
    replacement = tag_taxonomy.yara_classification_tag(detail)
    return [replacement] if replacement else [raw]


def _collections():
    r = storage.get_redis()
    return sorted(
        c.decode() if isinstance(c, bytes) else str(c)
        for c in r.smembers("global:collections")
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--collection", action="append", help="repeatable")
    ap.add_argument("--all", action="store_true", help="every known collection")
    ap.add_argument("--dry-run", action="store_true", help="report, write nothing")
    ap.add_argument("--demo", action="store_true", help="run rewrite checks and exit")
    args = ap.parse_args()

    if args.demo:
        assert rewrite_tag("yara:unknown:unknown#SUSP_ELF_UPX") == [
            "yara:classification:suspicious#SUSP_ELF_UPX"
        ]
        assert rewrite_tag("yara:anomally#ANOM_ELF") == [
            "yara:classification:anomaly#ANOM_ELF"
        ]
        assert rewrite_tag("yara:unknown:unknown#ordinary_rule") == [
            "yara:unknown:unknown#ordinary_rule"
        ]
        assert rewrite_tag("runtime-packer:pe:upx") == ["runtime-packer:pe:upx"]
        print("backfill_yara_classifications demo OK")
        return

    if not args.all and not args.collection:
        ap.error("pass --collection NAME (repeatable) or --all")

    storage._map_tag = rewrite_tag
    collections = _collections() if args.all else args.collection
    for collection in collections:
        storage.migrate(collection, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
