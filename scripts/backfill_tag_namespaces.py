#!/usr/bin/env python3
"""Backfill ancestor buckets for hierarchical tag fields.

New writes index `lib:uclibc:0.9.30.1:seekdir` into `lib`, `lib:uclibc` and
`lib:uclibc:0.9.30.1` as well. Collections indexed before that change only have
the leaf buckets, so `func_tag=lib` finds nothing until this has run.

Idempotent — re-running adds nothing new. Safe to run against a live instance:
it only ever adds members to ancestor buckets, so a concurrent search sees
either the old or the new set, never a partial one.

    python3 scripts/backfill_tag_namespaces.py --dry-run
    python3 scripts/backfill_tag_namespaces.py --collection main
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bsimvis.app.services.index_config import is_hierarchical, tag_ancestors
from bsimvis.app.services.redis_client import get_redis


def _dec(x):
    return x.decode() if isinstance(x, bytes) else str(x)


def registries(r, collection=None):
    """Every hierarchical-field registry key, optionally for one collection."""
    pattern = f"{collection}:reg:*" if collection else "*:reg:*"
    for key in r.scan_iter(match=pattern, count=1000):
        key = _dec(key)
        # {coll}:reg:{level}:{field} — field is the last segment
        field = key.rsplit(":", 1)[-1]
        if is_hierarchical(field):
            yield key, field


def backfill(r, collection=None, dry_run=False):
    added_buckets = 0
    scanned = 0

    for registry_key, field in registries(r, collection):
        prefix = registry_key.replace(":reg:", ":idx:", 1) + ":"
        new_registry_members = set()

        for bucket in r.sscan_iter(registry_key, count=1000):
            bucket = _dec(bucket)
            if not bucket.startswith(prefix):
                continue
            scanned += 1
            value = bucket[len(prefix) :]

            for ancestor in tag_ancestors(field, value):
                anc_key = prefix + ancestor
                if dry_run:
                    added_buckets += 1
                    continue
                # dest may be one of the sources: this unions the leaf's members
                # into the ancestor without disturbing what is already there.
                r.sunionstore(anc_key, anc_key, bucket)
                new_registry_members.add(anc_key)
                added_buckets += 1

        if new_registry_members and not dry_run:
            members = list(new_registry_members)
            for i in range(0, len(members), 1000):
                r.sadd(registry_key, *members[i : i + 1000])

        if new_registry_members or dry_run:
            print(
                f"{registry_key}: {len(new_registry_members)} ancestor buckets"
                f"{' (dry run)' if dry_run else ''}"
            )

    return scanned, added_buckets


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--collection", help="Limit to one collection or pool key")
    ap.add_argument(
        "--dry-run", action="store_true", help="Report what would be written"
    )
    args = ap.parse_args()

    r = get_redis()
    scanned, added = backfill(r, args.collection, args.dry_run)
    print(f"\nscanned {scanned} buckets, {added} ancestor writes")


if __name__ == "__main__":
    main()
