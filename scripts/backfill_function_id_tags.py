"""Refresh FunctionID and boilerplate tags without re-analyzing binaries.

Preview: ``uv run python scripts/backfill_function_id_tags.py --collection main``
Apply: ``uv run python scripts/backfill_function_id_tags.py --collection main --apply``
"""

import argparse
import json
import logging
import re
from itertools import islice

from bsimvis.app.services.bin_sim_tags import bump_tags_rev
from bsimvis.app.services.boilerplate_tag_service import (
    boilerplate_tag_for_function_name,
)
from bsimvis.app.services.ghidra_service import GhidraService, fid_tags_from_records
from bsimvis.app.services.index_service import _unindex_tag, save_file, save_function
from bsimvis.app.services.processing_service import lib_parents
from bsimvis.app.services.redis_client import get_redis

BATCH = 500
HASH = re.compile(r"^[0-9a-f]{16}$")
GENERATED = ("fid:", "boilerplate:")


def metas(r, ids):
    pipe = r.pipeline(transaction=False)
    for fid in ids:
        pipe.get(f"{fid}:meta")
    for fid, raw in zip(ids, pipe.execute()):
        if raw:
            try:
                yield fid, json.loads(raw)
            except (TypeError, ValueError):
                logging.warning("Skipping unreadable metadata: %s", fid)


def fid_long(value):
    value = int(value, 16)
    return value - (1 << 64) if value >= (1 << 63) else value


class FidLookup:
    """One JVM and FID service per language; no binary Program is opened."""

    def __init__(self):
        GhidraService().ensure_launcher()
        from ghidra.feature.fid.service import FidService
        from ghidra.program.model.lang import LanguageID
        from ghidra.program.util import DefaultLanguageService

        self.ids = LanguageID
        self.languages = DefaultLanguageService.getLanguageService()
        self.service, self.queries, self.cache = FidService(), {}, {}

    def __call__(self, language_id, hash_value):
        key = language_id, hash_value
        if key not in self.cache:
            query = self.queries.get(language_id)
            if query is None:
                query = self.service.openFidQueryService(
                    self.languages.getLanguage(self.ids(language_id)), False
                )
                self.queries[language_id] = query
            self.cache[key] = fid_tags_from_records(
                query, query.findFunctionsByFullHash(fid_long(hash_value))
            )
        return self.cache[key]

    def close(self):
        for query in self.queries.values():
            try:
                query.close()
            except Exception:
                pass


def file_fid_tags(r, collection, md5, changes):
    tags = set()
    ids = r.sscan_iter(f"{collection}:idx:file:functions:{md5}", count=BATCH)
    while batch := list(islice(ids, BATCH)):
        for fid, meta in metas(r, batch):
            tags.update(
                t
                for t in lib_parents(changes.get(fid, meta.get("tags")))
                if t.startswith("fid:")
            )
    return tags


def sync_file(r, collection, md5, changes, apply):
    key = f"{collection}:file:{md5}:meta"
    raw = r.get(key)
    if not raw:
        return False
    meta = json.loads(raw)
    old = list(meta.get("tags") or [])
    fid_tags = file_fid_tags(r, collection, md5, changes)
    new = [tag for tag in old if not str(tag).startswith("fid:")] + sorted(fid_tags)
    if new == old:
        return False
    if apply:
        meta["tags"] = new
        pipe = r.pipeline(transaction=False)
        _unindex_tag(pipe, collection, "file", "tags", old, f"{collection}:file:{md5}")
        pipe.set(key, json.dumps(meta))
        save_file(pipe, collection, md5, meta)
        acc = f"{collection}:file:{md5}:lib_tags"
        stale = [tag for tag in r.smembers(acc) if str(tag).startswith("fid:")]
        if stale:
            pipe.srem(acc, *stale)
        if fid_tags:
            pipe.sadd(acc, *fid_tags)
        pipe.execute()
    return True


def backfill(r, collection, lookup, apply=False):
    """Reconcile generated tags; lookup(language_id, hash) returns FID tags."""
    changes, md5s, files = {}, set(), 0
    ids = r.sscan_iter(f"{collection}:all_functions", count=BATCH)
    while batch := list(islice(ids, BATCH)):
        for fid, meta in metas(r, batch):
            old = list(meta.get("tags") or [])
            value = str(meta.get("function_id_hash") or "").lower()
            found = []
            if (
                HASH.fullmatch(value)
                and int(meta.get("instruction_count") or 0) >= 10
                and meta.get("language_id")
            ):
                try:
                    found = lookup(meta["language_id"], value)
                except Exception as exc:
                    logging.warning(
                        "Skipping %s after FID lookup failure: %s", fid, exc
                    )
                    continue
            boilerplate = boilerplate_tag_for_function_name(meta.get("function_name"))
            generated = set(found)
            if boilerplate:
                generated.add(boilerplate)
            new = [
                tag for tag in old if not str(tag).lower().startswith(GENERATED)
            ] + sorted(generated)
            if new == old:
                continue
            changes[fid] = new
            md5 = str(meta.get("file_md5") or fid.rsplit(":", 2)[-2])
            md5s.add(md5)
            if apply:
                meta["tags"] = new
                pipe = r.pipeline(transaction=False)
                _unindex_tag(pipe, collection, "func", "tags", old, fid)
                pipe.set(f"{fid}:meta", json.dumps(meta))
                save_function(pipe, collection, md5, fid.rsplit(":", 1)[-1], meta)
                pipe.execute()
    for md5 in md5s:
        files += sync_file(r, collection, md5, changes, apply)
    if apply and changes:
        bump_tags_rev(r, collection)
        for pool in r.smembers(f"{collection}:pools"):
            bump_tags_rev(r, f"global:pool:{pool}")
    return {"functions": len(changes), "files": files}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--collection", action="append")
    target.add_argument("--all", action="store_true")
    parser.add_argument(
        "--apply", action="store_true", help="write changes (default: preview)"
    )
    args = parser.parse_args()
    r = get_redis()
    collections = args.collection or sorted(r.smembers("global:collections"))
    lookup = FidLookup()
    try:
        for collection in collections:
            result = backfill(r, collection, lookup, args.apply)
            print(
                f"{collection}: {'updated' if args.apply else 'would update'} {result['functions']} function(s), {result['files']} file(s)"
            )
    finally:
        lookup.close()


if __name__ == "__main__":
    main()
