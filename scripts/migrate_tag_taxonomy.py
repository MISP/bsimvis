"""Rewrite every stored tag id, for either of the two vocabulary moves.

Default (`migrate_tag`) -- onto the four-namespace taxonomy:

    lib:libc:2.31:memcpy      -> origin:lib:libc:2.31:memcpy
    bundle:mirai              -> origin:bundle:mirai:unknown
    flag:suspicious:crypto    -> severity:medium + category:crypto:cipher
    llm:malicious:injection   -> severity:high   + category:process:inject
    mirai                     -> user:mirai

`--modernize` (`modernize_tag_id`) -- the later move, putting the detector in
the namespace and per-function evidence in the `#` tail:

    origin:lib:libc:2.31:memcpy   -> fid:libc:2.31#memcpy
    origin:bundle:mirai:unknown   -> malware:mirai
    yara:trojan:mirai:ELF_Mirai   -> yara:trojan:mirai#ELF_Mirai
    cve:cve-2021-44228            -> cve:2021-44228

Both mappings live in `bsimvis.app.services.tag_taxonomy`, so this script only
handles storage and never has to know what a tag means. Both are idempotent: an
already-migrated tag maps to itself, so a re-run after an interruption is safe.

Run them in order on a corpus that predates both. The `#` tail is what stops a
function name from being a grouping level, so after `--modernize` the index
holds no bucket named after a symbol -- which is also why it must be followed by
the re-index this script already performs.

What it rewrites, per collection:

  * `tags` / `user_tags` on every file and function `:meta` doc
  * the file-level `lib_tags` staging sets
  * the `tags_metadata` hash (colour / priority / llm vocabulary flag)
  * the file- and func-level tag index buckets and registries, rebuilt by
    re-driving `index_service.save_file` / `save_function` so the ancestor
    expansion is the real one and cannot drift from it
  * `tags_rev`, bumped last, which marks every stored bin_sim split stale and
    makes the UI offer a resplit

What it does NOT rewrite: the **sim-level** tag index buckets
(`{coll}:idx:sim:{file,func}_tags:*`). Those are built by `save_similarity` from
per-pair function and file metadata, which this script would have to reconstruct
for every pair. They are left in the old id form -- harmless but stale, so a
sim-level tag filter keeps matching old ids until similarities are next rebuilt.
Nothing else depends on them.

    uv run python scripts/migrate_tag_taxonomy.py --collection main --dry-run
    uv run python scripts/migrate_tag_taxonomy.py --all
"""

import argparse
import json
import logging

from tqdm import tqdm

from bsimvis.app.services import tag_taxonomy
from bsimvis.app.services.bin_sim_tags import bump_tags_rev
from bsimvis.app.services.index_service import (
    FILE_TAG_FIELDS,
    FUNC_TAG_FIELDS,
    save_file,
    save_function,
)
from bsimvis.app.services.redis_client import get_redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Metas are fat (call graph, parameters). Read and write them a slice at a time
# so a 20k-function binary does not land in memory all at once.
BATCH = 500

# The two fields on a doc that hold tag ids. Everything else the indexer touches
# is derived from these.
TAG_FIELDS = ("tags", "user_tags")


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


# Which rewrite this run performs. `--modernize` selects the later one: the
# detector out of segment 2 and into the namespace, and per-function evidence out
# of a level and into the `#` tail. Both are id -> [id], both are idempotent, and
# every piece of storage handling below is identical for either -- which is the
# whole reason the mapping lives in `tag_taxonomy` and this script does not know
# what a tag means.
_MAPPERS = {
    "taxonomy": tag_taxonomy.migrate_tag,
    "modernize": lambda tag: [t for t in [tag_taxonomy.modernize_tag_id(tag)] if t],
}
_map_tag = _MAPPERS["taxonomy"]


def migrate_tag_list(tags):
    """Old tag list -> new tag list, order-preserving and deduped.

    Accepts the shapes `normalize_tags` accepts: a list, or a {tag: confidence}
    mapping. A mapping keeps each new tag at its old tag's confidence; when one
    old tag splits into two, both inherit it.
    """
    if isinstance(tags, dict):
        out = {}
        for tag, conf in tags.items():
            for new in _map_tag(tag):
                out.setdefault(new, conf)
        return out
    if isinstance(tags, str):
        # Old docs stored a comma-separated string (see index_service.normalize_tags).
        tags = [t.strip() for t in tags.split(",") if t.strip()]
    if not isinstance(tags, (list, tuple, set)):
        return tags

    out = []
    for tag in tags:
        for new in _map_tag(tag):
            if new not in out:
                out.append(new)
    return out


def migrate_meta(doc):
    """Rewrite a doc's tag fields in place. Returns True if anything changed."""
    changed = False
    for field in TAG_FIELDS:
        if field not in doc:
            continue
        new = migrate_tag_list(doc[field])
        if new != doc[field]:
            doc[field] = new
            changed = True
    return changed


def _tag_index_keys(r, collection, level, fields):
    """Every bucket and registry key holding tag buckets for one level."""
    keys = []
    for field in fields:
        if "tags" not in field:
            continue
        registry = f"{collection}:reg:{level}:{field}"
        keys.extend(_s(k) for k in r.smembers(registry))
        keys.append(registry)
    return keys


def _migrate_docs(r, collection, level, doc_ids, fields, dry_run):
    """Rewrite the metas of one level, then rebuild that level's tag indexes."""
    if not doc_ids:
        return 0

    if not dry_run:
        # Drop the old buckets wholesale rather than moving members one at a
        # time: every doc is about to be re-indexed anyway, and a partial move
        # would leave a doc in both the old and the new bucket.
        stale = _tag_index_keys(r, collection, level, fields)
        for start in range(0, len(stale), BATCH):
            r.delete(*stale[start : start + BATCH])

    changed = 0
    for start in tqdm(
        range(0, len(doc_ids), BATCH),
        desc=f"{collection}/{level}",
        unit="batch",
    ):
        chunk = doc_ids[start : start + BATCH]
        pipe = r.pipeline(transaction=False)
        for doc_id in chunk:
            pipe.get(f"{doc_id}:meta")
        raws = pipe.execute()

        pipe = r.pipeline(transaction=False)
        writes = 0
        for doc_id, raw in zip(chunk, raws):
            if not raw:
                continue
            try:
                doc = json.loads(_s(raw))
            except ValueError:
                continue
            if not isinstance(doc, dict):
                continue
            if migrate_meta(doc):
                changed += 1
                if dry_run:
                    continue
                pipe.set(f"{doc_id}:meta", json.dumps(doc))
                writes += 1
            if dry_run:
                continue
            # Re-index even when the tags did not change: the buckets for this
            # level were just deleted, so every doc has to be put back.
            parts = doc_id.split(":")
            if level == "file":
                save_file(pipe, collection, parts[-1], doc)
            else:
                save_function(pipe, collection, parts[-2], parts[-1], doc)
            writes += 1
        if writes:
            pipe.execute()

    return changed


def _migrate_tags_metadata(r, collection, dry_run):
    """Rewrite the tag registry hash, merging where two old ids converge.

    Colour and priority follow the first old id that produced a given new id;
    the `llm` vocabulary flag is OR-ed, because a tag is LLM-written if any of
    the ids folding into it was.
    """
    key = f"{collection}:tags_metadata"
    raw = r.hgetall(key) or {}
    if not raw:
        return 0

    merged = {}
    for k, v in raw.items():
        old = _s(k)
        val = _s(v)
        try:
            meta = (
                json.loads(val)
                if isinstance(val, str) and val.startswith("{")
                else {"color": val}
            )
        except ValueError:
            meta = {"color": val}
        if not isinstance(meta, dict):
            meta = {"color": val}
        for new in _map_tag(old):
            if new in merged:
                merged[new]["llm"] = bool(merged[new].get("llm") or meta.get("llm"))
            else:
                merged[new] = dict(meta)

    if merged == {
        _s(k): json.loads(_s(v)) for k, v in raw.items() if _s(v).startswith("{")
    }:
        return 0
    if dry_run:
        return len(merged)

    pipe = r.pipeline(transaction=False)
    pipe.delete(key)
    pipe.hset(key, mapping={k: json.dumps(v) for k, v in merged.items()})
    pipe.execute()
    return len(merged)


def _migrate_lib_tag_sets(r, collection, md5s, dry_run):
    """Rewrite the `{coll}:file:{md5}:lib_tags` staging sets."""
    touched = 0
    for md5 in md5s:
        key = f"{collection}:file:{md5}:lib_tags"
        members = [_s(m) for m in r.smembers(key)]
        if not members:
            continue
        new = migrate_tag_list(members)
        if new == members:
            continue
        touched += 1
        if dry_run:
            continue
        pipe = r.pipeline(transaction=False)
        pipe.delete(key)
        pipe.sadd(key, *new)
        pipe.execute()
    return touched


def migrate(collection, dry_run=False):
    r = get_redis()

    files = sorted(_s(f) for f in r.smembers(f"{collection}:all_files"))
    funcs = sorted(_s(f) for f in r.smembers(f"{collection}:all_functions"))
    if not files and not funcs:
        logging.warning(f"[!] Nothing indexed in collection {collection}")
        return 0

    n_files = _migrate_docs(r, collection, "file", files, FILE_TAG_FIELDS, dry_run)
    n_funcs = _migrate_docs(r, collection, "func", funcs, FUNC_TAG_FIELDS, dry_run)
    n_meta = _migrate_tags_metadata(r, collection, dry_run)
    n_lib = _migrate_lib_tag_sets(
        r, collection, [f.split(":")[-1] for f in files], dry_run
    )

    verb = "would rewrite" if dry_run else "rewrote"
    logging.info(
        f"[+] {collection}: {verb} tags on {n_files}/{len(files)} files, "
        f"{n_funcs}/{len(funcs)} functions, {n_meta} registry entries, "
        f"{n_lib} lib_tags sets"
    )
    if not dry_run:
        # Last, so an interrupted run does not claim the splits are current.
        # Every stored bin_sim split now predates the tags and the UI will
        # offer a resplit; the split_schema bump makes that unconditional.
        bump_tags_rev(r, collection)
        logging.info(
            f"[+] {collection}: bumped tags_rev -- run a bin_sim resplit to "
            "rebuild the per-axis summaries"
        )
    return n_files + n_funcs


def demo():
    """Storage-free checks of the rewrite rules."""
    assert migrate_tag_list(["lib:libc:2.31:memcpy", "flag:suspicious:crypto"]) == [
        "origin:lib:libc:2.31:memcpy",
        "severity:medium",
        "category:crypto:cipher",
    ]
    # Two old tags folding onto one new id must not duplicate it.
    assert migrate_tag_list(["flag:benign:init", "llm:benign:string"]) == [
        "severity:none",
        "category:util:init",
        "category:util:string",
    ]
    # Confidence mapping: a split tag carries its confidence to both halves.
    assert migrate_tag_list({"flag:malicious:c2": 0.8}) == {
        "severity:high": 0.8,
        "category:network:c2": 0.8,
    }
    # The legacy comma-separated string shape still found on old docs.
    assert migrate_tag_list("lib:libc, mirai") == [
        "origin:lib:libc:unknown",
        "user:mirai",
    ]
    # Idempotent -- a second run is a no-op.
    once = migrate_tag_list(["lib:libc:2.31", "flag:suspicious:c2", "bookmark"])
    assert migrate_tag_list(once) == once, once

    doc = {"tags": ["lib:libc:2.31"], "user_tags": ["bookmark"], "name": "f"}
    assert migrate_meta(doc) is True
    assert doc == {
        "tags": ["origin:lib:libc:2.31"],
        "user_tags": ["user:bookmark"],
        "name": "f",
    }, doc
    assert migrate_meta(doc) is False, "second pass must be a no-op"

    # --modernize: same storage handling, the other mapping. Swapped the way
    # main() swaps it, so the two paths cannot drift.
    global _map_tag
    _map_tag = _MAPPERS["modernize"]
    try:
        assert migrate_tag_list(
            ["origin:lib:libc:2.31:memcpy", "origin:bundle:mirai:unknown"]
        ) == ["fid:libc:2.31#memcpy", "malware:mirai"]
        # A rule name is a level on the way in and a symbol on the way out, so
        # it must not be case-folded like the levels around it.
        assert migrate_tag_list(["yara:trojan:mirai:ELF_Mirai"]) == [
            "yara:trojan:mirai#ELF_Mirai"
        ]
        again = migrate_tag_list(["origin:lib:libc:2.31:memcpy", "user:bookmark"])
        assert migrate_tag_list(again) == again, again

        moderned = {"tags": {"origin:lib:libc:2.31:memcpy": 0.9}, "name": "f"}
        assert migrate_meta(moderned) is True
        assert moderned["tags"] == {"fid:libc:2.31#memcpy": 0.9}, moderned
        assert migrate_meta(moderned) is False, "second pass must be a no-op"
    finally:
        _map_tag = _MAPPERS["taxonomy"]

    print("migrate_tag_taxonomy demo OK")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--collection", action="append", help="repeatable")
    ap.add_argument("--all", action="store_true", help="every known collection")
    ap.add_argument("--dry-run", action="store_true", help="report, write nothing")
    ap.add_argument("--demo", action="store_true", help="run the rule checks and exit")
    ap.add_argument(
        "--modernize",
        action="store_true",
        help="run the later rewrite instead: detector into the namespace, "
        "per-function evidence into the # tail "
        "(origin:lib:libc:2.31:memcpy -> fid:libc:2.31#memcpy)",
    )
    args = ap.parse_args()

    if args.modernize:
        global _map_tag
        _map_tag = _MAPPERS["modernize"]

    if args.demo:
        demo()
        return
    if args.all:
        collections = sorted(_s(c) for c in get_redis().smembers("global:collections"))
    elif args.collection:
        collections = args.collection
    else:
        ap.error("pass --collection NAME (repeatable), --all, or --demo")

    for c in collections:
        migrate(c, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
