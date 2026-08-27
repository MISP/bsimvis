#!/usr/bin/env python3
"""Copy files + functions (no similarity/cluster data) for one or more named
collections from a source Kvrocks instance into a target Kvrocks instance,
then rebuild the target's derived indexes (search indexes, vector classes,
feature registry) using the app's own real indexing code -- not a raw key
dump, so nothing downstream (search, tags, LCA discovery) silently breaks
from a missed registry key.

What gets copied per function:
    :meta :source :vec:meta :vec:raw :vec:tf :callees :callers :funcid
    (+ the funcid reverse-lookup set, {coll}:funcid:{hash})
What gets copied per file:
    {coll}:file:{md5}:meta
What gets rebuilt at the collection level:
    global:collections membership (without this the collection is invisible
    to /api/search/collections -- it enumerates that set, not any
    per-collection key), global:collection:{coll}:meta stats (total_files/
    total_functions/total_batches/last_updated), per-collection and global
    batch docs + registries ({coll}:all_batches, {coll}:batch:{uuid},
    global:batches, global:batch:{uuid}), and archive lineage (containment
    edges + rolled-up function counts, from each file's own parent_md5/
    path_in_parent -- already present in its copied :meta, just not yet
    indexed into the lineage graph).

What does NOT get copied (by design -- this is a "files + functions only"
migration): similarity pairs, function/binary clusters, bin_sim docs, LLM
analyses, notes, job history. Run the normal similarity/cluster/bin_sim
build on the target afterward (see doc/lca-remote-benchmark-walkthrough.md).

After the raw copy, this script calls the SAME functions the real
ingestion pipeline calls to build every derived index, instead of
hand-copying registry keys:
    - bsimvis.app.services.index_service.save_file / save_function
      (search/filter registries, {coll}:all_files, {coll}:idx:file:functions:*)
    - bsimvis.app.services.feature_service.FeatureService.index_functions
      (vector-class hashing, feature registry, {coll}:indexed:functions --
      this is the expensive, CPU-bound step; it repeats the cost the
      source instance already paid once per function, so budget real time
      for a large collection)
    - bsimvis.app.services.lineage_service.record / record_function_count
      (archive parent/child graph + rolled-up container function counts)

Dry-run by default -- prints what it would do and exits. Pass --apply to
actually write.

Usage:
    python3 scripts/migrate_collection.py \\
        --source-host OLD_HOST --source-port OLD_KVROCKS_PORT \\
        --target-host NEW_HOST --target-port NEW_KVROCKS_PORT \\
        --collection mycoll [--collection other_coll:renamed_coll] \\
        [--apply] [--force]

--collection accepts SRC[:DST] to rename during migration; omit ":DST" to
keep the same name on the target. Repeat --collection for multiple sources.
--force allows migrating into a target collection name that already has
data (default: refuse, to avoid silently merging into an existing collection).
"""
import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

import redis

# Make `bsimvis.app.services.*` importable regardless of CWD -- this script
# deliberately reuses the app's own indexing code (see module docstring)
# rather than reimplementing the secondary-index field lists by hand.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bsimvis.app.services.index_service import save_file, save_function
from bsimvis.app.services.feature_service import FeatureService
from bsimvis.app.services import lineage_service

FUNC_SUFFIXES_STRING = (":meta", ":source", ":vec:meta", ":vec:raw", ":funcid")
FUNC_SUFFIXES_ZSET = (":vec:tf",)
FUNC_SUFFIXES_SET = (":callees", ":callers")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source-host", required=True)
    p.add_argument("--source-port", type=int, required=True)
    p.add_argument("--source-db", type=int, default=0)
    p.add_argument("--target-host", required=True)
    p.add_argument("--target-port", type=int, required=True)
    p.add_argument("--target-db", type=int, default=0)
    p.add_argument(
        "--collection",
        action="append",
        required=True,
        dest="collections",
        help="SRC[:DST] collection name; repeat for multiple. e.g. --collection mirai7 --collection samples:samples_bench",
    )
    p.add_argument("--apply", action="store_true", help="Actually write. Default is dry-run.")
    p.add_argument("--force", action="store_true", help="Allow migrating into a target collection that already has data.")
    p.add_argument("--batch-size", type=int, default=200, help="Redis pipeline flush interval (functions).")
    return p.parse_args()


def rewrite_prefix(value, src_coll, dst_coll):
    """Rewrite a `{src_coll}:...` id/key string to `{dst_coll}:...`. Leaves
    anything else (ext:name callee ids, plain hashes, unrelated strings)
    untouched."""
    if isinstance(value, str) and value.startswith(f"{src_coll}:"):
        return f"{dst_coll}{value[len(src_coll):]}"
    return value


def patch_meta_json(raw, src_coll, dst_coll):
    """Rewrite the self-referential `collection` / `function_id` / `file_id`
    / `batch_id` fields inside a :meta JSON blob so the copied doc points at
    the target collection, not the source it was read from. `collection`
    holds the bare collection name (exact match); the rest hold
    `{collection}:func:...` / `{collection}:file:...` / `{collection}:batch:...`
    composite ids (prefix rewrite)."""
    try:
        doc = json.loads(raw)
    except (TypeError, ValueError):
        return raw
    if not isinstance(doc, dict):
        return raw
    changed = False
    if doc.get("collection") == src_coll:
        doc["collection"] = dst_coll
        changed = True
    for field in ("function_id", "file_id", "batch_id"):
        if field in doc:
            new_val = rewrite_prefix(doc[field], src_coll, dst_coll)
            if new_val != doc[field]:
                doc[field] = new_val
                changed = True
    return json.dumps(doc) if changed else raw


def migrate_one(src_r, dst_r, src_coll, dst_coll, apply, batch_size, force):
    print(f"\n[*] {src_coll} -> {dst_coll}")

    existing = dst_r.scard(f"{dst_coll}:indexed:functions") or dst_r.scard(f"{dst_coll}:all_files")
    if existing and not force:
        print(
            f"[-] target collection '{dst_coll}' already has data "
            f"({existing} functions/files) -- refusing without --force"
        )
        return False

    fids = sorted(f for f in src_r.smembers(f"{src_coll}:indexed:functions"))
    if not fids:
        print(f"[-] source collection '{src_coll}' has no indexed functions -- nothing to migrate")
        return False

    md5s = sorted({fid.split(":")[2] for fid in fids if len(fid.split(":")) >= 4})
    print(f"[*] {len(fids)} functions across {len(md5s)} files")

    if not apply:
        print("[*] dry-run -- would copy the above, then rebuild indexes on the target. Pass --apply to execute.")
        return True

    t0 = time.time()

    # 1. Files: raw :meta + save_file() indexing.
    pipe = dst_r.pipeline(transaction=False)
    copied_files = 0
    file_metas = []  # (md5, meta_dict), reused below for collection/batch registry + lineage
    for i, md5 in enumerate(md5s):
        raw = src_r.get(f"{src_coll}:file:{md5}:meta")
        if not raw:
            continue
        patched = patch_meta_json(raw, src_coll, dst_coll)
        pipe.set(f"{dst_coll}:file:{md5}:meta", patched)
        try:
            file_meta = json.loads(patched)
        except (TypeError, ValueError):
            file_meta = {}
        if not isinstance(file_meta, dict):
            file_meta = {}
        save_file(pipe, dst_coll, md5, file_meta)
        file_metas.append((md5, file_meta))
        copied_files += 1
        if (i + 1) % batch_size == 0:
            pipe.execute()
            pipe = dst_r.pipeline(transaction=False)
    pipe.execute()
    print(f"[+] copied {copied_files} file docs")

    # 2. Functions: raw keys, rewritten, + save_function() indexing.
    pipe = dst_r.pipeline(transaction=False)
    func_metas = []  # (fid, meta_dict) for save_function, applied after raw copy
    for i, sfid in enumerate(fids):
        parts = sfid.split(":")
        md5, addr = parts[-2], parts[-1]
        dfid = f"{dst_coll}:func:{md5}:{addr}"

        raw_meta = src_r.get(f"{sfid}:meta")
        if not raw_meta:
            continue  # not a real, fully-indexed function -- skip
        patched_meta = patch_meta_json(raw_meta, src_coll, dst_coll)
        pipe.set(f"{dfid}:meta", patched_meta)
        try:
            meta_doc = json.loads(patched_meta)
        except (TypeError, ValueError):
            meta_doc = {}
        if isinstance(meta_doc, dict):
            func_metas.append((dfid, md5, addr, meta_doc))

        raw_source = src_r.get(f"{sfid}:source")
        if raw_source:
            pipe.set(f"{dfid}:source", raw_source)

        raw_vec_meta = src_r.get(f"{sfid}:vec:meta")
        if raw_vec_meta:
            pipe.set(f"{dfid}:vec:meta", raw_vec_meta)

        raw_vec_raw = src_r.get(f"{sfid}:vec:raw")
        if raw_vec_raw:
            pipe.set(f"{dfid}:vec:raw", raw_vec_raw)

        tf_data = src_r.zrange(f"{sfid}:vec:tf", 0, -1, withscores=True)
        if tf_data:
            pipe.zadd(f"{dfid}:vec:tf", dict(tf_data))

        for suffix in FUNC_SUFFIXES_SET:
            members = src_r.smembers(f"{sfid}{suffix}")
            if members:
                remapped = [rewrite_prefix(m, src_coll, dst_coll) for m in members]
                pipe.sadd(f"{dfid}{suffix}", *remapped)

        fid_hash = src_r.get(f"{sfid}:funcid")
        if fid_hash:
            pipe.set(f"{dfid}:funcid", fid_hash)
            pipe.sadd(f"{dst_coll}:funcid:{fid_hash}", dfid)

        if (i + 1) % batch_size == 0:
            pipe.execute()
            pipe = dst_r.pipeline(transaction=False)
            print(f"    ...{i + 1}/{len(fids)} functions copied", end="\r")
    pipe.execute()
    print(f"\n[+] copied raw keys for {len(func_metas)} functions")

    # 3. Secondary indexing (search/filter registries + relationship sets),
    # same helper the real ingestion path calls inline.
    pipe = dst_r.pipeline(transaction=False)
    for j, (dfid, md5, addr, meta_doc) in enumerate(func_metas):
        save_function(pipe, dst_coll, md5, addr, meta_doc)
        if (j + 1) % batch_size == 0:
            pipe.execute()
            pipe = dst_r.pipeline(transaction=False)
    pipe.execute()
    print(f"[+] built secondary indexes for {len(func_metas)} functions")

    # 4. Vector-class layer + feature registry + {coll}:indexed:functions.
    # This is the expensive step (one vclass hash + writes per function) --
    # it repeats the cost the source instance already paid once.
    print(f"[*] building vector classes / feature registry for {len(func_metas)} functions (this is the slow step)...")
    fsvc = FeatureService(r=dst_r)
    all_dfids = [dfid for dfid, _, _, _ in func_metas]
    fsvc.index_functions(dst_coll, all_dfids)

    # 5. Collection + batch registry -- without this the target collection
    # is invisible to /api/search/collections (it enumerates
    # `global:collections`, not any per-collection key) and shows no
    # batches. total_files/total_functions self-heal on first search from
    # {coll}:all_files/all_functions (already populated by save_file/
    # save_function above), so they're set here for immediacy, not
    # correctness; total_batches has no such self-heal path, so
    # {coll}:all_batches is the one that actually matters.
    now_ms = int(time.time() * 1000)
    batch_uuids = sorted({fm.get("batch_uuid") for _, fm in file_metas if fm.get("batch_uuid")})
    pipe = dst_r.pipeline(transaction=False)
    pipe.sadd("global:collections", dst_coll)
    for batch_uuid in batch_uuids:
        pipe.sadd(f"{dst_coll}:all_batches", batch_uuid)
        pipe.sadd("global:batches", batch_uuid)

        src_batch_raw = src_r.get(f"{src_coll}:batch:{batch_uuid}")
        if src_batch_raw:
            pipe.set(f"{dst_coll}:batch:{batch_uuid}", patch_meta_json(src_batch_raw, src_coll, dst_coll))

        global_batch_key = f"global:batch:{batch_uuid}"
        existing_global_raw = dst_r.get(global_batch_key) or src_r.get(global_batch_key)
        try:
            global_batch = json.loads(existing_global_raw) if existing_global_raw else {}
        except (TypeError, ValueError):
            global_batch = {}
        if not isinstance(global_batch, dict):
            global_batch = {}
        global_batch.setdefault("batch_uuid", batch_uuid)
        global_batch.setdefault("batch_id", global_batch_key)
        global_batch.setdefault("created_at", now_ms)
        global_batch["last_updated"] = now_ms
        collections_map = global_batch.get("collections") or {}
        collections_map[dst_coll] = True
        global_batch["collections"] = collections_map
        pipe.set(global_batch_key, json.dumps(global_batch))

    last_updated = max((fm.get("entry_date") or 0 for _, fm in file_metas), default=now_ms)
    pipe.hset(
        f"global:collection:{dst_coll}:meta",
        mapping={
            "total_files": copied_files,
            "total_functions": len(func_metas),
            "total_batches": len(batch_uuids),
            "last_updated": last_updated,
        },
    )
    pipe.execute()
    print(f"[+] registered collection ({len(batch_uuids)} batches, in global:collections + search)")

    # 6. Lineage (archive containment + rolled-up function counts). Uses the
    # parent_md5/path_in_parent fields already present in the copied file
    # meta -- record() is idempotent, safe to call even if a file has no
    # parent (no-ops on missing parent_md5).
    for md5, fm in file_metas:
        parent_md5 = fm.get("parent_md5")
        if parent_md5:
            path = fm.get("path_in_parent") or fm.get("file_name", "")
            lineage_service.record(dst_coll, parent_md5, md5, path, r=dst_r)
    funcs_per_md5 = Counter(fmd5 for _, fmd5, _, _ in func_metas)
    for md5, fm in file_metas:
        count = fm.get("function_count")
        if count is None:
            count = funcs_per_md5.get(md5, 0)
        lineage_service.record_function_count(dst_coll, md5, count, r=dst_r)
    print("[+] rebuilt archive lineage + rolled-up function counts")

    elapsed = time.time() - t0
    print(f"[+] done: {len(func_metas)} functions, {copied_files} files, {elapsed:.1f}s")
    return True


def main():
    args = parse_args()

    src_r = redis.Redis(host=args.source_host, port=args.source_port, db=args.source_db, decode_responses=True)
    dst_r = redis.Redis(host=args.target_host, port=args.target_port, db=args.target_db, decode_responses=True)

    try:
        src_r.ping()
    except redis.exceptions.RedisError as e:
        print(f"[-] cannot reach source {args.source_host}:{args.source_port} -- {e}")
        sys.exit(1)
    try:
        dst_r.ping()
    except redis.exceptions.RedisError as e:
        print(f"[-] cannot reach target {args.target_host}:{args.target_port} -- {e}")
        sys.exit(1)

    ok = True
    for spec in args.collections:
        if ":" in spec:
            src_coll, dst_coll = spec.split(":", 1)
        else:
            src_coll = dst_coll = spec
        ok = migrate_one(src_r, dst_r, src_coll, dst_coll, args.apply, args.batch_size, args.force) and ok

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
