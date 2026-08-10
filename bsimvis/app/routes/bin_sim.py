import logging
import math
import re
import threading
import time
from collections import defaultdict, OrderedDict
from flask import request
from flask_restx import abort
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import normalize_tags
from bsimvis.app.services.bin_sim_tags import (
    TAG_UNTAGGED,
    normalize_tags as tag_ids,
    read_tags_rev,
)
from bsimvis.app.services.cluster_utils import (
    pick_best_shared_cluster,
    pick_best_cluster,
)
from bsimvis.app.services import container_sim_service, lineage_service
import json

job_service = JobService()


def _files_metadata(r, coll_a, coll_b, diff):
    """`{md5: {...}}` for the child files named by a container pair's diff.

    The container-pair equivalent of `functions_metadata`: the rows carry md5s,
    and the client needs a name, an architecture and a size to draw them.
    """
    md5s = set()
    for row in diff.get("matched", []):
        md5s.update(x for x in (row.get("md5_a"), row.get("md5_b")) if x)
    for row in diff.get("unique_to_a", []) + diff.get("unique_to_b", []):
        if row.get("md5"):
            md5s.add(row["md5"])
    if not md5s:
        return {}

    md5s = list(md5s)
    # Both sides of a stored pair live in one collection (cross-collection pairs
    # are pool-only, and pools have no container rollup), but coll_b is honoured
    # so a caller that passes one cannot silently get the wrong document.
    pipe = r.pipeline(transaction=False)
    for m in md5s:
        pipe.get(f"{coll_a}:file:{m}:meta")
        pipe.get(f"{coll_b}:file:{m}:meta")
    res = pipe.execute()

    out = {}
    for idx, m in enumerate(md5s):
        raw = res[idx * 2] or res[idx * 2 + 1]
        if not raw:
            continue
        try:
            meta = json.loads(raw) if not isinstance(raw, dict) else raw
        except (ValueError, TypeError):
            continue
        if isinstance(meta, list) and meta:
            meta = meta[0]
        if not isinstance(meta, dict):
            continue
        out[m] = {
            "file_name": meta.get("file_name"),
            "architecture": meta.get("language_id"),
            "function_count": meta.get("function_count"),
            "file_size": meta.get("file_size"),
            "tags": meta.get("tags", []),
            "user_tags": meta.get("user_tags", []),
            "path_in_parent": meta.get("path_in_parent"),
        }
    return out


def _enrich_diff_clusters(r, diff, collection, pool_id, algo):
    """Derive best-shared cluster + rarity per diff row live from current cluster data.

    Slim docs (Change 1) persist no cluster columns; this reattaches them at read so a
    cluster rebuild can't leave them stale. Legacy fat docs already carry the columns and
    are left untouched. Matched rows key off the highest-cohesion cluster the two funcs
    SHARE (empty when none) — same rule as the similarity view. [[Change 1]]
    """
    matched = diff.get("matched", [])
    ua = diff.get("unique_to_a", [])
    ub = diff.get("unique_to_b", [])

    if not (matched or ua or ub):
        return
    # Always derive (even for legacy fat docs) so the cluster tag/rarity/tooltip stats
    # reflect the CURRENT clustering — change the algo and these update on next read.

    is_pool = bool(pool_id)
    cluster_coll = f"global:pool:{pool_id}" if is_pool else collection

    fids = set()
    for m in matched:
        fids.update(x for x in (m.get("func_a"), m.get("func_b")) if x)
    for u in ua + ub:
        if u.get("func_id"):
            fids.add(u["func_id"])
    if not fids:
        return

    fids = list(fids)
    pipe = r.pipeline(transaction=False)
    for fid in fids:
        pipe.smembers(
            f"{cluster_coll}:{fid}:clusters" if is_pool else f"{fid}:clusters"
        )
    fid_labels, all_labels = {}, set()
    for fid, res in zip(fids, pipe.execute()):
        labels = {
            c.decode() if isinstance(c, bytes) else str(c) for c in (res or set())
        }
        fid_labels[fid] = labels
        all_labels |= labels

    cluster_meta = {}
    if all_labels:
        labels_list = list(all_labels)
        pipe = r.pipeline(transaction=False)
        for lbl in labels_list:
            pipe.get(f"{cluster_coll}:cluster:{algo}:{lbl}:meta")
        for lbl, res in zip(labels_list, pipe.execute()):
            if not res:
                continue
            m = json.loads(res) if not isinstance(res, dict) else res
            if isinstance(m, str):
                m = json.loads(m)
            if isinstance(m, dict):
                cluster_meta[lbl] = m

    def rarity(meta):
        if not meta:
            return 1.0
        cnt = meta.get("unique_files_count", 0) or 0
        return min(1.0, 1.0 / math.log(1 + cnt + 1))

    def apply(row, best):
        row["cluster_id"] = best.get("cluster_id", "") if best else ""
        row["cluster_uuid"] = best.get("cluster_uuid", "") if best else ""
        row["cluster_name"] = best.get("cluster_name", "") if best else ""
        row["cohesion"] = float(best.get("cohesion_score", 0.0)) if best else 0.0
        row["is_clustered"] = best is not None
        # Cluster stats for the tooltip (distinct from the row's own avg_features).
        row["cluster_member_count"] = int(best.get("member_count", 0)) if best else 0
        row["cluster_stability"] = (
            float(best.get("cluster_stability", 0.0)) if best else 0.0
        )
        row["cluster_avg_features"] = (
            float(best.get("avg_features", 0.0)) if best else 0.0
        )
        # Sample member functions for the tooltip (already in cluster meta). Cross-collection
        # / pool bin-sim can't fetch them by collection at hover, so ship them with the row.
        row["cluster_sample_functions"] = (
            best.get("sample_functions", []) if best else []
        )
        row["sim_rarity"] = rarity(best)
        row["collection_rarity"] = row["sim_rarity"]

    for m in matched:
        apply(
            m,
            pick_best_shared_cluster(
                fid_labels.get(m.get("func_a"), set()),
                fid_labels.get(m.get("func_b"), set()),
                cluster_meta,
            ),
        )
    for u in ua + ub:
        apply(
            u, pick_best_cluster(fid_labels.get(u.get("func_id"), set()), cluster_meta)
        )


def build_bin_sim():
    """Trigger background job to build binary similarities."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    md5_a = data.get("md5_a")
    md5_b = data.get("md5_b")
    min_cohesion = data.get("min_cohesion", 0.5)

    job_id = job_service.create_job(
        JobType.BUILD_BIN_SIM.value,
        {
            "collection": collection,
            "algo": algo,
            "md5_a": md5_a,
            "md5_b": md5_b,
            "min_cohesion": min_cohesion,
        },
    )
    job_service.enqueue_job(job_id)
    return {
        "status": "success",
        "job_id": job_id,
        "message": "Binary similarity build job enqueued",
    }


def resplit_bin_sim():
    """Recompute the tag split of stored pairs without rebuilding them.

    What tagging actually invalidates. The pair score comes from the matched
    edges alone, so re-tagging never changes it -- only how the score is broken
    down by tag. Replaying the split over the stored diff skips the BSim
    queries, the greedy matching and the clustering a rebuild would redo.
    """
    data = request.json or {}
    job_id = job_service.create_job(
        JobType.RESPLIT_BIN_SIM.value,
        {
            "collection": data.get("collection", "main"),
            "algo": data.get("algo", "unweighted_cosine"),
            "md5": data.get("md5"),
        },
    )
    job_service.enqueue_job(job_id)
    return {
        "status": "success",
        "job_id": job_id,
        "message": "Binary similarity tag resplit job enqueued",
    }


def clear_bin_sim():
    """Trigger background job to clear binary similarities."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    md5 = data.get("md5")

    job_id = job_service.create_job(
        JobType.CLEAR_BIN_SIM.value,
        {
            "collection": collection,
            "algo": algo,
            "md5": md5,
        },
    )
    job_service.enqueue_job(job_id)
    return {
        "status": "success",
        "job_id": job_id,
        "message": "Binary similarity clear job enqueued",
    }


def rebuild_bin_sim():
    """Trigger background pipeline to clear then build binary similarities."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    md5_a = data.get("md5_a")
    md5_b = data.get("md5_b")
    min_cohesion = data.get("min_cohesion", 0.5)

    clear_payload = {
        "collection": collection,
        "algo": algo,
        "md5": md5_a if (md5_a and md5_a == md5_b) else None,
    }

    build_payload = {
        "collection": collection,
        "algo": algo,
        "md5_a": md5_a,
        "md5_b": md5_b,
        "min_cohesion": min_cohesion,
    }

    pipeline_id = job_service.create_pipeline(
        [
            (JobType.CLEAR_BIN_SIM.value, clear_payload),
            (JobType.CLEAR_BIN_CLUSTER.value, {"collection": collection, "algo": algo}),
            (JobType.BUILD_BIN_SIM.value, build_payload),
            (
                JobType.CLUSTER_BINARIES.value,
                {
                    "collection": collection,
                    "algo": algo,
                    "min_cohesion": min_cohesion,
                },
            ),
        ]
    )
    return {
        "status": "success",
        "pipeline_id": pipeline_id,
        "message": "Binary similarity rebuild pipeline enqueued",
    }


def _swap_side_keys(d):
    """Swap every `<x>_a`/`<x>_b` (and `_1`/`_2`) pair in a dict, in place."""
    for key in list(d):
        if key.endswith("_a") and key[:-1] + "b" in d:
            twin = key[:-1] + "b"
        elif key.endswith("_1") and key[:-1] + "2" in d:
            twin = key[:-1] + "2"
        else:
            continue
        d[key], d[twin] = d[twin], d[key]


def _flip_tag_row(row):
    """Mirror one tags_summary row, recursively through its version children."""
    _swap_side_keys(row)
    # `bins` values are positional [count_a, weight_a, count_b, weight_b], so the
    # generic `_a`/`_b` key swap above cannot reach them.
    for b in (row.get("bins") or {}).values():
        if isinstance(b, list) and len(b) == 4:
            b[0], b[1], b[2], b[3] = b[2], b[3], b[0], b[1]
    for child in row.get("children") or []:
        _flip_tag_row(child)


def _flip_diff_sides(diff_data):
    """Mirror a stored bin_sim doc so side A becomes side B and vice versa."""
    _swap_side_keys(diff_data)
    # tags_summary is a list, so it is invisible to the top-level key swap.
    for key in ("tags_summary", "flags_summary"):
        for row in diff_data.get(key) or []:
            _flip_tag_row(row)
    # flag_matrix cells are positional too: [w_shared_a, w_shared_b, w_uniq_a,
    # w_uniq_b, n_shared_a, n_shared_b, n_uniq_a, n_uniq_b].
    for row in (diff_data.get("flag_matrix") or {}).values():
        for cell in row.values():
            if isinstance(cell, list) and len(cell) == 8:
                cell[0:8] = [cell[1], cell[0], cell[3], cell[2],
                             cell[5], cell[4], cell[7], cell[6]]
    diff = diff_data.get("diff")
    if not isinstance(diff, dict):
        return
    _swap_side_keys(diff)
    for row in diff.get("matched", []):
        _swap_side_keys(row)


def _hydrate_diff(r, data_raw, coll_a, md5_a, coll_b, md5_b, pool_id, algo):
    """Parse a stored bin_sim doc and attach file metadata, function metadata and
    live cluster columns, oriented so "_a" is the caller's A side.

    coll_a/md5_a/coll_b/md5_b are the REQUESTED pair: get_bin_sim may reorder its
    own locals to reach the stored doc, so it passes the req_* values here.
    """
    diff_data = json.loads(data_raw) if not isinstance(data_raw, dict) else data_raw

    # A pair is stored once, in whatever order it was built (canonical md5 sort for
    # collection pairs, build order for pools). Re-orient the doc to the requested
    # order so every "_a"/"_b" — file metadata, unique_to_b, func_b — describes the
    # binary the caller called A/B.
    stored_a = diff_data.get("md5_a")
    if stored_a and stored_a != md5_a:
        _flip_diff_sides(diff_data)

    # Extract all unique function IDs
    fids = set()
    diff = diff_data.get("diff", {})
    for m in diff.get("matched", []):
        if m.get("func_a"):
            fids.add(m["func_a"])
        if m.get("func_b"):
            fids.add(m["func_b"])
    for u in diff.get("unique_to_a", []):
        if u.get("func_id"):
            fids.add(u["func_id"])
    for u in diff.get("unique_to_b", []):
        if u.get("func_id"):
            fids.add(u["func_id"])

    # Fetch File Metadata for both sides (each from their own collection)
    pipe = r.pipeline(transaction=False)
    pipe.get(f"{coll_a}:file:{md5_a}:meta")
    pipe.get(f"{coll_b}:file:{md5_b}:meta")
    file_meta_res = pipe.execute()

    if file_meta_res[0]:
        file_meta_0 = (
            json.loads(file_meta_res[0])
            if not isinstance(file_meta_res[0], dict)
            else file_meta_res[0]
        )
        if isinstance(file_meta_0, list) and file_meta_0:
            file_meta_0 = file_meta_0[0]
        diff_data["file_metadata_a"] = file_meta_0
    if file_meta_res[1]:
        file_meta_1 = (
            json.loads(file_meta_res[1])
            if not isinstance(file_meta_res[1], dict)
            else file_meta_res[1]
        )
        if isinstance(file_meta_1, list) and file_meta_1:
            file_meta_1 = file_meta_1[0]
        diff_data["file_metadata_b"] = file_meta_1

    # Retrieve function metadata
    fids = list(fids)
    if fids:
        pipe = r.pipeline(transaction=False)
        for fid in fids:
            pipe.get(f"{fid}:meta")
        meta_results = pipe.execute()

        funcs_metadata = {}
        for fid, raw_meta in zip(fids, meta_results):
            if raw_meta:
                meta = (
                    json.loads(raw_meta) if not isinstance(raw_meta, dict) else raw_meta
                )
                if isinstance(meta, list) and meta:
                    meta = meta[0]
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except ValueError:
                        pass
                if isinstance(meta, dict):
                    # index_service.normalize_tags drops any non-list `tags` to [],
                    # which silently erases the {tag_id: confidence} shape that
                    # build_bin_sim itself reads (bin_sim_service.py:316). Left as
                    # was, the tree counts a function under libc while every table
                    # shows it as Original. Keep the raw value and resolve it with
                    # the tag-aware normalizer; fall back to the legacy
                    # comma-string handling that index_service does do.
                    raw_tags = meta.get("tags")
                    normalize_tags(meta)
                    funcs_metadata[fid] = {
                        "name": meta.get("function_name"),
                        "return_type": meta.get("return_type"),
                        "parameters": meta.get("parameters"),
                        "namespace": meta.get("namespace"),
                        "entrypoint_address": meta.get("entrypoint_address")
                        or fid.split(":")[-1],
                        "tags": sorted(tag_ids(raw_tags)) or meta.get("tags", []),
                        "user_tags": meta.get("user_tags", []),
                        "note_owners": meta.get("note_owners", []),
                        "note_count": meta.get("note_count", 0),
                        "bsim_features_count": int(
                            meta.get("bsim_features_count") or 0
                        ),
                    }
                    continue

            # Fallback
            addr = fid.split(":")[-1]
            funcs_metadata[fid] = {
                "name": f"sub_{addr}",
                "return_type": "void",
                "parameters": [],
                "namespace": "",
                "entrypoint_address": addr,
                "tags": [],
                "user_tags": [],
                "note_owners": [],
                "note_count": 0,
                "bsim_features_count": 0,
            }
        diff_data["functions_metadata"] = funcs_metadata
    else:
        diff_data["functions_metadata"] = {}

    if diff_data.get("is_container_pair"):
        # Rows here are child files, not functions: there is no function cluster
        # to attribute them to, and the names the client needs are file names.
        diff_data["files_metadata"] = _files_metadata(r, coll_a, coll_b, diff)
    else:
        _enrich_diff_clusters(r, diff, coll_a, pool_id, algo)
    return diff_data


# Hydrating one diff costs ~2 kvrocks round trips per function (meta + clusters),
# so a 30k-function pair costs seconds — and the UI re-requests the same pair on
# every filter, sort and tree-node expansion. Cache the hydrated doc so only the
# first request pays. The requested A/B orientation is part of the key because
# _flip_diff_sides rewrites the doc in place.
# Expiry is sliding, with a ceiling. _DIFF_IDLE_TTL measures time since the last
# read, so someone who sits on one diff and keeps filtering never re-pays the
# hydration mid-session; _DIFF_MAX_AGE measures time since hydration and expires
# the entry regardless, so a tag edit or a cluster rebuild cannot be hidden
# indefinitely by continuous use.
# ponytail: expiry, not invalidation — the ceiling is how stale a doc can get.
# A generation counter bumped on the rebuild and tag-write paths would make this
# exact and let both numbers grow; add it if _DIFF_MAX_AGE has to be tuned.
# ponytail: entries are capped by count, not by bytes. A hydrated 30k-function
# doc is hundreds of MB and the app is one process, so the cap is deliberately
# small — two entries is one pair held in both A/B orientations, which is the
# working set of someone reading a diff. Measure bytes and cap on those if a
# bigger window is ever wanted.
_DIFF_IDLE_TTL = 60
_DIFF_MAX_AGE = 600
_DIFF_CACHE_MAX = 2
_DIFF_CACHE = OrderedDict()  # key -> (hydrated_at, last_used, diff_data)
# app.run() is threaded, so request threads share this dict: one thread can drop
# an expired entry while another is between its lookup and its move_to_end.
_DIFF_CACHE_LOCK = threading.Lock()


def _diff_cache_get(key):
    with _DIFF_CACHE_LOCK:
        entry = _DIFF_CACHE.get(key)
        if not entry:
            return None
        hydrated_at, last_used, doc = entry
        now = time.time()
        if now - last_used > _DIFF_IDLE_TTL or now - hydrated_at > _DIFF_MAX_AGE:
            _DIFF_CACHE.pop(key, None)
            return None
        _DIFF_CACHE[key] = (hydrated_at, now, doc)
        _DIFF_CACHE.move_to_end(key)
        return doc


def _diff_cache_put(key, doc):
    with _DIFF_CACHE_LOCK:
        now = time.time()
        _DIFF_CACHE[key] = (now, now, doc)
        _DIFF_CACHE.move_to_end(key)
        while len(_DIFF_CACHE) > _DIFF_CACHE_MAX:
            _DIFF_CACHE.popitem(last=False)


def get_bin_sim(collection=None, md5_a=None, md5_b=None, coll_b=None, pool_id=None):
    """Retrieve binary similarity diff for a pair."""
    if collection is None:
        collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    if md5_a is None:
        md5_a = request.args.get("md5_a")
    if md5_b is None:
        md5_b = request.args.get("md5_b")
    if coll_b is None:
        coll_b = request.args.get("coll_b", collection)
    if pool_id is None:
        pool_id = request.args.get("pool_id") or request.args.get("pool")

    if not md5_a or not md5_b:
        abort(400, "Both md5_a and md5_b are required")

    r = get_redis()
    coll_a = collection
    # What the caller asked for. Lookup below may reorder these to reach the
    # stored doc; the response must still come back in the caller's order.
    req_coll_a, req_md5_a, req_coll_b, req_md5_b = coll_a, md5_a, coll_b, md5_b

    if pool_id:
        # For pool pairs, look up the SID via the 'involves' index to avoid
        # guessing the ordering used at storage time.
        involves_a = f"global:pool:{pool_id}:bin_sim:involves:{coll_a}:{md5_a}"
        involves_b = f"global:pool:{pool_id}:bin_sim:involves:{coll_b}:{md5_b}"
        pipe = r.pipeline(transaction=False)
        pipe.smembers(involves_a)
        pipe.smembers(involves_b)
        res_a, res_b = pipe.execute()

        sids_a = {s.decode() if isinstance(s, bytes) else s for s in (res_a or set())}
        sids_b = {s.decode() if isinstance(s, bytes) else s for s in (res_b or set())}
        common = sids_a & sids_b

        if not common:
            return {
                "status": "not_found",
                "message": "Similarity not calculated for this pair",
            }, 404

        sid = next(iter(common))
    else:
        # Non-pool: canonical ordering md5_a < md5_b
        if md5_a > md5_b:
            md5_a, md5_b = md5_b, md5_a
            coll_a, coll_b = coll_b, coll_a
        sid = f"{collection}:bin_sim:{algo}:{md5_a}::{md5_b}"

    key = (sid, req_md5_a, req_coll_a, req_coll_b, algo, pool_id)
    # A resplit runs in the worker, so this process cannot be told to drop its
    # cache entry. Comparing the revision a doc was split at against the
    # collection's current one costs one GET, and answers two questions at once:
    # whether the cached copy is worth keeping, and whether the split the client
    # is about to draw predates the user's tagging.
    cur_rev = read_tags_rev(r, f"global:pool:{pool_id}" if pool_id else collection)
    diff_data = _diff_cache_get(key)
    if diff_data is not None and (diff_data.get("tags_rev") or 0) != cur_rev:
        diff_data = None
    if diff_data is None:
        data_raw = r.get(sid)

        if not data_raw:
            return {
                "status": "not_found",
                "message": "Similarity not calculated for this pair",
            }, 404

        # The lookup above may have reordered coll_a/md5_a to reach the stored doc;
        # hydration works in the order the caller asked for.
        diff_data = _hydrate_diff(
            r, data_raw, req_coll_a, req_md5_a, req_coll_b, req_md5_b, pool_id, algo
        )
        _diff_cache_put(key, diff_data)

    # Tags changed since this pair was split. The score is unaffected -- it comes
    # from the matched edges alone -- so this offers a resplit rather than
    # invalidating the pair.
    diff_data["tags_stale"] = (diff_data.get("tags_rev") or 0) != cur_rev

    # The canonical-md5 lookup above may have swapped these; everything below
    # answers in the order the caller asked for.
    coll_a, md5_a, coll_b, md5_b = req_coll_a, req_md5_a, req_coll_b, req_md5_b

    # Change 4: when a table is requested, filter/sort/paginate server-side and return
    # only the page (+ its function metadata). Absent `table` → full doc (back-compat).
    table = request.args.get("table")
    if table in ("matched", "unique_to_a", "unique_to_b", "all"):
        return _page_diff(diff_data, table, r, coll_a, algo, pool_id)

    # Change 4: compact projection for the simplified Sankey — cluster fields + the few
    # numerics its binning needs, feature counts inlined, NO names/tags/notes. Lets the
    # graph render for thousands of funcs without shipping fat rows. [[Change 4]]
    if request.args.get("view") == "sankey":
        return _sankey_summary(diff_data)

    return diff_data


def _sankey_summary(diff_data):
    diff = diff_data.get("diff", {})
    fmeta = diff_data.get("functions_metadata", {})

    if diff_data.get("is_container_pair"):
        # No function flow to project: the summary a container pair needs is how
        # much of each side its children account for, and how much of it the
        # score does not speak for at all.
        out = {
            k: diff_data.get(k)
            for k in (
                "score",
                "is_container_pair",
                "coverage_a",
                "coverage_b",
                "functions_count_a",
                "functions_count_b",
                "child_count_a",
                "child_count_b",
                "analyzed_bytes_a",
                "unanalyzed_bytes_a",
                "analyzed_bytes_b",
                "unanalyzed_bytes_b",
                "file_metadata_a",
                "file_metadata_b",
            )
        }
        out["counts"] = {
            "matched": len(diff.get("matched", [])),
            "unique_to_a": len(diff.get("unique_to_a", [])),
            "unique_to_b": len(diff.get("unique_to_b", [])),
        }
        out["tags_summary"] = []
        out["flags_summary"] = []
        out["flag_matrix"] = {}
        return out

    def feat(fid):
        try:
            return max(1, int(fmeta.get(fid, {}).get("bsim_features_count") or 1))
        except (ValueError, TypeError):
            return 1

    matched = [
        {
            "cluster_uuid": m.get("cluster_uuid", ""),
            "cluster_name": m.get("cluster_name", ""),
            "cohesion": m.get("cohesion", 0.0),
            "similarity": m.get("similarity", 0.0),
            "avg_features": m.get("avg_features", 0.0),
            "sim_rarity": m.get("sim_rarity", 0.0),
            "is_clustered": m.get("is_clustered", False),
            "feat_a": feat(m.get("func_a")),
            "feat_b": feat(m.get("func_b")),
        }
        for m in diff.get("matched", [])
    ]

    def uniq(rows):
        return [
            {
                "cluster_uuid": u.get("cluster_uuid", ""),
                "cluster_name": u.get("cluster_name", ""),
                "cohesion": u.get("cohesion", 0.0),
                "avg_features": u.get("avg_features", 0.0),
                "sim_rarity": u.get("sim_rarity", 0.0),
                "is_clustered": u.get("is_clustered", False),
                "feat": feat(u.get("func_id")),
            }
            for u in rows
        ]

    out = {
        k: diff_data.get(k)
        for k in (
            "score",
            "file_metadata_a",
            "file_metadata_b",
            "tags_summary",
            "flags_summary",
            "flag_matrix",
            "tags_stale",
        )
    }
    out["counts"] = {
        "matched": len(matched),
        "unique_to_a": len(diff.get("unique_to_a", [])),
        "unique_to_b": len(diff.get("unique_to_b", [])),
    }
    out["sankey"] = {
        "matched": matched,
        "unique_to_a": uniq(diff.get("unique_to_a", [])),
        "unique_to_b": uniq(diff.get("unique_to_b", [])),
    }
    return out


def _sim_pair_sid(fid_a, fid_b, collection, algo, pool_id):
    """Key of the function-level similarity doc a matched row came from.

    Every matched row IS one of those docs — bin_sim_service builds the diff by
    intersecting them (bin_sim_service.py:383) — so the pair's own tags, the ones
    the function-similarity view edits, are readable from here. Mirrors how
    similarity_service writes the key (similarity_service.py:1074): larger fid
    first, collection pairs stripped of their `<coll>:func:` prefix.
    """
    if not fid_a or not fid_b:
        return None
    a, b = (fid_a, fid_b) if fid_a > fid_b else (fid_b, fid_a)
    if pool_id:
        return f"global:pool:{pool_id}:sim:{a}::{b}"
    prefix = f"{collection}:func:"
    strip = lambda f: f[len(prefix) :] if f.startswith(prefix) else f  # noqa: E731
    return f"{collection}:sim:{algo}:{strip(a)}::{strip(b)}"


def _fetch_sim_tags(r, sids):
    """sid -> (tags, user_tags) for the similarity docs named."""
    sids = [s for s in dict.fromkeys(sids) if s]
    if not sids:
        return {}
    pipe = r.pipeline(transaction=False)
    for s in sids:
        pipe.get(s)
    out = {}
    for s, raw in zip(sids, pipe.execute()):
        if not raw:
            continue
        doc = json.loads(raw) if not isinstance(raw, dict) else raw
        if isinstance(doc, str):
            doc = json.loads(doc)
        if not isinstance(doc, dict):
            continue
        normalize_tags(doc)
        out[s] = (doc.get("tags") or [], doc.get("user_tags") or [])
    return out


def _sim_tag_match(pair_tags, needles):
    """True when every needle matches a tag, namespace prefixes included.

    Same rule as the tree's scope (`lib` catches `lib:libc:2.31`), so typing a
    namespace in the tag filter behaves the way clicking one in the tree does.
    """
    have = [str(t).lower() for t in pair_tags]
    return all(any(t == n or t.startswith(n + ":") for t in have) for n in needles)


def _fnum(name):
    v = request.args.get(name)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# The File sim view browses one table with a `state` column rather than three
# tables, so Matched / Unmatched / All are the same request with a different
# `state` filter. Rows are tagged with their origin on the way out.
_STATES = {"matched": "matched", "uniq_a": "unique_to_a", "uniq_b": "unique_to_b"}

# Ghidra's auto-generated names. Two functions both called FUN_00401234 are not
# two copies of one function, so these never fold together.
_DEFAULT_NAME = re.compile(r"^(FUN_|sub_|thunk_)", re.I)


def _diff_rows(diff_data, table):
    """Rows of one diff table, or of all three unioned, each tagged with `state`."""
    diff = diff_data.get("diff", {})
    if table != "all":
        state = next(s for s, t in _STATES.items() if t == table)
        return [dict(r, state=state) for r in diff.get(table, [])]
    return [
        dict(r, state=state) for state, t in _STATES.items() for r in diff.get(t, [])
    ]


def _row_tags(item, fmeta):
    """Tag ids attributed to a row, unioned over whichever sides it has.

    Each side falls back to `original_code` on its own, matching how the tag
    split attributes mass (bin_sim_tags.py:121): a match between a tagged and an
    untagged function belongs to both buckets, not to neither.
    """
    tags = set()
    for fid in (item.get("func_a"), item.get("func_b"), item.get("func_id")):
        if not fid:
            continue
        own = set(tag_ids((fmeta.get(fid) or {}).get("tags")))
        tags |= own or {TAG_UNTAGGED}
    return tags


def _fold_key(item, fmeta):
    """Name that folds duplicate copies together, or None to stand alone.

    A-side name wins so a stripped-A / symbolized-B pair folds under the symbol
    that actually names it.
    """
    for fid in (item.get("func_a"), item.get("func_id"), item.get("func_b")):
        if not fid:
            continue
        name = (fmeta.get(fid) or {}).get("name") or ""
        if name and not _DEFAULT_NAME.match(name):
            return name
    return None


def _collapse_by_name(rows, fmeta):
    """One row per distinct function name, carrying `n_copies`.

    Paging over rows would let a name's copies straddle a page boundary, and any
    sort other than by name would scatter them so the UI could never fold them.
    Paging over names cannot. The representative is the best matched copy, so the
    folded row shows the strongest evidence rather than an arbitrary one.
    """
    singles, groups = [], {}
    for idx, it in enumerate(rows):
        key = _fold_key(it, fmeta)
        if key is None:
            singles.append((idx, dict(it, n_copies=1)))
        else:
            groups.setdefault(key, []).append((idx, it))

    def rank(pair):
        it = pair[1]
        return (it.get("state") == "matched", it.get("similarity") or 0.0)

    out = singles
    for key, members in groups.items():
        _, best = max(members, key=rank)
        out.append((members[0][0], dict(best, n_copies=len(members), fold_name=key)))
    out.sort(key=lambda p: p[0])
    return [r for _, r in out]


def _page_diff(diff_data, table, r=None, collection=None, algo=None, pool_id=None):
    """Filter + sort + slice one diff table, returning only the requested page.
    Ports the former client-side applyFilters/sortItems (binary_similarity.js). [[Change 4]]
    """
    rows = _diff_rows(diff_data, table)
    fmeta = diff_data.get("functions_metadata", {})

    # The pair's own key, so the row can carry the pair's tags and be tagged back.
    # Pure string work, so it costs nothing to do for every row.
    if r is not None:
        for it in rows:
            if it.get("state") == "matched":
                it["sid"] = _sim_pair_sid(
                    it.get("func_a"), it.get("func_b"), collection, algo, pool_id
                )

    q = (request.args.get("q") or "").strip().lower()
    cl_q = (request.args.get("cl_q") or "").strip().lower()
    note_a = (request.args.get("note_a") or "").strip().lower()
    note_b = (request.args.get("note_b") or "").strip().lower()
    note = (request.args.get("note") or "").strip().lower()
    sim_min, sim_max = _fnum("sim_min"), _fnum("sim_max")
    feat_min, feat_max = _fnum("feat_min"), _fnum("feat_max")
    rar_min, rar_max = _fnum("rar_min"), _fnum("rar_max")

    # Tree scope. Prefix match so selecting `lib:libc:2.31` catches every
    # `lib:libc:2.31:memcpy` under it, which is the same rollup rule the tag
    # summary uses (bin_sim_tags.py:52).
    tag_scope = [t for t in (request.args.get("tags") or "").split(",") if t.strip()]
    tag_scope = [t.strip() for t in tag_scope]
    states = {s for s in (request.args.get("state") or "").split(",") if s.strip()}
    fold_name = request.args.get("name")

    def haystack(fid):
        m = fmeta.get(fid, {})
        addr = m.get("entrypoint_address") or (fid.split(":")[-1] if fid else "")
        parts = [m.get("name"), m.get("namespace"), addr]
        parts += m.get("tags", []) + m.get("user_tags", [])
        return " ".join(str(p) for p in parts if p).lower()

    def owners_match(fid, needle):
        owners = fmeta.get(fid, {}).get("note_owners", []) if fid else []
        return any(needle in str(o).lower() for o in owners)

    def file_haystack(item):
        """Searchable text of a container-pair row, which names files not functions."""
        parts = [
            item.get(k)
            for k in (
                "file_name_a",
                "file_name_b",
                "file_name",
                "path_in_parent_a",
                "path_in_parent_b",
                "path_in_parent",
                "md5_a",
                "md5_b",
                "md5",
            )
        ]
        return " ".join(str(p) for p in parts if p).lower()

    def keep(item):
        fids = [x for x in (item.get("func_a"), item.get("func_b")) if x] or (
            [item["func_id"]] if item.get("func_id") else []
        )
        if states and item.get("state") not in states:
            return False
        if tag_scope:
            tags = _row_tags(item, fmeta)
            if not any(
                t == p or t.startswith(p + ":") for t in tags for p in tag_scope
            ):
                return False
        if fold_name is not None and _fold_key(item, fmeta) != fold_name:
            return False
        if q:
            # A container-pair row has no function ids, so it is searched by the
            # file names and paths it does carry.
            hay_hit = (
                any(q in haystack(f) for f in fids)
                if fids
                else q in file_haystack(item)
            )
            if not hay_hit:
                return False
        if cl_q and cl_q not in (item.get("cluster_name") or "unclustered").lower():
            return False
        if note_a and not owners_match(item.get("func_a"), note_a):
            return False
        if note_b and not owners_match(item.get("func_b"), note_b):
            return False
        if note and not owners_match(item.get("func_id"), note):
            return False
        sim = item.get("similarity") or 0
        if sim_min is not None and sim < sim_min:
            return False
        if sim_max is not None and sim > sim_max:
            return False
        feat = item.get("avg_features") or 0
        if feat_min is not None and feat < feat_min:
            return False
        if feat_max is not None and feat > feat_max:
            return False
        rar = item.get("sim_rarity")
        if rar is not None:
            if rar_min is not None and rar < rar_min:
                return False
            if rar_max is not None and rar > rar_max:
                return False
        return True

    filtered = [it for it in rows if keep(it)]

    # Similarity-tag filter. Kept out of `keep` because it is the one filter that
    # needs a Redis read: only the rows that survive everything else are looked up,
    # and only when the filter is actually set. Unmatched rows have no pair, so a
    # similarity-tag filter excludes them by construction.
    # ponytail: one GET per surviving matched row; add a tag->sid index if a
    # filter over a very large diff turns out to be slow.
    sim_tags = [
        t.strip().lower()
        for t in (request.args.get("sim_tags") or "").split(",")
        if t.strip()
    ]
    sim_tags_not = [
        t.strip().lower()
        for t in (request.args.get("sim_tags_not") or "").split(",")
        if t.strip()
    ]
    sim_tag_cache = {}
    if r is not None and (sim_tags or sim_tags_not):
        sim_tag_cache = _fetch_sim_tags(r, [it.get("sid") for it in filtered])

        def tag_keep(it):
            static, user = sim_tag_cache.get(it.get("sid"), ([], []))
            have = list(static) + list(user)
            if sim_tags and not _sim_tag_match(have, sim_tags):
                return False
            if sim_tags_not and any(_sim_tag_match(have, [n]) for n in sim_tags_not):
                return False
            return True

        filtered = [it for it in filtered if tag_keep(it)]

    # Collapse before sorting and paging: the page must hold whole names, and the
    # representative each fold picks is what the sort then ranks. `name=` is the
    # expansion request for one fold, so it never re-collapses.
    if request.args.get("collapse") == "name" and fold_name is None:
        filtered = _collapse_by_name(filtered, fmeta)

    sort_col = request.args.get("sort_col")
    if sort_col:
        rev = request.args.get("sort_dir", "desc") != "asc"

        def sort_val(it):
            if sort_col == "func_name":
                # Unique rows have no name on the row; resolve from metadata.
                fid = it.get("func_id") or it.get("func_a")
                return (fmeta.get(fid, {}).get("name") or "").lower()
            return it.get(sort_col)

        def key(it):
            # Group by type so str never compares to num; missing -> -inf (numeric group).
            v = sort_val(it)
            if isinstance(v, str):
                return (1, v)
            return (0, v if v is not None else float("-inf"))

        filtered.sort(key=key, reverse=rev)

    total = len(filtered)
    try:
        offset = max(0, int(request.args.get("offset", 0)))
    except (TypeError, ValueError):
        offset = 0
    try:
        limit = int(request.args.get("limit", 100))
    except (TypeError, ValueError):
        limit = 100
    page = filtered[offset : offset + limit] if limit > 0 else filtered[offset:]

    # Pair tags for the page only — the rows the client is about to draw.
    if r is not None:
        missing = [
            it["sid"] for it in page if it.get("sid") and it["sid"] not in sim_tag_cache
        ]
        sim_tag_cache.update(_fetch_sim_tags(r, missing))
        for it in page:
            static, user = sim_tag_cache.get(it.get("sid"), ([], []))
            it["tags"] = list(static)
            it["user_tags"] = list(user)

    page_fids = set()
    for it in page:
        page_fids.update(
            x for x in (it.get("func_a"), it.get("func_b"), it.get("func_id")) if x
        )

    return {
        "items": page,
        "total": total,
        "offset": offset,
        "limit": limit,
        "table": table,
        "functions_metadata": {f: fmeta[f] for f in page_fids if f in fmeta},
        "files_metadata": diff_data.get("files_metadata", {}),
        "is_container_pair": bool(diff_data.get("is_container_pair")),
        "file_metadata_a": diff_data.get("file_metadata_a"),
        "file_metadata_b": diff_data.get("file_metadata_b"),
    }


def _group_by_container(collection, algo, md5, scored_sids, r):
    """Collapse matches that live inside a container into that container's row.

    Grouping has to happen before paging, not after: a container and the
    children it swallows are ranked by their own scores, so a post-filter over
    one page would leave a child on page 1 whose container sits on page 3.

    Returns `(top_level, {container_sid: [(child_sid, score)]})`. A match whose
    container is not itself in the results stays where it is -- the container is
    context, not a filter.
    """
    other_of = {}
    for sid, _score in scored_sids:
        parsed = container_sim_service._parse_sid(sid, collection, algo)
        if not parsed:
            continue
        a, b = parsed
        other_of[sid] = b if a == md5 else a

    present = {other: sid for sid, other in other_of.items()}
    containers = lineage_service.container_md5s(collection, r)

    # One walk per container in the results, rather than one per match: the
    # containers are the few, the matches are the many.
    owner_of = {}
    for c in (m for m in present if m in containers):
        for edge in lineage_service.descendants(collection, c, r):
            owner_of.setdefault(edge["md5"], c)

    top, children = [], defaultdict(list)
    for sid, score in scored_sids:
        other = other_of.get(sid)
        owner = owner_of.get(other) if other and other not in containers else None
        if owner and present.get(owner) and present[owner] != sid:
            children[present[owner]].append((sid, score))
        else:
            top.append((sid, score))
    return top, children


def list_bin_sims():
    """List similar binaries to a given binary."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    md5 = request.args.get("md5")
    limit = int(request.args.get("limit", 20))
    offset = int(request.args.get("offset", 0))

    if not md5:
        abort(400, "md5 parameter is required")

    r = get_redis()

    pool_id = request.args.get("pool")
    is_pool = pool_id is not None

    # To efficiently list, we check involves set
    if is_pool:
        cursor = 0
        matching_keys = []
        while True:
            cursor, found_keys = r.scan(
                cursor=cursor,
                match=f"global:pool:{pool_id}:bin_sim:involves:*:{md5}",
                count=1000,
            )
            matching_keys.extend(found_keys)
            if cursor == 0:
                break
        sids = set()
        if matching_keys:
            pipe = r.pipeline(transaction=False)
            for k in matching_keys:
                pipe.smembers(k)
            res = pipe.execute()
            for s_set in res:
                sids.update(s_set)
        sids = list(sids)
    else:
        involves_key = f"{collection}:bin_sim:involves:{md5}"
        sids = list(r.smembers(involves_key))

    if not sids:
        return {"total": 0, "results": [], "offset": offset, "limit": limit}

    sids = [sid.decode() if isinstance(sid, bytes) else sid for sid in sids]

    # We should get scores for these from the zset
    # Actually, we can just ZSCORE the zset to sort them
    if is_pool:
        zset_key = f"global:pool:{pool_id}:bin_sim:score:{algo}"
    else:
        zset_key = f"{collection}:bin_sim:score:{algo}"

    scored_sids = []
    pipe = r.pipeline(transaction=False)
    for sid in sids:
        pipe.zscore(zset_key, sid)
    scores = pipe.execute()

    for i, sid in enumerate(sids):
        score = scores[i]
        if score is not None:
            scored_sids.append((sid, float(score)))

    # Sort by score descending
    scored_sids.sort(key=lambda x: x[1], reverse=True)

    grouped = request.args.get("group") == "container" and not is_pool
    children_of = {}
    if grouped:
        scored_sids, children_of = _group_by_container(
            collection, algo, md5, scored_sids, r
        )

    total = len(scored_sids)

    # Paginate
    paged = scored_sids[offset : offset + limit]

    if not paged:
        return {"total": total, "results": [], "offset": offset, "limit": limit}

    # Fetch docs -- the page's own rows, plus the child rows folded under them.
    child_sids = [s for sid, _ in paged for s, _ in children_of.get(sid, ())]
    pipe = r.pipeline(transaction=False)
    for sid, _ in paged:
        pipe.get(sid)
    for sid in child_sids:
        pipe.get(sid)
    docs_res = pipe.execute()

    def _load(res):
        if not res:
            return None
        doc = json.loads(res) if not isinstance(res, dict) else res
        if isinstance(doc, str):
            doc = json.loads(doc)
        return doc

    child_docs = {}
    for sid, res in zip(child_sids, docs_res[len(paged) :]):
        doc = _load(res)
        if doc:
            child_docs[sid] = doc

    results = []
    for (sid, _score), res in zip(paged, docs_res[: len(paged)]):
        doc = _load(res)
        if not doc:
            continue
        if grouped:
            kids = [
                child_docs[s] for s, _ in children_of.get(sid, ()) if s in child_docs
            ]
            if kids:
                doc["children"] = kids
        results.append(doc)

    out = {"total": total, "offset": offset, "limit": limit, "results": results}
    if grouped:
        out["grouped"] = True
    return out


def reindex_bin_sim():
    """Trigger background job to rebuild secondary indexes for existing bin_sim docs.
    Pass pool_id to reindex a pool (enables fast pool search) instead of a collection.
    """
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    pool_id = data.get("pool_id") or data.get("pool")

    payload = {"collection": collection, "algo": algo}
    if pool_id:
        payload["pool_id"] = pool_id

    job_id = job_service.create_job(
        JobType.REINDEX_BIN_SIM.value,
        payload,
    )
    job_service.enqueue_job(job_id)
    return {
        "status": "success",
        "job_id": job_id,
        "message": "Bin sim reindex job enqueued",
    }
