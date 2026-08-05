import logging
import math
from flask import request
from flask_restx import abort
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import normalize_tags
from bsimvis.app.services.cluster_utils import (
    pick_best_shared_cluster,
    pick_best_cluster,
)
import json

job_service = JobService()


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


def _flip_diff_sides(diff_data):
    """Mirror a stored bin_sim doc so side A becomes side B and vice versa."""
    _swap_side_keys(diff_data)
    diff = diff_data.get("diff")
    if not isinstance(diff, dict):
        return
    _swap_side_keys(diff)
    for row in diff.get("matched", []):
        _swap_side_keys(row)


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

    data_raw = r.get(sid)

    if not data_raw:
        return {
            "status": "not_found",
            "message": "Similarity not calculated for this pair",
        }, 404

    diff_data = json.loads(data_raw) if not isinstance(data_raw, dict) else data_raw

    # A pair is stored once, in whatever order it was built (canonical md5 sort for
    # collection pairs, build order for pools). Re-orient the doc to the requested
    # order so every "_a"/"_b" — file metadata, unique_to_b, func_b — describes the
    # binary the caller called A/B.
    stored_a = diff_data.get("md5_a")
    if stored_a and stored_a != req_md5_a:
        _flip_diff_sides(diff_data)
    coll_a, md5_a, coll_b, md5_b = req_coll_a, req_md5_a, req_coll_b, req_md5_b

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
                    normalize_tags(meta)
                    funcs_metadata[fid] = {
                        "name": meta.get("function_name"),
                        "return_type": meta.get("return_type"),
                        "parameters": meta.get("parameters"),
                        "namespace": meta.get("namespace"),
                        "entrypoint_address": meta.get("entrypoint_address")
                        or fid.split(":")[-1],
                        "tags": meta.get("tags", []),
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

    _enrich_diff_clusters(r, diff, coll_a, pool_id, algo)

    # Change 4: when a table is requested, filter/sort/paginate server-side and return
    # only the page (+ its function metadata). Absent `table` → full doc (back-compat).
    table = request.args.get("table")
    if table in ("matched", "unique_to_a", "unique_to_b"):
        return _page_diff(diff_data, table)

    # Change 4: compact projection for the simplified Sankey — cluster fields + the few
    # numerics its binning needs, feature counts inlined, NO names/tags/notes. Lets the
    # graph render for thousands of funcs without shipping fat rows. [[Change 4]]
    if request.args.get("view") == "sankey":
        return _sankey_summary(diff_data)

    return diff_data


def _sankey_summary(diff_data):
    diff = diff_data.get("diff", {})
    fmeta = diff_data.get("functions_metadata", {})

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


def _fnum(name):
    v = request.args.get(name)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _page_diff(diff_data, table):
    """Filter + sort + slice one diff table, returning only the requested page.
    Ports the former client-side applyFilters/sortItems (binary_similarity.js). [[Change 4]]
    """
    rows = diff_data.get("diff", {}).get(table, [])
    fmeta = diff_data.get("functions_metadata", {})

    q = (request.args.get("q") or "").strip().lower()
    cl_q = (request.args.get("cl_q") or "").strip().lower()
    note_a = (request.args.get("note_a") or "").strip().lower()
    note_b = (request.args.get("note_b") or "").strip().lower()
    note = (request.args.get("note") or "").strip().lower()
    sim_min, sim_max = _fnum("sim_min"), _fnum("sim_max")
    feat_min, feat_max = _fnum("feat_min"), _fnum("feat_max")
    rar_min, rar_max = _fnum("rar_min"), _fnum("rar_max")

    def haystack(fid):
        m = fmeta.get(fid, {})
        addr = m.get("entrypoint_address") or (fid.split(":")[-1] if fid else "")
        parts = [m.get("name"), m.get("namespace"), addr]
        parts += m.get("tags", []) + m.get("user_tags", [])
        return " ".join(str(p) for p in parts if p).lower()

    def owners_match(fid, needle):
        owners = fmeta.get(fid, {}).get("note_owners", []) if fid else []
        return any(needle in str(o).lower() for o in owners)

    def keep(item):
        fids = [x for x in (item.get("func_a"), item.get("func_b")) if x] or (
            [item["func_id"]] if item.get("func_id") else []
        )
        if q and not any(q in haystack(f) for f in fids):
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
        "file_metadata_a": diff_data.get("file_metadata_a"),
        "file_metadata_b": diff_data.get("file_metadata_b"),
    }


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
    total = len(scored_sids)

    # Paginate
    paged = scored_sids[offset : offset + limit]

    if not paged:
        return {"total": total, "results": [], "offset": offset, "limit": limit}

    # Fetch docs
    pipe = r.pipeline(transaction=False)
    for sid, _ in paged:
        pipe.get(sid)

    docs_res = pipe.execute()

    results = []
    for i, res in enumerate(docs_res):
        if res:
            doc = json.loads(res) if not isinstance(res, dict) else res
            if isinstance(doc, str):
                doc = json.loads(doc)
            results.append(doc)

    return {"total": total, "offset": offset, "limit": limit, "results": results}


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
