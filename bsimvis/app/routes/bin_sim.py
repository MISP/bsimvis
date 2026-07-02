import logging
from flask import request
from flask_restx import abort
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.redis_client import get_redis
import json

job_service = JobService()


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

    # Resolve actual coll_a/coll_b from the stored doc for metadata lookups
    if pool_id:
        coll_a = diff_data.get("coll_1") or coll_a
        coll_b = diff_data.get("coll_2") or coll_b
        md5_a = diff_data.get("md5_1") or md5_a
        md5_b = diff_data.get("md5_2") or md5_b

        # Pool docs store clusters in `matched_clusters` with no `diff` wrapper.
        # Normalize into the same shape the renderer expects.
        if "diff" not in diff_data and "matched_clusters" in diff_data:
            diff_data["diff"] = {
                "matched": diff_data["matched_clusters"],
                "unique_to_a": [],
                "unique_to_b": [],
            }

    # Extract all unique function IDs
    fids = set()
    diff = diff_data.get("diff", {})
    for m in diff.get("matched", []):
        fids.update(m.get("funcs_a", []))
        fids.update(m.get("funcs_b", []))
    for u in diff.get("unique_to_a", []):
        fids.update(u.get("funcs", []))
    for u in diff.get("unique_to_b", []):
        fids.update(u.get("funcs", []))

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
                    funcs_metadata[fid] = {
                        "name": meta.get("function_name"),
                        "return_type": meta.get("return_type"),
                        "parameters": meta.get("parameters"),
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
                "bsim_features_count": 0,
            }
        diff_data["functions_metadata"] = funcs_metadata
    else:
        diff_data["functions_metadata"] = {}

    return diff_data


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
    """Trigger background job to rebuild secondary indexes for existing bin_sim docs."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")

    job_id = job_service.create_job(
        JobType.REINDEX_BIN_SIM.value,
        {"collection": collection, "algo": algo},
    )
    job_service.enqueue_job(job_id)
    return {
        "status": "success",
        "job_id": job_id,
        "message": "Bin sim reindex job enqueued",
    }
