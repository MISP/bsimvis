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
            (JobType.CLUSTER_BINARIES.value, {"collection": collection, "algo": algo}),
        ]
    )
    return {
        "status": "success",
        "pipeline_id": pipeline_id,
        "message": "Binary similarity rebuild pipeline enqueued",
    }


def get_bin_sim():
    """Retrieve binary similarity diff for a pair."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    md5_a = request.args.get("md5_a")
    md5_b = request.args.get("md5_b")

    if not md5_a or not md5_b:
        abort(400, "Both md5_a and md5_b are required")

    r = get_redis()

    # ensure a < b
    if md5_a > md5_b:
        md5_a, md5_b = md5_b, md5_a

    sid = f"{collection}:bin_sim:{algo}:{md5_a}::{md5_b}"
    data = r.json().get(sid, "$")

    if not data:
        return {
            "status": "not_found",
            "message": "Similarity not calculated for this pair",
        }, 404

    diff_data = data[0] if isinstance(data, list) else data

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

    # Retrieve metadata from pipeline
    fids = list(fids)
    if fids:
        pipe = r.pipeline()
        for fid in fids:
            pipe.json().get(f"{fid}:meta", "$")
        meta_results = pipe.execute()

        funcs_metadata = {}
        for fid, raw_meta in zip(fids, meta_results):
            if raw_meta:
                meta = raw_meta[0] if isinstance(raw_meta, list) else raw_meta
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

    # To efficiently list, we check involves set
    involves_key = f"{collection}:bin_sim:involves:{md5}"
    sids = list(r.smembers(involves_key))

    if not sids:
        return {"total": 0, "results": [], "offset": offset, "limit": limit}

    sids = [sid.decode() if isinstance(sid, bytes) else sid for sid in sids]

    # We should get scores for these from the zset
    # Actually, we can just ZSCORE the zset to sort them
    zset_key = f"{collection}:bin_sim:score:{algo}"

    scored_sids = []
    pipe = r.pipeline()
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
    pipe = r.pipeline()
    for sid, _ in paged:
        pipe.json().get(sid, "$")

    docs_res = pipe.execute()

    results = []
    for i, res in enumerate(docs_res):
        if res:
            doc = res[0] if isinstance(res, list) else res
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
