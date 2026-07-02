from flask import request
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.similarity_service import SimilarityService
from bsimvis.app.services.milvus_service import milvus_service
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import normalize_tags

job_service = JobService()
similarity_service = SimilarityService()


def list_similarities():
    """Lists similarities (scores) for a given md5 or batch_uuid."""
    collection = request.args.get("collection", "main")
    md5 = request.args.get("md5")
    batch_uuid = request.args.get("batch")
    algo = request.args.get("algo", "unweighted_cosine")
    limit = request.args.get("limit", 20, type=int)
    offset = request.args.get("offset", 0, type=int)

    r = get_redis()

    # Standardized Involves Index: col}:sim:involves:{lvl}:{id}
    index_key = None
    if md5:
        # File level involves
        index_key = f"{collection}:sim:involves:file:{md5}"
    elif batch_uuid:
        # Batch involves (currently mapped to all similarities involving ANY function in the batch)
        # For listing, we might just want to check similarity build status primarily.
        index_key = f"{collection}:batch:{batch_uuid}:functions"  # This is just funcs

    if not index_key:
        return {"error": "md5 or batch parameter required"}, 400

    # Get total and slice
    total = r.scard(index_key)
    # Since it's a set, we don't have a native 'slice' but we can get all or use SSCAN
    # For a list with limit/offset, we'll fetch members and slice in Python
    # (Note: In a large system, SSCAN would be better, but for this app SMEMBERS is fine)
    all_sim_keys = list(r.smembers(index_key))
    sim_keys = all_sim_keys[offset : offset + limit]

    results = []
    if sim_keys:
        pipe = r.pipeline(transaction=False)
        for k in sim_keys:
            pipe.get(k)
        raw_docs = pipe.execute()

        for i, doc in enumerate(raw_docs):
            if doc:
                d = json.loads(doc) if not isinstance(doc, dict) else doc
                sid_key = sim_keys[i]
                d["sid"] = sid_key.decode() if isinstance(sid_key, bytes) else sid_key
                normalize_tags(d)
                results.append(d)

    return {"total": total, "offset": offset, "limit": limit, "results": results}


def similarity_status():
    """Returns build status (counts) for a target."""
    collection = request.args.get("collection", "main")
    md5 = request.args.get("md5")
    batch_uuid = request.args.get("batch")
    algo = request.args.get("algo", "unweighted_cosine")

    status = similarity_service.get_build_status(
        collection, batch_uuid=batch_uuid, md5=md5, algo=algo
    )
    return status


def list_batches():
    """Returns detailed build status (for batches or files)."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    by_type = request.args.get("by", "batch")

    if by_type == "md5":
        results = similarity_service.list_files_build_status(collection, algo=algo)
    else:
        results = similarity_service.list_batches_build_status(collection, algo=algo)
    return {"results": results}


def build_similarity():
    """Enqueues a similarity build job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    md5 = data.get("md5")
    batch_uuid = data.get("batch")

    from bsimvis.app.services.config_service import config_service

    algo = data.get("algo")
    if algo is None:
        algo = config_service.get("similarity.algo", "unweighted_cosine")

    if not md5 and not batch_uuid and not data.get("all"):
        return {"error": "md5, batch, or all required"}, 400

    min_score = data.get("min_score")
    if min_score is None:
        min_score = config_service.get("similarity.min_score", 0.95)

    top_k = data.get("top_k")
    if top_k is None:
        top_k = config_service.get("similarity.top_k", 20)

    min_features = data.get("min_features")
    if min_features is None:
        min_features = config_service.get("similarity.min_features", 0)

    payload = {
        "collection": collection,
        "md5": md5,
        "batch_uuid": batch_uuid,
        "algo": algo,
        "min_score": min_score,
        "top_k": top_k,
        "min_features": min_features,
        "all": data.get("all", False),
    }

    tasks = [
        (JobType.BUILD_SIM, payload),
        (
            JobType.INDEX_SIM,
            {
                "collection": collection,
                "algo": algo,
                "md5": md5,
                "batch_uuid": batch_uuid,
            },
        ),
    ]

    if algo in ["milvus_sparse"]:
        if not milvus_service.enabled:
            return {
                "error": "Milvus is disabled. Cannot use milvus_sparse algorithm."
            }, 400
        tasks.insert(0, (JobType.SYNC_MILVUS, {"collection": collection}))

    pipeline_id = job_service.create_pipeline(tasks)
    return {"job_id": pipeline_id, "pipeline_id": pipeline_id, "status": "enqueued"}


def rebuild_similarity():
    """Enqueues a clear + build pipeline."""
    data = request.json or {}
    collection = data.get("collection", "main")
    md5 = data.get("md5")
    batch_uuid = data.get("batch")

    from bsimvis.app.services.config_service import config_service

    algo = data.get("algo")
    if algo is None:
        algo = config_service.get("similarity.algo", "unweighted_cosine")

    if not md5 and not batch_uuid and not data.get("all"):
        return {"error": "md5, batch, or all required"}, 400

    min_score = data.get("min_score")
    if min_score is None:
        min_score = config_service.get("similarity.min_score", 0.95)

    top_k = data.get("top_k")
    if top_k is None:
        top_k = config_service.get("similarity.top_k", 20)

    min_features = data.get("min_features")
    if min_features is None:
        min_features = config_service.get("similarity.min_features", 0)

    tasks = [
        (
            JobType.CLEAR_SIM,
            {
                "collection": collection,
                "md5": md5,
                "batch_uuid": batch_uuid,
                "algo": algo,
                "all": data.get("all", False),
            },
        ),
        (
            JobType.BUILD_SIM,
            {
                "collection": collection,
                "md5": md5,
                "batch_uuid": batch_uuid,
                "algo": algo,
                "min_score": min_score,
                "top_k": top_k,
                "min_features": min_features,
                "all": data.get("all", False),
            },
        ),
        (
            JobType.INDEX_SIM,
            {
                "collection": collection,
                "algo": algo,
                "md5": md5,
                "batch_uuid": batch_uuid,
            },
        ),
    ]

    if algo in ["milvus_sparse"]:
        if not milvus_service.enabled:
            return {
                "error": "Milvus is disabled. Cannot use milvus_sparse algorithm."
            }, 400
        tasks.insert(1, (JobType.SYNC_MILVUS, {"collection": collection}))

    pipeline_id = job_service.create_pipeline(tasks)
    return {"job_id": pipeline_id, "pipeline_id": pipeline_id, "status": "enqueued"}


def clear_similarity():
    """Enqueues a similarity clear job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    md5 = data.get("md5")
    batch_uuid = data.get("batch")
    algo = data.get("algo", "unweighted_cosine")

    if not md5 and not batch_uuid:
        return {"error": "md5 or batch required"}, 400

    job_id = job_service.create_job(
        JobType.CLEAR_SIM,
        {"collection": collection, "md5": md5, "batch_uuid": batch_uuid, "algo": algo},
    )
    return {"job_id": job_id, "status": "enqueued"}


def tag_similarity():
    """Adds a tag to a similarity pair."""
    data = request.json or {}
    collection = data.get("collection", "main")
    id1 = data.get("id1")
    id2 = data.get("id2")
    algo = data.get("algo", "unweighted_cosine")
    tag = data.get("tag")

    if not id1 or not id2 or not tag:
        return {"error": "id1, id2, and tag required"}, 400

    success = similarity_service.tag_similarity(collection, id1, id2, algo, tag)
    if success:
        return {"status": "success", "message": f"Tagged similarity with {tag}"}
    else:
        return {"status": "error", "message": "Failed to tag similarity"}, 500


def untag_similarity():
    """Removes a tag from a similarity pair."""
    data = request.json or {}
    collection = data.get("collection", "main")
    id1 = data.get("id1")
    id2 = data.get("id2")
    algo = data.get("algo", "unweighted_cosine")
    tag = data.get("tag")

    if not id1 or not id2 or not tag:
        return {"error": "id1, id2, and tag required"}, 400

    success = similarity_service.untag_similarity(collection, id1, id2, algo, tag)
    if success:
        return {"status": "success", "message": f"Untagged similarity from {tag}"}
    else:
        return {"status": "error", "message": "Failed to untag similarity"}, 500
