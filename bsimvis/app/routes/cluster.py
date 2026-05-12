from flask import Blueprint, jsonify, request
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.redis_client import get_redis

cluster_bp = Blueprint("cluster", __name__)
job_service = JobService()


@cluster_bp.route("/api/cluster/build", methods=["POST"])
def build_cluster():
    """Enqueues a clustering job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    min_cluster_size = data.get("min_cluster_size", 5)

    payload = {
        "collection": collection,
        "algo": algo,
        "min_cluster_size": min_cluster_size,
        "min_samples": data.get("min_samples"),
        "epsilon": data.get("epsilon", 0.0),
        "selection_method": data.get("selection_method", "eom"),
        "min_sim": data.get("min_sim", 0.0),
    }

    job_id = job_service.create_job(JobType.CLUSTER_FUNCTIONS, payload)
    return jsonify({"job_id": job_id, "status": "enqueued"})


@cluster_bp.route("/api/cluster/rebuild", methods=["POST"])
def rebuild_cluster():
    """Enqueues a clear + cluster pipeline."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    min_cluster_size = data.get("min_cluster_size", 5)

    tasks = [
        (
            JobType.CLEAR_CLUSTER,
            {
                "collection": collection,
                "algo": algo,
            },
        ),
        (
            JobType.CLUSTER_FUNCTIONS,
            {
                "collection": collection,
                "algo": algo,
                "min_cluster_size": min_cluster_size,
                "min_samples": data.get("min_samples"),
                "epsilon": data.get("epsilon", 0.0),
                "selection_method": data.get("selection_method", "eom"),
                "min_sim": data.get("min_sim", 0.0),
            },
        ),
    ]

    pipeline_id = job_service.create_pipeline(tasks)
    return jsonify(
        {"job_id": pipeline_id, "pipeline_id": pipeline_id, "status": "enqueued"}
    )


@cluster_bp.route("/api/cluster/clear", methods=["POST"])
def clear_cluster():
    """Enqueues a cluster clear job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")

    job_id = job_service.create_job(
        JobType.CLEAR_CLUSTER,
        {"collection": collection, "algo": algo},
    )
    return jsonify({"job_id": job_id, "status": "enqueued"})


@cluster_bp.route("/api/cluster/list", methods=["GET"])
def list_clusters():
    """Lists discovered clusters with metadata, filtering, and sorting."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")

    # Filtering
    cluster_id_q = request.args.get("cluster_id", "").lower()
    cluster_uuid_q = request.args.get("cluster_uuid", "").lower()
    cluster_name_q = request.args.get("cluster_name", "").lower()

    try:
        min_stability = float(request.args.get("min_stability", 0))
        min_count = int(request.args.get("min_count", 0))
        min_features = float(request.args.get("min_features", 0))
        min_cohesion = float(request.args.get("min_cohesion", 0))
    except ValueError:
        return jsonify({"error": "Invalid numeric parameter"}), 400

    sort_by = request.args.get(
        "sort_by", "count"
    )  # count, stability, features, cohesion
    sort_order = request.args.get("sort_order", "desc").lower()

    r = get_redis()

    # 1. Discover all cluster meta keys
    pattern = f"{collection}:cluster:{algo}:*:meta"
    cursor = 0
    all_meta_keys = []

    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
        all_meta_keys.extend([k.decode() if isinstance(k, bytes) else k for k in keys])
        if cursor == 0:
            break

    # 2. Fetch all metadata
    results = []
    if all_meta_keys:
        pipe = r.pipeline()
        for k in all_meta_keys:
            pipe.json().get(k, "$")
        raw_metas = pipe.execute()

        for meta in raw_metas:
            if not meta:
                continue
            m = meta[0] if isinstance(meta, list) else meta
            if isinstance(m, str):
                import json

                m = json.loads(m)

            # Apply filters
            cid = str(m.get("cluster_id", ""))
            cuuid = str(m.get("cluster_uuid", ""))

            if cluster_id_q and cluster_id_q not in cid.lower():
                continue
            if cluster_uuid_q and cluster_uuid_q not in cuuid.lower():
                continue
            if (
                cluster_name_q
                and cluster_name_q not in m.get("cluster_name", "").lower()
            ):
                continue
            if m.get("avg_stability", 0) < min_stability:
                continue
            if m.get("member_count", 0) < min_count:
                continue
            if m.get("avg_features", 0) < min_features:
                continue
            if m.get("cohesion_score", 0) < min_cohesion:
                continue

            results.append(
                {
                    "cluster_id": m.get("cluster_id"),
                    "cluster_uuid": m.get("cluster_uuid"),
                    "cluster_name": m.get("cluster_name"),
                    "avg_stability": m.get("avg_stability"),
                    "avg_features": m.get("avg_features", 0),
                    "cohesion_score": m.get("cohesion_score", 0),
                    "count": m.get("member_count"),
                    "created_at": m.get("created_at"),
                }
            )

    # 3. Sorting
    reverse = sort_order == "desc"
    if sort_by == "stability":
        results.sort(key=lambda x: x["avg_stability"], reverse=reverse)
    elif sort_by == "count":
        results.sort(key=lambda x: x["count"], reverse=reverse)
    elif sort_by == "features":
        results.sort(key=lambda x: x.get("avg_features", 0), reverse=reverse)
    elif sort_by == "cohesion":
        results.sort(key=lambda x: x.get("cohesion_score", 0), reverse=reverse)
    else:
        results.sort(key=lambda x: str(x["cluster_id"]), reverse=reverse)

    return jsonify(
        {
            "collection": collection,
            "algo": algo,
            "total": len(results),
            "results": results,
        }
    )


@cluster_bp.route("/api/cluster/tree", methods=["GET"])
def get_cluster_tree():
    """Returns the condensed tree for the clustering."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")

    r = get_redis()
    tree_key = f"{collection}:cluster:tree:{algo}"
    tree_data = r.get(tree_key)

    if not tree_data:
        return jsonify({"error": "No tree found"}), 404

    return tree_data  # Already JSON string from ClusterService


@cluster_bp.route("/api/cluster/meta", methods=["POST"])
def update_cluster_meta():
    """Updates metadata for a cluster (e.g. rename)."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    cluster_id = data.get("cluster_id")
    cluster_name = data.get("cluster_name")

    if not cluster_id or not cluster_name:
        return jsonify({"error": "cluster_id and cluster_name required"}), 400

    r = get_redis()
    meta_key = f"{collection}:cluster:{algo}:{cluster_id}:meta"

    if not r.exists(meta_key):
        return jsonify({"error": "Cluster meta not found"}), 404

    r.json().set(meta_key, "$.cluster_name", cluster_name)

    # Propagate name to all member functions for filtering
    from bsimvis.app.services.index_service import _index_tag, _unindex_tag

    members_key = f"{collection}:cluster:{algo}:{cluster_id}:members"
    members = r.smembers(members_key)

    # Get old name to unindex
    old_meta = r.json().get(meta_key, "$")
    old_name = (
        old_meta[0].get("cluster_name")
        if old_meta and isinstance(old_meta, list)
        else None
    )

    pipe = r.pipeline()
    for mid in members:
        mid_str = mid.decode() if isinstance(mid, bytes) else mid
        if old_name:
            _unindex_tag(pipe, collection, "func", "cluster_name", old_name, mid_str)
        _index_tag(pipe, collection, "func", "cluster_name", cluster_name, mid_str)
        # Also update the function's metadata JSON for consistency
        pipe.json().set(f"{mid_str}:meta", "$.cluster_name", cluster_name)
    pipe.execute()

    return jsonify({"status": "success", "cluster_name": cluster_name})


@cluster_bp.route("/api/cluster/members", methods=["GET"])
def list_cluster_members():
    """Lists all function IDs in a specific cluster."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    cluster_id = request.args.get("cluster_id")
    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)

    if not cluster_id:
        return jsonify({"error": "cluster_id required"}), 400

    r = get_redis()
    cluster_set_key = f"{collection}:cluster:{algo}:{cluster_id}:members"

    total = r.scard(cluster_set_key)
    # Sets don't support offset/limit natively, so we fetch and slice or use SRANDMEMBER
    # For a deterministic list, we'd need to sort, but let's just grab a chunk
    members_raw = r.smembers(cluster_set_key)
    members = [m.decode() if isinstance(m, bytes) else m for m in members_raw]
    members.sort()

    page = members[offset : offset + limit]

    # Enrich with metadata if requested
    results = []
    pipe = r.pipeline()
    for mid in page:
        pipe.json().get(f"{mid}:meta", "$")
    raw_metas = pipe.execute()

    for i, meta in enumerate(raw_metas):
        m = meta[0] if isinstance(meta, list) and meta else {}
        results.append({"id": page[i], "meta": m})

    return jsonify({"cluster_id": cluster_id, "total": total, "results": results})
