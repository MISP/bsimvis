from flask import request
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.redis_client import get_redis

job_service = JobService()


def build_cluster():
    """Enqueues a clustering job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    min_cluster_size = data.get("min_cluster_size", 2)

    payload = {
        "collection": collection,
        "algo": algo,
        "min_cluster_size": min_cluster_size,
        "min_samples": data.get("min_samples", 1),
        "epsilon": data.get("epsilon", 0.0),
        "selection_method": data.get("selection_method", "eom"),
        "min_sim": data.get("min_sim", 0.0),
        "min_features": data.get("min_features", 0),
    }

    job_id = job_service.create_job(JobType.CLUSTER_FUNCTIONS, payload)
    return {"job_id": job_id, "status": "enqueued"}


def rebuild_cluster():
    """Enqueues a clear + cluster pipeline."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    min_cluster_size = data.get("min_cluster_size", 2)

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
                "min_samples": data.get("min_samples", 1),
                "epsilon": data.get("epsilon", 0.0),
                "selection_method": data.get("selection_method", "eom"),
                "min_sim": data.get("min_sim", 0.0),
                "min_features": data.get("min_features", 0),
            },
        ),
    ]

    pipeline_id = job_service.create_pipeline(tasks)
    return {"job_id": pipeline_id, "pipeline_id": pipeline_id, "status": "enqueued"}


def rebuild_all_pipeline():
    """Enqueues a full re-analysis pipeline: clear clusters -> clear bin_sim -> cluster functions -> build bin_sim."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")

    tasks = [
        (JobType.CLEAR_CLUSTER, {"collection": collection, "algo": algo}),
        (JobType.CLEAR_BIN_SIM, {"collection": collection, "algo": algo}),
        (
            JobType.CLUSTER_FUNCTIONS,
            {
                "collection": collection,
                "algo": algo,
                "min_cluster_size": data.get("min_cluster_size", 2),
                "min_samples": data.get("min_samples", 1),
                "epsilon": data.get("epsilon", 0.0),
                "selection_method": data.get("selection_method", "eom"),
                "min_sim": data.get("min_sim", 0.0),
                "min_features": data.get("min_features", 0),
            },
        ),
        (
            JobType.BUILD_BIN_SIM,
            {
                "collection": collection,
                "algo": algo,
                "min_cohesion": data.get("min_cohesion", 0.5),
            },
        ),
    ]

    pipeline_id = job_service.create_pipeline(tasks)
    return {"job_id": pipeline_id, "pipeline_id": pipeline_id, "status": "enqueued"}


def clear_cluster():
    """Enqueues a cluster clear job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")

    job_id = job_service.create_job(
        JobType.CLEAR_CLUSTER,
        {"collection": collection, "algo": algo},
    )
    return {"job_id": job_id, "status": "enqueued"}


def list_clusters():
    """Lists discovered clusters with metadata, filtering, and sorting."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")

    # Filtering
    format_arg = request.args.get("format")
    q = request.args.get("q", "").lower().strip()
    cluster_id_q = request.args.get("cluster_id", "").lower()
    cluster_uuid_q = request.args.get("cluster_uuid", "").lower()
    cluster_name_q = request.args.get("cluster_name", "").lower()

    try:
        min_stability = float(request.args.get("min_stability", 0))
        max_stability = float(request.args.get("max_stability", 0))
        min_count = int(request.args.get("min_count", 0))
        max_count = int(request.args.get("max_count", 0))
        min_features = float(request.args.get("min_features", 0))
        max_features = float(request.args.get("max_features", 0))
        min_cohesion = float(request.args.get("min_cohesion", 0))
        max_cohesion = float(request.args.get("max_cohesion", 0))
        show_parents = request.args.get("show_parents", "false").lower() == "true"
    except ValueError:
        return {"error": "Invalid numeric parameter"}, 400

    sort_by = request.args.get(
        "sort_by", "count"
    )  # count, stability, features, cohesion
    sort_order = request.args.get("sort_order", "desc").lower()

    r = get_redis()

    cluster_list_key = f"{collection}:cluster:list:{algo}"
    cids_raw = r.smembers(cluster_list_key)
    all_meta_keys = []

    if cids_raw:
        all_meta_keys = [
            f"{collection}:cluster:{algo}:{cid.decode() if isinstance(cid, bytes) else cid}:meta"
            for cid in cids_raw
        ]
    else:
        # 1. Fallback to discover all cluster meta keys
        pattern = f"{collection}:cluster:{algo}:*:meta"
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            all_meta_keys.extend(
                [k.decode() if isinstance(k, bytes) else k for k in keys]
            )
            if cursor == 0:
                break

        # Populate the set for future fast lookups
        if all_meta_keys:
            cids_to_add = [
                k[len(f"{collection}:cluster:{algo}:") : -len(":meta")]
                for k in all_meta_keys
            ]
            if cids_to_add:
                r.sadd(cluster_list_key, *cids_to_add)

    # 2. Fetch all metadata
    results = []

    # Fetch tree links to provide parent information
    links_key = f"{collection}:cluster:tree_links:{algo}"
    links_raw = r.get(links_key)
    child_to_parent = {}
    if links_raw:
        import json
        try:
            links = json.loads(links_raw)
            child_to_parent = {str(l["child"]): str(l["parent"]) for l in links}
        except Exception:
            pass

    if all_meta_keys:
        pipe = r.pipeline()
        for k in all_meta_keys:
            pipe.json().get(k, "$")
        raw_metas = pipe.execute()

        meta_map = {}
        for meta in raw_metas:
            if not meta:
                continue
            m = meta[0] if isinstance(meta, list) else meta
            if isinstance(m, str):
                import json
                m = json.loads(m)
            cid = str(m.get("cluster_id", ""))
            meta_map[cid] = m

        valid_nodes = set()
        for cid, m in meta_map.items():
            cuuid = str(m.get("cluster_uuid", ""))
            cname = str(m.get("cluster_name", ""))

            # Global keyword search
            if q:
                keywords = [k for k in q.split() if k]
                match = True
                for kw in keywords:
                    if not any(kw in v.lower() for v in [cid, cuuid, cname]):
                        match = False
                        break
                if not match:
                    continue

            if cluster_id_q and cluster_id_q not in cid.lower():
                continue
            if cluster_uuid_q and cluster_uuid_q not in cuuid.lower():
                continue
            if cluster_name_q and cluster_name_q not in cname.lower():
                continue
            if min_stability > 0 and m.get("avg_stability", 0) < min_stability:
                continue
            if max_stability > 0 and m.get("avg_stability", 0) > max_stability:
                continue
            if min_count > 0 and m.get("member_count", 0) < min_count:
                continue
            if max_count > 0 and m.get("member_count", 0) > max_count:
                continue
            if min_features > 0 and m.get("avg_features", 0) < min_features:
                continue
            if max_features > 0 and m.get("avg_features", 0) > max_features:
                continue
            if min_cohesion > 0 and m.get("cohesion_score", 0) < min_cohesion:
                continue
            if max_cohesion > 0 and m.get("cohesion_score", 0) > max_cohesion:
                continue

            valid_nodes.add(cid)

        # Expand parents if requested
        if show_parents:
            expanded_nodes = set(valid_nodes)
            for node in valid_nodes:
                curr = node
                while curr in child_to_parent:
                    curr = child_to_parent[curr]
                    expanded_nodes.add(curr)
            valid_nodes = expanded_nodes

        for cid in valid_nodes:
            m = meta_map.get(cid)
            if not m:
                if not show_parents:
                    continue
                m = {"cluster_id": cid}
            results.append(
                {
                    "cluster_id": m.get("cluster_id"),
                    "cluster_uuid": m.get("cluster_uuid"),
                    "cluster_name": m.get("cluster_name"),
                    "avg_stability": m.get("avg_stability", 0.0),
                    "avg_features": m.get("avg_features", 0),
                    "cohesion_score": m.get("cohesion_score", 0),
                    "count": m.get("member_count"),
                    "created_at": m.get("created_at"),
                    "parent": child_to_parent.get(str(m.get("cluster_id"))),
                    "snippet": m.get("snippet", ""),
                    "sample_members": m.get("sample_members", []),
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

    response_data = {
        "collection": collection,
        "algo": algo,
        "total": len(results),
        "results": results,
    }
    if format_arg == "csv":
        from bsimvis.app.services.export_service import export_to_csv

        return export_to_csv(results, "clusters")
    elif format_arg == "json":
        from bsimvis.app.services.export_service import export_to_json

        return export_to_json(response_data, "clusters")
    else:
        return response_data


def get_cluster_tree():
    """Returns the condensed tree for the clustering."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")

    r = get_redis()
    tree_key = f"{collection}:cluster:tree:{algo}"
    tree_data = r.get(tree_key)

    if not tree_data:
        return {"error": "No tree found"}, 404

    import json

    return json.loads(tree_data)


def update_cluster_meta():
    """Updates metadata for a cluster (e.g. rename)."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    cluster_id = data.get("cluster_id")
    cluster_name = data.get("cluster_name")

    if not cluster_id or not cluster_name:
        return {"error": "cluster_id and cluster_name required"}, 400

    r = get_redis()
    meta_key = f"{collection}:cluster:{algo}:{cluster_id}:meta"

    if not r.exists(meta_key):
        return {"error": "Cluster meta not found"}, 404

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

    return {"status": "success", "cluster_name": cluster_name}


def list_cluster_members():
    """Lists all function IDs in a specific cluster."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    cluster_id = request.args.get("cluster_id")
    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)

    if not cluster_id:
        return {"error": "cluster_id required"}, 400

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

    return {"cluster_id": cluster_id, "total": total, "results": results}


def get_cluster_functions():
    """Returns a quick sample of function metadata for a given cluster_uuid."""
    collection = request.args.get("collection")
    cluster_uuid = request.args.get("cluster_uuid")
    algo = request.args.get("algo", "unweighted_cosine")
    if not collection or not cluster_uuid:
        return {"error": "collection and cluster_uuid required"}, 400

    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)

    r = get_redis()
    # Resolve the cluster_uuid by reading the Redis index set directly
    bucket_key = f"{collection}:idx:func:cluster_uuid:{cluster_uuid.lower()}"
    fids_raw = r.smembers(bucket_key)

    # Fallback to scanning metadata if the index is not populated
    if not fids_raw:
        pattern = f"{collection}:cluster:{algo}:*:meta"
        cursor = 0
        matching_cluster_id = None
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                pipe = r.pipeline()
                for k in keys:
                    pipe.json().get(k, "$.cluster_uuid")
                uuids = pipe.execute()
                for k, u_res in zip(keys, uuids):
                    u = u_res[0] if isinstance(u_res, list) and u_res else u_res
                    if isinstance(u, bytes):
                        u = u.decode()
                    if u == cluster_uuid:
                        k_str = k.decode() if isinstance(k, bytes) else k
                        parts = k_str.split(":")
                        if len(parts) >= 4:
                            matching_cluster_id = parts[3]
                            break
            if matching_cluster_id or cursor == 0:
                break

        if matching_cluster_id:
            cluster_set_key = (
                f"{collection}:cluster:{algo}:{matching_cluster_id}:members"
            )
            fids_raw = r.smembers(cluster_set_key)

    if not fids_raw:
        return {"functions": [], "total": 0}

    fids = [fid.decode() if isinstance(fid, bytes) else fid for fid in fids_raw]
    fids.sort()

    total = len(fids)
    page = fids[offset : offset + limit]

    # Bulk fetch function metadata
    pipe = r.pipeline()
    for fid in page:
        pipe.json().get(f"{fid}:meta", "$")
    raw_metas = pipe.execute()

    import json

    functions = []
    for fid, meta in zip(page, raw_metas):
        m = meta[0] if isinstance(meta, list) and meta else meta
        if isinstance(m, str):
            m = json.loads(m)
        if not m:
            m = {}

        functions.append(
            {
                "function_id": m.get("function_id") or fid,
                "function_name": m.get("function_name", "Unknown"),
                "parameters": m.get("parameters", []),
                "return_type": m.get("return_type", "void"),
                "namespace": m.get("namespace", ""),
                "entrypoint_address": m.get("entrypoint_address", "0x0"),
                "bsim_features_count": m.get("bsim_features_count", 0),
            }
        )

    return {
        "functions": functions,
        "total": total,
        "offset": offset,
        "limit": limit,
        "collection": collection,
        "cluster_uuid": cluster_uuid,
    }


