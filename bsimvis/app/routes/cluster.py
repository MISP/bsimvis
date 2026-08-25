import json
import logging
import time
from flask import request
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.index_service import get_pool_id

job_service = JobService()


def build_cluster():
    """Enqueues a clustering job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    min_cluster_size = data.get(
        "min_cluster_size", config_service.get("clustering.min_cluster_size", 2)
    )

    payload = {
        "collection": collection,
        "algo": algo,
        "min_cluster_size": min_cluster_size,
        "min_samples": data.get(
            "min_samples", config_service.get("clustering.min_samples", 1)
        ),
        "epsilon": data.get("epsilon", config_service.get("clustering.epsilon", 0.1)),
        "selection_method": data.get(
            "selection_method", config_service.get("clustering.selection_method", "eom")
        ),
        "min_sim": data.get("min_sim", config_service.get("clustering.min_sim", 0.0)),
        "min_features": data.get(
            "min_features", config_service.get("clustering.min_features", 0)
        ),
    }

    job_id = job_service.create_job(JobType.CLUSTER_FUNCTIONS, payload)
    return {"job_id": job_id, "status": "enqueued"}


def rebuild_cluster():
    """Submits a clear + cluster-functions-only pipeline to the collection's
    lane (no bin_sim rebuild -- use rebuild_all_pipeline for that)."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    priority = str(data.get("priority", "")).lower() == "high"

    tasks = build_rebuild_all_tasks(collection, algo, skip_sim=True, data=data)
    pipeline_id = job_service.submit_to_lane(collection, tasks, priority=priority)
    return {"job_id": pipeline_id, "pipeline_id": pipeline_id, "status": "queued"}


def build_rebuild_all_tasks(collection, algo, skip_sim=False, data=None):
    """The one definition of 'what a full collection rebuild looks like':
    clear clusters -> [clear/rebuild bin_sim] -> cluster functions ->
    [build bin_sim -> cluster binaries -> index sim]. Shared by
    rebuild_all_pipeline and JobService.seal_wave's automatic
    clustering-after-batch, so there's a single source of truth."""
    data = data or {}

    tasks = [(JobType.CLEAR_CLUSTER, {"collection": collection, "algo": algo})]
    if not skip_sim:
        tasks.append((JobType.CLEAR_BIN_SIM, {"collection": collection, "algo": algo}))
        tasks.append(
            (JobType.CLEAR_BIN_CLUSTER, {"collection": collection, "algo": algo})
        )

    tasks.append(
        (
            JobType.CLUSTER_FUNCTIONS,
            {
                "collection": collection,
                "algo": algo,
                "min_cluster_size": data.get(
                    "min_cluster_size",
                    config_service.get("clustering.min_cluster_size", 2),
                ),
                "min_samples": data.get(
                    "min_samples", config_service.get("clustering.min_samples", 1)
                ),
                "epsilon": data.get(
                    "epsilon", config_service.get("clustering.epsilon", 0.1)
                ),
                "selection_method": data.get(
                    "selection_method",
                    config_service.get("clustering.selection_method", "eom"),
                ),
                "min_sim": data.get(
                    "min_sim", config_service.get("clustering.min_sim", 0.0)
                ),
                "min_features": data.get(
                    "min_features", config_service.get("clustering.min_features", 0)
                ),
            },
        )
    )

    if not skip_sim:
        tasks.append(
            (
                JobType.BUILD_BIN_SIM,
                {
                    "collection": collection,
                    "algo": algo,
                    "min_cohesion": data.get("min_cohesion", 0.5),
                },
            )
        )
        tasks.append(
            (
                JobType.CLUSTER_BINARIES,
                {
                    "collection": collection,
                    "algo": algo,
                    "min_cluster_size": data.get(
                        "min_cluster_size",
                        config_service.get("clustering.min_cluster_size", 2),
                    ),
                    "min_samples": data.get(
                        "min_samples", config_service.get("clustering.min_samples", 1)
                    ),
                    "epsilon": data.get(
                        "epsilon", config_service.get("clustering.epsilon", 0.1)
                    ),
                    "selection_method": data.get(
                        "selection_method",
                        config_service.get("clustering.selection_method", "eom"),
                    ),
                    "min_sim": data.get(
                        "min_sim", config_service.get("clustering.min_sim", 0.0)
                    ),
                    "min_cohesion": data.get(
                        "min_cohesion",
                        config_service.get("clustering.min_cohesion", 0.5),
                    ),
                },
            )
        )
        tasks.append((JobType.INDEX_SIM, {"collection": collection, "algo": algo}))

    return tasks


def rebuild_all_pipeline():
    """Submits a full re-analysis pipeline to the collection's lane: clear
    clusters -> clear bin_sim -> cluster functions -> build bin_sim ->
    cluster binaries -> index sim. Queues behind whatever's already active
    for this collection instead of racing it."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    priority = str(data.get("priority", "")).lower() == "high"

    tasks = build_rebuild_all_tasks(collection, algo, skip_sim=False, data=data)
    pipeline_id = job_service.submit_to_lane(collection, tasks, priority=priority)
    return {"job_id": pipeline_id, "pipeline_id": pipeline_id, "status": "queued"}


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


def _get_matching_ids(r, collection, level, field, val):
    # ponytail: if collection is a pool (global:pool:{pool_id}), query the registries of all collections in the pool instead.
    if collection.startswith("global:pool:"):
        pool_id = collection.split(":")[2]
        col_list = [
            c.decode() if isinstance(c, bytes) else str(c)
            for c in r.smembers(f"global:pool:{pool_id}:collections_list")
        ]
        matching_ids = set()
        for col in col_list:
            matching_ids.update(_get_matching_ids(r, col, level, field, val))
        return matching_ids

    reg_key = f"{collection}:reg:{level}:{field}"
    matching_ids = set()
    val_lower = val.lower().strip()
    if not val_lower:
        return matching_ids

    matching_buckets = []
    try:
        for bucket in r.sscan_iter(reg_key, match=f"*{val_lower}*", count=1000):
            bucket_str = bucket.decode() if isinstance(bucket, bytes) else str(bucket)
            if val_lower in bucket_str.lower():
                matching_buckets.append(bucket_str)
    except Exception as e:
        import logging

        logging.warning(f"SSCAN failed for registry {reg_key}: {e}")

    if matching_buckets:
        pipe = r.pipeline(transaction=False)
        for bucket in matching_buckets:
            # ponytail: registry already stores the full bucket_key
            pipe.smembers(bucket)
        results = pipe.execute()
        for res in results:
            if res:
                matching_ids.update(
                    m.decode() if isinstance(m, bytes) else str(m) for m in res
                )
    return matching_ids


def list_clusters():
    """Lists discovered clusters with metadata, filtering, and sorting."""
    t_start = time.perf_counter()
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")

    # Filtering
    format_arg = request.args.get("format")
    q = request.args.get("q", "").lower().strip()
    cluster_id_q = request.args.get("cluster_id", "").lower()
    cluster_uuid_q = request.args.get("cluster_uuid", "").lower()
    cluster_name_q = request.args.get("cluster_name", "").lower()

    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)

    # Column-specific function member filters
    func_name_q = request.args.get("func_name", "").strip()
    func_addr_q = request.args.get("func_addr", "").strip()
    file_name_q = request.args.get("file_name", "").strip()

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
        show_children = request.args.get("show_children", "false").lower() == "true"
        show_members = request.args.get("show_members", "false").lower() == "true"
    except ValueError:
        return {"error": "Invalid numeric parameter"}, 400

    sort_by = request.args.get(
        "sort_by", "count"
    )  # count, stability, features, cohesion
    sort_order = request.args.get("sort_order", "desc").lower()

    r = get_redis()

    pool_id = request.args.get("pool") or get_pool_id(collection)
    is_pool = pool_id is not None

    if is_pool:
        # ponytail: standardise collection name for pool
        collection = f"global:pool:{pool_id}"

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
            if is_pool:
                prefix = f"global:pool:{pool_id}:cluster:{algo}:"
            else:
                prefix = f"{collection}:cluster:{algo}:"
            cids_to_add = [k[len(prefix) : -len(":meta")] for k in all_meta_keys]
            if cids_to_add:
                r.sadd(cluster_list_key, *cids_to_add)

    # 2. Fetch all metadata
    results = []
    total = 0

    # Fetch tree links to provide parent information
    links_key = f"{collection}:cluster:tree_links:{algo}"
    links_raw = r.get(links_key)
    child_to_parent = {}
    parent_to_children = {}
    if links_raw:
        try:
            links = json.loads(links_raw)
            child_to_parent = {str(l["child"]): str(l["parent"]) for l in links}
            for l in links:
                p = str(l["parent"])
                if p not in parent_to_children:
                    parent_to_children[p] = []
                parent_to_children[p].append(str(l["child"]))
        except Exception:
            pass

    if all_meta_keys:
        t_fetch = time.perf_counter()
        pipe = r.pipeline(transaction=False)
        for k in all_meta_keys:
            pipe.get(k)
        raw_metas = pipe.execute()
        logging.info(
            f"CLUSTERS | smembers+fetch {len(all_meta_keys)} metas: {time.perf_counter()-t_fetch:.3f}s"
        )

        meta_map = {}
        for meta in raw_metas:
            if not meta:
                continue
            m = json.loads(meta) if not isinstance(meta, dict) else meta
            if isinstance(m, str):
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

        # Filter valid_nodes by function member criteria if specified
        has_member_filters = bool(func_name_q or func_addr_q or file_name_q)
        if has_member_filters and valid_nodes:
            matched_fids = None
            first_filter = True

            if func_name_q:
                fids = _get_matching_ids(
                    r, collection, "func", "function_name", func_name_q
                )
                if first_filter:
                    matched_fids = fids
                    first_filter = False
                else:
                    matched_fids.intersection_update(fids)

            if func_addr_q:
                fids = _get_matching_ids(
                    r, collection, "func", "entrypoint_address", func_addr_q
                )
                if first_filter:
                    matched_fids = fids
                    first_filter = False
                else:
                    matched_fids.intersection_update(fids)

            if file_name_q:
                fids = _get_matching_ids(
                    r, collection, "func", "file_name", file_name_q
                )
                if first_filter:
                    matched_fids = fids
                    first_filter = False
                else:
                    matched_fids.intersection_update(fids)

            if not matched_fids:
                valid_nodes = set()
            else:
                # ponytail: use the inverse index (function -> clusters) to avoid overfetching
                c_pipe = r.pipeline(transaction=False)
                matched_fids_list = list(matched_fids)
                is_pool = collection.startswith("global:pool:")
                for fid in matched_fids_list:
                    if is_pool:
                        c_pipe.smembers(f"{collection}:{fid}:clusters")
                    else:
                        c_pipe.smembers(f"{fid}:clusters")
                associated_clusters_raw = c_pipe.execute()

                associated_clusters = set()
                for cluster_res in associated_clusters_raw:
                    if cluster_res:
                        associated_clusters.update(
                            c.decode() if isinstance(c, bytes) else str(c)
                            for c in cluster_res
                        )
                valid_nodes.intersection_update(associated_clusters)

        # Paginate matched nodes BEFORE expanding
        matched_results = []
        for cid in valid_nodes:
            m = meta_map.get(cid)
            if not m:
                m = {"cluster_id": cid}
            matched_results.append(m)

        reverse = sort_order == "desc"
        if sort_by == "stability":
            matched_results.sort(
                key=lambda x: x.get("avg_stability", 0.0), reverse=reverse
            )
        elif sort_by == "count":
            matched_results.sort(
                key=lambda x: x.get("member_count", 0), reverse=reverse
            )
        elif sort_by == "features":
            matched_results.sort(
                key=lambda x: x.get("avg_features", 0), reverse=reverse
            )
        elif sort_by == "cohesion":
            matched_results.sort(
                key=lambda x: x.get("cohesion_score", 0), reverse=reverse
            )
        else:
            matched_results.sort(
                key=lambda x: str(x.get("cluster_id", "")), reverse=reverse
            )

        total = len(matched_results)
        page_metas = matched_results[offset : offset + limit]

        page_nodes = set(str(m.get("cluster_id")) for m in page_metas)
        expanded_nodes = set(page_nodes)

        # Expand parents if requested
        if show_parents:
            for node in page_nodes:
                curr = node
                while curr in child_to_parent:
                    curr = child_to_parent[curr]
                    expanded_nodes.add(curr)

        # Expand children if requested
        if show_children:
            queue = list(page_nodes)
            while queue:
                curr = queue.pop(0)
                children = parent_to_children.get(curr, [])
                for child in children:
                    if child not in expanded_nodes:
                        expanded_nodes.add(child)
                        queue.append(child)

        for cid in expanded_nodes:
            m = meta_map.get(cid)
            if not m:
                if not show_parents and not show_children:
                    continue
                m = {"cluster_id": cid}
            raw_samples = m.get("sample_members") or m.get("sample_functions") or []
            sample_members = []
            for s in raw_samples:
                if is_pool:
                    fid = s.get("function_id") or s.get("id") or ""
                    parts = fid.split(":")
                    original_col = parts[0] if parts else pool_id
                    sample_members.append(
                        {
                            "bsim_features_count": s.get("bsim_features_count", 0),
                            "collection": original_col,
                            "entrypoint_address": s.get("entrypoint_address"),
                            "file_md5": s.get("file_md5"),
                            "function_id": fid,
                            "function_name": s.get("function_name", "Unknown"),
                        }
                    )
                else:
                    sample_members.append(s)

            direct_members = m.get("direct_members", [])

            cluster_result = {
                "cluster_id": m.get("cluster_id"),
                "cluster_uuid": m.get("cluster_uuid"),
                "cluster_name": m.get("cluster_name"),
                "avg_stability": m.get("avg_stability", 0.0),
                "avg_features": m.get("avg_features", 0),
                "cohesion_score": m.get("cohesion_score", 0),
                "count": m.get("member_count"),
                "created_at": m.get("created_at"),
                "parent": child_to_parent.get(str(m.get("cluster_id"))),
                "sample_members": sample_members,
                "direct_members": direct_members,
            }
            results.append(cluster_result)

    # 3. Sorting expanded results
    reverse = sort_order == "desc"
    if sort_by == "stability":
        results.sort(key=lambda x: x.get("avg_stability", 0.0), reverse=reverse)
    elif sort_by == "count":
        results.sort(key=lambda x: x.get("count") or 0, reverse=reverse)
    elif sort_by == "features":
        results.sort(key=lambda x: x.get("avg_features", 0), reverse=reverse)
    elif sort_by == "cohesion":
        results.sort(key=lambda x: x.get("cohesion_score", 0), reverse=reverse)
    else:
        results.sort(key=lambda x: str(x.get("cluster_id", "")), reverse=reverse)

    page = results

    # 4. Fetch direct members for ONLY the clusters in the current page
    if show_members and page:
        p_pipe = r.pipeline(transaction=False)
        page_cids = [str(c["cluster_id"]) for c in page]
        db_collection = f"global:pool:{pool_id}" if is_pool else collection
        for cid in page_cids:
            p_pipe.smembers(f"{db_collection}:cluster:{algo}:{cid}:direct_members")
        direct_members_ids_list = p_pipe.execute()

        all_member_ids = set()
        cluster_to_member_ids = {}
        for cid, ids_raw in zip(page_cids, direct_members_ids_list):
            if ids_raw:
                ids = [x.decode() if isinstance(x, bytes) else x for x in ids_raw]
                cluster_to_member_ids[cid] = ids
                all_member_ids.update(ids)

        member_meta_map = {}
        if all_member_ids:
            all_member_ids_list = list(all_member_ids)
            m_pipe = r.pipeline(transaction=False)
            for mid in all_member_ids_list:
                m_pipe.get(f"{mid}:meta")
            raw_metas = m_pipe.execute()
            for mid, meta in zip(all_member_ids_list, raw_metas):
                m = (
                    json.loads(meta)
                    if meta and not isinstance(meta, dict)
                    else (meta or {})
                )
                if isinstance(m, str):
                    try:
                        m = json.loads(m)
                    except Exception:
                        m = {}
                member_meta_map[mid] = m

        for cluster_res in page:
            cid = str(cluster_res["cluster_id"])
            mids = cluster_to_member_ids.get(cid, [])
            cluster_res["direct_members"] = [
                {
                    "id": mid,
                    "name": member_meta_map.get(mid, {}).get(
                        "function_name", "Unknown"
                    ),
                    "addr": member_meta_map.get(mid, {}).get("entrypoint_address"),
                    "bin": member_meta_map.get(mid, {}).get("file_name"),
                    "file_md5": member_meta_map.get(mid, {}).get("file_md5"),
                    "v_size": member_meta_map.get(mid, {}).get("bsim_features_count"),
                }
                for mid in mids
            ]

    logging.info(
        f"CLUSTERS | total={total} | TOTAL: {time.perf_counter()-t_start:.3f}s"
    )
    response_data = {
        "collection": collection,
        "algo": algo,
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": page,
    }
    if format_arg == "csv":
        from bsimvis.app.services.export_service import export_to_csv

        return export_to_csv(page, "clusters")
    elif format_arg == "json":
        from bsimvis.app.services.export_service import export_to_json

        return export_to_json(response_data, "clusters")
    else:
        return response_data


def get_cluster_tree():
    """Returns the condensed tree for the clustering."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")

    pool_id = request.args.get("pool") or get_pool_id(collection)
    is_pool = pool_id is not None

    r = get_redis()
    if is_pool:
        tree_key = f"global:pool:{pool_id}:cluster:tree:{algo}"
    else:
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

    pool_id = data.get("pool") or get_pool_id(collection)
    is_pool = pool_id is not None

    if not cluster_id or not cluster_name:
        return {"error": "cluster_id and cluster_name required"}, 400

    r = get_redis()
    if is_pool:
        meta_key = f"global:pool:{pool_id}:cluster:{algo}:{cluster_id}:meta"
        collection_for_tags = f"global:pool:{pool_id}"
    else:
        meta_key = f"{collection}:cluster:{algo}:{cluster_id}:meta"
        collection_for_tags = collection

    if not r.exists(meta_key):
        return {"error": "Cluster meta not found"}, 404

    meta_val = r.get(meta_key)
    if meta_val:
        meta_doc = json.loads(meta_val)
        meta_doc["cluster_name"] = cluster_name
        r.set(meta_key, json.dumps(meta_doc))

    # Propagate name to all member functions for filtering
    from bsimvis.app.services.index_service import _index_tag, _unindex_tag

    if is_pool:
        members_key = f"global:pool:{pool_id}:cluster:{algo}:{cluster_id}:members"
    else:
        members_key = f"{collection}:cluster:{algo}:{cluster_id}:members"
    members = r.smembers(members_key)

    # Get old name to unindex
    old_meta = json.loads(meta_val) if meta_val else {}
    old_name = old_meta.get("cluster_name")

    # Fetch all members' metadata
    mid_list = [m.decode() if isinstance(m, bytes) else str(m) for m in members]
    m_pipe = r.pipeline(transaction=False)
    for mid_str in mid_list:
        m_pipe.get(f"{mid_str}:meta")
    member_metas = m_pipe.execute()

    pipe = r.pipeline(transaction=False)
    for mid_str, raw_m in zip(mid_list, member_metas):
        m = json.loads(raw_m) if raw_m else {}
        if old_name:
            _unindex_tag(
                pipe, collection_for_tags, "func", "cluster_name", old_name, mid_str
            )
        _index_tag(
            pipe, collection_for_tags, "func", "cluster_name", cluster_name, mid_str
        )
        m["cluster_name"] = cluster_name
        pipe.set(f"{mid_str}:meta", json.dumps(m))
    pipe.execute()

    return {"status": "success", "cluster_name": cluster_name}


def list_cluster_members():
    """Lists all function IDs in a specific cluster."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    cluster_id = request.args.get("cluster_id")
    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)

    pool_id = request.args.get("pool") or get_pool_id(collection)
    is_pool = pool_id is not None

    if not cluster_id:
        return {"error": "cluster_id required"}, 400

    r = get_redis()
    if is_pool:
        cluster_set_key = f"global:pool:{pool_id}:cluster:{algo}:{cluster_id}:members"
    else:
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
    pipe = r.pipeline(transaction=False)
    for mid in page:
        pipe.get(f"{mid}:meta")
    raw_metas = pipe.execute()

    for i, meta in enumerate(raw_metas):
        m = json.loads(meta) if meta and not isinstance(meta, dict) else (meta or {})
        results.append({"id": page[i], "meta": m})

    return {
        "cluster_id": cluster_id,
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": results,
    }


def get_cluster_functions():
    """Returns a quick sample of function metadata for a given cluster_uuid."""
    collection = request.args.get("collection")
    cluster_uuid = request.args.get("cluster_uuid")
    algo = request.args.get("algo", "unweighted_cosine")

    pool_id = request.args.get("pool") or get_pool_id(collection)
    is_pool = pool_id is not None

    if not collection and not pool_id:
        return {"error": "collection or pool required"}, 400
    if not cluster_uuid:
        return {"error": "cluster_uuid required"}, 400

    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)

    r = get_redis()
    fids_raw = None

    if is_pool:
        collection = f"global:pool:{pool_id}"

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
                pipe = r.pipeline(transaction=False)
                for k in keys:
                    pipe.get(k)
                uuids = pipe.execute()
                for k, u_res in zip(keys, uuids):
                    u = ""
                    if u_res:
                        try:
                            u_doc = json.loads(u_res)
                            u = u_doc.get("cluster_uuid", "")
                        except:
                            u = ""
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
    pipe = r.pipeline(transaction=False)
    for fid in page:
        pipe.get(f"{fid}:meta")
    raw_metas = pipe.execute()

    functions = []
    for fid, meta in zip(page, raw_metas):
        m = json.loads(meta) if meta and not isinstance(meta, dict) else (meta or {})
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
