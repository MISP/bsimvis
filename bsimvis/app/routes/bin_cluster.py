import json
import logging
import time
from flask import request
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.index_service import get_pool_id

job_service = JobService()


def build_bin_cluster():
    """Enqueues a binary clustering job."""
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
        "min_cohesion": data.get(
            "min_cohesion", config_service.get("clustering.min_cohesion", 0.5)
        ),
    }

    job_id = job_service.create_job(JobType.CLUSTER_BINARIES, payload)
    return {"job_id": job_id, "status": "enqueued"}


def rebuild_bin_cluster():
    """Enqueues a clear + cluster pipeline for binaries."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    min_cluster_size = data.get(
        "min_cluster_size", config_service.get("clustering.min_cluster_size", 2)
    )

    tasks = [
        (
            JobType.CLEAR_BIN_CLUSTER,
            {
                "collection": collection,
                "algo": algo,
            },
        ),
        (
            JobType.CLUSTER_BINARIES,
            {
                "collection": collection,
                "algo": algo,
                "min_cluster_size": min_cluster_size,
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
                    "min_cohesion", config_service.get("clustering.min_cohesion", 0.5)
                ),
            },
        ),
    ]

    pipeline_id = job_service.create_pipeline(tasks)
    return {"job_id": pipeline_id, "pipeline_id": pipeline_id, "status": "enqueued"}


def clear_bin_cluster():
    """Enqueues a binary cluster clear job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")

    job_id = job_service.create_job(
        JobType.CLEAR_BIN_CLUSTER,
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


def list_bin_clusters():
    """Lists discovered binary clusters with metadata, filtering, and sorting."""
    t_start = time.perf_counter()
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    # Containers and files cluster in two separate graphs (a container holds
    # no code of its own, so it can never share a similarity edge with a
    # file) and persist under two separate key namespaces -- see
    # BinClusterService._persist_hierarchical_binary_clusters. node_type
    # picks which one this listing reads.
    node_type = request.args.get("node_type", "file").strip().lower()
    algo = f"{algo}:container" if node_type == "container" else algo

    # Filtering
    format_arg = request.args.get("format")
    q = request.args.get("q", "").lower().strip()
    cluster_id_q = request.args.get("cluster_id", "").lower()
    cluster_uuid_q = request.args.get("cluster_uuid", "").lower()
    cluster_name_q = request.args.get("cluster_name", "").lower()

    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)

    # Column-specific file member filters
    file_name_q = request.args.get("file_name", "").strip()
    file_md5_q = request.args.get("file_md5", "").strip()

    try:
        min_stability = float(request.args.get("min_stability") or 0)
        max_stability = float(request.args.get("max_stability") or 0)
        min_count = int(request.args.get("min_count") or 0)
        max_count = int(request.args.get("max_count") or 0)
        min_cohesion = float(request.args.get("min_cohesion") or 0)
        max_cohesion = float(request.args.get("max_cohesion") or 0)
        show_parents = request.args.get("show_parents", "false").lower() == "true"
        show_children = request.args.get("show_children", "false").lower() == "true"
        show_members = request.args.get("show_members", "false").lower() == "true"
    except ValueError:
        return {"error": "Invalid numeric parameter"}, 400

    sort_by = request.args.get("sort_by", "count")  # count, stability, cohesion
    sort_order = request.args.get("sort_order", "desc").lower()

    r = get_redis()

    pool_id = request.args.get("pool") or get_pool_id(collection)
    is_pool = pool_id is not None

    if is_pool:
        collection = f"global:pool:{pool_id}"
        cluster_list_key = f"global:pool:{pool_id}:bin_cluster:list"
    else:
        cluster_list_key = f"{collection}:bin_cluster:list:{algo}"

    cids_raw = r.smembers(cluster_list_key)
    all_meta_keys = []

    if cids_raw:
        if is_pool:
            all_meta_keys = [
                f"global:pool:{pool_id}:bin_cluster:{cid.decode() if isinstance(cid, bytes) else cid}:meta"
                for cid in cids_raw
            ]
        else:
            all_meta_keys = [
                f"{collection}:bin_cluster:{algo}:{cid.decode() if isinstance(cid, bytes) else cid}:meta"
                for cid in cids_raw
            ]
    else:
        if is_pool:
            pattern = f"global:pool:{pool_id}:bin_cluster:*:meta"
        else:
            pattern = f"{collection}:bin_cluster:{algo}:*:meta"
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            all_meta_keys.extend(
                [k.decode() if isinstance(k, bytes) else k for k in keys]
            )
            if cursor == 0:
                break

        if all_meta_keys:
            prefix = (
                f"global:pool:{pool_id}:bin_cluster:"
                if is_pool
                else f"{collection}:bin_cluster:{algo}:"
            )
            cids_to_add = [k[len(prefix) : -len(":meta")] for k in all_meta_keys]
            if cids_to_add:
                r.sadd(cluster_list_key, *cids_to_add)

    child_to_parent = {}
    parent_to_children = {}
    if is_pool:
        links_key = f"global:pool:{pool_id}:bin_cluster:tree_links:{algo}"
    else:
        links_key = f"{collection}:bin_cluster:tree_links:{algo}"

    links_raw = r.get(links_key)
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

    results = []
    total = 0
    if all_meta_keys:
        t_fetch = time.perf_counter()
        pipe = r.pipeline(transaction=False)
        for k in all_meta_keys:
            pipe.get(k)
        raw_metas = pipe.execute()
        logging.info(
            f"BIN_CLUSTERS | smembers+fetch {len(all_meta_keys)} metas: {time.perf_counter()-t_fetch:.3f}s"
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

            if q:
                keywords = [k for k in q.split() if k]
                match = True
                for kw in keywords:
                    yara_values = [
                        item.get("value", "").lower()
                        for item in m.get("yara_distribution", [])
                        if item.get("value")
                    ]
                    search_targets = [cid, cuuid, cname] + yara_values
                    if not any(kw in v.lower() for v in search_targets):
                        match = False
                        break
                if not match:
                    continue

            if cluster_id_q and cluster_id_q not in cid.lower():
                continue
            if cluster_uuid_q and cluster_uuid_q not in cuuid.lower():
                continue
            if cluster_name_q:
                yara_values = [
                    item.get("value", "").lower()
                    for item in m.get("yara_distribution", [])
                    if item.get("value")
                ]
                search_targets = [cname.lower()] + yara_values
                if not any(cluster_name_q in v for v in search_targets):
                    continue
            if min_stability > 0 and m.get("avg_stability", 0) < min_stability:
                continue
            if max_stability > 0 and m.get("avg_stability", 0) > max_stability:
                continue
            if min_count > 0 and m.get("member_count", 0) < min_count:
                continue
            if max_count > 0 and m.get("member_count", 0) > max_count:
                continue
            if min_cohesion > 0 and m.get("cohesion_score", 0) < min_cohesion:
                continue
            if max_cohesion > 0 and m.get("cohesion_score", 0) > max_cohesion:
                continue

            valid_nodes.add(cid)

        # Filter valid_nodes by file member criteria if specified
        has_member_filters = bool(file_name_q or file_md5_q)
        if has_member_filters and valid_nodes:
            matched_fids = None
            first_filter = True

            if file_name_q:
                fids = _get_matching_ids(
                    r, collection, "file", "file_name", file_name_q
                )
                if first_filter:
                    matched_fids = fids
                    first_filter = False
                else:
                    matched_fids.intersection_update(fids)

            if file_md5_q:
                fids = _get_matching_ids(r, collection, "file", "file_md5", file_md5_q)
                if first_filter:
                    matched_fids = fids
                    first_filter = False
                else:
                    matched_fids.intersection_update(fids)

            if not matched_fids:
                valid_nodes = set()
            else:
                # ponytail: use the inverse index (file -> clusters) to avoid overfetching
                c_pipe = r.pipeline(transaction=False)
                matched_fids_list = list(matched_fids)
                for fid in matched_fids_list:
                    parts = fid.split(":")
                    md5 = parts[-1]
                    if is_pool:
                        c_pipe.smembers(f"pool:{pool_id}:file:{md5}:bin_clusters")
                    else:
                        c_pipe.smembers(f"{fid}:bin_clusters")
                associated_clusters_raw = c_pipe.execute()

                associated_clusters = set()
                for cluster_res in associated_clusters_raw:
                    if cluster_res:
                        associated_clusters.update(
                            c.decode() if isinstance(c, bytes) else str(c)
                            for c in cluster_res
                        )
                # ponytail: pools store UUIDs in associated_clusters, while collections store raw labels. Check both.
                valid_nodes = {
                    cid
                    for cid in valid_nodes
                    if cid in associated_clusters
                    or meta_map.get(cid, {}).get("cluster_uuid") in associated_clusters
                }

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
                m = {"cluster_id": cid}
            cluster_result = {
                "cluster_id": m.get("cluster_id"),
                "cluster_uuid": m.get("cluster_uuid"),
                "cluster_name": m.get("cluster_name"),
                "is_custom_name": m.get("is_custom_name", False),
                "avg_stability": m.get("avg_stability", 0.0),
                "cohesion_score": m.get("cohesion_score", 0),
                "count": m.get("member_count"),
                "created_at": m.get("created_at"),
                "parent": child_to_parent.get(str(m.get("cluster_id"))),
                "snippet": m.get("snippet", ""),
                "sample_members": m.get("sample_members", []),
                "yara_distribution": m.get("yara_distribution", []),
                "avtype_distribution": m.get("avtype_distribution", []),
                "filetype_distribution": m.get("filetype_distribution", []),
                "ccip_distribution": m.get("ccip_distribution", []),
            }
            results.append(cluster_result)

    # Sorting expanded results
    reverse = sort_order == "desc"
    if sort_by == "stability":
        results.sort(key=lambda x: x.get("avg_stability", 0.0), reverse=reverse)
    elif sort_by == "count":
        results.sort(key=lambda x: x.get("count") or 0, reverse=reverse)
    elif sort_by == "cohesion":
        results.sort(key=lambda x: x.get("cohesion_score", 0), reverse=reverse)
    else:
        results.sort(key=lambda x: str(x.get("cluster_id", "")), reverse=reverse)

    page = results

    # Fetch direct members for ONLY the clusters in the current page
    if show_members and page:
        p_pipe = r.pipeline(transaction=False)
        for c in page:
            if is_pool:
                cuuid = str(c["cluster_uuid"])
                p_pipe.smembers(f"global:pool:{pool_id}:bin_cluster:{cuuid}:members")
            else:
                cid = str(c["cluster_id"])
                p_pipe.smembers(f"{collection}:bin_cluster:{algo}:{cid}:direct_members")
        direct_members_ids_list = p_pipe.execute()

        all_member_ids = set()
        cluster_to_member_ids = {}
        page_cids = [str(c["cluster_id"]) for c in page]
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
                parts = mid.split(":")
                md5 = parts[-1] if len(parts) >= 3 else ""
                actual_col = parts[0]
                m_pipe.scard(f"{actual_col}:idx:file:functions:{md5}")
            raw_results = m_pipe.execute()
            for idx, mid in enumerate(all_member_ids_list):
                meta = raw_results[2 * idx]
                func_count = raw_results[2 * idx + 1]
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
                m["function_count"] = func_count
                member_meta_map[mid] = m

        for cluster_res in page:
            cid = str(cluster_res["cluster_id"])
            mids = cluster_to_member_ids.get(cid, [])
            cluster_res["direct_members"] = [
                {
                    "id": mid,
                    "name": member_meta_map.get(mid, {}).get("file_name", "Unknown"),
                    "file_md5": member_meta_map.get(mid, {}).get("file_md5", ""),
                    "language_id": member_meta_map.get(mid, {}).get(
                        "language_id", "Unknown"
                    ),
                    "function_count": member_meta_map.get(mid, {}).get(
                        "function_count", 0
                    ),
                    "tags": member_meta_map.get(mid, {}).get("tags", []),
                    "user_tags": member_meta_map.get(mid, {}).get("user_tags", []),
                    "avtype": member_meta_map.get(mid, {}).get("avtype", []),
                    "filetype": member_meta_map.get(mid, {}).get("filetype", []),
                    "yara": member_meta_map.get(mid, {}).get("yara", []),
                    "cc_ip": member_meta_map.get(mid, {}).get("cc_ip", []),
                }
                for mid in mids
            ]

    logging.info(
        f"BIN_CLUSTERS | total={total} | TOTAL: {time.perf_counter()-t_start:.3f}s"
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

        return export_to_csv(page, "bin_clusters")
    elif format_arg == "json":
        from bsimvis.app.services.export_service import export_to_json

        return export_to_json(response_data, "bin_clusters")
    else:
        return response_data


def get_bin_cluster_tree():
    """Returns the condensed tree for binary clustering."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    node_type = request.args.get("node_type", "file").strip().lower()
    algo = f"{algo}:container" if node_type == "container" else algo

    pool_id = request.args.get("pool") or get_pool_id(collection)
    is_pool = pool_id is not None

    r = get_redis()
    if is_pool:
        tree_key = f"global:pool:{pool_id}:bin_cluster:tree:{algo}"
    else:
        tree_key = f"{collection}:bin_cluster:tree:{algo}"
    tree_data = r.get(tree_key)

    if not tree_data:
        return {"error": "No tree found"}, 404

    return json.loads(tree_data)


def update_bin_cluster_meta():
    """Updates metadata for a binary cluster (e.g. rename)."""
    data = request.json or {}
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    node_type = (data.get("node_type") or "file").strip().lower()
    algo = f"{algo}:container" if node_type == "container" else algo
    cluster_id = data.get("cluster_id")
    cluster_name = data.get("cluster_name")

    pool_id = data.get("pool") or get_pool_id(collection)
    is_pool = pool_id is not None

    if not cluster_id or not cluster_name:
        return {"error": "cluster_id and cluster_name required"}, 400

    r = get_redis()
    if is_pool:
        meta_key = f"global:pool:{pool_id}:bin_cluster:{cluster_id}:meta"
        collection_for_tags = f"global:pool:{pool_id}"
    else:
        meta_key = f"{collection}:bin_cluster:{algo}:{cluster_id}:meta"
        collection_for_tags = collection

    if not r.exists(meta_key):
        return {"error": "Cluster meta not found"}, 404

    meta_val = r.get(meta_key)
    if meta_val:
        meta_doc = json.loads(meta_val)
        meta_doc["cluster_name"] = cluster_name
        meta_doc["is_custom_name"] = True
        r.set(meta_key, json.dumps(meta_doc))

    # Propagate name to all member files for filtering
    from bsimvis.app.services.index_service import _index_tag, _unindex_tag

    if is_pool:
        members_key = f"global:pool:{pool_id}:bin_cluster:{cluster_id}:members"
    else:
        members_key = f"{collection}:bin_cluster:{algo}:{cluster_id}:members"
    members = r.smembers(members_key)

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
                pipe, collection_for_tags, "file", "bin_cluster_name", old_name, mid_str
            )
        _index_tag(
            pipe, collection_for_tags, "file", "bin_cluster_name", cluster_name, mid_str
        )
        m["bin_cluster_name"] = cluster_name
        pipe.set(f"{mid_str}:meta", json.dumps(m))
    pipe.execute()

    return {"status": "success", "cluster_name": cluster_name}


def list_bin_cluster_members():
    """Lists all file IDs in a specific binary cluster."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    node_type = request.args.get("node_type", "file").strip().lower()
    algo = f"{algo}:container" if node_type == "container" else algo
    cluster_id = request.args.get("cluster_id")
    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)

    if not cluster_id:
        return {"error": "cluster_id required"}, 400

    r = get_redis()
    pool_id = request.args.get("pool") or get_pool_id(collection)
    is_pool = pool_id is not None

    if is_pool:
        cluster_set_key = f"global:pool:{pool_id}:bin_cluster:{cluster_id}:members"
    else:
        cluster_set_key = f"{collection}:bin_cluster:{algo}:{cluster_id}:members"

    total = r.scard(cluster_set_key)
    members_raw = r.smembers(cluster_set_key)
    members = [m.decode() if isinstance(m, bytes) else m for m in members_raw]
    members.sort()

    page = members[offset : offset + limit]

    results = []
    pipe = r.pipeline(transaction=False)
    for mid in page:
        # If it's a pool, the members are formatted as {coll}:{md5}
        if is_pool and ":" in mid:
            parts = mid.split(":")
            coll, md5 = parts[0], parts[1]
            pipe.get(f"{coll}:file:{md5}:meta")
        else:
            pipe.get(f"{collection}:file:{mid}:meta")
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


def get_bin_cluster_files():
    """Returns a quick sample of file metadata for a given binary cluster_uuid."""
    collection = request.args.get("collection")
    cluster_uuid = request.args.get("cluster_uuid")
    algo = request.args.get("algo", "unweighted_cosine")
    # The primary lookup below (idx:file:bin_cluster_uuid:*) needs no
    # node_type at all -- uuids are random and never collide between the
    # file and container namespaces. Only the fallback meta-scan, keyed by
    # algo, needs it.
    node_type = request.args.get("node_type", "file").strip().lower()
    algo = f"{algo}:container" if node_type == "container" else algo

    pool_id = request.args.get("pool") or get_pool_id(collection)
    is_pool = pool_id is not None

    if not collection and not pool_id:
        return {"error": "collection or pool required"}, 400
    if not cluster_uuid:
        return {"error": "cluster_uuid required"}, 400

    if is_pool:
        collection = f"global:pool:{pool_id}"

    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)

    r = get_redis()
    fids_raw = None

    if is_pool:
        cluster_set_key = f"global:pool:{pool_id}:bin_cluster:{cluster_uuid}:members"
        fids_raw = r.smembers(cluster_set_key)
    else:
        bucket_key = f"{collection}:idx:file:bin_cluster_uuid:{cluster_uuid.lower()}"
        fids_raw = r.smembers(bucket_key)

        if not fids_raw:
            pattern = f"{collection}:bin_cluster:{algo}:*:meta"
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
                    f"{collection}:bin_cluster:{algo}:{matching_cluster_id}:members"
                )
                fids_raw = r.smembers(cluster_set_key)

    if not fids_raw:
        return {"files": [], "total": 0}

    fids = [fid.decode() if isinstance(fid, bytes) else fid for fid in fids_raw]
    fids.sort()

    total = len(fids)
    page = fids[offset : offset + limit]

    pipe = r.pipeline(transaction=False)
    for fid in page:
        pipe.get(f"{fid}:meta")
        parts = fid.split(":")
        md5 = parts[-1] if len(parts) >= 3 else ""
        actual_col = parts[0]
        pipe.scard(f"{actual_col}:idx:file:functions:{md5}")
    raw_metas = pipe.execute()

    files = []
    for idx, fid in enumerate(page):
        meta = raw_metas[2 * idx]
        func_count = raw_metas[2 * idx + 1]
        m = json.loads(meta) if meta and not isinstance(meta, dict) else (meta or {})
        if isinstance(m, str):
            try:
                m = json.loads(m)
            except Exception:
                m = {}
        if not m:
            m = {}

        files.append(
            {
                "file_id": m.get("file_id") or fid,
                "file_name": m.get("file_name", "Unknown"),
                "file_md5": m.get("file_md5", ""),
                "language_id": m.get("language_id", ""),
                "architecture": m.get("architecture", ""),
                "function_count": func_count,
                "avtype": m.get("avtype", []),
                "filetype": m.get("filetype", []),
                "yara": m.get("yara", []),
                "yara_matches": m.get("yara", []),
                "ips": m.get("cc_ip", []),
                "first_seen": m.get("first_seen", []),
                "tags": m.get("tags", []),
                "user_tags": m.get("user_tags", []),
            }
        )

    return {
        "files": files,
        "total": total,
        "offset": offset,
        "limit": limit,
        "collection": collection,
        "cluster_uuid": cluster_uuid,
    }
