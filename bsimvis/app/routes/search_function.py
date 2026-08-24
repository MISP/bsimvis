import json
import logging
import redis
import hashlib
import time
import uuid
from flask import request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import (
    parse_timestamp,
    normalize_tags,
    enrich_pool_data,
    get_pool_id,
)
from bsimvis.app.services.lua_manager import lua_manager
from bsimvis.app.services.query_syntax import resolve_targets
from bsimvis.app.services.config_service import config_service

DEFAULT_LIMIT = 100
DEFAULT_POOL_LIMIT = 1000000
MAX_POOL_LIMIT = 1000000


def _id_sorted_page(r, key, offset, limit):
    """Id-sorted page of an unordered set, matching the Lua tiebreak.

    Same shape as the unfiltered paths in search_file/search_feature: load the
    set, sort in Python, slice. SSCAN can't replace this — it gives no stable
    ordering, so paging off it makes pages overlap and skip.

    ponytail: O(N) full-set load only on the unfiltered path, as in the sibling
    routes. all_functions is the biggest of the three sets, so if any of them
    needs a maintained id index, this one goes first.
    """
    doc_ids = sorted(
        d.decode() if isinstance(d, bytes) else str(d) for d in r.smembers(key)
    )
    return doc_ids[offset : offset + limit]


def search_functions():
    try:
        t_req_start = time.perf_counter()
        pool_id = request.args.get("pool")
        col = request.args.get("collection")
        if not col and not pool_id:
            return {"error": "No collection or pool specified"}, 400

        if pool_id:
            col = f"global:pool:{pool_id}"
        else:
            pool_id = get_pool_id(col)

        session_id = str(uuid.uuid4())[:8]

        try:
            offset = int(request.args.get("offset", 0))
            limit = int(request.args.get("limit", DEFAULT_LIMIT))
            pool_limit = int(request.args.get("pool_limit", DEFAULT_POOL_LIMIT))
            min_cohesion = float(
                request.args.get(
                    "min_cohesion", config_service.get("clustering.min_cohesion", 0.5)
                )
            )
        except ValueError:
            return {"error": "Invalid numeric parameter"}, 400

        pool_limit = max(1, min(pool_limit, MAX_POOL_LIMIT))

        format_arg = request.args.get("format")
        if format_arg in ("csv", "json"):
            offset = 0
            limit = 100000

        # Search parameters
        search_q = request.args.get("q", "").lower().strip()

        tag_filters = request.args.getlist("tag")
        static_tag_filters = request.args.getlist("static_tag")
        user_tag_filters = request.args.getlist("user_tag")

        # Exclusion filters (list)
        ex_tag_filters = request.args.getlist("exclude_tag")
        ex_static_tag_filters = request.args.getlist("exclude_static_tag")
        ex_user_tag_filters = request.args.getlist("exclude_user_tag")

        func_tag_filters = request.args.getlist("func_tag")
        func_static_tag_filters = request.args.getlist("func_static_tag")
        func_user_tag_filters = request.args.getlist("func_user_tag")

        file_tag_filters = request.args.getlist("file_tag")
        file_static_tag_filters = request.args.getlist("file_static_tag")
        file_user_tag_filters = request.args.getlist("file_user_tag")

        ex_func_tag_filters = request.args.getlist("exclude_func_tag")
        ex_func_static_tag_filters = request.args.getlist("exclude_func_static_tag")
        ex_func_user_tag_filters = request.args.getlist("exclude_func_user_tag")

        ex_file_tag_filters = request.args.getlist("exclude_file_tag")
        ex_file_static_tag_filters = request.args.getlist("exclude_file_static_tag")
        ex_file_user_tag_filters = request.args.getlist("exclude_file_user_tag")

        try:
            min_features = int(request.args.get("min_features", 0))
        except (ValueError, TypeError):
            min_features = 0

        sort_by = request.args.get("sort_by", "id")
        sort_order = request.args.get("sort_order", "desc").lower()

        r = get_redis()

        # Match mode chosen per filter value, echoed in the response so a query
        # that resolved differently than the user expected is visible instead of
        # silently returning the wrong set.
        filter_modes = []
        filter_truncated = [False]

        def get_group_targets(lvl, val, allowed_fields=None, default_kind="exact"):
            from bsimvis.app.services.index_config import INDEX_CONFIG

            if not allowed_fields:
                # Dynamically discover all fields allowed at this level from config
                allowed_fields = []
                for src_lvl, fields in INDEX_CONFIG.items():
                    for field, targets in fields.items():
                        if lvl in targets:
                            from bsimvis.app.services.index_config import (
                                resolve_target_field,
                            )

                            allowed_fields.append(
                                resolve_target_field(src_lvl, lvl, field)
                            )
                # Deduplicate
                allowed_fields = list(set(allowed_fields))

            matches = []
            for field in allowed_fields:
                targets, truncated, spec = resolve_targets(
                    r, col, lvl, field, val, default_kind=default_kind
                )
                if truncated:
                    filter_truncated[0] = True
                if targets:
                    entry = spec.as_dict()
                    entry.update({"level": lvl, "field": field})
                    filter_modes.append(entry)
                    matches.append((lvl, targets, field))
            return matches

        groups_raw = []

        def add_group(sub_matches, field_name="q", exclude=False):
            if not sub_matches:
                return

            normalized_subs = []
            total_weight = 0

            for lvl, targets, field in sub_matches:
                weight = 0
                # Estimate weight
                prefix = f"{col}:idx:{lvl}:{field}:"
                for t in targets:
                    try:
                        weight += r.scard(f"{prefix}{t}")
                    except:
                        pass

                total_weight += weight
                # No slice here. Capping the bucket list silently dropped
                # documents out of both filters and exclusions; the bound now
                # lives in query_syntax.resolve_targets, which reports when it
                # is hit. The Lua side already chunks SUNION at 5000 keys.
                normalized_subs.append(
                    {
                        "level": lvl,
                        "targets": targets,
                        "field": field,
                    }
                )

            groups_raw.append(
                {
                    "type": "metadata",
                    "field": field_name,
                    "sub_groups": normalized_subs,
                    "weight": total_weight if not exclude else 99999999,
                    "exclude": exclude,
                }
            )

        # Core Filters — config-driven
        from bsimvis.app.services.index_config import (
            get_search_paths_for_field,
            INDEX_CONFIG,
            resolve_target_field,
        )

        def _paths(field):
            return get_search_paths_for_field(field, "func")

        def _paths_for_source(source_lvl, field):
            targets = INDEX_CONFIG.get(source_lvl, {}).get(field, [])
            path = []
            for lvl in ["func", "file"]:
                if lvl in targets:
                    path.append((lvl, resolve_target_field(source_lvl, lvl, field)))
            return [path] if path else []

        # Core Filters — fully config-driven
        filter_configs = []

        # 1. Native/Propagated fields from INDEX_CONFIG
        # We want to iterate over all fields that could end up at the 'func' level
        for src_lvl, fields in INDEX_CONFIG.items():
            for field, targets in fields.items():
                if "func" in targets:
                    target_field = resolve_target_field(src_lvl, "func", field)
                    # Check if this field is in request args
                    # We handle both the target name (e.g. file_tags) and common aliases (md5 -> file_md5)
                    val = request.args.get(target_field)

                    if target_field in [
                        "file_md5",
                        "parent_md5",
                        "related_md5",
                        "file_name",
                        "parent_file_name",
                        "related_file_name",
                    ]:
                        continue

                    # Alias handling (for backward compat or convenience)
                    if not val:
                        aliases = {
                            "entrypoint_address": ["address"],
                            "function_name": ["name"],
                            "language_id": ["language"],
                            "return_type": ["ret_type"],
                            "note_owners": ["note_owner"],
                        }
                        if target_field in aliases:
                            for alias in aliases[target_field]:
                                val = request.args.get(alias)
                                if val:
                                    break

                    if val:
                        paths = _paths_for_source(src_lvl, field)
                        if paths:
                            filter_configs.append((val, target_field, paths))

        # 2. Tag-specific logic (already handled above if they are in INDEX_CONFIG, but sometimes we want union)
        # The existing code had some manual additions for unions, keeping them for now if not redundant.
        # Actually, the logic below handles tag lists which are distinct from single request.args.get()
        md5_val = request.args.get("md5") or request.args.get("file_md5")
        md5_configs = []
        if md5_val:
            md5_paths = (
                _paths_for_source("file", "file_md5")
                + _paths_for_source("file", "parent_md5")
                + _paths_for_source("file", "related_md5")
            )
            md5_configs = [([md5_val], "any_md5", md5_paths)]

        file_name_val = request.args.get("file_name")
        file_name_configs = []
        if file_name_val:
            file_name_paths = (
                _paths_for_source("file", "file_name")
                + _paths_for_source("file", "parent_file_name")
                + _paths_for_source("file", "related_file_name")
            )
            file_name_configs = [([file_name_val], "any_file_name", file_name_paths)]

        tag_filter_configs = (
            md5_configs
            + file_name_configs
            + [
                (tag_filters, "tag", _paths("tags") + _paths("user_tags")),
                (static_tag_filters, "static_tag", _paths("tags")),
                (user_tag_filters, "user_tag", _paths("user_tags")),
                (
                    func_tag_filters,
                    "func_tag",
                    _paths_for_source("func", "tags")
                    + _paths_for_source("func", "user_tags"),
                ),
                (
                    func_static_tag_filters,
                    "func_static_tag",
                    _paths_for_source("func", "tags"),
                ),
                (
                    func_user_tag_filters,
                    "func_user_tag",
                    _paths_for_source("func", "user_tags"),
                ),
                (
                    file_tag_filters,
                    "file_tag",
                    _paths_for_source("file", "tags")
                    + _paths_for_source("file", "user_tags"),
                ),
                (
                    file_static_tag_filters,
                    "file_static_tag",
                    _paths_for_source("file", "tags"),
                ),
                (
                    file_user_tag_filters,
                    "file_user_tag",
                    _paths_for_source("file", "user_tags"),
                ),
            ]
        )
        for tfc in tag_filter_configs:
            if tfc[0]:
                filter_configs.append(tfc)

        for f_v, label, paths in filter_configs:
            if not f_v:
                continue
            vals = f_v if isinstance(f_v, list) else [f_v]
            for val in vals:
                if not val:
                    continue
                all_matches = []
                for path in paths:
                    for i, (lvl, physical_field) in enumerate(path):
                        matches = get_group_targets(
                            lvl, val, allowed_fields=[physical_field]
                        )
                        if matches:
                            if i > 0:
                                logging.info(
                                    f"FUNC SEARCH | {session_id} | Fallback triggered! '{physical_field}={val}' wasn't found natively at '{path[0][0]}', successfully joined via '{lvl}'."
                                )
                            all_matches.extend(matches)
                            break  # stop at first level that returns results

                if not all_matches:
                    logging.info(
                        f"FUNC SEARCH | {session_id} | Filter '{label}={val}' matched 0."
                    )
                    return {
                        "total": 0,
                        "functions": [],
                        "offset": offset,
                        "limit": limit,
                    }

                add_group(all_matches, field_name=f"{label}:{val}")

        # Exclusions — config-driven
        exclude_configs = [
            (ex_tag_filters, "ex_tag", _paths("tags") + _paths("user_tags")),
            (ex_static_tag_filters, "ex_static_tag", _paths("tags")),
            (ex_user_tag_filters, "ex_user_tag", _paths("user_tags")),
            (
                ex_func_tag_filters,
                "ex_func_tag",
                _paths_for_source("func", "tags")
                + _paths_for_source("func", "user_tags"),
            ),
            (
                ex_func_static_tag_filters,
                "ex_func_static_tag",
                _paths_for_source("func", "tags"),
            ),
            (
                ex_func_user_tag_filters,
                "ex_func_user_tag",
                _paths_for_source("func", "user_tags"),
            ),
            (
                ex_file_tag_filters,
                "ex_file_tag",
                _paths_for_source("file", "tags")
                + _paths_for_source("file", "user_tags"),
            ),
            (
                ex_file_static_tag_filters,
                "ex_file_static_tag",
                _paths_for_source("file", "tags"),
            ),
            (
                ex_file_user_tag_filters,
                "ex_file_user_tag",
                _paths_for_source("file", "user_tags"),
            ),
        ]

        for ex_v, label, paths in exclude_configs:
            if not ex_v:
                continue
            vals = ex_v if isinstance(ex_v, list) else [ex_v]
            for val in vals:
                if not val:
                    continue
                all_matches = []
                for path in paths:
                    for i, (lvl, physical_field) in enumerate(path):
                        matches = get_group_targets(
                            lvl, val, allowed_fields=[physical_field]
                        )
                        if matches:
                            if i > 0:
                                logging.info(
                                    f"FUNC SEARCH | {session_id} | Fallback triggered (Exclude)! '{physical_field}={val}' wasn't found natively at '{path[0][0]}', successfully joined via '{lvl}'."
                                )
                            all_matches.extend(matches)
                            break
                if all_matches:
                    add_group(all_matches, field_name=f"{label}:{val}", exclude=True)

        # Global search q
        if search_q:
            from bsimvis.app.services.index_config import INDEX_CONFIG

            all_levels = list(INDEX_CONFIG.keys())

            for word in [w for w in search_q.split() if w.strip()]:
                all_matches = []
                for lvl in all_levels:
                    # Free-text box: a bare word stays a substring search.
                    matches = get_group_targets(lvl, word, default_kind="substring")
                    if matches:
                        all_matches.extend(matches)

                if not all_matches:
                    return {
                        "total": 0,
                        "functions": [],
                        "offset": offset,
                        "limit": limit,
                        "q": search_q,
                    }

                add_group(all_matches, field_name=f"q({word})")

        # Numeric range filters
        if min_features > 0:
            feat_key = f"{col}:idx:func:bsim_features_count"
            weight = r.zcount(feat_key, min_features, "+inf") or 1000
            groups_raw.append(
                {
                    "type": "numeric_range",
                    "key": feat_key,
                    "min": min_features,
                    "weight": weight,
                }
            )

        if not groups_raw:
            all_key = f"{col}:all_functions"
            total = r.scard(all_key)
            pool_truncated = False

            if sort_by != "id":
                sort_key = f"{col}:idx:func:{sort_by}"
                sorted_total = r.zcard(sort_key)
                if sorted_total:
                    total = sorted_total
                    doc_ids = (
                        r.zrevrange(sort_key, offset, offset + limit - 1)
                        if sort_order == "desc"
                        else r.zrange(sort_key, offset, offset + limit - 1)
                    )
                else:
                    doc_ids = _id_sorted_page(r, all_key, offset, limit)
            else:
                doc_ids = _id_sorted_page(r, all_key, offset, limit)
        else:
            # Lua Exec
            search_script = lua_manager.get_script("search_function")
            if not search_script:
                # Fallback to manual reload if script not found (first time)
                lua_manager.register_all()
                search_script = lua_manager.get_script("search_function")

            lua_config = {
                "collection": col,
                "pool_limit": pool_limit,
                "groups": sorted(groups_raw, key=lambda x: x["weight"]),
                "offset": offset,
                "limit": limit,
                "sort_by": sort_by,
                "sort_order": sort_order,
            }

            try:
                res = search_script(keys=[], args=[json.dumps(lua_config)])
                total = res[0]
                pool_truncated = bool(res[1])
                doc_ids = res[2]
            except Exception as e:
                logging.error(f"FUNC LUA SEARCH CRASH: {e}")
                return {"error": str(e)}, 500

        # --- ENRICHMENT (Optimized & Deduplicated) ---
        t_enrich_start = time.perf_counter()

        # Phase 1: Fetch Function Metadata & Cluster Scores (Bulk)
        f_pipe = r.pipeline(transaction=False)
        for doc_id in doc_ids:
            f_pipe.get(f"{doc_id}:meta")
            if col.startswith("global:pool:"):
                f_pipe.hgetall(f"{col}:{doc_id}:cluster_scores")
            else:
                f_pipe.hgetall(f"{doc_id}:cluster_scores")

        f_results_raw = f_pipe.execute()

        f_meta_list = []  # List of {doc_id, meta, scores}
        unique_md5s = set()
        unique_cluster_ids = set()

        for i, doc_id in enumerate(doc_ids):
            m_json = f_results_raw[i * 2]
            scores_raw = f_results_raw[i * 2 + 1] or {}

            meta = (
                json.loads(m_json)
                if m_json and not isinstance(m_json, dict)
                else (m_json or {})
            )
            if isinstance(meta, str):
                meta = json.loads(meta)

            scores = {}
            for k, v in scores_raw.items():
                k_str = k.decode() if isinstance(k, bytes) else k
                scores[k_str] = float(v)
                unique_cluster_ids.add(k_str)

            f_meta_list.append({"doc_id": doc_id, "meta": meta, "scores": scores})
            if meta.get("file_md5"):
                # File docs live in the origin collection; a pool owns none, so
                # key the map by (collection, md5) taken from the function id.
                unique_md5s.add((doc_id.split(":")[0], meta["file_md5"]))

        # Phase 2: Fetch File Metadata (DEDUPLICATED)
        file_meta_map = {}
        if unique_md5s:
            file_pipe = r.pipeline(transaction=False)
            md5_list = list(unique_md5s)
            for f_coll, md5 in md5_list:
                file_pipe.get(f"{f_coll}:file:{md5}:meta")
            file_results = file_pipe.execute()
            for (f_coll, md5), res in zip(md5_list, file_results):
                fm = (
                    json.loads(res)
                    if res and not isinstance(res, dict)
                    else (res or {})
                )
                if isinstance(fm, str):
                    fm = json.loads(fm)
                file_meta_map[f"{f_coll}:{md5}"] = fm

        # Phase 3: Fetch Cluster Metadata (DEDUPLICATED), filtered by min_cohesion
        cluster_meta_map = {}
        algo = "unweighted_cosine"  # Default algo
        if unique_cluster_ids:
            c_pipe = r.pipeline(transaction=False)
            c_list = list(unique_cluster_ids)
            for cid in c_list:
                c_pipe.get(f"{col}:cluster:{algo}:{cid}:meta")
            c_results = c_pipe.execute()
            for cid, res in zip(c_list, c_results):
                cm = (
                    json.loads(res)
                    if res and not isinstance(res, dict)
                    else (res or {})
                )
                if isinstance(cm, str):
                    cm = json.loads(cm)
                # Apply cohesion threshold server-side
                if (cm.get("cohesion_score") or 0) >= min_cohesion:
                    cluster_meta_map[cid] = cm

        # Phase 4: Final Assembly
        functions_list = []
        for f_data in f_meta_list:
            doc_id = f_data["doc_id"]
            meta = f_data["meta"]
            scores = f_data["scores"]

            if not meta:
                continue

            # File tags enrichment
            md5 = meta.get("file_md5")
            file_meta = file_meta_map.get(f"{doc_id.split(':')[0]}:{md5}", {})
            if pool_id:
                enrich_pool_data(file_meta, pool_id)
            meta["file_tags"] = file_meta.get("tags", [])
            meta["file_user_tags"] = file_meta.get("user_tags", [])

            # ID construction
            addr = meta.get("entrypoint_address")
            b_uuid = meta.get("batch_uuid")
            if md5 and addr and "function_id" not in meta:
                meta["function_id"] = f"{col}:func:{md5}:{addr}"
            if md5 and "file_id" not in meta:
                meta["file_id"] = f"{col}:file:{md5}"
            if b_uuid and "batch_id" not in meta:
                meta["batch_id"] = f"{col}:batch:{b_uuid}"

            normalize_tags(meta)
            normalize_tags(meta, tag_fields=["file_tags", "file_user_tags"])

            if pool_id:
                enrich_pool_data(meta, pool_id)

            # Enforce Unix timestamps
            for field in ["entry_date", "file_date"]:
                if field in meta:
                    meta[field] = parse_timestamp(meta[field])

            # Cluster references — plain list of UUIDs (metadata is in top-level map)
            clusters = [cid for cid in scores if cid in cluster_meta_map]
            meta["clusters"] = clusters

            # Cleanup
            for field in [
                "cluster_id",
                "cluster_name",
                "cluster_uuid",
                "cluster_stability",
            ]:
                meta.pop(field, None)

            functions_list.append(meta)

        total_time = time.perf_counter() - t_req_start
        logging.info(
            f"FUNC SEARCH | {session_id} | Total: {total} | Enrich: {time.perf_counter()-t_enrich_start:.3f}s | Time: {total_time:.3f}s"
        )

        # Build the top-level cluster metadata map (keyed by UUID)
        clusters_response = {
            cid: {
                "cluster_id": cm.get("cluster_id"),
                "cluster_uuid": cm.get("cluster_uuid"),
                "cluster_name": cm.get("cluster_name"),
                "cohesion_score": cm.get("cohesion_score", 0),
                "member_count": cm.get("member_count", 0),
                "cluster_stability": cm.get("cluster_stability", 0.0),
                "avg_features": cm.get("avg_features", 0),
            }
            for cid, cm in cluster_meta_map.items()
        }

        response_data = {
            "total": total,
            "offset": offset,
            "limit": limit,
            "pool_truncated": pool_truncated,
            "filter_truncated": filter_truncated[0],
            "filters": filter_modes,
            "clusters": clusters_response,
            "functions": functions_list,
            "collection": col,
            "q": search_q,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
        if format_arg == "csv":
            from bsimvis.app.services.export_service import export_to_csv

            return export_to_csv(functions_list, "functions")
        elif format_arg == "json":
            from bsimvis.app.services.export_service import export_to_json

            return export_to_json(response_data, "functions")
        else:
            return response_data
    except Exception as e:
        logging.error(f"Error in search_functions: {e}", exc_info=True)
        return {"error": str(e)}, 500
