import json
import logging
import redis
import hashlib
import time
import uuid
from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import parse_timestamp
from bsimvis.app.services.lua_manager import lua_manager

search_function_bp = Blueprint("search_function", __name__)

DEFAULT_LIMIT = 100
DEFAULT_POOL_LIMIT = 1000000
MAX_POOL_LIMIT = 1000000


def normalize_tags(data):
    # Normalize legacy analysis tags
    tags = data.get("tags")
    if isinstance(tags, str):
        data["tags"] = [t.strip() for t in tags.split(",")] if tags else []
    elif tags is None:
        data["tags"] = []

    # Normalize new user tags
    user_tags = data.get("user_tags")
    if isinstance(user_tags, str):
        data["user_tags"] = (
            [t.strip() for t in user_tags.split(",")] if user_tags else []
        )
    elif user_tags is None:
        data["user_tags"] = []

    return data


@search_function_bp.route("/api/function/search")
def search_functions():
    t_req_start = time.perf_counter()
    col = request.args.get("collection")
    if not col:
        return jsonify({"error": "No collection specified"}), 400

    session_id = str(uuid.uuid4())[:8]

    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
        pool_limit = int(request.args.get("pool_limit", DEFAULT_POOL_LIMIT))
    except ValueError:
        return jsonify({"error": "Invalid numeric parameter"}), 400

    pool_limit = max(1, min(pool_limit, MAX_POOL_LIMIT))

    # Search parameters
    search_q = request.args.get("q", "").lower().strip()
    name_filter = (
        request.args.get("function_name", request.args.get("name", "")).lower().strip()
    )

    tag_filters = request.args.getlist("tag")
    static_tag_filters = request.args.getlist("static_tag")
    user_tag_filters = request.args.getlist("user_tag")

    lang_filter = (
        request.args.get("language_id", request.args.get("language", ""))
        .lower()
        .strip()
    )
    namespace_filter = request.args.get("namespace", "").lower().strip()
    ret_type_filter = (
        request.args.get("return_type", request.args.get("ret_type", ""))
        .lower()
        .strip()
    )
    address_filter = (
        request.args.get("entrypoint_address", request.args.get("address", ""))
        .lower()
        .strip()
    )
    md5_filter = (
        request.args.get("file_md5", request.args.get("md5", "")).lower().strip()
    )
    file_name_filter = request.args.get("file_name", "").lower().strip()

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

    def get_group_targets(lvl, val, allowed_fields=None):
        if not allowed_fields:
            if lvl == "func":
                allowed_fields = [
                    "tags",
                    "user_tags",
                    "function_name",
                    "return_type",
                    "namespace",
                    "entrypoint_address",
                    "file_name",
                    "file_md5",
                    "language_id",
                ]
            else:  # file level
                allowed_fields = ["tags", "user_tags", "file_name", "file_md5"]

        val_lower = val.lower()
        matches = []

        for field in allowed_fields:
            registry_key = f"{col}:reg:{lvl}:{field}"
            if r.exists(registry_key):
                matching_buckets = []
                try:
                    for bucket in r.sscan_iter(registry_key, match=f"*{val_lower}*"):
                        bucket_str = (
                            bucket.decode()
                            if isinstance(bucket, bytes)
                            else str(bucket)
                        )
                        if val_lower in bucket_str.lower():
                            matching_buckets.append(bucket_str)
                except Exception as e:
                    logging.warning(f"SSCAN failed for {registry_key}: {e}")

                targets = []
                if matching_buckets:
                    prefix = f"{col}:idx:{lvl}:{field}:"
                    targets = [
                        b[len(prefix) :]
                        for b in matching_buckets
                        if b.startswith(prefix)
                    ]

                if targets:
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
            normalized_subs.append(
                {
                    "level": lvl,
                    "targets": targets[:1000],
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

    filter_configs = [
        (lang_filter, "language_id", _paths("language_id")),
        (namespace_filter, "namespace", _paths("namespace")),
        (ret_type_filter, "ret_type", _paths("return_type")),
        (address_filter, "address", _paths("entrypoint_address")),
        (md5_filter, "md5", _paths("file_md5")),
        (file_name_filter, "file_name", _paths("file_name")),
        (name_filter, "name", _paths("function_name") + _paths("file_name")),
        # General tag search
        (tag_filters, "tag", _paths("tags") + _paths("user_tags")),
        (static_tag_filters, "static_tag", _paths("tags")),
        (user_tag_filters, "user_tag", _paths("user_tags")),
        # Func-scoped
        (
            func_tag_filters,
            "func_tag",
            _paths_for_source("func", "tags") + _paths_for_source("func", "user_tags"),
        ),
        (func_static_tag_filters, "func_static_tag", _paths_for_source("func", "tags")),
        (
            func_user_tag_filters,
            "func_user_tag",
            _paths_for_source("func", "user_tags"),
        ),
        # File-scoped
        (
            file_tag_filters,
            "file_tag",
            _paths_for_source("file", "tags") + _paths_for_source("file", "user_tags"),
        ),
        (file_static_tag_filters, "file_static_tag", _paths_for_source("file", "tags")),
        (
            file_user_tag_filters,
            "file_user_tag",
            _paths_for_source("file", "user_tags"),
        ),
    ]

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
                return jsonify(
                    {"total": 0, "functions": [], "offset": offset, "limit": limit}
                )

            add_group(all_matches, field_name=f"{label}:{val}")

    # Exclusions — config-driven
    exclude_configs = [
        (ex_tag_filters, "ex_tag", _paths("tags") + _paths("user_tags")),
        (ex_static_tag_filters, "ex_static_tag", _paths("tags")),
        (ex_user_tag_filters, "ex_user_tag", _paths("user_tags")),
        (
            ex_func_tag_filters,
            "ex_func_tag",
            _paths_for_source("func", "tags") + _paths_for_source("func", "user_tags"),
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
            _paths_for_source("file", "tags") + _paths_for_source("file", "user_tags"),
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
        for word in [w for w in search_q.split() if w.strip()]:
            all_matches = []
            for lvl in ["func", "file"]:
                matches = get_group_targets(lvl, word)
                if matches:
                    all_matches.extend(matches)

            if not all_matches:
                return jsonify(
                    {
                        "total": 0,
                        "functions": [],
                        "offset": offset,
                        "limit": limit,
                        "q": search_q,
                    }
                )

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
        return jsonify({"error": str(e)}), 500

    # Enrichment
    pipe = r.pipeline()
    for doc_id in doc_ids:
        pipe.json().get(f"{doc_id}:meta", "$")

    # Also fetch file-level metadata for tags
    for doc_id in doc_ids:
        try:
            # doc_id is {col}:func:{md5}:{addr}
            parts = doc_id.split(":")
            if len(parts) >= 3:
                md5 = parts[2]
                pipe.json().get(f"{col}:file:{md5}:meta", "$")
            else:
                pipe.json().get("nonexistent", "$")
        except:
            pipe.json().get("nonexistent", "$")

    raw_results_all = pipe.execute()
    raw_meta_results = raw_results_all[: len(doc_ids)]
    raw_file_results = raw_results_all[len(doc_ids) :]

    functions_list = []
    for i, (doc_id, raw) in enumerate(zip(doc_ids, raw_meta_results)):
        if not raw:
            continue
        data = raw[0] if isinstance(raw, list) and raw else raw
        if isinstance(data, str):
            data = json.loads(data)

        # File metadata
        file_raw = raw_file_results[i]
        file_data = (
            file_raw[0] if isinstance(file_raw, list) and file_raw else file_raw
        ) or {}
        if isinstance(file_data, str):
            file_data = json.loads(file_data)

        data["file_tags"] = file_data.get("tags", [])
        data["file_user_tags"] = file_data.get("user_tags", [])

        # ID construction if missing
        md5 = data.get("file_md5")
        addr = data.get("entrypoint_address")
        b_uuid = data.get("batch_uuid")

        if md5 and addr and "function_id" not in data:
            data["function_id"] = f"{col}:func:{md5}:{addr}"
        if md5 and "file_id" not in data:
            data["file_id"] = f"{col}:file:{md5}"
        if b_uuid and "batch_id" not in data:
            data["batch_id"] = f"{col}:batch:{b_uuid}"

        normalize_tags(data)

        # Enforce Unix timestamps
        for field in ["entry_date", "file_date"]:
            if field in data:
                data[field] = parse_timestamp(data[field])

        functions_list.append(data)

    total_time = time.perf_counter() - t_req_start
    logging.info(
        f"FUNC SEARCH | {session_id} | Total: {total} | Time: {total_time:.3f}s"
    )

    return jsonify(
        {
            "total": total,
            "offset": offset,
            "limit": limit,
            "pool_truncated": pool_truncated,
            "functions": functions_list,
            "collection": col,
            "q": search_q,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
    )
