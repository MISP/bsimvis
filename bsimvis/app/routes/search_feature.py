import json
import logging

from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import parse_timestamp

search_feature_bp = Blueprint("search_feature", __name__)


def _scan_feature_keys(r, collection, feature_prefix, offset, limit, sort_by):
    """Scans or ZRanges features based on sort criteria."""
    if sort_by == "tf":
        zset_key = f"idx:{collection}:features:by_tf"
        if feature_prefix:
            cursor = 0
            all_matches = []
            while True:
                cursor, matches = r.zscan(
                    zset_key, cursor=cursor, match=f"{feature_prefix}*", count=1000
                )
                all_matches.extend(matches)
                if cursor == 0 or len(all_matches) > 5000:
                    break
            all_matches.sort(key=lambda x: x[1], reverse=True)
            page = all_matches[offset : offset + limit]
            return [
                {
                    "hash": h,
                    "tf_score": s,
                    "frequency": r.zcard(f"idx:{collection}:feature:{h}:functions"),
                }
                for h, s in page
            ], len(all_matches)
        else:
            total = r.zcard(zset_key)
            page = r.zrevrange(zset_key, offset, offset + limit - 1, withscores=True)
            return [
                {
                    "hash": h,
                    "tf_score": s,
                    "frequency": r.zcard(f"idx:{collection}:feature:{h}:functions"),
                }
                for h, s in page
            ], total
    else:
        match_pattern = f"idx:{collection}:feature:{feature_prefix}*:functions"
        feature_list = []
        cursor = 0
        total_found = 0
        current_idx = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=match_pattern, count=1000)
            for key in keys:
                if current_idx >= offset and len(feature_list) < limit:
                    parts = key.split(":")
                    if len(parts) >= 4:
                        fh = parts[3]
                        feature_list.append({"hash": fh, "frequency": r.zcard(key)})
                current_idx += 1
                total_found += 1
            if cursor == 0 or (
                len(feature_list) >= limit and total_found > offset + limit + 1000
            ):
                break

        if feature_list:
            zset_key = f"idx:{collection}:features:by_tf"
            pipe = r.pipeline()
            for f in feature_list:
                pipe.zscore(zset_key, f["hash"])
            scores = pipe.execute()
            for i, f in enumerate(feature_list):
                f["tf_score"] = scores[i] if scores[i] is not None else 0

        return feature_list, total_found


def _enrich_feature_context(r, collection, feature_list):
    """Enriches feature list with Pcode and C-code context."""
    if not feature_list:
        return feature_list

    try:
        pipe = r.pipeline()
        for f in feature_list:
            pipe.execute_command("HVALS", f"idx:{collection}:feature:{f['hash']}:meta")
        first_metas_raw = pipe.execute()

        first_metas = []
        for res in first_metas_raw:
            if res and isinstance(res, list) and len(res) > 0:
                first_metas.append([json.loads(res[0])])
            else:
                first_metas.append([])

        pipe = r.pipeline()
        for i, meta_pkg in enumerate(first_metas):
            fm = meta_pkg[0] if meta_pkg else None
            f = feature_list[i]
            if fm:
                parts = fm.get("function_id", "").split(":")
                f["context"] = {
                    "type": fm.get("type", "N/A"),
                    "op": fm.get("pcode_op", "N/A"),
                    "pcode_full": fm.get("pcode_op_full"),
                    "func_id": fm.get("function_id"),
                    "seq": fm.get("seq"),
                    "line_idxs": fm.get("line_idx", []),
                    "md5": parts[2] if len(parts) >= 3 else "N/A",
                    "addr": parts[3] if len(parts) >= 4 else "N/A",
                    "name": fm.get(
                        "function_name", parts[3] if len(parts) >= 4 else "N/A"
                    ),
                    "c_code": None,
                }
                func_id = fm.get("function_id")
                if func_id and f["context"]["line_idxs"]:
                    pipe.json().get(f"{func_id}:source", "$")
                    f["_line_idx"] = f["context"]["line_idxs"][0]
                else:
                    pipe.execute_command("ECHO", "no_source")

                if not f["context"]["pcode_full"] and func_id:
                    pipe.json().get(f"{func_id}:vec:meta", "$")
                else:
                    pipe.execute_command("ECHO", "no_meta_fallback")
            else:
                f["context"] = {
                    "c_code": None,
                    "pcode_full": "N/A",
                    "type": "N/A",
                    "op": "N/A",
                }
                pipe.execute_command("ECHO", "no_fm_source")
                pipe.execute_command("ECHO", "no_fm_meta")

        second_results = pipe.execute()
        for i, f in enumerate(feature_list):
            if "context" not in f:
                continue
            source_data = second_results[i * 2]
            if isinstance(source_data, list) and source_data:
                source_data = source_data[0]

            vec_meta = second_results[i * 2 + 1]
            if isinstance(vec_meta, list) and vec_meta:
                vec_meta = vec_meta[0]

            if source_data and isinstance(source_data, dict) and "_line_idx" in f:
                target_line = int(f["_line_idx"])
                try:
                    c_tokens = source_data.get("c_tokens", [])
                    line_tokens = [
                        t for t in c_tokens if int(t.get("line", -1)) == target_line
                    ]
                    if line_tokens:
                        f["context"]["c_code"] = [
                            {
                                "type": t.get("type"),
                                "text": str(t.get("t", t.get("text", ""))),
                            }
                            for t in line_tokens
                        ]
                except Exception as e:
                    logging.error(
                        f"Error extracting tokens for feature {f['hash']}: {e}"
                    )

            if (
                vec_meta
                and isinstance(vec_meta, list)
                and not f["context"]["pcode_full"]
            ):
                for feat in vec_meta:
                    if feat.get("hash") == f["hash"]:
                        f["context"]["pcode_full"] = feat.get("pcode_op_full", "N/A")
                        break

            if not f["context"].get("pcode_full"):
                f["context"]["pcode_full"] = "N/A"
            f.pop("_line_idx", None)

    except Exception as e:
        logging.error(f"Error in feature context enrichment: {e}")
        for f in feature_list:
            if "context" not in f:
                f["context"] = {
                    "c_code": None,
                    "pcode_full": "N/A",
                    "type": "N/A",
                    "op": "N/A",
                }

    return feature_list


@search_feature_bp.route("/api/feature/search")
def search_features():
    r = get_redis()
    collection = request.args.get("collection")
    if not collection:
        return jsonify({"error": "No collection specified"}), 400

    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", 20))
    except ValueError:
        return jsonify({"error": "offset and limit must be integers"}), 400

    feature_prefix = request.args.get("hash", "")
    sort_by = request.args.get("sort", "default")

    feature_list, total_found = _scan_feature_keys(
        r, collection, feature_prefix, offset, limit, sort_by
    )
    feature_list = _enrich_feature_context(r, collection, feature_list)

    for f in feature_list:
        if "feature_id" not in f:
            f["feature_id"] = f"idx:{collection}:feature:{f['hash']}"

    return jsonify(
        {
            "total": total_found,
            "offset": offset,
            "limit": limit,
            "features": feature_list,
        }
    )


@search_feature_bp.route("/api/feature/details/<f_hash>")
def get_feature_details(f_hash):
    r = get_redis()
    collection = request.args.get("collection")
    if not collection:
        return jsonify({"error": "No collection specified"}), 400

    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", 1000))
    except ValueError:
        return jsonify({"error": "offset and limit must be integers"}), 400

    func_ids = r.zrange(f"idx:{collection}:feature:{f_hash}:functions", 0, -1)
    raw_meta_vals = r.hvals(f"idx:{collection}:feature:{f_hash}:meta")
    meta_data = []
    if raw_meta_vals:
        for v in raw_meta_vals:
            m = json.loads(v)
            if "entry_date" in m:
                m["entry_date"] = parse_timestamp(m["entry_date"])
            meta_data.append(m)

    total_occurrences = len(meta_data)
    paginated_meta = meta_data[offset : offset + limit]

    # Augment missing fields
    pipe = r.pipeline()
    augment_indices = []
    for i, occ in enumerate(paginated_meta):
        if (
            "pcode_op_full" not in occ
            or "tf" not in occ
            or "pcode_block" not in occ
            or "seq" not in occ
        ):
            func_id = occ.get("function_id")
            if func_id:
                pipe.json().get(f"{func_id}:vec:meta", "$")
                pipe.zscore(f"{func_id}:vec:tf", f_hash)
                augment_indices.append(i)

    if augment_indices:
        try:
            extra_results = pipe.execute()
            for i, idx in enumerate(augment_indices):
                occ = paginated_meta[idx]
                vec_meta = extra_results[i * 2]
                if isinstance(vec_meta, list) and vec_meta and len(vec_meta) == 1:
                    vec_meta = vec_meta[0]
                tf_score = extra_results[i * 2 + 1]
                if vec_meta:
                    for feat in vec_meta:
                        if feat.get("hash") == f_hash:
                            occ["pcode_op_full"] = feat.get("pcode_op_full", "N/A")
                            occ["pcode_block"] = feat.get("pcode_block", {})
                            occ["seq"] = feat.get("seq")
                            break
                occ["tf"] = int(tf_score) if tf_score is not None else 0
        except Exception:
            pass

    for occ in paginated_meta:
        col = occ.get("collection", collection)
        md5 = occ.get("file_md5")
        addr = occ.get("entrypoint_address")
        b_uuid = occ.get("batch_uuid")
        if "function_id" not in occ and col and md5 and addr:
            occ["function_id"] = f"{col}:function:{md5}:{addr}"
        if "file_id" not in occ and col and md5:
            occ["file_id"] = f"{col}:file:{md5}"
        if "batch_id" not in occ and col and b_uuid:
            occ["batch_id"] = f"{col}:batch:{b_uuid}"

    return jsonify(
        {
            "hash": f_hash,
            "occurrence_count": len(func_ids),
            "total_occurrences": total_occurrences,
            "offset": offset,
            "limit": limit,
            "associated_functions": list(func_ids),
            "occurrences": paginated_meta,
        }
    )
