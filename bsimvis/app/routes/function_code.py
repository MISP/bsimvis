from flask import Blueprint, request, jsonify
from bsimvis.app.services.function_service import fetch_function_data, get_feature_map
from bsimvis.app.services.index_service import parse_timestamp
from bsimvis.app.services.redis_client import get_redis
import traceback
import json

function_code_bp = Blueprint("function_code", __name__)


def render_single_function(source, features, tf_map):
    """
    Renders the semantic tokens for a single function without any diffing logic.
    """
    f_map = get_feature_map(features)
    tokens = source.get("c_tokens", [])
    if not tokens:
        return []

    max_line = max(t["line"] for t in tokens)
    lines_dict = {i: [] for i in range(max_line + 1)}
    for idx, t in enumerate(tokens):
        lines_dict[t["line"]].append((idx, t))

    addr_map = source.get("line_to_addr", {})

    rows = []
    tips = {}

    for i in range(max_line + 1):
        line_tokens = lines_dict.get(i, [])
        tokens_json = []

        for global_idx, token in line_tokens:
            token_features = f_map.get(global_idx, [])
            hash_list = [f["hash"] for f in token_features]

            # For single view, we mark everything as "unique" if it has features
            # Or just leave it neutral. Let's use diff-unique class for consistency.
            diff_class = "diff-unique" if token_features else ""

            tip_features = []
            if token_features:
                for f in token_features:
                    tip_features.append(
                        [
                            f["hash"],
                            f.get("pcode_op"),
                            f.get("pcode_op_full"),
                            f.get("type"),
                            f.get("seq"),
                            f.get("addr"),
                            f.get("line_idx"),
                            tf_map.get(f["hash"], "N/A"),
                            "#66d9ef",  # cyan for neutral base
                        ]
                    )
                tips[global_idx] = [token.get("type"), token.get("seq"), tip_features]

            tokens_json.append(
                {
                    "type": token.get("type"),
                    "has_features": bool(token_features),
                    "diff_class": diff_class,
                    "hash_list": hash_list,
                    "global_idx": global_idx,
                    "text": token["t"],
                }
            )

        addr = addr_map.get(str(i), [""])[0] if addr_map else ""

        rows.append({"line_idx": i + 1, "address": addr, "tokens": tokens_json})

    return rows, tips


@function_code_bp.route("/api/function/code", methods=["GET"])
def get_function_code():
    func_id = request.args.get("id")
    if not func_id:
        return jsonify({"detail": "Missing function id"}), 400

    try:
        parts = func_id.split(":")
        if len(parts) < 4:
            return jsonify({"detail": f"Invalid ID format: {func_id}"}), 400

        if parts[0] == "idx":
            # Standardized New Format: idx:collection:func:md5:addr
            collection = parts[1]
            md5 = parts[3]
            addr = parts[4]
        else:
            # Legacy Format: collection:function:md5:addr
            collection = parts[0]
            md5 = parts[2]
            addr = parts[3]

        source, features, meta, tf_map = fetch_function_data(collection, md5, addr)
        if not source:
            return jsonify({"detail": "Function not found"}), 404

        rows, tips = render_single_function(source, features, tf_map)

        # Ensure MD5 and Decompiler ID are in meta if missing, but otherwise keep full meta
        if meta:
            meta["file_md5"] = md5
            if "decompiler_id" not in meta:
                meta["decompiler_id"] = source.get("metadata", {}).get(
                    "decompiler_id", "unknown"
                )

            if "function_id" not in meta:
                meta["function_id"] = f"{collection}:func:{md5}:{addr}"
            if "file_id" not in meta:
                meta["file_id"] = f"{collection}:file:{md5}"
            if "batch_id" not in meta and meta.get("batch_uuid"):
                meta["batch_id"] = f"{collection}:batch:{meta['batch_uuid']}"
            if "entry_date" in meta:
                meta["entry_date"] = parse_timestamp(meta["entry_date"])
            if "file_date" in meta:
                meta["file_date"] = parse_timestamp(meta["file_date"])

            try:
                r = get_redis()
                fid = f"{collection}:func:{md5}:{addr}"
                cluster_ids = r.smembers(f"{fid}:clusters")
                scores = r.hgetall(f"{fid}:cluster_scores")
                clusters = []
                algo = "unweighted_cosine"
                if cluster_ids:
                    cluster_pipe = r.pipeline()
                    for cid_bytes in cluster_ids:
                        cid = (
                            cid_bytes.decode()
                            if isinstance(cid_bytes, bytes)
                            else cid_bytes
                        )
                        cluster_pipe.json().get(
                            f"{collection}:cluster:{algo}:{cid}:meta", "$"
                        )

                    raw_cluster_metas = cluster_pipe.execute()

                    for raw_cm in raw_cluster_metas:
                        if raw_cm:
                            cm = raw_cm[0] if isinstance(raw_cm, list) else raw_cm
                            if isinstance(cm, str):
                                cm = json.loads(cm)
                            if cm:
                                cid = str(cm.get("cluster_id"))
                                score = float(
                                    scores.get(
                                        cid.encode() if isinstance(cid, str) else cid,
                                        0.0,
                                    )
                                )
                                if not score and isinstance(scores, dict):
                                    for k, v in scores.items():
                                        k_str = (
                                            k.decode() if isinstance(k, bytes) else k
                                        )
                                        if k_str == cid:
                                            score = float(v)
                                            break
                                clusters.append(
                                    {
                                        "cluster_id": cm.get("cluster_id"),
                                        "cluster_uuid": cm.get("cluster_uuid"),
                                        "cluster_name": cm.get("cluster_name"),
                                        "cohesion_score": cm.get("cohesion_score", 0),
                                        "member_count": cm.get("member_count", 0),
                                        "cluster_stability": score
                                        or cm.get("cluster_stability", 0.0),
                                        "avg_features": cm.get("avg_features", 0),
                                    }
                                )

                    clusters.sort(key=lambda x: x.get("member_count", 0), reverse=True)

                meta["clusters"] = clusters

                for field in [
                    "cluster_id",
                    "cluster_name",
                    "cluster_uuid",
                    "cluster_stability",
                ]:
                    meta.pop(field, None)

            except Exception as ex:
                print(f"Error fetching clusters: {ex}")

        return jsonify({"rows": rows, "tips": tips, "meta": meta or {}})
    except Exception as e:
        # Capture the full stack trace as a string
        error_traceback = traceback.format_exc()

        # Log it to your console/file so you don't lose it
        print(error_traceback)

        return (
            jsonify(
                {
                    "detail": str(e),
                    "type": e.__class__.__name__,
                    "traceback": error_traceback,  # Optional: only for development
                }
            ),
            500,
        )
