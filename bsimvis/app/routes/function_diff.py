from flask import Flask, request, jsonify, Blueprint

from flask_cors import CORS
import difflib

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import parse_timestamp
from bsimvis.app.services.function_service import fetch_function_data, get_feature_map

function_diff_bp = Blueprint("function_diff", __name__)


# fetch_function_data and get_feature_map are now in function_service.py


def get_lines_data(source, f_map, common_hashes):
    tokens = source.get("c_tokens", [])
    if not tokens:
        return {}, []

    max_line = max(t["line"] for t in tokens)
    lines_dict = {i: [] for i in range(max_line + 1)}
    for idx, t in enumerate(tokens):
        lines_dict[t["line"]].append((idx, t))

    identities = []
    for i in range(max_line + 1):
        line_tokens = lines_dict[i]
        common_in_line = []

        for idx, t in line_tokens:
            for f in f_map.get(idx, []):
                common_in_line.append(f["hash"])

        if common_in_line:
            identities.append("FEATURES:" + ",".join(sorted(set(common_in_line))))
        else:
            text = "".join(t["t"] for idx, t in line_tokens).strip()
            identities.append("TEXT:" + text)

    return lines_dict, identities


def render_line_content(
    line_tokens, common_hashes, feature_map, tf_map, side="l", side_tips=None
):
    tokens_json = []
    if side_tips is None:
        side_tips = {}

    for global_idx, token in line_tokens:
        token_features = feature_map.get(global_idx, [])

        hash_list = [f["hash"] for f in token_features]
        has_match = any(h in common_hashes for h in hash_list)
        has_unique = any(h not in common_hashes for h in hash_list)

        diff_class = "diff-match" if has_match else "diff-unique" if has_unique else ""

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
                        "#00ff00" if f["hash"] in common_hashes else "#ff003c",
                    ]
                )
            side_tips[global_idx] = [token.get("type"), token.get("seq"), tip_features]

        tokens_json.append(
            {
                "type": token.get("type"),
                "has_features": bool(token_features),
                "diff_class": diff_class,
                "hash_list": hash_list,
                "global_idx": global_idx,
                "text": token["t"],
                "side": side,
            }
        )

    return tokens_json


def render_aligned_diff(s1, f1, s2, f2, common_hashes, tf1, tf2):
    f_map1 = get_feature_map(f1)
    f_map2 = get_feature_map(f2)

    lines1, id1 = get_lines_data(s1, f_map1, common_hashes)
    lines2, id2 = get_lines_data(s2, f_map2, common_hashes)

    sm = difflib.SequenceMatcher(None, id1, id2)

    rows = []
    left_tips = {}
    right_tips = {}

    def render_side(
        s_data,
        line_idx,
        lines_dict,
        f_map,
        tag="",
        opposing_idx=None,
        chunk_id=0,
        identities=[],
        tf_map={},
        side="l",
        side_tips=None,
    ):
        if line_idx is None:
            return None

        chunk_class = f"tag-{tag} chunk-{chunk_id}"
        line_identity = identities[line_idx] if line_idx < len(identities) else ""
        addr_map = s_data.get("line_to_addr", {})
        addr = addr_map.get(str(line_idx), [""])[0] if addr_map else ""

        line_tokens = lines_dict.get(line_idx, [])
        tokens_json = render_line_content(
            line_tokens, common_hashes, f_map, tf_map, side, side_tips
        )

        tooltip_text = (
            f"Address: {addr} | ID: {line_identity}" if addr else f"ID: {line_identity}"
        )

        return {
            "chunk_class": chunk_class,
            "chunk_id": chunk_id,
            "line_idx": line_idx + 1,
            "tooltip_text": tooltip_text,
            "tokens": tokens_json,
        }

    for chunk_id, (tag, i1, i2, j1, j2) in enumerate(sm.get_opcodes()):
        if tag == "equal" or tag == "replace":
            max_len = max(i2 - i1, j2 - j1)
            for k in range(max_len):
                l_idx = i1 + k if k < (i2 - i1) else None
                r_idx = j1 + k if k < (j2 - j1) else None
                rows.append(
                    {
                        "l": render_side(
                            s1,
                            l_idx,
                            lines1,
                            f_map1,
                            tag,
                            r_idx,
                            chunk_id,
                            id1,
                            tf1,
                            "l",
                            left_tips,
                        ),
                        "r": render_side(
                            s2,
                            r_idx,
                            lines2,
                            f_map2,
                            tag,
                            l_idx,
                            chunk_id,
                            id2,
                            tf2,
                            "r",
                            right_tips,
                        ),
                    }
                )
        elif tag == "insert":
            for r_idx in range(j1, j2):
                rows.append(
                    {
                        "l": render_side(
                            s1,
                            None,
                            lines1,
                            f_map1,
                            tag,
                            r_idx,
                            chunk_id,
                            id1,
                            tf1,
                            "l",
                            left_tips,
                        ),
                        "r": render_side(
                            s2,
                            r_idx,
                            lines2,
                            f_map2,
                            tag,
                            None,
                            chunk_id,
                            id2,
                            tf2,
                            "r",
                            right_tips,
                        ),
                    }
                )
        elif tag == "delete":
            for l_idx in range(i1, i2):
                rows.append(
                    {
                        "l": render_side(
                            s1,
                            l_idx,
                            lines1,
                            f_map1,
                            tag,
                            None,
                            chunk_id,
                            id1,
                            tf1,
                            "l",
                            left_tips,
                        ),
                        "r": render_side(
                            s2,
                            None,
                            lines2,
                            f_map2,
                            tag,
                            l_idx,
                            chunk_id,
                            id2,
                            tf2,
                            "r",
                            right_tips,
                        ),
                    }
                )

    return rows, left_tips, right_tips


@function_diff_bp.route("/api/diff", methods=["GET"])
def diff_api():
    # Use request.args to get query parameters
    id1 = request.args.get("id1")
    id2 = request.args.get("id2")

    if not id1 or not id2:
        return jsonify({"detail": "Missing id1 or id2"}), 400

    # Business logic
    try:
        parts1 = id1.split(":")
        parts2 = id2.split(":")

        if len(parts1) < 4 or len(parts2) < 4:
            return jsonify({"detail": "Invalid ID format"}), 400

        # Standard Robust Resolution
        if parts1[0] == "idx":
            collection1, md5_1, addr_1 = parts1[1], parts1[3], parts1[4]
        else:
            collection1, md5_1, addr_1 = parts1[0], parts1[2], parts1[3]

        if parts2[0] == "idx":
            collection2, md5_2, addr_2 = parts2[1], parts2[3], parts2[4]
        else:
            collection2, md5_2, addr_2 = parts2[0], parts2[2], parts2[3]
    except Exception:
        return jsonify({"detail": "Malformed ID"}), 400

    if (
        not collection1
        or not md5_1
        or not addr_1
        or not collection2
        or not md5_2
        or not addr_2
    ):
        return jsonify({"detail": "Invalid ID components"}), 400

    s1, f1, meta1, tf1 = fetch_function_data(collection1, md5_1, addr_1)
    s2, f2, meta2, tf2 = fetch_function_data(collection2, md5_2, addr_2)

    if s1 is None or s2 is None:
        # In fetch_function_data, s1 being None usually means the Redis fetch failed
        return jsonify({"detail": "Failed to fetch data from Redis"}), 500

    h1 = set(f["hash"] for f in (f1 or []))
    h2 = set(f["hash"] for f in (f2 or []))
    common_hashes = h1.intersection(h2)

    # Reusing your alignment logic
    rows, left_tips, right_tips = render_aligned_diff(
        s1, f1, s2, f2, common_hashes, tf1, tf2
    )

    if meta1:
        if "function_id" not in meta1:
            meta1["function_id"] = f"idx:{collection1}:func:{md5_1}:{addr_1}"
        if "file_id" not in meta1:
            meta1["file_id"] = f"idx:{collection1}:file:{md5_1}"
        if "batch_id" not in meta1 and meta1.get("batch_uuid"):
            meta1["batch_id"] = f"{collection1}:batch:{meta1['batch_uuid']}"
        if "entry_date" in meta1:
            meta1["entry_date"] = parse_timestamp(meta1["entry_date"])
        if "file_date" in meta1:
            meta1["file_date"] = parse_timestamp(meta1["file_date"])

    if meta2:
        if "function_id" not in meta2:
            meta2["function_id"] = f"idx:{collection2}:func:{md5_2}:{addr_2}"
        if "file_id" not in meta2:
            meta2["file_id"] = f"idx:{collection2}:file:{md5_2}"
        if "batch_id" not in meta2 and meta2.get("batch_uuid"):
            meta2["batch_id"] = f"{collection2}:batch:{meta2['batch_uuid']}"
        if "entry_date" in meta2:
            meta2["entry_date"] = parse_timestamp(meta2["entry_date"])
        if "file_date" in meta2:
            meta2["file_date"] = parse_timestamp(meta2["file_date"])

    # Flask's jsonify handles the dictionary to JSON conversion
    return jsonify(
        {
            "rows": rows,
            "left_tips": left_tips,
            "right_tips": right_tips,
            "meta1": meta1 or {},
            "meta2": meta2 or {},
        }
    )
