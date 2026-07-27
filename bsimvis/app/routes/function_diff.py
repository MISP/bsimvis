from flask import request

import difflib
import json

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import (
    parse_timestamp,
    get_pool_id,
    enrich_pool_data,
)
from bsimvis.app.services.function_service import fetch_function_data, get_feature_map
from bsimvis.app.services.node_service import get_enriched_nodes


def parse_diff_params():
    """Normalize query params to flat form: collection_a, collection_b, md5_a, md5_b, addr_a, addr_b, pool."""
    collection_a = request.args.get(
        "collection_a", request.args.get("collection", "main")
    )
    collection_b = request.args.get("collection_b") or request.args.get(
        "coll_b", collection_a
    )
    md5_a = request.args.get("md5_a", request.args.get("md5A"))
    md5_b = request.args.get("md5_b", request.args.get("md5B"))
    addr_a = request.args.get("addr_a", request.args.get("addrA"))
    addr_b = request.args.get("addr_b", request.args.get("addrB"))
    pool_id = request.args.get("pool") or request.args.get("pool_id")

    # Support legacy id1/id2 for backward compat
    id1 = request.args.get("id1")
    id2 = request.args.get("id2")

    if not md5_a or not md5_b:
        if id1 and id2:

            def parse_legacy_id(id_str):
                try:
                    if ":func:" in id_str:
                        if id_str.startswith("idx:"):
                            col, rest = id_str[4:].split(":func:", 1)
                        else:
                            col, rest = id_str.split(":func:", 1)
                        parts = rest.split(":")
                        return col or "main", parts[0], parts[1]
                    elif ":function:" in id_str:
                        if id_str.startswith("idx:"):
                            col, rest = id_str[4:].split(":function:", 1)
                        else:
                            col, rest = id_str.split(":function:", 1)
                        parts = rest.split(":")
                        return col or "main", parts[0], parts[1]
                    else:
                        parts = id_str.split(":")
                        if len(parts) < 4:
                            return None, None, None
                        if parts[0] == "idx":
                            return parts[1] or "main", parts[3], parts[4]
                        return parts[0] or "main", parts[2], parts[3]
                except Exception:
                    return None, None, None

            col1, md5_1, addr_1 = parse_legacy_id(id1)
            col2, md5_2, addr_2 = parse_legacy_id(id2)
            if col1 and md5_1 and addr_1 and col2 and md5_2 and addr_2:
                collection_a = col1
                collection_b = col2
                md5_a = md5_1
                md5_b = md5_2
                addr_a = addr_1
                addr_b = addr_2

        if not md5_a or not md5_b:
            return None, "Both md5_a and md5_b are required"

    if not collection_a:
        collection_a = "main"
    if not collection_b:
        collection_b = collection_a

    return {
        "collection_a": collection_a,
        "collection_b": collection_b,
        "md5_a": md5_a,
        "md5_b": md5_b,
        "addr_a": addr_a,
        "addr_b": addr_b,
        "pool": pool_id,
    }, None


def diff_api():
    """Unified diff endpoint: handles both function-level and file-level diffs."""
    params, err = parse_diff_params()
    if err:
        return {"detail": err}, 400

    collection_a = params["collection_a"]
    collection_b = params["collection_b"]
    md5_a = params["md5_a"]
    md5_b = params["md5_b"]
    addr_a = params["addr_a"]
    addr_b = params["addr_b"]
    pool_id = params["pool"]

    # Handle function diff (when addr_a/addr_b provided)
    if addr_a and addr_b:
        return _diff_functions(
            collection_a, md5_a, addr_a, collection_b, md5_b, addr_b, pool_id
        )

    # Handle file-level diff (no addresses) - reuse bin_sim logic
    return _diff_bin_sim(collection_a, md5_a, md5_b, collection_b, pool_id)


def _diff_functions(collection_a, md5_a, addr_a, collection_b, md5_b, addr_b, pool_id):
    """Function-level aligned code diff (original diff_api logic)."""
    s1, f1, meta1, tf1 = fetch_function_data(collection_a, md5_a, addr_a)
    s2, f2, meta2, tf2 = fetch_function_data(collection_b, md5_b, addr_b)

    if s1 is None or s2 is None:
        return {"detail": "Failed to fetch data from Redis"}, 500

    # Enrich with callers/callees
    nodes1 = get_enriched_nodes(collection_a, md5_a, addr_a)
    nodes2 = get_enriched_nodes(collection_b, md5_b, addr_b)
    if meta1:
        meta1["callers"] = nodes1["callers"]
        meta1["callees"] = nodes1["callees"]
    if meta2:
        meta2["callers"] = nodes2["callers"]
        meta2["callees"] = nodes2["callees"]

    h1 = set(f["hash"] for f in (f1 or []))
    h2 = set(f["hash"] for f in (f2 or []))
    common_hashes = h1.intersection(h2)

    rows, left_tips, right_tips = render_aligned_diff(
        s1,
        f1,
        s2,
        f2,
        common_hashes,
        tf1,
        tf2,
        collection_a,
        md5_a,
        collection_b,
        md5_b,
        meta1,
        meta2,
    )

    algo = "unweighted_cosine"
    r = get_redis()

    for side_meta, side_md5, side_addr, side_col in [
        (meta1, md5_a, addr_a, collection_a),
        (meta2, md5_b, addr_b, collection_b),
    ]:
        if not side_meta:
            continue
        side_addr_safe = addr_a if side_meta is meta1 else addr_b
        side_md5_safe = md5_a if side_meta is meta1 else md5_b
        side_col_safe = collection_a if side_meta is meta1 else collection_b

        if "function_id" not in side_meta:
            side_meta["function_id"] = (
                f"{side_col_safe}:func:{side_md5_safe}:{side_addr_safe}"
            )
        if "file_id" not in side_meta:
            side_meta["file_id"] = f"{side_col_safe}:file:{side_md5_safe}"
        if "batch_id" not in side_meta and side_meta.get("batch_uuid"):
            side_meta["batch_id"] = f"{side_col_safe}:batch:{side_meta['batch_uuid']}"
        if "entry_date" in side_meta:
            side_meta["entry_date"] = parse_timestamp(side_meta["entry_date"])
        if "file_date" in side_meta:
            side_meta["file_date"] = parse_timestamp(side_meta["file_date"])

        clusters = _fetch_clusters(
            side_meta, side_col_safe, side_md5_safe, side_addr_safe, pool_id, algo
        )
        side_meta["clusters"] = clusters

    # Enrich pool data
    if pool_id:
        if meta1:
            enrich_pool_data(meta1, pool_id)
        if meta2:
            enrich_pool_data(meta2, pool_id)

    return {
        "rows": rows,
        "left_tips": left_tips,
        "right_tips": right_tips,
        "meta1": meta1 or {},
        "meta2": meta2 or {},
    }


def _fetch_clusters(meta, collection, md5, addr, pool_id, algo):
    """Fetch clusters for a single function meta."""
    clusters = []
    try:
        r = get_redis()
        fid = f"{collection}:func:{md5}:{addr}"
        effective_pool = pool_id or get_pool_id(collection) or get_pool_id(collection)
        if effective_pool:
            cluster_ids = r.smembers(f"global:pool:{effective_pool}:{fid}:clusters")
        else:
            cluster_ids = r.smembers(f"{fid}:clusters")

        if cluster_ids:
            if effective_pool:
                scores = (
                    r.hgetall(f"global:pool:{effective_pool}:{fid}:cluster_scores")
                    or {}
                )
            else:
                scores = r.hgetall(f"{fid}:cluster_scores") or {}
            cluster_pipe = r.pipeline(transaction=False)
            for cid_bytes in cluster_ids:
                cid = cid_bytes.decode() if isinstance(cid_bytes, bytes) else cid_bytes
                if effective_pool:
                    cluster_pipe.get(
                        f"global:pool:{effective_pool}:cluster:{algo}:{cid}:meta"
                    )
                else:
                    cluster_pipe.get(f"{collection}:cluster:{algo}:{cid}:meta")

            raw_cluster_metas = cluster_pipe.execute()

            for raw_cm in raw_cluster_metas:
                if raw_cm:
                    cm = json.loads(raw_cm) if not isinstance(raw_cm, dict) else raw_cm
                    if isinstance(cm, str):
                        cm = json.loads(cm)
                    if cm:
                        cid = str(cm.get("cluster_id"))
                        score = float(
                            scores.get(
                                cid.encode() if isinstance(cid, str) else cid, 0.0
                            )
                        )
                        if not score and isinstance(scores, dict):
                            for k, v in scores.items():
                                k_str = k.decode() if isinstance(k, bytes) else k
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

    except Exception as ex:
        print(f"Error fetching clusters: {ex}")
        clusters = []
    return clusters


def _diff_bin_sim(collection_a, md5_a, md5_b, collection_b, pool_id):
    """File-level bin_sim diff (reused from bin_sim.py)."""
    from bsimvis.app.routes import bin_sim

    return bin_sim.get_bin_sim(
        collection=collection_a,
        md5_a=md5_a,
        md5_b=md5_b,
        coll_b=collection_b or collection_a,
        pool_id=pool_id,
    )


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
    line_tokens,
    common_hashes,
    feature_map,
    tf_map,
    side="l",
    side_tips=None,
    collection=None,
    md5=None,
    meta=None,
):
    tokens_json = []
    if side_tips is None:
        side_tips = {}

    callees_map = {}
    if meta and "callees" in meta:
        for callee in meta["callees"]:
            name = callee.get("name")
            if name:
                callees_map[name] = callee

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

        called_func_id = None
        target_name = token.get("target_name")
        is_external = token.get("is_external", False)

        if token.get("type") == "func_call":
            target_addr = token.get("target_addr")
            if not target_addr and token.get("t") in callees_map:
                callee = callees_map[token.get("t")]
                target_addr = callee.get("entrypoint")
                target_name = callee.get("name")
                is_external = callee.get("is_external", False)

            if collection and md5:
                if is_external:
                    called_func_id = f"ext:{target_name or token.get('t', '')}"
                elif target_addr:
                    called_func_id = f"{collection}:func:{md5}:{target_addr}"

        tokens_json.append(
            {
                "type": token.get("type"),
                "has_features": bool(token_features),
                "diff_class": diff_class,
                "hash_list": hash_list,
                "global_idx": global_idx,
                "text": token["t"],
                "side": side,
                "called_func_id": called_func_id,
                "target_name": target_name,
                "is_external": is_external,
            }
        )

    return tokens_json


def render_aligned_diff(
    s1,
    f1,
    s2,
    f2,
    common_hashes,
    tf1,
    tf2,
    collection1=None,
    md5_1=None,
    collection2=None,
    md5_2=None,
    meta1=None,
    meta2=None,
):
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
        coll = collection1 if side == "l" else collection2
        m = md5_1 if side == "l" else md5_2
        meta = meta1 if side == "l" else meta2
        tokens_json = render_line_content(
            line_tokens, common_hashes, f_map, tf_map, side, side_tips, coll, m, meta
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
