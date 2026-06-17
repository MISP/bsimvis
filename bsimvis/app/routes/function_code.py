from flask import request
from bsimvis.app.services.function_service import fetch_function_data, get_feature_map
from bsimvis.app.services.index_service import (
    parse_timestamp,
    get_pool_id,
    enrich_pool_data,
)
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.node_service import get_enriched_nodes
import traceback
import json


def render_single_function(
    source, features, tf_map, collection=None, md5=None, meta=None
):
    """
    Renders the semantic tokens for a single function without any diffing logic.
    """
    f_map = get_feature_map(features)
    tokens = source.get("c_tokens", [])
    if not tokens:
        return []

    max_line = max(t["line"] for t in tokens) if tokens else 0
    lines_dict = {i: [] for i in range(max_line + 1)}
    for idx, t in enumerate(tokens):
        lines_dict[t["line"]].append((idx, t))

    addr_map = source.get("line_to_addr", {})

    callees_map = {}
    if meta and "callees" in meta:
        for callee in meta["callees"]:
            name = callee.get("name")
            if name:
                callees_map[name] = callee

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
                    "called_func_id": called_func_id,
                    "target_name": target_name,
                    "is_external": is_external,
                }
            )

        addr = addr_map.get(str(i), [""])[0] if addr_map else ""

        rows.append({"line_idx": i + 1, "address": addr, "tokens": tokens_json})

    return rows, tips


def get_function_code():
    func_id = request.args.get("id")
    if not func_id:
        return {"detail": "Missing function id"}, 400

    try:
        if ":func:" in func_id:
            if func_id.startswith("idx:"):
                collection, rest = func_id[4:].split(":func:", 1)
            else:
                collection, rest = func_id.split(":func:", 1)
            parts = rest.split(":")
            md5 = parts[0]
            addr = parts[1]
        elif ":function:" in func_id:
            if func_id.startswith("idx:"):
                collection, rest = func_id[4:].split(":function:", 1)
            else:
                collection, rest = func_id.split(":function:", 1)
            parts = rest.split(":")
            md5 = parts[0]
            addr = parts[1]
        else:
            parts = func_id.split(":")
            if len(parts) < 4:
                return {"detail": f"Invalid ID format: {func_id}"}, 400
            if parts[0] == "idx":
                collection = parts[1]
                md5 = parts[3]
                addr = parts[4]
            else:
                collection = parts[0]
                md5 = parts[2]
                addr = parts[3]

        source, features, meta, tf_map = fetch_function_data(collection, md5, addr)
        if not source:
            return {"detail": "Function not found"}, 404

        # Dynamically fetch callers/callees
        nodes = get_enriched_nodes(collection, md5, addr)
        if meta:
            meta["callers"] = nodes["callers"]
            meta["callees"] = nodes["callees"]

        rows, tips = render_single_function(
            source, features, tf_map, collection, md5, meta
        )

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
                pool_id = request.args.get("pool") or get_pool_id(collection)
                if pool_id:
                    cluster_ids = r.smembers(f"global:pool:{pool_id}:{fid}:clusters")
                    scores = r.hgetall(f"global:pool:{pool_id}:{fid}:cluster_scores")
                else:
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
                        if pool_id:
                            cluster_pipe.json().get(
                                f"global:pool:{pool_id}:cluster:{algo}:{cid}:meta", "$"
                            )
                        else:
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

        pool_id = request.args.get("pool") or get_pool_id(collection)
        if pool_id and meta:
            enrich_pool_data(meta, pool_id)

        return {"rows": rows, "tips": tips, "meta": meta or {}}
    except Exception as e:
        # Capture the full stack trace as a string
        error_traceback = traceback.format_exc()

        # Log it to your console/file so you don't lose it
        print(error_traceback)

        return (
            {
                "detail": str(e),
                "type": e.__class__.__name__,
                "traceback": error_traceback,  # Optional: only for development
            },
            500,
        )


def get_file_call_graph():
    collection = request.args.get("collection")
    file_md5 = request.args.get("file_md5")

    if not collection or not file_md5:
        return {"detail": "Missing collection or file_md5"}, 400

    try:
        r = get_redis()
        # Get all functions in file
        func_ids_bytes = r.smembers(f"{collection}:idx:file:functions:{file_md5}") or []
        func_ids = [
            fid.decode() if isinstance(fid, bytes) else fid for fid in func_ids_bytes
        ]

        if not func_ids:
            return {"nodes": [], "edges": []}

        pipe = r.pipeline()
        for fid in func_ids:
            pipe.json().get(f"{fid}:meta", "$")
            pipe.smembers(f"{fid}:callees")

        results = pipe.execute()

        nodes = []
        edges = []
        node_ids_in_graph = set()
        external_nodes = {}
        unindexed_nodes = set()

        for i, fid in enumerate(func_ids):
            raw_meta = results[2 * i]
            callee_bytes = results[2 * i + 1] or []

            meta = None
            if raw_meta:
                meta = raw_meta[0] if isinstance(raw_meta, list) else raw_meta
                if isinstance(meta, str):
                    meta = json.loads(meta)

            func_name = (
                meta.get("function_name") if meta else f"func_{fid.split(':')[-1]}"
            )
            addr = fid.split(":")[-1]

            nodes.append(
                {
                    "id": fid,
                    "name": func_name,
                    "entrypoint": addr,
                    "namespace": meta.get("namespace") if meta else None,
                    "return_type": meta.get("return_type") if meta else None,
                    "parameters": meta.get("parameters") if meta else None,
                    "features_count": meta.get("bsim_features_count", 0) if meta else 0,
                    "is_external": False,
                    "is_unindexed": False,
                }
            )
            node_ids_in_graph.add(fid)

            callees = [c.decode() if isinstance(c, bytes) else c for c in callee_bytes]
            for callee_id in callees:
                edges.append({"source": fid, "target": callee_id})
                if callee_id.startswith("ext:"):
                    if callee_id not in external_nodes:
                        parts = callee_id.split(":", 1)
                        name = parts[1] if len(parts) > 1 else callee_id
                        external_nodes[callee_id] = name
                else:
                    # Internal function, but check if it's unindexed/filtered out
                    if callee_id not in node_ids_in_graph and callee_id not in func_ids:
                        unindexed_nodes.add(callee_id)

        # Fetch metadata for unindexed nodes
        if unindexed_nodes:
            other_ids = list(unindexed_nodes)
            other_pipe = r.pipeline()
            for oid in other_ids:
                other_pipe.json().get(f"{oid}:meta", "$")
            other_results = other_pipe.execute()

            for oid, raw_meta in zip(other_ids, other_results):
                meta = None
                if raw_meta:
                    meta = raw_meta[0] if isinstance(raw_meta, list) else raw_meta
                    if isinstance(meta, str):
                        meta = json.loads(meta)

                func_name = (
                    meta.get("function_name") if meta else f"func_{oid.split(':')[-1]}"
                )
                addr = oid.split(":")[-1]
                nodes.append(
                    {
                        "id": oid,
                        "name": func_name,
                        "entrypoint": addr,
                        "namespace": meta.get("namespace") if meta else None,
                        "return_type": meta.get("return_type") if meta else None,
                        "parameters": meta.get("parameters") if meta else None,
                        "features_count": (
                            meta.get("bsim_features_count", 0) if meta else 0
                        ),
                        "is_external": False,
                        "is_unindexed": True,
                    }
                )

        # Add external nodes to the node list
        for ext_id, ext_name in external_nodes.items():
            nodes.append(
                {
                    "id": ext_id,
                    "name": ext_name,
                    "entrypoint": None,
                    "features_count": 0,
                    "is_external": True,
                    "is_unindexed": False,
                }
            )

        return {"nodes": nodes, "edges": edges}
    except Exception as e:
        return {"detail": str(e)}, 500
