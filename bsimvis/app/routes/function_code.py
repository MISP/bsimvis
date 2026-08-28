from flask import request
from bsimvis.app.services.function_service import fetch_function_data, get_feature_map
from bsimvis.app.services.index_service import (
    parse_timestamp,
    get_pool_id,
    enrich_pool_data,
)
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.bin_sim_service import bin_sim_service
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
                    cluster_pipe = r.pipeline(transaction=False)
                    for cid_bytes in cluster_ids:
                        cid = (
                            cid_bytes.decode()
                            if isinstance(cid_bytes, bytes)
                            else cid_bytes
                        )
                        if pool_id:
                            cluster_pipe.get(
                                f"global:pool:{pool_id}:cluster:{algo}:{cid}:meta"
                            )
                        else:
                            cluster_pipe.get(f"{collection}:cluster:{algo}:{cid}:meta")

                    raw_cluster_metas = cluster_pipe.execute()

                    for raw_cm in raw_cluster_metas:
                        if raw_cm:
                            cm = (
                                json.loads(raw_cm)
                                if not isinstance(raw_cm, dict)
                                else raw_cm
                            )
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


def get_function_call_graph():
    """Depth-1 call graph for a single function: itself plus its direct
    callers/callees, without the decompiled-code cost of get_function_code."""
    func_id = request.args.get("id")
    if not func_id:
        return {"detail": "Missing function id"}, 400
    limit = request.args.get("limit", type=int)

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

        _, _, meta, _ = fetch_function_data(collection, md5, addr, meta_only=True)
        if not meta:
            return {"detail": "Function not found"}, 404

        nodes = get_enriched_nodes(collection, md5, addr, limit=limit)

        node = {
            "id": f"{collection}:func:{md5}:{addr}",
            "name": meta.get("function_name"),
            "entrypoint": meta.get("entrypoint_address", addr),
            "namespace": meta.get("namespace"),
            "return_type": meta.get("return_type"),
            "file_md5": md5,
            "file_name": meta.get("file_name"),
            "is_external": False,
        }

        return {
            "node": node,
            "callers": nodes["callers"],
            "callees": nodes["callees"],
            "callers_total": nodes["callers_total"],
            "callees_total": nodes["callees_total"],
        }
    except Exception as e:
        error_traceback = traceback.format_exc()
        print(error_traceback)
        return (
            {
                "detail": str(e),
                "type": e.__class__.__name__,
                "traceback": error_traceback,
            },
            500,
        )


def get_function_relations():
    """Given an arbitrary working set of function ids (possibly spanning
    multiple binaries), returns every direct-call edge and every
    similarity edge among just those ids -- the bulk equivalent of asking
    "does A connect to B" for every pair already in the set, instead of the
    single-center depth-1 view get_function_call_graph gives."""
    ids_param = request.args.get("ids", "")
    ids = [i.strip() for i in ids_param.split(",") if i.strip()]
    collection = request.args.get("collection")
    pool = request.args.get("pool")
    algo = request.args.get("algo", "unweighted_cosine")
    min_score = float(request.args.get("min_score", 0.85))
    new_ids_param = request.args.get("new_ids", "")
    new_ids = {i.strip() for i in new_ids_param.split(",") if i.strip()}
    # Pairwise similarity is O(ids^2) -- callers that only want the call
    # graph (e.g. analysis_orchestrator partitioning a big LLM batch) can
    # skip it instead of paying for millions of unused pair lookups.
    want_sim_edges = request.args.get("sim_edges", "1") not in ("0", "false", "")

    if len(ids) < 2 or not collection:
        return {"call_edges": [], "sim_edges": []}

    try:
        r = get_redis()
        id_set = set(ids)

        # --- call edges: pipeline :callees for every id, intersect against
        # the working set. Callee sets are populated symmetrically at ingest,
        # so this alone surfaces every directed edge with both endpoints in
        # the set -- no need to also pipeline :callers.
        pipe = r.pipeline(transaction=False)
        for fid in ids:
            pipe.smembers(f"{fid}:callees")
        callee_results = pipe.execute()

        call_edges = []
        for fid, callee_bytes in zip(ids, callee_results):
            for c in callee_bytes or []:
                cid = c.decode() if isinstance(c, bytes) else c
                if cid in id_set:
                    call_edges.append({"from": fid, "to": cid})

        if not want_sim_edges:
            return {"call_edges": call_edges, "sim_edges": []}

        # --- similarity edges: only pairs worth checking are (a) explicitly
        # requested via new_ids x ids (cheap re-add-a-function case), or (b)
        # every pair, when new_ids wasn't given (first resolve of a working set).
        from bsimvis.app.services.similarity_service import SimilarityService

        sim_collection = f"global:pool:{pool}" if pool else collection
        svc = SimilarityService(r)

        pairs = []
        if new_ids:
            others = [i for i in ids if i not in new_ids]
            for a in new_ids:
                for b in others:
                    pairs.append((a, b))
            new_id_list = list(new_ids)
            for i in range(len(new_id_list)):
                for j in range(i + 1, len(new_id_list)):
                    pairs.append((new_id_list[i], new_id_list[j]))
        else:
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    pairs.append((ids[i], ids[j]))

        sid_pipe = r.pipeline(transaction=False)
        sids = []
        for a, b in pairs:
            sid = svc._canonicalize_sid(sim_collection, a, b, algo)
            sids.append(sid)
            is_pool = sim_collection.startswith("global:pool:")
            zset_key = (
                f"{sim_collection}:sim:score"
                if is_pool
                else f"{sim_collection}:sim:score:{algo}"
            )
            sid_pipe.zscore(zset_key, sid)
        scores = sid_pipe.execute()

        misses = []
        sim_edges = []
        for (a, b), sid, score in zip(pairs, sids, scores):
            if score is not None:
                if float(score) >= min_score:
                    sim_edges.append({"id1": a, "id2": b, "score": float(score)})
                continue
            # Two-tier pool lookup: retry at the base pool namespace before
            # falling back to an exact per-pair computation.
            if sim_collection.startswith("global:pool:") and ":col:" in sim_collection:
                base_pool = sim_collection.split(":col:")[0]
                base_sid = svc._canonicalize_sid(base_pool, a, b, algo)
                base_score = r.zscore(f"{base_pool}:sim:score", base_sid)
                if base_score is not None:
                    if float(base_score) >= min_score:
                        sim_edges.append({"id1": a, "id2": b, "score": float(base_score)})
                    continue
            misses.append((a, b))

        # Cache misses: batch-fetch each unique id's feature vector once
        # (not once per pair) and compute cosine/jaccard in Python.
        if misses:
            miss_ids = sorted({i for pair in misses for i in pair})
            vec_pipe = r.pipeline(transaction=False)
            for fid in miss_ids:
                vec_pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
            vec_results = vec_pipe.execute()
            vecs = {
                fid: {h.decode() if isinstance(h, bytes) else h: float(s) for h, s in raw}
                for fid, raw in zip(miss_ids, vec_results)
                if raw
            }
            for a, b in misses:
                d1, d2 = vecs.get(a), vecs.get(b)
                if not d1 or not d2:
                    continue
                common = set(d1.keys()) & set(d2.keys())
                if algo == "jaccard":
                    sum_min = sum(min(d1[h], d2[h]) for h in common)
                    union = sum(d1.values()) + sum(d2.values()) - sum_min
                    score = (sum_min / union) if union > 0 else 0.0
                else:
                    dot = sum(d1[h] * d2[h] for h in common)
                    norm1 = sum(v**2 for v in d1.values()) ** 0.5
                    norm2 = sum(v**2 for v in d2.values()) ** 0.5
                    score = (dot / (norm1 * norm2)) if (norm1 > 0 and norm2 > 0) else 0.0
                if score >= min_score:
                    sim_edges.append({"id1": a, "id2": b, "score": score})

        return {"call_edges": call_edges, "sim_edges": sim_edges}
    except Exception as e:
        error_traceback = traceback.format_exc()
        print(error_traceback)
        return (
            {"detail": str(e), "type": e.__class__.__name__, "traceback": error_traceback},
            500,
        )


def get_file_call_graph():
    collection = request.args.get("collection")
    file_md5 = request.args.get("file_md5")

    retain = request.args.get("retain")
    max_nodes = request.args.get("max_nodes")
    if not collection or not file_md5:
        return {"detail": "Missing collection or file_md5"}, 400

    if max_nodes is not None:
        try:
            max_nodes = int(max_nodes)
        except (TypeError, ValueError):
            return {"detail": "max_nodes must be an integer"}, 400
        if max_nodes < 1:
            return {"detail": "max_nodes must be at least 1"}, 400
    try:
        r = get_redis()
        retain_set = None
        if retain:
            _, pair, retain_set = bin_sim_service.unique_functions_for_pair(
                collection,
                file_md5,
                retain,
                request.args.get("retain_collection", collection),
                request.args.get("pool"),
                request.args.get("algo", "unweighted_cosine"),
            )
            if not pair:
                return {"detail": "Similarity not calculated for this pair"}, 404
            if pair.get("is_container_pair"):
                return {"detail": "Container pairs have no function call graph"}, 400
            func_ids = sorted(retain_set)
        else:
            func_ids_bytes = (
                r.smembers(f"{collection}:idx:file:functions:{file_md5}") or []
            )
            func_ids = [
                fid.decode() if isinstance(fid, bytes) else fid
                for fid in func_ids_bytes
            ]

        if not func_ids:
            return {"nodes": [], "edges": []}

        pipe = r.pipeline(transaction=False)
        for fid in func_ids:
            pipe.get(f"{fid}:meta")
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
                meta = (
                    json.loads(raw_meta) if not isinstance(raw_meta, dict) else raw_meta
                )
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
                if retain_set is not None and callee_id not in retain_set:
                    continue
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
            other_pipe = r.pipeline(transaction=False)
            for oid in other_ids:
                other_pipe.get(f"{oid}:meta")
            other_results = other_pipe.execute()

            for oid, raw_meta in zip(other_ids, other_results):
                meta = None
                if raw_meta:
                    meta = (
                        json.loads(raw_meta)
                        if not isinstance(raw_meta, dict)
                        else raw_meta
                    )
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

        if retain_set is not None:
            degree = {fid: 0 for fid in retain_set}
            for edge in edges:
                degree[edge["source"]] += 1
                degree[edge["target"]] += 1
            nodes.sort(
                key=lambda node: (
                    -degree[node["id"]],
                    -int(node.get("features_count") or 0),
                    node["id"],
                )
            )
            for rank, node in enumerate(nodes, 1):
                node["unique_degree"] = degree[node["id"]]
                node["rank"] = rank
            if max_nodes is not None:
                nodes = nodes[:max_nodes]
                selected = {node["id"] for node in nodes}
                edges = [
                    edge
                    for edge in edges
                    if edge["source"] in selected and edge["target"] in selected
                ]

        return {"nodes": nodes, "edges": edges}
    except Exception as e:
        return {"detail": str(e)}, 500
