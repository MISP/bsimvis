import json
from flask import request
from bsimvis.app.services import bsim_profiles
from bsimvis.app.services.similarity_service import SimilarityService


def similarity_api():
    # Accept new flat params, fall back to legacy id1/id2
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

    legacy_id1 = request.args.get("id1")
    legacy_id2 = request.args.get("id2")

    # Build id1/id2 from flat params or legacy
    if legacy_id1 and legacy_id2:
        id1 = legacy_id1
        id2 = legacy_id2
        collection = id1.split(":")[0] if ":" in id1 else "main"
    elif md5_a and addr_a and md5_b and addr_b:
        id1 = f"{collection_a}:func:{md5_a}:{addr_a}"
        id2 = f"{collection_b}:func:{md5_b}:{addr_b}"
        collection = collection_a
    else:
        return {
            "detail": "Missing function identifiers (id1/id2 or addr/addr_b with md5)"
        }, 400

    if pool_id:
        collection = f"global:pool:{pool_id}:col:{collection}"

    try:
        from bsimvis.app.services.index_service import get_pool_id
        from bsimvis.app.services.milvus_service import milvus_service

        service = SimilarityService()
        # `algo` opts in to extra algorithms (comma-separated). weighted_cosine is
        # not returned by default: it is unbuilt, so every request would score it
        # on demand, and it raises on a collection whose features were extracted
        # under different signature settings.
        requested = request.args.get("algo") or request.args.get("algos")
        if requested:
            algorithms = [a.strip() for a in requested.split(",") if a.strip()]
        else:
            algorithms = ["jaccard", "unweighted_cosine"]
            if milvus_service.enabled:
                algorithms.append("milvus_sparse")

        scores = {}
        significance = {}
        errors = {}

        if pool_id:
            pass  # keep collection as f"global:pool:{pool_id}:col:{collection}"
        elif id1.startswith("global:pool:"):
            parts = id1.split(":")
            if len(parts) >= 5 and parts[3] == "col":
                collection = ":".join(parts[:5])
            else:
                collection = ":".join(parts[:3])
        elif id1.startswith("pool:"):
            parts = id1.split(":")
            if len(parts) >= 4 and parts[2] == "col":
                collection = ":".join(parts[:4])
            else:
                collection = ":".join(parts[:2])
        else:
            collection = id1.split(":")[0] if ":" in id1 else "main"

        pool_id = get_pool_id(collection)
        tags = []
        user_tags = []

        for algo in algorithms:
            base_algo, _ = bsim_profiles.parse_algo(algo)
            if base_algo == "weighted_cosine":
                # Never cached (weighted builds do not exist yet), so compute the
                # pair directly and report significance alongside the score.
                try:
                    sim, sig = service.calculate_exact_score(
                        id1, id2, algo=algo, with_significance=True
                    )
                    scores[algo] = sim
                    significance[algo] = sig
                except (ValueError, OSError) as e:
                    # Mask mismatch, unknown profile, or a missing weights table.
                    errors[algo] = str(e)
                    scores[algo] = None
                continue

            # Use service to get score, passing the resolved collection namespace
            score = service.get_pair_score(id1, id2, algo=algo, collection=collection)
            scores[algo] = score

            # Fetch the actual document to extract tags
            sid = service._canonicalize_sid(collection, id1, id2, algo)
            raw_doc = service.r.get(sid)
            if not raw_doc and pool_id:
                # Fall back to base pool namespace for document retrieval
                base_pool = f"global:pool:{pool_id}"
                sid_base = service._canonicalize_sid(base_pool, id1, id2, algo)
                raw_doc = service.r.get(sid_base)

            if raw_doc:
                d = json.loads(raw_doc)
                if d.get("tags") and not tags:
                    tags = d.get("tags")
                if d.get("user_tags") and not user_tags:
                    user_tags = d.get("user_tags")

        # Determine if any score was calculated on-demand or loaded from pool/cache
        source = "pool" if pool_id else "cache"
        for algo in algorithms:
            # If the score doesn't exist in the database (pool or collection cache), source becomes on-demand
            cached_score = service.check_cache(id1, id2, collection, algo)
            if cached_score is None:
                source = "on-demand"
                break

        result = {
            "id1": id1,
            "id2": id2,
            "scores": scores,
            "tags": tags,
            "user_tags": user_tags,
            "source": source,
        }
        # Only weighted_cosine defines a significance; omit the keys entirely
        # rather than padding every response with nulls.
        if significance:
            result["significance"] = significance
        if errors:
            result["errors"] = errors
        return result

    except Exception as e:
        return {"detail": f"Error retrieving similarity: {str(e)}"}, 500
