from flask import request
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
        algorithms = ["jaccard", "unweighted_cosine"]
        if milvus_service.enabled:
            algorithms.append("milvus_sparse")

        scores = {}

        if id1.startswith("global:pool:"):
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
            # Use service to get score, which triggers on-demand build if missing
            score = service.get_pair_score(id1, id2, algo=algo)
            scores[algo] = score

            # Fetch the actual document to extract tags
            sid = service._canonicalize_sid(collection, id1, id2, algo)
            raw_doc = service.r.get(sid)
            if raw_doc:
                d = json.loads(raw_doc)
                if d.get("tags") and not tags:
                    tags = d.get("tags")
                if pool_id:
                    p_tags = d.get(f"pool_tags_{pool_id}")
                    if p_tags and not user_tags:
                        user_tags = p_tags
                else:
                    if d.get("user_tags") and not user_tags:
                        user_tags = d.get("user_tags")

        return {
            "id1": id1,
            "id2": id2,
            "scores": scores,
            "tags": tags,
            "user_tags": user_tags,
            "source": "on-demand",
        }

    except Exception as e:
        return {"detail": f"Error retrieving similarity: {str(e)}"}, 500
