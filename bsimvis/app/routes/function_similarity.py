import json
from flask import request
from bsimvis.app.services import bsim_profiles
from bsimvis.similarity import registry
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
        # `algo` opts in to a subset of algorithms (comma-separated).
        requested = request.args.get("algo") or request.args.get("algos")
        if requested:
            algorithms = [a.strip() for a in requested.split(",") if a.strip()]
        else:
            algorithms = registry.names(milvus_enabled=milvus_service.enabled)

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
                # Report significance alongside weighted score.
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


# Args that describe the anchor or the pair, not the partner function, so they
# are not forwarded to the partner-side function filter.
_PAIR_ARGS = {
    "collection",
    "pool",
    "md5",
    "address",
    "min_score",
    "max_score",
    "limit",
    "offset",
    "cross_binary",
    "use_cache",
    "sort_by",
    "sort_order",
    "match_mode",
    "format",
}
_META_FIELDS = (
    "file_md5",
    "file_name",
    "entrypoint_address",
    "tags",
    "user_tags",
    "language_id",
    "return_type",
    "namespace",
    "parameters",
    "bsim_features_count",
    "caller_count",
    "callee_count",
    "note_owners",
)


def _anchor():
    """`(anchor fid, pool_id, error)` from the query string or JSON body."""
    data = (request.get_json(silent=True) or {}) if request.method == "POST" else {}

    def get(k):
        return data.get(k) or request.args.get(k)

    collection, md5, address = get("collection"), get("md5"), get("address")
    if not (collection and md5 and address):
        return None, None, ({"error": "collection, md5 and address required"}, 400)
    # With ?pool= the app rewrites collection to global:pool:{id}:col:{coll};
    # the anchor's own id is always the origin collection's.
    collection = collection.split(":col:")[-1]
    return f"{collection}:func:{md5}:{address}", get("pool") or None, None


def _metas(r, ids):
    raw = r.mget([f"{i}:meta" for i in ids]) if ids else []
    out = {}
    for i, m in zip(ids, raw):
        m = json.loads(m) if m else {}
        if isinstance(m, list):
            m = m[0] if m else {}
        meta = {k: m.get(k) for k in _META_FIELDS}
        meta["collection"] = i.split(":")[0]
        meta["function_name"] = m.get("user_name") or m.get("function_name")
        out[i] = meta
    return out


def discover_similar():
    """Live partners of one function below the stored edges' min_score.

    Read-only. Rows use the /api/similarity/search pair shape with `live: true`
    and no `sid`; partner filters go through /api/function/search's own parsing.
    """
    from werkzeug.datastructures import MultiDict
    from bsimvis.app.routes.home import _call
    from bsimvis.app.routes.search_function import search_functions
    from bsimvis.app.services.bin_sim_service import discovery_floor
    from bsimvis.app.services.config_service import config_service

    fid, pool_id, err = _anchor()
    if err:
        return err
    try:
        floor = discovery_floor()
        min_score = max(floor, float(request.args.get("min_score") or floor))
        max_score = float(request.args.get("max_score") or 1.0)
        limit = max(1, min(int(request.args.get("limit") or 50), 1000))
        top_k = int(config_service.get("similarity.top_k", 1000))
    except ValueError:
        return {"error": "Invalid numeric parameter"}, 400

    service = SimilarityService()
    try:
        found = service.discover_live(fid, min_score, top_k, pool_id=pool_id)
    except LookupError as e:
        return {"error": str(e)}, 404
    base = {
        "algo": found["algo"],
        "min_score": min_score,
        "floor": floor,
        "pairs": [],
        "total": 0,
    }
    if found.get("below_min_features"):
        return dict(base, below_min_features=True, min_features=found["min_features"])

    anchor_md5 = fid.split(":")[-2]
    cross = request.args.get("cross_binary")
    cands = [c for c in found["candidates"] if c["score"] <= max_score]
    if cross in ("true", "false"):
        want_cross = cross == "true"
        cands = [
            c for c in cands if (c["id"].split(":")[-2] != anchor_md5) == want_cross
        ]
    # A live pair carries no sim tags yet, so a positive sim-tag filter matches none.
    if any(
        request.args.getlist(k) for k in ("sim_tag", "sim_static_tag", "sim_user_tag")
    ):
        cands = []

    partner_args = {
        k: v
        for k, v in MultiDict(request.args).lists()
        if k not in _PAIR_ARGS and not k.startswith(("sim_", "exclude_sim_"))
    }
    if cands and partner_args:
        scope = {"pool": pool_id} if pool_id else {"collection": fid.split(":")[0]}
        res = _call(
            search_functions, **partner_args, **scope, ids=[c["id"] for c in cands]
        )
        if res.get("error"):
            return {"error": res["error"]}, 400
        keep = set(res.get("ids") or [])
        cands = [c for c in cands if c["id"] in keep]

    page = cands[:limit]
    metas = _metas(service.r, [fid] + [c["id"] for c in page])
    m1 = metas[fid]
    pairs = [
        {
            "id1": fid,
            "id2": c["id"],
            "name1": m1.get("function_name"),
            "name2": metas[c["id"]].get("function_name"),
            "score": c["score"],
            "feat_count": int(c["c_total"]),
            "meta1": m1,
            "meta2": metas[c["id"]],
            "shared_clusters": [],
            "tags": [],
            "user_tags": [],
            "algo": found["algo"],
            "live": True,
        }
        for c in page
    ]
    return dict(base, pairs=pairs, total=len(cands))


def persist_discovered():
    """Store chosen live pairs as real edges tagged `discovered`.

    JSON body: collection, md5, address (the anchor), partner_ids, optional pool.
    """
    from bsimvis.app.services.bin_sim_service import discovery_floor

    fid, pool_id, err = _anchor()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    partner_ids = data.get("partner_ids") or []
    if not isinstance(partner_ids, list) or not partner_ids:
        return {"error": "partner_ids must be a non-empty list"}, 400
    if len(partner_ids) > 1000:
        return {"error": "at most 1000 partner_ids"}, 400
    try:
        return SimilarityService().persist_live(
            fid, [str(p) for p in partner_ids], discovery_floor(), pool_id=pool_id
        )
    except LookupError as e:
        return {"error": str(e)}, 404
    except ValueError as e:
        return {"error": str(e)}, 400
