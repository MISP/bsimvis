from flask import Blueprint, request, jsonify
from bsimvis.app.services.redis_client import get_redis

function_similarity_bp = Blueprint("function_similarity", __name__)


@function_similarity_bp.route("/api/similarity", methods=["GET"])
def similarity_api():
    id1 = request.args.get("id1")
    id2 = request.args.get("id2")

    if not id1 or not id2:
        return jsonify({"detail": "Missing id1 or id2"}), 400

    try:
        coll1 = id1.split(":")[0]
        r = get_redis()

        algorithms = ["jaccard", "unweighted_cosine"]
        scores = {}

        for algo in algorithms:
            # Correcting format to match bsimvis_sim.py and index_service.py
            # Format: coll:sim_meta:algo:id1:id2
            key1 = f"{coll1}:sim_meta:{algo}:{id1}:{id2}"
            key2 = f"{coll1}:sim_meta:{algo}:{id2}:{id1}"

            zset_key = f"{coll1}:all_sim:{algo}"
            score = r.zscore(zset_key, key1)
            if score is None:
                score = r.zscore(zset_key, key2)

            scores[algo] = float(score) if score is not None else None

        return jsonify({"id1": id1, "id2": id2, "scores": scores, "source": "baked"})

    except Exception as e:
        return jsonify({"detail": f"Error retrieving similarity: {str(e)}"}), 500
