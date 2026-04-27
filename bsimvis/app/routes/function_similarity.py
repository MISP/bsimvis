from flask import Blueprint, request, jsonify
from bsimvis.app.services.similarity_service import SimilarityService

function_similarity_bp = Blueprint("function_similarity", __name__)


@function_similarity_bp.route("/api/similarity", methods=["GET"])
def similarity_api():
    id1 = request.args.get("id1")
    id2 = request.args.get("id2")

    if not id1 or not id2:
        return jsonify({"detail": "Missing id1 or id2"}), 400

    try:
        service = SimilarityService()
        algorithms = ["jaccard", "unweighted_cosine"]
        scores = {}

        collection = id1.split(":")[0] if ":" in id1 else "main"
        tags = []
        user_tags = []

        for algo in algorithms:
            # Use service to get score, which triggers on-demand build if missing
            score = service.get_pair_score(id1, id2, algo=algo)
            scores[algo] = score

            # Fetch the actual document to extract tags
            sid = service._canonicalize_sid(collection, id1, id2, algo)
            doc = service.r.json().get(sid, "$")
            if doc:
                d = doc[0] if isinstance(doc, list) else doc
                if d.get("tags") and not tags:
                    tags = d.get("tags")
                if d.get("user_tags") and not user_tags:
                    user_tags = d.get("user_tags")

        return jsonify(
            {
                "id1": id1,
                "id2": id2,
                "scores": scores,
                "tags": tags,
                "user_tags": user_tags,
                "source": "on-demand",
            }
        )

    except Exception as e:
        return jsonify({"detail": f"Error retrieving similarity: {str(e)}"}), 500
