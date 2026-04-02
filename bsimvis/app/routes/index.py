from flask import Blueprint, request, jsonify
from bsimvis.app.services.index_service import IndexStatsService

index_bp = Blueprint("index", __name__)
stats_service = IndexStatsService()

@index_bp.route("/api/index/status", methods=["GET"])
def get_index_status():
    """Returns database index statistics."""
    collection = request.args.get("collection", "main")
    details = request.args.get("details") == "true"
    
    stats = stats_service.get_collection_stats(collection, details=details)
    return jsonify(stats)
