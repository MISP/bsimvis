from flask import request
from bsimvis.app.services.index_service import IndexStatsService

stats_service = IndexStatsService()


def get_index_status():
    """Returns database index statistics."""
    collection = request.args.get("collection", "main")
    details = request.args.get("details") == "true"

    stats = stats_service.get_collection_stats(collection, details=details)
    return stats
