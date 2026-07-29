from flask import request
from bsimvis.app.services.index_service import IndexStatsService

stats_service = IndexStatsService()


def get_index_status():
    """Returns database index statistics."""
    collection = request.args.get("collection", "main")
    details = request.args.get("details") == "true"

    stats = stats_service.get_collection_stats(collection, details=details)
    return stats


def get_languages():
    """Returns the Ghidra language IDs and their valid compiler specs."""
    from bsimvis.app.services.ghidra_lang_service import get_languages as languages

    result = languages()
    return {"total": len(result), "languages": result}


def get_config():
    """Returns the default configuration values."""
    from bsimvis.app.services.config_service import config_service

    return {
        "clustering": {
            "epsilon": config_service.get("clustering.epsilon", 0.001),
            "min_cluster_size": config_service.get("clustering.min_cluster_size", 2),
            "min_samples": config_service.get("clustering.min_samples", 1),
            "selection_method": config_service.get(
                "clustering.selection_method", "eom"
            ),
            "min_sim": config_service.get("clustering.min_sim", 0.0),
            "min_features": config_service.get("clustering.min_features", 0),
            "min_cohesion": config_service.get("clustering.min_cohesion", 0.5),
        },
        "similarity": {
            "top_k": config_service.get("similarity.top_k", 1000),
            "min_score": config_service.get("similarity.min_score", 0.9),
            "min_features": config_service.get("similarity.min_features", 0),
            "algo": config_service.get("similarity.algo", "unweighted_cosine"),
        },
    }
