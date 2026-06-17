from flask import request
from bsimvis.app.services.tag_service import tag_service
from bsimvis.app.services.redis_client import get_redis
import logging


def add_tag():
    """
    Adds a user_tag to an entity.
    Payload: {
        "collection": str,
        "type": "file" | "function" | "similarity",
        "entry_id": str (the full Redis key or sid),
        "tag": str
    }
    """
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"

    etype = data.get("entity_type")
    entry_id = data.get("entity_id")
    tag = data.get("tag")

    if not all([collection, etype, entry_id, tag]):
        return {"error": "Missing parameters"}, 400

    if etype not in ["file", "function", "similarity"]:
        return {"error": "Invalid entity type"}, 400

    success = tag_service.add_user_tag(collection, etype, entry_id, tag)
    if success:
        return {"status": "success", "tag": tag}
    else:
        return {"status": "failed", "message": "Could not add tag"}, 500


def add_bulk_tags():
    """
    Adds a user_tag to multiple entities.
    Payload: {
        "collection": str,
        "entity_type": "file" | "function" | "similarity",
        "entity_ids": list[str],
        "tag": str
    }
    """
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"

    etype = data.get("entity_type")
    entity_ids = data.get("entity_ids")
    tag = data.get("tag")

    if not all([collection, etype, entity_ids, tag]) or not isinstance(
        entity_ids, list
    ):
        return {"error": "Missing or invalid parameters"}, 400

    success = tag_service.bulk_add_user_tag(collection, etype, entity_ids, tag)
    if success:
        return {"status": "success", "tag": tag, "count": len(entity_ids)}
    else:
        return {"status": "failed", "message": "Could not add tags"}, 500


def remove_tag():
    """Removes a user_tag from an entity."""
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"

    etype = data.get("entity_type")
    entry_id = data.get("entity_id")
    tag = data.get("tag")

    if not all([collection, etype, entry_id, tag]):
        return {"error": "Missing parameters"}, 400

    success = tag_service.remove_user_tag(collection, etype, entry_id, tag)
    if success:
        return {"status": "success", "tag": tag}
    else:
        return {"status": "failed", "message": "Could not remove tag"}, 500


def remove_bulk_tags():
    """Removes a user_tag from multiple entities."""
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"

    etype = data.get("entity_type")
    entity_ids = data.get("entity_ids")
    tag = data.get("tag")

    if not all([collection, etype, entity_ids, tag]) or not isinstance(
        entity_ids, list
    ):
        return {"error": "Missing or invalid parameters"}, 400

    success = tag_service.bulk_remove_user_tag(collection, etype, entity_ids, tag)
    if success:
        return {"status": "success", "tag": tag, "count": len(entity_ids)}
    else:
        return {"status": "failed", "message": "Could not remove tags"}, 500


def get_metadata():
    """Returns all tag metadata for a collection."""
    collection = request.args.get("collection")
    pool = request.args.get("pool") or request.args.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"

    if not collection:
        return {"error": "Missing collection"}, 400

    tags = tag_service.get_collection_tags(collection)
    return tags


def get_tag_stats():
    """Returns statistics for a specific tag."""
    collection = request.args.get("collection")
    pool = request.args.get("pool") or request.args.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"

    tag = request.args.get("tag")

    if not collection or not tag:
        return {"error": "Missing parameters"}, 400

    stats = tag_service.get_tag_stats(collection, tag)
    return stats


def get_tags():
    """Returns the global tag index for a collection."""
    collection = request.args.get("collection", "main")
    pool = request.args.get("pool") or request.args.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"

    tags = tag_service.get_tags(collection)
    return tags


def set_color():
    """Sets a custom color for a tag."""
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"

    tag = data.get("tag")
    color = data.get("color")

    if collection is None or tag is None or color is None:
        return {"error": "Missing parameters"}, 400

    tag_service.set_tag_color(collection, tag, color)
    return {"status": "success"}


def set_priority():
    """Sets a custom priority for a tag."""
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"

    tag = data.get("tag")
    priority = data.get("priority")

    if collection is None or tag is None or priority is None:
        return {"error": "Missing parameters"}, 400

    tag_service.set_tag_priority(collection, tag, priority)
    return {"status": "success"}
