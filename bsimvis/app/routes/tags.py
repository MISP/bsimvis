from flask import Blueprint, jsonify, request
from bsimvis.app.services.tag_service import tag_service
from bsimvis.app.services.redis_client import get_redis
import logging

tags_bp = Blueprint("tags", __name__)

@tags_bp.route("/api/tags/add", methods=["POST"])
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
    data = request.json
    collection = data.get("collection")
    etype = data.get("entity_type")
    entry_id = data.get("entity_id")
    tag = data.get("tag")

    if not all([collection, etype, entry_id, tag]):
        return jsonify({"error": "Missing parameters"}), 400

    if etype not in ["file", "function", "similarity"]:
        return jsonify({"error": "Invalid entity type"}), 400

    success = tag_service.add_user_tag(collection, etype, entry_id, tag)
    if success:
        return jsonify({"status": "success", "tag": tag})
    else:
        return jsonify({"status": "failed", "message": "Could not add tag"}), 500

@tags_bp.route("/api/tags/remove", methods=["POST"])
def remove_tag():
    """Removes a user_tag from an entity."""
    data = request.json
    collection = data.get("collection")
    etype = data.get("entity_type")
    entry_id = data.get("entity_id")
    tag = data.get("tag")

    if not all([collection, etype, entry_id, tag]):
        return jsonify({"error": "Missing parameters"}), 400

    success = tag_service.remove_user_tag(collection, etype, entry_id, tag)
    if success:
        return jsonify({"status": "success", "tag": tag})
    else:
        return jsonify({"status": "failed", "message": "Could not remove tag"}), 500

@tags_bp.route("/api/tags/metadata", methods=["GET"])
def get_metadata():
    """Returns all tag metadata for a collection."""
    collection = request.args.get("collection")
    if not collection:
        return jsonify({"error": "Missing collection"}), 400
    
    tags = tag_service.get_collection_tags(collection)
    return jsonify(tags)

@tags_bp.route("/api/tags/set_color", methods=["POST"])
def set_color():
    """Sets a custom color for a tag."""
    data = request.json
    collection = data.get("collection")
    tag = data.get("tag")
    color = data.get("color")
    
    if not all([collection, tag, color]):
        return jsonify({"error": "Missing parameters"}), 400
        
    tag_service.set_tag_color(collection, tag, color)
    return jsonify({"status": "success"})

@tags_bp.route("/api/tags/set_priority", methods=["POST"])
def set_priority():
    """Sets a custom priority for a tag."""
    data = request.json
    collection = data.get("collection")
    tag = data.get("tag")
    priority = data.get("priority")
    
    if not all([collection, tag, priority]):
        return jsonify({"error": "Missing parameters"}), 400
        
    tag_service.set_tag_priority(collection, tag, priority)
    return jsonify({"status": "success"})
