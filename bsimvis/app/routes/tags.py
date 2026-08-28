from flask import request
from bsimvis.app.services.tag_service import tag_service
from bsimvis.app.services.redis_client import get_redis
import logging


def _collection_of(source):
    """Return the canonical collection namespace for request data."""
    pool = source.get("pool") or source.get("pool_id")
    if pool:
        return f"global:pool:{pool}"

    collection = source.get("collection")
    if collection and collection.startswith("pool:"):
        return f"global:{collection}"
    return collection


def _collection_for(data, etype, entry_id):
    """A bin_sim entity_id is a sid that already fully qualifies its own
    collection/pool scope -- unlike file/function/similarity ids, trust it
    over whatever collection/pool the client happened to post."""
    if etype == "bin_sim":
        return (entry_id or "").split(":bin_sim:", 1)[0] or None
    return _collection_of(data)


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
    etype = data.get("entity_type")
    entry_id = data.get("entity_id")
    tag = data.get("tag")
    collection = _collection_for(data, etype, entry_id)

    if not all([collection, etype, entry_id, tag]):
        return {"error": "Missing parameters"}, 400

    if etype not in ["file", "function", "similarity", "cluster", "bin_cluster", "bin_sim"]:
        return {"error": "Invalid entity type"}, 400

    success = tag_service.add_user_tag(
        collection,
        etype,
        entry_id,
        tag,
        data.get("algo", "unweighted_cosine"),
        data.get("node_type", "file"),
    )
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
    collection = _collection_of(data)

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
    etype = data.get("entity_type")
    entry_id = data.get("entity_id")
    tag = data.get("tag")
    collection = _collection_for(data, etype, entry_id)

    if not all([collection, etype, entry_id, tag]):
        return {"error": "Missing parameters"}, 400

    if etype not in ["file", "function", "similarity", "cluster", "bin_cluster", "bin_sim"]:
        return {"error": "Invalid entity type"}, 400

    success = tag_service.remove_user_tag(
        collection,
        etype,
        entry_id,
        tag,
        data.get("algo", "unweighted_cosine"),
        data.get("node_type", "file"),
    )
    if success:
        return {"status": "success", "tag": tag}
    else:
        return {"status": "failed", "message": "Could not remove tag"}, 500


def remove_bulk_tags():
    """Removes a user_tag from multiple entities."""
    data = request.json or {}
    collection = _collection_of(data)

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
    collection = _collection_of(request.args)

    if not collection:
        return {"error": "Missing collection"}, 400

    tags = tag_service.get_collection_tags(collection)
    return tags


def get_tag_stats():
    """Returns statistics for a specific tag."""
    collection = _collection_of(request.args)

    tag = request.args.get("tag")

    if not collection or not tag:
        return {"error": "Missing parameters"}, 400

    stats = tag_service.get_tag_stats(collection, tag)
    return stats


def get_tags():
    """Returns the global tag index for a collection."""
    collection = _collection_of(request.args) or "main"

    tags = tag_service.get_tags(collection)
    return tags


def get_tag_provenance():
    """Returns `{tag: [source record, ...]}` for the requested tags.

    Deliberately its own endpoint rather than a field on the tag list: this is
    what a click on a tag asks for, and folding it into `/api/tags/` would put
    a Redis read per tag on the path of every page that renders a tag chip.

    No collection parameter -- which rule file a tag came from is a fact about
    the ruleset, not about a collection.
    """
    raw = request.args.getlist("tag") or []
    if not raw:
        raw = (request.args.get("tags") or "").split(",")
    tags = [t.strip() for t in raw if t and t.strip()]

    if not tags:
        return {"error": "Missing parameters"}, 400

    from bsimvis.app.services.tag_provenance import tag_rules, _row_from_id, rule_url

    out, counts = {}, {}
    for tag in tags:
        # `tag_rules` is paged, and for a broad mirror tag the count *is* the
        # answer -- 50 rows out of 21k must not read as "these are the rules".
        total, rules = tag_rules(tag)

        if not rules and tag.startswith("capa:"):
            ns = tag.split("capa:", 1)[1].replace(":", "/")
            rid = "capa:" + ns
            row = _row_from_id(rid)
            row["url"] = rule_url(rid, row)
            rules = {rid: row}

        res = []
        for rid, row in rules.items():
            if "id" not in row:
                row["id"] = rid
            res.append(row)
        out[tag] = res
        counts[tag] = total or len(res)

    return {"provenance": out, "counts": counts}


def get_rule_source():
    """Returns `{id, text}` for one rule -- its source, read on demand.

    Its own endpoint rather than a field on `/provenance`: a provenance answer
    can carry 50 rules and nobody reads 50 rule bodies, so the text is fetched
    only for the one the popup is actually showing.
    """
    rid = (request.args.get("id") or "").strip()
    if not rid:
        return {"error": "Missing parameters"}, 400

    from bsimvis.app.services.tag_provenance import rule_text

    return {"id": rid, "text": rule_text(rid)}


def get_match_provenance():
    """Returns {entity_id: {tag: [rule_id, ...]}} for given entities."""
    data = request.json or {}
    collection = _collection_of(data)
    entity_ids = data.get("entity_ids", [])
    if not collection or not entity_ids:
        return {"error": "Missing parameters"}, 400

    from bsimvis.app.services.tag_provenance import match_provenance

    out, rules = match_provenance(collection, entity_ids)

    for rid, row in rules.items():
        if "id" not in row:
            row["id"] = rid

    return {"hits": out, "rules": rules}


def get_color_config():
    """The parameters the UI needs to derive a tag's colour from its id.

    The rule lives in `tag_taxonomy`; only its parameters travel, because the
    browser colours folded ids the backend never sees.
    """
    from bsimvis.app.services.tag_taxonomy import color_config

    return color_config()


def set_color():
    """Sets a custom color for a tag."""
    data = request.json or {}
    collection = _collection_of(data)

    tag = data.get("tag")
    color = data.get("color")

    if collection is None or tag is None or color is None:
        return {"error": "Missing parameters"}, 400

    tag_service.set_tag_color(collection, tag, color)
    return {"status": "success"}


def set_priority():
    """Sets a custom priority for a tag."""
    data = request.json or {}
    collection = _collection_of(data)

    tag = data.get("tag")
    priority = data.get("priority")

    if collection is None or tag is None or priority is None:
        return {"error": "Missing parameters"}, 400

    tag_service.set_tag_priority(collection, tag, priority)
    return {"status": "success"}


def list_tags():
    """Tag vocabulary for a collection, with usage counts, as a sortable list."""
    collection = _collection_of(request.args)
    if not collection:
        return {"error": "Missing collection"}, 400

    meta = tag_service.get_collection_tags(collection)
    cluster_stats = tag_service.get_cluster_tag_stats(collection)
    q = (request.args.get("q") or "").lower().strip()

    items = []
    for tag, m in meta.items():
        if q and q not in tag.lower():
            continue
        stats = tag_service.get_tag_stats(collection, tag, cluster_stats)
        items.append(
            {
                "tag": tag,
                "color": m.get("color"),
                "priority": m.get("priority", 0),
                "llm": bool(m.get("llm")),
                "function_count": stats.get("function", 0),
                "file_count": stats.get("file", 0),
                "similarity_count": stats.get("similarity", 0),
                "total_count": sum(stats.values()),
            }
        )

    sort_by = request.args.get("sort_by") or "tag"
    reverse = (request.args.get("sort_order") or "asc").lower() == "desc"
    keyfn = {
        "tag": lambda i: i["tag"].lower(),
        "priority": lambda i: i["priority"],
        "total_count": lambda i: i["total_count"],
        "function_count": lambda i: i["function_count"],
        "file_count": lambda i: i["file_count"],
    }.get(sort_by, lambda i: i["tag"].lower())
    items.sort(key=keyfn, reverse=reverse)

    return {"total": len(items), "items": items, "collection": collection}


def create_tag():
    """Creates a vocabulary entry for a tag without tagging any entity."""
    data = request.json or {}
    collection = _collection_of(data)
    tag = (data.get("tag") or "").strip()

    if not collection or not tag:
        return {"error": "Missing parameters"}, 400

    existing = tag_service.get_collection_tags(collection)
    if tag in existing:
        return {"error": f"Tag '{tag}' already exists"}, 409

    tag_service.create_tag(
        collection,
        tag,
        color=data.get("color"),
        priority=data.get("priority") or 0,
        llm=bool(data.get("llm")),
    )
    return {"status": "success", "tag": tag}


def delete_tag():
    """Deletes a tag from the vocabulary AND from every entity carrying it."""
    data = request.json or {}
    collection = _collection_of(data)
    tag = (data.get("tag") or "").strip()

    if not collection or not tag:
        return {"error": "Missing parameters"}, 400

    removed = tag_service.delete_tag(collection, tag)
    if removed is None:
        return {"status": "failed", "message": "Could not delete tag"}, 500
    return {"status": "success", "tag": tag, "removed": removed}


def set_llm_flag():
    """Includes/excludes a tag from the LLM tagging vocabulary."""
    data = request.json or {}
    collection = _collection_of(data)
    tag = data.get("tag")
    enabled = data.get("llm")

    if not collection or not tag or enabled is None:
        return {"error": "Missing parameters"}, 400

    tag_service.set_tag_llm(collection, tag, bool(enabled))
    return {"status": "success", "tag": tag, "llm": bool(enabled)}
