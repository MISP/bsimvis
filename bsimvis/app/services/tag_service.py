import json
import random
import logging
from .redis_client import get_redis


class TagService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def _resolve_doc_id(self, collection, entity_type, entity_id):
        """Resolves a frontend ID into a backend Redis key."""
        if entity_type in ["file", "function"]:
            # Standardized IDs: {col}:file:{id} or {col}:func:{id}
            resolved_id = entity_id.replace(":function:", ":func:")
            if resolved_id.endswith(":meta"):
                return resolved_id
            return f"{resolved_id}:meta"

        if entity_type == "similarity":
            # Similarity IDs might be passed as "id1|id2|algo" from the UI
            if "|" in entity_id:
                parts = entity_id.split("|")
                if len(parts) == 3:
                    id1, id2, algo = parts
                    # Standard Canonical SID: {coll}:sim:{algo}:{c1}::{c2}
                    id1_clean = id1.replace(":function:", ":func:")
                    id2_clean = id2.replace(":function:", ":func:")
                    func_prefix = f"{collection}:func:"
                    c1 = (
                        id1_clean[len(func_prefix) :]
                        if id1_clean.startswith(func_prefix)
                        else id1_clean
                    )
                    c2 = (
                        id2_clean[len(func_prefix) :]
                        if id2_clean.startswith(func_prefix)
                        else id2_clean
                    )

                    if c1 > c2:
                        return f"{collection}:sim:{algo}:{c1}::{c2}"
                    else:
                        return f"{collection}:sim:{algo}:{c2}::{c1}"
            return entity_id

        return entity_id

    def add_user_tag(self, collection, entity_type, entity_id, tag):
        """
        Adds a user tag to an entity (file, function, or similarity).
        """
        r = self.r
        tag = tag.strip()
        if not tag:
            return False

        try:
            # 1. Resolve to the actual JSON document key
            doc_id = self._resolve_doc_id(collection, entity_type, entity_id)

            # 2. Update the JSON document
            doc = r.json().get(doc_id, "$")
            if not doc:
                logging.error(
                    f"TagService: Entity {doc_id} not found (from {entity_id})"
                )
                return False

            data = doc[0] if isinstance(doc, list) else doc
            user_tags = data.get("user_tags", [])

            if tag not in user_tags:
                user_tags.append(tag)
                r.json().set(doc_id, "$.user_tags", user_tags)

                # 3. Update Secondary Index
                tag_lower = tag.lower()
                lvl = (
                    "func"
                    if entity_type == "function"
                    else "sim" if entity_type == "similarity" else entity_type
                )

                # We store the BASE IDENTITY in the bucket (e.g. col:func:md5:addr)
                indexed_id = doc_id
                if indexed_id.endswith(":meta"):
                    indexed_id = indexed_id[:-5]

                # Standard Bucket: {col}:idx:{lvl}:user_tags:{tag}
                index_key = f"{collection}:idx:{lvl}:user_tags:{tag_lower}"
                r.sadd(index_key, indexed_id)

                # Standard Registry: {col}:reg:{lvl}:user_tags
                registry_key = f"{collection}:reg:{lvl}:user_tags"
                r.sadd(registry_key, index_key)

                # 4. Handle Propagation
                self._propagate_user_tag(
                    collection, entity_type, entity_id, tag, op="add"
                )

                # 5. Ensure metadata
                self._ensure_tag_metadata(collection, tag)

            return True
        except Exception as e:
            logging.error(f"TagService: Error adding tag to {entity_id}: {e}")
            return False

    def remove_user_tag(self, collection, entity_type, entity_id, tag):
        """Removes a user tag from an entity."""
        r = self.r
        tag = tag.strip()
        try:
            doc_id = self._resolve_doc_id(collection, entity_type, entity_id)

            doc = r.json().get(doc_id, "$.user_tags")
            if not doc or not isinstance(doc, list) or len(doc) == 0:
                return False

            user_tags = doc[0]
            if tag in user_tags:
                user_tags.remove(tag)
                r.json().set(doc_id, "$.user_tags", user_tags)

                # Update Index
                tag_lower = tag.lower()
                lvl = (
                    "func"
                    if entity_type == "function"
                    else "sim" if entity_type == "similarity" else entity_type
                )

                indexed_id = doc_id
                if indexed_id.endswith(":meta"):
                    indexed_id = indexed_id[:-5]

                index_key = f"{collection}:idx:{lvl}:user_tags:{tag_lower}"
                r.srem(index_key, indexed_id)

                # Handle Propagation
                self._propagate_user_tag(
                    collection, entity_type, entity_id, tag, op="remove"
                )

            return True
        except Exception as e:
            logging.error(f"TagService: Error removing tag from {entity_id}: {e}")
            return False

    def bulk_add_user_tag(self, collection, entity_type, entity_ids, tag):
        """Adds a user tag to multiple entities."""
        r = self.r
        tag = tag.strip()
        if not tag:
            return False

        try:
            tag_lower = tag.lower()
            lvl = (
                "func"
                if entity_type == "function"
                else "sim" if entity_type == "similarity" else entity_type
            )
            registry_key = f"{collection}:reg:{lvl}:user_tags"
            index_key = f"{collection}:idx:{lvl}:user_tags:{tag_lower}"

            pipe = r.pipeline()
            for eid in entity_ids:
                doc_id = self._resolve_doc_id(collection, entity_type, eid)
                # Note: Still need to check if tag exists to avoid duplicates
                # For simplicity in bulk, we'll do it sequentially but we could optimize further with Lua
                doc = r.json().get(doc_id, "$")
                if not doc or not isinstance(doc, list) or len(doc) == 0:
                    continue

                data = doc[0] if isinstance(doc, list) else doc
                user_tags = data.get("user_tags", [])
                if not isinstance(user_tags, list):
                    user_tags = []

                if tag not in user_tags:
                    user_tags.append(tag)
                    r.json().set(doc_id, "$.user_tags", user_tags)

                    indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
                    r.sadd(index_key, indexed_id)
                    r.sadd(registry_key, index_key)

                    # Handle Propagation
                    self._propagate_user_tag(
                        collection, entity_type, eid, tag, op="add"
                    )

            self._ensure_tag_metadata(collection, tag)
            return True
        except Exception as e:
            logging.error(f"TagService: Error in bulk add tags: {e}")
            return False

    def bulk_remove_user_tag(self, collection, entity_type, entity_ids, tag):
        """Removes a user tag from multiple entities."""
        r = self.r
        tag = tag.strip()
        try:
            tag_lower = tag.lower()
            lvl = (
                "func"
                if entity_type == "function"
                else "sim" if entity_type == "similarity" else entity_type
            )
            index_key = f"{collection}:idx:{lvl}:user_tags:{tag_lower}"

            for eid in entity_ids:
                doc_id = self._resolve_doc_id(collection, entity_type, eid)
                doc = r.json().get(doc_id, "$")
                if not doc or not isinstance(doc, list) or len(doc) == 0:
                    continue

                data = doc[0] if isinstance(doc, list) else doc
                user_tags = data.get("user_tags", [])
                if not isinstance(user_tags, list):
                    continue

                if tag in user_tags:
                    user_tags.remove(tag)
                    r.json().set(doc_id, "$.user_tags", user_tags)

                    indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
                    r.srem(index_key, indexed_id)

                    # Handle Propagation
                    self._propagate_user_tag(
                        collection, entity_type, eid, tag, op="remove"
                    )

            return True
        except Exception as e:
            logging.error(f"TagService: Error in bulk remove tags: {e}")
            return False

    def _ensure_tag_metadata(self, collection, tag):
        """Ensures a tag has metadata (color) in the global index."""
        meta_key = f"{collection}:tags_metadata"
        if not self.r.hexists(meta_key, tag):
            # Deterministic color based on tag name if we want, or just a better palette
            palette = [
                "#FF5555",
                "#50FA7B",
                "#F1FA8C",
                "#BD93F9",
                "#FF79C6",
                "#8BE9FD",
                "#FFB86C",
                "#A6E22E",
                "#66D9EF",
                "#FFD700",
                "#FF69B4",
                "#7B68EE",
                "#48D1CC",
                "#00FF7F",
                "#F4A460",
            ]
            # Use hash of tag name to pick a stable default color from palette
            import hashlib

            tag_hash = int(hashlib.md5(tag.encode()).hexdigest(), 16)
            color = palette[tag_hash % len(palette)]

            self.r.hset(meta_key, tag, json.dumps({"color": color, "priority": 0}))

    def get_tags(self, collection):
        """Returns the global tag index for a collection."""
        r = get_redis()
        # Tags are stored in a hash bsimvis:{collection}:tags:meta or similar
        # Based on routes, it seems we need to return metadata (color, priority)
        return self.get_collection_tags(collection)

    def get_collection_tags(self, collection):
        """Returns all tags (Analysis + User) and their metadata for a collection."""
        r = self.r
        meta_key = f"{collection}:tags_metadata"
        raw_meta = r.hgetall(meta_key)

        results = {}
        for k, v in raw_meta.items():
            tag_name = k.decode() if isinstance(k, bytes) else k
            meta = json.loads(v)
            results[tag_name] = meta

        return results

    def get_tag_stats(self, collection, tag):
        """Returns count breakdown by entity type for a given tag."""
        r = self.r
        tag_lower = tag.lower()
        stats = {"function": 0, "file": 0, "similarity": 0}

        # Check buckets for each level
        for lvl in ["func", "file", "sim"]:
            # Note: tags can be in 'tags' or 'user_tags' index buckets
            for field in ["tags", "user_tags"]:
                bkey = f"{collection}:idx:{lvl}:{field}:{tag_lower}"
                count = r.scard(bkey)
                if count > 0:
                    etype = (
                        "function"
                        if lvl == "func"
                        else "file" if lvl == "file" else "similarity"
                    )
                    stats[etype] += count
        return stats

    def set_tag_color(self, collection, tag, color):
        meta_key = f"{collection}:tags_metadata"
        raw = self.r.hget(meta_key, tag)
        meta = json.loads(raw) if raw else {"priority": 0}
        meta["color"] = color
        self.r.hset(meta_key, tag, json.dumps(meta))
        return True

    def set_tag_priority(self, collection, tag, priority):
        meta_key = f"{collection}:tags_metadata"
        raw = self.r.hget(meta_key, tag)
        meta = json.loads(raw) if raw else {"color": "#66d9ef"}
        meta["priority"] = int(priority)
        self.r.hset(meta_key, tag, json.dumps(meta))
        return True

    def _propagate_user_tag(self, collection, entity_type, entity_id, tag, op="add"):
        """Propagates a user tag to other levels if configured in INDEX_CONFIG."""
        from bsimvis.app.services.index_config import (
            get_propagation_targets,
            resolve_target_field,
        )

        # 1. Determine source level
        src_level = (
            "func"
            if entity_type == "function"
            else "sim" if entity_type == "similarity" else entity_type
        )

        # 2. Get targets for user_tags from this source level
        prop_levels = get_propagation_targets(src_level, "user_tags")
        if not prop_levels:
            return

        r = self.r
        tag_lower = tag.strip().lower()

        # Resolve to indexed_id (base identity)
        indexed_id = self._resolve_doc_id(collection, entity_type, entity_id)
        if indexed_id.endswith(":meta"):
            indexed_id = indexed_id[:-5]

        # 3. Handle File -> Func/Sim
        if src_level == "file":
            md5 = indexed_id.split(":")[-1]
            for target_lvl in prop_levels:
                target_field = resolve_target_field(src_level, target_lvl, "user_tags")
                index_key = f"{collection}:idx:{target_lvl}:{target_field}:{tag_lower}"
                registry_key = f"{collection}:reg:{target_lvl}:{target_field}"

                if target_lvl == "func":
                    related_ids = r.smembers(f"{collection}:idx:file:functions:{md5}")
                elif target_lvl == "sim":
                    related_ids = r.smembers(f"{collection}:sim:involves:file:{md5}")
                else:
                    continue

                if related_ids:
                    # Convert to list to avoid *set issues if empty (though checked above)
                    id_list = list(related_ids)
                    if op == "add":
                        r.sadd(index_key, *id_list)
                        r.sadd(registry_key, index_key)
                    else:
                        r.srem(index_key, *id_list)

        # 4. Handle Func -> Sim
        elif src_level == "func":
            # indexed_id is {coll}:func:{md5}:{addr}
            parts = indexed_id.split(":")
            if len(parts) >= 4:
                clean_id = f"{parts[-2]}:{parts[-1]}"
                for target_lvl in prop_levels:
                    if target_lvl == "sim":
                        target_field = resolve_target_field(
                            src_level, target_lvl, "user_tags"
                        )
                        index_key = (
                            f"{collection}:idx:{target_lvl}:{target_field}:{tag_lower}"
                        )
                        registry_key = f"{collection}:reg:{target_lvl}:{target_field}"

                        related_ids = r.smembers(
                            f"{collection}:sim:involves:func:{clean_id}"
                        )
                        if related_ids:
                            id_list = list(related_ids)
                            if op == "add":
                                r.sadd(index_key, *id_list)
                                r.sadd(registry_key, index_key)
                            else:
                                r.srem(index_key, *id_list)


tag_service = TagService()
