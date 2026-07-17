import json
import random
import logging
from .redis_client import get_redis
from bsimvis.app.services.index_service import (
    resolve_origin_collection,
    to_pool_indexed_id,
)


def _normalize_collection(collection, entity_id=None):
    return resolve_origin_collection(collection, entity_id)


def _to_pool_ids(ids, lvl, pool_id):
    """Batch form of to_pool_indexed_id; drops ids with no pool equivalent."""
    out = []
    for i in ids:
        i = i.decode() if isinstance(i, bytes) else i
        mapped = to_pool_indexed_id(i, lvl, pool_id)
        if mapped:
            out.append(mapped)
    return out


class TagService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def _resolve_doc_id(self, collection, entity_type, entity_id):
        """Resolves a frontend ID into a backend Redis key."""
        collection = _normalize_collection(collection, entity_id)
        resolved_id = entity_id.replace(":function:", ":func:")
        if ":col:" in resolved_id:
            resolved_id = resolved_id.split(":col:")[-1]

        if entity_type in ["file", "function"]:
            if resolved_id.endswith(":meta"):
                return resolved_id
            return f"{resolved_id}:meta"

        if entity_type == "similarity":
            if "|" in resolved_id:
                parts = resolved_id.split("|")
                if len(parts) == 3:
                    id1, id2, algo = parts
                    id1_clean = id1.replace(":function:", ":func:")
                    id2_clean = id2.replace(":function:", ":func:")
                    if ":col:" in id1_clean:
                        id1_clean = id1_clean.split(":col:")[-1]
                    if ":col:" in id2_clean:
                        id2_clean = id2_clean.split(":col:")[-1]

                    func_parts = id1_clean.split(":")
                    base_coll = func_parts[0] if func_parts else collection

                    func_prefix = f"{base_coll}:func:"
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
                        return f"{base_coll}:sim:{algo}:{c1}::{c2}"
                    else:
                        return f"{base_coll}:sim:{algo}:{c2}::{c1}"
            return resolved_id

        return resolved_id

    def _get_doc(self, doc_id):
        raw = self.r.get(doc_id)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception as e:
            logging.error(f"TagService: Error decoding JSON for key {doc_id}: {e}")
            return None

    def _set_doc(self, doc_id, doc):
        self.r.set(doc_id, json.dumps(doc))

    def add_user_tag(self, collection, entity_type, entity_id, tag):
        """
        Adds a user tag to an entity (file, function, or similarity).
        """
        collection = _normalize_collection(collection, entity_id)
        r = self.r
        tag = tag.strip()
        if not tag:
            return False

        try:
            doc_id = self._resolve_doc_id(collection, entity_type, entity_id)

            data = self._get_doc(doc_id)
            if not data:
                logging.error(
                    f"TagService: Entity {doc_id} not found (from {entity_id})"
                )
                return False

            json_field = "user_tags"
            user_tags = data.get(json_field, [])
            if not isinstance(user_tags, list):
                user_tags = []

            if tag not in user_tags:
                user_tags.append(tag)
                data[json_field] = user_tags
                self._set_doc(doc_id, data)

                # Update Secondary Index
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
                r.sadd(index_key, indexed_id)

                registry_key = f"{collection}:reg:{lvl}:user_tags"
                r.sadd(registry_key, index_key)

                # Propagate index to all associated pools
                associated_pools = r.smembers(f"{collection}:pools")
                for p_id in associated_pools:
                    p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                    pool_id_val = to_pool_indexed_id(indexed_id, lvl, p_id)
                    if not pool_id_val:
                        continue
                    pool_coll = f"global:pool:{p_id}"
                    pool_index_key = f"{pool_coll}:idx:{lvl}:user_tags:{tag_lower}"
                    r.sadd(pool_index_key, pool_id_val)
                    pool_registry_key = f"{pool_coll}:reg:{lvl}:user_tags"
                    r.sadd(pool_registry_key, pool_index_key)

                self._propagate_user_tag(
                    collection, entity_type, entity_id, tag, op="add"
                )

                self._ensure_tag_metadata(collection, tag)

            return True
        except Exception as e:
            logging.error(f"TagService: Error adding tag to {entity_id}: {e}")
            return False

    def remove_user_tag(self, collection, entity_type, entity_id, tag):
        """Removes a user tag from an entity."""
        collection = _normalize_collection(collection, entity_id)
        r = self.r
        tag = tag.strip()
        try:
            doc_id = self._resolve_doc_id(collection, entity_type, entity_id)

            json_field = "user_tags"
            data = self._get_doc(doc_id)
            if not data:
                return False

            user_tags = data.get(json_field, [])
            if not isinstance(user_tags, list):
                return False

            if tag in user_tags:
                user_tags.remove(tag)
                data[json_field] = user_tags
                self._set_doc(doc_id, data)

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

                # Propagate index removal to all associated pools
                associated_pools = r.smembers(f"{collection}:pools")
                for p_id in associated_pools:
                    p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                    pool_id_val = to_pool_indexed_id(indexed_id, lvl, p_id)
                    if not pool_id_val:
                        continue
                    pool_coll = f"global:pool:{p_id}"
                    pool_index_key = f"{pool_coll}:idx:{lvl}:user_tags:{tag_lower}"
                    r.srem(pool_index_key, pool_id_val)

                self._propagate_user_tag(
                    collection, entity_type, entity_id, tag, op="remove"
                )

            return True
        except Exception as e:
            logging.error(f"TagService: Error removing tag from {entity_id}: {e}")
            return False

    def bulk_add_user_tag(self, collection, entity_type, entity_ids, tag):
        """Adds a user tag to multiple entities."""
        collection = _normalize_collection(
            collection, entity_ids[0] if entity_ids else None
        )
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

            associated_pools = r.smembers(f"{collection}:pools")

            for eid in entity_ids:
                doc_id = self._resolve_doc_id(collection, entity_type, eid)
                data = self._get_doc(doc_id)
                if not data:
                    continue

                json_field = "user_tags"
                user_tags = data.get(json_field, [])
                if not isinstance(user_tags, list):
                    user_tags = []

                if tag not in user_tags:
                    user_tags.append(tag)
                    data[json_field] = user_tags
                    self._set_doc(doc_id, data)

                    indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
                    r.sadd(index_key, indexed_id)
                    r.sadd(registry_key, index_key)

                    # Propagate to pools
                    for p_id in associated_pools:
                        p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                        pool_id_val = to_pool_indexed_id(indexed_id, lvl, p_id)
                        if not pool_id_val:
                            continue
                        pool_coll = f"global:pool:{p_id}"
                        pool_index_key = f"{pool_coll}:idx:{lvl}:user_tags:{tag_lower}"
                        r.sadd(pool_index_key, pool_id_val)
                        pool_registry_key = f"{pool_coll}:reg:{lvl}:user_tags"
                        r.sadd(pool_registry_key, pool_index_key)

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
        collection = _normalize_collection(
            collection, entity_ids[0] if entity_ids else None
        )
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
            associated_pools = r.smembers(f"{collection}:pools")

            for eid in entity_ids:
                doc_id = self._resolve_doc_id(collection, entity_type, eid)
                data = self._get_doc(doc_id)
                if not data:
                    continue

                json_field = "user_tags"
                user_tags = data.get(json_field, [])
                if not isinstance(user_tags, list):
                    continue

                if tag in user_tags:
                    user_tags.remove(tag)
                    data[json_field] = user_tags
                    self._set_doc(doc_id, data)

                    indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
                    r.srem(index_key, indexed_id)

                    # Propagate removal to pools
                    for p_id in associated_pools:
                        p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                        pool_id_val = to_pool_indexed_id(indexed_id, lvl, p_id)
                        if not pool_id_val:
                            continue
                        pool_coll = f"global:pool:{p_id}"
                        pool_index_key = f"{pool_coll}:idx:{lvl}:user_tags:{tag_lower}"
                        r.srem(pool_index_key, pool_id_val)

                    self._propagate_user_tag(
                        collection, entity_type, eid, tag, op="remove"
                    )

            return True
        except Exception as e:
            logging.error(f"TagService: Error in bulk remove tags: {e}")
            return False

    def _ensure_tag_metadata(self, collection, tag):
        """Ensures a tag has metadata (color) in the global index."""
        collection = _normalize_collection(collection)
        meta_key = f"{collection}:tags_metadata"
        if not self.r.hexists(meta_key, tag):
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
            import hashlib

            tag_hash = int(hashlib.md5(tag.encode()).hexdigest(), 16)
            color = palette[tag_hash % len(palette)]

            self.r.hset(meta_key, tag, json.dumps({"color": color, "priority": 0}))

            # Propagate tag metadata to pools containing this collection
            associated_pools = self.r.smembers(f"{collection}:pools")
            for p_id in associated_pools:
                p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                pool_meta_key = f"global:pool:{p_id}:tags_metadata"
                if not self.r.hexists(pool_meta_key, tag):
                    self.r.hset(
                        pool_meta_key, tag, json.dumps({"color": color, "priority": 0})
                    )

    def get_tags(self, collection):
        """Returns the global tag index for a collection."""
        collection = _normalize_collection(collection)
        return self.get_collection_tags(collection)

    def get_collection_tags(self, collection):
        """Returns all tags (Analysis + User) and their metadata for a collection."""
        collection = _normalize_collection(collection)
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
        collection = _normalize_collection(collection)
        r = self.r
        tag_lower = tag.lower()
        stats = {"function": 0, "file": 0, "similarity": 0}

        for lvl in ["func", "file", "sim"]:
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
        collection = _normalize_collection(collection)
        meta_key = f"{collection}:tags_metadata"
        raw = self.r.hget(meta_key, tag)
        meta = json.loads(raw) if raw else {"priority": 0}
        meta["color"] = color
        self.r.hset(meta_key, tag, json.dumps(meta))

        # Propagate to pools
        associated_pools = self.r.smembers(f"{collection}:pools")
        for p_id in associated_pools:
            p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
            pool_meta_key = f"global:pool:{p_id}:tags_metadata"
            self.r.hset(pool_meta_key, tag, json.dumps(meta))
        return True

    def set_tag_priority(self, collection, tag, priority):
        collection = _normalize_collection(collection)
        meta_key = f"{collection}:tags_metadata"
        raw = self.r.hget(meta_key, tag)
        meta = json.loads(raw) if raw else {"color": "#66d9ef"}
        meta["priority"] = int(priority)
        self.r.hset(meta_key, tag, json.dumps(meta))

        # Propagate to pools
        associated_pools = self.r.smembers(f"{collection}:pools")
        for p_id in associated_pools:
            p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
            pool_meta_key = f"global:pool:{p_id}:tags_metadata"
            self.r.hset(pool_meta_key, tag, json.dumps(meta))
        return True

    def _propagate_user_tag(self, collection, entity_type, entity_id, tag, op="add"):
        """Propagates a user tag to other levels if configured in INDEX_CONFIG."""
        collection = _normalize_collection(collection, entity_id)
        from bsimvis.app.services.index_config import (
            get_propagation_targets,
            resolve_target_field,
        )

        src_level = (
            "func"
            if entity_type == "function"
            else "sim" if entity_type == "similarity" else entity_type
        )

        prop_levels = get_propagation_targets(src_level, "user_tags")
        if not prop_levels:
            return

        r = self.r
        tag_lower = tag.strip().lower()

        indexed_id = self._resolve_doc_id(collection, entity_type, entity_id)
        if indexed_id.endswith(":meta"):
            indexed_id = indexed_id[:-5]

        associated_pools = r.smembers(f"{collection}:pools")

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
                    id_list = list(related_ids)
                    if op == "add":
                        r.sadd(index_key, *id_list)
                        r.sadd(registry_key, index_key)
                        for p_id in associated_pools:
                            p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                            pool_ids = _to_pool_ids(id_list, target_lvl, p_id)
                            if not pool_ids:
                                continue
                            pool_index_key = f"global:pool:{p_id}:idx:{target_lvl}:{target_field}:{tag_lower}"
                            pool_registry_key = (
                                f"global:pool:{p_id}:reg:{target_lvl}:{target_field}"
                            )
                            r.sadd(pool_index_key, *pool_ids)
                            r.sadd(pool_registry_key, pool_index_key)
                    else:
                        r.srem(index_key, *id_list)
                        for p_id in associated_pools:
                            p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                            pool_ids = _to_pool_ids(id_list, target_lvl, p_id)
                            if not pool_ids:
                                continue
                            pool_index_key = f"global:pool:{p_id}:idx:{target_lvl}:{target_field}:{tag_lower}"
                            r.srem(pool_index_key, *pool_ids)

        elif src_level == "func":
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
                                for p_id in associated_pools:
                                    p_id = (
                                        p_id.decode()
                                        if isinstance(p_id, bytes)
                                        else p_id
                                    )
                                    pool_ids = _to_pool_ids(id_list, target_lvl, p_id)
                                    if not pool_ids:
                                        continue
                                    pool_index_key = f"global:pool:{p_id}:idx:{target_lvl}:{target_field}:{tag_lower}"
                                    pool_registry_key = f"global:pool:{p_id}:reg:{target_lvl}:{target_field}"
                                    r.sadd(pool_index_key, *pool_ids)
                                    r.sadd(pool_registry_key, pool_index_key)
                            else:
                                r.srem(index_key, *id_list)
                                for p_id in associated_pools:
                                    p_id = (
                                        p_id.decode()
                                        if isinstance(p_id, bytes)
                                        else p_id
                                    )
                                    pool_ids = _to_pool_ids(id_list, target_lvl, p_id)
                                    if not pool_ids:
                                        continue
                                    pool_index_key = f"global:pool:{p_id}:idx:{target_lvl}:{target_field}:{tag_lower}"
                                    r.srem(pool_index_key, *pool_ids)


tag_service = TagService()
