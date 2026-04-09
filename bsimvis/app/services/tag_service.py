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
            if entity_id.endswith(":meta"):
                return entity_id
            return f"{entity_id}:meta"
        
        if entity_type == "similarity":
            # Similarity IDs might be passed as "id1|id2|algo" from the new UI
            if "|" in entity_id:
                parts = entity_id.split("|")
                if len(parts) == 3:
                    id1, id2, algo = parts
                    k1 = f"{collection}:sim_meta:{algo}:{id1}:{id2}"
                    if self.r.exists(k1): return k1
                    k2 = f"{collection}:sim_meta:{algo}:{id2}:{id1}"
                    if self.r.exists(k2): return k2
            # Or they might be passed as raw SIDs already
            return entity_id
        
        return entity_id

    def add_user_tag(self, collection, entity_type, entity_id, tag):
        """
        Adds a user tag to an entity (file, function, or similarity).
        entity_type: 'file', 'function', or 'similarity'
        entity_id: The identifier from the UI or index
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
                logging.error(f"TagService: Entity {doc_id} not found (from {entity_id})")
                return False
            
            data = doc[0] if isinstance(doc, list) else doc
            user_tags = data.get("user_tags", [])
            
            if tag not in user_tags:
                user_tags.append(tag)
                r.json().set(doc_id, "$.user_tags", user_tags)
                
                # 3. Update Secondary Index
                tag_lower = tag.lower()
                idx_prefix = "sim" if entity_type == "similarity" else entity_type
                
                # We always store the full doc_id in the index for search compatibility
                # EXCEPT for files/functions where we store the base ID (without :meta) 
                # because the search engine expects base IDs to build display info.
                indexed_id = doc_id
                if entity_type in ["file", "function"] and indexed_id.endswith(":meta"):
                    indexed_id = indexed_id[:-5]
                
                index_key = f"idx:{collection}:{idx_prefix}:user_tags:{tag_lower}"
                r.sadd(index_key, indexed_id)
                
                # 4. Register index key
                registry_key = f"idx:{collection}:reg:{idx_prefix}:user_tags"
                r.sadd(registry_key, index_key)
                
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
                idx_prefix = "sim" if entity_type == "similarity" else entity_type
                
                indexed_id = doc_id
                if entity_type in ["file", "function"] and indexed_id.endswith(":meta"):
                    indexed_id = indexed_id[:-5]
                    
                index_key = f"idx:{collection}:{idx_prefix}:user_tags:{tag_lower}"
                r.srem(index_key, indexed_id)
                
            return True
        except Exception as e:
            logging.error(f"TagService: Error removing tag from {entity_id}: {e}")
            return False

    def _ensure_tag_metadata(self, collection, tag):
        """Ensures a tag has metadata (color) in the global index."""
        meta_key = f"idx:{collection}:tags_metadata"
        if not self.r.hexists(meta_key, tag):
            palette = [
                "#FF5555", "#50FA7B", "#F1FA8C", "#BD93F9", "#FF79C6", 
                "#8BE9FD", "#FFB86C", "#A6E22E", "#66D9EF"
            ]
            color = random.choice(palette)
            self.r.hset(meta_key, tag, json.dumps({"color": color, "priority": 0}))

    def get_collection_tags(self, collection):
        """Returns all tags (Analysis + User) and their metadata for a collection."""
        r = self.r
        meta_key = f"idx:{collection}:tags_metadata"
        raw_meta = r.hgetall(meta_key)
        
        results = {}
        for k, v in raw_meta.items():
            tag_name = k.decode() if isinstance(k, bytes) else k
            meta = json.loads(v)
            
            # Aggregate counts across all registries (Legacy + New)
            count = 0
            # Registry patterns to check
            registries = [
                f"idx:{collection}:sim:tags:{tag_name}", # Legacy Sim Case-sensitive
                f"idx:{collection}:sim:user_tags:{tag_name.lower()}", # New Sim
                f"idx:{collection}:file:tags:{tag_name.lower()}",     # Legacy File
                f"idx:{collection}:file:user_tags:{tag_name.lower()}", # New File
                f"idx:{collection}:function:tags:{tag_name.lower()}", # Legacy Func
                f"idx:{collection}:function:user_tags:{tag_name.lower()}" # New Func
            ]
            
            for rkey in registries:
                count += r.scard(rkey)
                
            meta["count"] = count
            results[tag_name] = meta
            
        return results

    def set_tag_color(self, collection, tag, color):
        meta_key = f"idx:{collection}:tags_metadata"
        raw = self.r.hget(meta_key, tag)
        meta = json.loads(raw) if raw else {"priority": 0}
        meta["color"] = color
        self.r.hset(meta_key, tag, json.dumps(meta))
        return True

    def set_tag_priority(self, collection, tag, priority):
        meta_key = f"idx:{collection}:tags_metadata"
        raw = self.r.hget(meta_key, tag)
        meta = json.loads(raw) if raw else {"color": "#66d9ef"}
        meta["priority"] = int(priority)
        self.r.hset(meta_key, tag, json.dumps(meta))
        return True

tag_service = TagService()
