import json

from bsimvis.app.services.bin_sim_tags import bump_tags_rev
import random
import logging
from .redis_client import get_redis
from bsimvis.app.services.index_config import tag_ancestors
from bsimvis.app.services.index_service import (
    resolve_origin_collection,
    to_pool_indexed_id,
)


# Marks a colour a human actually chose. Three writers used to assign one off a
# palette the first time a tag was seen -- two of them with `random.choice`, so
# the value carries no evidence of where it came from. A stored colour wins over
# the derived one in the UI, so that dice roll silently beat the namespace/hue
# rule for every tag an analyzer wrote: `fid:uclibc` green, its own
# `#ambiguous` red, no relation between a library and its versions.
#
# Only a colour carrying this marker is honoured now. Colours already stored
# have no marker and cannot be told apart from the random ones, so they give way
# to the derived colour -- on a vocabulary of thousands of analyzer tags, that
# is the outcome a human wants anyway, and a deliberate choice is one click to
# restore.
COLOR_SOURCE_USER = "user"


def _honoured_color(meta):
    """Whether this metadata row's colour was chosen rather than rolled."""
    return bool(meta.get("color")) and meta.get("color_source") == COLOR_SOURCE_USER


def _normalize_collection(collection, entity_id=None):
    return resolve_origin_collection(collection, entity_id)


def _tag_buckets(tag_lower, field="user_tags"):
    """The tag's own bucket plus every ancestor namespace bucket.

    Tags are written straight to Redis here rather than through
    index_service._index_tag, so the hierarchy expansion has to be mirrored —
    otherwise a user-applied `lib:uclibc:foo` would be invisible to a
    `func_tag=lib` lookup while an ingest-applied one is found.
    """
    return [tag_lower] + tag_ancestors(field, tag_lower)


def _add_tag_buckets(r, coll, lvl, tag_lower, members, field="user_tags"):
    if isinstance(members, str):
        members = [members]
    if not members:
        return
    registry_key = f"{coll}:reg:{lvl}:{field}"
    for bucket_value in _tag_buckets(tag_lower, field):
        key = f"{coll}:idx:{lvl}:{field}:{bucket_value}"
        r.sadd(key, *members)
        r.sadd(registry_key, key)


def _remove_tag_buckets(r, coll, lvl, tag_lower, members, remaining, field="user_tags"):
    """Drop members from the tag bucket, and from ancestors nothing else covers.

    remaining: tags the doc still carries. Bulk callers pass None when the
    per-doc remainder isn't known — ancestors are then left in place, which
    keeps a namespace filter over-inclusive rather than dropping a doc that
    still belongs there. Run the backfill script to resettle them.
    """
    if not members:
        return
    kept = set()
    for t in remaining or []:
        if not t:
            continue
        t_lower = str(t).lower()
        kept.add(t_lower)
        kept.update(tag_ancestors(field, t_lower))

    buckets = [tag_lower] if remaining is None else _tag_buckets(tag_lower, field)
    for bucket_value in buckets:
        if bucket_value in kept:
            continue
        r.srem(f"{coll}:idx:{lvl}:{field}:{bucket_value}", *members)


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

    def _bump_tag_rev(self, collection):
        """Mark this collection's tag state as changed, and every pool holding it.

        Bumped up front, before the write is attempted: a bump that follows a
        failed write costs one needless "split is stale" badge, while a missed
        bump leaves a wrong split looking current. A pool keeps its own revision
        because its bin_sim docs are split against the pool's tag metadata.
        """
        bump_tags_rev(self.r, collection)
        try:
            pools = self.r.smembers(f"{collection}:pools") or []
        except Exception:
            return
        for p_id in pools:
            p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
            bump_tags_rev(self.r, f"global:pool:{p_id}")

    def add_user_tag(self, collection, entity_type, entity_id, tag):
        """
        Adds a user tag to an entity (file, function, or similarity).
        """
        collection = _normalize_collection(collection, entity_id)
        r = self.r
        tag = tag.strip()
        if not tag:
            return False
        self._bump_tag_rev(collection)

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

                _add_tag_buckets(r, collection, lvl, tag_lower, indexed_id)

                # Propagate index to all associated pools
                associated_pools = r.smembers(f"{collection}:pools")
                for p_id in associated_pools:
                    p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                    pool_id_val = to_pool_indexed_id(indexed_id, lvl, p_id)
                    if not pool_id_val:
                        continue
                    _add_tag_buckets(
                        r, f"global:pool:{p_id}", lvl, tag_lower, pool_id_val
                    )

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
        self._bump_tag_rev(collection)
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

                _remove_tag_buckets(
                    r, collection, lvl, tag_lower, [indexed_id], user_tags
                )

                # Propagate index removal to all associated pools
                associated_pools = r.smembers(f"{collection}:pools")
                for p_id in associated_pools:
                    p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                    pool_id_val = to_pool_indexed_id(indexed_id, lvl, p_id)
                    if not pool_id_val:
                        continue
                    _remove_tag_buckets(
                        r,
                        f"global:pool:{p_id}",
                        lvl,
                        tag_lower,
                        [pool_id_val],
                        user_tags,
                    )

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
        self._bump_tag_rev(collection)

        try:
            tag_lower = tag.lower()
            lvl = (
                "func"
                if entity_type == "function"
                else "sim" if entity_type == "similarity" else entity_type
            )

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
                    _add_tag_buckets(r, collection, lvl, tag_lower, indexed_id)

                    # Propagate to pools
                    for p_id in associated_pools:
                        p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                        pool_id_val = to_pool_indexed_id(indexed_id, lvl, p_id)
                        if not pool_id_val:
                            continue
                        _add_tag_buckets(
                            r, f"global:pool:{p_id}", lvl, tag_lower, pool_id_val
                        )

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
        self._bump_tag_rev(collection)
        try:
            tag_lower = tag.lower()
            lvl = (
                "func"
                if entity_type == "function"
                else "sim" if entity_type == "similarity" else entity_type
            )
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
                    _remove_tag_buckets(
                        r, collection, lvl, tag_lower, [indexed_id], user_tags
                    )

                    # Propagate removal to pools
                    for p_id in associated_pools:
                        p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                        pool_id_val = to_pool_indexed_id(indexed_id, lvl, p_id)
                        if not pool_id_val:
                            continue
                        _remove_tag_buckets(
                            r,
                            f"global:pool:{p_id}",
                            lvl,
                            tag_lower,
                            [pool_id_val],
                            user_tags,
                        )

                    self._propagate_user_tag(
                        collection, entity_type, eid, tag, op="remove"
                    )

            return True
        except Exception as e:
            logging.error(f"TagService: Error in bulk remove tags: {e}")
            return False

    def _ensure_tag_metadata(self, collection, tag):
        """Ensures a tag has a metadata row in the global index.

        Deliberately stores no colour. A tag's colour is derived from its id --
        namespace picks the arc, the first level picks the hue, deeper levels
        shade -- so a library reads as one colour everywhere it appears and two
        libraries differ by hue rather than by luck. Rolling a palette entry per
        tag here overrode all of that, which is why `fid:uclibc` came out green
        and `fid:uclibc:0.9.30.1#ambiguous` red.

        A stored colour now means exactly one thing: a human chose it, and the
        UI still lets that win.
        """
        collection = _normalize_collection(collection)
        meta_key = f"{collection}:tags_metadata"
        if not self.r.hexists(meta_key, tag):
            self.r.hset(meta_key, tag, json.dumps({"priority": 0}))

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
            if meta.get("color") and not _honoured_color(meta):
                # Rolled by the old auto-assign, not chosen. Dropped on read
                # rather than migrated: the hash is rewritten by several paths,
                # and a row nobody has recoloured needs no rewrite at all.
                meta = {kk: vv for kk, vv in meta.items() if kk != "color"}
            results[tag_name] = meta

        return results

    # --- Tag vocabulary management (create / delete / llm flag) ---

    LVL_TO_ETYPE = {"func": "function", "file": "file", "sim": "similarity"}

    def create_tag(self, collection, tag, color=None, priority=0, llm=False):
        """Registers a tag in the collection vocabulary without tagging anything.

        The `tags_metadata` hash already holds tag -> {color, priority}
        independently of membership, so a vocabulary entry is just a metadata
        row with no indexed entities.
        """
        collection = _normalize_collection(collection)
        tag = (tag or "").strip()
        if not tag:
            return False

        self._ensure_tag_metadata(collection, tag)
        if color:
            self.set_tag_color(collection, tag, color)
        if priority:
            self.set_tag_priority(collection, tag, priority)
        if llm:
            self.set_tag_llm(collection, tag, True)
        return True

    def set_tag_llm(self, collection, tag, enabled):
        """Flags a tag as part of the LLM tagging vocabulary."""
        collection = _normalize_collection(collection)
        meta_key = f"{collection}:tags_metadata"
        raw = self.r.hget(meta_key, tag)
        meta = json.loads(raw) if raw else {"color": "#66d9ef", "priority": 0}
        meta["llm"] = bool(enabled)
        self.r.hset(meta_key, tag, json.dumps(meta))

        # Propagate to pools, same as color/priority.
        for p_id in self.r.smembers(f"{collection}:pools"):
            p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
            self.r.hset(f"global:pool:{p_id}:tags_metadata", tag, json.dumps(meta))
        return True

    def get_llm_vocabulary(self, collection):
        """Tags flagged for LLM use, sorted by descending priority then name."""
        tags = self.get_collection_tags(collection)
        flagged = [(t, m) for t, m in tags.items() if m.get("llm")]
        flagged.sort(key=lambda kv: (-int(kv[1].get("priority") or 0), kv[0]))
        return [t for t, _ in flagged]

    def _tagged_ids(self, collection, lvl, field, tag):
        """Entity ids (index form, no `:meta`) carrying `tag` at `lvl`.`field`."""
        raw = self.r.smembers(f"{collection}:idx:{lvl}:{field}:{tag.lower()}")
        return [i.decode() if isinstance(i, bytes) else i for i in (raw or [])]

    def _strip_static_tag(self, collection, lvl, ids, tag):
        """Removes an analysis tag from docs and its index (no user_tags path)."""
        r = self.r
        tag_lower = tag.lower()
        for eid in ids:
            doc_id = eid if eid.endswith(":meta") else f"{eid}:meta"
            remaining = []
            data = self._get_doc(doc_id)
            if data:
                tags = data.get("tags")
                if isinstance(tags, list):
                    remaining = [t for t in tags if t != tag]
                    if tag in tags:
                        data["tags"] = remaining
                        self._set_doc(doc_id, data)
            _remove_tag_buckets(
                r, collection, lvl, tag_lower, [eid], remaining, field="tags"
            )
            for p_id in r.smembers(f"{collection}:pools"):
                p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                mapped = to_pool_indexed_id(eid, lvl, p_id)
                if mapped:
                    _remove_tag_buckets(
                        r,
                        f"global:pool:{p_id}",
                        lvl,
                        tag_lower,
                        [mapped],
                        remaining,
                        field="tags",
                    )

    def delete_tag(self, collection, tag):
        """Deletes a tag: strips it from every entity, then drops its metadata.

        Destructive and irreversible -- callers must confirm with the user
        first. Returns the per-level removal counts.
        """
        collection = _normalize_collection(collection)
        tag = (tag or "").strip()
        if not tag:
            return None

        self._bump_tag_rev(collection)
        removed = {"function": 0, "file": 0, "similarity": 0}
        for lvl, etype in self.LVL_TO_ETYPE.items():
            user_ids = self._tagged_ids(collection, lvl, "user_tags", tag)
            if user_ids:
                self.bulk_remove_user_tag(collection, etype, user_ids, tag)
                removed[etype] += len(user_ids)

            static_ids = self._tagged_ids(collection, lvl, "tags", tag)
            if static_ids:
                self._strip_static_tag(collection, lvl, static_ids, tag)
                removed[etype] += len(static_ids)

        self.r.hdel(f"{collection}:tags_metadata", tag)
        for p_id in self.r.smembers(f"{collection}:pools"):
            p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
            self.r.hdel(f"global:pool:{p_id}:tags_metadata", tag)

        return removed

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
        # Marks it as chosen, which is what makes it beat the colour derived
        # from the tag id. Without the marker it is indistinguishable from the
        # palette rolls the old auto-assign left behind, and is ignored on read.
        meta["color_source"] = COLOR_SOURCE_USER
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
        # Priority decides which origin wins on a multi-tagged function, so it
        # changes the split without any function document being touched.
        self._bump_tag_rev(collection)
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

                if target_lvl == "func":
                    related_ids = r.smembers(f"{collection}:idx:file:functions:{md5}")
                elif target_lvl == "sim":
                    related_ids = r.smembers(f"{collection}:sim:involves:file:{md5}")
                else:
                    continue

                if related_ids:
                    id_list = list(related_ids)
                    for coll_key in [collection] + [
                        f"global:pool:{p.decode() if isinstance(p, bytes) else p}"
                        for p in associated_pools
                    ]:
                        if coll_key == collection:
                            ids = id_list
                        else:
                            ids = _to_pool_ids(
                                id_list, target_lvl, coll_key.split(":")[-1]
                            )
                        if not ids:
                            continue
                        if op == "add":
                            _add_tag_buckets(
                                r, coll_key, target_lvl, tag_lower, ids, target_field
                            )
                        else:
                            # Per-doc remainder is unknown on a propagated
                            # removal, so ancestors stay put — over-inclusive,
                            # never under-inclusive. The backfill script
                            # resettles them.
                            _remove_tag_buckets(
                                r,
                                coll_key,
                                target_lvl,
                                tag_lower,
                                ids,
                                None,
                                target_field,
                            )

        elif src_level == "func":
            parts = indexed_id.split(":")
            if len(parts) >= 4:
                clean_id = f"{parts[-2]}:{parts[-1]}"
                for target_lvl in prop_levels:
                    if target_lvl == "sim":
                        target_field = resolve_target_field(
                            src_level, target_lvl, "user_tags"
                        )
                        related_ids = r.smembers(
                            f"{collection}:sim:involves:func:{clean_id}"
                        )
                        if related_ids:
                            id_list = list(related_ids)
                            for coll_key in [collection] + [
                                f"global:pool:{p.decode() if isinstance(p, bytes) else p}"
                                for p in associated_pools
                            ]:
                                if coll_key == collection:
                                    ids = id_list
                                else:
                                    ids = _to_pool_ids(
                                        id_list, target_lvl, coll_key.split(":")[-1]
                                    )
                                if not ids:
                                    continue
                                if op == "add":
                                    _add_tag_buckets(
                                        r,
                                        coll_key,
                                        target_lvl,
                                        tag_lower,
                                        ids,
                                        target_field,
                                    )
                                else:
                                    _remove_tag_buckets(
                                        r,
                                        coll_key,
                                        target_lvl,
                                        tag_lower,
                                        ids,
                                        None,
                                        target_field,
                                    )


tag_service = TagService()
