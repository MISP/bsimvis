"""
Secondary index service for BSimVis.

Key naming conventions:
  {coll}:idx:{level}:{field}:{value}  -> SET  of doc IDs  (TAG / exact match)
  {coll}:idx:{level}:{field}          -> ZSET of doc IDs  (NUMERIC)
  {coll}:idx:file:functions:{md5}     -> SET  of func IDs (file->function relationship)

Field lists (FILE_TAG_FIELDS etc.) are derived from index_config.INDEX_FIELDS.
To change which fields are indexed and at which levels, edit index_config.py.
"""

import json
import datetime

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_config import tag_ancestors


def normalize_tags(data, tag_fields=None):
    """
    Ensures that specified tag fields in a dictionary are normalized to lists of strings.
    Handles legacy comma-separated strings and missing fields.
    """
    if tag_fields is None:
        tag_fields = ["tags", "user_tags"]

    for field in tag_fields:
        val = data.get(field)
        if isinstance(val, str):
            data[field] = [t.strip() for t in val.split(",")] if val else []
        elif val is None:
            data[field] = []
        elif not isinstance(val, list):
            data[field] = []

    return data


def get_pool_id(collection):
    """Consistent helper to extract pool ID from collection/pool names."""
    if not collection:
        return None
    if collection.startswith("global:pool:"):
        rest = collection[len("global:pool:") :]
        return rest.split(":")[0]
    if collection.startswith("pool:"):
        rest = collection[len("pool:") :]
        return rest.split(":")[0]
    return None


def resolve_origin_collection(collection, entity_id=None, r=None):
    """
    Maps a pool namespace back to the origin collection that owns the entity.

    Tags and notes live on the origin collection, never on the pool, so every
    write path must resolve `global:pool:{id}` down to a real collection name.
    Prefers the pool's declared member collections over parsing the entity id.
    """
    if not collection:
        return collection
    if ":col:" in collection:
        return collection.split(":col:")[-1]

    pool_id = get_pool_id(collection)
    if not pool_id or not entity_id:
        return collection

    if ":col:" in entity_id:
        return entity_id.split(":col:")[-1].split(":")[0]

    # An entity id is "{collection}:{file|func|sim}:..." — match its leading
    # segment against the collections this pool actually contains.
    if r is None:
        r = get_redis()
    members = {
        c.decode() if isinstance(c, bytes) else c
        for c in r.smembers(f"global:pool:{pool_id}:collections_list")
    }
    for part in entity_id.split(":"):
        if part in members:
            return part

    return collection


def to_pool_indexed_id(indexed_id, lvl, pool_id):
    """
    Rewrites a collection-scoped doc id into its pool-scoped equivalent.

    File and function docs are shared, so a pool index stores the same id the
    collection does. Similarity docs are not: a pool builds its own sim docs
    under `global:pool:{id}:sim:{a}::{b}`, keyed by full function ids, while a
    collection stores `{coll}:sim:{algo}:{a}::{b}` with the collection prefix
    stripped from both sides. Mirroring a collection sid into a pool index
    without this rewrite indexes an id the pool has no document for.

    Returns None when the id is not a sid this mapping applies to, so callers
    skip it rather than index a bad key.
    """
    if lvl != "sim":
        return indexed_id
    parts = indexed_id.split(":")
    if len(parts) < 4 or parts[1] != "sim":
        return None
    coll_name = parts[0]
    rest = ":".join(parts[3:])
    pivot = rest.find("::")
    if pivot == -1:
        return None
    id1, id2 = rest[:pivot], rest[pivot + 2 :]
    return f"global:pool:{pool_id}:sim:{coll_name}:func:{id1}::{coll_name}:func:{id2}"


def enrich_pool_data(data, pool_id):
    """
    Ensures tag/note keys are present on a metadata dict (file, function, or
    similarity) so callers can rely on them.
    """
    if not pool_id or not isinstance(data, dict):
        return data

    if "user_tags" not in data:
        data["user_tags"] = []
    if "notes" not in data:
        data["notes"] = []
    if "note_owners" not in data:
        data["note_owners"] = []
    if "note_count" not in data:
        data["note_count"] = len(data["notes"])

    return data


def parse_timestamp(val):
    """Normalize mixed UTC ISO strings and Unix integers to Unix Milliseconds."""
    if not val:
        return 0
    if isinstance(val, (int, float)):
        # If it's already a high number (likely already ms), return as is.
        # 1e12 is approx year 2001 in milliseconds, while it's year 33658 in seconds.
        if val > 1e12:
            return int(val)
        # Otherwise convert seconds to ms
        return int(val * 1000)
    if isinstance(val, str):
        try:
            # Try parsing as float first (e.g. string representation of timestamp)
            fval = float(val)
            if fval > 1e12:
                return int(fval)
            return int(fval * 1000)
        except (ValueError, TypeError):
            pass

        try:
            # Handle ISO 8601: 2026-03-26T11:48:07.851317Z or 2026-03-26T10:48:02.623Z
            return int(
                datetime.datetime.fromisoformat(val.replace("Z", "+00:00")).timestamp()
                * 1000
            )
        except (ValueError, TypeError):
            return 0
    return 0


# ---------------------------------------------------------------------------
# Field lists — derived from IndexConfig (edit index_config.py to change)
# ---------------------------------------------------------------------------
from bsimvis.app.services.index_config import (
    get_native_fields,
    get_propagated_fields,
    get_fields_targeting_level,
)

FILE_TAG_FIELDS = get_fields_targeting_level("file", is_num=False)
FUNC_TAG_FIELDS = get_fields_targeting_level("func", is_num=False)
FILE_NUM_FIELDS = get_fields_targeting_level("file", is_num=True)
FUNC_NUM_FIELDS = get_fields_targeting_level("func", is_num=True)
FEATURE_TAG_FIELDS = get_native_fields("feature", is_num=False)
FEATURE_NUM_FIELDS = get_native_fields("feature", is_num=True)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _index_tag(pipe, coll, level, field, value, doc_id, seen=None):
    """Add doc_id to the tag set for field=value in a standardized registry/bucket structure.

    seen: optional set to dedupe the registry sadd across many calls in one build
    (the registry maps field->bucket and is identical for every doc sharing a value).
    """
    if value is None:
        return
    # Handle list values (e.g. tags) and deduplicate them
    if isinstance(value, list):
        seen = set()
        values = []
        for v in value:
            if v not in seen:
                seen.add(v)
                values.append(v)
    else:
        values = [value]
    for v in values:
        if v is None or v == "":
            continue
        v_lower = str(v).lower()
        registry_key = f"{coll}:reg:{level}:{field}"

        # The value itself plus, for hierarchical fields, every ancestor path.
        # Indexing `lib` and `lib:uclibc` as real buckets is what lets a
        # namespace filter be one exact lookup instead of a vocabulary scan.
        for bucket_value in [v_lower] + tag_ancestors(field, v_lower):
            # Standardized Bucket: {col}:idx:{level}:{field}:{value}
            bucket_key = f"{coll}:idx:{level}:{field}:{bucket_value}"
            pipe.sadd(bucket_key, doc_id)
            # Standardized Registry: {coll}:reg:{level}:{field} (many buckets)
            if seen is None:
                pipe.sadd(registry_key, bucket_key)
            elif (registry_key, bucket_key) not in seen:
                pipe.sadd(registry_key, bucket_key)
                seen.add((registry_key, bucket_key))

        # AUTO-DISCOVERY: Ensure tags are registered in global metadata
        if "tags" in field:
            meta_key = f"{coll}:tags_metadata"
            import random

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
            ]
            default_meta = json.dumps({"color": random.choice(palette), "priority": 0})
            pipe.hsetnx(meta_key, str(v), default_meta)


def _unindex_tag(pipe, coll, level, field, value, doc_id, remaining=None):
    """Remove doc_id from the tag set for field=value.

    remaining: the values of this field the doc still carries after the removal.
    Ancestor buckets are shared, so dropping `lib:uclibc:seekdir` must not pull
    the doc out of `lib:uclibc` while it still holds `lib:uclibc:telldir`.
    Callers that delete the whole doc pass nothing and every ancestor goes.
    """
    if value is None:
        return
    if isinstance(value, list):
        seen = set()
        values = []
        for v in value:
            if v not in seen:
                seen.add(v)
                values.append(v)
    else:
        values = [value]

    kept = set()
    for v in remaining or []:
        if v is None or v == "":
            continue
        v_lower = str(v).lower()
        kept.add(v_lower)
        kept.update(tag_ancestors(field, v_lower))

    for v in values:
        if v is None or v == "":
            continue
        v_lower = str(v).lower()
        for bucket_value in [v_lower] + tag_ancestors(field, v_lower):
            if bucket_value in kept:
                continue
            pipe.srem(f"{coll}:idx:{level}:{field}:{bucket_value}", doc_id)


def _index_num(pipe, coll, level, field, value, doc_id):
    """Add doc_id to the numeric ZSET for field."""
    if value is None:
        return
    try:
        # Standard Numeric Index: {col}:idx:{level}:{field}
        pipe.zadd(f"{coll}:idx:{level}:{field}", {doc_id: float(value)})
    except (ValueError, TypeError):
        pass


def _unindex_num(pipe, coll, level, field, doc_id):
    """Remove doc_id from the numeric ZSET."""
    pipe.zrem(f"{coll}:idx:{level}:{field}", doc_id)


# ---------------------------------------------------------------------------
# Public: save
# ---------------------------------------------------------------------------


def save_file(pipe, coll, file_md5, data):
    """Index all fields for a file doc. Standardized as {col}:file:{md5}"""
    base_id = f"{coll}:file:{file_md5}"
    for f in FILE_TAG_FIELDS:
        _index_tag(pipe, coll, "file", f, data.get(f), base_id)
    for f in FILE_NUM_FIELDS:
        _index_num(pipe, coll, "file", f, data.get(f), base_id)
    pipe.sadd(f"{coll}:all_files", base_id)


def save_function(pipe, coll, md5, addr, data):
    """Index all fields for a function doc. Standardized as {col}:func:{md5}:{addr}"""
    base_id = f"{coll}:func:{md5}:{addr}"
    for f in FUNC_TAG_FIELDS:
        _index_tag(pipe, coll, "func", f, data.get(f), base_id)
    for f in FUNC_NUM_FIELDS:
        _index_num(pipe, coll, "func", f, data.get(f), base_id)
    # relationship links
    pipe.sadd(f"{coll}:idx:file:functions:{md5}", base_id)
    pipe.sadd(f"{coll}:all_functions", base_id)


def save_feature(pipe, coll, f_hash, data):
    """Index all fields for a feature doc. Standardized as {col}:feature:{f_hash}"""
    base_id = f"{coll}:feature:{f_hash}"
    for f in FEATURE_TAG_FIELDS:
        _index_tag(pipe, coll, "feature", f, data.get(f), base_id)
    for f in FEATURE_NUM_FIELDS:
        _index_num(pipe, coll, "feature", f, data.get(f), base_id)
    pipe.sadd(f"{coll}:all_features", base_id)


def save_similarity(
    pipe,
    coll,
    sid,
    sim_doc,
    func_meta1=None,
    func_meta2=None,
    file_meta1=None,
    file_meta2=None,
    index_depth="full",
    seen=None,
):
    """Write sim-level secondary indexes for all propagated fields.
    Pulls data from the sim doc itself, function meta, or file meta based on field source.
    seen: optional set to dedupe registry writes across a build (see _index_tag).
    """
    propagated = get_propagated_fields("sim")

    # 1. Native Sim Fields (source: sim)
    for orig_field, target_field in propagated["sim"]:
        value = sim_doc.get(orig_field)
        if value is not None:
            _index_tag(pipe, coll, "sim", target_field, value, sid, seen=seen)

    if index_depth == "minimal":
        # Only index file_md5 from file level
        for orig_field, target_field in propagated["file"]:
            if orig_field == "file_md5":
                value = [v for v in [sim_doc.get("md5_1"), sim_doc.get("md5_2")] if v]
                if value:
                    _index_tag(pipe, coll, "sim", target_field, value, sid, seen=seen)
        return

    # 2. Propagated Func Fields (source: func)
    for orig_field, target_field in propagated["func"]:
        value = []
        for meta in [func_meta1, func_meta2]:
            if meta:
                v = meta.get(orig_field)
                if v is not None:
                    if isinstance(v, list):
                        value.extend(v)
                    else:
                        value.append(v)
        if value:
            _index_tag(pipe, coll, "sim", target_field, value, sid, seen=seen)

    # 3. Propagated File Fields (source: file)
    for orig_field, target_field in propagated["file"]:
        value = []
        # Optimization: if propagating file_md5, we don't need file meta, it's in sim_doc
        if orig_field == "file_md5":
            value = [v for v in [sim_doc.get("md5_1"), sim_doc.get("md5_2")] if v]
        else:
            for meta in [file_meta1, file_meta2]:
                if meta:
                    v = meta.get(orig_field)
                    if v is not None:
                        if isinstance(v, list):
                            value.extend(v)
                        else:
                            value.append(v)
        if value:
            _index_tag(pipe, coll, "sim", target_field, value, sid, seen=seen)


def delete_similarity(
    pipe,
    coll,
    sid,
    sim_doc,
    func_meta1=None,
    func_meta2=None,
    file_meta1=None,
    file_meta2=None,
):
    """Remove sim-level secondary indexes for a similarity document."""
    propagated = get_propagated_fields("sim")

    # 1. Native Sim Fields
    for orig_field, target_field in propagated["sim"]:
        value = sim_doc.get(orig_field)
        if value is not None:
            _unindex_tag(pipe, coll, "sim", target_field, value, sid)

    # 2. Propagated Func Fields
    for orig_field, target_field in propagated["func"]:
        value = []
        for meta in [func_meta1, func_meta2]:
            if meta:
                v = meta.get(orig_field)
                if v is not None:
                    if isinstance(v, list):
                        value.extend(v)
                    else:
                        value.append(v)
        if value:
            _unindex_tag(pipe, coll, "sim", target_field, value, sid)

    # 3. Propagated File Fields
    for orig_field, target_field in propagated["file"]:
        value = []
        if orig_field == "file_md5":
            value = [v for v in [sim_doc.get("md5_1"), sim_doc.get("md5_2")] if v]
        else:
            for meta in [file_meta1, file_meta2]:
                if meta:
                    v = meta.get(orig_field)
                    if v is not None:
                        if isinstance(v, list):
                            value.extend(v)
                        else:
                            value.append(v)
        if value:
            _unindex_tag(pipe, coll, "sim", target_field, value, sid)


# ---------------------------------------------------------------------------
# Public: delete
# ---------------------------------------------------------------------------


def delete_file(r, coll, file_md5):
    """Remove a file from all indexes."""
    base_id = f"{coll}:file:{file_md5}"
    doc_id = f"{base_id}:meta"
    raw = r.get(doc_id)
    data = {}
    if raw:
        val = raw.decode() if isinstance(raw, bytes) else raw
        try:
            data = json.loads(val)
        except Exception:
            pass
    if not data:
        return
    pipe = r.pipeline(transaction=False)
    for f in FILE_TAG_FIELDS:
        _unindex_tag(pipe, coll, "file", f, data.get(f), base_id)
    for f in FILE_NUM_FIELDS:
        _unindex_num(pipe, coll, "file", f, base_id)
    pipe.srem(f"{coll}:all_files", base_id)
    pipe.hincrby(f"global:collection:{coll}:meta", "total_files", -1)
    pipe.execute()


def delete_function(r, coll, md5, addr):
    """Remove a function from all indexes."""
    base_id = f"{coll}:func:{md5}:{addr}"
    doc_id = f"{base_id}:meta"
    raw = r.get(doc_id)
    data = {}
    if raw:
        val = raw.decode() if isinstance(raw, bytes) else raw
        try:
            data = json.loads(val)
        except Exception:
            pass
    if not data:
        return
    pipe = r.pipeline(transaction=False)
    for f in FUNC_TAG_FIELDS:
        _unindex_tag(pipe, coll, "func", f, data.get(f), base_id)
    for f in FUNC_NUM_FIELDS:
        _unindex_num(pipe, coll, "func", f, base_id)
    pipe.srem(f"{coll}:idx:file:functions:{md5}", base_id)
    pipe.srem(f"{coll}:all_functions", base_id)
    pipe.hincrby(f"global:collection:{coll}:meta", "total_functions", -1)
    pipe.delete(f"{base_id}:callees")
    pipe.delete(f"{base_id}:callers")
    pipe.execute()


def delete_feature(r, coll, f_hash):
    """Remove a feature from all indexes."""
    base_id = f"{coll}:feature:{f_hash}"
    doc_id = f"{base_id}:global_meta"
    raw = r.get(doc_id)
    data = {}
    if raw:
        val = raw.decode() if isinstance(raw, bytes) else raw
        try:
            data = json.loads(val)
        except Exception:
            pass

    pipe = r.pipeline(transaction=False)
    if data:
        for f in FEATURE_TAG_FIELDS:
            _unindex_tag(pipe, coll, "feature", f, data.get(f), base_id)
        for f in FEATURE_NUM_FIELDS:
            _unindex_num(pipe, coll, "feature", f, base_id)
    pipe.srem(f"{coll}:all_features", base_id)
    pipe.delete(doc_id)
    pipe.execute()


# ---------------------------------------------------------------------------
# Public: query
# ---------------------------------------------------------------------------


def query_ids(
    r, coll, doc_type, tag_filters=None, num_filters=None, offset=0, limit=100
):
    """
    Resolve filters to a list of doc IDs using standardized buckets.
    """
    tag_filters = tag_filters or {}
    num_filters = num_filters or {}

    # Internal level mapping: API 'function' -> internal 'func'
    lvl = "func" if doc_type == "function" else doc_type

    all_key = f"{coll}:all_{doc_type}s"

    filter_key_groups = []

    for field, value in tag_filters.items():
        if value is None or value == "":
            continue

        # Standard Bucket: {col}:idx:{level}:{field}:{value}
        base_prefix = f"{coll}:idx:{lvl}:{field}:{str(value).lower()}"

        # User Tag Union Logic
        if field == "tags":
            user_tags_prefix = f"{coll}:idx:{lvl}:user_tags:{str(value).lower()}"
            filter_key_groups.append((True, [base_prefix, user_tags_prefix]))
        else:
            filter_key_groups.append((False, [base_prefix]))

    if filter_key_groups:
        is_union, group_keys = filter_key_groups[0]
        if is_union:
            candidates = list(r.sunion(*group_keys))
        else:
            candidates = list(r.smembers(group_keys[0]))

        other_groups = filter_key_groups[1:]
    else:
        candidates = list(r.smembers(all_key))
        other_groups = []

    if other_groups and candidates:
        for is_union, group_keys in other_groups:
            if not candidates:
                break
            if not is_union:
                pipe = r.pipeline(transaction=False)
                for cid in candidates:
                    pipe.sismember(group_keys[0], cid)
                results = pipe.execute()
                candidates = [cid for cid, ok in zip(candidates, results) if ok]
            else:
                new_candidates = []
                for cid in candidates:
                    exists = False
                    for gk in group_keys:
                        if r.sismember(gk, cid):
                            exists = True
                            break
                    if exists:
                        new_candidates.append(cid)
                candidates = new_candidates

    all_ids = candidates

    # Standardized Numerical Filtering: idx:{col}:idx:{level}:{field}
    if num_filters and all_ids:
        pipe = r.pipeline(transaction=False)
        for field, (fmin, fmax) in num_filters.items():
            pipe.zrangebyscore(f"{coll}:idx:{lvl}:{field}", fmin, fmax)
        range_results = pipe.execute()
        for id_set in range_results:
            id_set_s = set(id_set)
            all_ids = [i for i in all_ids if i in id_set_s]

    total = len(all_ids)
    all_ids_sorted = sorted(all_ids)
    page = all_ids_sorted[offset : offset + limit]

    return page, total


class IndexStatsService:
    def __init__(self, r=None):
        from .redis_client import get_redis

        self.r = r or get_redis()

    def get_key_count(self, k):
        """Unified cardinality check."""
        r = self.r
        try:
            rtype = r.type(k).lower()
            if "zset" in rtype:
                return r.zcard(k)
            if "set" in rtype:
                return r.scard(k)
            if "list" in rtype:
                return r.llen(k)
            if "hash" in rtype:
                return r.hlen(k)
        except:
            pass
        return 0

    def get_key_size(self, k):
        """Unified size estimator for different redis types in Kvrocks."""
        r = self.r
        try:
            # 1. Try MEMORY USAGE (Best)
            size = r.execute_command("MEMORY", "USAGE", k)
            if size:
                return size
        except:
            pass

        try:
            # 2. Fallback to Type-specific estimation
            rtype = r.type(k).lower()
            if rtype == "string":
                return r.strlen(k)
            if rtype == "list":
                return r.llen(k) * 100  # Approx
            if rtype == "set":
                return r.scard(k) * 40  # Approx
            if rtype == "zset":
                return r.zcard(k) * 50  # Approx
            if rtype == "hash":
                return r.hlen(k) * 150  # Approx
            if "rejson" in rtype or "json" in rtype:
                try:
                    val = r.execute_command("JSON.GET", k)
                except Exception:
                    val = r.get(k)
                return len(str(val)) if val is not None else 0
        except Exception:
            pass
        return 0

    def estimate_total_keys(self, pattern, num_files, num_funcs, num_unique_features):
        r = self.r
        # We try to avoid a full SCAN if possible.
        if "file:*:meta" in pattern:
            return num_files
        if "func:*:*:meta" in pattern:
            return num_funcs
        if "func:*:*:source" in pattern:
            return num_funcs
        if "func:*:*:vec:tf" in pattern:
            return num_funcs
        if "feature:*:functions" in pattern:
            return num_unique_features
        if "feature:*:meta" in pattern:
            return num_unique_features

        # For sim_meta and tags, we might need a quick scan to estimate.
        cursor = 0
        count_acc = 0
        for _ in range(100):
            cursor, keys = r.scan(cursor, match=pattern, count=5000)
            count_acc += len(keys)
            if cursor == 0:
                break
        return count_acc

    def estimate_group_size(
        self, pattern, count_total, tracking_set=None, key_formatter=None
    ):
        r = self.r
        sample_size = 10
        if count_total == 0:
            return 0

        found_keys = []
        if tracking_set:
            try:
                tset_type = r.type(tracking_set).lower()
                if "zset" in tset_type:
                    items = r.zrandmember(tracking_set, sample_size)
                else:
                    items = r.srandmember(tracking_set, sample_size)

                if items:
                    if key_formatter:
                        found_keys = [key_formatter(i) for i in items]
                    else:
                        found_keys = items
            except Exception:
                pass

        if not found_keys:
            cursor = 0
            for _ in range(30):
                cursor, keys = r.scan(cursor, match=pattern, count=2000)
                found_keys.extend([k for k in keys if k not in found_keys])
                if len(found_keys) >= sample_size or cursor == 0:
                    break

        if not found_keys:
            return 0
        sample = found_keys[:sample_size]
        total_size = 0
        actual_samples = 0
        for k in sample:
            sz = self.get_key_size(k)
            if sz > 0:
                total_size += sz
                actual_samples += 1
        return (total_size / actual_samples) if actual_samples > 0 else 0

    def get_collection_stats(self, collection, details=False):
        """Returns comprehensive index statistics for a collection."""
        r = self.r
        coll = collection

        # 1. Core Counts
        num_files = r.scard(f"{coll}:all_files")
        num_funcs = r.scard(f"{coll}:all_functions")
        num_indexed = r.scard(f"{coll}:indexed:functions")
        num_unique_features = r.zcard(f"{coll}:features:by_tf")
        num_sim_meta = self.estimate_total_keys(
            f"{coll}:sim:*:*:*", num_files, num_funcs, num_unique_features
        )

        summary = {
            "num_files": num_files,
            "num_functions": num_funcs,
            "num_indexed": num_indexed,
            "num_missing": max(0, num_funcs - num_indexed),
            "num_features": num_unique_features,
            "num_sim_meta": num_sim_meta,
            "indexing_ratio": (num_indexed / num_funcs * 100) if num_funcs > 0 else 0,
        }

        if not details:
            return summary

        # 2. Detailed Breakdown
        components = []
        patterns = [
            ("File Meta", f"{coll}:file:*:meta"),
            ("Func Meta", f"{coll}:func:*:*:meta"),
            ("Func Source", f"{coll}:func:*:*:source"),
            ("Func Vector (TF)", f"{coll}:func:*:*:vec:tf"),
            ("Sim Meta", f"{coll}:sim:*:*:*"),
            ("Inverted Index", f"{coll}:feature:*:functions"),
            ("Feature Meta", f"{coll}:feature:*:meta"),
        ]

        for name, pat in patterns:
            count = self.estimate_total_keys(
                pat, num_files, num_funcs, num_unique_features
            )
            if count > 0:
                avg_size = self.estimate_group_size(pat, count)
                components.append(
                    {
                        "name": name,
                        "pattern": pat,
                        "count": count,
                        "avg_size": avg_size,
                        "total_size": avg_size * count,
                    }
                )

        return {"summary": summary, "components": components}
