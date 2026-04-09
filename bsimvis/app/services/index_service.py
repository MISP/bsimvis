"""
Secondary index service for BSimVis.

Key naming conventions:
  idx:{coll}:{field}:{value}  -> SET  of doc IDs  (TAG / exact match)
  idx:{coll}:{field}          -> ZSET of doc IDs  (NUMERIC)
  idx:{coll}:file_funcs:{md5} -> SET  of func IDs (file->function relationship)
"""

import json
import datetime


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
            # Handle ISO 8601: 2026-03-26T11:48:07.851317Z or 2026-03-26T10:48:02.623Z
            return int(
                datetime.datetime.fromisoformat(val.replace("Z", "+00:00")).timestamp()
                * 1000
            )
        except (ValueError, TypeError):
            return 0
    return 0

# ---------------------------------------------------------------------------
# TAG fields that get a Set per value
# ---------------------------------------------------------------------------
FILE_TAG_FIELDS = [
    "type",
    "collection",
    "batch_uuid",
    "file_md5",
    "language_id",
    "tags",
    "user_tags",
    "file_name",
]
FUNC_TAG_FIELDS = [
    "type",
    "collection",
    "batch_uuid",
    "file_md5",
    "language_id",
    "tags",
    "user_tags",
    "file_name",
    "function_name",
    "decompiler_id",
    "return_type",
    "calling_convention",
    "entrypoint_address",
]
# NUMERIC fields stored in a ZSET (member=doc_id, score=value)
FILE_NUM_FIELDS = ["batch_order", "entry_date", "file_date"]
FUNC_NUM_FIELDS = [
    "batch_order",
    "instruction_count",
    "bsim_features_count",
    "entry_date",
    "file_date",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _index_tag(pipe, coll, field, value, doc_id):
    """Add doc_id to the tag set for field=value."""
    if value is None:
        return
    # Handle list values (e.g. tags)
    values = value if isinstance(value, list) else [value]
    for v in values:
        if v is None or v == "":
            continue
        # Store tags lower-cased for case-insensitive search
        bucket_key = f"idx:{coll}:{field}:{str(v).lower()}"
        pipe.sadd(bucket_key, doc_id)
        # Register the bucket key for safe lookups without using 'KEYS'
        pipe.sadd(f"idx:{coll}:reg:{field}", bucket_key)

        # AUTO-DISCOVERY: Ensure tags (Analysis or User) are registered in global metadata
        if ":tags" in field or ":user_tags" in field:
            meta_key = f"idx:{coll}:tags_metadata"
            # Default metadata for discovered tags (HSETNX prevents overwriting existing colors/prio)
            import random
            palette = ["#FF5555", "#50FA7B", "#F1FA8C", "#BD93F9", "#FF79C6", "#8BE9FD", "#FFB86C", "#A6E22E", "#66D9EF"]
            # We use a fixed-ish default to avoid random colors in pipelines if possible, 
            # but TagService uses random, so we match for consistency.
            default_meta = json.dumps({"color": random.choice(palette), "priority": 0})
            pipe.hsetnx(meta_key, str(v), default_meta)


def _unindex_tag(pipe, coll, field, value, doc_id):
    """Remove doc_id from the tag set for field=value."""
    if value is None:
        return
    values = value if isinstance(value, list) else [value]
    for v in values:
        if v is None or v == "":
            continue
        bucket_key = f"idx:{coll}:{field}:{str(v).lower()}"
        pipe.srem(bucket_key, doc_id)
        # We don't necessarily remove from registry on every unindex to avoid expensive scard checks,
        # but the bucket key itself will eventually be empty if all docs are removed.


def _index_num(pipe, coll, field, value, doc_id):
    """Add doc_id to the numeric ZSET for field."""
    if value is None:
        return
    try:
        pipe.zadd(f"idx:{coll}:{field}", {doc_id: float(value)})
    except (ValueError, TypeError):
        pass


def _unindex_num(pipe, coll, field, doc_id):
    """Remove doc_id from the numeric ZSET."""
    pipe.zrem(f"idx:{coll}:{field}", doc_id)


# ---------------------------------------------------------------------------
# Public: save
# ---------------------------------------------------------------------------


def save_file(pipe, coll, file_md5, data):
    """Index all fields for a file doc. Must be called with an active pipeline."""
    base_id = f"{coll}:file:{file_md5}"
    doc_id = f"{base_id}:meta"
    for f in FILE_TAG_FIELDS:
        _index_tag(pipe, coll, f"file:{f}", data.get(f), base_id)
    for f in FILE_NUM_FIELDS:
        _index_num(pipe, coll, f"file:{f}", data.get(f), base_id)
    # Track count
    pipe.sadd(f"idx:{coll}:all_files", base_id)


def save_function(pipe, coll, md5, addr, data):
    """Index all fields for a function doc."""
    base_id = f"{coll}:function:{md5}:{addr}"
    doc_id = f"{base_id}:meta"
    for f in FUNC_TAG_FIELDS:
        _index_tag(pipe, coll, f"function:{f}", data.get(f), base_id)
    for f in FUNC_NUM_FIELDS:
        _index_num(pipe, coll, f"function:{f}", data.get(f), base_id)
    # file->function relationship
    pipe.sadd(f"idx:{coll}:file_funcs:{md5}", base_id)
    pipe.sadd(f"idx:{coll}:all_functions", base_id)





# ---------------------------------------------------------------------------
# Public: delete
# ---------------------------------------------------------------------------


def delete_file(r, coll, file_md5):
    """Remove a file from all indexes. Reads current data first."""
    base_id = f"{coll}:file:{file_md5}"
    doc_id = f"{base_id}:meta"
    data = r.json().get(doc_id, "$")
    if isinstance(data, list) and data:
        data = data[0]
    if not data:
        return
    pipe = r.pipeline()
    for f in FILE_TAG_FIELDS:
        _unindex_tag(pipe, coll, f"file:{f}", data.get(f), doc_id)
    for f in FILE_NUM_FIELDS:
        _unindex_num(pipe, coll, f"file:{f}", doc_id)
    pipe.srem(f"idx:{coll}:all_files", base_id)
    pipe.execute()


def delete_function(r, coll, md5, addr):
    """Remove a function from all indexes. Reads current data first."""
    base_id = f"{coll}:function:{md5}:{addr}"
    doc_id = f"{base_id}:meta"
    data = r.json().get(doc_id, "$")
    if isinstance(data, list) and data:
        data = data[0]
    if not data:
        return
    pipe = r.pipeline()
    for f in FUNC_TAG_FIELDS:
        _unindex_tag(pipe, coll, f"function:{f}", data.get(f), doc_id)
    for f in FUNC_NUM_FIELDS:
        _unindex_num(pipe, coll, f"function:{f}", doc_id)
    pipe.srem(f"idx:{coll}:file_funcs:{md5}", base_id)
    pipe.srem(f"idx:{coll}:all_functions", base_id)
    pipe.execute()


# ---------------------------------------------------------------------------
# Public: query
# ---------------------------------------------------------------------------


def query_ids(
    r, coll, doc_type, tag_filters=None, num_filters=None, offset=0, limit=100
):
    """
    Resolve filters to a list of doc IDs.
    Supports a special 'tags' filter that checks both legacy analysis 'tags' 
    and the new 'user_tags' bucket.
    """
    tag_filters = tag_filters or {}
    num_filters = num_filters or {}

    all_key = f"idx:{coll}:all_{doc_type}s"

    # Build filter keys (skip empty values)
    # filter_keys will contain (is_union, keys_list)
    filter_key_groups = []
    
    for field, value in tag_filters.items():
        if value is None or value == "":
            continue
        
        # Standard filter key
        base_prefix = f"idx:{coll}:{doc_type}:{field}:{str(value).lower()}"
        
        # SPECIAL CASE: 'tags' search should also check 'user_tags'
        if field == "tags":
            user_tags_prefix = f"idx:{coll}:{doc_type}:user_tags:{str(value).lower()}"
            filter_key_groups.append((True, [base_prefix, user_tags_prefix]))
        else:
            filter_key_groups.append((False, [base_prefix]))

    # Choose the base group: smallest group wins (simple heuristic)
    # For now, just pick the first group to keep it simple and compatible with existing logic
    if filter_key_groups:
        is_union, group_keys = filter_key_groups[0]
        if is_union:
            # Union of legacy and user tags (ensure results from either)
            candidates = list(r.sunion(*group_keys))
        else:
            candidates = list(r.smembers(group_keys[0]))
        
        other_groups = filter_key_groups[1:]
    else:
        candidates = list(r.smembers(all_key))
        other_groups = []

    # Filter candidates against remaining groups via pipeline SISMEMBER
    if other_groups and candidates:
        for is_union, group_keys in other_groups:
            pipe = r.pipeline()
            for cid in candidates:
                if is_union:
                    # For union groups, we need to check SISMEMBER for ANY of the keys
                    # This is slightly more complex in a pipeline.
                    # We'll just execute it and merge in Python for the UNION case
                    pass 
                else:
                    pipe.sismember(group_keys[0], cid)
            
            if not is_union:
                results = pipe.execute()
                candidates = [cid for cid, ok in zip(candidates, results) if ok]
            else:
                # Union filtering: Check if in ANY key
                # This is less common in secondary filters, but we support it
                results_matrix = []
                for gk in group_keys:
                    p = r.pipeline()
                    for cid in candidates:
                        p.sismember(gk, cid)
                    results_matrix.append(p.execute())
                
                new_candidates = []
                for idx, cid in enumerate(candidates):
                    if any(res[idx] for res in results_matrix):
                        new_candidates.append(cid)
                candidates = new_candidates

            if not candidates:
                break

    all_ids = candidates

    # Numeric range filters (in-memory after tag narrowing)
    if num_filters and all_ids:
        pipe = r.pipeline()
        for field, (fmin, fmax) in num_filters.items():
            pipe.zrangebyscore(f"idx:{coll}:{doc_type}:{field}", fmin, fmax)
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
                val = r.execute_command("JSON.GET", k)
                return len(str(val)) if val is not None else 0
        except Exception:
            pass
        return 0

    def estimate_total_keys(self, pattern, num_files, num_funcs, num_unique_features):
        r = self.r
        # We try to avoid a full SCAN if possible.
        if "file:*:meta" in pattern:
            return num_files
        if "function:*:*:meta" in pattern:
            return num_funcs
        if "function:*:*:source" in pattern:
            return num_funcs
        if "function:*:*:vec:tf" in pattern:
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

    def estimate_group_size(self, pattern, count_total, tracking_set=None, key_formatter=None):
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
        num_files = r.scard(f"idx:{coll}:all_files")
        num_funcs = r.scard(f"idx:{coll}:all_functions")
        num_indexed = r.scard(f"idx:{coll}:indexed:functions")
        num_unique_features = r.zcard(f"idx:{coll}:features:by_tf")
        num_sim_meta = self.estimate_total_keys(f"{coll}:sim_meta:*:*:*", num_files, num_funcs, num_unique_features)

        summary = {
            "num_files": num_files,
            "num_functions": num_funcs,
            "num_indexed": num_indexed,
            "num_missing": max(0, num_funcs - num_indexed),
            "num_features": num_unique_features,
            "num_sim_meta": num_sim_meta,
            "indexing_ratio": (num_indexed / num_funcs * 100) if num_funcs > 0 else 0
        }

        if not details:
            return summary

        # 2. Detailed Breakdown
        components = []
        patterns = [
            ("File Meta", f"{coll}:file:*:meta"),
            ("Func Meta", f"{coll}:function:*:*:meta"),
            ("Func Source", f"{coll}:function:*:*:source"),
            ("Func Vector (TF)", f"{coll}:function:*:*:vec:tf"),
            ("Sim Meta", f"{coll}:sim_meta:*:*:*"),
            ("Inverted Index", f"idx:{coll}:feature:*:functions"),
            ("Feature Meta", f"idx:{coll}:feature:*:meta"),
        ]

        for name, pat in patterns:
            count = self.estimate_total_keys(pat, num_files, num_funcs, num_unique_features)
            if count > 0:
                avg_size = self.estimate_group_size(pat, count)
                components.append({
                    "name": name,
                    "pattern": pat,
                    "count": count,
                    "avg_size": avg_size,
                    "total_size": avg_size * count
                })

        return {
            "summary": summary,
            "components": components
        }
