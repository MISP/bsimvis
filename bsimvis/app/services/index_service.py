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


def _index_tag(pipe, coll, level, field, value, doc_id):
    """Add doc_id to the tag set for field=value in a standardized registry/bucket structure."""
    if value is None:
        return
    # Handle list values (e.g. tags)
    values = value if isinstance(value, list) else [value]
    for v in values:
        if v is None or v == "":
            continue
        # Standardized Bucket: idx:{col}:idx:{level}:{field}:{value}
        bucket_key = f"idx:{coll}:idx:{level}:{field}:{str(v).lower()}"
        pipe.sadd(bucket_key, doc_id)
        # Standardized Registry: idx:{coll}:reg:{level}:{field} (points to many buckets)
        registry_key = f"idx:{coll}:reg:{level}:{field}"
        pipe.sadd(registry_key, bucket_key)

        # AUTO-DISCOVERY: Ensure tags are registered in global metadata
        if "tags" in field:
            meta_key = f"idx:{coll}:tags_metadata"
            import random
            palette = ["#FF5555", "#50FA7B", "#F1FA8C", "#BD93F9", "#FF79C6", "#8BE9FD", "#FFB86C", "#A6E22E", "#66D9EF"]
            default_meta = json.dumps({"color": random.choice(palette), "priority": 0})
            pipe.hsetnx(meta_key, str(v), default_meta)


def _unindex_tag(pipe, coll, level, field, value, doc_id):
    """Remove doc_id from the tag set for field=value."""
    if value is None:
        return
    values = value if isinstance(value, list) else [value]
    for v in values:
        if v is None or v == "":
            continue
        bucket_key = f"idx:{coll}:idx:{level}:{field}:{str(v).lower()}"
        pipe.srem(bucket_key, doc_id)


def _index_num(pipe, coll, level, field, value, doc_id):
    """Add doc_id to the numeric ZSET for field."""
    if value is None:
        return
    try:
        # Standard Numeric Index: idx:{col}:idx:{level}:{field}
        pipe.zadd(f"idx:{coll}:idx:{level}:{field}", {doc_id: float(value)})
    except (ValueError, TypeError):
        pass


def _unindex_num(pipe, coll, level, field, doc_id):
    """Remove doc_id from the numeric ZSET."""
    pipe.zrem(f"idx:{coll}:idx:{level}:{field}", doc_id)


# ---------------------------------------------------------------------------
# Public: save
# ---------------------------------------------------------------------------


def save_file(pipe, coll, file_md5, data):
    """Index all fields for a file doc. Standardized as idx:{col}:file:{md5}"""
    base_id = f"idx:{coll}:file:{file_md5}"
    for f in FILE_TAG_FIELDS:
        _index_tag(pipe, coll, "file", f, data.get(f), base_id)
    for f in FILE_NUM_FIELDS:
        _index_num(pipe, coll, "file", f, data.get(f), base_id)
    pipe.sadd(f"idx:{coll}:all_files", base_id)


def save_function(pipe, coll, md5, addr, data):
    """Index all fields for a function doc. Standardized as idx:{col}:func:{md5}:{addr}"""
    base_id = f"idx:{coll}:func:{md5}:{addr}"
    for f in FUNC_TAG_FIELDS:
        _index_tag(pipe, coll, "func", f, data.get(f), base_id)
    for f in FUNC_NUM_FIELDS:
        _index_num(pipe, coll, "func", f, data.get(f), base_id)
    # relationship links
    pipe.sadd(f"idx:{coll}:file_funcs:{md5}", base_id)
    pipe.sadd(f"idx:{coll}:all_functions", base_id)


# ---------------------------------------------------------------------------
# Public: delete
# ---------------------------------------------------------------------------


def delete_file(r, coll, file_md5):
    """Remove a file from all indexes."""
    base_id = f"idx:{coll}:file:{file_md5}"
    doc_id = f"{base_id}:meta"
    data = r.json().get(doc_id, "$")
    if isinstance(data, list) and data:
        data = data[0]
    if not data:
        return
    pipe = r.pipeline()
    for f in FILE_TAG_FIELDS:
        _unindex_tag(pipe, coll, "file", f, data.get(f), base_id)
    for f in FILE_NUM_FIELDS:
        _unindex_num(pipe, coll, "file", f, base_id)
    pipe.srem(f"idx:{coll}:all_files", base_id)
    pipe.execute()


def delete_function(r, coll, md5, addr):
    """Remove a function from all indexes."""
    base_id = f"idx:{coll}:func:{md5}:{addr}"
    doc_id = f"{base_id}:meta"
    data = r.json().get(doc_id, "$")
    if isinstance(data, list) and data:
        data = data[0]
    if not data:
        return
    pipe = r.pipeline()
    for f in FUNC_TAG_FIELDS:
        _unindex_tag(pipe, coll, "func", f, data.get(f), base_id)
    for f in FUNC_NUM_FIELDS:
        _unindex_num(pipe, coll, "func", f, base_id)
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
    Resolve filters to a list of doc IDs using standardized buckets.
    """
    tag_filters = tag_filters or {}
    num_filters = num_filters or {}
    
    # Internal level mapping: API 'function' -> internal 'func'
    lvl = "func" if doc_type == "function" else doc_type

    all_key = f"idx:{coll}:all_{doc_type}s"

    filter_key_groups = []
    
    for field, value in tag_filters.items():
        if value is None or value == "":
            continue
        
        # Standard Bucket: idx:{col}:idx:{level}:{field}:{value}
        base_prefix = f"idx:{coll}:idx:{lvl}:{field}:{str(value).lower()}"
        
        # User Tag Union Logic
        if field == "tags":
            user_tags_prefix = f"idx:{coll}:idx:{lvl}:user_tags:{str(value).lower()}"
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
            if not candidates: break
            if not is_union:
                pipe = r.pipeline()
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
                            exists = True; break
                    if exists: new_candidates.append(cid)
                candidates = new_candidates

    all_ids = candidates

    # Standardized Numerical Filtering: idx:{col}:idx:{level}:{field}
    if num_filters and all_ids:
        pipe = r.pipeline()
        for field, (fmin, fmax) in num_filters.items():
            pipe.zrangebyscore(f"idx:{coll}:idx:{lvl}:{field}", fmin, fmax)
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
        num_sim_meta = self.estimate_total_keys(f"idx:{coll}:sim:*:*:*", num_files, num_funcs, num_unique_features)

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
            ("File Meta", f"idx:{coll}:file:*:meta"),
            ("Func Meta", f"idx:{coll}:func:*:*:meta"),
            ("Func Source", f"idx:{coll}:func:*:*:source"),
            ("Func Vector (TF)", f"idx:{coll}:func:*:*:vec:tf"),
            ("Sim Meta", f"idx:{coll}:sim:*:*:*"),
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
