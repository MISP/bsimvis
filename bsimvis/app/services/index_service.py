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
    "file_name",
]
FUNC_TAG_FIELDS = [
    "type",
    "collection",
    "batch_uuid",
    "file_md5",
    "language_id",
    "tags",
    "file_name",
    "function_name",
    "decompiler_id",
    "return_type",
    "calling_convention",
    "entrypoint_address",
]
SIM_TAG_FIELDS = [
    "type",
    "collection",
    "algo",
    "md5_1",
    "md5_2",
    "is_cross_binary",
    "id1",
    "id2",
    "name1",
    "name2",
    "tags1",
    "tags2",
    "batch_uuid1",
    "batch_uuid2",
    "language_id1",
    "language_id2",
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
SIM_NUM_FIELDS = ["score", "feat_count1", "feat_count2", "min_features", "entry_date"]


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
        pipe.sadd(f"idx:{coll}:{field}:{str(v).lower()}", doc_id)


def _unindex_tag(pipe, coll, field, value, doc_id):
    """Remove doc_id from the tag set for field=value."""
    if value is None:
        return
    values = value if isinstance(value, list) else [value]
    for v in values:
        if v is None or v == "":
            continue
        pipe.srem(f"idx:{coll}:{field}:{str(v).lower()}", doc_id)


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
    doc_id = f"{coll}:file:{file_md5}:meta"
    for f in FILE_TAG_FIELDS:
        _index_tag(pipe, coll, f"file:{f}", data.get(f), doc_id)
    for f in FILE_NUM_FIELDS:
        _index_num(pipe, coll, f"file:{f}", data.get(f), doc_id)
    # Track count
    pipe.sadd(f"idx:{coll}:all_files", doc_id)


def save_function(pipe, coll, md5, addr, data):
    """Index all fields for a function doc."""
    doc_id = f"{coll}:function:{md5}:{addr}:meta"
    for f in FUNC_TAG_FIELDS:
        _index_tag(pipe, coll, f"function:{f}", data.get(f), doc_id)
    for f in FUNC_NUM_FIELDS:
        _index_num(pipe, coll, f"function:{f}", data.get(f), doc_id)
    # file->function relationship
    pipe.sadd(f"idx:{coll}:file_funcs:{md5}", doc_id)
    pipe.sadd(f"idx:{coll}:all_functions", doc_id)


def save_similarity(pipe, coll, sim_id, data):
    """Index all fields for a similarity doc."""
    doc_id = f"{coll}:sim_meta:{sim_id}"
    for f in SIM_TAG_FIELDS:
        _index_tag(pipe, coll, f"sim:{f}", data.get(f), doc_id)
    for f in SIM_NUM_FIELDS:
        _index_num(pipe, coll, f"sim:{f}", data.get(f), doc_id)
    # Fast lookup by md5 pair
    md5_1 = data.get("md5_1")
    score = data.get("score", 0)
    md5_2 = data.get("md5_2")
    if md5_1 and md5_2:
        pipe.zadd(f"idx:{coll}:sim:{md5_1}", {doc_id: float(score)})

    # Global scoreboard for the algorithm
    parts = sim_id.split(":")
    if parts:
        algo = parts[0]
        pipe.zadd(f"{coll}:all_sim:{algo}", {doc_id: float(score)})

    pipe.sadd(f"idx:{coll}:all_similarities", doc_id)


# ---------------------------------------------------------------------------
# Public: delete
# ---------------------------------------------------------------------------


def delete_file(r, coll, file_md5):
    """Remove a file from all indexes. Reads current data first."""
    doc_id = f"{coll}:file:{file_md5}:meta"
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
    pipe.srem(f"idx:{coll}:all_files", doc_id)
    pipe.execute()


def delete_function(r, coll, md5, addr):
    """Remove a function from all indexes. Reads current data first."""
    doc_id = f"{coll}:function:{md5}:{addr}:meta"
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
    pipe.srem(f"idx:{coll}:file_funcs:{md5}", doc_id)
    pipe.srem(f"idx:{coll}:all_functions", doc_id)
    pipe.execute()


# ---------------------------------------------------------------------------
# Public: query
# ---------------------------------------------------------------------------


def query_ids(
    r, coll, doc_type, tag_filters=None, num_filters=None, offset=0, limit=100
):
    """
    Resolve filters to a list of doc IDs.

    tag_filters:  {field: value}  – exact match (uses TAG sets)
    num_filters:  {field: (min, max)}  – range (uses NUMERIC ZSETs)

    Strategy (Kvrocks-compatible, no SINTER):
      1. Pick the most specific key as base (smallest set wins).
      2. SMEMBERS the base key.
      3. For remaining keys, use a pipeline of SISMEMBER to filter.

    Returns (ids_page, total_count).
    """
    tag_filters = tag_filters or {}
    num_filters = num_filters or {}

    all_key = f"idx:{coll}:all_{doc_type}s"

    # Build filter keys (skip empty values)
    filter_keys = []
    for field, value in tag_filters.items():
        if value is None or value == "":
            continue
        filter_keys.append(f"idx:{coll}:{doc_type}:{field}:{str(value).lower()}")

    # Choose the base key: first specific filter if any, else all_key
    if filter_keys:
        base_key = filter_keys[0]
        other_keys = filter_keys[1:]
    else:
        base_key = all_key
        other_keys = []

    candidates = list(r.smembers(base_key))

    # Filter candidates against remaining sets via pipeline SISMEMBER
    if other_keys and candidates:
        for check_key in other_keys:
            pipe = r.pipeline()
            for cid in candidates:
                pipe.sismember(check_key, cid)
            results = pipe.execute()
            candidates = [cid for cid, ok in zip(candidates, results) if ok]

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
