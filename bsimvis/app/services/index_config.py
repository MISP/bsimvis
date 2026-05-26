"""
IndexConfig — single source of truth for all secondary index declarations.

Configured by source entity ("file", "func", "sim").
For each field on a source entity, we declare which target levels it should
propagate to via the list: ["file", "func", "sim"].

Naming Rule for Propagated Tags:
If the field is 'tags' or 'user_tags' and it is propagated to a DIFFERENT level,
it is prefixed with the source level.
e.g. file -> tags -> sim becomes 'file_tags' at the sim level.
This avoids namespace collisions and allows direct sim-level queries like ?file_tag=x.
"""

INDEX_CONFIG = {
    "file": {
        "file_name": ["file", "func", "sim"],  # propagated to sim for fast lookup
        "file_md5": ["file", "func", "sim"],  # fast MD5 lookup at sim level
        "tags": ["file", "func", "sim"],  # becomes 'file_tags' when propagated
        "user_tags": ["file", "func", "sim"],  # not propagated
        "language_id": ["file", "func", "sim"],
        "batch_uuid": ["file", "func", "sim"],
        "type": ["file", "func", "sim"],
        "batch_order": ["file", "func", "sim"],  # numeric
        "entry_date": ["file", "func", "sim"],  # numeric
        "file_date": ["file", "func", "sim"],  # numeric
    },
    "func": {
        "function_name": ["func", "sim"],  # fast sim search by function name
        "tags": ["func", "sim"],  # becomes 'func_tags' when propagated
        "user_tags": ["func", "sim"],
        "namespace": ["func", "sim"],
        "return_type": ["func", "sim"],
        "parameters": ["func", "sim"],
        "calling_convention": ["func", "sim"],
        "entrypoint_address": ["func", "sim"],
        "decompiler_id": ["func", "sim"],
        "instruction_count": ["func", "sim"],  # numeric
        "bsim_features_count": ["func", "sim"],  # numeric
        "cluster_id": ["func"],
        "cluster_uuid": ["func"],
        "cluster_name": ["func"],
        "cluster_stability": ["func"],
    },
    "sim": {
        "tags": ["sim"],
        "user_tags": ["sim"],
        "is_cross_binary": ["sim"],
    },
    "feature": {
        "hash": ["feature"],
        "type": ["feature"],
        "op": ["feature"],
        "frequency": ["feature"],
        "tf_score": ["feature"],
    },
}

INDEX_CONFIG_legacy = {
    "file": {
        "file_name": ["file", "func", "sim"],  # propagated to sim for fast lookup
        "file_md5": ["file", "func", "sim"],  # fast MD5 lookup at sim level
        "tags": ["file", "func", "sim"],  # becomes 'file_tags' when propagated
        "user_tags": ["file"],  # not propagated
        "language_id": ["file", "func", "sim"],
        "batch_uuid": ["file", "func"],
        "type": ["file", "func"],
        "batch_order": ["file", "func"],  # numeric
        "entry_date": ["file", "func"],  # numeric
        "file_date": ["file", "func"],  # numeric
    },
    "func": {
        "function_name": ["func", "sim"],  # fast sim search by function name
        "tags": ["func", "sim"],  # becomes 'func_tags' when propagated
        "user_tags": ["func"],
        "namespace": ["func", "sim"],
        "return_type": ["func"],
        "parameters": ["func"],
        "calling_convention": ["func"],
        "entrypoint_address": ["func", "sim"],
        "decompiler_id": ["func"],
        "instruction_count": ["func"],  # numeric
        "bsim_features_count": ["func"],  # numeric
    },
    "sim": {
        "tags": ["sim"],
        "user_tags": ["sim"],
        "cross_binary": ["sim"],
    },
}

NUM_FIELDS = {
    "batch_order",
    "entry_date",
    "file_date",
    "instruction_count",
    "bsim_features_count",
    "cluster_stability",
    "frequency",
    "tf_score",
}

EXACT_FIELDS = {
    "cluster_id",
    # "cluster_uuid",
    # "file_md5",
    # "batch_uuid",
}


def resolve_target_field(source_level: str, target_level: str, field: str) -> str:
    """
    Determines the name of the field as it is indexed at the target level.
    Only tags/user_tags are prefixed when crossing levels.
    """
    if source_level == target_level:
        return field
    if field in ["tags", "user_tags", "static_tags"]:
        return f"{source_level}_{field}"
    return field


# ---------------------------------------------------------------------------
# Accessors for index_service.py (Base entity saves)
# ---------------------------------------------------------------------------


def get_native_fields(source_level: str, is_num: bool) -> list[str]:
    """Returns fields that natively belong to this source level (no prefixing).
    Called when saving a file or func to its OWN level.
    """
    cfg = INDEX_CONFIG.get(source_level, {})
    return [
        field
        for field in cfg.keys()
        if (field in NUM_FIELDS) == is_num and source_level in cfg[field]
    ]


# ---------------------------------------------------------------------------
# Accessors for similarity_service.py (Propagation during build)
# ---------------------------------------------------------------------------


def get_propagated_fields(target_level: str) -> dict:
    """
    Returns a dictionary of all fields that should be written to the target_level,
    grouped by their original source_level.

    Returns: {
      "file": [ (original_field, target_field_name), ... ],
      "func": [ (original_field, target_field_name), ... ],
      "sim":  [ (original_field, target_field_name), ... ]
    }
    """
    result = {"file": [], "func": [], "sim": []}
    for src_level, fields in INDEX_CONFIG.items():
        for field, targets in fields.items():
            if target_level in targets and field not in NUM_FIELDS:
                target_field = resolve_target_field(src_level, target_level, field)
                result[src_level].append((field, target_field))
    return result


# ---------------------------------------------------------------------------
# Accessors for search routes
# ---------------------------------------------------------------------------


def get_search_paths_for_field(
    field: str, requested_target: str
) -> list[list[tuple[str, str]]]:
    """
    Returns a list of fallback paths for a given field across ALL source entities that declare it.

    Example: get_search_paths_for_field("tags", "sim") returns:
    [
        [("sim", "tags")],                                                   # tags originating from sim
        [("sim", "func_tags"), ("func", "tags")],                            # tags originating from func
        [("sim", "file_tags"), ("func", "file_tags"), ("file", "tags")]      # tags originating from file
    ]

    The search engine should evaluate each path independently, stopping at the first
    level in a path that returns results (short-circuiting the join for that specific source).
    """
    order = ["sim", "func", "file", "feature"]
    try:
        start_idx = order.index(requested_target)
    except ValueError:
        return []

    paths = []
    for source_level, cfg in INDEX_CONFIG.items():
        if field not in cfg:
            continue

        targets = cfg[field]
        path = []
        for lvl in order[start_idx:]:
            if lvl in targets:
                target_field = resolve_target_field(source_level, lvl, field)
                path.append((lvl, target_field))

        if path:
            paths.append(path)

    return paths


def get_propagation_targets(source_level: str, field: str) -> list[str]:
    """Returns the list of levels a field should propagate to (excluding source)."""
    cfg = INDEX_CONFIG.get(source_level, {}).get(field, [])
    return [lvl for lvl in cfg if lvl != source_level]
