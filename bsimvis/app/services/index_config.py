"""
IndexConfig — single source of truth for all secondary index declarations.

Configured by source entity ("file", "func", "sim", "bin_sim").
For each field on a source entity, we declare which target levels it should
propagate to via the list: ["file", "func", "sim"].

Naming Rule for Propagated Tags:
If the field is 'tags' or 'user_tags' and it is propagated to a DIFFERENT level,
it is prefixed with the source level.
e.g. file -> tags -> sim becomes 'file_tags' at the sim level.
This avoids namespace collisions and allows direct sim-level queries like ?file_tag=x.

bin_sim level uses native fields only (no propagation to other levels).
File metadata is denormalized directly into the bin_sim index at build time.
"""

INDEX_CONFIG = {
    "file": {
        "file_name": ["file", "func", "sim"],  # propagated to sim for fast lookup
        "parent_file_name": ["file", "func", "sim"],
        "related_file_name": ["file", "func", "sim"],
        "file_md5": ["file", "func", "sim"],  # fast MD5 lookup at sim level
        "parent_md5": ["file", "func", "sim"],
        "related_md5": ["file", "func", "sim"],
        "tags": ["file", "func", "sim"],  # becomes 'file_tags' when propagated
        "user_tags": ["file", "func", "sim"],  # not propagated
        "language_id": ["file", "func", "sim"],
        "batch_uuid": ["file", "func"],
        "type": ["file", "func"],
        "batch_order": ["file", "func"],  # numeric
        "entry_date": ["file", "func"],  # numeric
        "file_date": ["file", "func"],  # numeric
        "function_count": ["file"],  # numeric
        "bsim_features_count": ["file"],  # numeric
        "cohesion_score": ["file"],  # numeric
        "bin_cluster_id": ["file"],
        "bin_cluster_uuid": ["file"],
        "bin_cluster_name": ["file"],
        "bin_cluster_stability": ["file"],
        "first_seen": ["file", "func"],
        "last_seen": ["file", "func"],
        "filetype": ["file", "func"],
        "avtype": ["file", "func"],
        "yara": ["file", "func"],
        "cc_ip": ["file", "func"],
        "file_names": ["file", "func"],
        "inferred_yara": ["file"],
        "inferred_avtype": ["file"],
        "inferred_filetype": ["file"],
        "inferred_ccip": ["file"],
        "inferred_filename": ["file"],
        "inferred_md5": ["file"],
        "note_owners": ["file"],
    },
    "func": {
        "function_name": ["func", "sim"],  # fast sim search by function name
        "tags": ["func", "sim"],  # becomes 'func_tags' when propagated
        "user_tags": ["func", "sim"],
        "namespace": ["func", "sim"],
        "return_type": ["func", "sim"],
        "parameters": ["func"],
        "calling_convention": ["func"],
        "entrypoint_address": ["func", "sim"],
        "decompiler_id": ["func"],
        "instruction_count": ["func"],  # numeric
        "bsim_features_count": ["func"],  # numeric
        "cluster_id": ["func"],
        "cluster_uuid": ["func"],
        "cluster_name": ["func"],
        "cluster_stability": ["func"],
        "note_owners": ["func"],
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
    # bin_sim: native fields only, written directly from bin_sim doc + denormalized file meta
    "bin_sim": {
        "md5_a": ["bin_sim"],
        "md5_b": ["bin_sim"],
        "file_parent_md5_a": ["bin_sim"],
        "file_parent_md5_b": ["bin_sim"],
        "file_related_md5_a": ["bin_sim"],
        "file_related_md5_b": ["bin_sim"],
        "algo": ["bin_sim"],
        "file_name_a": ["bin_sim"],  # denormalized from file meta at build time
        "file_name_b": ["bin_sim"],
        "file_parent_file_name_a": ["bin_sim"],
        "file_parent_file_name_b": ["bin_sim"],
        "file_related_file_name_a": ["bin_sim"],
        "file_related_file_name_b": ["bin_sim"],
        "file_tags_a": ["bin_sim"],  # denormalized tags for binary A
        "file_tags_b": ["bin_sim"],
        "file_user_tags_a": ["bin_sim"],
        "file_user_tags_b": ["bin_sim"],
        "score": ["bin_sim"],  # numeric
        "coverage_a": ["bin_sim"],  # numeric
        "coverage_b": ["bin_sim"],  # numeric
        "shared_clusters": ["bin_sim"],  # numeric
        "computed_at": ["bin_sim"],  # numeric (timestamp)
        "architecture_a": ["bin_sim"],
        "architecture_b": ["bin_sim"],
        "functions_count_a": ["bin_sim"],  # numeric
        "functions_count_b": ["bin_sim"],  # numeric
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
        "note_owners": ["func"],
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
    "function_count",
    "instruction_count",
    "bsim_features_count",
    "cohesion_score",
    "cluster_stability",
    "frequency",
    "tf_score",
    # bin_sim numeric fields
    "score",
    "coverage_a",
    "coverage_b",
    "shared_clusters",
    "functions_count_a",
    "functions_count_b",
    "computed_at",
}

EXACT_FIELDS = {
    "cluster_id",
    # "cluster_uuid",
    # "file_md5",
    # "batch_uuid",
}

# Fields whose values are free text rather than a controlled vocabulary. A
# filter on these with no wildcard means "contains", because nobody can type a
# 200-char demangled symbol exactly. Every other field defaults to an exact
# bucket lookup — see query_syntax.parse_filter_value().
SUBSTRING_FIELDS = {
    "function_name",
    "file_name",
    "parent_file_name",
    "related_file_name",
    "file_names",
    "yara",
    "avtype",
    "filetype",
    "cc_ip",
    "cluster_name",
    "bin_cluster_name",
    "parameters",
    "note_owners",
    "inferred_yara",
    "inferred_avtype",
    "inferred_filetype",
    "inferred_ccip",
    "inferred_filename",
    "inferred_md5",
    "file_name_a",
    "file_name_b",
    "file_parent_file_name_a",
    "file_parent_file_name_b",
    "file_related_file_name_a",
    "file_related_file_name_b",
}

# Hierarchical fields: values are paths, and every ancestor of a value is
# indexed as its own bucket at write time. `lib:uclibc:0.9.30.1:seekdir` also
# lands in `lib`, `lib:uclibc` and `lib:uclibc:0.9.30.1`, which turns the common
# "everything under this namespace" query into a single exact bucket lookup
# instead of a scan over the whole tag vocabulary.
#
# Keyed by indexed field name, so propagated variants (file_tags, func_tags)
# must be listed explicitly. Value is the list of separators, longest first —
# `namespace` will want ["::", "/"] when function namespaces are folded in.
HIERARCHY_SEPARATORS = {
    "tags": [":"],
    "user_tags": [":"],
    "file_tags": [":"],
    "file_user_tags": [":"],
    "func_tags": [":"],
    "func_user_tags": [":"],
}


def tag_ancestors(field: str, value: str) -> list[str]:
    """Ancestor bucket values for a hierarchical field value, excluding itself.

    `lib:uclibc:seekdir` -> ['lib', 'lib:uclibc']. Empty for non-hierarchical
    fields, and for values with no separator (nothing above them).

    ponytail: splits on the first separator that appears rather than tokenizing
    a grammar. Mixed-separator values (`a::b/c`) split on whichever is listed
    first; revisit if `namespace` turns out to mix them in practice.
    """
    seps = HIERARCHY_SEPARATORS.get(field)
    if not seps or not value:
        return []
    for sep in seps:
        if sep in value:
            parts = value.split(sep)
            return [sep.join(parts[:i]) for i in range(1, len(parts))]
    return []

# Fields that are auto-analysis artifacts of clustering. Clustering runs per
# namespace (a collection OR a pool), so these indexes belong to whichever
# namespace produced them and must never be merged from member collections into
# a pool — the labels would describe a different member set than the pool's own
# similarity graph. Tags and notes are the opposite: they live on the origin
# collection and ARE mirrored into every pool containing that collection.
POOL_LOCAL_FIELDS = {
    "bin_cluster_id",
    "bin_cluster_uuid",
    "bin_cluster_name",
    "bin_cluster_stability",
    "cluster_id",
    "cluster_uuid",
    "cluster_name",
    "cluster_stability",
    "inferred_yara",
    "inferred_avtype",
    "inferred_filetype",
    "inferred_ccip",
    "inferred_filename",
    "inferred_md5",
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


def get_fields_targeting_level(level: str, is_num: bool) -> list[str]:
    """Returns all fields (from any source level) that target 'level'.
    Used to ensure that all fields targeting 'level' (both native and propagated)
    are indexed when saving/processing objects at that level.
    """
    fields = set()
    for src_level, cfg in INDEX_CONFIG.items():
        if src_level == "bin_sim":
            continue
        for field, targets in cfg.items():
            if level in targets and (field in NUM_FIELDS) == is_num:
                fields.add(resolve_target_field(src_level, level, field))
    return list(fields)


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
        if src_level == "bin_sim":
            continue  # bin_sim is standalone, not propagated
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
