"""Small cluster-selection helpers shared across bin_sim / similarity / cluster services.

Dedupes the "pick the highest-cohesion cluster" logic that was previously copied into
bin_sim_service, similarity_service and cluster_service.
"""

import json
import time
from collections import Counter, defaultdict

from bsimvis.app.services.tag_taxonomy import tag_body, tag_policy, tag_prefixes

MAX_CLUSTER_NAME_LEN = 40
# Plan D5b: yara/avtype/ccip retired from cluster meta -- av:/yara:/ip: tags
# in tag_distribution cover the same ground (Decision 8). filetype is not
# here because cluster_dimensions() already builds filetype_distribution off
# the same (kept, Decision 5) raw field; listing it again just double-built
# an identical distribution.
DISTRIBUTION_FIELDS = (
    "filename",
    "md5",
)


def _tag_distribution_label(roots):
    """Most prevalent leaf under a `tag_distribution` axis, short-form, or None.

    Walks the highest-count child at each level -- same "most common wins"
    rule `Counter.most_common(1)` gave the legacy field lists -- and returns
    only the tag's last segment: `av:clamav:mirai` -> `mirai`, matching the
    short AV-family-style names this function has always produced.
    """
    if not roots:
        return None
    node = max(roots, key=lambda n: n.get("count", 0))
    while node.get("children"):
        node = max(node["children"], key=lambda c: c.get("count", 0))
    tag_id = node.get("tag_id") or ""
    return tag_id.rsplit(":", 1)[-1] if tag_id else None


def default_bin_cluster_name(
    names_list, avtype_list, yara_list, fallback, tag_distribution=None
):
    """Short, human-meaningful default name for a binary cluster.

    A raw member filename (the old default) is often a long malware-scanner
    submission name, which reads as noise in a cluster list. AV family labels
    ('Emotet', 'Gafgyt') and YARA rule names are short and already describe
    what the cluster *is*, so prefer them; fall back to a truncated filename,
    then to the caller's generic placeholder.

    Plan D, Decision 8: the source is `tag_distribution`'s `family` then
    `yara` axis (mint on write's av:/yara: tags) when the caller has one,
    same preference order as the legacy `avtype_list`/`yara_list` it replaces.
    Callers that have not fully switched over may omit `tag_distribution`,
    which falls back to the legacy lists untouched.
    """
    if tag_distribution:
        label = _tag_distribution_label(
            tag_distribution.get("family")
        ) or _tag_distribution_label(tag_distribution.get("yara"))
        if label:
            return label
    if avtype_list:
        return Counter(avtype_list).most_common(1)[0][0]
    if yara_list:
        return Counter(yara_list).most_common(1)[0][0]
    if names_list:
        name = Counter(names_list).most_common(1)[0][0]
        if len(name) > MAX_CLUSTER_NAME_LEN:
            name = name[: MAX_CLUSTER_NAME_LEN - 3] + "..."
        return name
    return fallback


def collect_member_values(metas):
    """Flatten member file metadata into the value lists a cluster summary counts.

    `metas` is an iterable of per-file meta dicts. Ingest paths store these
    fields either as a scalar or as a list, so each one is coerced here.
    Returns (names, md5s, yara, avtype, filetype, cc_ip).
    """
    names_list = []
    md5s_list = []
    yara_list = []
    avtype_list = []
    filetype_list = []
    ccip_list = []

    for m in metas:
        if m.get("file_names"):
            names_list.extend(m["file_names"])
        elif m.get("file_name"):
            names_list.append(m["file_name"])

        if m.get("file_md5"):
            md5s_list.append(m["file_md5"])

        if m.get("yara"):
            yara_list.extend(m["yara"] if isinstance(m["yara"], list) else [m["yara"]])
        if m.get("avtype"):
            avtype_list.extend(
                m["avtype"] if isinstance(m["avtype"], list) else [m["avtype"]]
            )
        if m.get("filetype"):
            filetype_list.extend(
                m["filetype"] if isinstance(m["filetype"], list) else [m["filetype"]]
            )
        if m.get("cc_ip"):
            ccip_list.extend(
                m["cc_ip"] if isinstance(m["cc_ip"], list) else [m["cc_ip"]]
            )

    return names_list, md5s_list, yara_list, avtype_list, filetype_list, ccip_list


def build_freq(items, member_count, limit=None):
    """Return up to ``limit`` value frequencies for one cluster distribution.

    `percent` is a share of `member_count` -- the cluster's members -- never of
    `items`, which is flattened: a file carrying two yara hits contributes two
    entries. Every writer of a cluster `:meta` key has to use the same
    denominator, or the same cluster reports different percents depending on
    which path wrote it last.
    """
    return (
        [
            {
                "value": k,
                "count": v,
                "percent": round((v / member_count) * 100),
            }
            for k, v in Counter(items).most_common(limit)
        ]
        if items
        else []
    )


def cluster_dimensions(metas, member_count):
    """Return categorical metadata shown by cluster views."""
    values = {
        field: []
        for field in ("filetype", "architecture", "executable_format", "batch_uuid")
    }
    for meta in metas:
        if meta.get("filetype"):
            values["filetype"].extend(_values(meta["filetype"]))
        if meta.get("language_id"):
            values["architecture"].append(meta["language_id"])
        file_format = meta.get("file_format") or {}
        executable_format = file_format.get("Executable Format") or file_format.get(
            "format"
        )
        if executable_format:
            values["executable_format"].append(executable_format)
        if meta.get("batch_uuid"):
            values["batch_uuid"].append(meta["batch_uuid"])
    return {
        f"{field}_distribution": build_freq(items, member_count)
        for field, items in values.items()
    }


def build_tag_distribution(by_axis, member_count, limit=None):
    """Build namespace trees from per-member tag hits."""
    out = {}
    for axis, items in by_axis.items():
        counts = Counter(items)
        nodes = {}
        child_ids = defaultdict(set)
        parent_ids = set()
        for tag_id in counts:
            if ":" not in tag_id:
                continue
            chain = [p for p in tag_prefixes(tag_id) if p != axis]
            body, detail = tag_body(tag_id)
            if detail and body != axis:
                chain.append(body)
            if tag_id != axis:
                chain.append(tag_id)
            parent = None
            for node_id in chain:
                node = nodes.setdefault(
                    node_id,
                    {
                        "tag_id": node_id,
                        "count": counts.get(node_id, 0),
                        "children": [],
                    },
                )
                if parent is not None and node_id not in child_ids[parent["tag_id"]]:
                    child_ids[parent["tag_id"]].add(node_id)
                    parent_ids.add(node_id)
                    parent["children"].append(node)
                parent = node

        roots = [node for node_id, node in nodes.items() if node_id not in parent_ids]
        roots = sorted(roots, key=lambda n: (-n["count"], n["tag_id"]))
        if limit is not None:
            roots = roots[:limit]

        def finish(node):
            node["coverage"] = node["count"] / member_count if member_count else 0.0
            node["children"] = [finish(child) for child in node["children"]]
            return node

        out[axis] = [finish(node) for node in roots]
    return out


def flatten_tag_distribution(distribution, axes=None):
    """Every `tag_id` in a `tag_distribution`, across the given axes (or all).

    Plan D3: the legacy `yara_distribution`/`avtype_distribution` lists fed
    keyword search by their `value`s; this is the tag-sourced equivalent so a
    search keeps matching once a cluster's only evidence is a mint-on-write
    `av:`/`yara:` tag. Walks every node, not just leaves, so a namespace or a
    vendor segment is searchable too.
    """
    out = []

    def visit(nodes):
        for node in nodes or []:
            if node.get("tag_id"):
                out.append(node["tag_id"])
            visit(node.get("children"))

    for axis in axes if axes is not None else distribution.keys():
        visit(distribution.get(axis) or [])
    return out


def set_tag_distribution_score(distribution, score):
    def visit(nodes):
        for node in nodes:
            node["score"] = score
            visit(node.get("children", []))

    visit([node for nodes in distribution.values() for node in nodes])


def _values(value):
    if not value:
        return []
    return value if isinstance(value, list) else [value]


def _member_tag_values(meta):
    values = []

    for field, source in (("tags", "analysis"), ("user_tags", "user")):
        for tag in _values(meta.get(field)):
            tag = str(tag).strip()
            if not tag or not tag_policy(tag, source).aggregate:
                continue
            body, detail = tag_body(tag)
            candidates = set(tag_prefixes(tag))
            if detail or ":" in body:
                candidates.add(body)
            if detail:
                candidates.add(tag)
            elif source == "user":
                candidates.add(f"user:{body}")
            values.extend(
                (tag_policy(candidate).axis, candidate)
                for candidate in candidates
                if tag_policy(candidate).aggregate
            )
    return values


def normalize_tag_distribution(distribution, member_count, score=None):
    """Return the current nested shape, including older stored metadata."""
    if not distribution:
        return {}
    if all(item.get("tag_id") for values in distribution.values() for item in values):
        set_tag_distribution_score(distribution, score)
        return distribution

    out = {}
    for axis, values in distribution.items():
        exact = {item.get("value"): item for item in values if item.get("value")}
        nodes = {}
        parent_ids = set()
        for tag_id, item in exact.items():
            chain = [p for p in tag_prefixes(tag_id) if p != axis] + [tag_id]
            parent = None
            for node_id in chain:
                node = nodes.setdefault(node_id, {"tag_id": node_id, "children": []})
                source = exact.get(node_id, {})
                node["count"] = source.get("count", item.get("count", 0))
                node["coverage"] = source.get("percent", item.get("percent", 0)) / 100.0
                node["score"] = score
                if parent is not None and node_id not in {
                    c["tag_id"] for c in parent["children"]
                }:
                    parent["children"].append(node)
                    parent_ids.add(node_id)
                parent = node
        roots = [node for tag_id, node in nodes.items() if tag_id not in parent_ids]
        out[axis] = sorted(roots, key=lambda n: (-n["count"], n["tag_id"]))[:10]
    return out


def cluster_summary(metas, member_count=None, fields=DISTRIBUTION_FIELDS):
    """Build every distribution stored in one cluster's metadata."""
    metas = list(metas)
    member_count = len(metas) if member_count is None else member_count
    values = collect_member_values(metas)
    value_lists = dict(
        zip(
            ("yara", "avtype", "filetype", "ccip", "filename", "md5"),
            (values[2], values[3], values[4], values[5], values[0], values[1]),
        )
    )
    result = cluster_dimensions(metas, member_count)
    result.update(
        {
            f"{field}_distribution": build_freq(value_lists[field], member_count)
            for field in fields
            if field in value_lists
        }
    )

    by_axis = defaultdict(list)
    for meta in metas:
        by_member = defaultdict(set)
        for axis, value in _member_tag_values(meta):
            by_member[axis].add(value)
        for axis, member_values in by_member.items():
            by_axis[axis].extend(member_values)
    result["tag_distribution"] = build_tag_distribution(by_axis, member_count)
    return result


def inferred_tag_values(summary, cohesion, min_cohesion, min_coverage):
    """Deepest tag per axis the cluster's members actually support.

    Empty below `min_cohesion` -- a low-cohesion cluster's majority tag is
    coincidence, not a shared trait. Above it, each axis starts at its top
    root and walks down while a child's coverage still clears `min_coverage`,
    so a label never claims more specificity than the members carry. Every
    writer (threshold, hierarchical, pool) calls this one function; see
    Decisions 1 and 3 in plan_e_inferred_tag_gate.md.

    `build_tag_distribution` roots every axis at the bare namespace token
    (e.g. `av`), which is never itself a member value -- `tag_policy` requires
    a colon to resolve a real policy, so the token that has none never enters
    `counts` and its node sits at count 0. That placeholder is skipped before
    the coverage floor is applied, or every gated axis would read 0% and
    blank unconditionally.
    """
    if cohesion is None or cohesion < min_cohesion:
        return []
    out = []
    for distribution in (summary.get("tag_distribution") or {}).values():
        if not distribution:
            continue
        node = distribution[0]
        while node is not None and not node.get("count"):
            children = node.get("children") or []
            node = max(children, key=lambda c: c.get("coverage", 0.0), default=None)
        if node is None or node.get("coverage", 0.0) < min_coverage:
            continue
        best = node
        children = node.get("children") or []
        while children:
            candidate = max(children, key=lambda c: c.get("coverage", 0.0))
            if candidate.get("coverage", 0.0) < min_coverage:
                break
            best = candidate
            children = best.get("children") or []
        if best.get("tag_id"):
            out.append(best["tag_id"])
    return out


def resolve_hierarchical_inferred_tags(leaf_to_clusters, idx_to_id, gated_by_label):
    """Per-file inferred tags for a hierarchical tree (Decision 4).

    `leaf_to_clusters` maps each leaf to its surviving ancestors ordered
    deepest-first (`cluster_common.hierarchical_membership`'s shape).
    `gated_by_label` is {cluster_id: gated inferred-tag list}, already run
    through `inferred_tag_values` -- an empty list means that cluster failed
    the gate. A file takes the first (most specific) ancestor that passed,
    never a union of the tree it survives into.
    """
    resolved = {}
    for leaf, clusters in leaf_to_clusters.items():
        tags = []
        for c in clusters:
            gated = gated_by_label.get(c)
            if gated:
                tags = gated
                break
        resolved[idx_to_id[leaf]] = tags
    return resolved


def function_count_stats(metas):
    """Min / average / max function count over a cluster's member files.

    A cluster's member count says how many binaries it holds, never how big
    they are: a 50-file cluster of 200-function droppers and one of 40k-function
    statically linked binaries read the same in the listing. Files whose meta
    carries no `function_count` are skipped rather than counted as zero, so a
    partially enriched collection does not report a min of 0 for every cluster.
    Returns {} when no member reports one.
    """
    counts = []
    for m in metas:
        value = (m or {}).get("function_count")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        counts.append(int(value))
    if not counts:
        return {}
    return {
        "min": min(counts),
        "avg": round(sum(counts) / len(counts), 1),
        "max": max(counts),
        "files": len(counts),
    }


def pick_best_shared_cluster(cids_a, cids_b, cluster_meta):
    """Highest-cohesion cluster shared by two functions, or None.

    cids_a/cids_b: iterables of cluster ids. cluster_meta: {cid -> meta dict with
    'cohesion_score'}. Returns the winning meta dict (not the id) or None when the two
    share no cluster present in cluster_meta.
    """
    best, best_coh = None, -1.0
    for cid in set(cids_a) & set(cids_b):
        m = cluster_meta.get(cid)
        if m is None:
            continue
        coh = float(m.get("cohesion_score", 0.0))
        if coh > best_coh:
            best, best_coh = m, coh
    return best


def pick_best_cluster(cids, cluster_meta):
    """Best (highest-cohesion) cluster for a single function, or None."""
    return pick_best_shared_cluster(cids, cids, cluster_meta)


def bin_cluster_ns(algo, is_container):
    """Namespace holding a file's binary clusters.

    Containers are clustered in their own tree and persisted under
    `{algo}:container`. Both trees number internal nodes from the same base, so
    the bare labels in `{file_id}:bin_clusters` collide across the two: label
    1073741828 names one cluster among containers and a different one among
    plain files. A label is only meaningful together with the namespace that
    wrote it.
    """
    from bsimvis.app.services.config_service import config_service

    if (
        config_service.get("clustering.bin_engine", "hierarchical_snn")
        == "hierarchical_snn"
    ):
        if not algo.endswith(":snn"):
            algo = f"{algo}:snn"
    return f"{algo}:container" if is_container else algo


def fetch_bin_cluster_meta(
    r, collection, entries, algo="unweighted_cosine", pool_id=None
):
    """Resolve per-file binary cluster labels to metadata, in one pipeline.

    entries: iterable of (cluster_ids, is_container) -- the raw
    `{file_id}:bin_clusters` members and the file's own container flag.

    Returns (meta_by_uuid, uuids_per_entry). Callers hand `cluster_uuid` to the
    client instead of the label: uuids are unique across namespaces and algos,
    labels are not. Labels with no metadata (written by another algo, or by a
    build that has since been cleared) are dropped.
    """
    norm = [
        (
            [c.decode() if isinstance(c, bytes) else str(c) for c in (cids or [])],
            bin_cluster_ns(algo, bool(is_container)),
        )
        for cids, is_container in entries
    ]

    keys = {}
    for cids, ns in norm:
        for cid in cids:
            if pool_id:
                # Pool clusters are keyed by uuid already and have no namespace.
                keys[(ns, cid)] = f"global:pool:{pool_id}:bin_cluster:{cid}:meta"
            else:
                keys[(ns, cid)] = f"{collection}:bin_cluster:{ns}:{cid}:meta"

    pairs = list(keys)
    pipe = r.pipeline(transaction=False)
    for k in pairs:
        pipe.get(keys[k])

    meta_by_pair = {}
    for k, raw in zip(pairs, pipe.execute()):
        if not raw:
            continue
        cm = json.loads(raw) if not isinstance(raw, dict) else raw
        if isinstance(cm, str):
            cm = json.loads(cm)
        if cm:
            meta_by_pair[k] = cm

    meta_by_uuid, per_entry = {}, []
    for cids, ns in norm:
        uuids = []
        for cid in cids:
            cm = meta_by_pair.get((ns, cid))
            if not cm:
                continue
            key = cm.get("cluster_uuid") or cid
            meta_by_uuid[key] = cm
            uuids.append(key)
        per_entry.append(uuids)
    return meta_by_uuid, per_entry


_AXES = ["overall", "code", "library", "content"]


def fetch_bin_cluster_meta_all_axes(
    r, collection, entries, algo="unweighted_cosine", pool_id=None
):
    """Like fetch_bin_cluster_meta but fetches all 4 axes at once.

    entries: iterable of (cluster_ids_by_axis, is_container) where
             cluster_ids_by_axis is a dict {axis: [cid, ...]} as built by
             search_file when it SMEMBERS all 4 axis keys per file.

    Returns (meta_by_uuid, uuids_per_entry_by_axis).
    - meta_by_uuid: {uuid -> meta dict, with "axis" key injected}
    - uuids_per_entry_by_axis: list of {axis -> [uuid, ...]} per entry
    """
    # Build flat list of (axis, ns, cid, entry_idx) tuples to pipeline
    lookups = []
    norm_entries = []
    for entry_idx, (cids_by_axis, is_container) in enumerate(entries):
        by_axis = {}
        for axis in _AXES:
            axis_algo = f"{algo}:{axis}" if axis != "overall" else algo
            ns = bin_cluster_ns(axis_algo, bool(is_container))
            cids = [
                c.decode() if isinstance(c, bytes) else str(c)
                for c in (cids_by_axis.get(axis) or [])
            ]
            by_axis[axis] = (ns, cids)
            for cid in cids:
                lookups.append((entry_idx, axis, ns, cid))
        norm_entries.append(by_axis)

    # Single pipeline for all keys
    pipe = r.pipeline(transaction=False)
    unique_keys = {}
    for _, axis, ns, cid in lookups:
        if pool_id:
            key = f"global:pool:{pool_id}:bin_cluster:{cid}:meta"
        else:
            key = f"{collection}:bin_cluster:{ns}:{cid}:meta"
        unique_keys[(axis, ns, cid)] = key

    unique_list = list(unique_keys.items())
    for _, key in unique_list:
        pipe.get(key)

    parsed_meta = {}
    for (k_tuple, _), raw in zip(unique_list, pipe.execute()):
        if not raw:
            continue
        cm = json.loads(raw) if not isinstance(raw, dict) else raw
        if isinstance(cm, str):
            cm = json.loads(cm)
        if cm:
            parsed_meta[k_tuple] = cm

    meta_by_uuid = {}
    # (entry_idx, axis, cid) -> uuid
    resolved = {}
    for entry_idx, axis, ns, cid in lookups:
        cm = parsed_meta.get((axis, ns, cid))
        if not cm:
            continue

        # Copy to avoid mutating shared dict if we inject axis
        # But wait, axis is the same for the same (axis, ns, cid)

        uuid = cm.get("cluster_uuid") or cid
        cm["axis"] = axis
        meta_by_uuid[uuid] = cm
        resolved[(entry_idx, axis, cid)] = uuid

    uuids_per_entry_by_axis = []
    for entry_idx, by_axis in enumerate(norm_entries):
        result = {}
        for axis, (ns, cids) in by_axis.items():
            uuids = []
            for cid in cids:
                uuid = resolved.get((entry_idx, axis, cid))
                if uuid:
                    uuids.append(uuid)
            if uuids:
                result[axis] = uuids
        uuids_per_entry_by_axis.append(result)

    return meta_by_uuid, uuids_per_entry_by_axis


def _walk(distribution):
    for nodes in distribution.values():
        stack = list(nodes)
        while stack:
            node = stack.pop()
            yield node
            stack.extend(node["children"])


def real_links(links):
    """Drop links to the synthetic global root: it is never a cluster, so it
    has no meta and every top-level cluster would show it as a nameless parent.
    Every real cluster is some link's child; the root never is."""
    children = {l["child"] for l in links}
    return [l for l in links if l["parent"] in children]


def get_tree_links(r, collection, algo, tree_links_prefix="cluster"):
    """child_to_parent / parent_to_children maps from the stored tree_links blob.

    One GET regardless of collection size -- callers used to rebuild this by
    reading every cluster's meta key just to find its parent.
    """
    links_key = f"{collection}:{tree_links_prefix}:tree_links:{algo}"
    links_raw = r.get(links_key)
    child_to_parent, parent_to_children = {}, {}
    if links_raw:
        try:
            links = real_links(json.loads(links_raw))
            for l in links:
                c, p = str(l["child"]), str(l["parent"])
                child_to_parent[c] = p
                parent_to_children.setdefault(p, []).append(c)
        except Exception:
            pass
    return child_to_parent, parent_to_children


def resolve_cluster_id_by_uuid(r, collection, algo, uuid, meta_prefix="cluster"):
    """cluster_id for a uuid via the small uf:uuid hash, not a full meta scan.

    The hierarchical_snn/HDBSCAN engine never writes that hash (its labels
    come from a fresh uuid.uuid4() per build, kept only in the meta blob) --
    fall back to a cached map built by one pipelined meta scan.
    """
    uuid = str(uuid or "").lower()
    if not uuid:
        return None
    uuid_key = f"{collection}:{meta_prefix}:{algo}:uf:uuid"
    pairs = r.hgetall(uuid_key) or {}
    for cid, u in pairs.items():
        cid = cid.decode() if isinstance(cid, bytes) else cid
        u = u.decode() if isinstance(u, bytes) else u
        if str(u).lower() == uuid:
            return cid
    if pairs:
        return None  # hash exists and was checked; a real miss, not "unwritten"

    ns = (collection, algo, meta_prefix)
    meta_key_prefix = f"{collection}:{meta_prefix}:{algo}:"
    cached = _UUID_MAPS.get(ns, {}).get(uuid)
    if cached:
        # A rebuild re-mints uuids: trust the cache only if the meta still agrees.
        m = get_cluster_metas(r, collection, algo, [cached], meta_prefix).get(cached)
        if m and str(m.get("cluster_uuid", "")).lower() == uuid:
            return cached
    elif ns in _UUID_MAPS and time.time() - _UUID_MAPS_AT[ns] < 30:
        return None  # map is fresh; don't rescan for a bogus uuid

    _UUID_MAPS[ns] = _scan_uuid_map(r, collection, algo, meta_prefix)
    _UUID_MAPS_AT[ns] = time.time()
    return _UUID_MAPS[ns].get(uuid)


# ponytail: per-process uuid -> cid maps for engines with no uf:uuid hash;
# one pipelined meta scan per rebuild, then O(1). Persist a uuid hash at build
# time if the first-hit scan (~3s at 130k clusters) becomes a problem.
_UUID_MAPS = {}
_UUID_MAPS_AT = {}


def _scan_uuid_map(r, collection, algo, meta_prefix):
    list_key = f"{collection}:{meta_prefix}:list:{algo}"
    meta_key_prefix = f"{collection}:{meta_prefix}:{algo}:"
    cids_raw = r.smembers(list_key) or set()
    if not cids_raw:
        # The hierarchical/HDBSCAN engine never populates the `list` set
        # (only the union-find engine does) -- same bounded SCAN the plain
        # listing route falls back to, never a blocking KEYS.
        pattern = f"{meta_key_prefix}*:meta"
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            for k in keys:
                k = k.decode() if isinstance(k, bytes) else k
                cids_raw.add(k[len(meta_key_prefix) : -len(":meta")])
            if cursor == 0:
                break
    cids = {c.decode() if isinstance(c, bytes) else str(c) for c in cids_raw}
    # The list set can lag an incremental rebuild; tree_links never does.
    c2p, _ = get_tree_links(r, collection, algo, meta_prefix)
    cids.update(c2p)
    cids.update(c2p.values())
    cids = list(cids)

    out = {}
    for i in range(0, len(cids), 5000):
        chunk = cids[i : i + 5000]
        pipe = r.pipeline(transaction=False)
        for cid in chunk:
            pipe.get(f"{meta_key_prefix}{cid}:meta")
        for cid, blob in zip(chunk, pipe.execute()):
            if not blob:
                continue
            m = json.loads(blob)
            if isinstance(m, str):
                m = json.loads(m)
            u = str(m.get("cluster_uuid") or "").lower()
            if u:
                out[u] = cid
    return out


def get_cluster_metas(r, collection, algo, cids, meta_prefix="cluster"):
    """Pipelined GET + parse of only the given cluster ids' meta blobs."""
    cids = [str(c) for c in cids]
    if not cids:
        return {}
    pipe = r.pipeline(transaction=False)
    for cid in cids:
        pipe.get(f"{collection}:{meta_prefix}:{algo}:{cid}:meta")
    raw = pipe.execute()
    out = {}
    for cid, blob in zip(cids, raw):
        if not blob:
            continue
        m = json.loads(blob) if not isinstance(blob, dict) else blob
        if isinstance(m, str):
            m = json.loads(m)
        out[cid] = m
    return out


def cluster_tree_slice(
    r,
    collection,
    algo,
    uuid,
    up=3,
    down=3,
    width=10,
    meta_prefix="cluster",
    tree_links_prefix=None,
    links=None,
):
    """A bounded window of the cluster tree around `uuid`, cheap at any collection size.

    Never reads more metas than the window actually needs: `up` ancestors on
    the path, plus each visited node's top-`width` children by member_count.
    Returns (nodes: {cid: meta}, center_cid, hidden_above: int) so the caller
    can render "N more above" without walking further.
    """
    tree_links_prefix = tree_links_prefix or meta_prefix
    child_to_parent, parent_to_children = links or get_tree_links(
        r, collection, algo, tree_links_prefix
    )
    center = resolve_cluster_id_by_uuid(r, collection, algo, uuid, meta_prefix)
    if center is None:
        return {}, None, 0

    # Ancestor chain, capped at `up` hops; count how many more exist above.
    ancestors = []
    curr = center
    while curr in child_to_parent:
        curr = child_to_parent[curr]
        ancestors.append(curr)
    shown_ancestors = ancestors[:up]
    hidden_above = max(0, len(ancestors) - up)

    needed = set(shown_ancestors)
    needed.add(center)

    # Down: BFS by level, top-`width` children per node by member_count.
    frontier = [center]
    for _ in range(down):
        next_frontier = []
        for node in frontier:
            child_ids = parent_to_children.get(node, [])
            if not child_ids:
                continue
            child_metas = get_cluster_metas(r, collection, algo, child_ids, meta_prefix)
            ranked = sorted(
                child_ids,
                key=lambda c: child_metas.get(c, {}).get("member_count", 0),
                reverse=True,
            )
            shown = ranked[:width]
            needed.update(shown)
            next_frontier.extend(shown)
        frontier = next_frontier

    metas = get_cluster_metas(r, collection, algo, needed, meta_prefix)
    return metas, center, hidden_above


def cluster_children_page(
    r,
    collection,
    algo,
    parent_cid,
    offset=0,
    width=10,
    meta_prefix="cluster",
    links=None,
):
    """One page of a single node's children, sorted by member_count desc.

    Used for the tree's "+N more" row and for climbing further above the
    ancestor window ("N more above" -> re-run with a bigger `up`, or for a
    single extra parent hop this covers it). Bounded by that node's own
    fan-out, never the whole collection.
    """
    _, parent_to_children = links or get_tree_links(r, collection, algo, meta_prefix)
    child_ids = parent_to_children.get(str(parent_cid), [])
    metas = get_cluster_metas(r, collection, algo, child_ids, meta_prefix)
    ranked = sorted(
        child_ids, key=lambda c: metas.get(c, {}).get("member_count", 0), reverse=True
    )
    page = ranked[offset : offset + width]
    return {c: metas[c] for c in page if c in metas}, len(child_ids)


def demo():
    """The collision this resolver exists for: one label, two namespaces."""

    class FakeRedis:
        def __init__(self, kv):
            self.kv, self.queue = kv, []

        def pipeline(self, transaction=False):
            return self

        def get(self, k):
            self.queue.append(k)

        def execute(self):
            out, self.queue = [self.kv.get(k) for k in self.queue], []
            return out

    label = "1073741828"
    col = "C"
    r = FakeRedis(
        {
            f"{col}:bin_cluster:unweighted_cosine:{label}:meta": json.dumps(
                {"cluster_uuid": "aaaa", "cluster_name": "elf-cluster"}
            ),
            f"{col}:bin_cluster:unweighted_cosine:container:{label}:meta": json.dumps(
                {"cluster_uuid": "bbbb", "cluster_name": "zip-cluster"}
            ),
        }
    )

    metas, per_entry = fetch_bin_cluster_meta(
        r, col, [([label], True), ([label], False), (["99"], False)]
    )
    assert per_entry == [["bbbb"], ["aaaa"], []], per_entry
    assert metas["bbbb"]["cluster_name"] == "zip-cluster", metas
    assert metas["aaaa"]["cluster_name"] == "elf-cluster", metas
    assert bin_cluster_ns("x", True) == "x:container"

    # inferred_tag_values: gated out below cohesion.
    summary = cluster_summary([{"tags": ["av:clamav:mirai"]}] * 10, member_count=10)
    assert inferred_tag_values(summary, 0.4, 0.5, 0.5) == []

    # Gated out below coverage: only 3/10 members carry the tag.
    metas = [{"tags": ["av:clamav:mirai"]}] * 3 + [{}] * 7
    summary = cluster_summary(metas, member_count=10)
    assert inferred_tag_values(summary, 0.9, 0.5, 0.5) == []

    # Deepest passing node chosen: root at 100%, its child at 60% (>= floor).
    metas = [{"tags": ["av:clamav:mirai"]}] * 6 + [{"tags": ["av:clamav"]}] * 4
    summary = cluster_summary(metas, member_count=10)
    assert inferred_tag_values(summary, 0.9, 0.5, 0.5) == ["av:clamav:mirai"]

    # Same shape, but the child falls under the floor -- root wins instead.
    metas = [{"tags": ["av:clamav:mirai"]}] * 3 + [{"tags": ["av:clamav"]}] * 7
    summary = cluster_summary(metas, member_count=10)
    assert inferred_tag_values(summary, 0.9, 0.5, 0.5) == ["av:clamav"]

    # One member per prefix counted once: a single member with two leaf tags
    # sharing an ancestor ("av:clamav") contributes 1 to that ancestor, not 2.
    metas = [{"tags": ["av:clamav:mirai", "av:clamav:gafgyt"]}]
    summary = cluster_summary(metas, member_count=1)
    root = summary["tag_distribution"]["family"][0]
    assert root["tag_id"] == "av" and root["count"] == 0, root
    shared = root["children"][0]
    assert shared["tag_id"] == "av:clamav" and shared["count"] == 1, shared
    assert len(shared["children"]) == 2, shared["children"]

    # A `#detail` tail is the deepest node, hung off its body.
    summary = cluster_summary(
        [{"tags": ["yara:unknown:unknown#rule_x"]}] * 10, member_count=10
    )
    nodes = {n["tag_id"]: n for n in _walk(summary["tag_distribution"])}
    body = nodes["yara:unknown:unknown"]
    assert [c["tag_id"] for c in body["children"]] == [
        "yara:unknown:unknown#rule_x"
    ], body
    assert inferred_tag_values(summary, 0.9, 0.5, 0.5) == [
        "yara:unknown:unknown#rule_x"
    ]

    # resolve_hierarchical_inferred_tags: most specific passing cluster wins,
    # never a union of the ancestors a leaf survives into.
    resolved = resolve_hierarchical_inferred_tags(
        leaf_to_clusters={0: [20, 10], 1: [10]},
        idx_to_id={0: "fileA", 1: "fileB"},
        gated_by_label={20: [], 10: ["av:clamav"]},
    )
    assert resolved == {
        "fileA": ["av:clamav"],
        "fileB": ["av:clamav"],
    }, resolved

    print("cluster_utils demo OK")


if __name__ == "__main__":
    demo()
