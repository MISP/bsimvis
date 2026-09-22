"""Small cluster-selection helpers shared across bin_sim / similarity / cluster services.

Dedupes the "pick the highest-cohesion cluster" logic that was previously copied into
bin_sim_service, similarity_service and cluster_service.
"""

import json
from collections import Counter, defaultdict

from bsimvis.app.services.tag_taxonomy import tag_body, tag_policy, tag_prefixes

MAX_CLUSTER_NAME_LEN = 40
DISTRIBUTION_FIELDS = (
    "yara",
    "avtype",
    "filetype",
    "ccip",
    "filename",
    "md5",
)


def default_bin_cluster_name(names_list, avtype_list, yara_list, fallback):
    """Short, human-meaningful default name for a binary cluster.

    A raw member filename (the old default) is often a long malware-scanner
    submission name, which reads as noise in a cluster list. AV family labels
    ('Emotet', 'Gafgyt') and YARA rule names are short and already describe
    what the cluster *is*, so prefer them; fall back to a truncated filename,
    then to the caller's generic placeholder.
    """
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


def build_freq(items, member_count, limit=5):
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


def build_tag_distribution(by_axis, member_count, limit=10):
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
        roots = sorted(roots, key=lambda n: (-n["count"], n["tag_id"]))[:limit]

        def finish(node):
            node["coverage"] = node["count"] / member_count if member_count else 0.0
            node["children"] = [finish(child) for child in node["children"]]
            return node

        out[axis] = [finish(node) for node in roots]
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


def inferred_tag_values(summary):
    """Top derived tag per axis, kept separate from analyst-evidence tags."""
    return [
        f"inferred:{item['tag_id']}"
        for distribution in (summary.get("tag_distribution") or {}).values()
        for item in distribution[:1]
        if item.get("tag_id")
    ]


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
        config_service.get("clustering.bin_engine", "threshold_uf")
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
    for _, axis, ns, cid in lookups:
        if pool_id:
            pipe.get(f"global:pool:{pool_id}:bin_cluster:{cid}:meta")
        else:
            pipe.get(f"{collection}:bin_cluster:{ns}:{cid}:meta")

    meta_by_uuid = {}
    # (entry_idx, axis, cid) -> uuid
    resolved = {}
    for (entry_idx, axis, ns, cid), raw in zip(lookups, pipe.execute()):
        if not raw:
            continue
        cm = json.loads(raw) if not isinstance(raw, dict) else raw
        if isinstance(cm, str):
            cm = json.loads(cm)
        if not cm:
            continue
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

    print("cluster_utils demo OK")


if __name__ == "__main__":
    demo()
