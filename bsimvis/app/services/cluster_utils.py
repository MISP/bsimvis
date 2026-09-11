"""Small cluster-selection helpers shared across bin_sim / similarity / cluster services.

Dedupes the "pick the highest-cohesion cluster" logic that was previously copied into
bin_sim_service, similarity_service and cluster_service.
"""

import json
from collections import Counter

MAX_CLUSTER_NAME_LEN = 40


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
