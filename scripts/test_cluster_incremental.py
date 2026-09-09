"""Pure checks for incremental hierarchical_uf's MST summary."""

import numpy as np
import pandas as pd

from bsimvis.app.services.cluster_common import (
    edgeset_from,
    hierarchical_membership,
    stabilise,
)
from bsimvis.app.services.cluster_threshold import build_single_linkage_tree
from bsimvis.app.services.sim_edges import EdgeSet


def _edges(n, triples):
    ids = {str(i): i for i in range(n)}
    return EdgeSet(
        np.array([u for u, _, _ in triples], dtype=np.int32),
        np.array([v for _, v, _ in triples], dtype=np.int32),
        np.array([1.0 - sim for _, _, sim in triples], dtype=np.float32),
        ids,
        {i: str(i) for i in range(n)},
        len(triples),
    )


def test_mst_summary_matches_full_rebuild():
    for seed in range(200):
        rng = np.random.default_rng(seed)
        n = 12
        pairs = [(u, v) for u in range(n) for v in range(u + 1, n)]
        rng.shuffle(pairs)
        pairs = pairs[:36]
        sims = rng.permutation(np.arange(1, len(pairs) + 1)) / (len(pairs) + 1)
        edges = [(u, v, float(sim)) for (u, v), sim in zip(pairs, sims)]
        split = len(edges) // 3
        old, added = edges[:split], edges[split:]

        _, _, _, old_mst = build_single_linkage_tree(_edges(n, old))
        _, _, _, expected = build_single_linkage_tree(_edges(n, edges))
        combined = old_mst + added
        _, _, _, actual = build_single_linkage_tree(_edges(n, combined))

        assert [(u, v) for u, v, _ in actual] == [(u, v) for u, v, _ in expected]
        assert np.allclose(
            [sim for _, _, sim in actual], [sim for _, _, sim in expected]
        )


def test_incremental_tight_subgroup_surfaces():
    old = [(0, 1, 0.90), (1, 2, 0.89)]
    added = [(3, 4, 0.99), (0, 3, 0.88)]
    _, _, _, old_mst = build_single_linkage_tree(_edges(5, old))
    rows, root, n, _ = build_single_linkage_tree(_edges(5, old_mst + added))
    memberships, _ = hierarchical_membership(pd.DataFrame(rows), n, root, min_size=2)
    clusters = {}
    for leaf, chain in memberships.items():
        for cluster in chain:
            clusters.setdefault(cluster, set()).add(leaf)
    assert {3, 4} in clusters.values()


def test_incremental_input_is_linear_in_nodes_and_new_edges():
    n = 100
    dense = [
        (u, v, 1.0 - (u * n + v) / (n * n * 2))
        for u in range(n)
        for v in range(u + 1, n)
    ]
    _, _, _, mst = build_single_linkage_tree(_edges(n, dense))
    new_edges = [(0, n - 1, 0.999)] * 7
    assert len(mst) <= n - 1
    assert len(mst) + len(new_edges) <= n - 1 + len(new_edges)
    assert len(mst) + len(new_edges) < len(dense)


def test_stable_ids_survive_unrelated_increment():
    state = {
        "idx": {"a": 0, "b": 1, "c": 2},
        "next_idx": 3,
        "mst": [(0, 1, 0.9), (1, 2, 0.8)],
        "node_ids": {},
        "next_node_id": 1 << 30,
        "root_id": None,
    }
    rows, _, _, mst = build_single_linkage_tree(edgeset_from(state, []))
    stabilise(rows, mst, state)
    stable_ab = state["node_ids"]["0:1"]
    state["mst"] = mst

    rows, _, _, mst = build_single_linkage_tree(edgeset_from(state, [("c", "d", 0.95)]))
    stabilise(rows, mst, state)
    assert state["node_ids"]["0:1"] == stable_ab


def test_incremental_flag_skips_clears():
    """scripts/run_pipeline.py runs mid-batch: it must never enqueue a CLEAR_*."""
    from bsimvis.app.routes.cluster import build_rebuild_all_tasks
    from bsimvis.app.services.job_service import JobType

    types = [t for t, _ in build_rebuild_all_tasks("c", "unweighted_cosine")]
    assert JobType.CLEAR_CLUSTER in types and JobType.CLEAR_BIN_SIM in types

    types = [
        t
        for t, _ in build_rebuild_all_tasks(
            "c", "unweighted_cosine", data={"incremental": True}
        )
    ]
    assert not any(t.value.startswith("clear_") for t in types), types
    # ...and still the whole pipeline: bin sim, binary clustering, function clustering
    assert {
        JobType.BUILD_BIN_SIM,
        JobType.CLUSTER_BINARIES,
        JobType.CLUSTER_FUNCTIONS,
    } <= set(types)


class _Redis:
    """Just the commands the binary threshold path touches."""

    def __init__(self):
        self.hashes, self.sets, self.strings, self.zsets = {}, {}, {}, {}

    # hashes
    def hset(self, key, field=None, value=None, mapping=None):
        h = self.hashes.setdefault(key, {})
        if mapping:
            h.update({k: str(v) for k, v in mapping.items()})
            return len(mapping)
        h[field] = str(value)
        return 1

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hdel(self, key, *fields):
        for f in fields:
            self.hashes.get(key, {}).pop(f, None)

    # sets
    def sadd(self, key, *vals):
        self.sets.setdefault(key, set()).update(vals)

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def srem(self, key, *vals):
        self.sets.get(key, set()).difference_update(vals)

    def scard(self, key):
        return len(self.sets.get(key, set()))

    def sunionstore(self, dest, *keys):
        out = set()
        for k in keys:
            out |= self.sets.get(k, set())
        self.sets[dest] = out

    # strings / zsets / misc
    def get(self, key):
        return self.strings.get(key)

    def set(self, key, val, **kw):
        self.strings[key] = val

    def delete(self, *keys):
        for k in keys:
            self.hashes.pop(k, None)
            self.sets.pop(k, None)
            self.strings.pop(k, None)
            self.zsets.pop(k, None)

    def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)

    def zscore(self, key, member):
        return self.zsets.get(key, {}).get(member)

    def pipeline(self, transaction=False):
        return _Pipe(self)


class _Pipe:
    def __init__(self, r):
        self._r, self._q = r, []

    def __getattr__(self, name):
        def queue(*a, **k):
            self._q.append((name, a, k))
            return self

        return queue

    def __len__(self):
        return len(self._q)

    def execute(self):
        out = [getattr(self._r, n)(*a, **k) for n, a, k in self._q]
        self._q = []
        return out


def _bin_service(r):
    from bsimvis.app.services.bin_cluster_service import BinClusterService

    return BinClusterService(r)


def test_binary_incremental_reuses_the_full_rebuild_s_clusters():
    """A batch upload must extend the clusters the full rebuild left behind,
    not start a second, parallel set keyed on bare md5s."""
    from bsimvis.app.services import lineage_service

    coll, algo = "main", "unweighted_cosine"
    a, b, c = "aa" * 16, "bb" * 16, "cc" * 16
    r = _Redis()
    lineage_service.container_md5s = lambda collection, red: set()

    def fid(md5):
        return f"{coll}:file:{md5}"

    # a and b already clustered together by a full rebuild.
    svc = _bin_service(r)
    svc._enrich_and_persist_binary_clusters(
        coll,
        algo,
        {fid(a): [fid(a), fid(b)]},
        f"{coll}:bin_cluster:{algo}:uf:uuid",
        0.0,
        None,
        None,
        parent_key=f"{coll}:bin_cluster:{algo}:uf:parent",
    )
    r.sadd(f"{coll}:bin_cluster:{algo}:{fid(a)}:members", fid(a), fid(b))

    # the full rebuild seeded the union-find state the next batch reads
    parent = r.hashes[f"{coll}:bin_cluster:{algo}:uf:parent"]
    assert parent[fid(a)] == fid(a) and parent[fid(b)] == fid(a), parent

    # c arrives in a new batch, similar to b
    sid = f"{coll}:bin_sim:{algo}:{b}::{c}"
    r.sadd(f"{coll}:bin_sim:involves:{c}", sid)
    r.zadd(f"{coll}:bin_sim:score:{algo}", {sid: 0.9})

    assert svc._incremental_cluster_binaries(coll, algo, 0.1, [c], min_cohesion=0.0)

    # one cluster, not two: c joined a/b's existing root
    members = r.smembers(f"{coll}:bin_cluster:{algo}:{fid(a)}:members")
    assert members == {fid(a), fid(b), fid(c)}, members
    assert r.smembers(f"{coll}:bin_cluster:list:{algo}") == {fid(a)}

    # and every id stayed in the file-id space search resolves
    for m in members:
        assert m.startswith(f"{coll}:file:"), m
    bucket = r.smembers(f"{coll}:idx:file:bin_cluster_id:{fid(a).lower()}")
    assert fid(c) in bucket, bucket


if __name__ == "__main__":
    test_mst_summary_matches_full_rebuild()
    test_incremental_tight_subgroup_surfaces()
    test_incremental_input_is_linear_in_nodes_and_new_edges()
    test_stable_ids_survive_unrelated_increment()
    test_incremental_flag_skips_clears()
    test_binary_incremental_reuses_the_full_rebuild_s_clusters()
    print("incremental hierarchical_uf checks OK")
