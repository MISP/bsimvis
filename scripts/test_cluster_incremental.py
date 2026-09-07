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


if __name__ == "__main__":
    test_mst_summary_matches_full_rebuild()
    test_incremental_tight_subgroup_surfaces()
    test_incremental_input_is_linear_in_nodes_and_new_edges()
    test_stable_ids_survive_unrelated_increment()
    print("incremental hierarchical_uf checks OK")
