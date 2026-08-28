"""Shared helpers for function/binary hierarchical clustering."""


def hierarchical_membership(tree_df, num_nodes, global_root_id, min_size=2):
    """Map leaves to the condensed-tree clusters they actually belong to.

    A leaf belongs to an ancestor cluster C only if it *survives* down to C's
    death lambda (``fallout(leaf) >= death(C)``). Points that shed as singleton
    noise before C dissolves — e.g. a loosely-attached binary peeling off a tight
    pair — are NOT members of C. Without this rule the shed points dilute the
    tight core, so a 100%-similar pair shows up as one low-cohesion cluster of 3
    instead of a precise pair.

    The synthetic global root (which only stitches unrelated connected components
    together) is never treated as a cluster.

    Args:
        tree_df: pandas DataFrame with columns parent, child, lambda_val, child_size
                 (the final, post-prune condensed tree).
        num_nodes: number of leaf nodes (leaves are ids 0..num_nodes-1).
        global_root_id: id of the synthetic global root to exclude.

    Returns:
        (leaf_to_clusters, leaf_home)
          leaf_to_clusters: {leaf: [cluster_id, ...]}  (all surviving ancestors)
          leaf_home:        {leaf: deepest_cluster_id}  (its own cluster; absent if noise)
    """
    child_to_parent = dict(zip(tree_df["child"], tree_df["parent"]))

    death = {}
    leaf_fall = {}
    for row in tree_df.itertuples(index=False):
        p = int(row.parent)
        l = float(row.lambda_val)
        if l > death.get(p, 0.0):
            death[p] = l
        if int(row.child_size) == 1:
            leaf_fall[int(row.child)] = l

    # First pass: surviving ancestors per leaf, ordered deepest-first.
    survived = {}
    counts = {}
    for leaf in range(num_nodes):
        chain = []
        curr = leaf
        fall = leaf_fall.get(leaf, float("inf"))
        while curr in child_to_parent:
            p = int(child_to_parent[curr])
            if p != global_root_id and fall >= death.get(p, 0.0):
                chain.append(p)
                counts[p] = counts.get(p, 0) + 1
            curr = p
        survived[leaf] = chain

    # Drop nodes that ended up with fewer than min_size survivors — a "cluster"
    # of one is just noise once its neighbours shed.
    valid = {c for c, n in counts.items() if n >= min_size}

    leaf_to_clusters = {}
    leaf_home = {}
    for leaf, chain in survived.items():
        kept = [c for c in chain if c in valid]  # deepest-first
        leaf_to_clusters[leaf] = kept
        if kept:
            leaf_home[leaf] = kept[0]

    return leaf_to_clusters, leaf_home


def _demo():
    """Self-check against real HDBSCAN condensed trees (no Redis needed)."""
    import numpy as np
    import hdbscan
    import pandas as pd

    def run(sim):
        d = 1.0 - np.array(sim)
        c = hdbscan.HDBSCAN(
            min_cluster_size=2,
            min_samples=1,
            metric="precomputed",
            gen_min_span_tree=True,
            allow_single_cluster=True,
        )
        c.fit(d.astype(np.float64))
        t = c.condensed_tree_.to_pandas()
        n = len(sim)
        # No synthetic root in this single-component test; pass a sentinel id.
        l2c, home = hierarchical_membership(t, n, global_root_id=-999)
        members = {}
        for leaf, cs in l2c.items():
            for cid in cs:
                members.setdefault(cid, set()).add(leaf)
        return members, home

    # S1: A,B identical (.98), C only .75 -> tight pair {0,1}, C shed as noise.
    m, home = run([[1, 0.98, 0.75], [0.98, 1, 0.75], [0.75, 0.75, 1]])
    sets = sorted(sorted(v) for v in m.values())
    assert sets == [[0, 1]], f"S1 expected one tight pair, got {sets}"
    assert 2 not in home, f"C should be noise, home={home}"

    # S2: two genuine families -> full hierarchy preserved (parent + two children).
    s2 = [
        [1, 0.98, 0.98, 0.75, 0.75, 0.75],
        [0.98, 1, 0.98, 0.75, 0.75, 0.75],
        [0.98, 0.98, 1, 0.75, 0.75, 0.75],
        [0.75, 0.75, 0.75, 1, 0.98, 0.98],
        [0.75, 0.75, 0.75, 0.98, 1, 0.98],
        [0.75, 0.75, 0.75, 0.98, 0.98, 1],
    ]
    m, home = run(s2)
    sets = sorted(sorted(v) for v in m.values())
    assert [0, 1, 2] in sets and [3, 4, 5] in sets, f"missing tight families: {sets}"
    assert [0, 1, 2, 3, 4, 5] in sets, f"missing parent level: {sets}"

    print("hierarchical_membership demo OK")


if __name__ == "__main__":
    _demo()
