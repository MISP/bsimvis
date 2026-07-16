"""Shared helpers for function/binary hierarchical clustering."""


def _adjacency(edges):
    """edges: list of (i, j, dist). Returns {node: {nbr: sim}} with sim = 1 - dist."""
    adj = {}
    for i, j, d in edges:
        s = 1.0 - d
        adj.setdefault(i, {})[j] = s
        adj.setdefault(j, {})[i] = s
    return adj


def _cohesion(members, adj):
    """Average pairwise similarity over all member pairs (missing pair = 0)."""
    m = list(members)
    k = len(m)
    if k < 2:
        return 1.0
    tot = 0.0
    for a in range(k):
        nb = adj.get(m[a], {})
        for b in range(a + 1, k):
            tot += nb.get(m[b], 0.0)
    return tot / (k * (k - 1) / 2.0)


def build_leiden_tree(
    edges,
    num_nodes,
    resolution=1.0,
    stop_cohesion=0.9,
    min_size=2,
    job_service=None,
    job_id=None,
):
    """Recursive-Leiden hierarchy in the same condensed-tree shape HDBSCAN emits.

    Each connected component is clustered with Leiden, then each community is
    recursively re-clustered until it is either tight enough (cohesion >=
    ``stop_cohesion``), too small, or can no longer be split. This yields a
    strict nested tree: coarse low-cohesion parents on top (dendrogram insight),
    tight high-cohesion groups at the leaves (the sub-groups HDBSCAN's
    single-linkage buries). Points that never join a >=2 group stay direct
    members of their parent (i.e. not force-split into arbitrary pairs).

    Lambda is depth-based (child edge lambda = parent_depth + 1), which makes
    every leaf survive to all of its ancestors under
    ``hierarchical_membership`` — Leiden hierarchies have no shed-noise, so the
    membership rule keeps the full ancestor chain.

    Returns (tree_df, global_root_id) — a pandas DataFrame with columns
    parent, child, lambda_val, child_size, ready to flow through the same
    persistence path as the HDBSCAN tree.
    """
    import igraph as ig
    import leidenalg as la
    import pandas as pd

    adj = _adjacency(edges)
    global_root_id = num_nodes
    next_id = [num_nodes + 1]
    rows = []

    def leiden_split(members):
        idx = {g: i for i, g in enumerate(members)}
        es, w = [], []
        for a in members:
            for b, s in adj.get(a, {}).items():
                if b in idx and idx[a] < idx[b]:
                    es.append((idx[a], idx[b]))
                    w.append(s)
        g = ig.Graph(n=len(members), edges=es)
        g.es["weight"] = w
        part = la.RBConfigurationVertexPartition(
            g, weights="weight", resolution_parameter=resolution
        )
        la.Optimiser().optimise_partition(part)
        return [[members[i] for i in comm] for comm in part]

    def rec(members, depth):
        """Return (node_or_leaf_id, subtree_leaf_count); append rows for its children."""
        if len(members) == 1:
            return members[0], 1  # bare leaf point; parent creates the edge row
        node_id = next_id[0]
        next_id[0] += 1
        subs = None
        if len(members) >= max(2, min_size) and _cohesion(members, adj) < stop_cohesion:
            parts = [p for p in leiden_split(members) if p]
            if len(parts) >= 2:
                subs = parts
        lam = float(depth + 1)
        if subs is None:
            # leaf cluster: attach every member point directly
            for leaf in members:
                rows.append(
                    {
                        "parent": node_id,
                        "child": leaf,
                        "lambda_val": lam,
                        "child_size": 1,
                    }
                )
            return node_id, len(members)
        total = 0
        for part in subs:
            cid, sz = rec(part, depth + 1)
            rows.append(
                {"parent": node_id, "child": cid, "lambda_val": lam, "child_size": sz}
            )
            total += sz
        return node_id, total

    # connected components via igraph
    all_edges = [(i, j) for i, j, d in edges if d < 1.0]
    g_full = ig.Graph(n=num_nodes, edges=all_edges)
    comps = g_full.connected_components()

    for comp in comps:
        comp = list(comp)
        cid, sz = rec(comp, 0)
        if sz >= 1 and cid != global_root_id:
            rows.append(
                {
                    "parent": global_root_id,
                    "child": cid,
                    "lambda_val": 0.0,
                    "child_size": sz,
                }
            )

    if job_service and job_id:
        job_service.add_log(
            job_id,
            f"Recursive Leiden built {len(rows)} tree rows "
            f"(resolution={resolution}, stop_cohesion={stop_cohesion}).",
        )
    return pd.DataFrame(rows), global_root_id


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
    for _, row in tree_df.iterrows():
        p = int(row["parent"])
        l = float(row["lambda_val"])
        if l > death.get(p, 0.0):
            death[p] = l
        if int(row["child_size"]) == 1:
            leaf_fall[int(row["child"])] = l

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


def _demo_leiden():
    """Recursive-Leiden builder: keeps low-cohesion parent, surfaces tight children."""
    import numpy as np

    def edges_from_sim(sim, thresh=0.7):
        e = []
        n = len(sim)
        for i in range(n):
            for j in range(i + 1, n):
                s = sim[i][j]
                if s >= thresh:  # mimic min_score store threshold
                    e.append((i, j, 1.0 - s))
        return e, n

    def members_of(tree_df, n, groot):
        l2c, home = hierarchical_membership(tree_df, n, groot)
        mem = {}
        for leaf, cs in l2c.items():
            for cid in cs:
                mem.setdefault(cid, set()).add(leaf)
        return mem

    # Two tight triangles joined by one weak bridge edge. The whole thing is a
    # low-cohesion parent; each triangle is a tight child that must be surfaced.
    s2 = [
        [1, 0.98, 0.98, 0.00, 0.00, 0.00],
        [0.98, 1, 0.98, 0.00, 0.00, 0.00],
        [0.98, 0.98, 1, 0.75, 0.00, 0.00],
        [0.00, 0.00, 0.75, 1, 0.98, 0.98],
        [0.00, 0.00, 0.00, 0.98, 1, 0.98],
        [0.00, 0.00, 0.00, 0.98, 0.98, 1],
    ]
    e, n = edges_from_sim(s2)
    tdf, groot = build_leiden_tree(e, n, resolution=1.0, stop_cohesion=0.9, min_size=2)
    mem = members_of(tdf, n, groot)
    sets = sorted(sorted(v) for v in mem.values())
    assert [0, 1, 2] in sets and [3, 4, 5] in sets, f"tight children missing: {sets}"
    assert [0, 1, 2, 3, 4, 5] in sets, f"parent level missing (dendrogram): {sets}"

    # Already-tight quad: must NOT over-split -> one cluster only.
    s1 = [[1, 0.98, 0.97, 0.98]] * 0 + [
        [1, 0.98, 0.97, 0.98],
        [0.98, 1, 0.98, 0.97],
        [0.97, 0.98, 1, 0.98],
        [0.98, 0.97, 0.98, 1],
    ]
    e, n = edges_from_sim(s1)
    tdf, groot = build_leiden_tree(e, n, resolution=1.0, stop_cohesion=0.9, min_size=2)
    mem = members_of(tdf, n, groot)
    sets = sorted(sorted(v) for v in mem.values())
    assert sets == [[0, 1, 2, 3]], f"tight quad should stay one cluster, got {sets}"

    print("build_leiden_tree demo OK")


if __name__ == "__main__":
    _demo()
    _demo_leiden()
