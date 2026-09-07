"""Shared helpers for function/binary hierarchical clustering."""

import hashlib
import json


def hier_fingerprint(min_sim, min_features, min_cluster_size, cohesion_cut, is_lca):
    """Config fingerprint for incremental hierarchical_uf state. A mismatch
    against the persisted fingerprint means the last full/incremental run
    used different clustering parameters -- the MST it left behind isn't
    trustworthy as a starting point, so the caller should force a full
    rebuild instead of silently incrementing onto it."""
    raw = f"{min_sim}|{min_features}|{min_cluster_size}|{cohesion_cut}|{bool(is_lca)}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def load_hier_state(r, base):
    """Load persisted incremental-hierarchical state for one `{base}` prefix.

    Returns None if any piece is missing -- first run ever, or a
    clear_clustering()/invalidation hook deleted it -- which is the signal
    for "fall back to a full rebuild" (see cluster_service.py's
    run_clustering dispatch).
    """
    nodes_raw = r.hgetall(f"{base}:nodes")
    mst_raw = r.get(f"{base}:mst")
    state_raw = r.get(f"{base}:state")
    if not nodes_raw or mst_raw is None or state_raw is None:
        return None

    def _s(v):
        return v.decode() if isinstance(v, bytes) else v

    idx = {_s(k): int(_s(v)) for k, v in nodes_raw.items()}
    mst = [tuple(e) for e in json.loads(_s(mst_raw))]
    nodeids_raw = r.hgetall(f"{base}:nodeids")
    node_ids = {_s(k): int(_s(v)) for k, v in (nodeids_raw or {}).items()}
    meta = json.loads(_s(state_raw))

    return {
        "idx": idx,
        "next_idx": meta["next_idx"],
        "mst": mst,
        "node_ids": node_ids,
        "next_node_id": meta["next_node_id"],
        "fingerprint": meta.get("fingerprint"),
        "root_id": meta.get("root_id"),
    }


def save_hier_state(r, base, state):
    """Persist incremental-hierarchical state after a rebuild -- full or
    incremental. Call this even after a full rebuild, so the next upload has
    an MST to start from instead of falling back again."""
    pipe = r.pipeline(transaction=False)
    pipe.delete(f"{base}:nodes")
    if state["idx"]:
        pipe.hset(
            f"{base}:nodes", mapping={fid: idx for fid, idx in state["idx"].items()}
        )
    pipe.set(f"{base}:mst", json.dumps(state["mst"]))
    pipe.delete(f"{base}:nodeids")
    if state["node_ids"]:
        pipe.hset(
            f"{base}:nodeids", mapping={k: v for k, v in state["node_ids"].items()}
        )
    pipe.set(
        f"{base}:state",
        json.dumps(
            {
                "next_idx": state["next_idx"],
                "next_node_id": state["next_node_id"],
                "fingerprint": state["fingerprint"],
                "root_id": state.get("root_id"),
            }
        ),
    )
    pipe.execute()


def clear_hier_state(r, base):
    """Delete all incremental-hierarchical state under `{base}` -- forces
    the next run (any engine parameters) back to a full rebuild. Called from
    clear_clustering() and from every sim-edge-deletion hook, since deleting
    an edge can invalidate the MST in a way this insert-only scheme can't
    repair (see cluster_service.py's incremental hierarchical_uf design)."""
    r.delete(f"{base}:nodes", f"{base}:mst", f"{base}:nodeids", f"{base}:state")


def edgeset_from(state, new_edges):
    """The persisted MST plus a batch of new (fid_a, fid_b, sim) edges, as
    one sim_edges.EdgeSet in the persisted index space -- ready to feed
    straight back into build_single_linkage_tree(). Fids not seen before get
    a freshly minted index (mutates state['idx']/state['next_idx'] in
    place); this is the ONLY place new leaves enter the incremental scheme.
    """
    import numpy as np

    from bsimvis.app.services.sim_edges import EdgeSet

    idx = state["idx"]

    def get_idx(fid):
        i = idx.get(fid)
        if i is None:
            i = state["next_idx"]
            state["next_idx"] += 1
            idx[fid] = i
        return i

    src, dst, dist = [], [], []
    for u, v, sim in state["mst"]:
        src.append(u)
        dst.append(v)
        dist.append(max(0.0, 1.0 - sim))
    for fid_a, fid_b, sim in new_edges:
        src.append(get_idx(fid_a))
        dst.append(get_idx(fid_b))
        dist.append(max(0.0, 1.0 - sim))

    idx_to_id = {i: f for f, i in idx.items()}
    return EdgeSet(
        src=np.array(src, dtype=np.int32),
        dst=np.array(dst, dtype=np.int32),
        dist=np.array(dist, dtype=np.float32),
        id_to_idx=idx,
        idx_to_id=idx_to_id,
        n_scanned=len(src),
    )


def stabilise(tree_rows, mst, state):
    """Remap this run's synthetic internal node ids (freshly minted every
    call to build_single_linkage_tree, by its own docstring) onto STABLE ids
    keyed on each merge's defining edge -- see cluster_service.py's
    incremental hierarchical_uf cluster-id stability contract: "a
    hierarchical cluster's identity is the MST edge that created it."
    Mutates state's id tables in place -- mints an id the first time an edge
    is seen, recycles it if that same edge later leaves and re-enters the
    MST.

    tree_rows[2*i] / tree_rows[2*i+1] are exactly the two child rows
    build_single_linkage_tree() creates for mst[i]'s merge (one pair of rows
    per accepted edge, in acceptance order -- see that function): that
    positional correspondence is what makes the remap free.

    Returns (tree_rows, stable_root_id). tree_rows is mutated in place and
    also returned for convenience.
    """
    node_ids = state["node_ids"]

    remap = {}
    for i, (u, v, _sim) in enumerate(mst):
        synth_id = tree_rows[2 * i]["parent"]
        key = f"{min(u, v)}:{max(u, v)}"
        stable_id = node_ids.get(key)
        if stable_id is None:
            stable_id = state["next_node_id"]
            state["next_node_id"] += 1
            node_ids[key] = stable_id
        remap[synth_id] = stable_id

    # The synthetic global root (stitches disconnected components together,
    # if there's more than one) has no defining edge of its own, so it gets
    # one reserved stable id, minted once and kept in state from then on.
    old_root = tree_rows[-1]["parent"] if tree_rows else None
    stable_root = state.get("root_id")
    if stable_root is None:
        stable_root = state["next_node_id"]
        state["next_node_id"] += 1
        state["root_id"] = stable_root
    if old_root is not None:
        remap[old_root] = stable_root

    for row in tree_rows:
        row["parent"] = remap.get(row["parent"], row["parent"])
        row["child"] = remap.get(row["child"], row["child"])

    return tree_rows, stable_root


def dirty_ancestors(tree_rows, dirty_leaves):
    """Every node id on the root-path of any leaf in dirty_leaves, in the
    NEW (already-stabilised) tree.

    This is a correct bound, not just a heuristic one: any merge node whose
    OWN defining edge is unchanged but whose recursive membership grew (a
    leaf now flows up through it that didn't before) still has that leaf
    among its descendants, so the leaf's root-path walk reaches it. And any
    merge whose defining edge WAS displaced has that edge's own endpoints in
    the seed set (see cluster_service.py's incremental hierarchical_uf,
    dirty_leaves = new leaves | endpoints of the old/new MST's symmetric
    difference), so the walk from there reaches whatever replaced it too.
    Bounded by O(|dirty_leaves| x tree depth) -- small unless single-linkage
    chaining makes this corpus's tree unusually deep.
    """
    child_to_parent = {row["child"]: row["parent"] for row in tree_rows}
    dirty = set()
    for leaf in dirty_leaves:
        curr = leaf
        while curr in child_to_parent:
            curr = child_to_parent[curr]
            dirty.add(curr)
    return dirty


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
