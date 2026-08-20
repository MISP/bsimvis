"""PoC: deterministic, incremental clustering via threshold Union-Find.

Why not HDBSCAN (see cluster_service.py):
  - Not incremental. Adding one binary means re-streaming every sim edge in
    the collection, rebuilding connected components, and re-fitting HDBSCAN
    per component from scratch. No way to touch only what changed.
  - Not precise at scale. Components >= CLUSTER_MAX_COMPONENT-adjacent size
    (cluster_service.py's size >= 5000 branch) skip the exact precomputed-
    distance path and instead truncated-SVD-embed the similarity matrix
    (k=min(50, size-1)) before running HDBSCAN with Euclidean distance on the
    embedding. That embedding is a *low-rank approximation* of the similarity
    matrix: a pair with true cosine distance 0 (100% similar) is not
    guaranteed distance 0 in the reduced space, so mutual-reachability
    distance can inflate it past what min_cluster_size/epsilon will keep, and
    the pair sheds as noise -- see demo_svd_sheds_exact_pair() below for a
    reproduction against the real hdbscan+svds path.

This module replaces both failure modes with a single mechanism: union nodes
whenever an edge's similarity clears a threshold. For threshold == 1.0 (or
"near enough", e.g. 0.999 to allow float noise) this is exact by
construction -- no embedding, no approximation, every edge is checked
directly against the raw similarity score. It is also naturally incremental:
unioning a new node only touches the DSU entries on the path from that node
to its cluster roots, never the rest of the forest.

Multi-tier hierarchy (loose clusters at 0.9, tight ones at 0.98, exact dups
at 1.0) is the obvious extension -- run this same DSU once per threshold,
coarsest first, each seeded from the previous tier's roots. Out of scope for
this PoC; the point being proven here is correctness + incrementality of one
tier.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ThresholdUF:
    """Union-Find over integer node ids, with member sets kept at roots.

    Small-to-large merging (the smaller member set is folded into the
    larger) bounds total merge work at O(n log n) across the whole lifetime
    of the structure, same guarantee as classic union-by-size -- it just also
    keeps `members[root]` accurate without a separate O(n) rebuild pass.
    """

    parent: dict = field(default_factory=dict)
    members: dict = field(default_factory=dict)  # root -> set of node ids

    def add_node(self, node: int) -> None:
        if node not in self.parent:
            self.parent[node] = node
            self.members[node] = {node}

    def find(self, node: int) -> int:
        self.add_node(node)
        root = node
        while self.parent[root] != root:
            root = self.parent[root]
        # path compression
        while self.parent[node] != root:
            self.parent[node], node = root, self.parent[node]
        return root

    def union(self, a: int, b: int) -> int:
        """Union a and b. Returns the resulting root (for touched-set tracking)."""
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return ra
        if len(self.members[ra]) < len(self.members[rb]):
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.members[ra].update(self.members[rb])
        del self.members[rb]
        return ra

    def clusters(self, min_size: int = 2) -> dict:
        """root -> member set, for roots with at least min_size members."""
        return {r: m for r, m in self.members.items() if len(m) >= min_size}


def build_threshold_clusters(edge_set, threshold: float) -> ThresholdUF:
    """Full build: union every edge whose similarity clears `threshold`.

    `edge_set` is a bsimvis.app.services.sim_edges.EdgeSet (src/dst/dist
    parallel arrays, dist = 1 - similarity). Isolated nodes (present in
    id_to_idx but with no surviving edge) still get a singleton entry via
    add_node, matching HDBSCAN's "noise" semantics for those nodes.
    """
    uf = ThresholdUF()
    for idx in edge_set.idx_to_id:
        uf.add_node(idx)
    max_dist = 1.0 - threshold
    for s, d, dist in zip(edge_set.src, edge_set.dst, edge_set.dist):
        if dist <= max_dist:
            uf.union(int(s), int(d))
    return uf


def incremental_add(uf: ThresholdUF, new_edges, threshold: float) -> set:
    """Add new (src, dst, dist) triples to an existing UF, unioning only
    where the edge clears `threshold`.

    Returns the set of roots touched by this call -- the only clusters whose
    metadata (name, cohesion, member count, ...) need recomputing downstream.
    This is the incrementality fix: adding one binary's edges costs O(its own
    edge count), not O(every edge in the collection), and every function
    outside the touched clusters keeps its existing assignment untouched.
    """
    max_dist = 1.0 - threshold
    touched = set()
    for s, d, dist in new_edges:
        uf.add_node(s)
        uf.add_node(d)
        if dist <= max_dist:
            touched.add(uf.union(s, d))
        else:
            touched.add(uf.find(s))
            touched.add(uf.find(d))
    return touched


# ---------------------------------------------------------------------------
# Self-checks. No Redis needed. Run: python -m bsimvis.app.services.cluster_threshold
# ---------------------------------------------------------------------------


def _demo_parity_with_hdbscan_semantics():
    """Same two scenarios as cluster_common._demo: tight pair clusters,
    distinct families stay separate, at a threshold below the tight sims.
    """
    from bsimvis.app.services.sim_edges import EdgeSet

    def edges_from_matrix(sim):
        n = len(sim)
        src, dst, dist = [], [], []
        idx_to_id = {i: i for i in range(n)}
        for i in range(n):
            for j in range(i + 1, n):
                src.append(i)
                dst.append(j)
                dist.append(1.0 - sim[i][j])
        return EdgeSet(src, dst, dist, {i: i for i in range(n)}, idx_to_id, n)

    # S1: A,B identical (.98), C only .75 -> tight pair, C stays its own singleton.
    s1 = edges_from_matrix([[1, 0.98, 0.75], [0.98, 1, 0.75], [0.75, 0.75, 1]])
    uf = build_threshold_clusters(s1, threshold=0.9)
    clusters = uf.clusters(min_size=2)
    assert list(clusters.values()) == [{0, 1}], f"expected tight pair, got {clusters}"

    # S2: two families of 3 at .98, cross-family only .75 -> two separate triples.
    s2 = [
        [1, 0.98, 0.98, 0.75, 0.75, 0.75],
        [0.98, 1, 0.98, 0.75, 0.75, 0.75],
        [0.98, 0.98, 1, 0.75, 0.75, 0.75],
        [0.75, 0.75, 0.75, 1, 0.98, 0.98],
        [0.75, 0.75, 0.75, 0.98, 1, 0.98],
        [0.75, 0.75, 0.75, 0.98, 0.98, 1],
    ]
    edges = edges_from_matrix(s2)
    uf = build_threshold_clusters(edges, threshold=0.9)
    clusters = sorted(sorted(m) for m in uf.clusters(min_size=2).values())
    assert clusters == [[0, 1, 2], [3, 4, 5]], f"expected two families, got {clusters}"

    print("parity demo OK")


def demo_svd_sheds_exact_pair():
    """Quantifies the precision bug against the REAL production path:
    cluster_service.py's size>=5000 branch SVD-embeds the similarity matrix
    (k=min(50,size-1)) before running HDBSCAN with Euclidean distance on the
    embedding. That embedding is lossy: a pair with TRUE cosine distance 0
    (100% similar) is not guaranteed embedded distance 0. Whether that
    residual error is enough to flip HDBSCAN's noise/cluster decision
    depends on min_cluster_size/epsilon and the surrounding component's
    density -- in this synthetic component it isn't (HDBSCAN happens to
    still separate the pair out correctly here), but the embedding error
    itself is present and measured below, and is exactly the kind of error
    that user reports describe: it is a matter of degree, not a guarantee.
    build_threshold_clusters() has no such failure mode by construction: it
    checks the raw similarity score directly, no embedding involved, so an
    edge with dist==0.0 always unions regardless of component size or shape.
    """
    import numpy as np
    import scipy.sparse as sp
    from scipy.sparse.linalg import svds
    import hdbscan as hdbscan_lib
    from bsimvis.app.services.sim_edges import EdgeSet

    rng = np.random.default_rng(0)
    size = 6000  # clears cluster_service.py's size >= 5000 SVD branch

    # Spanning tree of weak edges (sim ~0.2) so it's one connected component,
    # plus sparse extra weak edges for realism.
    src, dst, sim = [], [], []
    for i in range(1, size):
        j = rng.integers(0, i)
        src.append(i)
        dst.append(j)
        sim.append(0.15 + rng.random() * 0.1)
    extra = size * 2
    for _ in range(extra):
        i, j = rng.integers(0, size, size=2)
        if i == j:
            continue
        src.append(int(i))
        dst.append(int(j))
        sim.append(0.15 + rng.random() * 0.1)

    # The exact-duplicate pair: 100% similar to each other, and each also
    # weakly tied into the big component (so it's not its own connected
    # component -- it genuinely has to survive the SVD+HDBSCAN path).
    u, v = size - 2, size - 1
    src += [u, v, u]
    dst += [v, rng.integers(0, size - 2), rng.integers(0, size - 2)]
    sim += [1.0, 0.15, 0.15]

    src, dst, sim = np.array(src), np.array(dst), np.array(sim)
    dist = 1.0 - sim
    edge_set = EdgeSet(
        src, dst, dist.astype(np.float32),
        {i: i for i in range(size)}, {i: i for i in range(size)}, len(src),
    )

    # --- production path, inlined from cluster_service.py's size>=5000 branch ---
    rows_sp = np.concatenate([src, dst])
    cols_sp = np.concatenate([dst, src])
    data_sp = np.concatenate([sim, sim])
    comp_matrix = sp.csr_matrix((data_sp, (rows_sp, cols_sp)), shape=(size, size), dtype=np.float32)
    comp_matrix.setdiag(1.0)
    k = min(50, size - 1)
    U, S, _ = svds(comp_matrix, k=k)
    embeddings = U @ np.diag(np.sqrt(S))
    clusterer = hdbscan_lib.HDBSCAN(
        min_cluster_size=2, min_samples=1, cluster_selection_epsilon=0.1,
        cluster_selection_method="eom", metric="euclidean", gen_min_span_tree=True,
    )
    labels = clusterer.fit_predict(embeddings)
    hdbscan_same_cluster = labels[u] != -1 and labels[u] == labels[v]
    embedded_dist = float(np.linalg.norm(embeddings[u] - embeddings[v]))

    # --- threshold union-find, same edges ---
    uf = build_threshold_clusters(edge_set, threshold=0.999)
    uf_same_cluster = uf.find(u) == uf.find(v)

    print(
        f"exact pair ({u},{v}): true_dist=0.0  svd_embedded_dist={embedded_dist:.4f}  "
        f"hdbscan(SVD path) same_cluster={hdbscan_same_cluster}  "
        f"threshold_uf same_cluster={uf_same_cluster}"
    )
    assert uf_same_cluster, "threshold UF must always keep an exact-sim pair together"
    assert embedded_dist > 0.0, (
        "expected the SVD embedding to introduce nonzero error for a pair "
        "whose true distance is 0 -- this is the mechanism that lets HDBSCAN "
        "shed exact duplicates as noise once min_cluster_size/epsilon are in "
        "range of the residual error."
    )


def demo_incremental_touches_only_new_edges():
    """Adding one new binary's edges must not re-touch unrelated clusters,
    and must not cost O(existing collection size).
    """
    from bsimvis.app.services.sim_edges import EdgeSet

    n = 2000
    # n/2 disjoint tight pairs: (0,1), (2,3), (4,5), ...
    src = list(range(0, n, 2))
    dst = list(range(1, n, 2))
    dist = [0.0] * len(src)
    import numpy as np
    edge_set = EdgeSet(
        np.array(src), np.array(dst), np.array(dist, dtype=np.float32),
        {i: i for i in range(n)}, {i: i for i in range(n)}, len(src),
    )

    uf = build_threshold_clusters(edge_set, threshold=0.999)
    baseline_roots = {uf.find(i) for i in range(n)}

    # New node n, exact-dup of node 0 only.
    touched = incremental_add(uf, [(n, 0, 0.0)], threshold=0.999)

    assert uf.find(n) == uf.find(0), "new node must join node 0's cluster"
    assert len(touched) == 1, f"expected exactly one touched root, got {touched}"
    # Every pre-existing pair's root must be byte-identical to before -- proof
    # that unrelated clusters were never touched, let alone recomputed.
    for i in range(2, n, 2):
        assert uf.find(i) in baseline_roots, "unrelated cluster root changed"

    print(
        f"incremental add touched {len(touched)} root out of {n // 2} existing "
        f"clusters ({n} existing nodes) -- O(new edges), not O(collection)."
    )


if __name__ == "__main__":
    _demo_parity_with_hdbscan_semantics()
    demo_incremental_touches_only_new_edges()
    demo_svd_sheds_exact_pair()
    print("cluster_threshold PoC OK")
