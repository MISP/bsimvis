"""Small cluster-selection helpers shared across bin_sim / similarity / cluster services.

Dedupes the "pick the highest-cohesion cluster" logic that was previously copied into
bin_sim_service, similarity_service and cluster_service.
"""


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
