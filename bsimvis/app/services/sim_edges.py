"""Turning a similarity ZSET into an edge list, without holding it twice.

Every clustering entry point (function clustering for a collection, the same for
a pool via delegation, and the two binary variants) used to do this:

    pairs = []                       # every member string, ~180 chars each
    for sid, score in zscan(...):
        pairs.append((sid, score))
    for sid, score in pairs:
        edges.append((i, j, dist))   # every edge again, as a Python tuple

Measured on a real 5.4M-pair pool, that costs 2.15 GiB before any clustering
happens -- 1.58 GiB of it just `pairs`, because each member embeds the full
`global:pool:<uuid>:sim:` prefix and Python strings carry ~49 bytes of overhead
on top. Two more copies followed (rows/cols/data, then comp_to_edges) for a
2.75 GiB peak, against a 3 GB per-worker cap.

So: stream the ZSET and never materialise the member strings, and accumulate
into stdlib `array` buffers -- 12 bytes per edge instead of roughly 120, typed,
contiguous, and free to reinterpret as NumPy without copying.

Peak here is a small constant times the edge COUNT, with no term for the length
of the id strings, which is what made the old version scale so badly.
"""

import json
from array import array
from collections import namedtuple

import numpy as np

# src/dst are node indices, dist is 1 - score. Parallel arrays, one entry each.
EdgeSet = namedtuple("EdgeSet", "src dst dist id_to_idx idx_to_id n_scanned")


def _iter_pairs(r, sim_score_key, count=1000):
    """Yields (member, score) straight off the ZSET, holding one page at a time."""
    cursor = 0
    while True:
        cursor, results = r.zscan(sim_score_key, cursor=cursor, count=count)
        for sid, score in results:
            yield (sid.decode() if isinstance(sid, bytes) else sid), score
        if cursor == 0:
            break


def _split_ids(sid, prefix, is_pool, collection, node_kind, id_fn=None):
    """Member -> (fid1, fid2), or None if it does not belong to this key.

    `id_fn` covers callers whose halves need a different transform -- pool
    binary similarity stores `<coll>:<md5>` on each side and rebuilds it as
    `<coll>:file:<md5>`.
    """
    if not sid.startswith(prefix):
        return None
    ids_part = sid[len(prefix) :]
    if "::" not in ids_part:
        return None
    parts = ids_part.split("::")
    if len(parts) != 2:
        return None
    c1, c2 = parts
    if id_fn is not None:
        return id_fn(c1, c2)
    if is_pool:
        return c1, c2
    return f"{collection}:{node_kind}:{c1}", f"{collection}:{node_kind}:{c2}"


def collect_allowed_fids(
    r,
    sim_score_key,
    prefix,
    is_pool,
    collection,
    min_features,
    node_kind="func",
    id_fn=None,
):
    """Ids whose bsim_features_count clears min_features.

    A second streaming pass over the ZSET rather than a retained copy of it:
    trading I/O for the 1.58 GiB the retained copy cost.
    """
    unique_fids = set()
    for sid, _ in _iter_pairs(r, sim_score_key):
        ids = _split_ids(sid, prefix, is_pool, collection, node_kind, id_fn)
        if ids:
            unique_fids.add(ids[0])
            unique_fids.add(ids[1])

    allowed = set()
    fids_list = list(unique_fids)
    del unique_fids
    # Chunked so the reply buffer is bounded too; a single pipeline over every
    # id would just move the allocation from one place to another.
    CHUNK = 5000
    for start in range(0, len(fids_list), CHUNK):
        chunk = fids_list[start : start + CHUNK]
        pipe = r.pipeline(transaction=False)
        for fid in chunk:
            pipe.get(f"{fid}:meta")
        for fid, res in zip(chunk, pipe.execute()):
            try:
                val = 0
                if res:
                    val = json.loads(res).get("bsim_features_count", 0)
                if int(val) >= min_features:
                    allowed.add(fid)
            except (ValueError, TypeError, IndexError):
                continue
    return allowed


def load_edges(
    r,
    sim_score_key,
    prefix,
    is_pool,
    collection,
    min_sim=0.0,
    allowed_fids=None,
    node_kind="func",
    id_fn=None,
):
    """Streams the ZSET into typed edge arrays.

    Semantics preserved exactly from the original inline version, including the
    subtle one: a node is registered in id_to_idx even when its edge is then
    dropped by min_sim, so an isolated node still exists in the graph.
    """
    src, dst = array("i"), array("i")
    dist = array("f")
    id_to_idx, idx_to_id = {}, {}
    n_scanned = 0

    for sid, score in _iter_pairs(r, sim_score_key):
        n_scanned += 1
        ids = _split_ids(sid, prefix, is_pool, collection, node_kind, id_fn)
        if not ids:
            continue
        fid1, fid2 = ids

        if allowed_fids is not None and (
            fid1 not in allowed_fids or fid2 not in allowed_fids
        ):
            continue

        for fid in (fid1, fid2):
            if fid not in id_to_idx:
                idx = len(id_to_idx)
                id_to_idx[fid] = idx
                idx_to_id[idx] = fid

        score_val = float(score)
        if min_sim > 0 and score_val < min_sim:
            continue

        src.append(id_to_idx[fid1])
        dst.append(id_to_idx[fid2])
        dist.append(max(0.0, 1.0 - score_val))

    return EdgeSet(
        src=np.frombuffer(src, dtype=np.int32),
        dst=np.frombuffer(dst, dtype=np.int32),
        dist=np.frombuffer(dist, dtype=np.float32),
        id_to_idx=id_to_idx,
        idx_to_id=idx_to_id,
        n_scanned=n_scanned,
    )


def group_edges_by_component(edges, labels):
    """comp_id -> (src, dst, dist) views, for intra-component edges only.

    Replaces building a dict of Python tuple lists, which was a third full copy
    of the edge set. One argsort and a set of slices: the returned arrays are
    views into the sorted permutation, so no per-edge objects exist at all.
    """
    if edges.src.size == 0:
        return {}

    comp_of_src = labels[edges.src]
    intra = comp_of_src == labels[edges.dst]
    if not intra.any():
        return {}

    comp = comp_of_src[intra]
    s, d, w = edges.src[intra], edges.dst[intra], edges.dist[intra]

    order = np.argsort(comp, kind="stable")
    comp, s, d, w = comp[order], s[order], d[order], w[order]

    uniq, starts = np.unique(comp, return_index=True)
    bounds = list(starts) + [comp.size]
    return {
        int(c): (s[bounds[k] : bounds[k + 1]], d[bounds[k] : bounds[k + 1]], w[bounds[k] : bounds[k + 1]])
        for k, c in enumerate(uniq)
    }


def build_adjacency(edges, num_nodes):
    """Symmetric 0/1 adjacency for connected-components, built without Python lists."""
    import scipy.sparse as sp

    mask = edges.dist < 1.0
    s, d = edges.src[mask], edges.dst[mask]
    rows = np.concatenate([s, d])
    cols = np.concatenate([d, s])
    data = np.ones(rows.size, dtype=np.int8)
    return sp.csr_matrix((data, (rows, cols)), shape=(num_nodes, num_nodes))
