"""Streaming edge loading must match the inline version it replaced.

All four clustering entry points used to build a `pairs` list of every ZSET
member and then a second list of edge tuples. Measured on a real 5.4M-pair pool
that peaked at 2.75 GiB against a 3 GB per-worker cap, 1.58 GiB of it just the
member strings.

The replacement is only worth anything if it produces the SAME graph, so these
tests pin behaviour, not memory: identical edges, identical node numbering, and
the subtle rule that a node dropped by min_sim still exists in the graph.

Run: uv run python test_sim_edges.py
"""

import numpy as np

from bsimvis.app.services import sim_edges


class FakeZRedis:
    """Serves a ZSET through zscan in small pages, like the real thing."""

    def __init__(self, members, page=3):
        self.members = list(members)
        self.page = page
        self.pages_served = 0

    def zscan(self, key, cursor=0, count=10):
        self.pages_served += 1
        chunk = self.members[cursor : cursor + self.page]
        nxt = cursor + self.page
        if nxt >= len(self.members):
            nxt = 0
        return nxt, chunk


PREFIX = "coll:sim:algo:"


def members(pairs):
    return [(f"{PREFIX}{a}::{b}", s) for a, b, s in pairs]


# --------------------------------------------------------------------------
# parity with the old inline logic
# --------------------------------------------------------------------------


def reference_impl(rows, prefix, is_pool, collection, min_sim=0.0, node_kind="func"):
    """The original inline algorithm, kept here as the oracle."""
    id_to_idx, idx_to_id, edges = {}, {}, []
    for sid, score in rows:
        if not sid.startswith(prefix):
            continue
        ids_part = sid[len(prefix) :]
        if "::" not in ids_part:
            continue
        c1, c2 = ids_part.split("::")
        if is_pool:
            fid1, fid2 = c1, c2
        else:
            fid1 = f"{collection}:{node_kind}:{c1}"
            fid2 = f"{collection}:{node_kind}:{c2}"
        for fid in (fid1, fid2):
            if fid not in id_to_idx:
                idx = len(id_to_idx)
                id_to_idx[fid] = idx
                idx_to_id[idx] = fid
        score_val = float(score)
        if min_sim > 0 and score_val < min_sim:
            continue
        edges.append((id_to_idx[fid1], id_to_idx[fid2], max(0.0, 1.0 - score_val)))
    return id_to_idx, edges


PAIRS = [
    ("a", "b", 0.9),
    ("b", "c", 0.5),
    ("c", "d", 0.2),
    ("d", "e", 0.95),
    ("e", "a", 0.1),
    ("f", "g", 0.8),
]


def test_matches_the_old_implementation():
    rows = members(PAIRS)
    r = FakeZRedis(rows)

    es = sim_edges.load_edges(r, "k", PREFIX, False, "coll")
    ref_ids, ref_edges = reference_impl(rows, PREFIX, False, "coll")

    assert es.id_to_idx == ref_ids, "node numbering diverged"
    got = list(zip(es.src.tolist(), es.dst.tolist()))
    assert got == [(i, j) for i, j, _ in ref_edges]
    np.testing.assert_allclose(
        es.dist, [d for _, _, d in ref_edges], rtol=1e-6
    )


def test_min_sim_still_registers_the_node():
    """The subtle rule: a node exists even when its only edge is filtered out."""
    rows = members(PAIRS)
    es = sim_edges.load_edges(
        FakeZRedis(rows), "k", PREFIX, False, "coll", min_sim=0.9
    )
    ref_ids, ref_edges = reference_impl(rows, PREFIX, False, "coll", min_sim=0.9)

    assert es.id_to_idx == ref_ids
    assert es.src.size == len(ref_edges) == 2  # only 0.9 and 0.95 survive
    # ...but every node from every scanned pair is still present
    assert len(es.id_to_idx) == 7


def test_pool_ids_are_used_verbatim():
    rows = members(PAIRS)
    es = sim_edges.load_edges(FakeZRedis(rows), "k", PREFIX, True, None)
    assert "a" in es.id_to_idx and "coll:func:a" not in es.id_to_idx


def test_custom_id_fn():
    rows = [(f"{PREFIX}collA:md5one::collB:md5two", 0.7)]
    def id_fn(c1, c2):
        p1, p2 = c1.split(":"), c2.split(":")
        return f"{p1[0]}:file:{p1[1]}", f"{p2[0]}:file:{p2[1]}"

    es = sim_edges.load_edges(FakeZRedis(rows), "k", PREFIX, True, None, id_fn=id_fn)
    assert set(es.id_to_idx) == {"collA:file:md5one", "collB:file:md5two"}


def test_foreign_and_malformed_members_are_skipped():
    rows = [
        ("other:sim:algo:x::y", 0.9),  # wrong prefix
        (f"{PREFIX}no-separator", 0.9),
        (f"{PREFIX}a::b::c", 0.9),  # three halves
        (f"{PREFIX}a::b", 0.9),  # the only good one
    ]
    es = sim_edges.load_edges(FakeZRedis(rows), "k", PREFIX, True, None)
    assert es.src.size == 1
    assert es.n_scanned == 4, "n_scanned counts everything seen, good or not"


def test_empty_zset():
    es = sim_edges.load_edges(FakeZRedis([]), "k", PREFIX, True, None)
    assert es.src.size == 0 and es.n_scanned == 0
    assert sim_edges.group_edges_by_component(es, np.array([])) == {}


# --------------------------------------------------------------------------
# grouping / adjacency
# --------------------------------------------------------------------------


def test_grouping_keeps_only_intra_component_edges():
    es = sim_edges.load_edges(FakeZRedis(members(PAIRS)), "k", PREFIX, True, None)
    # a,b,c,d,e in component 0; f,g in component 1
    labels = np.array([0, 0, 0, 0, 0, 1, 1])

    groups = sim_edges.group_edges_by_component(es, labels)

    assert set(groups) == {0, 1}
    assert groups[0][0].size == 5
    assert groups[1][0].size == 1
    total = sum(g[0].size for g in groups.values())
    assert total == es.src.size, "no edge may be dropped or duplicated"


def test_grouping_drops_cross_component_edges():
    es = sim_edges.load_edges(FakeZRedis(members(PAIRS)), "k", PREFIX, True, None)
    labels = np.array([0, 1, 0, 1, 0, 1, 0])  # deliberately shredded
    groups = sim_edges.group_edges_by_component(es, labels)
    for comp, (s, d, _) in groups.items():
        assert (labels[s] == comp).all() and (labels[d] == comp).all()


def test_adjacency_is_symmetric_and_matches_edge_count():
    es = sim_edges.load_edges(FakeZRedis(members(PAIRS)), "k", PREFIX, True, None)
    adj = sim_edges.build_adjacency(es, len(es.id_to_idx))
    dense = adj.toarray()
    assert (dense == dense.T).all(), "adjacency must be symmetric"
    # every edge with dist < 1.0 contributes both directions
    real = int((es.dist < 1.0).sum())
    assert int((dense > 0).sum()) == real * 2


def test_arrays_are_compact_types():
    """The whole point: 12 bytes an edge, not ~120."""
    es = sim_edges.load_edges(FakeZRedis(members(PAIRS)), "k", PREFIX, True, None)
    assert es.src.dtype == np.int32
    assert es.dst.dtype == np.int32
    assert es.dist.dtype == np.float32
    per_edge = es.src.itemsize + es.dst.itemsize + es.dist.itemsize
    assert per_edge == 12, per_edge


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
            passed += 1
    print(f"\n{passed} passed")
