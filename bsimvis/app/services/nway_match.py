"""N-way constrained greedy match of functions across N files.

Pure: no I/O. Generalises `bin_sim_tags.greedy_match` -- best edges first,
union-find over functions, and a merge is refused when it would put two
functions of the same file in one row. With two files the rows equal
`greedy_match`'s accepted edges.
"""

import random

import numpy as np


class PairEdges:
    """One file pair's edges as int arrays into a shared fid list.

    Iterates as `(fid_a, fid_b, score)` like the tuple list it replaces, at
    ~16 B an edge instead of ~250: a 500-file set holds tens of millions.
    Edges naming a fid outside `ix` are dropped.
    """

    __slots__ = ("fids", "ix", "a", "b", "s")

    def __init__(self, fids, ix, edges):
        edges = list(edges)
        try:
            a, b = [ix[e[0]] for e in edges], [ix[e[1]] for e in edges]
        except KeyError:
            edges = [e for e in edges if e[0] in ix and e[1] in ix]
            a, b = [ix[e[0]] for e in edges], [ix[e[1]] for e in edges]
        s = [e[2] for e in edges]
        self.fids, self.ix = fids, ix
        self.a = np.array(a, np.int32)
        self.b = np.array(b, np.int32)
        self.s = np.array(s, np.float64)

    def __len__(self):
        return len(self.s)

    def __iter__(self):
        f = self.fids
        return (
            (f[x], f[y], s)
            for x, y, s in zip(self.a.tolist(), self.b.tolist(), self.s.tolist())
        )

    def score(self, x, y):
        """Score of the edge joining x and y (either side), or None."""
        i, j = self.ix.get(x), self.ix.get(y)
        if i is None or j is None:
            return None
        a, b = self.a, self.b
        hit = np.flatnonzero(((a == i) & (b == j)) | ((a == j) & (b == i)))
        return float(self.s[hit[0]]) if len(hit) else None


def edge_score(edges, x, y):
    """Score between x and y in one pair's edges (PairEdges or tuples), or None."""
    if isinstance(edges, PairEdges):
        return edges.score(x, y)
    return next((s for a, b, s in edges if {a, b} == {x, y}), None)


def concat_edges(edges_by_pair, fids, ix):
    """All pairs' edges as (a, b, s) arrays of indexes into `fids`."""
    parts, remap = [], {}
    for e in edges_by_pair.values():
        if not isinstance(e, PairEdges):
            e = PairEdges(fids, ix, e)
        if not len(e):
            continue
        if e.fids is fids:
            parts.append((e.a, e.b, e.s))
            continue
        m = remap.get(id(e.fids))
        if m is None:
            m = remap[id(e.fids)] = np.array([ix.get(f, -1) for f in e.fids], np.int64)
        parts.append((m[e.a], m[e.b], e.s))
    if not parts:
        return np.zeros(0, np.int64), np.zeros(0, np.int64), np.zeros(0)
    a, b, s = (np.concatenate(p) for p in zip(*parts))
    keep = (a >= 0) & (b >= 0)
    return a[keep], b[keep], s[keep]


def nway_match(edges_by_pair, members, min_edge=0.0):
    """Group functions of N files into rows.

    `members` is `{member: {fid: weight}}`; its key order fixes the mask bits.
    Fids must be unique across members. `edges_by_pair` is
    `{(member_i, member_j): edges}`, edges being `[(fid_i, fid_j, score), ...]`
    or a `PairEdges`. A pair left out was never read (sampled sets): `support`
    then only counts the row's pairs that are present.

    Returns rows, one per group, singletons included (a singleton is a function
    unique to its file):
    `{mask, cells, span, weight, support, cohesion}`.
    `mask` is a bitmask over `members`, `cells` is `{member: fid}`, `span` the
    number of files in the row and `weight` the heaviest member function.
    `support` is edges present / possible pairs in the row and `cohesion` the
    mean edge score; a singleton has both at 1.0.
    """
    bit = {m: 1 << i for i, m in enumerate(members)}
    owner = {fid: m for m, funcs in members.items() for fid in funcs}
    weight = {fid: w for funcs in members.values() for fid, w in funcs.items()}
    # Int ids in fid order: same tie-break as sorting on the fids, much cheaper.
    fids = sorted(owner)
    ix = {fid: i for i, fid in enumerate(fids)}
    parent = list(range(len(fids)))
    mask = [bit[owner[fid]] for fid in fids]

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    a, b, s = concat_edges(edges_by_pair, fids, ix)
    keep = s >= min_edge
    a, b, s = a[keep], b[keep], s[keep]
    order = np.lexsort((b, a, -s))
    a, b, s = a[order], b[order], s[order]
    for x, y in zip(a.tolist(), b.tolist()):
        root_a, root_b = find(x), find(y)
        if root_a == root_b or mask[root_a] & mask[root_b]:
            continue
        parent[root_b] = root_a
        mask[root_a] |= mask[root_b]

    root = np.array([find(i) for i in range(len(fids))], np.int64)
    groups = {}
    for i, r in enumerate(root.tolist()):
        groups.setdefault(r, []).append(i)
    ra = root[a]
    same = ra == root[b]
    n_found = np.bincount(ra[same], minlength=len(fids)).tolist()
    total = np.bincount(ra[same], weights=s[same], minlength=len(fids)).tolist()

    # pairs actually read, per member bit: all of them unless the set was sampled
    n = len(members)
    complete = len(edges_by_pair) >= n * (n - 1) // 2
    near = {m: 0 for m in bit}
    if not complete:
        for x, y in edges_by_pair:
            near[x] |= bit[y]
            near[y] |= bit[x]

    rows = []
    for r, members_ix in groups.items():
        span = len(members_ix)
        found = n_found[r]
        if complete:
            possible = span * (span - 1) / 2
        else:
            possible = (
                sum((near[owner[fids[i]]] & mask[r]).bit_count() for i in members_ix)
                / 2
            )
        rows.append(
            {
                "mask": mask[r],
                "cells": {owner[fids[i]]: fids[i] for i in members_ix},
                "span": span,
                "weight": max(weight[fids[i]] for i in members_ix),
                "support": found / possible if span > 1 and possible else 1.0,
                "cohesion": total[r] / found if found else 1.0,
            }
        )
    rows.sort(key=lambda r: (-r["span"], -r["weight"], min(r["cells"].values())))
    return rows


def demo():
    from bsimvis.app.services.bin_sim_tags import greedy_match

    # N=2 equals greedy_match, including ties and contention
    rng = random.Random(1)
    a = {f"a{i}": 1.0 for i in range(8)}
    b = {f"b{i}": 1.0 for i in range(8)}
    edges = [
        (x, y, rng.choice([0.5, 0.7, 0.9])) for x in a for y in b if rng.random() < 0.5
    ]
    rows = nway_match({("A", "B"): edges}, {"A": a, "B": b})
    got = {(r["cells"]["A"], r["cells"]["B"]) for r in rows if r["span"] == 2}
    want = {(x, y) for x, y, _ in greedy_match(edges)[0]}
    assert got == want

    # one function per file in a row, under contention
    m = {"A": {"a1": 1, "a2": 1}, "B": {"b1": 1}, "C": {"c1": 1}}
    e = {
        ("A", "B"): [("a1", "b1", 0.9), ("a2", "b1", 0.95)],
        ("A", "C"): [("a1", "c1", 0.8)],
    }
    rows = nway_match(e, m)
    assert sorted(f for r in rows for f in r["cells"].values()) == [
        "a1",
        "a2",
        "b1",
        "c1",
    ]
    assert all(len(r["cells"]) == r["span"] for r in rows)
    assert {"A": "a2", "B": "b1"} in [r["cells"] for r in rows]

    # drift: a~b, b~c, a!~c is one row with support < 1
    m = {"A": {"a": 1}, "B": {"b": 1}, "C": {"c": 1}}
    e = {("A", "B"): [("a", "b", 0.95)], ("B", "C"): [("b", "c", 0.9)], ("A", "C"): []}
    rows = nway_match(e, m)
    assert len(rows) == 1 and rows[0]["span"] == 3
    assert abs(rows[0]["support"] - 2 / 3) < 1e-9
    assert abs(rows[0]["cohesion"] - 0.925) < 1e-9

    # input order never changes the result
    base = nway_match({("A", "B"): edges}, {"A": a, "B": b})
    for seed in range(5):
        shuffled = edges[:]
        random.Random(seed).shuffle(shuffled)
        assert nway_match({("A", "B"): shuffled}, {"A": a, "B": b}) == base

    # min_edge splits a bridged row
    e = {("A", "B"): [("a", "b", 0.9)], ("B", "C"): [("b", "c", 0.3)]}
    assert len(nway_match(e, m, min_edge=0.0)) == 1
    rows = nway_match(e, m, min_edge=0.5)
    assert sorted(r["span"] for r in rows) == [1, 2]

    # a pair never read (sampled set) is not held against support
    e = {("A", "B"): [("a", "b", 0.95)], ("B", "C"): [("b", "c", 0.9)]}
    assert nway_match(e, m)[0]["support"] == 1.0

    # PairEdges round-trips and answers single scores
    fids = ["a", "b", "c"]
    pe = PairEdges(
        fids, {f: i for i, f in enumerate(fids)}, [("a", "b", 0.5), ("x", "c", 1.0)]
    )
    assert list(pe) == [("a", "b", 0.5)]
    assert edge_score(pe, "b", "a") == 0.5 and edge_score(pe, "a", "c") is None
    assert edge_score([("a", "b", 0.5)], "b", "a") == 0.5
    print("nway_match demo ok")


if __name__ == "__main__":
    demo()
