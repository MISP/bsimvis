"""N-way constrained greedy match of functions across N files.

Pure: no I/O. Generalises `bin_sim_tags.greedy_match` -- best edges first,
union-find over functions, and a merge is refused when it would put two
functions of the same file in one row. With two files the rows equal
`greedy_match`'s accepted edges.
"""

import random


def nway_match(edges_by_pair, members, min_edge=0.0):
    """Group functions of N files into rows.

    `members` is `{member: {fid: weight}}`; its key order fixes the mask bits.
    Fids must be unique across members. `edges_by_pair` is
    `{(member_i, member_j): [(fid_i, fid_j, score), ...]}`, one entry per pair.

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
    parent = {fid: fid for fid in owner}
    mask = {fid: bit[owner[fid]] for fid in owner}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    edges = sorted(
        (
            (-score, fid_a, fid_b)
            for pair_edges in edges_by_pair.values()
            for fid_a, fid_b, score in pair_edges
            if score >= min_edge
        )
    )
    for _, fid_a, fid_b in edges:
        root_a, root_b = find(fid_a), find(fid_b)
        if root_a == root_b or mask[root_a] & mask[root_b]:
            continue
        parent[root_b] = root_a
        mask[root_a] |= mask[root_b]

    groups = {}
    for fid in sorted(owner):
        groups.setdefault(find(fid), []).append(fid)
    scores = {root: [] for root in groups}
    for neg_score, fid_a, fid_b in edges:
        root = find(fid_a)
        if root == find(fid_b):
            scores[root].append(-neg_score)

    rows = []
    for root, fids in groups.items():
        span = len(fids)
        found = scores[root]
        rows.append(
            {
                "mask": mask[root],
                "cells": {owner[fid]: fid for fid in fids},
                "span": span,
                "weight": max(weight[fid] for fid in fids),
                "support": len(found) / (span * (span - 1) / 2) if span > 1 else 1.0,
                "cohesion": sum(found) / len(found) if found else 1.0,
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
    e = {("A", "B"): [("a", "b", 0.95)], ("B", "C"): [("b", "c", 0.9)]}
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
    print("nway_match demo ok")


if __name__ == "__main__":
    demo()
