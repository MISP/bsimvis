"""Lineage tree of the files of an N-way set.

Pure: no I/O. Files become nodes, a tree (or chain, or star) over them gives
parent -> child edges, and each edge is annotated by diffing the child against
the parent over the matched rows:

  gained  rows holding the child but not the parent
  lost    rows holding the parent but not the child
  shared  rows holding both (child functions = shared + gained, parent = shared + lost)

`recall_suspect` is the part of gained/lost that is probably a matcher miss.
Evidence, in order of preference (each function counted once):
  1. edge: a lost parent function and a gained child function that the file
     pair's edges still link. The edge was dropped by `min_edge` ("below_floor")
     or refused by the one-per-file rule ("refused"). The matcher is untouched:
     the pair edges the N-way compute holds already keep sub-floor edges, only
     `nway_match` filters them.
  2. name: same non-default function name left on both sides.
A same-caller-pair fallback is not implemented.

Order: tree = maximum spanning tree (Prim, O(N^2)); chain = nearest neighbour +
2-opt, O(N^2) per pass, capped at CHAIN_MAX files; star = all files vs the root.
A version-key (e.g. 1.2 < 1.3) order is out of scope.
"""

from collections import Counter

import numpy as np

from bsimvis.app.services.nway_match import PairEdges

ORDERS = ("tree", "chain", "star")
CHAIN_MAX = 200  # 2-opt is O(N^2) per pass
TWO_OPT_PASSES = 20


class BadParams(ValueError):
    pass


def _packed(edges):
    """`edges` as a PairEdges (lists of tuples get a private fid index)."""
    if isinstance(edges, PairEdges):
        return edges
    fids = sorted({f for a, b, _ in edges for f in (a, b)})
    return PairEdges(fids, {f: i for i, f in enumerate(fids)}, edges)


def similarity_matrix(columns, edges_by_pair, min_edge):
    """File-pair similarity: matched edges >= min_edge / functions in the pair's union.

    Read off the pair edges the N-way compute loaded (hits capped by the smaller
    file); a pair never read (sampled set) scores 0.
    """
    pos = {c["id"]: i for i, c in enumerate(columns)}
    n = [c["functions"] for c in columns]
    w = np.zeros((len(columns), len(columns)))
    for (x, y), e in edges_by_pair.items():
        if x not in pos or y not in pos:
            continue
        i, j = pos[x], pos[y]
        hits = min(int((_packed(e).s >= min_edge).sum()), n[i], n[j])
        union = n[i] + n[j] - hits
        w[i, j] = w[j, i] = hits / union if union else 0.0
    return w


def spanning_tree(w, root):
    """Maximum spanning tree (Prim) rooted at index `root`: [(parent, child)]."""
    n = len(w)
    seen = np.zeros(n, bool)
    seen[root] = True
    best = w[root].copy()
    via = np.full(n, root)
    out = []
    for _ in range(n - 1):
        j = int(np.argmax(np.where(seen, -1.0, best)))
        out.append((int(via[j]), j))
        seen[j] = True
        closer = (w[j] > best) & ~seen
        best[closer], via[closer] = w[j][closer], j
    return out


def chain_path(w, root):
    """Seriation: nearest neighbour from `root`, then 2-opt. Root stays first."""
    n = len(w)
    path, left = [root], set(range(n)) - {root}
    while left:
        nxt = max(left, key=lambda j: (w[path[-1], j], -j))
        path.append(nxt)
        left.discard(nxt)

    def total(p):
        return sum(w[p[k], p[k + 1]] for k in range(n - 1))

    best = total(path)
    for _ in range(TWO_OPT_PASSES):
        improved = False
        for i in range(1, n - 1):
            for j in range(i + 1, n):
                cand = path[:i] + path[i : j + 1][::-1] + path[j + 1 :]
                t = total(cand)
                if t > best + 1e-12:
                    path, best, improved = cand, t, True
        if not improved:
            break
    return list(zip(path, path[1:]))


def _recall(lost, gained, e, min_edge, name_of):
    """Matcher-miss counts for one edge, by kind of evidence."""
    ev = {"edge_below_floor": 0, "edge_refused": 0, "name": 0}
    if e is not None and lost and gained:
        e = _packed(e)
        lm = np.zeros(len(e.fids), bool)
        gm = np.zeros(len(e.fids), bool)
        for f in lost:
            if f in e.ix:
                lm[e.ix[f]] = True
        for f in gained:
            if f in e.ix:
                gm[e.ix[f]] = True
        hit = np.flatnonzero((lm[e.a] & gm[e.b]) | (lm[e.b] & gm[e.a]))
        used = set()
        for k in hit[np.argsort(-e.s[hit], kind="stable")].tolist():
            x, y = int(e.a[k]), int(e.b[k])
            if x in used or y in used:
                continue
            used.update((x, y))
            ev["edge_below_floor" if e.s[k] < min_edge else "edge_refused"] += 1
        gone = {e.fids[i] for i in used}
        lost = [f for f in lost if f not in gone]
        gained = [f for f in gained if f not in gone]
    ln = Counter(n for f in lost if (n := name_of(f)))
    gn = Counter(n for f in gained if (n := name_of(f)))
    ev["name"] = sum((ln & gn).values())
    return ev


def build(columns, rows, edges_by_pair, min_edge, name_of, root=None, order="tree"):
    """Lineage of the files `columns` (dicts with id, file_name, functions) over `rows`.

    `rows` have `cells` {column id: fid}; `edges_by_pair` is the N-way pair edges
    {(col_a, col_b): PairEdges | tuples}; `name_of(fid)` is the function's real
    name or None. Returns {order, root, nodes, edges}.
    """
    if order not in ORDERS:
        raise BadParams(f"order must be one of {', '.join(ORDERS)}")
    ids = [c["id"] for c in columns]
    if root is not None and root not in ids:
        raise BadParams("root must be a file of the set")
    if len(ids) < 2:
        raise BadParams("need at least two files")
    if order == "chain" and len(ids) > CHAIN_MAX:
        raise BadParams(f"order=chain handles at most {CHAIN_MAX} files")
    w = similarity_matrix(columns, edges_by_pair, min_edge)
    if root is not None:
        r = ids.index(root)
    else:
        r = max(range(len(ids)), key=lambda i: (columns[i]["functions"], -i))
    if order == "tree":
        pairs = spanning_tree(w, r)
    elif order == "chain":
        pairs = chain_path(w, r)
    else:
        pairs = [(r, j) for j in range(len(ids)) if j != r]

    # one pass over the rows: per edge, shared rows and the lost / gained functions
    adj = {}
    for k, (p, c) in enumerate(pairs):
        adj.setdefault(ids[p], []).append((k, True))
        adj.setdefault(ids[c], []).append((k, False))
    shared = [0] * len(pairs)
    lost = [[] for _ in pairs]
    gained = [[] for _ in pairs]
    for row in rows:
        cells = row["cells"]
        for f in cells:
            for k, is_parent in adj.get(f, ()):
                p, c = ids[pairs[k][0]], ids[pairs[k][1]]
                if p in cells and c in cells:
                    shared[k] += is_parent  # once per row, from the parent side
                elif is_parent:
                    lost[k].append(cells[p])
                else:
                    gained[k].append(cells[c])

    out = []
    for k, (p, c) in enumerate(pairs):
        e = edges_by_pair.get((ids[p], ids[c]))
        if e is None:
            e = edges_by_pair.get((ids[c], ids[p]))
        ev = _recall(lost[k], gained[k], e, min_edge, name_of)
        out.append(
            {
                "parent": ids[p],
                "child": ids[c],
                "similarity": round(float(w[p, c]), 4),
                "gained": len(gained[k]),
                "lost": len(lost[k]),
                "shared": shared[k],
                "recall_suspect": sum(ev.values()),
                "recall_evidence": ev,
            }
        )
    nodes = [
        {
            "id": c["id"],
            "file_name": c.get("file_name"),
            "functions": c["functions"],
            "root": i == r,
        }
        for i, c in enumerate(columns)
    ]
    return {"order": order, "root": ids[r], "nodes": nodes, "edges": out}


def demo():
    cols = [
        {"id": i, "file_name": i, "functions": n} for i, n in zip("ABCD", (4, 5, 6, 3))
    ]
    e = {
        ("A", "B"): [(f"a{k}", f"b{k}", 0.9) for k in (1, 2, 3, 4)],
        ("B", "C"): [("b1", "c1", 0.9), ("b2", "c2", 0.9), ("b3", "c3", 0.9)]
        + [("b5", "c5", 0.2)],
        ("C", "D"): [("c1", "d1", 0.9), ("c2", "d2", 0.9)],
        ("A", "C"): [("a1", "c1", 0.5)],
        ("A", "D"): [],
        ("B", "D"): [],
    }
    rows = [
        {"cells": {"A": "a1", "B": "b1", "C": "c1", "D": "d1"}},
        {"cells": {"A": "a2", "B": "b2", "C": "c2", "D": "d2"}},
        {"cells": {"A": "a3", "B": "b3", "C": "c3"}},
        {"cells": {"A": "a4", "B": "b4"}},
        {"cells": {"B": "b5"}},
        {"cells": {"C": "c5"}},
        {"cells": {"C": "c6"}},
        {"cells": {"C": "c7"}},
        {"cells": {"D": "d3"}},
    ]
    names = {"c6": "foo", "d3": "foo"}
    got = {o: build(cols, rows, e, 0.5, names.get, order=o) for o in ORDERS}
    t = got["tree"]
    assert t["root"] == "C" and len(t["edges"]) == 3
    assert {frozenset((x["parent"], x["child"])) for x in t["edges"]} == {
        frozenset(p) for p in ("AB", "BC", "CD")
    }
    n = {x["id"]: x["functions"] for x in t["nodes"]}
    for x in t["edges"]:  # child = shared + gained, parent = shared + lost
        assert n[x["child"]] == x["shared"] + x["gained"], x
        assert n[x["parent"]] == x["shared"] + x["lost"], x
    cb = next(x for x in t["edges"] if x["child"] == "B")
    assert (cb["parent"], cb["shared"], cb["gained"], cb["lost"]) == ("C", 3, 2, 3)
    # b5 and c5 are linked by a 0.2 edge under the floor
    assert cb["recall_evidence"]["edge_below_floor"] == 1
    cd = next(x for x in t["edges"] if x["child"] == "D")
    assert cd["recall_evidence"] == {
        "edge_below_floor": 0,
        "edge_refused": 0,
        "name": 1,
    }
    assert len({tuple(sorted(x)) for o in ORDERS for x in got[o]["edges"]}) == 1
    assert [x["child"] for x in got["star"]["edges"]] == ["A", "B", "D"]
    assert len(got["chain"]["edges"]) == 3
    for bad in ({"order": "x"}, {"root": "Z"}):
        try:
            build(cols, rows, e, 0.5, names.get, **bad)
            raise AssertionError("expected BadParams")
        except BadParams:
            pass
    print("nway_lineage demo ok")


if __name__ == "__main__":
    demo()
