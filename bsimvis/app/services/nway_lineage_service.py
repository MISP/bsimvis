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
MAX_KIDS = 4  # more children than this: keep the biggest subsets, fold the rest
KEEP_SUBSETS = 6
REUSE_TOP = 12  # reuse classes returned; the rest fold into one entry
BY_FILE_MAX = 60  # beyond this many files by_file is served for one file= at a time
REUSE_MAX = 300  # reuse classes are skipped beyond this many files
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


def _partitions(parent, kids, counts, by_file):
    """Disjoint parts of a parent's kept code: one per exact subset of children holding it.

    `counts` {subset tuple: n}; more than MAX_KIDS children keeps the KEEP_SUBSETS
    biggest subsets and folds the rest into one `folded` part, whose `per_child`
    says how much of it each child holds.
    """
    items = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    keep, rest = items, []
    if len(kids) > MAX_KIDS and len(items) > KEEP_SUBSETS:
        keep, rest = items[:KEEP_SUBSETS], items[KEEP_SUBSETS:]
    out = [
        {
            "parent": parent,
            "children": list(sub),
            "count": n,
            "by_file": dict(by_file[sub]) if by_file.get(sub) else None,
        }
        for sub, n in keep
    ]
    if rest:
        per, bf = Counter(), Counter()
        for sub, n in rest:
            for c in sub:
                per[c] += n
            bf.update(by_file.get(sub) or {})
        out.append(
            {
                "parent": parent,
                "children": [c for c in kids if per[c]],
                "count": sum(n for _, n in rest),
                "folded": True,
                "per_child": dict(per),
                "by_file": dict(bf) or None,
            }
        )
    return out


def _components(keys, tedges):
    """Connected components of the tree edges induced on the file set `keys`."""
    root = {k: k for k in keys}

    def find(x):
        while root[x] != x:
            root[x] = root[root[x]]
            x = root[x]
        return x

    for a, b in tedges:
        if a in root and b in root:
            root[find(a)] = find(b)
    comps = {}
    for k in keys:
        comps.setdefault(find(k), []).append(k)
    return list(comps.values())


def build(
    columns, rows, edges_by_pair, min_edge, name_of, root=None, order="tree", file=None
):
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
    classes = (
        {}
    )  # presence mask -> [rows, file ids]: every per-file count derives from this
    fbit = {i: 1 << n for n, i in enumerate(ids)}
    for row in rows:
        cells = row["cells"]
        m = 0
        for f in cells:
            m |= fbit.get(f, 0)
        classes.setdefault(m, [0, tuple(cells)])[0] += 1
        for f in cells:
            for k, is_parent in adj.get(f, ()):
                p, c = ids[pairs[k][0]], ids[pairs[k][1]]
                if p in cells and c in cells:
                    shared[k] += is_parent  # once per row, from the parent side
                elif is_parent:
                    lost[k].append(cells[p])
                else:
                    gained[k].append(cells[c])

    kids = {}
    for p, c in pairs:
        kids.setdefault(ids[p], []).append(ids[c])
    big = len(ids) > BY_FILE_MAX
    sel = file if big and file in ids else None  # big sets: by_file for one file
    want = not big or sel is not None
    reuse_ok = len(ids) <= REUSE_MAX
    part_n, part_bf, born_bf = {}, {}, {}
    core, only_here, co = 0, Counter(), {}
    for cnt, keys in classes.values():
        present = set(keys)
        if len(keys) == len(ids):
            core += cnt
        if len(keys) == 1:
            only_here[keys[0]] += cnt
        for f in keys:
            for k, is_parent in adj.get(f, ()):
                if want and not is_parent and ids[pairs[k][0]] not in present:
                    born_bf.setdefault(k, Counter()).update(
                        {g: cnt for g in keys if sel is None or g == sel}
                    )
            if f in kids:
                sub = tuple(c for c in kids[f] if c in present)
                part_n.setdefault(f, Counter())[sub] += cnt
                if want:
                    part_bf.setdefault((f, sub), Counter()).update(
                        {g: cnt for g in keys if sel is None or g == sel}
                    )
        if want:
            for f in keys:
                if sel is not None and f != sel:
                    continue
                row_co = co.setdefault(f, Counter())
                for g in keys:
                    if g != f:
                        row_co[g] += cnt
    # reuse: a presence mask whose files are not connected in the tree
    depth = {ids[r]: 0}
    for p, c in pairs:
        depth[ids[c]] = depth[ids[p]] + 1
    pos = {i: n for n, i in enumerate(ids)}
    reuse, reuse_in, reuse_total = [], Counter(), 0
    if reuse_ok:
        tedges = [(ids[p], ids[c]) for p, c in pairs]
        for cnt, keys in classes.values():
            comps = _components(keys, tedges) if len(keys) > 1 else []
            if len(comps) < 2:
                continue
            reuse_total += cnt
            reuse_in.update({f: cnt for f in keys})
            ds = [depth[f] for f in keys]
            reuse.append(
                {
                    "files": sorted(keys, key=pos.get),
                    "components": sorted(
                        (sorted(c, key=pos.get) for c in comps),
                        key=lambda c: (min(depth[f] for f in c), pos[c[0]]),
                    ),
                    "count": cnt,
                    "depth_span": max(ds) - min(ds),
                    "same_level": max(ds) == min(ds),
                }
            )
        reuse.sort(key=lambda x: (-x["count"], x["files"]))
        rest = reuse[REUSE_TOP:]
        reuse = reuse[:REUSE_TOP]
        if rest:
            reuse.append(
                {
                    "folded": True,
                    "count": sum(x["count"] for x in rest),
                    "classes": len(rest),
                }
            )
    partitions, died_all, died_bf = [], {}, {}
    for f, counts in part_n.items():
        died_all[f] = counts.pop((), 0)
        died_bf[f] = part_bf.pop((f, ()), None)
        partitions += _partitions(
            f, kids[f], counts, {s: part_bf.get((f, s)) for s in counts}
        )

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
                "born_by_file": dict(born_bf[k]) if k in born_bf else None,
            }
        )
    nodes = [
        {
            "id": c["id"],
            "file_name": c.get("file_name"),
            "functions": c["functions"],
            "root": i == r,
            "died_all": died_all.get(c["id"]),
            "died_by_file": dict(died_bf[c["id"]]) if died_bf.get(c["id"]) else None,
            "only_here": only_here[c["id"]],
            "reuse_in": reuse_in[c["id"]] if reuse_ok else None,
            "depth": depth[c["id"]],
            "top_shared": (
                [[g, n] for g, n in co.get(c["id"], Counter()).most_common(3)]
                if want
                else None
            ),
        }
        for i, c in enumerate(columns)
    ]
    return {
        "order": order,
        "root": ids[r],
        "nodes": nodes,
        "edges": out,
        "partitions": partitions,
        "core": core,
        "by_file_skipped": not want,
        "reuse": reuse,
        "reuse_total": reuse_total,
        "reuse_skipped": not reuse_ok,
        "by_file_for": sel,
    }


def check_invariants(doc):
    """Assert the partition / by_file bookkeeping of a `build` result adds up."""
    n = {x["id"]: x for x in doc["nodes"]}
    parts = {}
    for p in doc["partitions"]:
        parts.setdefault(p["parent"], []).append(p)
    for pid, ps in parts.items():
        assert sum(p["count"] for p in ps) + n[pid]["died_all"] == n[pid]["functions"]
    for e in doc["edges"]:
        got = sum(
            p.get("per_child", {}).get(e["child"], p["count"])
            for p in parts.get(e["parent"], [])
            if e["child"] in p["children"]
        )
        assert got == e["shared"], (got, e)
    check_reuse(doc, n)
    if doc["by_file_skipped"] or doc["by_file_for"]:
        return
    for p in doc["partitions"]:
        bf = p["by_file"] or {}
        assert all(v <= p["count"] for v in bf.values())
        if not p.get("folded"):
            assert bf[p["parent"]] == p["count"]
            assert all(bf[c] == p["count"] for c in p["children"])
    for e in doc["edges"]:
        if e["gained"]:
            bf = e["born_by_file"]
            assert bf[e["child"]] == e["gained"] and e["parent"] not in bf
    for x in doc["nodes"]:
        if x["died_all"]:
            assert x["died_by_file"][x["id"]] == x["died_all"]
        assert x["only_here"] <= x["functions"]
    assert doc["core"] <= min(x["functions"] for x in doc["nodes"])


def check_reuse(doc, n):
    """Reuse classes: real nodes, never a tree-connected set, counts add up."""
    if doc["reuse_skipped"]:
        assert not doc["reuse"] and doc["reuse_total"] == 0
        return
    root = {i: i for i in n}

    def find(x):
        while root[x] != x:
            x = root[x]
        return x

    for e in doc["edges"]:
        root[find(e["parent"])] = find(e["child"])
    tree = [(e["parent"], e["child"]) for e in doc["edges"]]
    got = 0
    for r in doc["reuse"]:
        got += r["count"]
        if r.get("folded"):
            continue
        assert set(r["files"]) <= set(n) and len(r["files"]) > 1
        comps = _components(r["files"], tree)
        assert len(comps) == len(r["components"]) > 1  # never a connected set
        assert sorted(sum(r["components"], [])) == sorted(r["files"])
        assert r["same_level"] == (r["depth_span"] == 0)
    assert got == doc["reuse_total"]
    for x in doc["nodes"]:
        assert x["reuse_in"] <= x["functions"]
    assert sum(x["reuse_in"] for x in doc["nodes"]) >= sum(
        r["count"] * len(r["files"]) for r in doc["reuse"] if not r.get("folded")
    )


def _reuse_demo():
    ids = ["R", "A", "B", "X", "Y"]  # tree R-A, R-B, A-X, B-Y; X, Y are cousins
    cols = [{"id": i, "file_name": i, "functions": 0} for i in ids]
    sets = (
        ["RAXBY"] * 2  # core: connected
        + ["RAX"] * 3  # connected
        + ["RX"] * 4  # grandparent -> grandchild skip
        + ["XY"] * 5  # cousins
        + ["AB"] * 6  # siblings without the parent
        + ["RXY"] * 1  # three components
        + ["A"] * 2
    )
    rows = [{"cells": {f: f"{f}{i}" for f in s}} for i, s in enumerate(sets)]
    for c in cols:
        c["functions"] = sum(c["id"] in s for s in sets)
    e = {}
    for a, b in (("R", "A"), ("R", "B"), ("A", "X"), ("B", "Y")):
        e[(a, b)] = [
            (r["cells"][a], r["cells"][b], 0.9)
            for r in rows
            if a in r["cells"] and b in r["cells"]
        ]
    t = build(cols, rows, e, 0.5, lambda f: None, root="R")
    assert {frozenset((x["parent"], x["child"])) for x in t["edges"]} == {
        frozenset(p) for p in ("RA", "RB", "AX", "BY")
    }
    got = {"".join(r["files"]): r for r in t["reuse"]}
    assert set(got) == {"RX", "XY", "AB", "RXY"}, set(got)
    assert (got["RX"]["count"], got["RX"]["depth_span"], got["RX"]["same_level"]) == (
        4,
        2,
        False,
    )
    assert (got["XY"]["count"], got["XY"]["same_level"]) == (5, True)
    assert (got["AB"]["count"], got["AB"]["same_level"]) == (6, True)
    assert got["RXY"]["components"] == [["R"], ["X"], ["Y"]]
    assert t["reuse_total"] == 4 + 5 + 6 + 1 == sum(r["count"] for r in t["reuse"])
    assert [r["count"] for r in t["reuse"]] == [6, 5, 4, 1]
    assert {x["id"]: x["reuse_in"] for x in t["nodes"]} == {
        "R": 5,
        "A": 6,
        "B": 6,
        "X": 10,
        "Y": 6,
    }
    check_invariants(t)
    # chain A-B-C-D: A and C are reuse unless B holds the code too
    cc = [{"id": i, "file_name": i, "functions": 0} for i in "ABCD"]
    cs = ["ABCD"] * 3 + ["AC"] * 2 + ["ABC"] * 4 + ["AD"] * 1 + ["BC"] * 5
    cr = [{"cells": {f: f"{f}{i}" for f in s}} for i, s in enumerate(cs)]
    for c in cc:
        c["functions"] = sum(c["id"] in s for s in cs)
    ce = {
        (a, b): [
            (r["cells"][a], r["cells"][b], 0.9)
            for r in cr
            if a in r["cells"] and b in r["cells"]
        ]
        for a, b in ("AB", "BC", "CD")
    }
    ch = build(cc, cr, ce, 0.5, lambda f: None, root="A", order="chain")
    assert {"".join(r["files"]) for r in ch["reuse"]} == {"AC", "AD"}
    assert ch["reuse_total"] == 3
    check_invariants(ch)
    st = build(cols, rows, {}, 0.5, lambda f: None, root="R", order="star")
    assert "XY" in {
        "".join(r["files"]) for r in st["reuse"]
    }  # leaves are never adjacent
    assert not any(set(r["files"]) <= {"R", "A"} for r in st["reuse"])
    check_invariants(st)


def synthetic(n, shape="tree", seed=1, base=60):
    """n files in a made-up family (tree or star) with some cross-branch reuse."""
    import random

    rng = random.Random(seed)
    ids = [f"F{i}" for i in range(n)]
    held = {0: [(0, k) for k in range(base * 3)]}  # file -> [(birth file, k)]
    parent = {0: None}
    for i in range(1, n):
        parent[i] = 0 if shape == "star" else rng.randrange(i)
        keep = rng.uniform(0.5, 0.9)
        mine = [f for f in held[parent[i]] if rng.random() < keep]
        mine += [(i, k) for k in range(rng.randint(2, base))]
        if i > 2 and rng.random() < 0.3:  # reuse from a file outside the tree path
            other = rng.randrange(1, i)
            mine += rng.sample(held[other], min(len(held[other]), rng.randint(1, 8)))
        held[i] = list(dict.fromkeys(mine))
    rows = {}
    for i in range(n):
        for f in held[i]:
            rows.setdefault(f, {})[ids[i]] = f"{ids[i]}:{f[0]}:{f[1]}"
    rows = [{"cells": c} for c in rows.values()]
    cols = [
        {"id": ids[i], "file_name": f"bin_{i}.exe", "functions": len(held[i])}
        for i in range(n)
    ]
    edges = {
        (ids[parent[i]], ids[i]): [
            (c[ids[parent[i]]], c[ids[i]], 0.9)
            for c in (r["cells"] for r in rows)
            if ids[parent[i]] in c and ids[i] in c
        ]
        for i in range(1, n)
    }
    return cols, rows, edges


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
    for g in got.values():
        check_invariants(g)
    # C keeps 2 functions in B and D, 1 in B only, and 3 in no child
    cp = {
        tuple(p["children"]): p["count"] for p in t["partitions"] if p["parent"] == "C"
    }
    assert cp == {("B", "D"): 2, ("B",): 1} and t["nodes"][2]["died_all"] == 3
    assert (
        t["core"] == 2
        and t["nodes"][0]["only_here"] == 0
        and t["nodes"][3]["only_here"] == 1
    )
    # a star with many children folds its small subsets and still sums up
    kids = "ABCDE"
    cols5 = [{"id": "R", "file_name": "R", "functions": 8}] + [
        {"id": k, "file_name": k, "functions": 1 + (k in "AB")} for k in kids
    ]
    subs = [("A",), ("B",), ("C",), ("D",), ("E",), ("A", "B"), ("C", "D"), ("A", "E")]
    rows5 = [
        {"cells": {"R": f"r{i}", **{k: f"{k}{i}" for k in s5}}}
        for i, s5 in enumerate(subs)
    ]
    star = build(cols5, rows5, {}, 0.5, lambda f: None, root="R", order="star")
    pr = [p for p in star["partitions"] if p["parent"] == "R"]
    assert len(pr) == KEEP_SUBSETS + 1 and pr[-1]["folded"]
    assert sum(p["count"] for p in pr) == 8 and star["nodes"][0]["died_all"] == 0
    check_invariants(star)
    _reuse_demo()
    cols, rows, edges = synthetic(100)
    big = build(cols, rows, edges, 0.5, lambda f: None, root="F0")
    assert big["by_file_skipped"] and big["reuse"] and not big["reuse_skipped"]
    check_invariants(big)
    one = build(cols, rows, edges, 0.5, lambda f: None, root="F0", file="F7")
    assert one["by_file_for"] == "F7" and not one["by_file_skipped"]
    assert one["partitions"] == [
        {**p, "by_file": q["by_file"]}
        for p, q in zip(big["partitions"], one["partitions"])
    ]
    check_invariants(one)
    cols, rows, edges = synthetic(100, "star")
    star = build(cols, rows, edges, 0.5, lambda f: None, root="F0", order="star")
    pr = [p for p in star["partitions"] if p["parent"] == "F0"]
    assert len(pr) == KEEP_SUBSETS + 1 and pr[-1]["folded"]
    check_invariants(star)
    print("nway_lineage demo ok")


if __name__ == "__main__":
    demo()
