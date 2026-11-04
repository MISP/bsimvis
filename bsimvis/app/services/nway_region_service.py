"""Call-graph regions of an N-way table: rows whose functions call each other.

The graph has one node per row; two rows are linked when a function of one
calls a function of the other (in the same binary), weighted by how many such
calls there are. Communities come from Louvain modularity. Library and runtime
rows never link regions (they are called from everywhere) and get regions of
their own. Orchestrators, plumbing and any row called by more than HUB rows
bridge modules, so they stay out of the graph and then join the region they
link to most: on real binaries that takes a file from 1-2 regions to 8-12.
Regions are named later, like lanes.
"""

from collections import Counter, defaultdict

SKIP = ("library", "runtime")
BRIDGE = ("orchestrator", "plumbing")
HUB = 25
MIN_REGION = 3
# region keys that are not call-graph communities, in display order after the real ones
SPECIAL = (
    ("small", "Small regions", "Regions of fewer than 3 rows."),
    ("none", "Outside the call graph", "Rows with no call to or from another row."),
    ("runtime", "Runtime", "C runtime start-up and teardown."),
    ("library", "Library", "Recognised by the library analyzer."),
)


def _local_moving(graph):
    """One Louvain level: move nodes to the neighbour community with the best modularity gain."""
    k = {u: sum(vs.values()) for u, vs in graph.items()}
    m2 = sum(k.values())
    comm = {u: u for u in graph}
    if not m2:
        return comm, False
    tot = dict(k)
    order = sorted(graph)
    moved, changed, passes = False, True, 0
    while changed and passes < 20:
        changed, passes = False, passes + 1
        for u in order:
            cu, ku = comm[u], k[u]
            links = defaultdict(float)
            for v, w in graph[u].items():
                if v != u:
                    links[comm[v]] += w
            tot[cu] -= ku
            best, gain = cu, links.get(cu, 0.0) - tot[cu] * ku / m2
            for c in sorted(links):
                g = links[c] - tot[c] * ku / m2
                if g > gain + 1e-12:
                    best, gain = c, g
            tot[best] += ku
            if best != cu:
                comm[u], changed, moved = best, True, True
    return comm, moved


def louvain(adj):
    """{node: community} of a weighted undirected graph `{u: {v: w}}` (both directions stored)."""
    graph = {u: dict(vs) for u, vs in adj.items()}
    node_of = {u: u for u in graph}
    for _ in range(10):
        comm, moved = _local_moving(graph)
        if not moved:
            break
        node_of = {o: comm[s] for o, s in node_of.items()}
        agg = {c: defaultdict(float) for c in set(comm.values())}
        for u, vs in graph.items():
            for v, w in vs.items():
                agg[comm[u]][comm[v]] += w
        graph = agg
    return node_of


def row_regions(rows, calls, fids_of):
    """{row index: region key}: `r1`, `r2`... by size, or one of SPECIAL's keys."""
    row_of = {f: i for i, row in enumerate(rows) for f in fids_of(row)}
    pairs, callers = Counter(), defaultdict(set)
    for f, callees in calls.items():
        a = row_of.get(f)
        if a is None:
            continue
        for c in callees:
            b = row_of.get(c)
            if b is not None and b != a:
                callers[b].add(a)
                pairs[(min(a, b), max(a, b))] += 1
    out = {i: row["role"] for i, row in enumerate(rows) if row.get("role") in SKIP}
    hubs = {b for b, cs in callers.items() if len(cs) > HUB}
    hubs |= {i for i, row in enumerate(rows) if row.get("role") in BRIDGE}
    adj = defaultdict(dict)
    for (a, b), w in pairs.items():
        if a in out or b in out or a in hubs or b in hubs:
            continue
        adj[a][b] = adj[b][a] = float(w)
    comm = louvain(adj)
    # hubs, and rows only linked through one, join the region they call or are called from most
    # rounds, so a bridge row linked only to other bridges inherits through them
    for _ in range(6):
        votes = defaultdict(Counter)
        for (a, b), w in pairs.items():
            if a not in out and b in comm and a not in comm:
                votes[a][comm[b]] += w
            if b not in out and a in comm and b not in comm:
                votes[b][comm[a]] += w
        if not votes:
            break
        for i, v in votes.items():
            comm[i] = max(v, key=lambda c: (v[c], -c))
    size = Counter(comm.values())
    big = sorted(
        (c for c, n in size.items() if n >= MIN_REGION), key=lambda c: (-size[c], c)
    )
    name = {c: f"r{k}" for k, c in enumerate(big, 1)}
    for i in range(len(rows)):
        if i not in out:
            out[i] = name.get(comm[i], "small") if i in comm else "none"
    return out


def attach(doc, fids_of):
    """Set `region` on every row of `doc` once; needs the roles pass (`doc["_calls"]`)."""
    if doc.get("_regions_done") or "_roles" not in doc:
        return
    regions = row_regions(doc["rows"], doc.get("_calls") or {}, fids_of)
    for i, row in enumerate(doc["rows"]):
        row["region"] = regions[i]
    doc["_regions_done"] = True


def unit_region(u):
    """A merged function sits in the region most of its rows sit in."""
    if "_rows" not in u:
        return u.get("region")
    count = Counter(r["region"] for r in u["_rows"] if r.get("region"))
    return max(sorted(count), key=lambda k: count[k]) if count else None


def demo():
    # two triangles joined by one edge split in two; a library row stays apart
    f = lambda i: [f"c:func:m:{i}"]
    rows = [{"cells": {"A": f(i)[0]}, "role": "logic"} for i in range(6)]
    rows.append({"cells": {"A": "c:func:m:6"}, "role": "library"})
    calls = {
        "c:func:m:0": ["c:func:m:1", "c:func:m:2", "c:func:m:6"],
        "c:func:m:1": ["c:func:m:2"],
        "c:func:m:2": ["c:func:m:3"],
        "c:func:m:3": ["c:func:m:4", "c:func:m:5", "c:func:m:6"],
        "c:func:m:4": ["c:func:m:5"],
    }
    fids = lambda row: list(row["cells"].values())
    got = row_regions(rows, calls, fids)
    assert got[6] == "library", got
    assert got[0] == got[1] == got[2] and got[3] == got[4] == got[5], got
    assert got[0] != got[3] and {got[0], got[3]} == {"r1", "r2"}, got
    assert (
        unit_region({"_rows": [{"region": "r2"}, {"region": "r2"}, {"region": "r1"}]})
        == "r2"
    )
    print("nway_region_service demo ok")


if __name__ == "__main__":
    demo()
