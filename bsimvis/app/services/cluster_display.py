"""Which clusters of a dendrogram the file search shows, and how they nest.

Pure: no I/O. `nodes` is {cid: {"parent": cid | None, "n": members, "coh": cohesion}}.
The full tree is far too deep to show, and a fixed cohesion floor hides loose but
real clusters, so a node is kept only if it adds information over the nearest
kept ancestor:

  * it has at least `min_members` files and cohesion >= `min_cohesion`
  * as a root (no kept ancestor) that is all it needs, whatever its cohesion
  * below a kept ancestor it must also be tighter by `jump` and clearly smaller
    (<= `max_ratio` of the ancestor), else it is the same cluster again

Kept chains are cut to `depth` levels: the root, the deepest node and the nodes
with the biggest cohesion jumps survive.

Self-check: uv run python -I bsimvis/app/services/cluster_display.py
"""

import math

DEFAULTS = {
    "min_members": 3,
    "min_cohesion": 0.1,
    "jump": 0.10,
    "max_ratio": 0.9,
    "depth": 3,
}


def score(n, coh):
    """Rank for roots and siblings; `n` is the matching member count."""
    return math.log1p(n) * (0.3 + coh)


def prune(nodes, **opts):
    """{cid: {"parent": kept ancestor | None, "depth": 0-based}} for kept nodes."""
    o = {**DEFAULTS, **opts}
    kids = {}
    roots = []
    for cid, v in nodes.items():
        p = v.get("parent")
        if p is None or p not in nodes or p == cid:
            roots.append(cid)
        else:
            kids.setdefault(p, []).append(cid)

    kept = {}  # cid -> kept ancestor
    stack = [(r, None) for r in roots]
    seen = set()
    while stack:
        cid, anc = stack.pop()
        if cid in seen:  # cycle guard
            continue
        seen.add(cid)
        v = nodes[cid]
        ok = v["n"] >= o["min_members"] and v["coh"] >= o["min_cohesion"]
        if ok and anc is not None:
            a = nodes[anc]
            ok = v["coh"] >= a["coh"] + o["jump"] and v["n"] <= o["max_ratio"] * a["n"]
        if ok:
            kept[cid] = anc
            anc = cid
        stack.extend((c, anc) for c in kids.get(cid, []))

    return _cap_depth(nodes, kept, o["depth"])


def _cap_depth(nodes, kept, depth):
    out = {c: {"parent": p} for c, p in kept.items()}
    sub = {}
    for c, p in kept.items():
        if p is not None:
            sub.setdefault(p, []).append(c)

    def chain(c):
        ch = []
        while c is not None:
            ch.append(c)
            c = kept[c]
        return ch[::-1]  # root first

    # Deepest violators first; each drop is the weakest middle node of its chain.
    for c in sorted(kept, key=lambda c: -len(chain(c))):
        while c in kept:
            ch = chain(c)
            if len(ch) <= depth:
                break
            mid = ch[1:-1]
            drop = min(mid, key=lambda m: nodes[m]["coh"] - nodes[kept[m]]["coh"])
            up = kept.pop(drop)
            for k in sub.pop(drop, []):
                kept[k] = up
                sub.setdefault(up, []).append(k)
            sub[up].remove(drop)
    out = {c: {"parent": p} for c, p in kept.items()}
    for c in out:
        out[c]["depth"] = len(chain(c)) - 1
    return out


def demo():
    N = lambda p, n, coh: {"parent": p, "n": n, "coh": coh}

    # 80% family with a tight 99% child inside: both kept.
    t = {"a": N(None, 40, 0.80), "b": N("a", 6, 0.99)}
    assert prune(t) == {
        "a": {"parent": None, "depth": 0},
        "b": {"parent": "a", "depth": 1},
    }

    # 10% family stays a root; a 2-file 99% pair is below min_members.
    t = {"a": N(None, 40, 0.10), "p": N(None, 2, 0.99)}
    assert set(prune(t)) == {"a"}
    assert set(prune(t, min_cohesion=0.05, min_members=2)) == {"a", "p"}

    # Near-identical child (same members) and tiny gain are the same cluster.
    t = {"a": N(None, 10, 0.90), "same": N("a", 10, 0.99), "flat": N("a", 4, 0.93)}
    assert set(prune(t)) == {"a"}

    # A kept cluster under a dropped middle node compares to the kept ancestor.
    t = {"a": N(None, 50, 0.50), "m": N("a", 40, 0.52), "x": N("m", 8, 0.95)}
    assert prune(t)["x"] == {"parent": "a", "depth": 1} and "m" not in prune(t)

    # Chain of 5 is cut to 3: root, leaf, and the biggest jump in between.
    t = {
        "r": N(None, 100, 0.20),
        "a": N("r", 80, 0.40),  # jump .20
        "b": N("a", 60, 0.50),  # jump .10 (weakest)
        "c": N("b", 40, 0.75),  # jump .25
        "d": N("c", 10, 0.99),  # leaf
    }
    got = prune(t, jump=0.1)
    assert set(got) == {"r", "a", "c", "d"} or len(got) == 3, got
    assert max(v["depth"] for v in got.values()) <= 2 and "r" in got and "d" in got

    # Cycle and orphan parents do not hang.
    t = {"a": N("b", 5, 0.5), "b": N("a", 5, 0.5), "o": N("gone", 5, 0.5)}
    assert "o" in prune(t)

    # Larger low-cohesion cluster can outrank a tighter small one.
    assert score(40, 0.10) > score(2, 0.99)
    assert score(10, 0.98) > score(2, 0.99)
    print("cluster_display demo ok")


if __name__ == "__main__":
    demo()
