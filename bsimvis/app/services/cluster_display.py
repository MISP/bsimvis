"""Which clusters of a dendrogram the file search shows, and how they nest.

`prune` is pure; `load_display_tree` at the bottom reads the stored tree.
`nodes` is {cid: {"parent": cid | None, "n": members, "coh": cohesion}}.
The full tree is far too deep to show, and a fixed cohesion floor hides loose but
real clusters, so a node is kept only if it adds information over the nearest
kept ancestor:

  * it has at least `min_members` files and cohesion >= `min_cohesion`
  * as a root (no kept ancestor) that is all it needs, whatever its cohesion
  * below a kept ancestor it must also be tighter by `jump` and clearly smaller
    (<= `max_ratio` of the ancestor), else it is the same cluster again

Kept chains are cut to `depth` levels: the root, the deepest node and the most
relevant (`score`) nodes in between survive.

`pinned` nodes (analyst-named clusters) are always kept when they hold 2 files
or more, and the depth cap never drops one.

Self-check: uv run python -I bsimvis/app/services/cluster_display.py
"""

import hashlib
import json
import math
import threading

DEFAULTS = {
    "min_members": 3,
    "min_cohesion": 0.1,
    "jump": 0.10,
    "max_ratio": 0.9,
    "depth": 3,
}


def score(n, coh):
    """Rank for roots and siblings; `n` is the matching member count.

    Cohesion saturates (coh / (coh + 0.2)): past ~0.3 a cluster is coherent
    enough to be a family and size decides, so a 78-file 0.32 family outranks
    a 7-file 0.97 sub-cluster. 0.2 was picked on more_mirai_yara against the
    Mirai family report's clusters; 0.15 to 0.3 give the same families there.
    """
    return math.log1p(n) * coh / (coh + 0.2)


def prune(nodes, pinned=(), **opts):
    """{cid: {"parent": kept ancestor | None, "depth": 0-based}} for kept nodes."""
    o = {**DEFAULTS, **opts}
    pinned = {c for c in pinned if c in nodes and nodes[c]["n"] >= 2}
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
        if ok or cid in pinned:
            kept[cid] = anc
            anc = cid
        stack.extend((c, anc) for c in kids.get(cid, []))

    return _cap_depth(nodes, kept, o["depth"], pinned)


def _cap_depth(nodes, kept, depth, pinned=frozenset()):
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

    # Deepest violators first; each drop is the least relevant middle node of its chain.
    for c in sorted(kept, key=lambda c: -len(chain(c))):
        while c in kept:
            ch = chain(c)
            if len(ch) <= depth:
                break
            mid = [m for m in ch[1:-1] if m not in pinned]
            if not mid:  # only pinned nodes in between: the chain stays long
                break
            drop = min(mid, key=lambda m: score(nodes[m]["n"], nodes[m]["coh"]))
            up = kept.pop(drop)
            for k in sub.pop(drop, []):
                kept[k] = up
                sub.setdefault(up, []).append(k)
            sub[up].remove(drop)
    out = {c: {"parent": p} for c, p in kept.items()}
    for c in out:
        out[c]["depth"] = len(chain(c)) - 1
    return out


def load_display_tree(r, collection, algo, cache=None, **opts):
    return _load(r, collection, algo, cache, **opts)[0]


def _load(r, collection, algo, cache=None, **opts):
    """{cid: {"parent", "depth", "n", "coh"}} of the kept clusters of one tree.

    `algo` is the final key namespace (axis / engine / node type already in it).
    Reads the tree_links blob (one GET) and the meta of nodes with enough
    members only: a small cluster is never kept, and a child is never bigger
    than its parent, so the 30 KB distribution blobs of the long tail are not
    touched. `cache` (a redis client) keeps the result for an hour, keyed by
    the blob's hash and the analyst-named (pinned) set, so a rebuild or a
    rename invalidates it.
    """
    from bsimvis.app.services.cluster_utils import get_cluster_metas, real_links

    o = {**DEFAULTS, **opts}
    raw = r.get(f"{collection}:bin_cluster:tree_links:{algo}")
    if not raw:
        return {}, ""
    named = sorted(
        c.decode() if isinstance(c, bytes) else str(c)
        for c in r.smembers(f"{collection}:bin_cluster:named:{algo}")
    )
    tag = hashlib.sha1(raw if isinstance(raw, bytes) else raw.encode())
    tag.update(
        json.dumps([o, named], sort_keys=True).encode() + b"|v3"
    )  # stored-set format / drop rule
    ck = f"files:clusterdisp:{collection}:{algo}:{tag.hexdigest()[:16]}"
    if cache is not None:
        hit = cache.get(ck)
        if hit:
            return json.loads(hit), tag.hexdigest()[:16]

    links = real_links(json.loads(raw))
    size = {str(l["child"]): int(l.get("size") or 0) for l in links}
    parent = {str(l["child"]): str(l["parent"]) for l in links}
    cands = [c for c, n in size.items() if n >= o["min_members"] or c in named]
    nodes = {}
    for i in range(0, len(cands), 500):
        metas = get_cluster_metas(
            r, collection, algo, cands[i : i + 500], "bin_cluster"
        )
        for c, m in metas.items():
            nodes[c] = {
                "parent": parent.get(c),
                "n": int(m.get("member_count") or size[c]),
                "coh": float(m.get("cohesion_score") or 0),
            }
    out = {c: {**nodes[c], **v} for c, v in prune(nodes, named, **o).items()}
    if cache is not None:
        cache.setex(ck, 3600, json.dumps(out))
    return out, tag.hexdigest()[:16]


# --- precomputed file -> deepest kept cluster, stored in Kvrocks -------------
# `{coll}:bin_cluster:display:{algo}` holds the tag of the current hash
# `...:display:{algo}:{tag}` (file_id -> cid). The tag hashes the tree blob and
# the display opts, so a recluster makes it stale and the next read rebuilds it.
_BUILD_LOCKS = {}


def _dkey(collection, algo):
    return f"{collection}:bin_cluster:display:{algo}"


def clear_display(r, collection, algo):
    k = _dkey(collection, algo)
    tag = r.get(k)
    if tag:
        hk = f"{k}:{tag.decode() if isinstance(tag, bytes) else tag}"
        cids = {c.decode() if isinstance(c, bytes) else str(c) for c in r.hvals(hk)}
        r.delete(hk, *[f"{hk}:c:{c}" for c in cids])
    r.delete(k)


def display_assignment(r, collection, algo, tree, tag, only=None):
    """{file_id: deepest kept cid} from the stored hash, built on first use.

    `only` (a set of file ids) limits the answer; absent ids are unclustered.
    """
    k = _dkey(collection, algo)

    def current():
        t = r.get(k)
        return (t.decode() if isinstance(t, bytes) else t) == tag

    if not current():
        lock = _BUILD_LOCKS.setdefault((collection, algo), threading.Lock())
        with lock:  # one build at a time; others wait, then find it ready
            if not current():
                _build_display(r, collection, algo, tree, tag)
    hk = f"{k}:{tag}"
    dec = lambda v: v.decode() if isinstance(v, bytes) else v
    if only is not None and len(only) * 4 < r.hlen(hk):
        ids = list(only)
        out = {}
        for i in range(0, len(ids), 5000):
            chunk = ids[i : i + 5000]
            for f, c in zip(chunk, r.hmget(hk, chunk)):
                if c:
                    out[f] = dec(c)
        return out
    allf = {dec(f): dec(c) for f, c in r.hgetall(hk).items()}
    return allf if only is None else {f: c for f, c in allf.items() if f in only}


def display_direct(r, collection, algo, tag, cids):
    """{cid: [file ids]} directly under each kept cluster (stored sets)."""
    hk = f"{_dkey(collection, algo)}:{tag}"
    cids = list(cids)
    pipe = r.pipeline(transaction=False)
    for c in cids:
        pipe.smembers(f"{hk}:c:{c}")
    dec = lambda v: v.decode() if isinstance(v, bytes) else str(v)
    return {c: [dec(m) for m in mem] for c, mem in zip(cids, pipe.execute())}


def display_counts(r, collection, algo, tree, tag):
    """{cid: number of files directly under it}, no file id leaves Kvrocks."""
    hk = f"{_dkey(collection, algo)}:{tag}"
    cids = list(tree)
    pipe = r.pipeline(transaction=False)
    for c in cids:
        pipe.scard(f"{hk}:c:{c}")
    return dict(zip(cids, pipe.execute()))


def display_direct(r, collection, algo, tag, cids):
    """{cid: [file ids]} directly under each kept cluster (stored sets)."""
    hk = f"{_dkey(collection, algo)}:{tag}"
    cids = list(cids)
    pipe = r.pipeline(transaction=False)
    for c in cids:
        pipe.smembers(f"{hk}:c:{c}")
    dec = lambda v: v.decode() if isinstance(v, bytes) else str(v)
    return {c: [dec(m) for m in mem] for c, mem in zip(cids, pipe.execute())}


def display_counts(r, collection, algo, tree, tag):
    """{cid: number of files directly under it}, no file id leaves Kvrocks."""
    hk = f"{_dkey(collection, algo)}:{tag}"
    cids = list(tree)
    pipe = r.pipeline(transaction=False)
    for c in cids:
        pipe.scard(f"{hk}:c:{c}")
    return dict(zip(cids, pipe.execute()))


def _build_display(r, collection, algo, tree, tag):
    k = _dkey(collection, algo)
    clear_display(r, collection, algo)
    hk = f"{k}:{tag}"
    where = {}
    for c in sorted(tree, key=lambda c: tree[c]["depth"]):  # deeper wins
        mem = r.smembers(f"{collection}:bin_cluster:{algo}:{c}:members")
        for m in mem:
            where[m.decode() if isinstance(m, bytes) else str(m)] = c
    direct = {}
    for f, c in where.items():
        direct.setdefault(c, []).append(f)
    fids = list(where)
    for i in range(0, len(fids), 5000):
        r.hset(hk, mapping={f: where[f] for f in fids[i : i + 5000]})
    for c, ids in direct.items():
        for i in range(0, len(ids), 5000):
            r.sadd(f"{hk}:c:{c}", *ids[i : i + 5000])
    r.set(k, tag)  # last: a reader never sees a half-built hash


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

    # Pinned (analyst-named) nodes: kept though too small / not tighter, and the
    # depth cap drops an unpinned middle node instead.
    t = {
        "r": N(None, 100, 0.20),
        "a": N("r", 99, 0.21),  # same cluster again: dropped unless pinned
        "b": N("a", 60, 0.50),
        "c": N("b", 40, 0.75),
        "d": N("c", 10, 0.99),
        "p": N("d", 2, 0.99),  # under min_members
        "one": N("d", 1, 1.0),
    }
    got = prune(t, pinned={"a", "p", "one"}, jump=0.1)
    assert "a" in got and "p" in got and "one" not in got, got
    assert got["p"] == {"parent": "a", "depth": 2}, got  # b, c, d made way
    got = prune(t, pinned={"a", "b", "c"}, jump=0.1)  # every middle pinned
    assert {"a", "b", "c"} <= set(got) and got["d"]["depth"] == 4, got
    print("cluster_display demo ok")


if __name__ == "__main__":
    demo()
