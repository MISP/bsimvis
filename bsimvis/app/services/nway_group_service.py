"""Group the rows of the N-way function table.

Two steps over the rows the table already filtered and sorted (units are
rows, or merged functions once a merge is on):

1. Merge (`group_merge`, optional): rows that are probably one function the
   matcher split are joined by a shared name and/or a shared caller. Rows that
   share a file are never joined. A merged function covers the union of its
   rows' files.
2. Group (`group`, optional, up to three levels): `prefix` groups by the first
   word of the function name (`attack_*`), `lane` bundles by the set of files,
   `tag` nests under the tags of one axis (optionally crossed with a second).
   With a merge on, the unit being grouped is the merged function, so
   a function split over two families sits in the lane of both together.

Under every leaf, merged functions are "same function" sub-groups holding their
rows. Rows with no partner are listed directly under the leaf, after them.

`group_rows` returns `(entries, groups, note)`. `entries` is the flat list in
display order, `(group id, row, extra fields)`; a closed group is one entry with
no row, and a tag row can appear under several groups. `groups` is `{id: meta}`
in display order; `meta["path"]` lists the ancestor ids a client prints a header
for. Masks go out as 0/1 strings over the table's columns so JavaScript never
sees a big int.
"""

import json
import math
import re
import threading
import time
from collections import Counter, defaultdict
from itertools import combinations

import numpy as np

from bsimvis.app.services.bin_sim_tags import AXES, merge_tag_fields, tag_axis
from bsimvis.app.services.nway_diff_service import _DEFAULT_NAME, _LRU, BadParams
from bsimvis.app.services.nway_match import PairEdges, concat_edges
from bsimvis.app.services.tag_taxonomy import tag_body, tag_prefixes

MODES = ("tag", "lane", "prefix")
PREFIX_MIN = 2  # functions that must share a prefix before it makes a group
LANE_MIN = 2  # units that must share a set of files before it makes a lane
LANE_FIT = (
    0.9  # a unit with this share of a bundle's files, all inside it, joins the bundle
)
SORTS = ("files", "lanes", "weight", "rows")
TAG_FANOUT = 3  # tags of one axis a row is listed under, most shared first
NAME_BUCKET = 12  # rows sharing one name that are paired up
CALL_HUB = 25  # a caller with more callee rows than this says nothing
CALL_MAX_FIDS = 30000  # call position reads one Redis set per function
UNTAGGED = "(no tag)"
# Grouped tree per (doc, filtered rows, group params): opening a group or
# scrolling only rebuilds the flat entry list.
_CACHE = _LRU(8)

_SUFFIX = re.compile(r"\.(part|isra|constprop|cold|llvm)(\.\d+)?$")


def _bits(mask, n):
    return "".join("1" if mask >> i & 1 else "0" for i in range(n))


def _pop(mask):
    return bin(mask).count("1")


def _fids(row):
    return [
        f for f in (row.get("files") or row["cells"]).values() if isinstance(f, str)
    ]


def _fileset(r):
    """Ids of the file columns holding a unit; a merged function holds its rows' files."""
    if "_rows" in r:
        return set().union(*map(_fileset, r["_rows"]))
    return set(r.get("files") or r["cells"])


def _meta(rows, n):
    """Totals of units: a unit is a row or a merged function (`_rows`)."""
    union, held = 0, set()
    for r in rows:
        union |= r["mask"]
        held |= _fileset(r)
    return {
        "_held": held,
        "rows": sum(len(r["_rows"]) if "_rows" in r else 1 for r in rows),
        "weight": sum(r["weight"] for r in rows),
        "union": _bits(union, n),
        "files": len(held),
    }


def _rank(g, sort):
    """Sort value of a group: the chosen measure first, the other two break ties.

    Files first is the default: a group covering more files comes before one
    covering fewer, whatever its kind, and features only decide between equals.
    """
    w, f, r = g["weight"], g["files"], g["rows"]
    return {
        "files": (f, w, r),
        "lanes": (f, w, r),
        "weight": (w, f, r),
        "rows": (r, f, w),
    }[sort]


def _by_rank(items, sort, meta=lambda x: x):
    """Highest rank first, equal ranks in id order."""
    items = sorted(items, key=lambda x: meta(x)["id"])
    return sorted(items, key=lambda x: _rank(meta(x), sort), reverse=True)


def _order(groups, sort):
    return _by_rank(groups, sort)


def lane_groups(rows, n, sort, min_size=LANE_MIN, fit=LANE_FIT):
    """One group per set of files shared by `min_size`+ units, plus a headless pair for the rest.

    A unit with a set of files of its own stays a plain row beside the lanes (or
    carries on to the next level), with one exception: a unit whose files all
    lie inside a bundle's and make up `fit` or more of them (it misses a file or
    two of many) joins the smallest such bundle. It carries `sub`, the number of
    missing files, and `miss`, the missing files as a 0/1 string.
    """
    count = Counter(r["mask"] for r in rows)
    bundles = [m for m, c in count.items() if c >= min_size]
    members = defaultdict(list)
    loose = []
    for r in rows:
        mask = r["mask"]
        if mask in bundles:
            members[mask].append((r, {}))
            continue
        holds = [m for m in bundles if not mask & ~m]
        near = min(holds, key=_pop) if holds else None
        if near is not None and _pop(mask) / _pop(near) >= fit:
            extra = {"sub": _pop(near) - _pop(mask), "miss": _bits(near & ~mask, n)}
            members[near].append((r, extra))
        else:
            loose.append(r)
    out = []
    for lane, items in members.items():
        g = _meta([r for r, _ in items], n)
        g.update(id=f"lane:{_bits(lane, n)}", kind="lane", mask=_bits(lane, n))
        g["path"] = [g["id"]]
        out.append((g, items))
    out = _by_rank(out, sort, lambda p: p[0])
    if loose:
        out.append(({"id": "", "path": [], "headless": True}, [(r, {}) for r in loose]))
    return out


def _mix_siblings(sibs, link):
    """Join sibling lanes whose files overlap (Jaccard >= `link`) under a node covering their union.

    Greedy, best pair first. The new node holds no rows; `core` is the files
    all its lanes share. Joined nodes can join again.
    """
    sibs = sorted(sibs, key=lambda s: s["mask"])
    while len(sibs) > 1:
        best = None
        for i, a in enumerate(sibs):
            for b in sibs[i + 1 :]:
                core, union = a["mask"] & b["mask"], a["mask"] | b["mask"]
                if union in (a["mask"], b["mask"]):
                    continue  # one holds the other: a parent would repeat its mask
                j = _pop(core) / _pop(union)
                if core and j >= link and (best is None or j > best[0]):
                    best = (j, a, b)
        if best is None:
            break
        _, a, b = best
        sibs = [s for s in sibs if s is not a and s is not b]
        sibs.append(
            {
                "mask": a["mask"] | b["mask"],
                "core": a["mask"] & b["mask"],
                "items": [],
                "kids": [a, b],
            }
        )
    return sibs


def lane_forest(rows, n, sort, min_size=LANE_MIN, fit=LANE_FIT, depth=2, link=0.0):
    """Lanes nested by file containment: a lane sits under the smallest lane holding all its files.

    -> (roots, loose). A root is `{"g", "items", "kids"}`: the lane's group meta
    (counting its sub-lanes), its own `(unit, extra)` pairs and its sub-lane
    roots. Lanes deeper than `depth` levels move up to sit beside their
    ancestor at `depth`. Units without a lane join one the way `lane_groups`
    does, and `loose` is what is left. With `link` > 0, sibling lanes sharing
    enough files get a parent of their own (`mix:`) covering their union.
    """
    count = Counter(r["mask"] for r in rows)
    bundles = [m for m, c in count.items() if c >= min_size]
    parent = {}
    for m in bundles:
        sup = [b for b in bundles if b != m and not m & ~b]
        parent[m] = min(sup, key=lambda b: (_pop(b), b)) if sup else None
    up = dict(parent)
    for m in bundles:
        chain = []
        p = parent[m]
        while p is not None:
            chain.append(p)
            p = parent[p]
        if len(chain) + 1 > depth:
            up[m] = chain[::-1][depth - 2] if depth >= 2 else None
    nodes = {m: {"mask": m, "items": [], "kids": []} for m in bundles}
    loose = []
    for r in rows:
        mask = r["mask"]
        if mask in nodes:
            nodes[mask]["items"].append((r, {}))
            continue
        holds = [m for m in bundles if not mask & ~m]
        near = min(holds, key=lambda b: (_pop(b), b)) if holds else None
        if near is not None and _pop(mask) / _pop(near) >= fit:
            extra = {"sub": _pop(near) - _pop(mask), "miss": _bits(near & ~mask, n)}
            nodes[near]["items"].append((r, extra))
        else:
            loose.append(r)
    roots = []
    for m, node in nodes.items():
        (nodes[up[m]]["kids"] if up[m] is not None else roots).append(node)

    def finish(node):
        if link > 0 and "core" not in node:
            node["kids"] = _mix_siblings(node["kids"], link)
        units = [u for u, _ in node["items"]]
        for k in node["kids"]:
            units += finish(k)
        g = _meta(units, n)
        g.update(id=f"lane:{_bits(node['mask'], n)}", kind="lane")
        g.update(mask=_bits(node["mask"], n), path=[g["id"]])
        if "core" in node:
            g.update(id=f"mix:{_bits(node['mask'], n)}", core=_bits(node["core"], n))
            g.update(lanes=len(node["kids"]), path=[g["id"]])
        node["g"] = g
        node["kids"] = _by_rank(node["kids"], sort, lambda k: k["g"])
        return units

    if link > 0:
        roots = _mix_siblings(roots, link)
    for node in roots:
        finish(node)
    return _by_rank(roots, sort, lambda k: k["g"]), loose


def name_prefix(name):
    """The first word of a function name -> (word, separator) or None.

    `attack_udp_flood` -> (`attack`, `_`), `Foo::bar` -> (`foo`, `:`),
    `killerKill` -> (`killer`, ``). None for auto names (FUN_, sub_) and for a
    name that is one word, which has no prefix of its own.
    """
    name = (name or "").strip().lstrip("_")
    if not name or _DEFAULT_NAME.match(name):
        return None
    word = re.split(r"[_.:$@]|(?<=[a-z0-9])(?=[A-Z])", name, maxsplit=1)[0]
    if len(word) < 2 or word == name:
        return None
    sep = name[len(word) : len(word) + 1]
    return word.lower(), sep if sep in "_.:" else ""


def _unit_prefix(unit):
    """The most common prefix over the names of a unit (a merged function has several rows)."""
    names = [r["name"] for r in unit["_rows"]] if "_rows" in unit else [unit["name"]]
    found = [p for p in map(name_prefix, names) if p]
    return Counter(found).most_common(1)[0][0] if found else None


def prefix_groups(units, n, sort, min_size=PREFIX_MIN):
    """One group per name prefix shared by `min_size`+ units, plus a headless pair for the rest."""
    by = defaultdict(list)
    loose = []
    for u in units:
        p = _unit_prefix(u)
        (by[p] if p else loose).append(u)
    out = []
    for (word, sep), members in by.items():
        if len(members) < min_size:
            loose += members
            continue
        g = _meta(members, n)
        g.update(id=f"prefix:{word}{sep}", kind="prefix", label=f"{word}{sep}*")
        g["path"] = [g["id"]]
        out.append((g, [(u, {}) for u in members]))
    out = _by_rank(out, sort, lambda p: p[0])
    if loose:
        # units without a usable prefix go on to the next level beside the prefix groups
        pos = {id(u): i for i, u in enumerate(units)}
        loose.sort(key=lambda u: pos[id(u)])
        out.append(({"id": "", "path": [], "headless": True}, [(u, {}) for u in loose]))
    return out


def _row_tags(row, fmeta, axis):
    """Tags on `axis` held by at least half of the row's functions, most shared first."""
    fids = _fids(row)
    count = Counter(
        t for f in fids for t in merge_tag_fields(fmeta.get(f)) if tag_axis(t) == axis
    )
    need = max(1, len(fids) / 2)
    ranked = sorted(
        (t for t in count if count[t] >= need), key=lambda t: (-count[t], t)
    )
    return ranked[:TAG_FANOUT]


def _tag_path(tag):
    """Two nesting levels of a tag, below its namespace: `yara:botnet` > `yara:botnet:mirai`."""
    chain = tag_prefixes(tag) + [tag_body(tag)[0]]
    return chain[1:3] or chain[-1:]


def tag_groups(rows, fmeta, n, sort, axis, axis_b=""):
    """Tag tree on `axis`; with `axis_b`, `axis` top level crossed with the other's."""
    for a in (axis, axis_b):
        if a and a not in AXES:
            raise BadParams(f"group_axis must be one of {AXES}")
    nodes, order = {}, []

    def node(path):
        gid = "tag:" + ">".join(path)
        if gid not in nodes:
            nodes[gid] = {
                "id": gid,
                "label": path[-1],
                "path": [],
                "rows": [],
                "own": [],
                "kids": [],
            }
            order.append(gid)
        return nodes[gid]

    also = {}
    for row in rows:
        a_tags = _row_tags(row, fmeta, axis)
        if axis_b:
            b_tags = _row_tags(row, fmeta, axis_b)
            tops = [_tag_path(b)[0] for b in b_tags] or [UNTAGGED]
            paths = [
                [_tag_path(a)[0], _tag_path(a)[0] + " | " + b]
                for a in a_tags
                for b in tops
            ]
        else:
            paths = [_tag_path(a) for a in a_tags]
        # two tags can share their first two levels: list the row once per path
        paths = [list(p) for p in dict.fromkeys(map(tuple, paths))] or [[UNTAGGED]]
        also[id(row)] = len(paths)
        for path in paths:
            parent = None
            for i in range(len(path)):
                nd = node(path[: i + 1])
                nd["rows"].append(row)
                if parent and nd["id"] not in parent["kids"]:
                    parent["kids"].append(nd["id"])
                parent = nd
            parent["own"].append(row)
    roots = [g for g in order if len(nodes[g]["id"].split(">")) == 1]
    out = []

    def walk(gid, ancestors):
        nd = nodes[gid]
        uniq = list({id(r): r for r in nd["rows"]}.values())
        g = _meta(uniq, n)
        g.update(
            id=gid,
            kind="tag",
            label=nd["label"],
            path=ancestors + [gid],
            depth=len(ancestors),
        )
        out.append((g, [(r, {"also": also[id(r)]}) for r in nd["own"]]))
        for kid in _order([_summary(nodes[k], n) for k in nd["kids"]], sort):
            walk(kid["id"], ancestors + [gid])

    for top in _order([_summary(nodes[g], n) for g in roots], sort):
        walk(top["id"], [])
    return out


def _summary(nd, n):
    g = _meta(list({id(r): r for r in nd["rows"]}.values()), n)
    g["id"] = nd["id"]
    return g


def name_key(row):
    """The row's name without compiler suffixes, or None for an auto name."""
    name = (row.get("name") or "").strip()
    if not name or _DEFAULT_NAME.match(name) or row.get("name_from") == "cluster":
        return None
    while m := _SUFFIX.search(name):
        name = name[: m.start()]
    return name.lower() or None


def _best_links(edges, row_of):
    """{(row, row): best edge score} between functions of different rows."""
    shared = {id(e.fids): e for e in edges.values() if isinstance(e, PairEdges)}
    if len(shared) == 1 and all(isinstance(e, PairEdges) for e in edges.values()):
        # one numpy pass over every edge instead of tens of millions of tuples
        e = next(iter(shared.values()))
        a, b, s = concat_edges(edges, e.fids, e.ix)
        row = np.array([row_of.get(f, -1) for f in e.fids], np.int64)
        ra, rb = row[a], row[b]
        keep = (ra >= 0) & (rb >= 0) & (ra != rb) & (s > 0)
        lo, hi, s = np.minimum(ra, rb)[keep], np.maximum(ra, rb)[keep], s[keep]
        width = int(row.max()) + 1 if len(row) else 1
        key = lo * width + hi
        order = np.lexsort((-s, key))
        key, s = key[order], s[order]
        uniq, first = np.unique(key, return_index=True)
        return dict(
            zip(
                zip((uniq // width).tolist(), (uniq % width).tolist()),
                s[first].tolist(),
            )
        )
    best = {}
    for pair_edges in edges.values():
        for fa, fb, score in pair_edges:
            ra, rb = row_of.get(fa), row_of.get(fb)
            if ra is None or rb is None or ra == rb:
                continue
            k = (min(ra, rb), max(ra, rb))
            if score > best.get(k, 0.0):
                best[k] = score
    return best


def _caller_rows(r, rows, row_of):
    """{row: set of caller rows}, read from the `{fid}:callers` sets."""
    fids = [(i, f) for i, row in rows for f in _fids(row)]
    pipe = r.pipeline(transaction=False)
    for _, f in fids:
        pipe.smembers(f"{f}:callers")
    out = defaultdict(set)
    for (i, _), members in zip(fids, pipe.execute()):
        for m in members:
            m = m.decode() if isinstance(m, bytes) else m
            if m in row_of and row_of[m] != i:
                out[i].add(row_of[m])
    return out


def merge_units(rows, doc, r, evidence, min_link):
    """Join rows that are probably one function -> (units, note).

    A unit is a row left alone, or a pseudo-row for 2+ joined rows: `mask` is
    the union of their files, `_rows` the rows, `_kinds` the evidence.
    """
    all_rows = doc["rows"]
    # every edge is scanned once per doc, not once per filter change
    if "_links" not in doc:
        row_of = {f: i for i, row in enumerate(all_rows) for f in _fids(row)}
        doc["_links"] = (row_of, _best_links(doc["edges"], row_of))
    row_of, best = doc["_links"]
    at = {id(row): i for i, row in enumerate(all_rows)}
    cand = [(at[id(row)], row) for row in rows if id(row) in at]
    link = {}  # (row, row) -> {"kinds", "sim"}
    # Rows are joined on files, lanes group on columns: with twin or child columns
    # one column holds several files, and two rows in one column may sit in different files.
    fbit = {
        c["id"]: 1 << i for i, c in enumerate(doc.get("file_columns") or doc["columns"])
    }

    def fmask(row):
        return sum(fbit.get(c, 0) for c in (row.get("files") or row["cells"]))

    def add(a, b, kind, sim):
        k = (min(a, b), max(a, b))
        e = link.setdefault(k, {"kinds": set(), "sim": 0.0})
        e["kinds"].add(kind)
        e["sim"] = max(e["sim"], sim, best.get(k, 0.0))

    note = ""
    if "name" in evidence:
        byname = defaultdict(list)
        for i, row in cand:
            if key := name_key(row):
                byname[key].append(i)
        for ids in byname.values():
            for a, b in combinations(ids[:NAME_BUCKET], 2):
                add(a, b, "name", 0.0)
    if "call" in evidence:
        if sum(len(_fids(row)) for _, row in cand) > CALL_MAX_FIDS:
            note = "Call position skipped: too many functions. Filter the rows first."
        else:
            callees = defaultdict(list)
            for i, callers in _caller_rows(r, cand, row_of).items():
                for c in callers:
                    callees[c].append(i)
            for c, ids in callees.items():
                if len(ids) > CALL_HUB:
                    continue
                for a, b in combinations(sorted(ids), 2):
                    sim = best.get((a, b), 0.0)
                    if sim >= min_link and (sim > 0 or min_link <= 0):
                        add(a, b, "call", sim)

    # Merge the strongest evidence first and never join rows that share a file.
    grp = {
        i: {
            "rows": [i],
            "kinds": set(),
            "sim": 0.0,
            "mask": row["mask"],
            "fmask": fmask(row),
        }
        for i, row in cand
    }
    for (a, b), e in sorted(
        link.items(), key=lambda kv: (-len(kv[1]["kinds"]), -kv[1]["sim"], kv[0])
    ):
        if a not in grp or b not in grp:
            continue
        ga, gb = grp[a], grp[b]
        if ga is not gb:
            if ga["fmask"] & gb["fmask"]:
                continue
            ga["rows"] += gb["rows"]
            ga["mask"] |= gb["mask"]
            ga["fmask"] |= gb["fmask"]
            ga["kinds"] |= gb["kinds"]
            ga["sim"] = max(ga["sim"], gb["sim"])
            for i in gb["rows"]:
                grp[i] = ga
        ga["kinds"] |= e["kinds"]
        ga["sim"] = max(ga["sim"], e["sim"])
    byrow, seen, units = dict(cand), set(), []
    for i, _ in cand:
        g = grp[i]
        if id(g) in seen:
            continue
        seen.add(id(g))
        if len(g["rows"]) == 1:
            units.append(byrow[i])
            continue
        members = [byrow[j] for j in sorted(g["rows"])]
        cells = {
            f"{k}:{c}": f
            for k, row in enumerate(members)
            for c, f in (row.get("files") or row["cells"]).items()
            if isinstance(f, str)
        }
        units.append(
            {
                "mask": g["mask"],
                "weight": sum(m["weight"] for m in members),
                "cells": cells,
                "_rows": members,
                "_kinds": g["kinds"],
                "_sim": g["sim"],
                "_idx": min(g["rows"]),
                "_fspan": _pop(g["fmask"]),
            }
        )
    return units, note


def _level(mode, units, ctx):
    """The groups of one level over `units`: [(meta, [(unit, extra)])]."""
    if mode == "prefix":
        return prefix_groups(units, ctx["n"], ctx["sort"], ctx["prefix_min"])
    if mode == "lane":
        return lane_groups(
            units, ctx["n"], ctx["sort"], ctx["lane_min"], ctx["lane_fit"]
        )
    return tag_groups(
        units, ctx["fmeta"], ctx["n"], ctx["sort"], ctx["axis"], ctx["axis_b"]
    )


def _graft(g, parent_id, parent_path):
    """Prefix a sub-group's id and path with its parent's, so ids are unique across the tree."""
    pre = (lambda i: f"{parent_id}/{i}") if parent_id else (lambda i: i)
    g["path"] = parent_path + [pre(p) for p in g["path"]]
    g["id"] = pre(g["id"])
    return g


SORT_COLS = ("name", "span", "weight", "support", "cohesion")


def _unit_sortkey(u, ctx):
    """The table's sort value of a unit: a merged function sorts like one function."""
    if "_rows" in u:
        rs = u["_rows"]
        vals = {
            "name": str(rs[0]["name"]).lower(),
            "span": u["_fspan"],
            "weight": max(r["weight"] for r in rs),
            "support": sum(r.get("support", 1.0) for r in rs) / len(rs),
            "cohesion": sum(r.get("cohesion", 1.0) for r in rs) / len(rs),
        }
    else:
        vals = {
            "name": str(u["name"]).lower(),
            "span": u.get("file_span", u.get("span", _pop(u["mask"]))),
            "weight": u["weight"],
            "support": u.get("support", 1.0),
            "cohesion": u.get("cohesion", 1.0),
        }
    col = ctx["sort_col"]
    # the table's default order: most files first, then the heaviest
    return vals[col] if col else (vals["span"], vals["weight"])


def _unit_rank(u, ctx):
    """The group rank of a unit, on the same measures a group is ranked on."""
    if "_rows" in u:
        meta = {"weight": u["weight"], "files": u["_fspan"], "rows": len(u["_rows"])}
    else:
        files = u.get("file_span", u.get("span", _pop(u["mask"])))
        meta = {"weight": u["weight"], "files": files, "rows": 1}
    return _rank(meta, ctx["sort"])


def _expand(units, ctx, parent_id, parent_path, carried):
    """Merged units become "same function" sub-groups; rows with no partner stay plain rows.

    -> sibling nodes, one per merged function or plain row, in the table's sort
    order, so a merged function sits among the plain rows where one function
    with its values would.
    """
    n, items = ctx["n"], []
    for u in units:
        if "_rows" not in u:
            head = {"id": parent_id, "path": list(parent_path), "headless": True}
            pair = (head, [(u, carried.get(id(u), {}))])
        else:
            members = u["_rows"]
            m = _meta(members, n)
            m.update(
                id=f"same:{u['_idx']}",
                kind="same",
                evidence=sorted(u["_kinds"]),
                link=round(u["_sim"], 3),
                names=list(dict.fromkeys(row["name"] for row in members))[:4],
                fids=list(dict.fromkeys(f for row in members for f in _fids(row))),
                cohesion=sum(row.get("cohesion", 1.0) for row in members)
                / len(members),
            )
            extra = carried.get(id(u), {})
            if extra.get("also", 0) > 1:
                m["also"] = extra["also"]
            if extra.get("sub"):
                m["sub"], m["miss"] = extra["sub"], extra["miss"]
            m["path"] = [m["id"]]
            pair = (_graft(m, parent_id, parent_path), [(row, {}) for row in members])
        node = {
            "container": False,
            "rank": _unit_rank(u, ctx),
            "key": _unit_sortkey(u, ctx),
            "pairs": [pair],
        }
        items.append(node)
    items.sort(key=lambda it: it["key"], reverse=ctx["reverse"])
    return items


def _nodes(units, levels, ctx, parent_id, parent_path, carried):
    """Sibling nodes under one parent: `{"container", "rank", "id", "pairs"}`.

    A container is a group with everything inside it; the other nodes are the
    merged functions and plain rows the levels did not place, one each.
    """
    if not levels:
        return _expand(units, ctx, parent_id, parent_path, carried)
    if levels[0] == "lane" and (
        ctx.get("lane_depth", 1) > 1 or ctx.get("lane_tier") or ctx.get("lane_fold")
    ):
        return _lane_nodes(units, levels, ctx, parent_id, parent_path, carried)
    nodes = []
    for g, items in _level(levels[0], units, ctx):
        extras = {id(u): {**carried.get(id(u), {}), **extra} for u, extra in items}
        if g.get("headless"):
            # what this level could not place continues with the next levels, beside its groups
            nodes += _nodes(
                [u for u, _ in items], levels[1:], ctx, parent_id, parent_path, extras
            )
            continue
        _graft(g, parent_id, parent_path)
        if len(levels) == 1 and not ctx["merge"]:
            pairs = [(g, [(u, extras[id(u)]) for u, _ in items])]
        else:
            inner = _nodes(
                [u for u, _ in items], levels[1:], ctx, g["id"], g["path"], extras
            )
            pairs = [(g, [])] + _flat(inner, ctx)
        nodes.append(
            {
                "container": True,
                "rank": _rank(g, ctx["sort"]),
                "id": g["id"],
                "pairs": pairs,
            }
        )
    return nodes


TIERS = (("core", "Core"), ("common", "Common"), ("rare", "Rare"))
FOLD_RANK = (-1, -1, -1)  # a fold sorts after its sibling lanes


def _gpop(node):
    return node["g"]["mask"].count("1")


def _node_units(node):
    units = [u for u, _ in node["items"]]
    for k in node["kids"]:
        units += _node_units(k)
    return units


def _unit_span(u):
    if "_rows" in u:
        return u["_fspan"]
    return u.get("file_span", u.get("span", _pop(u["mask"])))


def _lane_tiers(roots, loose, ctx):
    """Split lanes and loose units into core / common / rare bands by file count.

    -> (bands, bounds): per band the lane roots and loose units. A lane whose
    parent falls in a stronger band leaves it and heads its own band, with
    `under` (the parent's file count) for a chip; the parent's counts shrink.
    """
    n = ctx["n"]
    core = max(2, math.ceil(ctx["lane_tier"] * n))
    common = max(2, math.ceil(core / 2))
    tier = lambda p: 0 if p >= core else 1 if p >= common else 2
    bands = [{"lanes": [], "rows": []} for _ in TIERS]

    def split(node, t):
        keep = []
        for k in node["kids"]:
            kt = tier(_gpop(k))
            if kt == t:
                keep.append(k)
            else:
                k["g"]["under"] = _gpop(node)
                bands[kt]["lanes"].append(k)
            split(k, kt)
        node["kids"] = keep
        node["g"].update(_meta(_node_units(node), n))

    for r in roots:
        t = tier(_gpop(r))
        bands[t]["lanes"].append(r)
        split(r, t)
    for u in loose:
        bands[tier(_unit_span(u))]["rows"].append(u)
    return bands, [(core, n), (common, core - 1), (0, common - 1)]


def _lane_nodes(units, levels, ctx, parent_id, parent_path, carried):
    """The `lane` level with sub-lanes: each lane lists its own rows, then its sub-lanes.

    `lane_tier` wraps the lanes in core / common / rare bands; `lane_fold`
    gathers sibling lanes holding fewer rows than it into one collapsed
    `fold:` group.
    """
    roots, loose = lane_forest(
        units,
        ctx["n"],
        ctx["sort"],
        ctx["lane_min"],
        ctx["lane_fit"],
        ctx["lane_depth"],
        ctx.get("lane_link", 0.0),
    )
    n = ctx["n"]
    fold_min = ctx.get("lane_fold", 0)

    def with_fold(lanes, parent, key):
        """Built nodes of sibling lanes; two or more lanes under `fold_min` rows share one closed fold."""
        cut = [k for k in lanes if k["g"]["rows"] < fold_min]
        if len(cut) < 2:
            cut = []
        out = [build(k, parent["id"], parent["path"]) for k in lanes if k not in cut]
        if cut:
            fg = _meta([u for k in cut for u in _node_units(k)], n)
            fg.update(id=f"fold:{key}", kind="fold", lanes=len(cut))
            fg["path"] = [fg["id"]]
            _graft(fg, parent["id"], parent["path"])
            sub = [build(k, fg["id"], fg["path"]) for k in cut]
            out.append(
                {
                    "container": True,
                    "rank": FOLD_RANK,
                    "id": fg["id"],
                    "pairs": [(fg, [])] + _flat(sub, ctx),
                }
            )
        return out

    def build(node, pid, ppath):
        g, own = _graft(node["g"], pid, ppath), node["items"]
        extras = {id(u): {**carried.get(id(u), {}), **e} for u, e in own}
        kids = with_fold(node["kids"], g, g["mask"])
        if len(levels) == 1 and not ctx["merge"]:
            pairs = [(g, [(u, extras[id(u)]) for u, _ in own])] + _flat(kids, ctx)
        else:
            inner = _nodes(
                [u for u, _ in own], levels[1:], ctx, g["id"], g["path"], extras
            )
            pairs = [(g, [])] + _flat(inner + kids, ctx)
        return {
            "container": True,
            "rank": _rank(g, ctx["sort"]),
            "id": g["id"],
            "pairs": pairs,
        }

    if not ctx.get("lane_tier"):
        top = {"id": parent_id, "path": parent_path}
        nodes = with_fold(roots, top, "top")
        return nodes + _nodes(loose, levels[1:], ctx, parent_id, parent_path, carried)
    nodes = []
    bands, bounds = _lane_tiers(roots, loose, ctx)
    for t, (band, (lo, hi)) in enumerate(zip(bands, bounds)):
        if not band["lanes"] and not band["rows"]:
            continue
        lanes = _by_rank(band["lanes"], ctx["sort"], lambda k: k["g"])
        held = [u for k in lanes for u in _node_units(k)] + band["rows"]
        tg = _meta(held, n)
        key, label = TIERS[t]
        tg.update(id=f"tier:{key}", kind="tier", label=label, min=lo, max=hi)
        tg["path"] = [tg["id"]]
        _graft(tg, parent_id, parent_path)
        inner = with_fold(lanes, tg, key)
        inner += _nodes(band["rows"], levels[1:], ctx, tg["id"], tg["path"], carried)
        nodes.append(
            {
                "container": True,
                "rank": (len(TIERS) - t,),
                "id": tg["id"],
                # inside a band a lone function ranks with the lanes, by the same measure
                "pairs": [(tg, [])] + _flat(inner, {**ctx, "sort": "files"}),
            }
        )
    return nodes


def _flat(nodes, ctx):
    """Every sibling in one list.

    By default groups of every kind, merged functions and plain rows are ordered
    together by the group order (file coverage first), so a plain row that
    covers more files than a lane comes before it, at every level. With a table
    sort column the groups come first and the rows follow that column; the
    `lanes` order puts the groups first too, widest coverage first.
    """
    boxes = sorted((n for n in nodes if n["container"]), key=lambda n: n["id"])
    rest = [n for n in nodes if not n["container"]]
    if ctx["sort_col"] or ctx["sort"] == "lanes":
        boxes.sort(key=lambda n: n["rank"], reverse=True)
        if not ctx["sort_col"]:
            rest.sort(key=lambda n: n["rank"], reverse=True)
        ordered = boxes + rest
    else:
        ordered = sorted(boxes + rest, key=lambda n: n["rank"], reverse=True)
    return [pair for n in ordered for pair in n["pairs"]]


def _nest(units, levels, ctx, parent_id, parent_path, carried):
    """Groups of `levels[0]` over `units`, each one split again by the next level.

    A group with sub-levels, or with a merge on, lists no rows itself: its
    sub-groups follow it. `carried` holds the per-unit extras of outer levels.
    """
    return _flat(_nodes(units, levels, ctx, parent_id, parent_path, carried), ctx)


def _annotate(pairs, fmeta):
    """Lane groups get what names them: `sample` (heaviest names), `hint` (shared name prefix), `tags` (capa / yara findings) and `fids` (heaviest functions)."""
    rows = defaultdict(list)
    for g, items in pairs:
        for gid in g["path"]:
            rows[gid] += [r for r, _ in items]
    for g, _ in pairs:
        if g.get("kind") not in ("lane", "tier", "fold"):
            continue
        best = {}
        for r in rows[g["id"]]:
            if k := name_key(r):
                best[k] = max(best.get(k, 0), r["weight"])
        g["sample"] = sorted(best, key=best.get, reverse=True)[:10]
        if g["kind"] == "lane":
            heavy = sorted(rows[g["id"]], key=lambda r: r["weight"], reverse=True)
            g["fids"] = [f for r in heavy[:LANE_FIDS] for f in _fids(r)[:1]]
            found = Counter(
                t.split("#")[0]
                for r in rows[g["id"]]
                for f in _fids(r)[:1]
                for t in (fmeta.get(f) or {}).get("tags") or []
                if t.startswith(("capa:", "yara:"))
            )
            g["tags"] = [t for t, _ in found.most_common(5)]
        pre = Counter(p[0] + p[1] for k in best if (p := name_prefix(k)))
        if pre:
            top, c = pre.most_common(1)[0]
            if c >= 2 and c >= 0.4 * len(best):
                g["hint"] = top + "*"


LANE_NAMES_MAX = 40  # lanes named per request
LANE_FIDS = 6  # heaviest functions of a lane the model reads findings and strings from
LANE_CODE_FNS = 2  # ... and code from
_DECL = re.compile(r"^\s*[A-Za-z_][\w ]*[ *]\**\w+(\[\d+\])?;\s*$")
LANE_NAME_TTL = 30 * 86400
LANE_NAME_RETRY = 120  # seconds before a lane that failed is tried again
_LANE_POOL = None
_LANE_STATE = {}  # content key -> {"status": "pending"|"failed", "at": time}
_LANE_LOCK = threading.Lock()


def _lane_evidence(l):
    """The text the model names a lane from: names, findings, strings and a little decompiled code of its heaviest functions."""
    from bsimvis.app.routes.llm import get_code_for_llm
    from bsimvis.app.services import llm_tools, string_service
    from bsimvis.app.services.function_service import fetch_function_data
    from bsimvis.app.services.redis_client import get_redis

    out = [
        f"{l['rows']} functions found together in {l['files']} of {l['total']} files of a malware family."
    ]
    if l["sample"]:
        out.append("Function names: " + ", ".join(l["sample"]))
    tags, strings, code = list(l["tags"]), [], []
    for fid in l["fids"][:LANE_FIDS]:
        try:
            coll, md5, addr = llm_tools.parse_func_id(fid)
            _, _, meta, _ = fetch_function_data(coll, md5, addr, meta_only=True)
            for t in (meta or {}).get("tags") or []:
                t = t.split("#")[0]
                if t.startswith(("capa:", "yara:")) and t not in tags:
                    tags.append(t)
            for rec in string_service.function_strings(get_redis(), coll, md5, addr):
                v = " ".join(str(rec.get("value") or "").split())[:60]
                if len(v) >= 4 and v not in strings:
                    strings.append(v)
            if len(code) < LANE_CODE_FNS:
                src, _ = get_code_for_llm(fid)
                # the signature and body, without the block of variable declarations
                lines = [
                    x
                    for x in (src or {}).get("code", "").splitlines()
                    if x.strip() and not _DECL.match(x)
                ]
                if lines:
                    code.append("\n".join(lines[:14])[:600])
        except Exception:
            pass
    if tags:
        out.append("Findings: " + ", ".join(tags[:8]))
    if strings:
        out.append("Strings: " + " | ".join(strings[:10]))
    out += [f"Function {i + 1} code:\n{c}" for i, c in enumerate(code)]
    return "\n".join(out)[:2400]


def _name_lane(l, red):
    """Worker: name one lane and cache it."""
    from bsimvis.app.services.llm_service import llm_service

    name, _ = llm_service.name_lane(_lane_evidence(l))
    with _LANE_LOCK:
        if name:
            try:
                red.set(f"nway:lane:{l['key']}", name, ex=LANE_NAME_TTL)
            except Exception:
                pass
            _LANE_STATE.pop(l["key"], None)
        else:
            _LANE_STATE[l["key"]] = {"status": "failed", "at": time.time()}


def lane_names(lanes):
    """{'status': off|pending|ready, 'names': {lane id: name}} for lane groups.

    `lanes` is the lane group meta: id, files, total, rows, sample, hint, tags,
    fids. A name is cached by the lane's content (30 days). Lanes not seen yet
    are named by a small pool of workers, one short model call each, the
    biggest lane first; the answer lists the names ready so far and stays
    `pending` until every lane has one (or failed). Never waits for the model.
    """
    global _LANE_POOL
    import hashlib
    from concurrent.futures import ThreadPoolExecutor

    from bsimvis.app.services.config_service import config_service
    from bsimvis.app.services.redis_client import get_queue_redis

    clean = {}
    for l in lanes[:LANE_NAMES_MAX]:
        sample = [str(x)[:80] for x in (l.get("sample") or [])[:10]]
        hint = str(l.get("hint") or "")[:40]
        tags = [str(x)[:80] for x in (l.get("tags") or [])[:5]]
        fids = [str(x) for x in (l.get("fids") or [])[:LANE_FIDS]]
        if not (sample or hint or tags or fids):
            continue
        item = {
            "files": int(l["files"]),
            "total": int(l["total"]),
            "sample": sample,
            "hint": hint,
            "tags": tags,
            "fids": fids,
            "v": 2,  # bump when the prompt changes, so cached names are redone
        }
        key = hashlib.sha1(json.dumps(item, sort_keys=True).encode()).hexdigest()
        clean[str(l["id"])] = {**item, "rows": int(l["rows"]), "key": key}
    red = get_queue_redis()
    names, pending = {}, False
    todo = []
    for lid, l in clean.items():
        try:
            hit = red.get(f"nway:lane:{l['key']}")
        except Exception:
            hit = None
        if hit:
            names[lid] = hit if isinstance(hit, str) else hit.decode()
            continue
        with _LANE_LOCK:
            st = _LANE_STATE.get(l["key"])
            if st and st["status"] == "pending" and time.time() - st["at"] < 180:
                pending = True
            elif (
                st
                and st["status"] == "failed"
                and time.time() - st["at"] < LANE_NAME_RETRY
            ):
                pass
            else:
                _LANE_STATE[l["key"]] = {"status": "pending", "at": time.time()}
                todo.append(l)
                pending = True
    if todo and not _LANE_POOL:
        workers = int(config_service.get("llm.lane_parallel", 4))
        _LANE_POOL = ThreadPoolExecutor(max_workers=max(1, workers))
    for l in sorted(todo, key=lambda x: x["rows"], reverse=True):
        _LANE_POOL.submit(_name_lane, l, red)
    return {"status": "pending" if pending else "ready", "names": names}


def group_rows(rows, doc, r, mode, params):
    """Group `rows` (already filtered and sorted) -> (entries, groups, note).

    `mode` is a comma-separated chain of levels (`prefix`, `lane`, `tag`, `prefix,lane`) and
    may be empty when `group_merge` is set. See the module doc.
    """
    key = doc.get("key") and (
        doc["key"],
        mode,
        tuple(
            sorted(
                (k, v)
                for k, v in params.items()
                if k.startswith(("group_", "sort_"))
                and k not in ("group_open", "group_flip")
            )
        ),
        tuple(map(id, rows)),
    )
    hit = key and _CACHE.get(key)
    if hit:
        pairs, groups, note = hit
        return _entries(pairs, params), groups, note
    levels = [m for m in (mode or "").split(",") if m]
    if set(levels) - set(MODES) or len(set(levels)) != len(levels):
        raise BadParams(f"group must be {MODES}, separated by commas, each once")
    evidence = {e for e in (params.get("group_merge") or "").split(",") if e}
    if evidence - {"name", "call"}:
        raise BadParams("group_merge must be name, call or name,call")
    if not levels and not evidence:
        raise BadParams("group needs a level or group_merge")
    sort = params.get("group_sort") or "files"
    if sort not in SORTS:
        raise BadParams(f"group_sort must be one of {SORTS}")
    ctx = {
        "n": len(doc["columns"]),
        "sort": sort,
        "fmeta": doc["fmeta"],
        "axis": params.get("group_axis") or "origin",
        "axis_b": params.get("group_axis_b") or "",
        "merge": bool(evidence),
        "note": "",
        "prefix_min": PREFIX_MIN,
        "lane_min": LANE_MIN,
        "lane_fit": LANE_FIT,
        "lane_depth": 1,
        "lane_link": 0.0,
        "lane_tier": 0.0,
        "lane_fold": 0,
        "sort_col": (
            params.get("sort_col") if params.get("sort_col") in SORT_COLS else ""
        ),
        "reverse": params.get("sort_dir", "desc") != "asc"
        or params.get("sort_col") not in SORT_COLS,
    }
    if params.get("group_lane_fit"):
        try:
            ctx["lane_fit"] = float(params["group_lane_fit"])
        except ValueError:
            raise BadParams("group_lane_fit must be a number between 0 and 1")
        if not 0 < ctx["lane_fit"] <= 1:
            raise BadParams("group_lane_fit must be a number between 0 and 1")
    if params.get("group_lane_min"):
        try:
            ctx["lane_min"] = max(1, int(params["group_lane_min"]))
        except ValueError:
            raise BadParams("group_lane_min must be an integer")
    if params.get("group_lane_depth"):
        try:
            ctx["lane_depth"] = max(1, min(6, int(params["group_lane_depth"])))
        except ValueError:
            raise BadParams("group_lane_depth must be an integer")
    if params.get("group_lane_link"):
        try:
            ctx["lane_link"] = float(params["group_lane_link"])
        except ValueError:
            raise BadParams("group_lane_link must be a number between 0 and 1")
        if not 0 <= ctx["lane_link"] <= 1:
            raise BadParams("group_lane_link must be a number between 0 and 1")
    if params.get("group_lane_tier"):
        try:
            ctx["lane_tier"] = float(params["group_lane_tier"])
        except ValueError:
            raise BadParams("group_lane_tier must be a number between 0 and 1")
        if not 0 <= ctx["lane_tier"] <= 1:
            raise BadParams("group_lane_tier must be a number between 0 and 1")
    if params.get("group_lane_fold"):
        try:
            ctx["lane_fold"] = max(0, int(params["group_lane_fold"]))
        except ValueError:
            raise BadParams("group_lane_fold must be an integer")
    if params.get("group_prefix_min"):
        try:
            ctx["prefix_min"] = max(2, int(params["group_prefix_min"]))
        except ValueError:
            raise BadParams("group_prefix_min must be an integer")
    units = rows
    if evidence:
        try:
            min_link = float(params.get("group_min_link") or 0.3)
        except ValueError:
            raise BadParams("group_min_link must be a number")
        units, ctx["note"] = merge_units(rows, doc, r, evidence, min_link)
    pairs = _nest(units, levels, ctx, "", [], {})
    _annotate(pairs, doc["fmeta"])
    # `fcover`: the files the group holds, one 0/1 per file column. Child and twin columns stand for
    # several files, so the column-level `union` alone would show a half-covered column as full.
    fids = [c["id"] for c in doc.get("file_columns") or doc["columns"]]
    for g, _ in pairs:
        held = g.pop("_held", set())
        g["fcover"] = "".join("1" if f in held else "0" for f in fids)
    groups = {g["id"]: g for g, _ in pairs if not g.get("headless")}
    entries = _entries(pairs, params)
    if key:
        _CACHE.put(key, (pairs, groups, ctx["note"]))
    return entries, groups, ctx["note"]


def _entries(pairs, params):
    """Flat display list: a header entry `(id, None, {})` per visible group, then its rows when open.

    `group_open` (`none` default, or `all`) is the default state of every group
    and `group_flip` (JSON list of ids) the groups that differ from it. A group
    under a closed ancestor is not listed at all.
    """
    default = params.get("group_open") or "none"
    if default not in ("none", "all"):
        raise BadParams("group_open must be none or all")
    try:
        flip = json.loads(params.get("group_flip") or "[]")
    except ValueError:
        raise BadParams("group_flip must be a JSON list of group ids")
    if not isinstance(flip, list) or not all(isinstance(i, str) for i in flip):
        raise BadParams("group_flip must be a JSON list of group ids")
    flip = set(flip)

    # a tier starts open and a fold closed, whatever the default
    start = {
        g["id"]: kind == "tier"
        for g, _ in pairs
        if (kind := g.get("kind")) in ("tier", "fold")
    }

    def is_open(gid):
        return start.get(gid, default == "all") != (gid in flip)

    entries = []
    for g, items in pairs:
        if g.get("headless"):
            # rows of the parent group itself: no header of their own
            if all(is_open(a) for a in g["path"]):
                entries += [(g["id"], row, extra) for row, extra in items]
            continue
        if not all(is_open(a) for a in g["path"][:-1]):
            continue
        entries.append((g["id"], None, {}))
        if is_open(g["id"]):
            entries += [(g["id"], row, extra) for row, extra in items]
    return entries


def demo():
    def row(mask, weight, name="FUN_1", cells=None):
        return {
            "mask": mask,
            "weight": weight,
            "name": name,
            "cells": cells or {"A": f"f{mask}{name}"},
        }

    # lanes: a set of files shared by 2+ rows is a lane; the other rows are plain, not forced into one
    rows = [row(3, 10), row(3, 5), row(3, 1), row(1, 4), row(12, 2)]
    lanes = lane_groups(rows, 4, "weight")
    assert [g["id"] for g, _ in lanes] == ["lane:1100", ""], lanes
    assert lanes[0][0]["rows"] == 3 and lanes[1][0]["headless"]
    assert [r["mask"] for r, _ in lanes[1][1]] == [1, 12]
    # lane_min 1 gives every set of files a lane of its own
    assert len(lane_groups(rows, 4, "weight", 1)) == 3
    # near-bundle: 9 of a bundle's 10 files joins it (90%+), 5 of 10 does not
    full, nine, five = (1 << 10) - 1, (1 << 10) - 1 - (1 << 3), 0b11111
    near = lane_groups(
        [row(full, 5), row(full, 4), row(nine, 3), row(five, 2)], 10, "weight"
    )
    assert [g["id"] for g, _ in near][0] == "lane:" + "1" * 10 and near[0][0][
        "rows"
    ] == 3
    assert near[0][1][2][1] == {"sub": 1, "miss": "0001000000"}, near[0][1]
    assert near[-1][0]["headless"] and near[-1][1][0][0]["mask"] == five
    # a strict fit of 1 turns the exception off
    assert (
        near[0][0]["rows"] == 3
        and lane_groups(
            [row(full, 5), row(full, 4), row(nine, 3)], 10, "weight", 2, 1.0
        )[-1][0]["headless"]
    )

    # prefixes: first word of the name, auto names and one-word names have none
    assert name_prefix("attack_udp_flood") == ("attack", "_")
    assert name_prefix("__do_global") == ("do", "_")
    assert name_prefix("Foo::bar") == ("foo", ":")
    assert name_prefix("killerKill") == ("killer", "")
    assert name_prefix("main") is None and name_prefix("FUN_00101020") is None
    named = [
        row(1, 5, n) for n in ("attack_a", "attack_b", "killer_x", "main", "FUN_1")
    ]
    pg = prefix_groups(named, 4, "weight")
    assert [g["id"] for g, _ in pg] == ["prefix:attack_", ""], pg
    assert pg[0][0]["rows"] == 2 and pg[1][0]["headless"] and len(pg[1][1]) == 3
    # unplaced units carry on to the next level, beside the prefix groups
    nest_ctx = {
        "n": 4,
        "sort": "weight",
        "fmeta": {},
        "axis": "origin",
        "axis_b": "",
        "merge": False,
        "note": "",
        "prefix_min": 2,
        "lane_min": 1,
        "lane_fit": 0.9,
        "sort_col": "",
        "reverse": True,
    }
    deep = _nest(named, ["prefix", "lane"], nest_ctx, "", [], {})
    heads = [g["id"] for g, _ in deep if not g.get("headless")]
    # features order: the 3-unit lane (15 feat) outranks the prefix group (10 feat)
    assert heads == ["lane:1000", "prefix:attack_", "prefix:attack_/lane:1000"], heads
    assert sum(len(items) for _, items in deep) == 5

    # sibling groups of every kind are ordered together, by file coverage first
    kinds = [
        row(1, 100, "attack_a"),
        row(1, 100, "attack_b"),
        row(7, 1, "x1", {"A": "a1", "B": "b1", "C": "c1"}),
        row(7, 1, "x2", {"A": "a2", "B": "b2", "C": "c2"}),
    ]
    sctx = {**nest_ctx, "lane_min": 2}
    by_files = _nest(kinds, ["prefix", "lane"], {**sctx, "sort": "files"}, "", [], {})
    top = lambda pairs: [
        g["id"] for g, _ in pairs if g.get("path") and len(g["path"]) == 1
    ]
    assert top(by_files) == ["lane:1110", "prefix:attack_"], top(by_files)
    by_weight = _nest(kinds, ["prefix", "lane"], {**sctx, "sort": "weight"}, "", [], {})
    assert top(by_weight) == ["prefix:attack_", "lane:1110"], top(by_weight)

    # at every level: under a prefix, a plain row covering more files comes before a lane
    under = [
        row(1, 5, "attack_a"),
        row(1, 5, "attack_b"),
        row(7, 1, "attack_c"),
    ]
    inner = _nest(under, ["prefix", "lane"], {**sctx, "sort": "files"}, "", [], {})
    shape = ["row" if g.get("headless") else g["id"] for g, _ in inner]
    assert shape == ["prefix:attack_", "row", "prefix:attack_/lane:1000"], shape
    # ... while the lanes order lists the groups first, plain rows after
    lanes = _nest(under, ["prefix", "lane"], {**sctx, "sort": "lanes"}, "", [], {})
    shape = ["row" if g.get("headless") else g["id"] for g, _ in lanes]
    assert shape == ["prefix:attack_", "prefix:attack_/lane:1000", "row"], shape
    # ... and the other way round when features decide
    heavy = [row(1, 50, "attack_a"), row(1, 50, "attack_b"), row(7, 1, "attack_c")]
    flip = _nest(heavy, ["prefix", "lane"], {**sctx, "sort": "weight"}, "", [], {})
    assert ["row" if g.get("headless") else g["id"] for g, _ in flip] == [
        "prefix:attack_",
        "prefix:attack_/lane:1000",
        "row",
    ]

    # names: suffixes and auto names
    assert name_key({"name": "resolv_lookup.part.0"}) == "resolv_lookup"
    assert name_key({"name": "FUN_00401000"}) is None

    # merge: equal names join disjoint rows, overlapping rows stay apart
    doc = {
        "columns": [{"id": i} for i in "ABCD"],
        "edges": {},
        "fmeta": {},
        "rows": [
            row(3, 9, "init", {"A": "a1", "B": "b1"}),
            row(12, 8, "init", {"C": "c1", "D": "d1"}),
            row(2, 7, "init", {"B": "b2"}),
            row(1, 6, "other", {"A": "a2"}),
        ],
    }
    units, note = merge_units(doc["rows"], doc, None, {"name"}, 0.3)
    merged = [u for u in units if "_rows" in u]
    assert len(units) == 3 and len(merged) == 1 and note == ""
    assert merged[0]["mask"] == 15 and merged[0]["_kinds"] == {"name"}

    # a merged function is grouped by the lane of ALL its files, with its rows under it
    ctx = {
        "n": 4,
        "sort": "weight",
        "fmeta": {},
        "axis": "origin",
        "axis_b": "",
        "merge": True,
        "note": "",
        "lane_min": 1,
        "lane_fit": 0.9,
        "sort_col": "",
        "reverse": True,
    }
    got = _nest(units, ["lane"], ctx, "", [], {})
    heads = [g["id"] for g, _ in got if not g.get("headless")]
    assert heads == ["lane:1111", "lane:1111/same:0", "lane:0100", "lane:1000"], heads
    own = [(g["path"], len(items)) for g, items in got if g.get("headless")]
    assert own == [(["lane:0100"], 1), (["lane:1000"], 1)], own
    merged_pair = next(p for p in got if p[0]["id"] == "lane:1111/same:0")
    assert merged_pair[0]["path"] == ["lane:1111", "lane:1111/same:0"]
    assert merged_pair[0]["fids"] == ["a1", "b1", "c1", "d1"]
    assert merged_pair[0]["cohesion"] == 1.0
    assert len(merged_pair[1]) == 2 and got[0][1] == []
    # no levels: the merged function and the plain rows are ordered together, like functions
    top = _nest(units, [], ctx, "", [], {})
    assert [g["id"] for g, _ in top] == ["same:0", "", ""], top
    assert all(len(items) == 1 for _, items in top[1:])

    # the merged function (4 files, heaviest row 9) sorts by its own values, not first
    def order(**over):
        c = {**ctx, **over}
        return [
            g["id"] or items[0][0]["name"] + str(items[0][0]["weight"])
            for g, items in _nest(units, [], c, "", [], {})
        ]

    assert order(sort_col="weight", reverse=True) == ["same:0", "init7", "other6"]
    assert order(sort_col="weight", reverse=False) == ["other6", "init7", "same:0"]
    assert order(sort_col="span", reverse=False)[-1] == "same:0"
    assert order(sort_col="name", reverse=False)[0] == "same:0"  # "init" < "other"
    for mode in ("lane,lane", "bogus", "same"):
        try:
            group_rows(doc["rows"], doc, None, mode, {})
        except BadParams:
            continue
        raise AssertionError(mode)
    for params in ({}, {"group_merge": "bogus"}):
        try:
            group_rows(doc["rows"], doc, None, "", params)
        except BadParams:
            continue
        raise AssertionError(params)

    # tags: two tags with the same first two levels list the row once
    fmeta = {
        "f1": {"tags": ["yara:botnet:mirai:A", "yara:botnet:mirai:B"]},
        "f2": {"tags": ["yara:botnet:mirai:A"]},
    }
    trows = [
        row(1, 5, cells={"A": "f1"}),
        row(2, 3, cells={"A": "f2"}),
        row(4, 1, cells={"A": "f3"}),
    ]
    tree = {
        g["id"]: items for g, items in tag_groups(trows, fmeta, 4, "weight", "yara")
    }
    assert len(tree["tag:yara:botnet>yara:botnet:mirai"]) == 2
    assert len(tree["tag:yara:botnet"]) == 0 and len(tree["tag:(no tag)"]) == 1

    # open/closed: headers always, rows only for open groups, nothing under a closed parent
    pairs = [
        ({"id": "a", "path": ["a"]}, [("r1", {}), ("r2", {})]),
        ({"id": "b", "path": ["a", "b"]}, [("r3", {})]),
        ({"id": "c", "path": ["c"]}, [("r4", {})]),
    ]
    ids = lambda p: [(g, r) for g, r, _ in _entries(pairs, p)]
    assert ids({}) == [("a", None), ("c", None)]
    assert ids({"group_open": "all"}) == [
        ("a", None),
        ("a", "r1"),
        ("a", "r2"),
        ("b", None),
        ("b", "r3"),
        ("c", None),
        ("c", "r4"),
    ]
    assert ids({"group_flip": '["a"]'}) == [
        ("a", None),
        ("a", "r1"),
        ("a", "r2"),
        ("b", None),
        ("c", None),
    ]
    assert ids({"group_open": "all", "group_flip": '["a"]'}) == [
        ("a", None),
        ("c", None),
        ("c", "r4"),
    ]
    # nested lanes: 1111 holds 0011, which holds 0001; a row missing a file of 1111 joins it
    tree = [row(15, 4, "a"), row(15, 4, "b"), row(3, 2, "c"), row(3, 2, "d")]
    tree += [row(1, 1, "e"), row(1, 1, "f"), row(7, 1, "g")]
    roots, loose = lane_forest(tree, 4, "files", 2, 0.7, 3)
    assert [r["g"]["id"] for r in roots] == ["lane:1111"] and not loose
    sub = roots[0]["kids"]
    assert [k["g"]["id"] for k in sub] == ["lane:1100"], sub
    assert [k["g"]["id"] for k in sub[0]["kids"]] == ["lane:1000"]
    assert roots[0]["g"]["rows"] == 7 and sub[0]["g"]["rows"] == 4
    assert [u["name"] for u, e in roots[0]["items"] if e] == ["g"]
    # depth 2 lifts the deepest lane beside its grandparent's child
    shallow, _ = lane_forest(tree, 4, "files", 2, 0.7, 2)
    assert [k["g"]["id"] for k in shallow[0]["kids"]] == ["lane:1100", "lane:1000"]
    assert [r["g"]["id"] for r in lane_forest(tree, 4, "files", 2, 0.7, 1)[0]] == [
        "lane:1111",
        "lane:1100",
        "lane:1000",
    ]
    # mixed lanes: 0110 and 0011 share one file in three; 0001 nests under 0011
    mix = [row(6, 1, "m1"), row(6, 1, "m2"), row(12, 1, "m3"), row(12, 1, "m4")]
    mix += [row(8, 1, "m5"), row(8, 1, "m6")]
    roots, _ = lane_forest(mix, 4, "files", 2, 0.9, 2, 0.3)
    assert [r["g"]["id"] for r in roots] == ["mix:0111"], roots
    top = roots[0]["g"]
    assert top["core"] == "0010" and top["rows"] == 6 and top["lanes"] == 2
    assert sorted(k["g"]["id"] for k in roots[0]["kids"]) == ["lane:0011", "lane:0110"]
    flat_roots = lane_forest(mix, 4, "files", 2, 0.9, 2)[0]
    assert sorted(r["g"]["id"] for r in flat_roots) == ["lane:0011", "lane:0110"]
    assert not lane_forest(mix, 4, "files", 2, 0.9, 2, 0.5)[0][0]["g"]["id"].startswith(
        "mix:"
    )
    lctx = {
        **nest_ctx,
        "lane_min": 2,
        "lane_fit": 0.7,
        "lane_depth": 3,
        "sort": "files",
    }
    lt = _nest(tree, ["lane"], lctx, "", [], {})
    heads = [g["id"] for g, _ in lt]
    assert heads == [
        "lane:1111",
        "lane:1111/lane:1100",
        "lane:1111/lane:1100/lane:1000",
    ], heads
    assert lt[2][0]["path"] == heads
    assert sum(len(items) for _, items in lt) == 7
    # tiers wrap lanes by file count; a lane in a weaker band than its parent heads its own band
    tt = _nest(tree, ["lane"], {**lctx, "lane_tier": 0.75}, "", [], {})
    theads = [g["id"] for g, _ in tt]
    assert theads == [
        "tier:core",
        "tier:core/lane:1111",
        "tier:common",
        "tier:common/lane:1100",
        "tier:rare",
        "tier:rare/lane:1000",
    ], theads
    assert tt[3][0]["under"] == 4 and sum(len(i) for _, i in tt) == 7
    # a fold gathers sibling lanes under `lane_fold` rows into one closed group
    small = [row(15, 4, n) for n in "abc"] + [
        row(m, 2, n) for m, n in zip((3, 3, 12, 12), "defg")
    ]
    ft = _nest(small, ["lane"], {**lctx, "lane_fold": 3}, "", [], {})
    fheads = [g["id"] for g, _ in ft]
    assert fheads[:2] == ["lane:1111", "lane:1111/fold:1111"], fheads
    assert sorted(fheads[2:]) == [
        "lane:1111/fold:1111/lane:0011",
        "lane:1111/fold:1111/lane:1100",
    ], fheads
    assert ft[1][0]["kind"] == "fold" and ft[1][0]["lanes"] == 2
    assert sum(len(i) for _, i in ft) == 7
    # one small lane alone is not worth a fold
    assert "fold" not in str(
        _nest(small[:5], ["lane"], {**lctx, "lane_fold": 3}, "", [], {})
    )
    # in a band a lone function ranks with the lanes, not after them
    lone = small + [row(7, 9, "h")]
    bt = _nest(lone, ["lane"], {**lctx, "lane_tier": 0.5, "sort": "lanes"}, "", [], {})
    ids = [g["id"] for g, _ in bt if not g.get("headless")]
    assert ids[0] == "tier:core" and ids.index("tier:core/lane:1111") == 1, ids
    # annotate: a shared prefix becomes the lane's hint
    named = [row(15, 4, n) for n in ("attack_a", "attack_b", "attack_c", "main")]
    nt = _nest(named, ["lane"], {**lctx, "lane_depth": 1}, "", [], {})
    fm = {"fattack_a": {"tags": ["capa:network#send", "yara:x", "origin:lib"]}}
    named[0]["cells"] = {"A": "fattack_a"}
    _annotate(nt, fm)
    assert nt[0][0]["tags"] == ["capa:network", "yara:x"], nt[0][0]
    assert len(nt[0][0]["fids"]) == 4, nt[0][0]
    assert nt[0][0]["hint"] == "attack_*" and "main" in nt[0][0]["sample"], nt[0][0]
    print("nway_group_service demo ok")


if __name__ == "__main__":
    demo()
