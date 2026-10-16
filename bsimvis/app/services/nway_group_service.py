"""Group the rows of the N-way function table.

Three modes, all over the rows the table already filtered and sorted:

- `lane`: rows bundled by the set of files they appear in (`row["mask"]`).
- `tag`: rows nested under the tags of one axis, optionally crossed with a second.
- `same`: rows that are probably one function the matcher split, joined by a
  shared name and/or a shared caller. Rows that share a file are never joined.

`group_rows` returns `(entries, groups, note)`. `entries` is the flat list in
display order, `(group id, row, extra fields)`; a closed group is one entry with
no row, and a tag row can appear under several groups. `groups` is `{id: meta}`
in display order; `meta["path"]` lists the ancestor ids a client prints a header
for. Masks go out as 0/1 strings over the table's columns so JavaScript never
sees a big int.
"""

import json
import re
from collections import Counter, defaultdict
from itertools import combinations

from bsimvis.app.services.bin_sim_tags import AXES, merge_tag_fields, tag_axis
from bsimvis.app.services.nway_diff_service import _DEFAULT_NAME, BadParams
from bsimvis.app.services.tag_taxonomy import tag_body, tag_prefixes

MODES = ("tag", "same", "lane")
SORTS = ("weight", "rows", "files")
LANES = 12  # lanes kept before the rest go to "other"
TAG_FANOUT = 3  # tags of one axis a row is listed under, most shared first
NAME_BUCKET = 12  # rows sharing one name that are paired up
CALL_HUB = 25  # a caller with more callee rows than this says nothing
CALL_MAX_FIDS = 30000  # call position reads one Redis set per function
UNTAGGED = "(no tag)"
# ponytail: the grouping is recomputed on every page request; cache it on the
# doc key if the scroll-append calls get slow.

_SUFFIX = re.compile(r"\.(part|isra|constprop|cold|llvm)(\.\d+)?$")


def _bits(mask, n):
    return "".join("1" if mask >> i & 1 else "0" for i in range(n))


def _pop(mask):
    return bin(mask).count("1")


def _fids(row):
    return [
        f for f in (row.get("files") or row["cells"]).values() if isinstance(f, str)
    ]


def _meta(rows, n):
    union = 0
    for r in rows:
        union |= r["mask"]
    return {
        "rows": len(rows),
        "weight": sum(r["weight"] for r in rows),
        "union": _bits(union, n),
        "files": _pop(union),
    }


def _rank(g, sort):
    return {"weight": g["weight"], "rows": g["rows"], "files": g["files"]}[sort]


def _order(groups, sort):
    return sorted(groups, key=lambda g: (-_rank(g, sort), g["id"]))


def lane_groups(rows, n, sort):
    """One group per bundle of rows with the same file set, plus "other"."""
    count = Counter(r["mask"] for r in rows)
    lanes = [m for m, c in count.most_common() if c >= 2][:LANES]
    members = defaultdict(list)
    for r in rows:
        lane = r["mask"] if r["mask"] in lanes else None
        if lane is None:
            # a subset of a bundle's files joins the smallest bundle that holds it
            fits = [m for m in lanes if not r["mask"] & ~m]
            lane = min(fits, key=_pop) if fits else None
        members[lane].append((r, lane is not None and lane != r["mask"]))
    out = []
    for lane, items in members.items():
        g = _meta([r for r, _ in items], n)
        g.update(
            id=f"lane:{_bits(lane, n)}" if lane is not None else "lane:other",
            kind="lane" if lane is not None else "other",
            mask=_bits(lane, n) if lane is not None else None,
            subset=sum(1 for _, s in items if s),
            path=[],
        )
        g["path"] = [g["id"]]
        out.append((g, [(r, {"sub": True} if s else {}) for r, s in items]))
    out.sort(key=lambda p: (p[0]["kind"] == "other", -_rank(p[0], sort), p[0]["id"]))
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


def same_groups(rows, doc, r, n, sort, evidence, min_link):
    """Rows joined by a shared name and/or a shared caller; see the module doc."""
    all_rows = doc["rows"]
    row_of = {f: i for i, row in enumerate(all_rows) for f in _fids(row)}
    at = {id(row): i for i, row in enumerate(all_rows)}
    cand = [(at[id(row)], row) for row in rows if id(row) in at]
    best = _best_links(doc["edges"], row_of)
    link = {}  # (row, row) -> {"kinds", "sim"}

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
        i: {"rows": [i], "kinds": set(), "sim": 0.0, "mask": row["mask"]}
        for i, row in cand
    }
    for (a, b), e in sorted(
        link.items(), key=lambda kv: (-len(kv[1]["kinds"]), -kv[1]["sim"], kv[0])
    ):
        if a not in grp or b not in grp:
            continue
        ga, gb = grp[a], grp[b]
        if ga is not gb:
            if ga["mask"] & gb["mask"]:
                continue
            ga["rows"] += gb["rows"]
            ga["mask"] |= gb["mask"]
            ga["kinds"] |= gb["kinds"]
            ga["sim"] = max(ga["sim"], gb["sim"])
            for i in gb["rows"]:
                grp[i] = ga
        ga["kinds"] |= e["kinds"]
        ga["sim"] = max(ga["sim"], e["sim"])
    seen, sets, rest = set(), [], []
    for i, _ in cand:
        g = grp[i]
        if id(g) in seen:
            continue
        seen.add(id(g))
        (sets if len(g["rows"]) > 1 else rest).append(g)
    byrow = dict(cand)
    out = []
    for g in sets:
        members = [byrow[i] for i in sorted(g["rows"])]
        m = _meta(members, n)
        m.update(
            id=f"same:{min(g['rows'])}",
            kind="same",
            evidence=sorted(g["kinds"]),
            link=round(g["sim"], 3),
            names=list(dict.fromkeys(row["name"] for row in members))[:4],
        )
        m["path"] = [m["id"]]
        out.append((m, [(row, {}) for row in members]))
    out.sort(key=lambda p: (-len(p[0]["evidence"]), -_rank(p[0], sort), p[0]["id"]))
    if rest:
        members = [byrow[g["rows"][0]] for g in rest]
        m = _meta(members, n)
        m.update(id="same:rest", kind="rest", path=["same:rest"])
        out.append((m, [(row, {}) for row in members]))
    return out, note


def group_rows(rows, doc, r, mode, params):
    """Group `rows` (already filtered and sorted) -> (entries, groups, note)."""
    if mode not in MODES:
        raise BadParams(f"group must be one of {MODES}")
    sort = params.get("group_sort") or "weight"
    if sort not in SORTS:
        raise BadParams(f"group_sort must be one of {SORTS}")
    n = len(doc["columns"])
    note = ""
    if mode == "lane":
        pairs = lane_groups(rows, n, sort)
    elif mode == "tag":
        pairs = tag_groups(
            rows,
            doc["fmeta"],
            n,
            sort,
            params.get("group_axis") or "origin",
            params.get("group_axis_b") or "",
        )
    else:
        evidence = {e for e in (params.get("group_ev") or "name,call").split(",") if e}
        if evidence - {"name", "call", "none"}:
            raise BadParams("group_ev must be name, call, name,call or none")
        try:
            min_link = float(params.get("group_min_link") or 0.3)
        except ValueError:
            raise BadParams("group_min_link must be a number")
        pairs, note = same_groups(rows, doc, r, n, sort, evidence, min_link)
    return _entries(pairs, params), {g["id"]: g for g, _ in pairs}, note


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

    def is_open(gid):
        return (default == "all") != (gid in flip)

    entries = []
    for g, items in pairs:
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

    # lanes: three rows share 0b0011, one row (0b0001) is a subset, one fits no lane
    rows = [row(3, 10), row(3, 5), row(3, 1), row(1, 4), row(12, 2)]
    lanes = lane_groups(rows, 4, "weight")
    assert (
        lanes[0][0]["id"] == "lane:1100"
        and lanes[0][0]["rows"] == 4
        and lanes[0][0]["subset"] == 1
    )
    assert lanes[1][0]["kind"] == "other" and lanes[1][0]["rows"] == 1

    # names: suffixes and auto names
    assert name_key({"name": "resolv_lookup.part.0"}) == "resolv_lookup"
    assert name_key({"name": "FUN_00401000"}) is None

    # same: equal names join disjoint rows, overlapping rows stay apart
    doc = {
        "columns": [0, 0, 0, 0],
        "edges": {},
        "rows": [
            row(3, 9, "init", {"A": "a1", "B": "b1"}),
            row(12, 8, "init", {"C": "c1", "D": "d1"}),
            row(2, 7, "init", {"B": "b2"}),
            row(1, 6, "other", {"A": "a2"}),
        ],
    }
    pairs, note = same_groups(doc["rows"], doc, None, 4, "weight", {"name"}, 0.3)
    sets = [g for g, _ in pairs if g["kind"] == "same"]
    assert len(sets) == 1 and sets[0]["rows"] == 2 and sets[0]["union"] == "1111", sets
    assert sets[0]["evidence"] == ["name"] and note == ""
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
    print("nway_group_service demo ok")


if __name__ == "__main__":
    demo()
