"""Group the rows of the N-way function table.

Two steps over the rows the table already filtered and sorted:

1. Merge (`group_merge`, optional): rows that are probably one function the
   matcher split are joined by a shared name and/or a shared caller. Rows that
   share a file are never joined. A merged function covers the union of its
   rows' files.
2. Group (`group`, optional, up to two levels): `lane` bundles by the set of
   files, `tag` nests under the tags of one axis (optionally crossed with a
   second). With a merge on, the unit being grouped is the merged function, so
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
import re
from collections import Counter, defaultdict
from itertools import combinations

from bsimvis.app.services.bin_sim_tags import AXES, merge_tag_fields, tag_axis
from bsimvis.app.services.nway_diff_service import _DEFAULT_NAME, BadParams
from bsimvis.app.services.tag_taxonomy import tag_body, tag_prefixes

MODES = ("tag", "lane")
SORTS = ("weight", "rows", "files")
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
    """Totals of units: a unit is a row or a merged function (`_rows`)."""
    union = 0
    for r in rows:
        union |= r["mask"]
    return {
        "rows": sum(len(r["_rows"]) if "_rows" in r else 1 for r in rows),
        "weight": sum(r["weight"] for r in rows),
        "union": _bits(union, n),
        "files": _pop(union),
    }


def _rank(g, sort):
    return {"weight": g["weight"], "rows": g["rows"], "files": g["files"]}[sort]


def _order(groups, sort):
    return sorted(groups, key=lambda g: (-_rank(g, sort), g["id"]))


def lane_groups(rows, n, sort):
    """One group per set of files. A unit whose files are a subset of a shared bundle joins it."""
    count = Counter(r["mask"] for r in rows)
    bundles = [m for m, c in count.items() if c >= 2]
    members = defaultdict(list)
    for r in rows:
        lane = r["mask"]
        if lane not in bundles:
            # a subset of a bundle's files joins the smallest bundle that holds it
            fits = [m for m in bundles if not lane & ~m]
            if fits:
                lane = min(fits, key=_pop)
        members[lane].append((r, lane != r["mask"]))
    out = []
    for lane, items in members.items():
        g = _meta([r for r, _ in items], n)
        g.update(
            id=f"lane:{_bits(lane, n)}",
            kind="lane",
            mask=_bits(lane, n),
            subset=sum(1 for _, s in items if s),
        )
        g["path"] = [g["id"]]
        out.append((g, [(r, {"sub": True} if s else {}) for r, s in items]))
    out.sort(key=lambda p: (-_rank(p[0], sort), p[0]["id"]))
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


def merge_units(rows, doc, r, evidence, min_link):
    """Join rows that are probably one function -> (units, note).

    A unit is a row left alone, or a pseudo-row for 2+ joined rows: `mask` is
    the union of their files, `_rows` the rows, `_kinds` the evidence.
    """
    all_rows = doc["rows"]
    row_of = {f: i for i, row in enumerate(all_rows) for f in _fids(row)}
    at = {id(row): i for i, row in enumerate(all_rows)}
    cand = [(at[id(row)], row) for row in rows if id(row) in at]
    best = _best_links(doc["edges"], row_of)
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
            }
        )
    return units, note


def _level(mode, units, ctx):
    """The groups of one level over `units`: [(meta, [(unit, extra)])]."""
    if mode == "lane":
        return lane_groups(units, ctx["n"], ctx["sort"])
    return tag_groups(
        units, ctx["fmeta"], ctx["n"], ctx["sort"], ctx["axis"], ctx["axis_b"]
    )


def _graft(g, parent_id, parent_path):
    """Prefix a sub-group's id and path with its parent's, so ids are unique across the tree."""
    pre = (lambda i: f"{parent_id}/{i}") if parent_id else (lambda i: i)
    g["path"] = parent_path + [pre(p) for p in g["path"]]
    g["id"] = pre(g["id"])
    return g


def _expand(units, ctx, parent_id, parent_path, carried):
    """Merged units become "same function" sub-groups, the rest one "no partner" group."""
    n, pairs = ctx["n"], []
    for u in units:
        if "_rows" not in u:
            continue
        members = u["_rows"]
        m = _meta(members, n)
        m.update(
            id=f"same:{u['_idx']}",
            kind="same",
            evidence=sorted(u["_kinds"]),
            link=round(u["_sim"], 3),
            names=list(dict.fromkeys(row["name"] for row in members))[:4],
        )
        extra = carried.get(id(u), {})
        if extra.get("sub"):
            m["sub"] = True
        if extra.get("also", 0) > 1:
            m["also"] = extra["also"]
        m["path"] = [m["id"]]
        pairs.append(
            (_graft(m, parent_id, parent_path), [(row, {}) for row in members])
        )
    pairs.sort(
        key=lambda p: (-len(p[0]["evidence"]), -_rank(p[0], ctx["sort"]), p[0]["id"])
    )
    single = [u for u in units if "_rows" not in u]
    if single:
        # rows with no partner sit directly under the parent, after the merged functions
        head = {"id": parent_id, "path": list(parent_path), "headless": True}
        pairs.append((head, [(u, carried.get(id(u), {})) for u in single]))
    return pairs


def _nest(units, levels, ctx, parent_id, parent_path, carried):
    """Groups of `levels[0]` over `units`, each one split again by the next level.

    A group with sub-levels, or with a merge on, lists no rows itself: its
    sub-groups follow it. `carried` holds the per-unit extras of outer levels.
    """
    if not levels:
        return _expand(units, ctx, parent_id, parent_path, carried)
    out = []
    for g, items in _level(levels[0], units, ctx):
        _graft(g, parent_id, parent_path)
        extras = {id(u): {**carried.get(id(u), {}), **extra} for u, extra in items}
        if len(levels) == 1 and not ctx["merge"]:
            out.append((g, [(u, extras[id(u)]) for u, _ in items]))
        else:
            out.append((g, []))
            out += _nest(
                [u for u, _ in items], levels[1:], ctx, g["id"], g["path"], extras
            )
    return out


def group_rows(rows, doc, r, mode, params):
    """Group `rows` (already filtered and sorted) -> (entries, groups, note).

    `mode` is a comma-separated chain of levels (`lane`, `tag`, `lane,tag`) and
    may be empty when `group_merge` is set. See the module doc.
    """
    levels = [m for m in (mode or "").split(",") if m]
    if set(levels) - set(MODES) or len(set(levels)) != len(levels):
        raise BadParams(
            f"group must be {MODES}, one or both separated by commas, each once"
        )
    evidence = {e for e in (params.get("group_merge") or "").split(",") if e}
    if evidence - {"name", "call"}:
        raise BadParams("group_merge must be name, call or name,call")
    if not levels and not evidence:
        raise BadParams("group needs a level or group_merge")
    sort = params.get("group_sort") or "weight"
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
    }
    units = rows
    if evidence:
        try:
            min_link = float(params.get("group_min_link") or 0.3)
        except ValueError:
            raise BadParams("group_min_link must be a number")
        units, ctx["note"] = merge_units(rows, doc, r, evidence, min_link)
    pairs = _nest(units, levels, ctx, "", [], {})
    groups = {g["id"]: g for g, _ in pairs if not g.get("headless")}
    return _entries(pairs, params), groups, ctx["note"]


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

    # lanes: three rows share 0b0011, one row (0b0001) is a subset, one fits no lane
    rows = [row(3, 10), row(3, 5), row(3, 1), row(1, 4), row(12, 2)]
    lanes = lane_groups(rows, 4, "weight")
    assert lanes[0][0]["id"] == "lane:1100" and lanes[0][0]["rows"] == 4
    assert lanes[0][0]["subset"] == 1
    # no catch-all: a row that fits no bundle is a lane of its own
    assert lanes[1][0]["id"] == "lane:0011" and lanes[1][0]["rows"] == 1

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
    }
    got = _nest(units, ["lane"], ctx, "", [], {})
    heads = [g["id"] for g, _ in got if not g.get("headless")]
    assert heads == ["lane:1111", "lane:1111/same:0", "lane:0100", "lane:1000"], heads
    own = [(g["path"], len(items)) for g, items in got if g.get("headless")]
    assert own == [(["lane:0100"], 1), (["lane:1000"], 1)], own
    merged_pair = next(p for p in got if p[0]["id"] == "lane:1111/same:0")
    assert merged_pair[0]["path"] == ["lane:1111", "lane:1111/same:0"]
    assert len(merged_pair[1]) == 2 and got[0][1] == []
    # no levels: merged functions first, then the rows with no partner at the top
    top = _nest(units, [], ctx, "", [], {})
    assert [g["id"] for g, _ in top] == ["same:0", ""] and top[1][0]["headless"]
    assert len(top[1][1]) == 2
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
    print("nway_group_service demo ok")


if __name__ == "__main__":
    demo()
