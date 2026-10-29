"""Indicators of an N-way table: how well a row singles out one family.

The *family* is the column(s) describing what you want to single out. The *contrast* is every other
file of the table, minus the *ignored* columns (a sibling variant, say). For a row
(a function block or a string) held by `held` files:

    cover  share of family files holding it
    leak   share of contrast files also holding it
    reach  files outside the table holding it (strings always, functions opt-in)
    score  cover x (1 - leak), softened by reach

`string_score` is the one scorer: the YARA workbench imports it too.
"""

import json
from collections import Counter, OrderedDict

from bsimvis.app.services import nway_diff_service, string_service
from bsimvis.app.services.nway_diff_service import BadParams

REACH_SOFT = 25  # files outside the table at which the score halves
MIN_STRING_LEN = 6
XREF_CAP = 300  # functions whose callers one page of strings reads
LANE_FIDS = 6  # heaviest strings of a lane that name it
INLINE_MIN_LEN = 4  # strings shown inside a function row
MAX_STRING_FILES = 200
_STRINGS = OrderedDict()  # file set -> string table, newest last
_STRINGS_KEPT = 6


def string_score(pos, targets, neg, others):
    """How well one string separates the sets: the share of target files that hold
    it times the share of other files that do not. Rates on both sides, so a small
    or sampled set weighs the same as a big one. 1.0 = every target and no other;
    0 = in no target, or in every other file."""
    if not targets:
        return None
    clean = 1 - min(1, neg / others) if others else 1
    return round(pos / targets * clean, 4)


def ids(text):
    """A comma separated query arg as a list."""
    return [t.strip() for t in (text or "").split(",") if t.strip()]


def context(doc, family, ignore=()):
    """File ids of the family and the contrast. `family` / `ignore` hold column ids
    (a file or a group of the table)."""
    cols = doc["file_columns"] or doc["columns"]
    members = {c["id"]: c.get("members") or [c["id"]] for c in doc["columns"]}
    for c in doc["columns"]:
        for p in c.get("path") or []:
            members.setdefault(p["id"], []).extend(c.get("members") or [c["id"]])
    members.update({c["id"]: [c["id"]] for c in cols})

    def files(wanted):
        out = set()
        for i in wanted:
            if i not in members:
                raise BadParams(f"unknown column {i!r}")
            out.update(members[i])
        return out

    fam, ignored = files(family), files(ignore)
    if not fam:
        raise BadParams("family needs at least one column")
    contrast = {c["id"] for c in cols} - fam - ignored
    return {"family": fam, "contrast": contrast, "ignored": ignored - fam}


def stats(held, ctx, reach=None):
    """Cover / leak / reach / score of a row held by the file ids `held`."""
    tp = len(held & ctx["family"])
    leak = len(held & ctx["contrast"])
    n_fam, n_con = len(ctx["family"]), len(ctx["contrast"])
    score = string_score(tp, n_fam, leak, n_con) or 0.0
    out = None
    if reach is not None:
        out = max(0, reach - tp - leak - len(held & ctx["ignored"]))
        score = round(score * REACH_SOFT / (REACH_SOFT + out), 4)
    return {
        "tp": tp,
        "family": n_fam,
        "cover": round(tp / n_fam, 4),
        "leak": leak,
        "contrast": n_con,
        "leak_rate": round(leak / n_con, 4) if n_con else 0.0,
        "reach": out,
        "score": score,
    }


def annotate(rows, ctx):
    """Copies of function rows carrying `stats` (the cached doc stays untouched)."""
    return [
        {
            **row,
            "stats": stats(
                {k for k, v in (row.get("files") or row["cells"]).items() if v}, ctx
            ),
        }
        for row in rows
    ]


def _groups(doc):
    return [
        {"id": c["id"], "members": c.get("members") or [c["id"]]}
        for c in doc["columns"]
    ]


def _read_strings(r, files, min_length):
    """`{string id: {value, encoding, section, tags, files: {file id: addr}}}`."""
    key = (tuple(sorted(files)), min_length)
    if key in _STRINGS:
        _STRINGS.move_to_end(key)
        return _STRINGS[key]
    pipe = r.pipeline(transaction=False)
    ordered = sorted(files)
    for fid in ordered:
        coll, md5 = fid.rsplit(":", 1)
        pipe.hvals(string_service.file_key(coll, md5))
    table = {}
    for fid, vals in zip(ordered, pipe.execute()):
        for raw in vals:
            try:
                rec = json.loads(raw)
            except ValueError:
                continue
            sid = rec.get("id")
            if not sid or rec.get("length", len(rec.get("value", ""))) < min_length:
                continue
            row = table.setdefault(
                sid,
                {
                    "value": rec.get("value", ""),
                    "encoding": rec.get("encoding", ""),
                    "section": rec.get("section", ""),
                    "tags": list(rec.get("tags") or []),
                    "files": {},
                },
            )
            row["files"].setdefault(fid, rec.get("addr", ""))
    _STRINGS[key] = table
    while len(_STRINGS) > _STRINGS_KEPT:
        _STRINGS.popitem(last=False)
    return table


def inline_strings(r, rows, top=6, cap=600):
    """Per row, the strings its functions reference: `{total, top: [{id, value, n, of}]}`
    with `n` the functions of the row that reference it and `of` the functions read."""
    fid_rows = [
        list(
            dict.fromkeys(
                f
                for f in (row.get("files") or row["cells"]).values()
                if isinstance(f, str)
            )
        )
        for row in rows
    ]
    fids = list(dict.fromkeys(f for fs in fid_rows for f in fs))[:cap]
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.smembers(f"{f}:strings")
    addrs = [
        sorted(a.decode() if isinstance(a, bytes) else str(a) for a in got)
        for got in pipe.execute()
    ]
    pipe = r.pipeline(transaction=False)
    for f, a in zip(fids, addrs):
        coll, rest = f.split(":func:", 1)
        if a:
            pipe.hmget(string_service.file_key(coll, rest.split(":")[0]), a)
    got = iter(pipe.execute())
    recs = {}
    for f, a in zip(fids, addrs):
        recs[f] = {}
        for raw in next(got) if a else []:
            try:
                rec = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if (
                rec.get("id")
                and rec.get("length", len(rec.get("value", ""))) >= INLINE_MIN_LEN
            ):
                recs[f][rec["id"]] = rec["value"]
    out = []
    for fs in fid_rows:
        fs = [f for f in fs if f in recs]
        count, value = {}, {}
        for f in fs:
            for sid, v in recs[f].items():
                count[sid] = count.get(sid, 0) + 1
                value[sid] = v
        best = sorted(count, key=lambda s: (-count[s], -len(value[s])))[:top]
        out.append(
            {
                "total": len(count),
                "top": [
                    {"id": s, "value": value[s][:80], "n": count[s], "of": len(fs)}
                    for s in best
                ],
            }
        )
    return out


def _txt(v):
    return v.decode() if isinstance(v, bytes) else str(v)


def _collections(doc):
    return sorted(
        {
            c["collection"]
            for c in doc["file_columns"] or doc["columns"]
            if c.get("collection")
        }
    )


def _fid_rows(doc):
    """Function id -> name of the function row holding it."""
    return {
        f: row["name"]
        for row in doc["rows"]
        for f in (row.get("files") or row["cells"]).values()
        if isinstance(f, str)
    }


def _referencing(r, doc, sids, fid_row):
    """`{string id: [function ids of the table that reference it]}`."""
    colls = _collections(doc)
    pipe = r.pipeline(transaction=False)
    for sid in sids:
        for coll in colls:
            pipe.smembers(f"{coll}:idx:string:value:{sid}")
    got = iter(pipe.execute())
    out = {}
    for sid in sids:
        fns = set()
        for _ in colls:
            fns |= {_txt(f) for f in next(got)}
        out[sid] = sorted(f for f in fns if f in fid_row)
    return out


def _top(count, n):
    return [{"name": k, "n": v} for k, v in count.most_common(n)]


def string_refs(r, doc, rows, top=4):
    """Give each string row `refs`: the function rows whose functions reference it
    (`direct`) and the ones that call those (`xref`), as `{name, n}` plus the
    number of functions that reference it."""
    fid_row = _fid_rows(doc)
    direct = _referencing(r, doc, [row["id"] for row in rows], fid_row)
    fids = list(dict.fromkeys(f for fs in direct.values() for f in fs))[:XREF_CAP]
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.smembers(f"{f}:callers")
    callers = {f: {_txt(c) for c in got} for f, got in zip(fids, pipe.execute())}
    for row in rows:
        fs = direct[row["id"]]
        own = Counter(fid_row[f] for f in fs)
        far = Counter(
            fid_row[c]
            for f in fs
            for c in callers.get(f, ())
            if c in fid_row and fid_row[c] not in own
        )
        row["refs"] = {
            "direct": _top(own, top),
            "xref": _top(far, top),
            "functions": len(fs),
        }


def lane_fids(r, doc, rows, groups, top=LANE_FIDS):
    """Name the string lanes from code: `fids` of a lane = a function using each of its
    heaviest strings. Tags and the name hint do not apply to strings."""
    fid_row = _fid_rows(doc)
    lanes = {}
    for g in groups.values():
        if g.get("kind") not in ("lane", "tier", "fold") or not g.get("mask"):
            continue
        m = sum(1 << i for i, ch in enumerate(g["mask"]) if ch == "1")
        best = sorted(
            (x for x in rows if not x["mask"] & ~m), key=lambda x: -x["weight"]
        )
        lanes[g["id"]] = [x["id"] for x in best[:top]]
    sids = list(dict.fromkeys(s for ss in lanes.values() for s in ss))
    direct = _referencing(r, doc, sids, fid_row)
    for g in groups.values():
        g["fids"] = list(
            dict.fromkeys(direct[s][0] for s in lanes.get(g["id"], []) if direct[s])
        )
        g["tags"] = []
        g.pop("hint", None)


def reach_of(r, sids, collections):
    """Files holding each string id, over the collections the table's files come from."""
    pipe = r.pipeline(transaction=False)
    for sid in sids:
        for coll in collections:
            pipe.scard(f"{coll}:idx:string:file:{sid}")
    counts = pipe.execute()
    n = len(collections)
    return {sid: sum(counts[i * n : (i + 1) * n]) for i, sid in enumerate(sids)}


def string_rows(r, doc, ctx=None, min_length=MIN_STRING_LEN):
    """The table's strings as N-way rows over the doc's columns, with `stats` when a
    family is set. Rows hold `cells` (per column `{present, total, on}`), `files` (file
    id -> address) and no reach yet (`page_reach`)."""
    cols = doc["file_columns"] or doc["columns"]
    files = [c["id"] for c in cols]
    if len(files) > MAX_STRING_FILES:
        raise BadParams(
            f"too many files for strings: {len(files)} > {MAX_STRING_FILES}"
        )
    table = _read_strings(r, files, min_length)
    base = [
        {
            "id": sid,
            "name": s["value"],
            "encoding": s["encoding"],
            "section": s["section"],
            "tags": s["tags"],
            "cells": s["files"],
            "span": len(s["files"]),
            "weight": len(s["value"]),
            "library": "origin:lib" in s["tags"],
            "is_string": True,
        }
        for sid, s in table.items()
    ]
    rows = nway_diff_service.project_rows(base, _groups(doc), 0.5)
    for row in rows:
        row["span"] = row["file_span"]
        if ctx:
            row["stats"] = stats(set(row["files"]), ctx)
    return rows


def page_reach(r, doc, rows, ctx):
    """Fill `reach` and the softened score on the rows about to be shown."""
    colls = sorted(
        {
            c["collection"]
            for c in doc["file_columns"] or doc["columns"]
            if c.get("collection")
        }
    )
    if not colls:
        return
    reach = reach_of(r, [row["id"] for row in rows], colls)
    for row in rows:
        row["stats"] = stats(set(row["files"]), ctx, reach[row["id"]])


def demo():
    ctx = {"family": {"a", "b"}, "contrast": {"c", "d", "e"}, "ignored": {"f"}}
    s = stats({"a", "b"}, ctx)
    assert (s["cover"], s["leak"], s["score"]) == (1.0, 0, 1.0)
    s = stats({"a", "c", "f"}, ctx)
    assert (s["cover"], s["leak"], s["score"]) == (0.5, 1, round(0.5 * (1 - 1 / 3), 4))
    assert stats({"a", "b"}, ctx, reach=2 + 25)["score"] == 0.5
    assert string_score(1, 0, 0, 5) is None


if __name__ == "__main__":
    demo()
    print("ok")
