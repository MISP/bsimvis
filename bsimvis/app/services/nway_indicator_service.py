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
from collections import OrderedDict

from bsimvis.app.services import nway_diff_service, string_service
from bsimvis.app.services.nway_diff_service import BadParams

REACH_SOFT = 25  # files outside the table at which the score halves
MIN_STRING_LEN = 6
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
