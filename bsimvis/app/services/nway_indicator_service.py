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
import re
from collections import Counter, OrderedDict

from bsimvis.app.services import nway_diff_service, string_service
from bsimvis.app.services.nway_diff_service import BadParams

REACH_SOFT = 25  # files outside the table at which the score halves
MIN_STRING_LEN = 6
LANE_FIDS = 6  # heaviest strings of a lane that name it
REF_FIDS = 6  # functions of a row kept to draw its signature
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
    """Copies of function rows carrying `stats` (the cached doc stays untouched).
    `_at` is the row's index in `doc["rows"]`, for code that matches copies back."""
    return [
        {
            **row,
            "_at": i,
            "stats": stats(
                {k for k, v in (row.get("files") or row["cells"]).items() if v}, ctx
            ),
        }
        for i, row in enumerate(rows)
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


_CONST_RE = re.compile(r"\(const, 0x([0-9a-f]+), ([1248])\)")
_TRIVIAL = {0xFFFFFFFF, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFFFFFFFFFF}


def consts_in(raw):
    """Non-trivial `(value, size)` constants of one function's stored pcode (`vec:meta`)."""
    out = set()
    for v, size in _CONST_RE.findall(raw or ""):
        v, size = int(v, 16), int(size)
        # small negative offsets (0xfffffff8) are stack/struct noise
        if 0xFFFF < v < (1 << 8 * size) - 0xFFFF and v not in _TRIVIAL:
            out.add((v, size))
    return out


def _string_name(value):
    """A string as an identifier: `/dev/watchdog` -> `dev_watchdog`; None when too thin."""
    name = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_")[:40].strip("_")
    return name if len(name) >= 5 and sum(c.isalpha() for c in name) >= 3 else None


_PATH_NOISE = {
    "usr",
    "bin",
    "etc",
    "lib",
    "var",
    "tmp",
    "www",
    "com",
    "org",
    "http",
    "https",
}


def _common_token(values, need=3):
    """The word most of a row's strings share (`/proc/stat`, `/proc/net/tcp` -> `proc`):
    in at least `need` strings and half of them, or None."""
    count = Counter()
    for v in values:
        count.update(
            {
                t.lower()
                for t in re.findall(r"[A-Za-z]{3,}", v)
                if t.lower() not in _PATH_NOISE
            }
        )
    if not count:
        return None
    token, n = max(count.items(), key=lambda kv: (kv[1], len(kv[0])))
    return token if n >= need and n * 2 >= len(values) else None


def name_by_strings(r, rows):
    """Name rows that have no name at all (FUN_xxx, no tag, no cluster name) after the
    word most of their strings share (`uses_proc`), else the string their functions
    share that few other functions use (`uses_dev_watchdog`); `name_from` = string.
    """
    todo = [
        row
        for row in rows
        if nway_diff_service._DEFAULT_NAME.match(row["name"])
        and row.get("name_from") not in ("tag", "cluster")
        and not row.get("user_named")
    ]
    if not todo:
        return
    pipe, order, best = r.pipeline(transaction=False), [], {}
    for row, found in zip(todo, inline_strings(r, todo, top=20)):
        fids = _row_fid_lists([row])[0]
        if not fids:
            continue
        if token := _common_token([s["value"] for s in found["top"]]):
            best[id(row)] = ((0, 0), row, token)
            continue
        coll = fids[0].split(":func:", 1)[0]
        for s in found["top"]:
            if name := _string_name(s["value"]):
                pipe.scard(f"{coll}:idx:string:value:{s['id']}")
                order.append((row, name, len(fids)))
    for (row, name, mine), held in zip(order, pipe.execute()):
        # discriminating: the string is (almost) only used by this row's own functions
        if held <= 2 * mine and (
            id(row) not in best or (held, -len(name)) < best[id(row)][0]
        ):
            best[id(row)] = ((held, -len(name)), row, name)
    for _, row, name in best.values():
        row["name_was"], row["name"], row["name_from"] = (
            row["name"],
            f"uses_{name}",
            "string",
        )


def _fid_at(doc):
    """Function id -> index of the table row holding it (kept on the cached doc)."""
    if "_fid_at" not in doc:
        doc["_fid_at"] = {
            f: i
            for i, row in enumerate(doc["rows"])
            for f in (row.get("files") or row["cells"]).values()
            if isinstance(f, str)
        }
    return doc["_fid_at"]


def _row_fid_lists(rows):
    return [
        list(
            dict.fromkeys(
                f
                for f in (row.get("files") or row["cells"]).values()
                if isinstance(f, str)
            )
        )
        for row in rows
    ]


def inline_links(r, doc, rows, top=4, cap=600):
    """Per row, the other table rows it calls and is called by:
    `{callers, callees}`, each `{total, top: [{name, n, of, ext}]}`. A neighbor is a
    whole row (the same function in several files counts once), `n` the functions of
    this row linked to it and `of` the functions read. Calls out of the binary are
    `ext` callees, named by what they call."""
    at = _fid_at(doc)
    fid_rows = _row_fid_lists(rows)
    fids = list(dict.fromkeys(f for fs in fid_rows for f in fs))[:cap]
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.smembers(f"{f}:callers")
        pipe.smembers(f"{f}:callees")
    got = iter(pipe.execute())
    links = {f: (next(got), next(got)) for f in fids}
    out = []
    for fs in fid_rows:
        me = at.get(fs[0]) if fs else None
        res = {}
        for side, role in enumerate(("callers", "callees")):
            count, name, ext, var = Counter(), {}, set(), {}
            read = [f for f in fs if f in links]
            for f in read:
                keys = set()
                for m in links[f][side]:
                    m = _txt(m)
                    if m.startswith("ext:"):
                        key, name[key] = m, m[4:]
                        ext.add(key)
                    elif (i := at.get(m)) is not None and i != me:
                        key, name[key] = f"row:{i}", doc["rows"][i]["name"]
                        var[key] = doc["rows"][i].get("variant")
                    else:
                        continue
                    keys.add(key)
                count.update(keys)
            best = sorted(count, key=lambda k: (-count[k], name[k]))[:top]
            res[role] = {
                "total": len(count),
                "top": [
                    {
                        "name": name[k],
                        "n": count[k],
                        "of": len(read),
                        "ext": k in ext,
                        "variant": var.get(k),
                    }
                    for k in best
                ],
            }
        out.append(res)
    return out


def inline_consts(r, rows, top=4, cap=600):
    """Per row, the pcode constants at least half of its functions use:
    `{total, top: [{value, n, of}]}` (`n` = functions of the row that use it)."""
    fid_rows = _row_fid_lists(rows)
    fids = list(dict.fromkeys(f for fs in fid_rows for f in fs))[:cap]
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.get(f"{f}:vec:meta")
    found = {
        f: consts_in(_txt(raw) if raw else "") for f, raw in zip(fids, pipe.execute())
    }
    out = []
    for fs in fid_rows:
        read = [f for f in fs if f in found]
        count = Counter(c for f in read for c in found[f])
        # a constant one file holds alone is an address, not an indicator
        count = Counter({c: n for c, n in count.items() if n * 2 >= len(read)})
        best = sorted(count, key=lambda c: (-count[c], -c[0]))[:top]
        out.append(
            {
                "total": len(count),
                "top": [
                    {"value": f"0x{v:x}", "n": count[(v, s)], "of": len(read)}
                    for v, s in best
                ],
            }
        )
    return out


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


def string_refs(r, doc, rows, top=4):
    """Give each string row `refs`: the function rows whose functions reference it
    (the links stored in the database) as `{name, n, cells}`, and how many function rows.
    `cells` = {file id: function id} of the referencing functions (a few), so the client
    can draw the function like a table row. Returns their metadata `{function id: meta}`.
    """
    # by table row, not by name: two rows may share a name
    at, file_of = {}, {}
    for i, row in enumerate(doc["rows"]):
        for k, f in (row.get("files") or row["cells"]).items():
            if isinstance(f, str):
                at[f], file_of[f] = i, k
    direct = _referencing(r, doc, [row["id"] for row in rows], at)
    fids = set()
    for row in rows:
        fs = direct[row["id"]]
        own, cells = Counter(at[f] for f in fs), {}
        for f in fs:
            c = cells.setdefault(at[f], {})
            if len(c) < REF_FIDS:
                c[file_of[f]] = f
        top_rows = [
            {
                "name": doc["rows"][i]["name"],
                "variant": doc["rows"][i].get("variant"),
                "n": n,
                "cells": cells[i],
            }
            for i, n in own.most_common(top)
        ]
        fids.update(f for x in top_rows for f in x["cells"].values())
        # one function row (the same function in several files) counts once
        row["refs"] = {"direct": top_rows, "functions": len(own)}
    return {f: doc["fmeta"][f] for f in fids if f in doc["fmeta"]}


EXPAND_MAX = 300  # sub-rows returned for one expanded row


def expand_function(r, doc, cells):
    """Sub-rows of one function row: every string its functions reference, with
    presence over the table's columns. `cells` = {file id: function id}."""
    fids = {k: v for k, v in cells.items() if isinstance(v, str)}
    pipe = r.pipeline(transaction=False)
    for f in fids.values():
        pipe.smembers(f"{f}:strings")
    addrs = [sorted(_txt(a) for a in got) for got in pipe.execute()]
    pipe = r.pipeline(transaction=False)
    todo = [(k, f, a) for (k, f), a in zip(fids.items(), addrs) if a]
    for _, f, a in todo:
        coll, rest = f.split(":func:", 1)
        pipe.hmget(string_service.file_key(coll, rest.split(":")[0]), a)
    table = {}
    for (file, _, _), got in zip(todo, pipe.execute()):
        for raw in got:
            try:
                rec = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if (
                not rec.get("id")
                or rec.get("length", len(rec.get("value", ""))) < INLINE_MIN_LEN
            ):
                continue
            row = table.setdefault(
                rec["id"],
                {
                    "id": rec["id"],
                    "name": rec["value"],
                    "encoding": rec.get("encoding", ""),
                    "section": rec.get("section", ""),
                    "cells": {},
                },
            )
            row["cells"].setdefault(file, rec.get("addr", ""))
    # the string is shown in every file that holds it, not only through this row's functions
    files = [c["id"] for c in doc["file_columns"] or doc["columns"]]
    full = (
        _read_strings(r, files, INLINE_MIN_LEN)
        if len(files) <= MAX_STRING_FILES
        else {}
    )
    for sid, x in table.items():
        x["held"] = {**full.get(sid, {}).get("files", {}), **x["cells"]}
    base = list(table.values())
    base.sort(key=lambda x: (-len(x["held"]), -len(x["name"])))
    return _expanded(doc, base)


def _fid_file(doc):
    """Function id -> the column key of the file it is in (kept on the cached doc)."""
    if "_fid_file" not in doc:
        doc["_fid_file"] = {
            f: k
            for row in doc["rows"]
            for k, f in (row.get("files") or row["cells"]).items()
            if isinstance(f, str)
        }
    return doc["_fid_file"]


def _with_meta(doc, out):
    out["functions_metadata"] = {
        f: doc["fmeta"][f]
        for x in out["items"]
        for f in x["fcells"].values()
        if f in doc["fmeta"]
    }
    return out


def expand_links(r, doc, cells, role):
    """Sub-rows of one function row: the table rows it is called by (`role=callers`) or
    calls (`callees`), with presence over the columns. A row is shown in every file it is
    in; `via` marks the files where it is linked to the row opened. Calls out of the
    binary are `ext` rows, present only where they are called."""
    at, file_of = _fid_at(doc), _fid_file(doc)
    fids = {k: v for k, v in cells.items() if isinstance(v, str)}
    me = at.get(next(iter(fids.values()), None))
    pipe = r.pipeline(transaction=False)
    for f in fids.values():
        pipe.smembers(f"{f}:{role}")
    table = {}
    for (file, _), got in zip(fids.items(), pipe.execute()):
        for m in map(_txt, got):
            if m.startswith("ext:"):
                key, name, ext = m, m[4:], True
            elif (i := at.get(m)) is not None and i != me:
                key, name, ext = f"row:{i}", doc["rows"][i]["name"], False
            else:
                continue
            x = table.setdefault(
                key,
                {
                    "id": key,
                    "name": name,
                    "ext": ext,
                    "variant": (
                        None if ext else doc["rows"][int(key[4:])].get("variant")
                    ),
                    "cells": {},
                },
            )
            x["cells"].setdefault(file_of.get(m, file), m)
    for key, x in table.items():
        rowcells = {} if x["ext"] else _row_cells(doc, int(key[4:]))
        x["held"] = {**rowcells, **x["cells"]}
        x["fcells"] = dict(list(x["cells"].items())[:REF_FIDS])
    base = sorted(table.values(), key=lambda x: (-len(x["held"]), x["name"]))
    return _with_meta(doc, _expanded(doc, base))


def _row_cells(doc, i):
    row = doc["rows"][i]
    return {
        k: v
        for k, v in (row.get("files") or row["cells"]).items()
        if isinstance(v, str)
    }


def expand_consts(r, doc, cells):
    """Sub-rows of one function row: the constants at least half of its functions use,
    with presence over the columns."""
    fids = {k: v for k, v in cells.items() if isinstance(v, str)}
    pipe = r.pipeline(transaction=False)
    for f in fids.values():
        pipe.get(f"{f}:vec:meta")
    table = {}
    for (file, f), raw in zip(fids.items(), pipe.execute()):
        for v, size in consts_in(_txt(raw) if raw else ""):
            x = table.setdefault(
                (v, size), {"id": f"const:0x{v:x}", "name": f"0x{v:x}", "cells": {}}
            )
            x["cells"][file] = f
    base = []
    for x in table.values():
        if len(x["cells"]) * 2 >= len(fids):
            x["held"], x["fcells"] = x["cells"], dict(
                list(x["cells"].items())[:REF_FIDS]
            )
            base.append(x)
    base.sort(key=lambda x: (-len(x["held"]), x["name"]))
    return _with_meta(doc, _expanded(doc, base))


def expand_string(r, doc, sid):
    """Sub-rows of one string: the function rows whose functions reference it, with
    presence over the table's columns."""
    where = {}  # function id -> (row index, file id)
    for i, row in enumerate(doc["rows"]):
        for file, f in (row.get("files") or row["cells"]).items():
            if isinstance(f, str):
                where[f] = (i, file)
    held = {}
    for f in _referencing(r, doc, [sid], where)[sid]:
        i, file = where[f]
        held.setdefault(i, {})[file] = f
    # the function row is shown in every file it is in, with or without the string linked there
    base = [
        {
            "id": f"row:{i}",
            "name": doc["rows"][i]["name"],
            "variant": doc["rows"][i].get("variant"),
            "cells": cells,
            "fcells": dict(list(cells.items())[:REF_FIDS]),
            "held": {
                **{
                    k: v
                    for k, v in (
                        doc["rows"][i].get("files") or doc["rows"][i]["cells"]
                    ).items()
                    if isinstance(v, str)
                },
                **cells,
            },
        }
        for i, cells in held.items()
    ]
    base.sort(key=lambda x: (-len(x["held"]), x["name"]))
    out = _expanded(doc, base)
    out["functions_metadata"] = {
        f: doc["fmeta"][f]
        for x in out["items"]
        for f in x["fcells"].values()
        if f in doc["fmeta"]
    }
    return out


def _expanded(doc, base):
    """Sub-rows over the columns. Each item has `cells[column] = {present, total, on, via}`:
    `present` = files of the column that hold the item, `via` = those where it is linked
    to the row it was opened from (via <= present)."""
    shown, groups = base[:EXPAND_MAX], _groups(doc)
    linked = nway_diff_service.project_rows(
        [dict(x, span=len(x["cells"])) for x in shown], groups, 0.5
    )
    held = nway_diff_service.project_rows(
        [dict(x, cells=x["held"], span=len(x["held"])) for x in shown], groups, 0.5
    )
    items = []
    for x, v, h in zip(shown, linked, held):
        item = {k: x[k] for k in x if k not in ("cells", "held")}
        item["span"] = len(x["held"])
        item["cells"] = {
            g: dict(c, via=v["cells"][g]["present"]) for g, c in h["cells"].items()
        }
        items.append(item)
    return {"items": items, "total": len(base)}


def fids_with_string(r, doc, text, cap=200):
    """Function ids of the table that reference a string containing `text` (3+ characters)."""
    q = text.lower()
    files = [c["id"] for c in doc["file_columns"] or doc["columns"]]
    if len(q) < 3 or len(files) > MAX_STRING_FILES:
        return set()
    table = _read_strings(r, files, INLINE_MIN_LEN)
    sids = [sid for sid, s in table.items() if q in s["value"].lower()][:cap]
    direct = _referencing(r, doc, sids, _fid_rows(doc))
    return {f for fs in direct.values() for f in fs}


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
    # a nameless row is named after a string few others use
    assert _string_name("/dev/watchdog") == "dev_watchdog"
    assert _string_name("%s %d") is None and _string_name("ab") is None

    class _P:
        def __init__(self):
            self.q = []

        def smembers(self, k):
            self.q.append(("s", k))

        def hmget(self, k, f):
            self.q.append(("h",))

        def scard(self, k):
            self.q.append(("c",))

        def execute(self):
            out = [
                (
                    {b"0x10"}
                    if q[0] == "s" and "f1:" in q[1]
                    else (
                        [
                            json.dumps(
                                {"id": "s1", "value": "/dev/watchdog", "length": 13}
                            )
                        ]
                        if q[0] == "h"
                        else (2 if q[0] == "c" else set())
                    )
                )
                for q in self.q
            ]
            self.q = []
            return out

    class _R:
        def pipeline(self, transaction=False):
            return _P()

    rows = [
        {"name": "FUN_0001", "cells": {"a": "c:func:m:f1"}},
        {"name": "main", "cells": {"a": "c:func:m:f2"}},
    ]
    name_by_strings(_R(), rows)
    assert [r["name"] for r in rows] == ["uses_dev_watchdog", "main"], rows
    assert (
        _common_token(["/proc/stat", "/proc/net/tcp", "/proc/cpuinfo", "ok"]) == "proc"
    )
    assert _common_token(["/proc/stat", "kill -9", "hello"]) is None
    assert rows[0]["name_from"] == "string" and rows[0]["name_was"] == "FUN_0001"
    ctx = {"family": {"a", "b"}, "contrast": {"c", "d", "e"}, "ignored": {"f"}}
    s = stats({"a", "b"}, ctx)
    assert (s["cover"], s["leak"], s["score"]) == (1.0, 0, 1.0)
    s = stats({"a", "c", "f"}, ctx)
    assert (s["cover"], s["leak"], s["score"]) == (0.5, 1, round(0.5 * (1 - 1 / 3), 4))
    assert stats({"a", "b"}, ctx, reach=2 + 25)["score"] == 0.5
    assert string_score(1, 0, 0, 5) is None
    # a string held by 3 files but linked to the row's function in 2: the third shows as present, not linked
    doc = {"file_columns": None, "columns": [{"id": i} for i in "ABC"]}
    item = {
        "id": "s",
        "name": "x",
        "cells": {"A": 1, "B": 2},
        "held": {"A": 1, "B": 2, "C": 3},
    }
    cells = _expanded(doc, [item])["items"][0]["cells"]
    assert [(cells[i]["present"], cells[i]["via"]) for i in "ABC"] == [
        (1, 1),
        (1, 1),
        (1, 0),
    ]


if __name__ == "__main__":
    demo()
    print("ok")
