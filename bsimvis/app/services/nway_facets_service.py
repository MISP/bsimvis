"""Inline N-way function diff: facets and consensus code for one row.

Everything is derived from stored data, nothing is extracted again:

| Facet            | Source                                                    |
| Strings          | `string_service.function_strings` (empty until it exists) |
| Imported APIs    | `ext:` ids of `{fid}:callees`                             |
| Internal callees | other ids of `{fid}:callees`, grouped by the doc's rows   |
| Constants        | numeric `variable` tokens of `{fid}:source`               |
| Shape            | `{fid}:meta` counts, plus keyword counts in the source    |
| Consensus code   | `{fid}:source` lines aligned like the 2-way diff          |

Lines are aligned with the 2-way diff's own line identity (BSim feature hashes,
else the text), so the arm / mips / x86 builds of a function line up although
their text differs. Work is capped by function count (`nway.facet_max_functions`
and `nway.code_max_functions`) and line count (`nway.max_code_lines`).
"""

import difflib
import json
import re
from itertools import combinations
from statistics import median

from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.nway_diff_service import _DEFAULT_NAME, _LRU, _col_id

try:
    from bsimvis.app.services.string_service import function_strings
except ImportError:  # string extraction lives on another branch

    def function_strings(r, collection, md5, addr):
        return []


ITEM_CAP = 400  # items kept per facet, rows that differ first
EXTRA_CAP = 300  # lines only in other functions
SHAPE_OFF = 0.2  # shape values this far from the median are flagged

_CACHE = _LRU(16)
_ADDR_NAME = re.compile(r"\b(FUN|DAT|LAB|PTR|SUB|UNK|switchD|caseD)_[0-9a-fA-F]+")
_TOK = re.compile(r"\w+|\s+|[^\w\s]")
_NUM = re.compile(r"^(0[xX][0-9a-fA-F]+|\d+)[uUlL]*$")
_LOOPS = {"for", "while", "do"}
_BRANCHES = {"if", "switch", "goto"}


def limits():
    return (
        int(config_service.get("nway.facet_max_functions", 64)),
        int(config_service.get("nway.code_max_functions", 16)),
        int(config_service.get("nway.max_code_lines", 1500)),
    )


def letter(i):
    """0 -> A, 25 -> Z, 26 -> AA."""
    out = ""
    i += 1
    while i:
        i, rem = divmod(i - 1, 26)
        out = chr(65 + rem) + out
    return out


def split_fid(fid):
    coll, _, rest = fid.partition(":func:")
    md5, _, addr = rest.partition(":")
    return coll, md5, addr


def _norm(text):
    """Whitespace and auto-generated address names carry no meaning across files."""
    return _ADDR_NAME.sub(r"\1_", " ".join(text.split()))


def _s(v):
    return v.decode() if isinstance(v, bytes) else str(v)


def pair_scores(doc, fids):
    """N x N matrix of the matcher's edge scores; None where no edge exists."""
    cols = [_col_id(*split_fid(f)[:2]) for f in fids]
    n = len(fids)
    out = [[None] * n for _ in range(n)]
    for i in range(n):
        out[i][i] = 1.0
    for i, j in combinations(range(n), 2):
        for key in ((cols[i], cols[j]), (cols[j], cols[i])):
            for a, b, s in doc["edges"].get(key, ()):
                if {a, b} == {fids[i], fids[j]}:
                    out[i][j] = out[j][i] = round(float(s), 4)
                    break
    return out


def _medoid(sim):
    score = [sum(v or 0.0 for v in row) for row in sim]
    return max(range(len(sim)), key=lambda i: (score[i], -i))


def _facet(key, title, per_fn, skipped=None):
    n = len(per_fn)
    if skipped:
        return {"key": key, "title": title, "skipped": skipped, "items": []}
    seen = {}
    for j, d in enumerate(per_fn):
        for k, label in d.items():
            seen.setdefault(k, [label, [0] * n])[1][j] = 1
    items = [
        {"key": k, "label": label, "mask": "".join(map(str, m)), "count": sum(m)}
        for k, (label, m) in seen.items()
    ]
    items.sort(key=lambda i: (i["count"] == n, -i["count"], i["label"].lower()))
    return {
        "key": key,
        "title": title,
        "total": len(items),
        "in_all": sum(1 for i in items if i["count"] == n),
        "truncated": max(0, len(items) - ITEM_CAP),
        "items": items[:ITEM_CAP],
    }


def _const(text):
    m = _NUM.match(text)
    if not m:
        return None
    raw = text.rstrip("uUlL")
    v = int(raw, 16) if raw[:2] in ("0x", "0X") else int(raw)
    return str(v), (f"0x{v:x} ({v})" if v >= 10 else str(v))


def _source_facts(src):
    toks = (src or {}).get("c_tokens") or []
    consts = {}
    for t in toks:
        if t.get("type") == "variable":
            c = _const(t["t"])
            if c:
                consts[c[0]] = c[1]
    words = [t["t"] for t in toks]
    return {
        "consts": consts,
        "loops": sum(1 for w in words if w in _LOOPS),
        "branches": sum(1 for w in words if w in _BRANCHES),
        "lines": len((src or {}).get("c_lines") or ()) or None,
    }


def _callee_key(fid, fmeta, row_of, rows):
    """(key, label) of an internal callee: its N-way row, else its name, else itself."""
    if fid in row_of:
        return f"row:{row_of[fid]}", rows[row_of[fid]]["name"]
    name = (fmeta.get(fid) or {}).get("name") or fid.rsplit(":", 1)[-1]
    if _DEFAULT_NAME.match(name):
        return f"fn:{fid}", name
    return f"name:{name}", name


def _segments(ref, text):
    """`text` as [[token run, changed]] against the reference line."""
    a, b = _TOK.findall(ref), _TOK.findall(text)
    key = lambda t: " " if t.isspace() else _norm(t)
    sm = difflib.SequenceMatcher(
        None, [key(t) for t in a], [key(t) for t in b], autojunk=False
    )
    segs = []
    for tag, _, _, j1, j2 in sm.get_opcodes():
        run, changed = "".join(b[j1:j2]), tag != "equal"
        if not run:
            continue
        if segs and segs[-1][1] == changed:
            segs[-1][0] += run
        else:
            segs.append([run, changed])
    return segs


def _lines(src, feats, cap):
    from bsimvis.app.routes.function_diff import get_lines_data
    from bsimvis.app.services.function_service import get_feature_map

    by_line, ids = get_lines_data(src or {}, get_feature_map(feats), set())
    texts = ["".join(t["t"] for _, t in by_line[i]).rstrip() for i in range(len(ids))]
    return texts[:cap], ids[:cap], len(ids) > cap


def _consensus(srcs, feats, ref, cap):
    """Reference lines with a per-function presence mask and token-level variants."""
    n = len(srcs)
    got = [_lines(s, f, cap) for s, f in zip(srcs, feats)]
    texts, ids = [g[0] for g in got], [g[1] for g in got]
    mapped = [dict() for _ in texts[ref]]
    extras, order = {}, []

    def extra(after, j, text):
        k = (after, _norm(text))
        if k not in extras:
            extras[k] = {"ins": True, "after": after + 1, "text": text, "has": set()}
            order.append(k)
        extras[k]["has"].add(j)

    for j in range(n):
        if j == ref:
            continue
        sm = difflib.SequenceMatcher(None, ids[ref], ids[j], autojunk=False)
        for tag, i1, i2, j1, j2 in sm.get_opcodes():
            if tag in ("equal", "replace"):
                m = min(i2 - i1, j2 - j1)
                for k in range(m):
                    mapped[i1 + k][j] = texts[j][j1 + k]
                for k in range(m, j2 - j1):
                    extra(i2 - 1, j, texts[j][j1 + k])
            elif tag == "insert":
                for k in range(j1, j2):
                    extra(i1 - 1, j, texts[j][k])

    out, dropped = [], 0
    after = {}
    for k in order:
        after.setdefault(k[0], []).append(extras[k])

    def put_extras(i):
        nonlocal dropped
        for e in after.get(i, ()):
            if sum(1 for o in out if o.get("ins")) >= EXTRA_CAP:
                dropped += 1
                continue
            out.append(
                {k: e[k] for k in ("ins", "after", "text")}
                | {"mask": "".join("1" if j in e["has"] else "0" for j in range(n))}
            )

    put_extras(-1)
    for i, text in enumerate(texts[ref]):
        mask, var = [], {}
        for j in range(n):
            if j == ref:
                mask.append("1")
            elif j not in mapped[i]:
                mask.append("0")
                var[j] = None
            elif _norm(mapped[i][j]) == _norm(text):
                mask.append("1")
            else:
                mask.append("0")
                var[j] = _segments(text, mapped[i][j])
        line = {"n": i + 1, "text": text, "mask": "".join(mask)}
        if var:
            line["v"] = var
        out.append(line)
        put_extras(i)
    return {
        "reference": ref,
        "lines": out,
        "lines_total": len(texts[ref]),
        "truncated": any(g[2] for g in got),
        "extras_dropped": dropped,
    }


def _shape(rows):
    """Rows of per-function counts; `off` marks values far from the median."""
    out = []
    for key, label in (
        ("instr", "Instructions"),
        ("lines", "Decompiled lines"),
        ("features", "BSim features"),
        ("params", "Parameters"),
        ("calls", "Callees"),
        ("callers", "Callers"),
        ("loops", "Loops"),
        ("branches", "Branches"),
    ):
        vals = [r.get(key) for r in rows]
        known = [v for v in vals if v is not None]
        if not known:
            continue
        m = median(known)
        out.append(
            {
                "key": key,
                "label": label,
                "values": vals,
                "median": m,
                "off": [
                    v is not None and abs(v - m) / max(abs(m), 1) > SHAPE_OFF
                    for v in vals
                ],
            }
        )
    return out


def compute(r, doc, fids):
    """Facets, similarity matrix and (for small N) consensus code of one row."""
    cache_key = (doc["key"], tuple(fids))
    hit = _CACHE.get(cache_key)
    if hit is not None:
        return hit
    n = len(fids)
    facet_max, code_max, line_cap = limits()
    level = "full" if n <= code_max else "facets" if n <= facet_max else "light"
    fmeta, rows = doc["fmeta"], doc["rows"]
    row_of = {
        f: i
        for i, row in enumerate(rows)
        for f in (row.get("files") or row["cells"]).values()
        if isinstance(f, str)
    }
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.get(f"{f}:meta")
        pipe.smembers(f"{f}:callees")
    res = pipe.execute()
    metas = [json.loads(x) if x else {} for x in res[0::2]]
    metas = [m[0] if isinstance(m, list) and m else m for m in metas]
    callees = [[_s(c) for c in s] for s in res[1::2]]

    srcs, feats, facts = [None] * n, [None] * n, [{} for _ in range(n)]
    strings = []
    for j, f in enumerate(fids):
        coll, md5, addr = split_fid(f)
        if level != "light":
            raw = r.get(f"{f}:source")
            srcs[j] = json.loads(raw) if raw else None
            if isinstance(srcs[j], list) and len(srcs[j]) == 1:
                srcs[j] = srcs[j][0]
            facts[j] = _source_facts(srcs[j])
            strings.append(
                {s["id"]: s["value"] for s in function_strings(r, coll, md5, addr)}
            )
        if level == "full":
            raw = r.get(f"{f}:vec:meta")
            feats[j] = json.loads(raw) if raw else []
            if (
                isinstance(feats[j], list)
                and len(feats[j]) == 1
                and isinstance(feats[j][0], list)
            ):
                feats[j] = feats[j][0]

    apis = [{c[4:]: c[4:] for c in cs if c.startswith("ext:")} for cs in callees]
    internal = [
        dict(
            _callee_key(c, fmeta, row_of, rows) for c in cs if not c.startswith("ext:")
        )
        for cs in callees
    ]
    light = f"more than {facet_max} functions: source-derived facets are off"
    facets = [
        _facet("strings", "Strings", strings, None if strings else light),
        _facet("apis", "Imported / called APIs", apis),
        _facet("callees", "Internal callees", internal),
        _facet(
            "consts",
            "Constants",
            [x.get("consts", {}) for x in facts],
            None if strings else light,
        ),
    ]
    shape = _shape(
        [
            {
                "instr": m.get("instruction_count"),
                "lines": x.get("lines"),
                "features": m.get("bsim_features_count"),
                "params": len(m.get("parameters") or ()),
                "calls": m.get("callee_count", len(cs)),
                "callers": m.get("caller_count"),
                "loops": x.get("loops"),
                "branches": x.get("branches"),
            }
            for m, x, cs in zip(metas, facts, callees)
        ]
    )
    sim = pair_scores(doc, fids)
    ref = _medoid(sim)
    cols = {c["id"]: c for c in (doc.get("file_columns") or doc["columns"])}
    out = {
        "level": level,
        "functions": [
            {
                "id": f,
                "letter": letter(j),
                "file_name": (cols.get(_col_id(*split_fid(f)[:2])) or {}).get(
                    "file_name"
                ),
                "addr": split_fid(f)[2],
                "name": (fmeta.get(f) or {}).get("name"),
                "instr": metas[j].get("instruction_count"),
                "tags": list(fmeta.get(f, {}).get("tags") or [])[:6],
            }
            for j, f in enumerate(fids)
        ],
        "similarity": sim,
        "reference": ref,
        "facets": facets,
        "shape": shape,
        "consensus": None,
        "limits": {"facets": facet_max, "code": code_max, "lines": line_cap},
    }
    if level == "full":
        out["consensus"] = _consensus(srcs, feats, ref, line_cap)
    _CACHE.put(cache_key, out)
    return out


def demo():
    assert letter(0) == "A" and letter(25) == "Z" and letter(26) == "AA"
    assert _const("0x400") == ("1024", "0x400 (1024)") and _const("5u") == ("5", "5")
    assert _const("abc") is None
    assert _norm("call  FUN_00012a40( x )") == "call FUN_( x )"
    assert _segments("a = 80;", "a = 1024;") == [
        ["a = ", False],
        ["1024", True],
        [";", False],
    ]
    f = _facet("k", "K", [{"x": "x", "y": "y"}, {"x": "x"}])
    assert (
        f["in_all"] == 1
        and f["items"][0]["key"] == "y"
        and f["items"][0]["mask"] == "10"
    )
    src = lambda lines: {
        "c_tokens": [{"t": t, "line": i, "type": None} for i, t in enumerate(lines)]
    }
    a, b, c = (
        src(["int f()", "{", "x = 80;", "}"]),
        src(["int f()", "{", "x = 1;", "extra();", "}"]),
        src(["int f()", "{", "}"]),
    )
    out = _consensus([a, b, c], [[], [], []], 0, 100)
    lines = {x["n"]: x for x in out["lines"] if "n" in x}
    assert lines[3]["mask"] == "100" and lines[3]["v"][1] == [
        ["x = ", False],
        ["1", True],
        [";", False],
    ]
    assert lines[3]["v"][2] is None and lines[1]["mask"] == "111"
    assert [x for x in out["lines"] if x.get("ins")][0]["mask"] == "010"
    print("nway_facets_service demo ok")


if __name__ == "__main__":
    demo()
