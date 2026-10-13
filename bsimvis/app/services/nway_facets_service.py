"""Inline N-way function diff: facets and consensus code for one row.

Everything is derived from stored data, nothing is extracted again:

| Facet            | Source                                                    |
| Strings          | `string_service.function_strings` (empty until it exists) |
| Imported APIs    | `ext:` ids of `{fid}:callees`                             |
| Internal callees | other ids of `{fid}:callees`, grouped by the doc's rows   |
| Constants        | numeric `variable` tokens of `{fid}:source`               |
| Shape            | `{fid}:meta` counts, plus keyword counts in the source    |
| Code blocks      | `{fid}:source` split into blocks, matched by token bag    |

Code blocks: each function is cut into blocks (signature, compound statements,
straight runs) and blocks are matched across functions by multiset Dice over
normalised tokens plus the BSim feature hashes of their lines, in any order.
Work is capped by function count (`nway.facet_max_functions` and
`nway.code_max_functions`), line count (`nway.max_code_lines`) and block count
(`nway.max_code_blocks`).
"""

import difflib
import hashlib
import json
import threading
import time
import re
from collections import Counter
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
SHAPE_OFF = 0.2  # shape values this far from the median are flagged

_CACHE = _LRU(16)
_ADDR_NAME = re.compile(
    r"\b(FUN|DAT|LAB|PTR|SUB|UNK|switchD|caseD)_(?:[a-z]+_)?[0-9a-fA-F]+"
)
# Decompiler-chosen names and type words; param_N is positional, so it stays.
_VAR = re.compile(r"^([a-z]{0,3}Var\d+|local_\w+|[a-z]?Stack_\w+|in_\w+|extraout_\w+)$")
_TYPE = re.compile(
    r"^(undefined\d*|[ui]?int(\d+_t)?|u?(long|short|char)|u?longlong|byte|bool|code"
    r"|[su]?size_t|unsigned|signed|[dq]?word)$"
)
_TOK = re.compile(r"\w+|\s+|[^\w\s]")
_NUM = re.compile(r"^(0[xX][0-9a-fA-F]+|\d+)[uUlL]*$")
_LOOPS = {"for", "while", "do"}
_BRANCHES = {"if", "switch", "goto"}


def limits():
    return (
        int(config_service.get("nway.facet_max_functions", 64)),
        int(config_service.get("nway.code_max_functions", 30)),
        int(config_service.get("nway.max_code_lines", 1500)),
        int(config_service.get("nway.max_code_blocks", 60)),
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


def _keys(toks, sig=False):
    """Comparison key per token, None where it carries no meaning across builds.

    Whitespace and casts are dropped; local names, type words and generated
    names collapse to a placeholder. Constants, callees and strings stay exact.
    In a signature block (`sig`) the function's own name is dropped too.
    """
    keys = []
    for t in toks:
        if t.isspace():
            keys.append(None)
        elif _VAR.match(t):
            keys.append("V")
        elif _TYPE.match(t):
            keys.append("T")
        else:
            keys.append(_ADDR_NAME.sub(r"\1_", t))
    if sig:
        seen = False
        for i, t in enumerate(toks):
            if t.isspace():
                if seen and "\n" in t:
                    break
                continue
            seen = True
            nxt = next((u for u in toks[i + 1 :] if not u.isspace()), "")
            if keys[i] and t.isidentifier() and nxt == "(":
                keys[i] = "N"
                break
    i = 0
    while i < len(toks):
        if toks[i] == "(":
            j, typed = i + 1, False
            while j < len(toks) and keys[j] in (None, "T", "*", "const"):
                typed = typed or keys[j] == "T"
                j += 1
            if typed and j < len(toks) and toks[j] == ")":
                keys[i : j + 1] = [None] * (j + 1 - i)
                i = j
        i += 1
    return keys


def _norm(text, sig=False):
    """Block text with build noise removed: equal `_norm` means the same code."""
    return " ".join(k for k in _keys(_TOK.findall(text), sig) if k)


def _s(v):
    return v.decode() if isinstance(v, bytes) else str(v)


def pair_scores(doc, fids):
    """N x N matrix of the matcher's edge scores; None where no edge exists."""
    cols = [_col_id(*split_fid(f)[:2]) for f in fids]
    n = len(fids)
    out = [[None] * n for _ in range(n)]
    for i in range(n):
        out[i][i] = 1.0
    if "fscore" in doc:
        for i, j in combinations(range(n), 2):
            out[i][j] = out[j][i] = doc["fscore"].get(frozenset((fids[i], fids[j])))
        return out
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


def _segments(ref, text, sig=False):
    """`text` as [[token run, changed]] against the reference line."""
    a, b = _TOK.findall(ref), _TOK.findall(text)
    ka, kb = _keys(a, sig), _keys(b, sig)
    ia = [i for i, k in enumerate(ka) if k is not None]
    ib = [i for i, k in enumerate(kb) if k is not None]
    sm = difflib.SequenceMatcher(
        None, [ka[i] for i in ia], [kb[i] for i in ib], autojunk=False
    )
    flag = [False] * len(b)
    for tag, _, _, j1, j2 in sm.get_opcodes():
        if tag != "equal":
            for i in ib[j1:j2]:
                flag[i] = True
    # ignored tokens (spaces, casts) read as changed only between changed tokens
    for i in range(1, len(b) - 1):
        if kb[i] is None and flag[i - 1] and flag[i + 1]:
            flag[i] = True
    segs = []
    for tok, changed in zip(b, flag):
        if segs and segs[-1][1] == changed:
            segs[-1][0] += tok
        else:
            segs.append([tok, changed])
    return segs


def _lines(src, feats, cap):
    from bsimvis.app.routes.function_diff import get_lines_data
    from bsimvis.app.services.function_service import get_feature_map

    f_map = get_feature_map(feats)
    by_line, ids = get_lines_data(src or {}, f_map, set())
    texts = ["".join(t["t"] for _, t in by_line[i]).rstrip() for i in range(len(ids))]
    return texts[:cap], ids[:cap], by_line, len(ids) > cap, f_map


_CTRL = re.compile(r"^(if|for|while|switch|do|else)\b")
_DECL = re.compile(r"^[A-Za-z_][\w \t\*]*(\(\*\w+\))?[\w\*]?\s*(\[\w*\])?\s*;$")
_IDENT = re.compile(r"[A-Za-z_]\w*|0[xX][0-9a-fA-F]+|\d+|[^\w\s]")
# Decompiler-made local names carry no meaning across builds.
_LOCAL = re.compile(
    r"^([a-z]{0,2}Var\d+|local_\w+|[a-z]?Stack_\w+|param_\d+|in_\w+|extraout_\w+)$"
)
MATCH_MIN = 0.5  # block pairs below this Dice score are not the same block
COVER_MIN = 0.6  # share of a block's tokens another block must hold to absorb it
SPLIT_MIN = 20  # smaller blocks are never reported as split over several
RUN = 10  # longest straight run of statements kept as one block
DIFF_CAP = 1500  # longer blocks are shown without token highlights
TEXT_CAP = 4000  # characters of a block kept for display and token diff
SEED_MIN = 0.6  # feature-hash Dice for two blocks to seed the CFG alignment
HASH_MIN = 4  # fewer features than this say nothing about a block
PROP_MIN = 0.3  # token-bag Dice for a CFG neighbour of a matched pair to match


def _sig_end(texts):
    """Index after the line that opens the function body."""
    depth = 0
    for i, t in enumerate(texts):
        depth += t.count("{") - t.count("}")
        if depth >= 1:
            return i + 1
    return len(texts)


def split_blocks(texts, run=RUN):
    """Line spans (start, end) of a function's blocks.

    A block is the signature with its local declarations, a compound statement
    (if / for / while / switch / do, with its else chain) or a straight run of
    at most `run` statements. Braces are counted, so nesting stays inside its
    top-level block.
    """
    n, i = len(texts), _sig_end(texts)
    sig = i
    while i < n and _DECL.match(texts[i].strip()):
        i += 1
    spans = ([(0, sig)] if sig else []) + ([(sig, i)] if i > sig else [])
    run_at, last_comp = None, False

    def close(end):
        nonlocal run_at
        if run_at is not None and end > run_at:
            spans.append((run_at, end))
        run_at = None

    while i < n:
        s = texts[i].strip()
        if not s:
            i += 1
            continue
        if s.startswith("}"):  # the function's own closing brace
            break
        is_else = s.startswith("else") and last_comp
        comp = (
            is_else
            or s.count("{") > s.count("}")
            or (_CTRL.match(s) and not s.endswith(";"))
        )
        if not comp:
            run_at = i if run_at is None else run_at
            i += 1
            last_comp = False
            if i - run_at >= run:
                close(i)
            continue
        close(i)
        start = spans.pop()[0] if is_else else i
        d, pending, k = 0, 0, i
        while k < n:
            t = texts[k].strip()
            d += t.count("{") - t.count("}")
            if t.endswith(";"):
                pending = 0
            elif _CTRL.match(t.lstrip("} ")) and "{" not in t:
                pending += 1
            k += 1
            if d <= 0 and pending == 0:
                break
        spans.append((start, k))
        i, last_comp = k, True
    close(i)
    return _unwrap(texts, spans)


def _unwrap(texts, spans):
    """Split a guard-shaped if/else (short `if`, long `else {}`) into its parts.

    A guard clause (`if (x) return 0; ...`) and its decompiled single-exit form
    (`if (x) r = 0; else { ... }`) then yield the same blocks.
    """
    out = []
    for a, b in spans:
        e = _guard_else(texts, a, b)
        if e is None:
            out.append((a, b))
            continue
        sub = split_blocks(["{"] + texts[e + 1 : b - 1])
        out += [(a, e)] + [(x + e, y + e) for x, y in sub]
    return out


def _guard_else(texts, a, b):
    """Index of the top-level `else {` of block (a, b) when its `if` part is short."""
    if not texts[a].strip().startswith("if"):
        return None
    d = 0
    for e in range(a, b):
        s = texts[e].strip()
        if e > a and d == 0 and s.startswith("else"):
            ok = s.endswith("{") and e - a <= 3 and b - e > 2 * (e - a)
            return e if ok else None
        d += s.count("{") - s.count("}")
    return None


def has_cfg(src):
    return bool(src and src.get("cfg", {}).get("blocks") and src.get("tok_block"))


def _cfg_spans(src, texts, by_line, feats):
    """Line spans of a function's display blocks, from its stored Ghidra CFG.

    A line belongs to the Ghidra block most of its tokens belong to. Runs of
    lines with one block are cut, then adjacent runs joined by a single edge
    (one out, one in) are merged. Returns (spans, info) or None when no line
    carries a block. info[k]: `bb` (Ghidra blocks touched), `H` (feature hashes
    of the blocks it owns), `succ` / `pred` (display-block edges, out order).
    """
    n_tok = len(src["c_tokens"])
    blk = [-1] * n_tok
    tb = src["tok_block"]
    for (a, b), nxt in zip(tb, [r[0] for r in tb[1:]] + [n_tok]):
        blk[a:nxt] = [b] * (nxt - a)
    dom = []
    for i in range(len(texts)):
        c = Counter(blk[idx] for idx, t in by_line.get(i, ()) if blk[idx] >= 0)
        dom.append(c.most_common(1)[0][0] if c else None)
    first = next((i for i, d in enumerate(dom) if d is not None), None)
    if first is None:
        return None
    sig = min(_sig_end(texts), first)
    runs = [[a, b, None] for a, b in ((0, sig), (sig, first)) if b > a]
    for i in range(first, len(texts)):
        d = dom[i] if dom[i] is not None else runs[-1][2]
        if runs and runs[-1][2] == d:
            runs[-1][1] = i + 1
        else:
            runs.append([i, i + 1, d])
    touched, owner = [], {}
    for u, (a, b, _) in enumerate(runs):
        c = Counter(
            blk[idx]
            for i in range(a, b)
            for idx, _t in by_line.get(i, ())
            if blk[idx] >= 0
        )
        touched.append(set(c))
        for bb, k in c.items():
            if k > owner.get(bb, (0, -1))[0]:
                owner[bb] = (k, u)
    out_of = {b["i"]: b["out"] for b in src["cfg"]["blocks"]}

    def graph(group, count):
        succ = [[] for _ in range(count)]
        for u, bbs in enumerate(touched):
            for bb in bbs:
                for t in out_of.get(bb, ()):
                    v = group[owner[t][1]] if t in owner else None
                    if v is not None and v != group[u] and v not in succ[group[u]]:
                        succ[group[u]].append(v)
        return succ

    def has_loop(bbs):
        seen, stack = set(), set()

        def dfs(x):
            seen.add(x)
            stack.add(x)
            for t in out_of.get(x, ()):
                if t in bbs and (t in stack or (t not in seen and dfs(t))):
                    return True
            stack.discard(x)
            return False

        return any(x not in seen and dfs(x) for x in bbs)

    loop = [has_loop(t) for t in touched]
    ident = list(range(len(runs)))
    succ0 = graph(ident, len(runs))
    pred0 = [[u for u in range(len(runs)) if v in succ0[u]] for v in ident]
    group = []
    for u in ident:
        joined = (
            u
            and succ0[u - 1] == [u]
            and pred0[u] == [u - 1]
            and not (loop[u - 1] or loop[u])
        )
        group.append(group[-1] if joined else (group[-1] + 1 if group else 0))
    count = group[-1] + 1
    succ = graph(group, count)
    bbs, owned = [set() for _ in range(count)], [set() for _ in range(count)]
    for u in range(len(runs)):
        g = group[u]
        bbs[g] |= touched[u]
        owned[g] |= {bb for bb, (_, o) in owner.items() if o == u}
    spans = [
        (
            min(r[0] for u, r in enumerate(runs) if group[u] == g),
            max(r[1] for u, r in enumerate(runs) if group[u] == g),
        )
        for g in range(count)
    ]
    by_bb = {}
    for f in feats or ():
        if f.get("block_index") is not None:
            by_bb.setdefault(f["block_index"], []).append(f["hash"])
    info = [
        {
            "bb": sorted(bbs[g]),
            "H": Counter(h for bb in owned[g] for h in by_bb.get(bb, ())),
            "succ": succ[g],
            "pred": [u for u in range(count) if g in succ[u]],
        }
        for g in range(count)
    ]
    for x in info:
        x["shape"] = (min(len(x["pred"]), 2), min(len(x["succ"]), 2))
    return spans, info


def _cfg_align(ref, other):
    """Block pairs and the subset whose CFG neighbours also match.

    Seeds are blocks sharing rare feature hashes; the CFG edges of every matched
    pair then pull in their unmatched neighbours by token-bag Dice.
    """

    def where(blocks):
        w = {}
        for k, b in enumerate(blocks):
            for h in b["H"]:
                w.setdefault(h, []).append(k)
        return w

    wr, wo = where(ref), where(other)
    votes = Counter()
    for h, rs in wr.items():
        os_ = wo.get(h, ())
        if len(rs) <= 2 and 0 < len(os_) <= 2:
            for i in rs:
                for j in os_:
                    votes[i, j] += 1
    cand = sorted(
        (-_dice(ref[i]["H"], other[j]["H"]), -v, i, j) for (i, j), v in votes.items()
    )
    mr, mo = {}, {}
    for neg, _, i, j in cand:
        if -neg >= SEED_MIN and i not in mr and j not in mo:
            mr[i], mo[j] = j, i
    work = list(mr.items())
    while work:
        i, j = work.pop()
        for key in ("succ", "pred"):
            nr = [x for x in ref[i][key] if x not in mr]
            no = [y for y in other[j][key] if y not in mo]
            for neg, x, y in sorted(
                (-_dice(ref[x]["bag"], other[y]["bag"]), x, y) for x in nr for y in no
            ):
                lone = len(nr) == len(no) == 1 and ref[x]["shape"] == other[y]["shape"]
                if x not in mr and y not in mo and (-neg >= PROP_MIN or lone):
                    mr[x], mo[y] = y, x
                    work.append((x, y))
    ok = {
        (i, j)
        for i, j in mr.items()
        if any(
            mr.get(a) in other[j]["succ"] + other[j]["pred"]
            for a in ref[i]["succ"] + ref[i]["pred"]
        )
    }
    return sorted(mr.items()), ok


def _bag(text, ids):
    """Token multiset of a block: normalised tokens plus the BSim feature hashes of its lines."""
    bag = Counter()
    for t in _IDENT.findall(text):
        if _LOCAL.match(t):
            t = "V"
        elif _ADDR_NAME.match(t):
            t = _ADDR_NAME.sub(r"\1_", t)
        bag[t] += 1
    for ident in ids:
        if ident.startswith("FEATURES:"):
            bag.update("#" + h for h in ident[9:].split(","))
    return bag


_NUMLIT = re.compile(r"\b(0[xX][0-9a-fA-F]+|\d+)[uUlL]*\b")


def _lits(text):
    """What BSim features do not see: constants, string literals and callee names."""
    c = Counter("s:" + x for x in _STR.findall(text))
    rest = _STR.sub(" ", text)
    c.update(
        "n:" + str(int(m.group(1), 16 if m.group(1)[1:2] in "xX" else 10))
        for m in _NUMLIT.finditer(rest)
    )
    c.update("c:" + _ADDR_NAME.sub(r"\1_", n) for n in _calls(rest))
    return c


def _same(r, o):
    """Same code up to build noise: equal `_norm`, or equal BSim features and literals.

    The feature set sees through renamed locals and different types, but not
    through a changed constant, string or callee, so those are compared apart.
    """
    if r["norm"] == o["norm"]:
        return True
    hr, ho = set(r.get("H") or r.get("hashes", ())), set(
        o.get("H") or o.get("hashes", ())
    )
    return (
        min(len(hr), len(ho)) >= HASH_MIN
        and hr == ho
        and "lits" in r
        and r["lits"] == o.get("lits")
    )


def _dice(a, b):
    tot = sum(a.values()) + sum(b.values())
    return 2 * sum((a & b).values()) / tot if tot else 1.0


def _cover(a, b):
    """Share of the tokens of `a` that `b` holds."""
    tot = sum(a.values())
    return sum((a & b).values()) / tot if tot else 0.0


def _lis(pairs):
    """Indices into `pairs` (sorted by ref index) forming a longest increasing run of j."""
    best, prev = [], []
    for k, (_, j) in enumerate(pairs):
        best.append(1)
        prev.append(-1)
        for m in range(k):
            if pairs[m][1] < j and best[m] + 1 > best[k]:
                best[k], prev[k] = best[m] + 1, m
    if not pairs:
        return set()
    k, keep = max(range(len(pairs)), key=lambda x: best[x]), set()
    while k >= 0:
        keep.add(k)
        k = prev[k]
    return keep


_CALL = re.compile(r"\b([A-Za-z_]\w*)\s*\(")
_STR = re.compile(r'"((?:[^"\\]|\\.)*)"')
_NOT_CALL = {"if", "for", "while", "switch", "return", "sizeof", "do", "else"}


def _calls(text):
    return [c for c in _CALL.findall(text) if c not in _NOT_CALL]


def _label(text, kind, rarity):
    """`<kind> . <most distinctive items>`; swap this function for a better namer.

    Kind comes from structure (see `_kind`). Items are the callees called by the
    fewest blocks of the function (`rarity`), ties by first appearance, default
    FUN_ names last; a block without calls falls back to its first string
    literal. At most two items, 24 characters each.
    """
    names = list(dict.fromkeys(_calls(text)))
    names.sort(key=lambda c: (bool(_DEFAULT_NAME.match(c)), rarity.get(c, 0)))
    items = [c[:24] for c in names[:2]]
    if not items:
        items = [m[:24] for m in _STR.findall(text)[:1] if m.strip()]
    if kind == "signature":
        head = text.split("{")[0]
        args = head[head.find("(") + 1 : head.rfind(")")].strip()
        n = 0 if args in ("", "void") else args.count(",") + 1
        return f"signature \u00b7 {n} param{'s' if n != 1 else ''}"
    return kind + (" \u00b7 " + ", ".join(items) if items else "")


def _kind(text, i, last):
    first = text.lstrip().split("(")[0].split()[:1]
    word = first[0] if first else ""
    if i == 0:
        return "signature"
    if word in ("for", "while", "do"):
        return "loop"
    if word in ("if", "switch", "else"):
        return "branch"
    stmts = [ln for ln in text.splitlines() if ln.strip().endswith(";")]
    if stmts and sum(1 for ln in stmts if _calls(ln)) * 2 >= len(stmts):
        return "call"
    return "epilogue" if last and "return" in text else "block"


def _titles(texts):
    """One deterministic label per reference block, computed once with the alignment."""
    rarity = Counter(c for t in texts for c in set(_calls(t)))
    return [
        _label(t, _kind(t, i, i == len(texts) - 1), rarity) for i, t in enumerate(texts)
    ]


def _blocks_of(src, feats, cap, max_blocks, callees=(), cfg=False):
    texts, ids, by_line, cut, f_map = _lines(src, feats, cap)
    got = _cfg_spans(src, texts, by_line, feats) if cfg and has_cfg(src) else None
    spans, info = got or (split_blocks(texts), None)
    callees_by_name = {c["name"]: c for c in callees if c.get("name")}
    out = []
    for k, (a, b) in enumerate(spans[:max_blocks]):
        text = "\n".join(texts[a:b])
        syntax = []
        # feature hashes of the block; (start, end, hashes) of its local names
        hashes, locals_ = set(), []
        base = 0
        for i in range(a, b):
            left = len(texts[i])
            col = 0
            for idx, token in by_line[i]:
                part = token["t"][:left]
                if part:
                    hs = {f["hash"] for f in f_map.get(idx, ())}
                    # ponytail: first 6 hashes per token keep the payload small
                    shown = sorted(hs)[:6]
                    hashes |= hs
                    if hs and _LOCAL.match(part):
                        locals_.append((base + col, base + col + len(part), hs))
                    col += len(part)
                    typ = token.get("type")
                    if typ == "func_call":
                        callee = callees_by_name.get(token["t"], {})
                        syntax.append(
                            [
                                part,
                                typ,
                                token.get("target_addr") or callee.get("entrypoint"),
                                token.get("target_name") or callee.get("name"),
                                bool(
                                    token.get("is_external")
                                    or callee.get("is_external")
                                ),
                                *([shown] if shown else []),
                            ]
                        )
                    elif shown:
                        syntax.append([part, typ, None, None, False, shown])
                    elif syntax and syntax[-1][1] == typ and len(syntax[-1]) < 6:
                        syntax[-1][0] += part
                    else:
                        syntax.append([part, typ])
                    left -= len(part)
            base += len(texts[i]) + 1
            if i < b - 1:
                if syntax and syntax[-1][1] is None and len(syntax[-1]) < 6:
                    syntax[-1][0] += "\n"
                else:
                    syntax.append(["\n", None])
        remaining = TEXT_CAP
        clipped = []
        for part, typ, *call in syntax:
            if remaining <= 0:
                break
            clipped.append([part[:remaining], typ, *call])
            remaining -= len(part)
        out.append(
            {
                "text": text[:TEXT_CAP],
                "syntax": clipped,
                "norm": _norm(text, a == 0),
                "lits": _lits(text),
                "bag": _bag(text, ids[a:b]),
                "hashes": hashes,
                "locals": locals_,
                "span": (a, b),
                "decl": k == 1
                and all(_DECL.match(t.strip()) for t in texts[a:b] if t.strip()),
                **(
                    {
                        **info[k],
                        "succ": [v for v in info[k]["succ"] if v < max_blocks],
                        "pred": [v for v in info[k]["pred"] if v < max_blocks],
                    }
                    if info
                    else {}
                ),
            }
        )
    return out, cut or len(spans) > max_blocks


def _match_blocks(ref, other, seeds=(), cfg_ok=()):
    """Per reference block: ('same'|'mod'|'moved'|'restr'|'miss', sim, [other indices], note).

    `seeds` are block pairs already fixed by the CFG alignment; those in `cfg_ok`
    sit at the same place in both CFGs, so a different block order is not a move.
    """
    taken_r = {i for i, _ in seeds}
    taken_o = {j for _, j in seeds}
    pairs = [(i, j, _dice(ref[i]["bag"], other[j]["bag"])) for i, j in seeds]
    cand = sorted(
        (
            (-_dice(r["bag"], o["bag"]), i, j)
            for i, r in enumerate(ref)
            if i not in taken_r
            for j, o in enumerate(other)
            if j not in taken_o
        )
    )
    for neg, i, j in cand:
        if -neg < MATCH_MIN:
            break
        if i not in taken_r and j not in taken_o:
            taken_r.add(i)
            taken_o.add(j)
            pairs.append((i, j, -neg))
    pairs.sort()
    free = [k for k, (i, j, _) in enumerate(pairs) if (i, j) not in cfg_ok]
    stay = {free[k] for k in _lis([(pairs[k][0], pairs[k][1]) for k in free])}
    stay |= {k for k, (i, j, _) in enumerate(pairs) if (i, j) in cfg_ok}
    res = {}
    for k, (i, j, sim) in enumerate(pairs):
        if k in stay:
            same = _same(ref[i], other[j])
            res[i] = ("same", 1.0, [j], "") if same else ("mod", sim, [j], "")
        else:
            res[i] = (
                "moved",
                sim,
                [j],
                "Same code in a different place: the block order differs from the reference.",
            )
    used = set(taken_o)
    for i, r in enumerate(ref):
        if i in res:
            continue
        # Merged into one block, or split over a few, of the other function.
        # The signature and declarations are not code: they absorb nothing.
        ranked = sorted(
            (j for j, o in enumerate(other) if o["span"][0] > 0 and not o["decl"]),
            key=lambda j: -_cover(r["bag"], other[j]["bag"]),
        )
        best = ranked[0] if ranked else None
        if best is not None and _cover(r["bag"], other[best]["bag"]) >= COVER_MIN:
            owner = next((x for x, p in res.items() if p[2] == [best]), None)
            note = "Merged: one block here holds this code" + (
                f" together with {_bid(owner)}." if owner is not None else "."
            )
            res[i] = ("restr", _cover(r["bag"], other[best]["bag"]), [best], note)
            used.add(best)
            continue
        union, pick = Counter(), []
        for j in ranked[:3]:
            if _cover(r["bag"], other[j]["bag"]) >= 0.25:
                union |= other[j]["bag"]
                pick.append(j)
        # a few tokens are covered by any three blocks: no split for tiny blocks
        if (
            len(pick) > 1
            and sum(r["bag"].values()) >= SPLIT_MIN
            and _cover(r["bag"], union) >= COVER_MIN
        ):
            res[i] = (
                "restr",
                _cover(r["bag"], union),
                sorted(pick),
                f"Split: the code is spread over {len(pick)} blocks here.",
            )
            used.update(pick)
        else:
            res[i] = ("miss", 0.0, [], "No matching code in this function.")
    return res, used


def _bid(i):
    return f"b{i + 1}"


def _runs(ref_text, text, changed=False, sig=False):
    """`text` as [[token run, changed]]: diffed against the reference block, or all flagged."""
    if changed:
        return [[text, True]] if text else []
    if len(text) > DIFF_CAP or len(ref_text) > DIFF_CAP:
        return [[text, False]] if text else []
    return _segments(ref_text, text, sig)


def _confirm(runs, locals_, other):
    """Clear the change flag on decompiler-named locals whose feature hash `other` also holds.

    A renamed local (`iVar3` vs `param_2`) that BSim saw in the same role on
    both sides is not a change. Other tokens keep the spelling diff.
    """
    if not runs or not locals_:
        return runs
    flags = [c for t, c in runs for _ in t]
    for s, e, hs in locals_:
        if hs & other:
            e = min(e, len(flags))
            flags[s:e] = [False] * max(e - s, 0)
    out, pos = [], 0
    for t, _ in runs:
        for k, ch in enumerate(t):
            c = flags[pos + k]
            if out and out[-1][1] == c:
                out[-1][0] += ch
            else:
                out.append([ch, c])
        pos += len(t)
    return out


def _consensus(srcs, feats, ref, cap, max_blocks=60, callees=None, mode="hybrid"):
    """Block-level comparison of the functions against the reference.

    `blocks`: the reference's blocks, each with a state per other function
    (`cells`) and the functions grouped by identical variant (`groups`).
    `funcs[j]`: the whole of function j as its blocks in its own order, each
    with the reference blocks it matches (`refs`) and its text, once. The
    reference's own code is `blocks[*].ref_text`. `extras` are blocks of other
    functions with no counterpart in the reference.
    """
    n = len(srcs)
    # Hybrid: CFG blocks when the reference has a CFG; a function without one
    # keeps the text splitter and is matched by token bags alone. `text` never
    # uses the CFG; `cfg` is hybrid with the caller having checked every function.
    use_cfg = mode != "text" and has_cfg(srcs[ref])
    got = [
        _blocks_of(s, f, cap, max_blocks, cs, use_cfg)
        for s, f, cs in zip(srcs, feats, callees or [[] for _ in srcs])
    ]
    blocks = [g[0] for g in got]
    rb = blocks[ref]
    titles = _titles([b["text"] for b in rb])
    out = [
        {
            "id": _bid(i),
            "title": titles[i],
            "ref_text": b["text"],
            "ref_syntax": b["syntax"],
            "lines": [b["span"][0] + 1, b["span"][1]],
            "hashes": len(b["H"] if "H" in b else b["hashes"]),
            **({"bb": b["bb"]} if "bb" in b else {}),
            "cells": {},
            "groups": [],
        }
        for i, b in enumerate(rb)
    ]
    funcs, extras = {}, []
    members = {}  # (block id, state, text) -> group
    for j in range(n):
        if j == ref:
            continue
        cfg_pair = "succ" in rb[0] and blocks[j] and "succ" in blocks[j][0]
        res, _ = _match_blocks(
            rb, blocks[j], *(_cfg_align(rb, blocks[j]) if cfg_pair else ())
        )
        # Local declarations differ with register allocation: never a change.
        for i, (st, sim, idx, note) in res.items():
            if st == "mod" and rb[i]["decl"] and all(blocks[j][k]["decl"] for k in idx):
                res[i] = ("same", 1.0, idx, "")
        # A block claimed by two reference blocks is shown once, on the first.
        first = {}
        for i, (st, sim, idx, note) in sorted(res.items()):
            if st != "miss" and len(idx) == 1 and first.setdefault(idx[0], i) != i:
                note = f"Merged: one block here holds this code together with {_bid(first[idx[0]])}."
                res[i] = ("restr", sim, idx, note)
        refs = {}  # j block -> [(ref index, state, sim)]
        for i, (st, sim, idx, note) in res.items():
            if st != "miss":
                for k in idx:
                    refs.setdefault(k, []).append((i, st, sim))
        entries = []
        for k, b in enumerate(blocks[j]):
            mine = sorted(refs.get(k, []))
            ent = {
                "syntax": b["syntax"],
                "refs": [
                    {"b": _bid(i), "st": st, "sim": round(sim, 3)}
                    for i, st, sim in mine
                ],
                **({"bb": b["bb"]} if "bb" in b else {}),
            }
            if mine:
                i, st, _ = mine[0]
                if (st == "same" and len(mine) == 1) or b["decl"]:
                    ent["text"] = [[b["text"], False]]
                else:
                    # merged or split: diff against every reference block it maps to
                    ref_text = "\n".join(rb[m[0]]["text"] for m in mine)
                    ent["text"] = _confirm(
                        _runs(ref_text, b["text"], sig=b["span"][0] == 0),
                        b["locals"],
                        set().union(*(rb[m[0]]["hashes"] for m in mine)),
                    )
            else:
                ent["text"] = [[b["text"], False]]
                if len(_IDENT.findall(b["text"])) >= 3:
                    for e in extras:
                        if _dice(e["bag"], b["bag"]) >= 0.8:
                            e["funcs"].append(j)
                            ent["x"] = e["id"]
                            break
                    else:
                        if len(extras) >= 40:
                            entries.append(ent)
                            continue
                        ent["x"] = f"x{len(extras) + 1}"
                        extras.append(
                            {
                                "id": ent["x"],
                                "funcs": [j],
                                "text": b["text"],
                                "syntax": b["syntax"],
                                "bag": b["bag"],
                            }
                        )
            entries.append(ent)
        funcs[str(j)] = entries
        for i, (st, sim, idx, note) in res.items():
            cell = {"st": st, "sim": round(sim, 3), "segs": idx}
            if st == "restr" and len(idx) == 1:
                owner = min(r[0] for r in refs[idx[0]])
                if owner != i:
                    cell["into"] = _bid(owner)
            out[i]["cells"][str(j)] = cell
            if st == "same":
                continue
            text = "\n".join(blocks[j][k]["text"] for k in idx)
            g = members.setdefault(
                (i, st, _norm(text, any(blocks[j][k]["span"][0] == 0 for k in idx))),
                {"funcs": [], "sims": {}, "st": st, "note": note, "text": text},
            )
            g["funcs"].append(j)
            g["sims"][str(j)] = round(sim, 3)
    for (i, st, _), g in members.items():
        text = g.pop("text")
        if st in ("mod", "moved"):
            j0 = g["funcs"][0]
            idx = out[i]["cells"][str(j0)]["segs"]
            g["ref_segs"] = (
                _confirm(
                    _segments(text, out[i]["ref_text"], rb[i]["span"][0] == 0),
                    rb[i]["locals"],
                    set().union(*(blocks[j0][k]["hashes"] for k in idx)),
                )
                if len(text) <= DIFF_CAP
                else None
            )
        if st == "mod":
            ch = sum(
                1
                for _, c in funcs[str(g["funcs"][0])][
                    out[i]["cells"][str(g["funcs"][0])]["segs"][0]
                ]["text"]
                if c
            )
            g["note"] = (
                f"{ch} changed run{'s' if ch != 1 else ''} of tokens."
                if ch
                else "Only renamed locals differ."
            )
        out[i]["groups"].append(g)
    cfg = {}
    for j in range(n):
        if blocks[j] and "succ" in blocks[j][0]:
            match = (
                {str(i): _bid(i) for i in range(len(rb))}
                if j == ref
                else {
                    str(k): e["refs"][0]["b"]
                    for k, e in enumerate(funcs[str(j)])
                    if e["refs"]
                }
            )
            cfg[str(j)] = {
                "nodes": [{"bb": b["bb"]} for b in blocks[j]],
                "edges": [[u, v] for u, b in enumerate(blocks[j]) for v in b["succ"]],
                "match": match,
            }
    # whole-function key: functions with equal keys have the same code
    keys = {
        str(j): _norm(
            (
                "\n".join(b["text"] for b in rb)
                if j == ref
                else "\n".join("".join(t for t, _ in e["text"]) for e in funcs[str(j)])
            ),
            True,
        )
        for j in range(n)
    }
    return {
        "reference": ref,
        "cfg": cfg,
        "keys": keys,
        "blocks": out,
        "funcs": funcs,
        "extras": [
            {k: e[k] for k in ("id", "funcs", "text", "syntax")}
            | {"title": _label(e["text"], _kind(e["text"], 1, False), {})}
            for e in extras[:40]
        ],
        "truncated": any(g[1] for g in got),
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


BLOCK_MODES = ("hybrid", "cfg", "text")


def compute(r, doc, fids, mode="hybrid"):
    """Facets, similarity matrix and (for small N) consensus code of one row.

    `mode` picks the blocks: `hybrid` (Ghidra basic blocks where stored, text
    splitting elsewhere), `cfg` (Ghidra only; `blocks_error` when a function has
    none) or `text` (text splitting only).
    """
    cache_key = (doc["key"], tuple(fids), mode)
    hit = _CACHE.get(cache_key)
    if hit is not None:
        return hit
    n = len(fids)
    facet_max, code_max, line_cap, block_cap = limits()
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
                "collection": split_fid(f)[0],
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
        "limits": {
            "facets": facet_max,
            "code": code_max,
            "lines": line_cap,
            "blocks": block_cap,
        },
    }
    out["blocks_mode"] = mode
    if level == "full":
        out["cfg_funcs"] = [has_cfg(x) for x in srcs]
        missing = [j for j, ok in enumerate(out["cfg_funcs"]) if not ok]
        if mode == "cfg" and missing:
            out["blocks_error"] = {"reason": "no_cfg", "missing": missing}
        else:
            out["consensus"] = _consensus(
                srcs,
                feats,
                ref,
                line_cap,
                block_cap,
                [m.get("callees") or [] for m in metas],
                mode,
            )
    _CACHE.put(cache_key, out)
    return out


# ---- LLM block titles: on demand, cached, never part of the alignment answer ----
_TITLE_TTL = 30 * 86400
_TITLE_RETRY = 120  # seconds before a failed naming may be tried again
_TITLE_PENDING_MAX = 180  # a naming still pending after this is treated as failed
_TITLE_STATE = {}  # key -> {"status": "pending"|"failed", "at": time}
_TITLE_LOCK = threading.Lock()
_TITLE_TEXT_CAP = 800


def llm_configured():
    return bool(config_service.get("llm.ollama_url"))


def titles_key(consensus):
    """Content key: the reference function's block texts."""
    blob = json.dumps([[b["id"], b["ref_text"]] for b in consensus["blocks"]])
    return hashlib.sha1(blob.encode()).hexdigest()


def _cached_llm(prefix, key, work):
    """{'status': off|pending|ready|failed, 'value': ...} for one LLM answer.

    The first call after a cache miss runs `work()` (returns a JSON-able value
    or None) in a background thread; later calls (any user) read the Redis /
    in-process cache. Never waits for the model.
    """
    from bsimvis.app.services.redis_client import get_queue_redis

    if not llm_configured():
        return {"status": "off"}
    skey = (prefix, key)

    def run():
        value = work()
        with _TITLE_LOCK:
            if value:
                try:
                    get_queue_redis().set(
                        f"{prefix}:{key}", json.dumps(value), ex=_TITLE_TTL
                    )
                except Exception:
                    pass
                _TITLE_STATE[skey] = {"status": "ready", "value": value}
            else:
                _TITLE_STATE[skey] = {"status": "failed", "at": time.time()}

    with _TITLE_LOCK:
        st = _TITLE_STATE.get(skey)
        if st and st["status"] == "ready":
            return st
        if st and st["status"] == "pending":
            if time.time() - st["at"] < _TITLE_PENDING_MAX:
                return {"status": "pending"}
            st = _TITLE_STATE[skey] = {"status": "failed", "at": time.time()}
        if st and time.time() - st["at"] < _TITLE_RETRY:
            return {"status": "failed"}
        try:
            raw = get_queue_redis().get(f"{prefix}:{key}")
        except Exception:
            raw = None
        if raw:
            st = _TITLE_STATE[skey] = {"status": "ready", "value": json.loads(raw)}
            return st
        _TITLE_STATE[skey] = {"status": "pending", "at": time.time()}
    threading.Thread(target=run, daemon=True).start()
    return {"status": "pending"}


def block_titles(consensus):
    """{'status': off|pending|ready|failed, 'titles': {block id: title}} for a consensus."""
    from bsimvis.app.services.llm_service import llm_service

    blocks = consensus["blocks"]

    def work():
        titles, _ = llm_service.name_blocks(
            [(b["id"], b["ref_text"][:_TITLE_TEXT_CAP]) for b in blocks]
        )
        return titles

    out = _cached_llm("nway:titles", titles_key(consensus), work)
    return {"status": out["status"], "titles": out.get("value") or {}}


_EXPLAIN_FN_CAP = (
    5000  # chars of one function shown to the model; the target block is cut last
)


def _fn_view(parts):
    """[(header, text, is_target)] -> annotated source, trimming non-target blocks to fit."""
    tcap = 1600
    used = sum(min(len(t), tcap) for _, t, hit in parts if hit)
    rest = [p for p in parts if not p[2]]
    per = max(150, (_EXPLAIN_FN_CAP - used) // max(1, len(rest)))
    out = []
    for head, text, hit in parts:
        if hit:
            out.append(f"// {head}  <<< TARGET\n{text[:tcap]}")
        else:
            cut = "\n// ..." if len(text) > per else ""
            out.append(f"// {head}\n{text[:per]}{cut}")
    return "\n".join(out)


def explain_block(consensus, block_id, j, fids=None):
    """{'status': ..., 'verdict': cosmetic|semantic|unclear, 'summary': str}.

    Explains how function `j` differs from the reference in one block. The
    model sees both whole functions (other blocks trimmed), the state of every
    block, and the functions' stored LLM summaries; cached by that context.
    """
    from bsimvis.app.services.llm_service import llm_service

    blocks = consensus["blocks"]
    block = next((b for b in blocks if b["id"] == block_id), None)
    cell = block and block["cells"].get(str(j))
    if not cell or cell["st"] == "same":
        return {"status": "off"}
    segs = consensus["funcs"].get(str(j)) or []
    own = set(cell["segs"])
    ref_view = _fn_view(
        [(f"[{b['id']}] {b['title']}", b["ref_text"], b is block) for b in blocks]
    )
    other_view = _fn_view(
        [
            (
                (
                    "matches " + ", ".join(r["b"] for r in seg["refs"])
                    if seg["refs"]
                    else "no counterpart in the reference"
                ),
                "".join(t for t, _ in seg["text"]),
                k in own,
            )
            for k, seg in enumerate(segs)
        ]
    )
    outline = "; ".join(
        f"{b['id']} {b['title']}: {(b['cells'].get(str(j)) or {'st': 'same'})['st']}"
        for b in blocks
    )
    notes = function_summaries(fids) if fids else {}
    ctx = {
        "target": block_id,
        "state": cell["st"],
        "ref": ref_view,
        "other": other_view,
        "outline": outline,
        "notes": [notes.get(f, "") for f in (fids or [])],
    }
    key = hashlib.sha1(json.dumps(ctx).encode()).hexdigest()

    def work():
        value, _ = llm_service.explain_block_change(ctx)
        return value

    out = _cached_llm("nway:explain", key, work)
    return {"status": out["status"], **(out.get("value") or {})}


def function_summaries(fids):
    """{function id: first line of its newest LLM note}; only functions that have one."""
    from bsimvis.app.services.llm_batch_service import LLM_NOTE_OWNER
    from bsimvis.app.services.note_service import note_service

    out = {}
    for f in fids:
        coll = split_fid(f)[0]
        notes = [
            n
            for n in note_service.get_notes(coll, f) or []
            if n.get("owner") == LLM_NOTE_OWNER and (n.get("text") or "").strip()
        ]
        if notes:
            newest = max(notes, key=lambda n: n.get("timestamp", 0))
            out[f] = newest["text"].strip().splitlines()[0][:240]
    return out


def demo():
    assert letter(0) == "A" and letter(25) == "Z" and letter(26) == "AA"
    assert _const("0x400") == ("1024", "0x400 (1024)") and _const("5u") == ("5", "5")
    assert _const("abc") is None
    assert _norm("call  FUN_00012a40( x )") == "call FUN_ ( x )"
    # build noise: locals, types, casts, generated names (also with a space prefix)
    assert _norm("iVar20 = FUN_ram_0001d6fa();") == _norm("local_60 = FUN_00419abc();")
    assert _norm("x = (long)iVar2 + 1;") == _norm("x = (uint)uVar9 + 1;")
    assert _norm("undefined4 a;") == _norm("int a;")
    # real changes survive: constant, callee, string, positional param, operator
    for old, new in [
        ("a = b * 7;", "a = b * 9;"),
        ("a = g1(b);", "a = g2(b);"),
        ('puts("zero");', 'puts("none");'),
        ("a = param_1;", "a = param_2;"),
        ("a = b + c;", "a = b - c;"),
    ]:
        assert _norm(old) != _norm(new), (old, new)
    assert _segments("iVar1 = FUN_ram_0001d6fa(0);", "local_60 = FUN_00419abc(0);") == [
        ["local_60 = FUN_00419abc(0);", False]
    ]
    assert _norm("\nint f_a(int param_1)\n{", True) == _norm(
        "int f_b(int param_1)\n{", True
    )
    assert _norm("int f_a(int param_1)") != _norm("int f_b(int param_1)")
    assert _segments("a = g1(5);", "a = (int)g2(5);") == [
        ["a = (int)", False],
        ["g2", True],
        ["(5);", False],
    ]
    # BSim features agree but a constant moved: the literals break the tie
    blk = lambda text, hs: {
        "norm": _norm(text),
        "lits": _lits(text),
        "hashes": hs,
    }
    hs = {"a", "b", "c", "d"}
    assert _same(blk("x = y * 7;", hs), blk("x = (long)y * 7L;", hs) | {"norm": "z"})
    assert not _same(blk("x = y * 7;", hs), blk("x = y * 9;", hs))
    assert not _same(blk("x = y * 7;", hs), blk("x = y * 7 + 0;", {"a"}))
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
    code = lambda *ls: list(ls)
    ref = [
        "int f(int a)",
        "{",
        "  int i;",
        "  for (i = 0; i < a; i++) {",
        "    g(i, 512);",
        "  }",
        "  if (a == 0) {",
        '    puts("nope"); abort();',
        "  }",
        "  h(a);",
        "  return 0;",
        "}",
    ]
    mod = [x.replace("512", "1024") for x in ref]
    moved = ref[:3] + ref[6:9] + ref[3:6] + ref[9:]
    gone = ref[:6] + ref[9:]
    spans = split_blocks(ref)
    assert spans == [(0, 2), (2, 3), (3, 6), (6, 9), (9, 11)], spans
    mk = lambda ls: [
        {
            "text": "\n".join(ls[a:b]),
            "norm": _norm("\n".join(ls[a:b])),
            "bag": _bag("\n".join(ls[a:b]), []),
            "span": (a, b),
            "decl": False,
        }
        for a, b in split_blocks(ls)
    ]
    R = mk(ref)
    st = lambda o: {i: v[0] for i, v in _match_blocks(R, mk(o))[0].items()}
    assert set(st(ref).values()) == {"same"}
    assert st(mod)[2] == "mod" and "moved" in st(moved).values()
    assert st(gone)[3] == "miss"
    assert _dice(Counter("aab"), Counter("ab")) == 0.8
    typed = {
        "c_tokens": [
            {"t": "void", "line": 0, "type": "type"},
            {"t": " ", "line": 0, "type": "syntax"},
            {"t": "f", "line": 0, "type": "func_call"},
            {"t": "()  ", "line": 0, "type": "syntax"},
            {"t": "{", "line": 1, "type": "syntax"},
            {"t": "  return 0;  ", "line": 2, "type": "op"},
            {"t": "}", "line": 3, "type": "syntax"},
        ]
    }
    for block in _blocks_of(typed, [], 100, 60)[0]:
        assert "".join(part for part, *_ in block["syntax"]) == block["text"]
    first = _blocks_of(typed, [], 100, 60, [{"name": "f", "entrypoint": "2000"}])[0][0]
    assert first["syntax"][0] == ["void", "type"]
    assert ["f", "func_call", "2000", "f", False] in first["syntax"]
    src = lambda ls: {
        "c_tokens": [{"t": t, "line": i, "type": None} for i, t in enumerate(ls)]
    }
    out = _consensus(
        [src(ref), src(mod), src(moved), src(gone)], [[], [], [], []], 0, 100
    )
    assert [b["cells"]["1"]["st"] for b in out["blocks"]] == [
        "same",
        "same",
        "mod",
        "same",
        "same",
    ]
    assert out["blocks"][3]["cells"]["3"]["st"] == "miss"
    assert any(c["st"] == "moved" for b in out["blocks"] for c in [b["cells"]["2"]])
    whole = "\n".join(x["text"][0][0] for x in out["funcs"]["3"])
    assert whole.count("h(a)") == 1 and out["blocks"][2]["groups"][0]["funcs"] == [1]
    toks = lambda name: {
        "c_tokens": [
            {"t": "{", "line": 0, "type": "syntax"},
            {"t": "  r = ", "line": 1, "type": None},
            {"t": name, "line": 1, "type": "variable"},
            {"t": " + 1;", "line": 1, "type": None},
            {"t": "}", "line": 2, "type": "syntax"},
        ]
    }
    feat = lambda h: [{"hash": h, "addr_to_token_idx": [2]}]
    changed = lambda o: any(c for e in o["funcs"]["1"] for t, c in e["text"])
    run = lambda h1, h2: _consensus(
        [toks("param_2"), toks("iVar3")], [feat(h1), feat(h2)], 0, 100
    )
    got = [
        e
        for b in _blocks_of(toks("param_2"), feat("a1"), 100, 60)[0]
        for e in b["syntax"]
    ]
    assert ["param_2", "variable", None, None, False, ["a1"]] in got
    assert changed(run("a1", "b2"))  # hashes differ: the spelling diff stands
    assert not changed(run("a1", "a1"))  # same hash: a renamed local, not a change

    # CFG path: blocks come from tok_block / cfg, features carry block_index.
    def gsrc(lines, edges, nfeat=3):
        toks = [
            {"t": t, "line": i, "type": None, "seq": "s" if b is not None else None}
            for i, (t, b) in enumerate(lines)
        ]
        tb, last = [], object()
        for k, (_, b) in enumerate(lines):
            b = -1 if b is None else b
            if b != last:
                tb.append([k, b])
                last = b
        bbs = sorted({b for _, b in lines if b is not None})
        cfg = {
            "entry": 0,
            "blocks": [
                {"i": b, "ops": 1, "kind": "plain", "out": edges.get(b, [])}
                for b in bbs
            ],
        }
        return {"c_tokens": toks, "tok_block": tb, "cfg": cfg}

    gfeats = lambda hs: [
        {"hash": h, "block_index": b, "addr_to_token_idx": []} for b, h in hs
    ]
    a_lines = [
        ("void f(int a)", None),
        ("{", None),
        ("  if (a) {", 0),
        ("    p(a);", 1),
        ("  }", None),
        ("  q(a);", 2),
        ("  r(a);", 3),
        ("}", None),
    ]
    a_edges = {0: [1, 2], 1: [2], 2: [3]}
    a_f = gfeats([(0, "h0"), (1, "h1"), (1, "h1b"), (2, "h2"), (3, "h3")])
    spans, info = _cfg_spans(
        gsrc(a_lines, a_edges),
        [t for t, _ in a_lines],
        {i: [(i, {})] for i in range(len(a_lines))},
        a_f,
    )
    assert spans == [(0, 2), (2, 3), (3, 5), (5, 8)], spans  # BB 2 and 3 are a chain
    assert info[1]["succ"] == [2, 3] and info[3]["pred"] == [1, 2]
    # B inserts a block on the else path; same hashes elsewhere, one block renamed
    b_lines = a_lines[:5] + [("  z = 1;", 4)] + a_lines[5:]
    b_lines = [(t.replace("q(a)", "q(b)"), b) for t, b in b_lines]
    b_edges = {0: [1, 4], 1: [2], 4: [2], 2: [3]}
    b_f = a_f + gfeats([(4, "h4")])
    out = _consensus(
        [gsrc(a_lines, a_edges), gsrc(b_lines, b_edges)], [a_f, b_f], 0, 100
    )
    assert [b["cells"]["1"]["st"] for b in out["blocks"]][:2] == ["same", "same"]
    assert out["cfg"]["1"]["match"]["0"] == "b1" and out["blocks"][1]["bb"] == [0]
    assert any(not e["refs"] for e in out["funcs"]["1"])  # the inserted block
    # a function without cfg is matched by the text splitter, no crash
    mixed = _consensus(
        [gsrc(a_lines, a_edges), src([t for t, _ in a_lines])], [a_f, []], 0, 100
    )
    assert "1" not in mixed["cfg"] and "0" in mixed["cfg"]
    assert (
        _consensus([src(ref), gsrc(a_lines, a_edges)], [[], a_f], 0, 100)["cfg"] == {}
    )
    # text mode ignores a stored CFG
    flat = _consensus(
        [gsrc(a_lines, a_edges), gsrc(b_lines, b_edges)],
        [a_f, b_f],
        0,
        100,
        mode="text",
    )
    assert flat["cfg"] == {} and all("bb" not in b for b in flat["blocks"])
    print("nway_facets_service demo ok")


if __name__ == "__main__":
    demo()
