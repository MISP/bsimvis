"""Live YARA workbench: compile a rule, scan a target set and a negative set from
the stored `{coll}:file:{md5}:raw` bytes, and report how well it separates them.

Nothing here runs Ghidra. A hit's file offset is mapped to an address with LIEF
and to a function with the function list already in Kvrocks, the same
offset -> address -> function walk the worker does with a live program.

Registry (global, versioned; see doc/kvrocks_database_structure.md):

    global:yara_rules                       ZSET  rule uuid -> updated ts
    global:yara_rule:{uuid}                 JSON  current version + scope
    global:yara_rule:{uuid}:v:{n}           JSON  one saved version
    global:yara_rule:{uuid}:tags            Set   the `yara:custom:` tags it has applied
"""

import hashlib
import io
import json
import re
import time
import uuid as uuidlib

from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.nway_indicator_service import (  # noqa: F401
    consts_in,
    string_score,
)

MAX_OFFSETS = 8  # offsets kept per string instance list
LIST_CAP = 50  # missed / false-hit files returned
CUSTOM_TAG = "yara:custom:"
_RULE_RE = re.compile(r"^\s*(?:(?:private|global)\s+)*rule\s+(\w+)", re.M)
_STRING_RE = re.compile(r"^\s*(\$\w*)\s*=")


def cfg(key, default):
    return config_service.get(f"yara_editor.{key}", default)


def _txt(v):
    return v.decode() if isinstance(v, bytes) else str(v)


# --- compile -----------------------------------------------------------------


def _error(e):
    m = re.match(r"line (\d+): (.*)", str(e))
    return {"line": int(m.group(1)) if m else 1, "message": m.group(2) if m else str(e)}


def compile_source(text):
    """`(rules, errors)`; exactly one is empty. `includes=False`: a rule typed in the
    browser must not make the server read files."""
    import yara

    try:
        return yara.compile(source=text, includes=False), []
    except yara.Error as e:
        return None, [_error(e)]


def rule_names(text):
    return _RULE_RE.findall(text)


def defined_strings(text):
    """`[(rule, $id)]` for every string a rule defines, read off the source text."""
    out, rule = [], None
    for line in text.splitlines():
        m = _RULE_RE.match(line)
        if m:
            rule = m.group(1)
            continue
        m = _STRING_RE.match(line)
        if m and rule:
            out.append((rule, m.group(1)))
    return out


# --- lint ---------------------------------------------------------------------

META_KEYS = ("description", "author", "date", "kind", "reference_md5")
KINDS = {"family", "variant", "capability", "packer", "generic"}


EXTERNAL_MAX = 200_000


def _yarax_item(d, level):
    return {
        "message": d["title"],
        "line": d.get("line"),
        "column": d.get("column"),
        "code": d.get("code"),
        "level": level,
        "detail": "\n".join(
            [x["text"] for x in d.get("labels", []) if x.get("text")]
            + [x["text"] for x in d.get("footers", [])]
        )
        or None,
    }


def _yarax(text):
    import io

    import yara_x

    c = yara_x.Compiler()
    c.enable_includes(False)  # a rule must never read a file of this machine
    try:
        c.add_source(text)
    except yara_x.CompileError:
        pass
    errors = [_yarax_item(e, "error") for e in c.errors()]
    warnings = [_yarax_item(w, "warning") for w in c.warnings()]
    out = {
        "available": True,
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
    }
    try:
        buf = io.BytesIO()
        yara_x.Formatter().format(io.BytesIO(text.encode()), buf)
        fixed = buf.getvalue().decode()
        if fixed != text:
            out["fixed_text"] = fixed
    except Exception:  # not formattable: advice only
        pass
    return out


def external_checks(text):
    """Advice from tools outside libyara, for the editor's Checks panel:
    `{tool: {available, ok, reason?, errors, warnings, fixed_text?}}`. A tool that
    is missing or crashes says so instead of failing the call."""
    if len(text) > EXTERNAL_MAX:
        return {}
    out = {}
    try:
        out["yarax"] = _yarax(text)
    except Exception as e:
        out["yarax"] = {
            "available": False,
            "ok": False,
            "reason": f"YARA-X unavailable: {e}",
            "errors": [],
            "warnings": [],
        }
    return out


def lint(text):
    """Style / robustness warnings for a rule, `[{rule, level, message}]`. Reads the
    source text only; the measured side (P/R) is /test."""
    out = []
    for block in re.split(r"(?m)^(?=\s*(?:(?:private|global)\s+)*rule\s)", text):
        m = _RULE_RE.match(block)
        if not m:
            continue
        name = m.group(1)

        def warn(level, msg):
            out.append({"rule": name, "level": level, "message": msg})

        meta = {}
        for k, _q, v in re.findall(
            r'(?m)^\s+(\w+)\s*=\s*("?)(.*?)\2\s*$', block.split("strings:")[0]
        ):
            meta[k] = v
        for k in META_KEYS:
            if k not in meta:
                warn("warn", f"meta `{k}` missing")
        if meta.get("kind") and meta["kind"] not in KINDS:
            warn("warn", f"meta kind `{meta['kind']}` not one of {sorted(KINDS)}")
        need = "capability" if meta.get("kind") == "capability" else "family"
        if meta.get("kind") != "generic" and need not in meta:
            warn("warn", f"meta `{need}` missing")
        cond = block.split("condition:")[-1]
        if not re.search(
            r"uint32\(0\)\s*==\s*0x464c457f|uint32be\(0\)\s*==\s*0x7f454c46|elf\.", cond
        ):
            warn("warn", "condition does not check the ELF magic")
        if "filesize" not in cond:
            warn("info", "no filesize bound: scans huge files and matches in archives")
        strings = re.findall(
            r"(?m)^\s*(\$\w*)\s*=\s*(.+)$", block.split("condition:")[0]
        )
        if not strings:
            warn("warn", "rule has no strings (module/filesize-only rule)")
        if len(strings) == 1 and "any of them" in cond:
            warn(
                "warn", "one string decides the rule: add a second, independent anchor"
            )
        for sid, val in strings:
            val = val.strip()
            if val.startswith('"'):
                lit = val[1:].split('"')[0]
                if len(lit) < 6:
                    warn("warn", f"{sid} is only {len(lit)} chars: too common")
            elif val.startswith("{"):
                fixed = len(re.findall(r"(?<![\w?])[0-9A-Fa-f]{2}(?![\w?])", val))
                if fixed < 6:
                    warn("warn", f"{sid} has only {fixed} fixed bytes")
                if re.search(r"(?:\?\?\s*){6,}", val):
                    warn("info", f"{sid} has a long wildcard run")
    return out


# --- scan --------------------------------------------------------------------


def scan_bytes(rules, data):
    """`{rule: {$id: [offsets]}}` for one file."""
    out = {}
    for m in rules.match(data=data, timeout=cfg("scan_timeout", 20)):
        out[m.rule] = {
            s.identifier: [i.offset for i in s.instances][:MAX_OFFSETS]
            for s in m.strings
        }
    return out


def _fid(coll, md5):
    return f"{coll}:file:{md5}"


def _sample(groups, targets, cap_files, cap_bytes, sizes):
    """Negatives to scan: round-robin over the negative groups (so a small group
    is not drowned by a big one), in md5 hash order so the sample is stable
    between keystrokes. Returns `(members, total_available)`."""
    seen, queues = set(targets), []
    for g in groups:
        q = [m for m in g["members"] if m not in seen]
        seen.update(q)
        queues.append(
            sorted(q, key=lambda m: hashlib.md5("".join(m).encode()).digest())
        )
    total = sum(len(q) for q in queues)
    out, used = [], 0
    while any(queues) and len(out) < cap_files:
        for q in queues:
            if not q:
                continue
            m = q.pop(0)
            size = sizes.get(m, 0)
            if used + size > cap_bytes and out:
                return out, total
            out.append(m)
            used += size
    return out, total


def file_label(r, m):
    """`{id: "coll:md5", name}` for a (collection, md5) member."""
    meta = r.get(f"{_fid(*m)}:meta")
    try:
        name = json.loads(meta).get("file_name") if meta else None
    except ValueError:
        name = None
    return {"id": f"{m[0]}:{m[1]}", "name": name or m[1]}


def _ratio(a, b):
    return round(a / b, 4) if b else None


def _scan_sets(r_raw, rules, targets, negatives):
    """Scan every target and a capped sample of the negatives.
    Returns `(tmembers, nmembers, hits, truncated, n_total, unreadable)`."""
    t_cap = cfg("target_cap_files", 2000)
    tmembers = list(dict.fromkeys(m for g in targets for m in g["members"]))
    truncated = len(tmembers) > t_cap
    tmembers = tmembers[:t_cap]
    tset = set(tmembers)

    pipe = r_raw.pipeline(transaction=False)
    pool = list(
        dict.fromkeys(m for g in negatives for m in g["members"] if m not in tset)
    )
    for m in pool:
        pipe.strlen(f"{_fid(*m)}:raw")
    sizes = dict(zip(pool, pipe.execute()))
    nmembers, n_total = _sample(
        negatives,
        tmembers,
        cfg("neg_cap_files", 500),
        cfg("neg_cap_mb", 500) * 1024 * 1024,
        sizes,
    )

    hits, unreadable = {}, []
    for role, members in (("pos", tmembers), ("neg", nmembers)):
        for m in members:
            data = r_raw.get(f"{_fid(*m)}:raw")
            if not data:
                unreadable.append(m)
                continue
            hits[m] = (role, scan_bytes(rules, data))

    return tmembers, nmembers, hits, truncated, n_total, unreadable


def evaluate(r, r_raw, text, targets, negatives):
    """Compile `text` and measure it. `targets` / `negatives` are lists of
    `{label, members: [(coll, md5)]}`. Returns the report dict."""
    t0 = time.time()
    rules, errors = compile_source(text)
    names = rule_names(text)
    if rules is None:
        return {"ok": False, "errors": errors, "rules": names}

    tmembers, nmembers, hits, truncated, n_total, unreadable = _scan_sets(
        r_raw, rules, targets, negatives
    )
    pos = [m for m in tmembers if m in hits]
    neg = [m for m in nmembers if m in hits]
    tp = [m for m in pos if hits[m][1]]
    fp = [m for m in neg if hits[m][1]]
    # no negatives scanned: precision is unknown, not 100%
    precision = _ratio(len(tp), len(tp) + len(fp)) if neg else None
    recall = _ratio(len(tp), len(pos))
    f1 = (
        round(2 * precision * recall / (precision + recall), 4)
        if precision and recall
        else None if precision is None or recall is None else 0.0
    )

    groups = []
    for role, gs, scanned in (
        ("target", targets, set(pos)),
        ("negative", negatives, set(neg)),
    ):
        for g in gs:
            mem = [m for m in g["members"] if m in scanned]
            groups.append(
                {
                    "label": g["label"],
                    "role": role,
                    "files": len(mem),
                    "hits": sum(1 for m in mem if hits[m][1]),
                    "hit_rate": _ratio(sum(1 for m in mem if hits[m][1]), len(mem)),
                }
            )

    per_rule = {n: [0, 0] for n in names}
    per_string = {(rule, sid): [0, 0] for rule, sid in defined_strings(text)}
    for m, (role, found) in hits.items():
        i = 0 if role == "pos" else 1
        for rule, strs in found.items():
            per_rule.setdefault(rule, [0, 0])[i] += 1
            for sid in strs:
                per_string.setdefault((rule, sid), [0, 0])[i] += 1

    def label(m):
        return file_label(r, m)

    missed = [label(m) for m in pos if not hits[m][1]][:LIST_CAP]
    false_hits = [{**label(m), "rules": sorted(hits[m][1])} for m in fp[:LIST_CAP]]
    true_hits = [{**label(m), "rules": sorted(hits[m][1])} for m in tp[:LIST_CAP]]
    for rows, members in ((false_hits, fp), (true_hits, tp)):
        detail = explain_hits(r, r_raw, rules, members)
        for f in rows:
            f["matches"] = detail.get(f["id"], [])
    shown = (
        [m for m in pos if not hits[m][1]][:LIST_CAP] + fp[:LIST_CAP] + tp[:LIST_CAP]
    )
    cl = file_clusters(r, shown)
    for rows, members in (
        (missed, [m for m in pos if not hits[m][1]]),
        (false_hits, fp),
        (true_hits, tp),
    ):
        for f, m in zip(rows, members):
            f["clusters"] = _slim(cl.get(m, []))

    landing = locate_hits(
        r, r_raw, [(m, hits[m][1]) for m in tp][: cfg("land_files", 20)]
    )
    return {
        "ok": True,
        "errors": [],
        "rules": names,
        "metrics": {
            "tp": len(tp),
            "fn": len(pos) - len(tp),
            "fp": len(fp),
            "tn": len(neg) - len(fp),
            "precision": precision,
            "recall": recall,
            "f1": f1,
        },
        "sample": {
            "targets": len(pos),
            "targets_truncated": truncated,
            "negatives_available": n_total,
            "negatives_scanned": len(neg),
            "sampled": len(neg) < n_total,
            "unreadable": len(unreadable),
        },
        "groups": groups,
        "rules_stats": [
            {"rule": k, "pos": v[0], "neg": v[1]} for k, v in per_rule.items()
        ],
        "strings": [
            {
                "rule": k[0],
                "id": k[1],
                "pos": v[0],
                "neg": v[1],
                "score": string_score(v[0], len(pos), v[1], len(neg)),
            }
            for k, v in per_string.items()
        ],
        "missed": missed,
        "false_hits": false_hits,
        "true_hits": true_hits,
        "landing": landing,
        "ms": int((time.time() - t0) * 1000),
    }


# --- hit offset -> function / string ----------------------------------------


class Locator:
    """One file's offset -> (function, string) lookup. LIEF and the function list
    are loaded on first use, so a file nobody asks about costs nothing."""

    def __init__(self, r, coll, md5, raw):
        self.r, self.coll, self.md5, self.raw = r, coll, md5, raw
        self._ready = False

    def _load(self):
        self._ready = True
        self.binary = None
        try:
            import lief

            lief.logging.disable()
            self.binary = lief.parse(io.BytesIO(self.raw))
        except Exception:
            pass
        meta = self.r.get(f"{_fid(self.coll, self.md5)}:meta")
        try:
            base = (json.loads(meta) if meta else {}).get("image_base")
            self.ghidra_base = int(base, 16) if base else None
        except (ValueError, TypeError):
            self.ghidra_base = None
        fids = self.r.smembers(f"{self.coll}:idx:file:functions:{self.md5}")
        self.funcs = sorted(
            int(_txt(f).removesuffix(":meta").rsplit(":", 1)[-1], 16) for f in fids
        )
        self.strings = None

    def to_ghidra(self, offset):
        """File offset -> Ghidra address, or None."""
        if self.binary is None:
            return None
        try:
            va = self.binary.offset_to_virtual_address(offset)
        except Exception:
            return None
        if not isinstance(va, int) or va < 0:
            return None
        base = self.binary.imagebase
        if va < base:  # PE gives an RVA
            va += base
        if self.ghidra_base is None:
            return va
        return self.ghidra_base + va - base

    def function_at(self, va):
        """`(entry, name)` of the function whose body holds `va`, or None."""
        import bisect

        i = bisect.bisect_right(self.funcs, va) - 1
        if i < 0:
            return None
        start = self.funcs[i]
        fid = f"{self.coll}:func:{self.md5}:{start:08x}"
        end = self.function_end(fid, start)
        if va >= end:
            return None
        meta = self.r.get(f"{fid}:meta")
        try:
            name = (json.loads(meta) if meta else {}).get("function_name")
        except ValueError:
            name = None
        return start, name or f"FUN_{start:08x}"

    def function_end(self, fid, start):
        """Exclusive end of a function: its last CFG block, else a ceiling of
        15 bytes (x86 max) per instruction -- ponytail: no stored byte size, and
        the cfg is only there when the decompile ran."""
        src = self.r.get(f"{fid}:source")
        try:
            blocks = (json.loads(src) if src else {}).get("cfg", {}).get("blocks")
        except ValueError:
            blocks = None
        if blocks:
            return max(int(b["stop"], 16) for b in blocks) + 1
        meta = self.r.get(f"{fid}:meta")
        try:
            n = (json.loads(meta) if meta else {}).get("instruction_count") or 1
        except ValueError:
            n = 1
        return start + 15 * n

    def fn_name(self, entry):
        meta = self.r.get(f"{self.coll}:func:{self.md5}:{entry}:meta")
        try:
            name = (json.loads(meta) if meta else {}).get("function_name")
        except ValueError:
            name = None
        return name or f"FUN_{entry}"

    def _load_strings(self):
        """`self.strings`: (start, end, rec) for rows with a real address;
        `self.records`: every row. A backfilled table (scripts/backfill_strings.py)
        keys rows `decomp::{id}`: no address, so those can only be matched by value."""
        self.strings, self.records = [], []
        table = self.r.hgetall(f"{self.coll}:file:{self.md5}:strings")
        for addr, rec in table.items():
            try:
                rec = json.loads(rec)
            except (ValueError, TypeError):
                continue
            self.records.append(rec)
            addr = _txt(addr)
            if addr.startswith("decomp::"):
                continue
            try:
                a = int(addr.split(":")[-1], 16)
            except ValueError:
                continue
            self.strings.append((a, a + max(1, rec.get("length", 1)), rec))
        self.strings.sort(key=lambda s: s[0])

    def string_near(self, offset):
        """The table string whose bytes (ascii or utf-16) cover file `offset`,
        found by value. The longest wins; None when the table has no such row."""
        if self.strings is None:
            self._load_strings()
        lo = max(0, offset - 512)
        window = self.raw[lo : offset + 512]
        best = None
        for rec in self.records:
            v = rec.get("value") or ""
            if len(v) < 4 or (best and len(v) <= len(best["value"])):
                continue
            for enc in ("ascii", "utf-16-le"):
                try:
                    b = v.encode(enc)
                except UnicodeEncodeError:
                    continue
                p = window.find(b)
                while p != -1:
                    if lo + p <= offset < lo + p + len(b):
                        best = rec
                        break
                    p = window.find(b, p + 1)
        return best

    def string_at(self, va):
        if self.strings is None:
            self._load_strings()
        for a, b, rec in self.strings:
            if a <= va < b:
                return rec
            if a > va:
                break
        return None


# --- file / function -> cluster names ---------------------------------------


AXES = ("overall", "code", "library", "content")


def _bin_ns(coll, axis="overall"):
    from bsimvis.app.services.cluster_utils import bin_cluster_ns
    from bsimvis.app.services.collection_config import resolve_collection_algo

    algo = resolve_collection_algo(coll)
    return bin_cluster_ns(algo if axis == "overall" else f"{algo}:{axis}", False)


def file_clusters(r, members, min_cohesion=0.5, axes=AXES):
    """`{(coll, md5): [{coll, ns, axis, cid, name, size, cohesion}]}`, axes in order
    and the smallest cluster first within one; clusters below `min_cohesion` drop."""
    members = list(members)
    ns = {(c, a): _bin_ns(c, a) for c in {m[0] for m in members} for a in axes}
    pipe = r.pipeline(transaction=False)
    for coll, md5 in members:
        for a in axes:
            pipe.smembers(f"{_fid(coll, md5)}:bin_clusters:{ns[(coll, a)]}")
    raw = iter(pipe.execute())
    cids = [{a: {_txt(c) for c in next(raw)} for a in axes} for _ in members]
    keys = sorted(
        {(m[0], a, c) for m, d in zip(members, cids) for a in axes for c in d[a]}
    )
    pipe = r.pipeline(transaction=False)
    for coll, a, c in keys:
        pipe.get(f"{coll}:bin_cluster:{ns[(coll, a)]}:{c}:meta")
    meta = {}
    for k, blob in zip(keys, pipe.execute()):
        try:
            m = json.loads(blob) if blob else {}
        except ValueError:
            continue
        if (m.get("cohesion_score") or 0) >= min_cohesion:
            meta[k] = {
                "coll": k[0],
                "ns": ns[(k[0], k[1])],
                "axis": k[1],
                "cid": k[2],
                "uuid": m.get("cluster_uuid"),
                "name": m.get("cluster_name") or f"Binary Cluster {k[2]}",
                "size": m.get("member_count") or 0,
                "cohesion": m.get("cohesion_score"),
            }
    return {
        m: [
            c
            for a in axes
            for c in sorted(
                (meta[(m[0], a, x)] for x in d[a] if (m[0], a, x) in meta),
                key=lambda c: c["size"],
            )
        ]
        for m, d in zip(members, cids)
    }


def _card(c):
    """A file cluster in the shape `renderClusterCards` takes."""
    return {
        "cluster_id": c["cid"],
        "cluster_uuid": c["uuid"],
        "cluster_name": c["name"],
        "cohesion_score": c["cohesion"],
        "member_count": c["size"],
        "axis": c["axis"],
    }


def _slim(cl):
    """The tightest cluster of each axis."""
    seen, out = set(), []
    for c in cl:
        if c["axis"] not in seen:
            seen.add(c["axis"])
            out.append(_card(c))
    return out


def fn_clusters(r, coll, md5, entry, min_cohesion=0.5, cap=2):
    """Names of the function clusters one function belongs to, best score first."""
    from bsimvis.app.services.collection_config import resolve_collection_algo

    sc = r.hgetall(f"{coll}:func:{md5}:{entry}:cluster_scores")
    if not sc:
        return []
    algo = resolve_collection_algo(coll)
    top = sorted(((float(v), _txt(k)) for k, v in sc.items()), reverse=True)[: cap * 3]
    pipe = r.pipeline(transaction=False)
    for _, c in top:
        pipe.get(f"{coll}:cluster:{algo}:{c}:meta")
    out = []
    for (_, c), raw in zip(top, pipe.execute()):
        try:
            m = json.loads(raw) if raw else {}
        except ValueError:
            continue
        if (m.get("cohesion_score") or 0) >= min_cohesion:
            out.append(
                {
                    "cluster_id": c,
                    "cluster_uuid": m.get("cluster_uuid"),
                    "cluster_name": m.get("cluster_name") or f"Cluster {c}",
                    "cohesion_score": m.get("cohesion_score"),
                    "member_count": m.get("member_count") or 0,
                }
            )
    return out[:cap]


def function_doc(r, coll, md5, entry):
    """The function's own meta, shaped for the UI's function renderer."""
    fid = f"{coll}:func:{md5}:{entry}"
    try:
        meta = json.loads(r.get(f"{fid}:meta") or "{}")
    except ValueError:
        meta = {}
    keep = ("function_name", "namespace", "parameters", "return_type")
    return {
        "function_name": f"FUN_{entry}",
        "clusters": fn_clusters(r, coll, md5, entry),
        **{k: meta[k] for k in keep if k in meta},
        "entrypoint_address": entry,
        "bsim_features_count": meta.get("bsim_features_count", 0),
        "collection": coll,
        "file_md5": md5,
        "function_id": fid,
    }


def apply_pins(groups, pins, role):
    """Per-file overrides on top of the searches. `pins` maps (coll, md5) to
    "target" | "negative" | "drop": a pinned file leaves every set, then joins the
    first set of its role, which is added when `role` has none."""
    if not pins:
        return groups
    out = [{**g, "members": [m for m in g["members"] if m not in pins]} for g in groups]
    add = [m for m, v in pins.items() if v == role]
    if add:
        if not out:
            out.append({"label": "Pinned", "members": []})
        out[0]["members"] += add
    return out


def locate_hits(r, r_raw, hit_files):
    """Where do the string hits land? `hit_files` is `[((coll, md5), found)]`.
    Returns `{"rule:$id": {functions: [...], strings: [...], unmapped: n}}`."""
    agg = {}
    for (coll, md5), found in hit_files:
        raw = r_raw.get(f"{_fid(coll, md5)}:raw")
        if not raw:
            continue
        loc = Locator(r, coll, md5, raw)
        loc._load()
        for rule, strs in found.items():
            for sid, offsets in strs.items():
                slot = agg.setdefault(
                    f"{rule}:{sid}", {"functions": {}, "strings": {}, "unmapped": 0}
                )
                for off in offsets[:3]:
                    va = loc.to_ghidra(off)
                    fn = loc.function_at(va) if va is not None else None
                    st = loc.string_at(va) if va is not None and not fn else None
                    if not fn and not st:
                        st = loc.string_near(off)
                    if fn:
                        e = slot["functions"].setdefault(
                            fn[0],
                            {"addr": f"{fn[0]:08x}", "name": fn[1], "files": set()},
                        )
                        e["files"].add(f"{coll}:{md5}")
                    elif st:
                        e = slot["strings"].setdefault(
                            st.get("id") or st.get("value"),
                            {"value": st.get("value", "")[:80], "files": set()},
                        )
                        e["files"].add(f"{coll}:{md5}")
                        for a in st.get("funcs") or []:
                            slot["functions"].setdefault(
                                a,
                                {
                                    "addr": a,
                                    "name": loc.fn_name(a),
                                    "files": set(),
                                    "xref": True,
                                },
                            )["files"].add(f"{coll}:{md5}")
                    else:
                        slot["unmapped"] += 1
    out = {}
    for key, slot in agg.items():
        rank = lambda d: sorted(d.values(), key=lambda e: -len(e["files"]))[:5]
        out[key] = {
            "functions": [
                {
                    **e,
                    "files": len(e["files"]),
                    "example": min(e["files"]),
                    "doc": function_doc(r, *min(e["files"]).rsplit(":", 1), e["addr"]),
                }
                for e in rank(slot["functions"])
            ],
            "strings": [
                {**e, "files": len(e["files"]), "example": sorted(e["files"])[0]}
                for e in rank(slot["strings"])
            ],
            "unmapped": slot["unmapped"],
        }
    return out


# --- seeds -------------------------------------------------------------------


def explain_hits(r, r_raw, rules, members, per_file=12):
    """What the rule matched in each file: string id, offset, bytes, and the
    function / string-table entry the offset lands in. Capped to `land_files` files."""
    out = {}
    for coll, md5 in members[: cfg("land_files", 20)]:
        raw = r_raw.get(f"{_fid(coll, md5)}:raw")
        if not raw:
            continue
        loc = Locator(r, coll, md5, raw)
        loc._load()
        rows, docs = [], {}

        def fn_doc(entry):
            if entry not in docs:
                docs[entry] = function_doc(r, coll, md5, entry)
            return docs[entry]

        for m in rules.match(data=raw, timeout=cfg("scan_timeout", 20)):
            if not m.strings:
                rows.append({"rule": m.rule})
            for s in m.strings:
                for i in s.instances[:3]:
                    b = i.matched_data[:32]
                    va = loc.to_ghidra(i.offset)
                    fn = loc.function_at(va) if va is not None else None
                    st = loc.string_at(va) if va is not None else None
                    if not fn and not st:
                        st = loc.string_near(i.offset)
                    rows.append(
                        {
                            "rule": m.rule,
                            "string": s.identifier,
                            "offset": i.offset,
                            "address": None if va is None else f"{va:08x}",
                            "hex": b.hex(" "),
                            "text": "".join(
                                chr(c) if 32 <= c < 127 else "." for c in b
                            ),
                            "function": fn and fn_doc(f"{fn[0]:08x}"),
                            # a hit in a string: the functions that reference it
                            "xrefs": (
                                [fn_doc(a) for a in (st.get("funcs") or [])[:6]]
                                if st and not fn
                                else []
                            ),
                            "xref_total": (
                                len(st.get("funcs") or []) if st and not fn else 0
                            ),
                            "str": st and st.get("value", "")[:80],
                        }
                    )
        out[f"{coll}:{md5}"] = rows[:per_file]
    return out


def function_hex(r, r_raw, coll, md5, addr):
    """Raw bytes of one function as a YARA hex string, or `{"error": ...}`.
    Analyst wildcards the operands by hand -- the live recall shows where."""
    raw = r_raw.get(f"{_fid(coll, md5)}:raw")
    if not raw:
        return {"error": "file has no stored bytes"}
    loc = Locator(r, coll, md5, raw)
    loc._load()
    if loc.binary is None:
        return {"error": "file format not readable by LIEF"}
    start = int(addr, 16)
    fid = f"{coll}:func:{md5}:{start:08x}"
    end = loc.function_end(fid, start)
    import bisect

    i = bisect.bisect_right(loc.funcs, start)
    approx = '"cfg"' not in _txt(r.get(f"{fid}:source") or "")
    if approx and i < len(loc.funcs):
        end = min(end, loc.funcs[i])  # the next function bounds the 15-byte ceiling
    base = loc.binary.imagebase
    lief_va = start - (loc.ghidra_base or base) + base
    try:
        off = loc.binary.virtual_address_to_offset(lief_va)
    except Exception:
        return {"error": "address is not backed by file bytes"}
    body = raw[off : off + (end - start)]
    return {
        "bytes": len(body),
        "approximate": approx,
        "hex": "{ " + " ".join(f"{b:02x}" for b in body) + " }",
    }


def suggest(r, groups_pos, members_neg, min_frac=0, limit=500, scan=1500):
    """Strings of the target files, best coverage first (then fewest negatives).
    `min_frac` drops the rare ones; only the `scan` most shared are checked
    against the negatives. Reads the per-file string tables of up to 50 targets and the cross-file
    `idx:string:file:{id}` sets for the negative side."""
    from bsimvis.app.services import string_service as ss

    targets = list(dict.fromkeys(m for g in groups_pos for m in g["members"]))[:50]
    if not targets:
        return []
    pipe = r.pipeline(transaction=False)
    for coll, md5 in targets:
        pipe.hvals(ss.file_key(coll, md5))
    count, example, files = {}, {}, {}
    for (coll, md5), vals in zip(targets, pipe.execute()):
        seen = set()
        for rec in vals:
            try:
                rec = json.loads(rec)
            except ValueError:
                continue
            sid = rec.get("id")
            if not sid or sid in seen or rec.get("length", 0) < 6:
                continue
            seen.add(sid)
            count[(coll, sid)] = count.get((coll, sid), 0) + 1
            files.setdefault((coll, sid), []).append(f"{coll}:{md5}")
            example.setdefault((coll, sid), rec.get("value", ""))
    need = max(1, int(len(targets) * min_frac + 0.9999))
    cands = sorted(
        (k for k, n in count.items() if n >= need),
        key=lambda k: (-count[k], -len(example[k])),
    )[:scan]
    neg_ids = {f"{c}:file:{m}" for c, m in members_neg}
    pipe = r.pipeline(transaction=False)
    for coll, sid in cands:
        pipe.scard(f"{coll}:idx:string:file:{sid}")
    keep = [
        (k, size) for k, size in zip(cands, pipe.execute()) if size <= 5000
    ]  # a bigger holder set is ubiquitous: useless as a family marker
    pipe = r.pipeline(transaction=False)
    for (coll, sid), _ in keep:
        pipe.smembers(f"{coll}:idx:string:file:{sid}")
    out = []
    for ((coll, sid), _), held in zip(keep, pipe.execute()):
        holders = {_txt(h) for h in held}
        out.append(
            {
                "value": example[(coll, sid)][:200],
                "pos": count[(coll, sid)],
                "of": len(targets),
                "neg": len(holders & neg_ids),
                "files": files[(coll, sid)],
            }
        )
    for s in out:
        s["score"] = string_score(s["pos"], s["of"], s["neg"], len(neg_ids))
    out.sort(key=lambda s: (-s["score"], -s["pos"], -len(s["value"])))
    return out[:limit]


def _target_consts(r, coll, md5, cap=3000):
    """Non-trivial `(value, size)` constants in the stored pcode of one file."""
    fids = [
        _txt(f).removesuffix(":meta")
        for f in r.smembers(f"{coll}:idx:file:functions:{md5}")
    ]
    found = set()
    for i in range(0, min(len(fids), cap), 200):
        pipe = r.pipeline(transaction=False)
        for f in fids[i : i + 200]:
            pipe.get(f"{f}:vec:meta")
        for raw in pipe.execute():
            found |= consts_in(raw)
    return found


def suggest_consts(r, r_raw, groups_pos, members_neg, limit=100, scan=300, neg_cap=60):
    """Pcode constants of the target files as byte patterns. The pcode only names
    the candidates; the counts come from the raw bytes, so they are what YARA will see.
    A constant is tried in the file's own byte order; MIPS/ARM immediates are split
    across instructions and mostly show up only in data, which the counts show."""
    targets = list(dict.fromkeys(m for g in groups_pos for m in g["members"]))[:30]
    if not targets:
        return []
    cnt, order = {}, {}
    for coll, md5 in targets:
        meta = r.get(f"{_fid(coll, md5)}:meta")
        lang = (json.loads(meta) if meta else {}).get("language_id", "")
        order[(coll, md5)] = "big" if ":BE:" in lang else "little"
        for v, size in _target_consts(r, coll, md5):
            if v < 1 << (8 * size):
                cnt[(v, size)] = cnt.get((v, size), 0) + 1
    cands = sorted(cnt, key=lambda k: -cnt[k])[:scan]
    seen = set(targets)
    neg = [m for m in dict.fromkeys(members_neg) if m not in seen]
    neg = sorted(neg, key=lambda m: hashlib.md5("".join(m).encode()).digest())[:neg_cap]
    pos_raw = [(m, r_raw.get(f"{_fid(*m)}:raw") or b"") for m in targets]
    neg_raw = [r_raw.get(f"{_fid(*m)}:raw") or b"" for m in neg]
    out = []
    for v, size in cands:
        pats = {o: v.to_bytes(size, o) for o in ("little", "big")}
        held = [m for m, raw in pos_raw if pats[order[m]] in raw]
        pos = len(held)
        if not pos:
            continue
        # a pattern is tried in both orders on the negatives: it is the same bytes, either way round
        pat = {pats[order[m]] for m, _ in pos_raw}
        n = sum(any(p in raw for p in pat) for raw in neg_raw)
        hexs = "{ " + " ".join(f"{b:02x}" for b in pats["little"]) + " }"
        out.append(
            {
                "kind": "const",
                "value": hexs,
                "label": f"0x{v:x}",
                "pos": pos,
                "of": len(targets),
                "neg": n,
                "files": [f"{c}:{md5}" for c, md5 in held],
                "score": string_score(pos, len(targets), n, len(neg)),
            }
        )
    out.sort(key=lambda s: (-s["score"], -s["pos"]))
    return out[:limit]


# --- hit map: rule -> function clusters --------------------------------------


def _spread(members, cap):
    """Stable md5-hash-ordered sample of `cap` members."""
    return sorted(members, key=lambda m: hashlib.md5("".join(m).encode()).digest())[
        :cap
    ]


def _cluster_meta(r, coll, cids, min_cohesion):
    from bsimvis.app.services.collection_config import resolve_collection_algo

    algo = resolve_collection_algo(coll)
    pipe = r.pipeline(transaction=False)
    for c in cids:
        pipe.get(f"{coll}:cluster:{algo}:{c}:meta")
    out = {}
    for c, raw in zip(cids, pipe.execute()):
        try:
            m = json.loads(raw) if raw else {}
        except ValueError:
            m = {}
        if (m.get("cohesion_score") or 0) >= min_cohesion:
            out[c] = m
    return algo, out


def hitmap(r, r_raw, text, targets, negatives, cap=60, min_cohesion=0.5):
    """Which function clusters do the rule's hits land in?
    Scans up to `cap` files per set, maps every hit offset to its function and the
    function to its (cohesive) clusters. A cluster hit in many groups is a shared
    capability; one hit in a single group is a family marker."""
    rules, errors = compile_source(text)
    if rules is None:
        return {"ok": False, "errors": errors}
    sets = [("target", g) for g in targets] + [("negative", g) for g in negatives]
    per_fn = {}  # (coll, md5, entry) -> {"strings": set, "groups": set}
    scanned, hit_files, langs = {}, {}, {}
    seen = set()
    for role, g in sets:
        label = g["label"]
        for m in _spread([x for x in g["members"] if x not in seen], cap):
            seen.add(m)
            raw = r_raw.get(f"{_fid(*m)}:raw")
            if not raw:
                continue
            scanned[label] = scanned.get(label, 0) + 1
            found = scan_bytes(rules, raw)
            meta = r.get(f"{_fid(*m)}:meta")
            langs[m] = (json.loads(meta) if meta else {}).get("language_id", "")
            if not found:
                continue
            hit_files.setdefault(label, set()).add(m)
            loc = Locator(r, *m, raw)
            loc._load()
            for rule, strs in found.items():
                for sid, offs in strs.items():
                    for off in offs:
                        va = loc.to_ghidra(off)
                        fn = loc.function_at(va) if va is not None else None
                        entries = [fn[0]] if fn else []
                        if not fn:
                            st = (
                                loc.string_at(va) if va is not None else None
                            ) or loc.string_near(off)
                            entries = (
                                [
                                    int(a, 16)
                                    for a in (st.get("funcs") or [])[:6]
                                    if re.fullmatch(r"[0-9a-f]+", a)
                                ]
                                if st
                                else []
                            )
                        for e in entries:
                            slot = per_fn.setdefault(
                                (m[0], m[1], e), {"strings": set(), "groups": set()}
                            )
                            slot["strings"].add(f"{rule}:{sid}")
                            slot["groups"].add(label)

    keys = list(per_fn)
    pipe = r.pipeline(transaction=False)
    for coll, md5, e in keys:
        pipe.hgetall(f"{coll}:func:{md5}:{e:08x}:cluster_scores")
    scores = pipe.execute()
    cids_by_coll = {}
    for (coll, _, _), sc in zip(keys, scores):
        cids_by_coll.setdefault(coll, set()).update(_txt(k) for k in sc)
    meta = {}
    for coll, cids in cids_by_coll.items():
        algo, ms = _cluster_meta(r, coll, sorted(cids), min_cohesion)
        for c, m in ms.items():
            meta[(coll, c)] = (algo, m)

    clusters, unclustered = {}, 0
    for (coll, md5, e), sc in zip(keys, scores):
        slot = per_fn[(coll, md5, e)]
        mine = [(float(v), _txt(k)) for k, v in sc.items() if (coll, _txt(k)) in meta]
        if not mine:
            unclustered += 1
            continue
        c = max(mine)[1]  # the tightest-scored cluster is the function's identity
        row = clusters.setdefault(
            (coll, c),
            {"strings": set(), "fids": [], "files": set(), "groups": {}},
        )
        row["strings"] |= slot["strings"]
        row["files"].add((coll, md5))
        if len(row["fids"]) < 40:
            row["fids"].append(f"{coll}:func:{md5}:{e:08x}")
        for gl in slot["groups"]:
            row["groups"].setdefault(gl, set()).add((coll, md5))

    out = []
    for (coll, c), row in clusters.items():
        algo, m = meta[(coll, c)]
        members = r.smembers(f"{coll}:cluster:{algo}:{c}:members")
        span = {tuple(_txt(x).split(":")[0::2]) for x in members}
        out.append(
            {
                "cluster_id": int(c),
                "cluster_uuid": m.get("cluster_uuid"),
                "name": m.get("cluster_name"),
                "cohesion": m.get("cohesion_score"),
                "members": len(members),
                "card": {
                    "cluster_id": c,
                    "cluster_uuid": m.get("cluster_uuid"),
                    "cluster_name": m.get("cluster_name") or f"Cluster {c}",
                    "cohesion_score": m.get("cohesion_score"),
                    "member_count": len(members),
                },
                "span_files": len(span),
                "hit_files": len(row["files"]),
                "strings": sorted(row["strings"]),
                "groups": {k: len(v) for k, v in row["groups"].items()},
                "arches": sorted({langs.get(f, "") for f in row["files"]}),
                "fids": row["fids"],
            }
        )
    out.sort(key=lambda x: (-x["hit_files"], -x["members"]))
    return {
        "ok": True,
        "scanned": scanned,
        "hit_files": {k: len(v) for k, v in hit_files.items()},
        "unclustered_functions": unclustered,
        "clusters": out[:200],
    }


# --- scope: which file clusters to focus on ---------------------------------


def _greedy(cands, universe, min_purity, limit):
    """Pick clusters one by one, each covering the most still-uncovered files of
    `universe`; clusters whose target share is under `min_purity` are skipped."""
    left, out, done = set(universe), [], 0
    pool = [c for c in cands if c["purity"] >= min_purity]
    while left and len(out) < limit and pool:
        best = max(pool, key=lambda c: (len(c["_t"] & left), c["purity"], -c["size"]))
        gain = best["_t"] & left
        if not gain:
            break
        pool.remove(best)
        left -= gain
        done += len(gain)
        out.append({**best, "gain": len(gain), "covered": done})
    return out


def _cluster_members(r, cands):
    """`{(coll, cid): {(coll, md5)}}` for every candidate cluster."""
    keys = list(cands)
    pipe = r.pipeline(transaction=False)
    for coll, cid in keys:
        pipe.smembers(f"{coll}:bin_cluster:{cands[(coll, cid)]['ns']}:{cid}:members")
    return {
        k: {(k[0], _txt(x).rpartition(":")[2]) for x in raw}
        for k, raw in zip(keys, pipe.execute())
    }


def _clean(picks):
    out = []
    for p in picks:
        p = {k: v for k, v in p.items() if not k.startswith("_")}
        p["purity"] = round(p["purity"], 3)
        out.append(p)
    return out


def scope_hits(
    r, hits, min_cohesion=0.5, min_purity=0.8, partial=0.3, cap=5000, axis="code"
):
    """Scope without a Must match set: the rule's own hits stand in for the targets.
    Same row shape as `scope`; here `purity` is the share of the cluster the rule
    matches, `missed` the cluster-mates it does not. `scope` is the greedy cover of
    the hits by clusters it matches at least `min_purity` of; `focus` the greedy cover
    of the unmatched cluster-mates of clusters it matches at least `partial` of."""
    hits = list(hits)
    hset = set(hits[:cap])
    cl = file_clusters(r, hits[:cap], min_cohesion, (axis,))
    cands = {}
    for m in hset:
        for c in cl[m]:
            cands.setdefault((c["coll"], c["cid"]), c)
    rows = []
    for k, mem in _cluster_members(r, cands).items():
        c, h = cands[k], mem & hset
        size = len(mem) or c["size"]
        rows.append(
            {
                "name": c["name"],
                "cid": c["cid"],
                "uuid": c["uuid"],
                "card": _card({**c, "size": size}),
                "size": size,
                "cohesion": c["cohesion"],
                "targets": len(h),
                "negatives": 0,
                "purity": len(h) / size if size else 0,
                "hit": len(h),
                "missed": size - len(h),
                "false": 0,
                "_t": h,
                "_miss": mem - hset,
            }
        )
    unmatched = set().union(
        *(c["_miss"] for c in rows if c["purity"] >= partial), set()
    )
    return {
        "ok": True,
        "mode": "hits",
        "axis": axis,
        "targets": len(hset),
        "truncated": len(hits) > cap,
        "hit": len(hset),
        "missed": len(unmatched),
        "min_purity": min_purity,
        "unclustered": sum(1 for m in hset if not cl[m]),
        "scope": _clean(_greedy(rows, hset, min_purity, 15)),
        "focus": _clean(
            _greedy([{**c, "_t": c["_miss"]} for c in rows], unmatched, partial, 15)
        ),
        "impure": [],
    }


def scope(
    r, r_raw, text, targets, negatives, min_cohesion=0.5, min_purity=0.8, axis="code"
):
    """File clusters worth focusing on. Every cluster that holds a target is scored
    by how pure it is (targets / targets + negatives, over the whole sets, not the
    scan sample) and by what the rule does inside it. `scope` is the greedy cover of
    all targets, `focus` the greedy cover of the targets the rule misses."""
    rules, errors = compile_source(text)
    if rules is None:
        return {"ok": False, "errors": errors}
    tmem, nmem, hits, *_ = _scan_sets(r_raw, rules, targets, negatives)
    tset = set(tmem)
    allneg = {m for g in negatives for m in g["members"]} - tset
    tp = {m for m in tmem if hits.get(m, (0, 0))[1]}
    fp = {m for m in nmem if hits.get(m, (0, 0))[1]}
    scanned = {m for m in tmem if m in hits}
    cl = file_clusters(r, tmem, min_cohesion, (axis,))
    cands = {}
    for m in tmem:
        for c in cl[m]:
            cands.setdefault((c["coll"], c["cid"]), c)
    rows = []
    for k, mem in _cluster_members(r, cands).items():
        t, n = mem & tset, mem & allneg
        c = cands[k]
        rows.append(
            {
                "name": c["name"],
                "cid": c["cid"],
                "uuid": c["uuid"],
                "card": _card({**c, "size": len(mem) or c["size"]}),
                "size": len(mem) or c["size"],
                "cohesion": c["cohesion"],
                "targets": len(t),
                "negatives": len(n),
                "purity": len(t) / (len(t) + len(n)) if t else 0,
                "hit": len(t & tp),
                "missed": len((t & scanned) - tp),
                "false": len(mem & fp),
                "_t": t,
                "_miss": (t & scanned) - tp,
            }
        )
    miss = scanned - tp
    clean = _clean

    focus_in = [{**c, "_t": c["_miss"]} for c in rows]
    picks = _greedy(rows, tset, min_purity, 15)
    fpicks = _greedy(focus_in, miss, min_purity, 15)
    return {
        "ok": True,
        "axis": axis,
        "targets": len(tset),
        "scanned": len(scanned),
        "hit": len(tp),
        "missed": len(miss),
        "min_purity": min_purity,
        "unclustered": sum(1 for m in tmem if not cl[m]),
        "scope": clean(picks),
        "focus": clean(fpicks),
        "impure": clean(
            sorted(
                (c for c in rows if c["purity"] < min_purity),
                key=lambda c: -c["targets"],
            )[:10]
        ),
    }


# --- byte seeds from a function cluster / lane -------------------------------


def function_body(r, r_raw, coll, md5, addr):
    """`(bytes, approximate)` of one function, or None."""
    raw = r_raw.get(f"{_fid(coll, md5)}:raw")
    if not raw:
        return None
    loc = Locator(r, coll, md5, raw)
    loc._load()
    if loc.binary is None:
        return None
    import bisect

    start = int(addr, 16)
    fid = f"{coll}:func:{md5}:{start:08x}"
    end = loc.function_end(fid, start)
    i = bisect.bisect_right(loc.funcs, start)
    approx = '"cfg"' not in _txt(r.get(f"{fid}:source") or "")
    if approx and i < len(loc.funcs):
        end = min(end, loc.funcs[i])
    lief_va = start - (loc.ghidra_base or loc.binary.imagebase) + loc.binary.imagebase
    try:
        off = loc.binary.virtual_address_to_offset(lief_va)
    except Exception:
        return None
    return raw[off : off + (end - start)], approx, raw


def _hex_regex(hexs):
    toks = hexs.strip("{} ").split()
    return re.compile(
        b"".join(b"." if t == "??" else re.escape(bytes.fromhex(t)) for t in toks),
        re.S,
    )


def consensus_windows(bodies, widths=(16, 24, 32, 48), min_fixed=10):
    """Same-length bodies -> per width, the window with the most bytes every body
    agrees on, differing bytes as `??`. Edges are trimmed to fixed bytes."""
    n = len(bodies[0])
    fixed = [len({b[i] for b in bodies}) == 1 for i in range(n)]
    out = []
    for w in widths:
        if w > n:
            continue
        best = max(range(n - w + 1), key=lambda s: sum(fixed[s : s + w]))
        lo, hi = best, best + w
        while lo < hi and not fixed[lo]:
            lo += 1
        while hi > lo and not fixed[hi - 1]:
            hi -= 1
        cnt = sum(fixed[lo:hi])
        if cnt < min_fixed:
            continue
        hexs = (
            "{ "
            + " ".join(
                f"{bodies[0][i]:02x}" if fixed[i] else "??" for i in range(lo, hi)
            )
            + " }"
        )
        out.append({"width": hi - lo, "offset": lo, "fixed": cnt, "hex": hexs})
    return out


def seed_functions(r, r_raw, fids, per_arch=16):
    """Byte-pattern seeds from a set of functions that are 'the same' (a function
    cluster, an N-way row or lane): per language, the same-length bodies are aligned
    byte for byte and the stable window is returned, with the share of the sampled
    files whose raw bytes contain it. ponytail: equal length only, no indel alignment.
    """
    by_lang = {}
    for fid in fids:
        coll, _, md5, addr = fid.split(":")
        meta = r.get(f"{_fid(coll, md5)}:meta")
        lang = (json.loads(meta) if meta else {}).get("language_id", "")
        by_lang.setdefault(lang, {}).setdefault(md5, (coll, addr))
    arches = []
    for lang, files in sorted(by_lang.items()):
        picks = sorted(
            files.items(), key=lambda kv: hashlib.md5(kv[0].encode()).digest()
        )
        picks = picks[:per_arch]
        bodies, raws = [], []
        for md5, (coll, addr) in picks:
            got = function_body(r, r_raw, coll, md5, addr)
            if got and len(got[0]) >= 16:
                bodies.append(got[0])
                raws.append(got[2])
        if len(bodies) < 2:
            arches.append(
                {
                    "language_id": lang,
                    "files": len(files),
                    "sampled": len(bodies),
                    "windows": [],
                }
            )
            continue
        sizes = {}
        for b in bodies:
            sizes[len(b)] = sizes.get(len(b), 0) + 1
        L = max(sizes, key=lambda k: (sizes[k], k))
        same = [b for b in bodies if len(b) == L]
        wins = consensus_windows(same) if len(same) >= 2 else []
        for w in wins:
            rx = _hex_regex(w["hex"])
            w["coverage"] = round(sum(1 for x in raws if rx.search(x)) / len(raws), 3)
        arches.append(
            {
                "language_id": lang,
                "files": len(files),
                "sampled": len(bodies),
                "same_length": len(same),
                "length": L,
                "windows": wins,
            }
        )
    return {"arches": arches}


def suggest_functions(r, r_raw, rows, targets, members_neg, per_arch=8, neg_cap=30):
    """Function-byte suggestions from N-way indicator rows (`fids` of a function
    that singles out the family). Per row and architecture the aligned stable window
    becomes a hex string; counts are taken on the raw bytes, like the constants."""
    targets = list(dict.fromkeys(targets))[:30]
    tset = set(targets)
    pos_raw = [r_raw.get(f"{_fid(*m)}:raw") or b"" for m in targets]
    neg = [m for m in dict.fromkeys(members_neg) if m not in tset]
    neg = sorted(neg, key=lambda m: hashlib.md5("".join(m).encode()).digest())[:neg_cap]
    neg_raw = [r_raw.get(f"{_fid(*m)}:raw") or b"" for m in neg]
    out, seen = [], set()
    for row in rows:
        fids = [
            f for f in row.get("fids", []) if (f.split(":")[0], f.split(":")[2]) in tset
        ]
        if len(fids) < 2:
            continue
        for arch in seed_functions(r, r_raw, fids, per_arch)["arches"]:
            if not arch["windows"]:
                continue
            best = None
            for w in arch["windows"]:
                if w["hex"] in seen:
                    continue
                rx = _hex_regex(w["hex"])
                held = [m for m, raw in zip(targets, pos_raw) if rx.search(raw)]
                if not held:
                    continue
                n = sum(1 for raw in neg_raw if rx.search(raw))
                sc = string_score(len(held), len(targets), n, len(neg_raw))
                # the widest window is often generic code: keep the one that separates best
                if best is None or (sc, w["fixed"]) > best[:2]:
                    best = (sc, w["fixed"], w["hex"], held, n)
            if best is None:
                continue
            sc, _, hexs, held, n = best
            seen.add(hexs)
            out.append(
                {
                    "kind": "function",
                    "value": hexs,
                    "label": f"{row.get('preview', '')} ({arch['language_id'].split(':')[0]})",
                    "pos": len(held),
                    "of": len(targets),
                    "neg": n,
                    "files": [f"{c}:{md5}" for c, md5 in held],
                    "score": sc,
                }
            )
    out.sort(key=lambda s: (-s["score"], -s["pos"]))
    return out


# --- registry ----------------------------------------------------------------

KEY = "global:yara_rule:{}"


def save_rule(r, text, uuid=None, scope=None, note=""):
    """Store `text` as the next version of `uuid` (a new rule when None)."""
    if compile_source(text)[0] is None:
        return {"error": "rule does not compile"}
    now = int(time.time())
    uuid = uuid or str(uuidlib.uuid4())
    cur = r.get(KEY.format(uuid))
    doc = json.loads(cur) if cur else {"uuid": uuid, "created": now, "version": 0}
    names = rule_names(text)
    doc.update(
        version=doc["version"] + 1,
        name=names[0] if names else "unnamed",
        names=names,
        text=text,
        updated=now,
        note=note,
    )
    if scope is not None:
        doc["scope"] = scope
    pipe = r.pipeline(transaction=False)
    pipe.set(
        f"{KEY.format(uuid)}:v:{doc['version']}",
        json.dumps({"version": doc["version"], "text": text, "ts": now, "note": note}),
    )
    pipe.set(KEY.format(uuid), json.dumps(doc))
    pipe.zadd("global:yara_rules", {uuid: now})
    pipe.execute()
    return doc


def get_rule(r, uuid, version=None):
    raw = r.get(f"{KEY.format(uuid)}:v:{version}" if version else KEY.format(uuid))
    return json.loads(raw) if raw else None


def list_rules(r, limit=200):
    ids = [_txt(u) for u in r.zrevrange("global:yara_rules", 0, limit - 1)]
    docs = (get_rule(r, u) for u in ids)
    return [
        {
            k: d[k]
            for k in ("uuid", "name", "names", "version", "updated", "note")
            if k in d
        }
        for d in docs
        if d
    ]


def rule_versions(r, uuid):
    cur = get_rule(r, uuid)
    if not cur:
        return []
    out = []
    for n in range(cur["version"], 0, -1):
        v = get_rule(r, uuid, n)
        if v:
            out.append({k: v[k] for k in ("version", "ts", "note")})
    return out


def delete_rule(r, uuid):
    cur = get_rule(r, uuid)
    if not cur:
        return False
    pipe = r.pipeline(transaction=False)
    for n in range(1, cur["version"] + 1):
        pipe.delete(f"{KEY.format(uuid)}:v:{n}")
    pipe.delete(KEY.format(uuid))
    pipe.zrem("global:yara_rules", uuid)
    pipe.execute()
    return True


# --- sweep / apply (one file chunk per job) ---------------------------------


def run_chunk(
    r,
    r_raw,
    q,
    collection,
    text,
    mode,
    run_id,
    rule_uuid=None,
    cursor="0",
    chunk_size=100,
):
    """Scan the next `chunk_size` files of `collection`.

    `sweep` records the hit file ids under `yara:sweep:{run_id}:*` in the queue
    Redis (TTL'd, nothing is written to Kvrocks). `apply` also moves the
    `yara:custom:` file tags: a file that hits gets them, one that no longer does
    loses them. Returns the next cursor, "0" when done."""
    from bsimvis.app.services.index_service import _index_tag, _unindex_tag

    rules, _ = compile_source(text)
    if rules is None:
        raise ValueError("rule does not compile")
    tags = {f"{CUSTOM_TAG}{n}" for n in rule_names(text)}
    new_cursor, ids = r.sscan(
        f"{collection}:all_files", cursor=int(cursor), count=chunk_size
    )
    ids = [_txt(i) for i in ids]
    hit_ids, changed, strs = [], 0, {}
    # tags an earlier version of this rule applied: a renamed rule must drop them
    previous = (
        {_txt(t) for t in r.smembers(f"{KEY.format(rule_uuid)}:tags")}
        if mode == "apply" and rule_uuid
        else set()
    )
    pipe = r.pipeline(transaction=False)
    for fid in ids:
        data = r_raw.get(f"{fid}:raw")
        found = scan_bytes(rules, data) if data else {}
        if found:
            hit_ids.append(fid)
            strs[fid] = "|".join(
                f"{rule}:{sid}" for rule, ss in found.items() for sid in ss
            )
        if mode != "apply":
            continue
        raw_meta = r.get(f"{fid}:meta")
        if not raw_meta:
            continue
        meta = json.loads(raw_meta)
        old = sorted(set(meta.get("tags") or []))
        stale = {t for t in old if t in tags or t in previous}
        merged = sorted((set(old) - stale) | {f"{CUSTOM_TAG}{n}" for n in found})
        if merged == old:
            continue
        meta["tags"] = merged
        pipe.set(f"{fid}:meta", json.dumps(meta))
        _unindex_tag(pipe, collection, "file", "tags", old, fid)
        _index_tag(pipe, collection, "file", "tags", merged, fid)
        changed += 1
    pipe.execute()
    if mode == "apply" and rule_uuid:
        r.sadd(f"{KEY.format(rule_uuid)}:tags", *tags)
    base = f"yara:sweep:{run_id}"
    if hit_ids:
        q.sadd(f"{base}:hits", *hit_ids)
    if strs:
        q.hset(f"{base}:strs", mapping=strs)
    q.hincrby(f"{base}:stat", "seen", len(ids))
    q.hincrby(f"{base}:stat", "changed", changed)
    if str(new_cursor) == "0":
        q.hincrby(f"{base}:stat", "done", 1)
    for k in (f"{base}:hits", f"{base}:stat", f"{base}:strs"):
        q.expire(k, 6 * 3600)
    return str(new_cursor), len(ids), len(hit_ids), changed


def demo():
    """Compile + scan + error mapping, no database."""
    text = 'rule a { strings: $x = "hello" $y = { 41 ?? 43 } condition: any of them }'
    rules, errs = compile_source(text)
    assert rules and not errs
    assert scan_bytes(rules, b"xx hello A_C") == {"a": {"$x": [3], "$y": [9]}}
    assert scan_bytes(rules, b"nothing") == {}
    assert defined_strings(text) == []  # one-line source: line parser is per-line
    multi = 'rule a {\n strings:\n  $x = "h"\n condition: $x\n}'
    assert defined_strings(multi) == [("a", "$x")]
    a, b, c = ("c", "a"), ("c", "b"), ("c", "c")
    gs = [{"label": "T", "members": [a, b]}]
    pins = {a: "drop", c: "target", b: "negative"}
    assert apply_pins(gs, pins, "target")[0]["members"] == [c]
    assert apply_pins([], pins, "negative") == [{"label": "Pinned", "members": [b]}]
    assert apply_pins(gs, {}, "target") is gs
    assert string_score(10, 10, 0, 0) == 1.0 and string_score(5, 10, 0, 0) == 0.5
    assert string_score(95, 101, 12, 12) == 0.0  # in every other file: useless
    assert string_score(27, 50, 6, 42) > string_score(45, 50, 40, 42)
    assert string_score(1, 0, 0, 5) is None
    bad, errs = compile_source("rule a { condition: $nope }")
    assert bad is None and errs[0]["line"] == 1 and "nope" in errs[0]["message"]
    good = (
        'rule g { meta:\n description = "d"\n author = "a"\n date = "2026-10-28"\n'
        ' kind = "family"\n family = "mirai"\n reference_md5 = "x"\n strings:\n'
        ' $a = "abcdefghij"\n $b = { 01 02 03 04 05 06 07 }\n condition:\n'
        " uint32(0) == 0x464c457f and filesize < 2MB and all of them }"
    )
    assert lint(good) == [], lint(good)
    bad = lint('rule b {\n strings:\n $a = "ab"\n condition:\n any of them\n}')
    assert any("only 2 chars" in w["message"] for w in bad) and len(bad) >= 6
    print("yara_edit_service demo OK")


if __name__ == "__main__":
    demo()
