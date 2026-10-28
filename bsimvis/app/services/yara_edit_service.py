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


def evaluate(r, r_raw, text, targets, negatives):
    """Compile `text` and measure it. `targets` / `negatives` are lists of
    `{label, members: [(coll, md5)]}`. Returns the report dict."""
    t0 = time.time()
    rules, errors = compile_source(text)
    names = rule_names(text)
    if rules is None:
        return {"ok": False, "errors": errors, "rules": names}

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
            {"rule": k[0], "id": k[1], "pos": v[0], "neg": v[1]}
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

    def string_at(self, va):
        if self.strings is None:
            self.strings = []
            table = self.r.hgetall(f"{self.coll}:file:{self.md5}:strings")
            for addr, rec in table.items():
                try:
                    rec = json.loads(rec)
                    a = int(_txt(addr).split(":")[-1], 16)
                except (ValueError, TypeError):
                    continue
                self.strings.append((a, a + max(1, rec.get("length", 1)), rec))
            self.strings.sort(key=lambda s: s[0])
        for a, b, rec in self.strings:
            if a <= va < b:
                return rec
            if a > va:
                break
        return None


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
                    if va is None:
                        slot["unmapped"] += 1
                        continue
                    fn, st = loc.function_at(va), loc.string_at(va)
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
                                    "name": f"FUN_{a}",
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
                {**e, "files": len(e["files"]), "example": sorted(e["files"])[0]}
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
        rows = []
        for m in rules.match(data=raw, timeout=cfg("scan_timeout", 20)):
            if not m.strings:
                rows.append({"rule": m.rule})
            for s in m.strings:
                for i in s.instances[:3]:
                    b = i.matched_data[:32]
                    va = loc.to_ghidra(i.offset)
                    fn = loc.function_at(va) if va is not None else None
                    st = loc.string_at(va) if va is not None else None
                    rows.append(
                        {
                            "rule": m.rule,
                            "string": s.identifier,
                            "offset": i.offset,
                            "hex": b.hex(" "),
                            "text": "".join(
                                chr(c) if 32 <= c < 127 else "." for c in b
                            ),
                            "function": fn
                            and {
                                "addr": f"{fn[0]:08x}",
                                "name": fn[1],
                                "example": f"{coll}:{md5}",
                            },
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
    approx = not (r.get(f"{fid}:source") or b"").__contains__(b'"cfg"')
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


def suggest(r, groups_pos, members_neg, min_frac=0, limit=200, scan=400):
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
    count, example = {}, {}
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
    out = []
    for (coll, sid), size in zip(cands, pipe.execute()):
        key = f"{coll}:idx:string:file:{sid}"
        if size > 5000:  # ubiquitous: useless as a family marker
            continue
        holders = {_txt(h) for h in r.smembers(key)}
        out.append(
            {
                "value": example[(coll, sid)][:200],
                "pos": count[(coll, sid)],
                "of": len(targets),
                "neg": len(holders & neg_ids),
            }
        )
    out.sort(key=lambda s: (-s["pos"], s["neg"], -len(s["value"])))
    return out[:limit]


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
    hit_ids, changed = [], 0
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
    q.hincrby(f"{base}:stat", "seen", len(ids))
    q.hincrby(f"{base}:stat", "changed", changed)
    if str(new_cursor) == "0":
        q.hincrby(f"{base}:stat", "done", 1)
    for k in (f"{base}:hits", f"{base}:stat"):
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
    bad, errs = compile_source("rule a { condition: $nope }")
    assert bad is None and errs[0]["line"] == 1 and "nope" in errs[0]["message"]
    print("yara_edit_service demo OK")


if __name__ == "__main__":
    demo()
