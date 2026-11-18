"""Capabilities: what a group of functions does, as MBC and ATT&CK ids plus free text.

A capability is its own record per collection (or pool), linked to the N-way
groups (nway_sem_service name records) it was found in. The model picks ids
from a curated list only, at a level that follows the group's size: big groups
(lanes, regions) get MBC behaviours and ATT&CK techniques, small ones MBC
micro-behaviours. Ids and names come from data/capabilities/catalog.json, a
trimmed snapshot of MBC and ATT&CK (see VENDORED_FROM.md there).

Kvrocks keys, `ns` as in nway_sem_service:
  {ns}:capabilities                      hash  capability id -> JSON record
  {ns}:idx:capability:label:{mbc|attack id}  set   capability ids
"""

import hashlib
import json
import re
import os
import time
import uuid

from bsimvis.app.services import nway_sem_service
from bsimvis.app.services.config_service import config_service

CATALOG = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "data", "capabilities", "catalog.json"
)
# Curated for Linux / IoT malware and general native code, most common first (the
# model leans on the head of a list); [capabilities] overrides. No MBC E-ids: they
# repeat ATT&CK techniques, which are offered at every level.
BEHAVIOURS = [
    "B0033", "B0030", "B0011", "E1643", "B0023", "B0022", "B0018", "B0019",
    "B0024", "B0025", "B0031", "B0032", "B0029", "B0038", "B0009", "B0027",
    "B0040", "B0047", "F0001", "F0004", "F0007", "F0010", "F0014",
]  # fmt: skip
MICRO = [
    "C0001", "C0002", "C0011", "C0014", "C0003", "C0004", "C0017", "C0018",
    "C0038", "C0042", "C0016", "C0051", "C0052", "C0045", "C0047", "C0050",
    "C0021", "C0026", "C0053", "C0027", "C0031", "C0028", "C0029", "C0030",
    "C0032", "C0024", "C0025", "C0019", "C0033", "C0034",
]  # fmt: skip
ATTACK = [
    "T1498.001", "T1498.002", "T1499", "T1499.002", "T1110.001", "T1046",
    "T1595", "T1018", "T1584.005", "T1071.001", "T1071.004", "T1095", "T1105",
    "T1059.004", "T1037", "T1037.004", "T1053.003", "T1543", "T1543.002",
    "T1489", "T1529", "T1685", "T1027", "T1027.002", "T1140", "T1132.001",
    "T1497", "T1622", "T1070.004", "T1082", "T1083", "T1057", "T1016", "T1049",
    "T1090", "T1571", "T1573", "T1568.002", "T1496", "T1485", "T1486",
    "T1210", "T1190", "T1078.001", "T1021.004", "T1014", "T1036", "T1564",
    "T1222",
]  # fmt: skip
MICRO_BELOW = (
    8  # a group with fewer rows gets micro-behaviours instead of MBC behaviours
)
CAPS_V = 5  # bump when the capability prompt or parsing changes: groups are asked again
MATCH = 0.5  # anchor similarity two answers need to be one capability
IDS_MAX, TRAITS_MAX = 4, 5
_CAT = None


def catalog():
    global _CAT
    if _CAT is None:
        with open(CATALOG) as f:
            _CAT = json.load(f)
    return _CAT


def vocab():
    """(behaviours, micro, attack) id lists; an id missing from the catalog raises."""
    cfg = lambda k, d: list(config_service.get(f"capabilities.{k}", d) or d)
    b, m, a = cfg("behaviours", BEHAVIOURS), cfg("micro", MICRO), cfg("attack", ATTACK)
    cat = catalog()
    bad = [i for i in b + m if i not in cat["mbc"]] + [
        i for i in a if i not in cat["attack"]
    ]
    if bad:
        raise ValueError(f"[capabilities] ids not in the catalog: {', '.join(bad)}")
    return b, m, a


def enabled():
    return bool(config_service.get("capabilities.enabled", True))


def level(rows):
    below = int(config_service.get("capabilities.micro_below", MICRO_BELOW))
    return "micro" if int(rows or 0) < below else "behaviour"


def prompt_lists(lvl):
    """(id lines the model chooses from, MBC ids, ATT&CK ids) for one level, in
    list order. ATT&CK is offered at every level: a 3-row group can be a whole
    persistence installer."""
    b, m, a = vocab()
    cat = catalog()
    mbc = m if lvl == "micro" else b
    lines = ["MBC ids:"] + [f"{i} {cat['mbc'][i]['name']}" for i in mbc]
    lines += ["ATT&CK ids:"] + [f"{i} {cat['attack'][i]['name']}" for i in a]
    return "\n".join(lines), list(mbc), list(a)


def schema(lvl):
    """The JSON schema the model answers in: ids only from the lists (enum), each
    with the evidence text that shows it, and a few short traits."""
    _, mbc, att = prompt_lists(lvl)
    ids = lambda allowed: {
        "type": "array",
        "maxItems": IDS_MAX,
        "items": {
            "type": "object",
            "properties": {
                "id": {"type": "string", "enum": allowed},
                "quote": {"type": "string"},
            },
            "required": ["id", "quote"],
        },
    }
    return {
        "type": "object",
        "properties": {
            "traits": {
                "type": "array",
                "maxItems": TRAITS_MAX,
                "items": {"type": "string"},
            },
            "mbc": ids(mbc),
            "attack": ids(att),
        },
        "required": ["traits", "mbc", "attack"],
    }


_ID = re.compile(r"\b([BCEF]\d{4}|T\d{4}(?:\.\d{3})?)\b")
_WS = re.compile(r"\s+")


def _norm(s):
    return _WS.sub(" ", str(s or "")).strip().lower()


# code or a string: a call with its arguments, a path, a format, a flag
_CODEISH = re.compile(r"[/()%\"'=<>{};:\\+]")


def _strong(q):
    """Whether a quote can back a behaviour or technique. A bare call name
    ("sendto", or "socket, sendto" off the library-call line) backs a
    micro-behaviour (C*), which is one call, but not a DoS or a C2 claim: those
    need the code or string that shows how the call is used."""
    return bool(_CODEISH.search(q))


def _ids(items, allowed, evidence, quotes):
    """The curated ids of a model list whose quote is really in the evidence.

    An item is {id, quote} (or a bare / echoed "C0021 Name" string, which has no
    quote and is dropped). A list that is just the head of the offered list, in
    order, is the model copying it: dropped whole."""
    ev, out = _norm(evidence), []
    for x in items if isinstance(items, list) else [items]:
        x = x if isinstance(x, dict) else {"id": x}
        m = _ID.search(str(x.get("id") or ""))
        q = _norm(x.get("quote"))
        if not m or m.group(1) not in allowed or m.group(1) in out:
            continue
        if len(q) < 4 or q not in ev:
            continue
        if not m.group(1).startswith("C") and not _strong(q):
            continue
        out.append(m.group(1))
        quotes[m.group(1)] = str(x.get("quote")).strip()[:120]
    if len(out) >= 3 and out == list(allowed[: len(out)]):
        return []
    return out[:IDS_MAX]


def _traits(items):
    """A few short capability phrases: at most TRAITS_MAX, six words, 48 chars."""
    out = []
    for t in items if isinstance(items, list) else []:
        t = " ".join(str(t or "").split()).strip(" .;,").lower()
        if t and len(t) <= 48 and len(t.split()) <= 6 and t not in out:
            out.append(t)
    return out[:TRAITS_MAX]


def clean(answer, lvl, evidence=""):
    """Keeps the curated, quoted ids and the short traits; None when nothing is left."""
    _, mbc, att = prompt_lists(lvl)
    a = answer if isinstance(answer, dict) else {}
    quotes = {}
    out = {
        "mbc": _ids(a.get("mbc") or [], mbc, evidence, quotes),
        "attack": _ids(a.get("attack") or [], att, evidence, quotes),
        "traits": _traits(a.get("traits")),
    }
    out["quotes"] = {i: quotes[i] for i in out["mbc"] + out["attack"]}
    return out if out["mbc"] or out["attack"] or out["traits"] else None


def unlink(r, ns, cap_id, group):
    """Drops `group` from a capability; a capability left with no group goes."""
    raw = r.hget(f"{ns}:capabilities", cap_id)
    if not raw:
        return
    rec = json.loads(_decode(raw))
    rec["groups"] = [g for g in rec.get("groups") or [] if g != group]
    pipe = r.pipeline(transaction=False)
    if rec["groups"]:
        pipe.hset(f"{ns}:capabilities", cap_id, json.dumps(rec))
    else:
        pipe.hdel(f"{ns}:capabilities", cap_id)
        for i in labels(rec):
            pipe.srem(f"{ns}:idx:capability:label:{i}", cap_id)
    pipe.execute()


def labels(rec):
    return list(rec.get("mbc") or []) + list(rec.get("attack") or [])


def _decode(v):
    return v.decode() if isinstance(v, bytes) else v


def save(r, ns, anchors, answer, lvl, group=None, evidence=None, source="llm"):
    """Stores one group's capability answer, merged into an existing capability
    that shares an id and >= MATCH of its anchors. Returns the record."""
    if not ns or not answer:
        return None
    lab = labels(answer)
    cand = set()
    for i in lab:
        cand |= {_decode(x) for x in r.smembers(f"{ns}:idx:capability:label:{i}")}
    rec = None
    if cand:
        raw = r.hmget(f"{ns}:capabilities", sorted(cand))
        for v in raw:
            old = v and json.loads(_decode(v))
            if old and nway_sem_service.similarity(old["anchors"], anchors) >= MATCH:
                rec = old
                break
    now = int(time.time() * 1000)
    if rec:
        for k in ("mbc", "attack"):
            rec[k] = list(dict.fromkeys(rec.get(k, []) + answer[k]))
        rec["traits"] = list(dict.fromkeys(rec.get("traits", []) + answer["traits"]))[
            : 2 * TRAITS_MAX
        ]
        rec["quotes"] = {**rec.get("quotes", {}), **answer["quotes"]}
        rec.pop("text", None)
    else:
        rec = {"id": uuid.uuid4().hex[:16], "created": now, **answer}
        rec["anchors"] = list(anchors)
    rec.update(level=lvl, source=source, updated=now)
    if group and group not in rec.setdefault("groups", []):
        rec["groups"].append(group)
    if evidence:
        rec["evidence"] = evidence
    pipe = r.pipeline(transaction=False)
    pipe.hset(f"{ns}:capabilities", rec["id"], json.dumps(rec))
    for i in labels(rec):
        pipe.sadd(f"{ns}:idx:capability:label:{i}", rec["id"])
    pipe.execute()
    return rec


def get_many(r, ns, ids):
    ids = [i for i in dict.fromkeys(ids or []) if i]
    if not ns or not ids:
        return []
    return [json.loads(_decode(v)) for v in r.hmget(f"{ns}:capabilities", ids) if v]


def named(rec):
    """`rec` with each id's catalog name: [{id, name}] for mbc and attack."""
    cat = catalog()
    return {
        **rec,
        "mbc": [
            {"id": i, "name": (cat["mbc"].get(i) or {}).get("name")}
            for i in rec.get("mbc") or []
        ],
        "attack": [
            {"id": i, "name": (cat["attack"].get(i) or {}).get("name")}
            for i in rec.get("attack") or []
        ],
    }


def search(r, ns, label=None, limit=200):
    """Capabilities of a collection, optionally those carrying one MBC / ATT&CK id."""
    if label:
        ids = sorted(
            _decode(x) for x in r.smembers(f"{ns}:idx:capability:label:{label}")
        )
    else:
        ids = sorted(_decode(x) for x in r.hkeys(f"{ns}:capabilities"))
    return [named(c) for c in get_many(r, ns, ids[:limit])], len(ids)


# --- Analyst capabilities (plan G) ------------------------------------------
# A capability the analyst owns: a free-text name, optional MBC / ATT&CK ids and
# typed evidence pointers. The definition is global (like YARA rules), evidence
# is per collection (or pool), keyed on md5:addr so it survives every rebuild.
#   global:capability:{uuid}           str   definition JSON (name, description, mbc, attack, in, history)
#   global:capabilities                zset  uuid -> updated (ms)
#   global:idx:capability:label:{id}   set   uuids per MBC / ATT&CK id
#   {ns}:capability:{uuid}:ev          hash  evidence item id -> JSON
#   {ns}:capabilities:in               set   uuids accepted in this namespace
#   {ns}:idx:capability:file:{md5}     set   uuids with a pointer into that file

EV_TYPES = ("functions", "group", "string", "yara", "rulezet", "capa")
EV_FIDS_MAX = 500
# a group pointer keeps its N-way rows (each row = the matched copies across files)
GROUP_ROWS_MAX, GROUP_FIDS_MAX = 2000, 20000
NAME_MAX, DESC_MAX, HISTORY_MAX = 120, 1000, 50
_FID = re.compile(r"^[0-9a-f]{32}:[^:\s]{1,32}$")
_TEXT = {
    "why": 300,
    "label": 120,
    "name_id": 64,
    "gid": 120,
    "rule_uuid": 64,
    "value": 200,
    "rule": 120,
}


def _gkey(uid):
    return f"global:capability:{uid}"


def _ekey(ns, uid):
    return f"{ns}:capability:{uid}:ev"


def check_ids(ids, kind):
    """Catalog ids of one kind (mbc | attack), deduped; an unknown id raises."""
    if ids is None:
        return []
    if not isinstance(ids, list):
        raise ValueError(f"{kind} must be a list of ids")
    cat = catalog()[kind]
    out = list(dict.fromkeys(str(i).strip().upper() for i in ids if str(i).strip()))
    bad = [i for i in out if i not in cat]
    if bad:
        raise ValueError(f"unknown {kind} id: {bad[0][:20]}")
    return out


def _fid_list(v, what, cap=EV_FIDS_MAX):
    if v is None:
        return []
    if not isinstance(v, list) or not all(isinstance(f, str) for f in v):
        raise ValueError(f"{what} must be a list of md5:addr")
    out = list(dict.fromkeys(nway_sem_service.short(f).lower() for f in v))
    bad = [f for f in out if not _FID.match(f)]
    if bad:
        raise ValueError(f"{what}: not md5:addr: {bad[0][:80]}")
    if len(out) > cap:
        raise ValueError(f"{what}: at most {cap} per item")
    return out


def _group_rows(it):
    """A group pointer's rows ([[md5:addr, ...], ...]) and row names, checked."""
    rows = it.get("rows")
    if rows is None:
        return [], []
    if not isinstance(rows, list) or len(rows) > GROUP_ROWS_MAX:
        raise ValueError(f"rows must be a list of at most {GROUP_ROWS_MAX} rows")
    rows = [r for r in (_fid_list(x, "rows", GROUP_FIDS_MAX) for x in rows) if r]
    if sum(map(len, rows)) > GROUP_FIDS_MAX:
        raise ValueError(f"rows: at most {GROUP_FIDS_MAX} functions per group")
    names = it.get("row_names") if isinstance(it.get("row_names"), list) else []
    names = [str(n or "")[:120] for n in names[: len(rows)]]
    return rows, names


def clean_item(it):
    """One evidence pointer, checked and trimmed, with a content id (the same
    pointer added twice is one item)."""
    if not isinstance(it, dict) or it.get("type") not in EV_TYPES:
        raise ValueError(f"evidence type must be one of {', '.join(EV_TYPES)}")
    t = it["type"]
    out = {"type": t}
    # functions listed by their N-way rows: each row = its copies across files
    frows, fnames = _group_rows(it) if t == "functions" else ([], [])
    if frows:
        out["rows"], out["row_names"] = frows, fnames
        fids = list(dict.fromkeys(f for r in frows for f in r))
    else:
        fids = _fid_list(it.get("fids"), "fids") if t != "group" else []
    anchors = _fid_list(it.get("anchors"), "anchors")
    for k, n in _TEXT.items():
        v = it.get(k)
        if isinstance(v, str) and v.strip():
            out[k] = (v.strip() if k == "value" else " ".join(v.split()))[:n]
    if fids:
        out["fids"] = fids
    if t == "functions" and not fids:
        raise ValueError("a functions item needs fids")
    if t == "group":
        if not anchors:
            raise ValueError("a group item needs its anchors")
        out["anchors"] = anchors[: nway_sem_service.K]
        rows, names = _group_rows(it)
        if rows:
            out["rows"], out["row_names"] = rows, names
            out["fids"] = [f for r in rows for f in r]
    if t == "string" and "value" not in out:
        raise ValueError("a string item needs its value")
    if t in ("yara", "rulezet", "capa") and "rule" not in out:
        raise ValueError(f"a {t} item needs its rule")
    if isinstance(it.get("v"), int):
        out["v"] = it["v"]
    out["src"] = "llm" if it.get("src") == "llm" else "user"
    # a group is one pointer whatever rows it carries: its anchors are its identity
    key = [t, None if t == "group" else out.get("fids")]
    key += [out.get("anchors"), out.get("value"), out.get("rule")]
    out["id"] = hashlib.sha1(json.dumps(key).encode()).hexdigest()[:12]
    return out


def md5s_of(items):
    """Files the pointers land in (a group's anchors stand for its files)."""
    return {
        f.split(":")[0]
        for it in items
        for f in (it.get("fids") or []) + (it.get("anchors") or [])
    }


def _missing_fids(r, ns, items):
    """Function pointers with no function meta in collection `ns` (pools: none checked)."""
    if not ns or ns.startswith("global:"):
        return []
    # a group's rows are read off the N-way table (/capability/group_functions): format-checked only
    fids = sorted(
        {f for it in items if it["type"] != "group" for f in it.get("fids") or []}
    )
    if not fids:
        return []
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.exists(f"{ns}:func:{f}:meta")
    return [f for f, ok in zip(fids, pipe.execute()) if not ok]


def _loads(v):
    return json.loads(_decode(v)) if v else None


def put(r, ns, data, owner="user", replace=True):
    """Creates or updates one capability in `ns`. `replace` (the editor) sets the
    ids and this namespace's evidence to what is given; otherwise (accept onto an
    existing capability) ids and evidence are added. Returns (definition, evidence)."""
    from redis.exceptions import WatchError

    if not ns:
        raise ValueError("collection or pool is required")
    uid = data.get("uuid") or None
    name = " ".join(str(data.get("name") or "").split())[:NAME_MAX]
    if not uid and not name:
        raise ValueError("name is required")
    desc = data.get("description")
    mbc = check_ids(data.get("mbc"), "mbc")
    att = check_ids(data.get("attack"), "attack")
    ev = data.get("evidence") or []
    if not isinstance(ev, list):
        raise ValueError("evidence must be a list")
    items = {i["id"]: i for i in map(clean_item, ev)}
    missing = _missing_fids(r, ns, list(items.values()))
    if missing:
        raise ValueError(f"no such function in {ns}: {missing[0]}")
    owner = str(owner or "user")[:40]
    uid = uid or uuid.uuid4().hex
    gk, ek = _gkey(uid), _ekey(ns, uid)
    for _ in range(8):
        with r.pipeline() as p:
            try:
                p.watch(gk, ek)
                old = _loads(p.get(gk))
                if data.get("uuid") and not old:
                    raise LookupError(f"no capability {uid}")
                old_ev = {
                    _decode(k): json.loads(_decode(v)) for k, v in p.hgetall(ek).items()
                }
                now = int(time.time() * 1000)
                rec = dict(
                    old or {"uuid": uid, "created": now, "user": owner, "history": []}
                )
                if name and name != rec.get("name"):
                    if rec.get("name"):
                        rec["history"] = (rec.get("history") or []) + [
                            {
                                "at": now,
                                "user": owner,
                                "op": "rename",
                                "from": rec["name"],
                            }
                        ]
                    rec["name"] = name
                if isinstance(desc, str):
                    rec["description"] = desc.strip()[:DESC_MAX]
                old_labels = set(labels(rec))
                if replace:
                    rec["mbc"], rec["attack"] = mbc, att
                    new_ev = items
                else:
                    rec["mbc"] = list(dict.fromkeys((rec.get("mbc") or []) + mbc))
                    rec["attack"] = list(dict.fromkeys((rec.get("attack") or []) + att))
                    new_ev = {**old_ev, **items}
                rec["history"] = (rec.get("history") or [])[-HISTORY_MAX:]
                rec["in"] = sorted(set(rec.get("in") or []) | {ns})
                rec["updated"] = now
                gone = md5s_of(old_ev.values()) - md5s_of(new_ev.values())
                p.multi()
                p.set(gk, json.dumps(rec))
                p.zadd("global:capabilities", {uid: now})
                for i in old_labels - set(labels(rec)):
                    p.srem(f"global:idx:capability:label:{i}", uid)
                for i in labels(rec):
                    p.sadd(f"global:idx:capability:label:{i}", uid)
                if replace:
                    p.delete(ek)
                if new_ev:
                    p.hset(ek, mapping={k: json.dumps(v) for k, v in new_ev.items()})
                p.sadd(f"{ns}:capabilities:in", uid)
                for m in gone:
                    p.srem(f"{ns}:idx:capability:file:{m}", uid)
                for m in md5s_of(new_ev.values()):
                    p.sadd(f"{ns}:idx:capability:file:{m}", uid)
                p.execute()
                return rec, list(new_ev.values())
            except WatchError:
                continue
    raise RuntimeError("capability is being edited elsewhere, try again")


def remove(r, ns, uid):
    """Drops a capability's evidence in `ns`; the definition goes with its last namespace."""
    from redis.exceptions import WatchError

    gk, ek = _gkey(uid), _ekey(ns, uid)
    for _ in range(8):
        with r.pipeline() as p:
            try:
                p.watch(gk, ek)
                rec = _loads(p.get(gk))
                if not rec:
                    return False
                old_ev = [json.loads(_decode(v)) for v in p.hgetall(ek).values()]
                rec["in"] = [x for x in rec.get("in") or [] if x != ns]
                p.multi()
                p.delete(ek)
                p.srem(f"{ns}:capabilities:in", uid)
                for m in md5s_of(old_ev):
                    p.srem(f"{ns}:idx:capability:file:{m}", uid)
                if rec["in"]:
                    p.set(gk, json.dumps(rec))
                else:
                    p.delete(gk)
                    p.zrem("global:capabilities", uid)
                    for i in labels(rec):
                        p.srem(f"global:idx:capability:label:{i}", uid)
                p.execute()
                return True
            except WatchError:
                continue
    raise RuntimeError("capability is being edited elsewhere, try again")


def names_of(rec):
    """{id: catalog name} for a definition's ids."""
    cat = catalog()
    return {
        i: (cat[k].get(i) or {}).get("name")
        for k in ("mbc", "attack")
        for i in rec.get(k) or []
    }


def listing(r, ns, uid=None, label=None, q=None, md5s=None):
    """Capabilities accepted in `ns`, newest first, each with its evidence there and
    the files it points into (only `md5s` when given: a cluster's files)."""
    ids = {_decode(x) for x in r.smembers(f"{ns}:capabilities:in")}
    if uid:
        ids &= {uid}
    if label:
        ids &= {_decode(x) for x in r.smembers(f"global:idx:capability:label:{label}")}
    if md5s is not None:
        pipe = r.pipeline(transaction=False)
        for m in md5s:
            pipe.smembers(f"{ns}:idx:capability:file:{m}")
        ids &= {_decode(x) for s in pipe.execute() for x in s}
    ids = sorted(ids)
    if not ids:
        return []
    pipe = r.pipeline(transaction=False)
    for i in ids:
        pipe.get(_gkey(i))
        pipe.hgetall(_ekey(ns, i))
    res = pipe.execute()
    out, ql = [], (q or "").lower().strip()
    for k in range(len(ids)):
        rec = _loads(res[2 * k])
        if not rec or (ql and ql not in rec.get("name", "").lower()):
            continue
        ev = [json.loads(_decode(v)) for v in (res[2 * k + 1] or {}).values()]
        files = md5s_of(ev)
        if md5s is not None:
            files &= set(md5s)
        out.append(
            {**rec, "evidence": ev, "files": sorted(files), "names": names_of(rec)}
        )
    return sorted(out, key=lambda c: -c.get("updated", 0))


# --- Suggestions (model output, never stored in Kvrocks) ---------------------
# capsugg:{ns}:{gkey}          queue Redis, JSON list, SUGG_TTL
# capsugg:{ns}:{gkey}:pending  queue Redis, set while a group is being asked
SUGG_TTL = 30 * 86400
SUGG_MAX, ACCEPTED_MAX = 3, 40
# words a suggested name may use without a pointer that holds them
GENERIC = set(
    "udp tcp syn ack http https dns icmp gre vse stomp raw flood ddos dos attack "
    "telnet ssh ftp smb brute force credential scanner scanning scan exploit "
    "persistence cron init rc.d systemd killer kill process watchdog disable "
    "string strings decryption encryption decode encoding xor rc4 chacha aes "
    "c2 command control communication beacon download downloader upload proxy "
    "socks library runtime libc code helper helpers memory network socket "
    "random generation config configuration table setup main loop handler "
    "self delete daemon fork anti debug analysis obfuscation miner mining "
    "server client service bot bots competing game port router device iot web "
    "remote local file files shell reverse bind packet packets payload via and "
    "the for with from over".split()
)


def gkey(name_id, anchors):
    """Cache key of a group: its name record id, else its anchors."""
    if name_id:
        return name_id
    return "a" + hashlib.sha1(",".join(anchors or []).encode()).hexdigest()[:16]


def accepted_lines(r, ns):
    """(prompt lines, {code: uuid}) of the accepted capabilities the model may attach to."""
    caps = listing(r, ns)[:ACCEPTED_MAX]
    lines, amap = [], {}
    for n, c in enumerate(caps, 1):
        code = f"A{n}"
        amap[code] = c["uuid"]
        ids = " ".join((c.get("mbc") or []) + (c.get("attack") or []))
        lines.append(f"{code} {c['name']}" + (f" [{ids}]" if ids else ""))
    return "\n".join(lines), amap


def _holders(items, needle):
    n = _norm(needle)
    return [
        it["fid"]
        for it in items
        if n
        and n
        in _norm(
            " ".join(
                it.get("strings", [])
                + it.get("calls", [])
                + it.get("findings", [])
                + [it.get("snippet") or ""]
            )
        )
    ]


def unbacked(name, text):
    """Words of a suggested name that are neither generic nor in the evidence."""
    ev = _norm(text)
    words = re.findall(r"[a-z0-9][a-z0-9.\-]{2,}", name.lower())
    return [w for w in words if w not in GENERIC and w not in ev]


def build_suggestions(answer, lvl, text, items, group, amap, rows=None, row_names=None):
    """The model's answer as suggestions shaped like capabilities: name, ids (the
    quote rules of `clean`), attach_to, and evidence pointers resolved against the
    evidence items (the group itself, the functions holding a quote or a cited
    string, those strings, the rule findings quoted). With `rows` ({md5:addr: its
    N-way row's copies}) the functions pointer lists each one as its row, named by
    `row_names`, under the group's label."""
    rows, row_names = rows or {}, row_names or {}
    out = []
    raw = (answer or {}).get("suggestions") if isinstance(answer, dict) else None
    for s in (raw if isinstance(raw, list) else [])[:SUGG_MAX]:
        if not isinstance(s, dict):
            continue
        name = " ".join(str(s.get("name") or "").split())[:NAME_MAX]
        attach = amap.get(str(s.get("attach") or "").strip().upper())
        if not name and not attach:
            continue
        ids = clean(
            {"mbc": s.get("mbc"), "attack": s.get("attack"), "traits": [name]},
            lvl,
            text,
        ) or {"mbc": [], "attack": [], "quotes": {}}
        ev = [dict(group)]
        fids = []
        for q in ids["quotes"].values():
            fids += _holders(items, q)
        for v in s.get("strings") or []:
            if isinstance(v, str) and (h := _holders(items, v)):
                ev.append({"type": "string", "value": v.strip()[:200], "fids": h})
                fids += h
        for it in items:
            for f in it.get("findings") or []:
                if any(_norm(f) in _norm(q) for q in ids["quotes"].values()):
                    kind = "yara" if "yara" in f.lower() else "capa"
                    ev.append({"type": kind, "rule": f[:120], "fids": [it["fid"]]})
        if fids:
            fids = list(dict.fromkeys(fids))
            fn = {"type": "functions", "fids": fids, "label": group.get("label")}
            if any(rows.get(f) for f in fids):
                # a function with no known row stays, as a row of its one copy
                fn["rows"] = [rows.get(f) or [f] for f in fids]
                fn["row_names"] = [row_names.get(f) or "" for f in fids]
            ev.append(fn)
        for e in ev:
            e["src"] = "llm"
        ev = list({i["id"]: i for i in map(clean_item, ev)}.values())
        why = " ".join(str(s.get("why") or "").split())[:300]
        out.append(
            {
                "name": name,
                "attach_to": attach,
                "mbc": ids["mbc"],
                "attack": ids["attack"],
                "quotes": ids["quotes"],
                "why": why,
                "evidence": ev,
                "unbacked": unbacked(name, text) if name else [],
                "group": group.get("name_id") or group.get("label"),
            }
        )
    return out


def accepted_for_group(caps, name_id, anchors):
    """The accepted capabilities (from `listing`) with a group pointer at this group:
    same name record, or anchors that match it."""
    out = []
    for c in caps:
        for e in c.get("evidence") or []:
            if e.get("type") != "group":
                continue
            if (name_id and e.get("name_id") == name_id) or (
                anchors
                and nway_sem_service.similarity(e.get("anchors") or [], anchors)
                >= MATCH
            ):
                out.append(c)
                break
    return out


def _nname(s):
    return " ".join(re.findall(r"[a-z0-9]+", str(s or "").lower()))


def covered_by(s, acc):
    """uuid of the accepted capability (of the same group) a suggestion repeats:
    the one it attaches to, or one with the same name; None when it is new."""
    for c in acc:
        if s.get("attach_to") == c["uuid"] or (
            s.get("name") and _nname(s["name"]) == _nname(c.get("name"))
        ):
            return c["uuid"]
    return None


def _files(anchors):
    return sorted(
        {nway_sem_service.short(a).split(":")[0] for a in anchors or [] if ":" in a}
    )


def cache_put(red, ns, key, sugg, anchors=None):
    """Caches one group's suggestions, and indexes them by the files of the group's
    anchors (`capsugg:{ns}:f:{md5}`), so a cluster finds them whatever N-way set
    it stores later."""
    pipe = red.pipeline(transaction=False)
    pipe.set(
        f"capsugg:{ns}:{key}",
        json.dumps({"v": CAPS_V, "at": int(time.time() * 1000), "suggestions": sugg}),
        ex=SUGG_TTL,
    )
    pipe.delete(f"capsugg:{ns}:{key}:pending")
    for m in _files(anchors):
        pipe.sadd(f"capsugg:{ns}:f:{m}", key)
        pipe.expire(f"capsugg:{ns}:f:{m}", SUGG_TTL)
    pipe.execute()


def keys_for_files(red, ns, md5s):
    """Cache keys of the suggestions whose group has an anchor in these files."""
    pipe = red.pipeline(transaction=False)
    for m in md5s:
        pipe.smembers(f"capsugg:{ns}:f:{m}")
    return {_decode(k) for s in pipe.execute() for k in s}


# --- Background steps the UI shows (naming, suggesting, a Build job) ----------
# capsugg:{ns}:run  queue Redis hash, "{what}:{key}" -> JSON; a step past its ttl
# is a dead thread or a lost job and is dropped on read.
RUN_TTL, BUILD_TTL = 600, 6 * 3600


def run_mark(red, ns, what, key, label, anchors=None, cluster=None, job=None):
    if not ns:
        return
    ttl = BUILD_TTL if what == "build" else RUN_TTL
    v = {"what": what, "label": label, "files": _files(anchors), "cluster": cluster}
    v.update(job=job, at=int(time.time()), ttl=ttl)
    pipe = red.pipeline(transaction=False)
    pipe.hset(f"capsugg:{ns}:run", f"{what}:{key}", json.dumps(v))
    pipe.expire(f"capsugg:{ns}:run", BUILD_TTL)
    pipe.execute()


def run_done(red, ns, what, key):
    if ns:
        red.hdel(f"capsugg:{ns}:run", f"{what}:{key}")


def running(red, ns, md5s=None, cluster=None):
    """Live background steps of `ns` on these files or this cluster, oldest first."""
    k, now, out, dead = f"capsugg:{ns}:run", time.time(), [], []
    for f, v in (red.hgetall(k) or {}).items():
        v = _loads(v)
        if not v or now - v.get("at", 0) > v.get("ttl", RUN_TTL):
            dead.append(f)
        elif (
            md5s is None
            or (cluster and v.get("cluster") == cluster)
            or set(v.get("files") or []) & md5s
        ):
            out.append(v)
    if dead:
        red.hdel(k, *dead)
    return sorted(out, key=lambda v: v["at"])


def cache_get(red, ns, keys):
    """{key: {suggestions, at} | None} and the keys still being asked."""
    keys = list(dict.fromkeys(keys))
    if not keys:
        return {}, []
    vals = red.mget([f"capsugg:{ns}:{k}" for k in keys])
    pend = red.mget([f"capsugg:{ns}:{k}:pending" for k in keys])
    got = {k: _loads(v) for k, v in zip(keys, vals)}
    got = {k: (v if v and v.get("v") == CAPS_V else None) for k, v in got.items()}
    return got, [k for k, p in zip(keys, pend) if p and not got[k]]


if __name__ == "__main__":
    b, m, a = BEHAVIOURS, MICRO, ATTACK
    cat = catalog()
    missing = [i for i in b + m if i not in cat["mbc"]] + [
        i for i in a if i not in cat["attack"]
    ]
    assert not missing, missing
    assert all(cat["mbc"][i]["micro"] for i in m) and not any(
        cat["mbc"][i]["micro"] for i in b
    )
    lines, mbc, att = prompt_lists("behaviour")
    assert (
        "B0033 Denial of Service" in lines and "T1498.001" in att and "C0001" not in mbc
    )
    ev = (
        "Library calls: socket, sendto, random\nStrings: /dev/urandom | POST /cdn-cgi/\n"
        "Function 1 code:\nwhile (1) { sendto(fd,buf,len,0,&sa,0x10); }"
    )
    got = clean(
        {
            "mbc": [
                {"id": "B0033", "quote": "sendto(fd,buf,len"},
                {"id": "B0019", "quote": "socket"},  # a bare call backs no behaviour
                {"id": "C0001", "quote": "socket"},  # micro id at behaviour level
                {"id": "B0031", "quote": "domain generation"},  # quote not in evidence
            ],
            "attack": [{"id": "T1498.001 Direct Network Flood", "quote": " SENDTO "}],
            "traits": [
                "UDP flood",
                "udp flood",
                "a much too long trait that goes on and on",
            ],
        },
        "behaviour",
        ev,
    )
    # T1498.001's quote is the bare call name: not enough for a technique
    assert got["mbc"] == ["B0033"] and got["attack"] == [], got
    assert (
        got["traits"] == ["udp flood"] and got["quotes"]["B0033"] == "sendto(fd,buf,len"
    ), got
    # ... but a bare call is a micro-behaviour's evidence
    assert clean({"mbc": [{"id": "C0001", "quote": "socket"}]}, "micro", ev)["mbc"] == [
        "C0001"
    ]
    assert "C0007" not in MICRO
    assert clean({"mbc": [{"id": "B0033", "quote": "sendto"}]}, "micro", ev) is None
    # an echoed id with no quote is not evidence
    assert (
        clean({"mbc": ["C0021 Generate Pseudo-random Sequence"]}, "micro", ev) is None
    )
    # the head of the offered list, in order, is the model copying it
    head = [{"id": i, "quote": "/dev/urandom"} for i in BEHAVIOURS[:3]]
    assert clean({"mbc": head, "traits": ["x"]}, "behaviour", ev)["mbc"] == []
    assert (
        schema("micro")["properties"]["mbc"]["items"]["properties"]["id"]["enum"]
        == MICRO
    )

    class _R:  # the redis calls save / search use, over dicts
        def __init__(self):
            self.h, self.s = {}, {}

        def pipeline(self, transaction=False):
            return self

        def execute(self):
            return []

        def smembers(self, k):
            return set(self.s.get(k, ()))

        def sadd(self, k, v):
            self.s.setdefault(k, set()).add(v)

        def hmget(self, k, fs):
            return [self.h.get(k, {}).get(f) for f in fs]

        def hset(self, k, f, v):
            self.h.setdefault(k, {})[f] = v

        def hkeys(self, k):
            return list(self.h.get(k, {}))

    r = _R()
    lane = nway_sem_service.anchors(f"c:func:{i:032x}:1" for i in range(60))
    near = nway_sem_service.anchors(f"c:func:{i:032x}:1" for i in range(55))
    other = nway_sem_service.anchors(f"c:func:{i:032x}:1" for i in range(500, 560))
    a = save(r, "c", lane, got, "behaviour", group="n1")
    plain = lambda t: {"mbc": ["B0033"], "attack": [], "traits": [t], "quotes": {}}
    b = save(r, "c", near, plain("kill competing bots"), "behaviour", group="n2")
    c = save(r, "c", other, plain("x"), "behaviour", group="n3")
    assert a["id"] == b["id"] and b["groups"] == ["n1", "n2"]
    assert b["traits"] == ["udp flood", "kill competing bots"], b
    assert c["id"] != a["id"]
    found, n = search(r, "c", "B0033")
    assert n == 2 and found[0]["mbc"][0]["name"] == "Denial of Service", found
    # analyst capabilities: pointers are checked, trimmed and deduped by content
    m5 = "a" * 32
    it = clean_item(
        {"type": "functions", "fids": [f"c:func:{m5}:00401000", f"{m5}:00401000"]}
    )
    assert it["fids"] == [f"{m5}:00401000"] and it["src"] == "user", it
    same = {"type": "functions", "fids": [f"{m5}:00401000"], "why": " x  y "}
    assert clean_item(same)["id"] == it["id"] and clean_item(same)["why"] == "x y"
    for bad in (
        {"type": "nope"},
        {"type": "functions"},
        {"type": "functions", "fids": ["not-a-fid"]},
        {"type": "group", "fids": [f"{m5}:1"]},
        {"type": "string"},
        {"type": "yara", "fids": [f"{m5}:1"]},
    ):
        try:
            clean_item(bad)
            raise AssertionError(bad)
        except ValueError:
            pass
    g = clean_item(
        {"type": "group", "anchors": [f"{m5}:{i}" for i in range(30)], "label": "F"}
    )
    assert len(g["anchors"]) == nway_sem_service.K and md5s_of([g, it]) == {m5}
    # a group keeps its N-way rows; it stays one pointer with or without them
    m6 = "b" * 32
    gr = clean_item(
        {
            "type": "group",
            "anchors": [f"{m5}:{i}" for i in range(30)],
            "rows": [[f"{m5}:1", f"{m6}:1"], [f"{m5}:2"], []],
            "row_names": ["attack_udp", "FUN_2", "extra"],
            "gid": "lane:0011",
        }
    )
    assert gr["rows"] == [[f"{m5}:1", f"{m6}:1"], [f"{m5}:2"]], gr
    assert gr["row_names"] == ["attack_udp", "FUN_2"] and gr["gid"] == "lane:0011"
    assert gr["fids"] == [f"{m5}:1", f"{m6}:1", f"{m5}:2"] and gr["id"] == g["id"]
    assert md5s_of([gr]) == {m5, m6}
    assert check_ids(["b0033", "B0033"], "mbc") == ["B0033"]
    try:
        check_ids(["T9999"], "attack")
        raise AssertionError("unknown id")
    except ValueError:
        pass
    # suggestions: quotes resolve to the functions and strings holding them
    f1, f2 = f"{m5}:00401000", f"{m5}:00402000"
    items = [
        {"fid": f1, "strings": ["minecraft"], "calls": ["sendto"], "findings": [],
         "snippet": "while (1) { sendto(fd,buf,len,0,&sa,0x10); }"},
        {"fid": f2, "strings": ["/proc/net/tcp"], "calls": [], "findings": [], "snippet": ""},
    ]  # fmt: skip
    ev_text = "Strings: minecraft | /proc/net/tcp\n" + items[0]["snippet"]
    grp = {"type": "group", "anchors": [f1, f2], "label": "Attack floods"}
    ans = {
        "suggestions": [
            {"name": "Minecraft server flood", "mbc": [{"id": "B0033", "quote": "sendto(fd,buf,len"}],
             "strings": ["minecraft", "not in evidence"], "why": "flood loop"},
            {"attach": "a1", "name": ""},
            {"attach": "A9", "name": ""},  # unknown code, no name: dropped
        ]
    }  # fmt: skip
    m7 = "c" * 32
    sgr = build_suggestions(
        ans, "behaviour", ev_text, items, grp, {"A1": "u1"},
        rows={f1: [f1, f"{m7}:00501000"]}, row_names={f1: "attack_udp_plain"},
    )  # fmt: skip
    fnr = next(e for e in sgr[0]["evidence"] if e["type"] == "functions")
    assert fnr["rows"] == [[f1, f"{m7}:00501000"]], fnr
    assert fnr["row_names"] == ["attack_udp_plain"] and fnr["label"] == "Attack floods"
    assert fnr["fids"] == [f1, f"{m7}:00501000"] and md5s_of([fnr]) == {m5, m7}
    sg = build_suggestions(ans, "behaviour", ev_text, items, grp, {"A1": "u1"})
    assert len(sg) == 2 and sg[1]["attach_to"] == "u1", sg
    s0 = sg[0]
    assert s0["mbc"] == ["B0033"] and s0["unbacked"] == [], s0
    kinds = {e["type"]: e for e in s0["evidence"]}
    assert (
        kinds["functions"]["fids"] == [f1] and kinds["string"]["value"] == "minecraft"
    )
    assert kinds["group"]["label"] == "Attack floods" and all(
        e["src"] == "llm" for e in s0["evidence"]
    )
    assert unbacked("Fortnite server flood", ev_text) == ["fortnite"]
    assert gkey("n1", []) == "n1" and gkey(None, [f1]).startswith("a")
    # an accepted capability covers the suggestions of its group, by attach or name
    acc_caps = [
        {"uuid": "u1", "name": "UDP Flood", "evidence": [dict(grp, name_id="n1")]},
        {"uuid": "u2", "name": "other", "evidence": [it]},
    ]
    mine = accepted_for_group(acc_caps, None, grp["anchors"])
    assert [c["uuid"] for c in mine] == ["u1"]
    assert accepted_for_group(acc_caps, "n1", [])[0]["uuid"] == "u1"
    assert covered_by({"name": "udp  flood!"}, mine) == "u1"
    assert covered_by({"attach_to": "u1", "name": ""}, mine) == "u1"
    assert covered_by({"name": "telnet brute force"}, mine) is None

    class _Q:  # queue redis over dicts; a pipeline runs each call at once
        def __init__(self):
            self.kv, self.res = {}, []

        def pipeline(self, transaction=False):
            return self

        def execute(self):
            out, self.res = self.res, []
            return out

        def _do(self, v):
            self.res.append(v)
            return v

        def set(self, k, v, ex=None):
            self.kv[k] = v

        def delete(self, *ks):
            [self.kv.pop(k, None) for k in ks]

        def expire(self, k, s):
            pass

        def sadd(self, k, v):
            self.kv.setdefault(k, set()).add(v)

        def smembers(self, k):
            return self._do(set(self.kv.get(k, ())))

        def mget(self, ks):
            return [self.kv.get(k) for k in ks]

        def hset(self, k, f, v):
            self.kv.setdefault(k, {})[f] = v

        def hdel(self, k, *fs):
            [self.kv.get(k, {}).pop(f, None) for f in fs]

        def hgetall(self, k):
            return dict(self.kv.get(k, {}))

    q = _Q()
    # a suggestion is found by the files of its group, whatever set is stored later
    cache_put(q, "c", "n1", sg, grp["anchors"])
    assert keys_for_files(q, "c", [m5, m6]) == {"n1"} and not keys_for_files(
        q, "c", [m6]
    )
    got, _ = cache_get(q, "c", ["n1"])
    assert got["n1"]["suggestions"][0]["name"] == "Minecraft server flood"
    # background steps: by file or cluster, done ones and dead ones go
    run_mark(q, "c", "suggest", "n1", "Attack floods", anchors=[f1])
    run_mark(q, "c", "build", "cu1", "Build", cluster="cu1", job="j1")
    run_mark(q, "c", "name", "k2", "other", anchors=[f"{m6}:1"])
    assert [v["what"] for v in running(q, "c", {m5}, "cu1")] == ["suggest", "build"]
    run_done(q, "c", "suggest", "n1")
    q.kv["capsugg:c:run"]["build:cu1"] = json.dumps({"at": 0, "ttl": 1})
    assert running(q, "c", {m5}, "cu1") == [] and len(q.kv["capsugg:c:run"]) == 1
    print("ok")
