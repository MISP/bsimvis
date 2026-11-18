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
    "C0032", "C0024", "C0025", "C0019", "C0033", "C0034", "C0007",
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
CAPS_V = 3  # bump when the capability prompt or parsing changes: groups are asked again
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
    ev = "Library calls: socket, sendto, random\nStrings: /dev/urandom | POST /cdn-cgi/"
    got = clean(
        {
            "mbc": [
                {"id": "B0033", "quote": "sendto"},
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
    assert got["mbc"] == ["B0033"] and got["attack"] == ["T1498.001"], got
    assert got["traits"] == ["udp flood"] and got["quotes"]["B0033"] == "sendto", got
    assert clean({"mbc": [{"id": "B0033", "quote": "sendto"}]}, "micro", ev) is None
    # an echoed id with no quote is not evidence
    assert (
        clean({"mbc": ["C0021 Generate Pseudo-random Sequence"]}, "micro", ev) is None
    )
    # the head of the offered list, in order, is the model copying it
    head = [{"id": i, "quote": "socket"} for i in BEHAVIOURS[:3]]
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
    print("ok")
