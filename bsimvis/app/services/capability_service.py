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
import os
import time
import uuid

from bsimvis.app.services import nway_sem_service
from bsimvis.app.services.config_service import config_service

CATALOG = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "data", "capabilities", "catalog.json"
)
# Curated for Linux / IoT malware and general native code; [capabilities] overrides.
BEHAVIOURS = [
    "B0001", "B0007", "B0009", "B0011", "B0018", "B0019", "B0022", "B0023",
    "B0024", "B0025", "B0027", "B0029", "B0030", "B0031", "B0032", "B0033",
    "B0038", "B0040", "B0047", "E1014", "E1059", "E1082", "E1083", "E1105",
    "E1485", "E1486", "E1564", "E1643", "F0001", "F0004", "F0007", "F0010",
    "F0014",
]  # fmt: skip
MICRO = [
    "C0001", "C0002", "C0003", "C0004", "C0007", "C0011", "C0014", "C0016",
    "C0017", "C0018", "C0019", "C0021", "C0024", "C0025", "C0026", "C0027",
    "C0029", "C0030", "C0031", "C0032", "C0033", "C0034", "C0038", "C0042",
    "C0045", "C0047", "C0051", "C0052", "C0053",
]  # fmt: skip
ATTACK = [
    "T1014", "T1016", "T1021.004", "T1027", "T1036", "T1037", "T1046", "T1049",
    "T1053.003", "T1057", "T1059.004", "T1070.004", "T1071.001", "T1071.004",
    "T1078.001", "T1082", "T1083", "T1090", "T1095", "T1105", "T1110.001",
    "T1140", "T1190", "T1210", "T1222", "T1485", "T1486", "T1489", "T1496",
    "T1497", "T1498.001", "T1498.002", "T1499", "T1543", "T1564",
    "T1568.002", "T1571", "T1573", "T1595", "T1685",
]  # fmt: skip
MICRO_BELOW = 8  # a group with fewer rows gets micro-behaviours
MATCH = 0.5  # anchor similarity two answers need to be one capability
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
    """The id lines the model chooses from, for one level."""
    b, m, a = vocab()
    cat = catalog()
    mbc = m if lvl == "micro" else b
    lines = ["MBC ids:"] + [f"{i} {cat['mbc'][i]['name']}" for i in mbc]
    if lvl != "micro":
        lines += ["ATT&CK ids:"] + [f"{i} {cat['attack'][i]['name']}" for i in a]
    return "\n".join(lines), set(mbc), set(a) if lvl != "micro" else set()


def clean(answer, lvl):
    """Keeps only curated ids of the right level; None when nothing is left."""
    _, mbc, att = prompt_lists(lvl)
    a = answer if isinstance(answer, dict) else {}
    out = {
        "mbc": [i for i in dict.fromkeys(map(str, a.get("mbc") or [])) if i in mbc][:6],
        "attack": [
            i for i in dict.fromkeys(map(str, a.get("attack") or [])) if i in att
        ][:6],
        "text": " ".join(str(a.get("text") or "").split())[:240],
    }
    return out if out["mbc"] or out["attack"] or out["text"] else None


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
        rec["text"] = answer["text"] or rec.get("text", "")
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
    got = clean(
        {
            "mbc": ["B0033", "C0001", "X"],
            "attack": ["T1498.001"],
            "text": " UDP  flood ",
        },
        "behaviour",
    )
    assert got == {"mbc": ["B0033"], "attack": ["T1498.001"], "text": "UDP flood"}, got
    assert clean({"mbc": ["B0033"]}, "micro") is None

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
    b = save(
        r,
        "c",
        near,
        {"mbc": ["B0033"], "attack": [], "text": ""},
        "behaviour",
        group="n2",
    )
    c = save(
        r,
        "c",
        other,
        {"mbc": ["B0033"], "attack": [], "text": "x"},
        "behaviour",
        group="n3",
    )
    assert (
        a["id"] == b["id"] and b["groups"] == ["n1", "n2"] and b["text"] == "UDP flood"
    )
    assert c["id"] != a["id"]
    found, n = search(r, "c", "B0033")
    assert n == 2 and found[0]["mbc"][0]["name"] == "Denial of Service", found
    print("ok")
