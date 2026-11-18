"""Persistent N-way semantics: group names and per-set summaries that survive rebuilds.

An N-way doc and its group ids (`lane:{mask}`, `region:r{n}`) change with every
rebuild, column order or tag write. What survives is the code: a group is
recognised again by its anchors, a bottom-k MinHash sketch of its function ids
(`md5:addr`), and a stored group matches a fresh one when the sketches' Jaccard
estimate is at least MATCH. Names are per collection (or pool): a lane renamed in
one set is named the same in any set that holds that code from the same files.

Kvrocks keys, `ns` = collection or `global:pool:{id}`:
  {ns}:nway_names           hash  name id -> JSON record
  {ns}:nway_names:anchor    hash  anchor -> JSON list of name ids
  {ns}:nway_sem:set:{hash}  str   JSON summary of one member set
  {ns}:nway_sem:sets        zset  set hash -> saved at (ms)
  {ns}:nway_sem:by_cluster  hash  bin cluster uuid -> set hash
  {ns}:nway_sem:ev:{hash}   hash  gid -> JSON: what the model reads for that group
                                  (naming payload + each read function's row)
"""

import hashlib
import json
import time
import uuid
import zlib

K = 20  # anchors per group
MATCH = 0.5  # sketch Jaccard a stored group needs to name a fresh one
NAMED_KINDS = ("lane", "region", "presence", "delta", "born")
SET_GROUPS_MAX = 400


def ns_of(collection=None, pool=None):
    """Registry namespace, or None for a set that spans collections without a pool."""
    if pool:
        return f"global:pool:{pool}"
    return collection or None


def short(fid):
    """`{coll}:func:{md5}:{addr}` -> `md5:addr` (the file, not the collection, owns it)."""
    _, _, rest = str(fid).partition(":func:")
    return rest or str(fid)


def _crc(a):
    return zlib.crc32(a.encode())


def anchors(fids, k=K):
    """Bottom-k sketch: the k anchors with the lowest crc32, in crc order."""
    return sorted({short(f) for f in fids}, key=_crc)[:k]


def similarity(a, b, k=K):
    """Jaccard estimate of the two sets two bottom-k sketches came from."""
    a, b = set(a), set(b)
    union = sorted(a | b, key=_crc)[:k]
    return sum(1 for x in union if x in a and x in b) / len(union) if union else 0.0


def _decode(v):
    return v.decode() if isinstance(v, bytes) else v


def _records(r, ns, ids):
    ids = list(dict.fromkeys(ids))
    if not ids:
        return {}
    raw = r.hmget(f"{ns}:nway_names", ids)
    return {i: json.loads(_decode(v)) for i, v in zip(ids, raw) if v}


def lookup(r, ns, groups):
    """{group id: stored record} for `groups` [{id, kind, anchors}].

    One stored record names at most one group, the best match, so a sub-lane
    does not take its parent lane's name."""
    groups = [g for g in groups if g.get("anchors")]
    if not ns or not groups:
        return {}
    every = sorted({a for g in groups for a in g["anchors"]})
    raw = r.hmget(f"{ns}:nway_names:anchor", every)
    cand = {}
    for a, v in zip(every, raw):
        for nid in json.loads(_decode(v)) if v else []:
            cand.setdefault(nid, set()).add(a)
    recs = _records(r, ns, cand)
    scored = []
    for g in groups:
        mine = set(g["anchors"])
        for nid, rec in recs.items():
            if rec.get("kind") != g.get("kind") or not (cand[nid] & mine):
                continue
            s = similarity(rec["anchors"], g["anchors"])
            if s >= MATCH:
                scored.append((s, g["id"], nid))
    out, used = {}, set()
    for s, gid, nid in sorted(scored, reverse=True):
        if gid not in out and nid not in used:
            out[gid] = {**recs[nid], "score": round(s, 3)}
            used.add(nid)
    return out


def remember(r, ns, kind, sketch, llm=None, user_name=None, user=None, **extra):
    """Upsert the record matching (kind, sketch); returns it.

    `llm` replaces the model's answer; `user_name` (a string, "" to clear) is the
    analyst's name, which the model never overwrites."""
    if not ns or not sketch:
        return None
    hit = lookup(r, ns, [{"id": "_", "kind": kind, "anchors": sketch}]).get("_")
    now = int(time.time() * 1000)
    rec = hit or {"id": uuid.uuid4().hex[:16], "kind": kind, "created": now}
    rec.pop("score", None)
    old = set(rec.get("anchors") or [])
    rec["anchors"] = list(sketch)
    rec["updated"] = now
    rec.update({k: v for k, v in extra.items() if v is not None})
    if llm is not None:
        rec["llm"] = llm
    if user_name is not None:
        hist = rec.setdefault("history", [])
        hist.append({"name": user_name, "user": user, "at": now})
        del hist[:-10]
        if user_name:
            rec["user_name"], rec["user"] = user_name, user
        else:
            rec.pop("user_name", None)
            rec.pop("user", None)
    key = f"{ns}:nway_names:anchor"
    pipe = r.pipeline(transaction=False)
    pipe.hset(f"{ns}:nway_names", rec["id"], json.dumps(rec))
    # ponytail: read-modify-write per anchor; two writers on one anchor can drop
    # an id, which only costs a missed match until the next write. WATCH if it bites.
    touched = sorted(old | set(sketch))
    for a, v in zip(touched, r.hmget(key, touched)):
        ids = set(json.loads(_decode(v))) if v else set()
        if a in sketch:
            ids.add(rec["id"])
        else:
            ids.discard(rec["id"])
        if ids:
            pipe.hset(key, a, json.dumps(sorted(ids)))
        else:
            pipe.hdel(key, a)
    pipe.execute()
    return rec


def shown_name(rec):
    """The analyst's name first, then the model's."""
    if not rec:
        return None
    return rec.get("user_name") or (rec.get("llm") or {}).get("name")


def attach(r, ns, groups):
    """Puts the stored names on /nway group metas in place: `name_id`,
    `user_name` and `stored_name` (the model's). Never raises."""
    try:
        named = [
            {"id": gid, "kind": g.get("kind"), "anchors": g.get("anchors")}
            for gid, g in groups.items()
            if g.get("kind") in NAMED_KINDS
        ]
        hits = lookup(r, ns, named)
    except Exception:
        return groups
    for gid, g in groups.items():
        g["ns"] = ns
        rec = hits.get(gid)
        for k in ("name_id", "user_name", "stored_name"):
            g.pop(k, None)
        if rec:
            g["name_id"] = rec["id"]
            if rec.get("user_name"):
                g["user_name"] = rec["user_name"]
            if (rec.get("llm") or {}).get("name"):
                g["stored_name"] = rec["llm"]["name"]
    return groups


# --- per-set summary ---------------------------------------------------------


def set_hash(ns, md5s):
    return hashlib.md5(json.dumps([ns, sorted(md5s)]).encode()).hexdigest()


def members_of(doc):
    return sorted({c["md5"] for c in (doc.get("file_columns") or doc["columns"])})


def summary(doc, groups, params, cluster_uuid=None):
    """The small part of a set worth keeping: tiers, groups and their anchors."""
    tiers = {
        g["key"]: g.get("rows", 0)
        for g in groups.values()
        if g.get("kind") == "tier" and g.get("key")
    }
    keep = [
        {
            "gid": gid,
            "kind": g.get("kind"),
            "parent": (
                (g.get("path") or [None, None])[-2]
                if len(g.get("path") or []) > 1
                else None
            ),
            "tier": g.get("tier"),
            "files": g.get("files"),
            "rows": g.get("rows"),
            "label": g.get("label"),
            "tags": (g.get("tags") or [])[:5],
            "anchors": g.get("anchors"),
            "name_id": g.get("name_id"),
        }
        for gid, g in groups.items()
        if g.get("kind") in NAMED_KINDS + ("tier",)
    ]
    keep.sort(key=lambda g: -(g["rows"] or 0))
    return {
        "members": members_of(doc),
        "cluster_uuid": cluster_uuid,
        "doc_key": doc.get("key"),
        "params": {
            k: params.get(k)
            for k in ("group", "scope", "axis", "min_edge", "pair_k")
            if params.get(k)
        },
        "rows": len(doc.get("rows") or []),
        "tiers": tiers,
        "groups": keep[:SET_GROUPS_MAX],
        "groups_total": len(keep),
    }


def stored_meta(r, ns, md5s):
    """The stored summary of this member set as saved (no names attached), or None."""
    raw = r.get(f"{ns}:nway_sem:set:{set_hash(ns, md5s)}")
    return json.loads(_decode(raw)) if raw else None


def load_ev(r, ns, set_hash_, gids):
    """{gid: evidence payload} of a stored set's groups (only the default build has them)."""
    gids = list(dict.fromkeys(gids))
    if not gids:
        return {}
    raw = r.hmget(f"{ns}:nway_sem:ev:{set_hash_}", gids)
    return {g: json.loads(_decode(v)) for g, v in zip(gids, raw) if v}


def save_set(r, ns, rec, ev=None):
    """Stores one set summary, and `ev` ({gid: evidence}) beside it; returns its set hash."""
    if not ns:
        return None
    h = set_hash(ns, rec["members"])
    key = f"{ns}:nway_sem:set:{h}"
    now = int(time.time() * 1000)
    pipe = r.pipeline(transaction=False)
    pipe.set(key, json.dumps({**rec, "set_hash": h, "ns": ns, "saved_at": now}))
    pipe.delete(f"{ns}:nway_sem:ev:{h}")
    if ev:
        pipe.hset(
            f"{ns}:nway_sem:ev:{h}", mapping={g: json.dumps(v) for g, v in ev.items()}
        )
    pipe.zadd(f"{ns}:nway_sem:sets", {h: now})
    if rec.get("cluster_uuid"):
        pipe.hset(f"{ns}:nway_sem:by_cluster", rec["cluster_uuid"], h)
    pipe.execute()
    return h


def load_set(r, ns, set_hash_=None, cluster_uuid=None):
    """A stored set summary with each group's current name, or None."""
    if not ns:
        return None
    if not set_hash_ and cluster_uuid:
        set_hash_ = _decode(r.hget(f"{ns}:nway_sem:by_cluster", cluster_uuid))
    raw = set_hash_ and r.get(f"{ns}:nway_sem:set:{set_hash_}")
    if not raw:
        return None
    rec = json.loads(_decode(raw))
    names = _records(r, ns, [g["name_id"] for g in rec["groups"] if g.get("name_id")])
    hits = lookup(
        r,
        ns,
        [
            {"id": g["gid"], "kind": g["kind"], "anchors": g["anchors"]}
            for g in rec["groups"]
        ],
    )
    found = {}
    for g in rec["groups"]:
        n = hits.get(g["gid"]) or names.get(g.get("name_id"))
        g["name"] = shown_name(n)
        g["name_source"] = (n and ("user" if n.get("user_name") else "llm")) or None
        g["name_id"] = n and n["id"]
        found[g["gid"]] = (n or {}).get("caps") or []
    _attach_caps(r, ns, rec, found)
    return rec


def _attach_caps(r, ns, rec, found):
    """Each group's capabilities (`caps`), and `capabilities`: one entry per MBC /
    ATT&CK id over the set, ranked by the rows of the groups holding it."""
    from bsimvis.app.services import capability_service

    caps = {
        c["id"]: capability_service.named(c)
        for c in capability_service.get_many(
            r, ns, [i for ids in found.values() for i in ids]
        )
    }
    by_label, by_trait = {}, {}
    for g in rec["groups"]:
        g["caps"] = [
            {
                k: caps[i].get(k)
                for k in ("id", "mbc", "attack", "traits", "quotes", "level")
            }
            for i in found.get(g["gid"], [])
            if i in caps
        ]
        for c in g["caps"]:
            for kind in ("mbc", "attack"):
                for x in c[kind] or []:
                    e = by_label.setdefault(
                        x["id"],
                        {
                            "id": x["id"],
                            "name": x["name"],
                            "kind": kind,
                            "groups": [],
                            "rows": 0,
                        },
                    )
                    if g["gid"] not in e["groups"]:
                        e["groups"].append(g["gid"])
                        e["rows"] += g.get("rows") or 0
            for t in c.get("traits") or []:
                e = by_trait.setdefault(t, {"trait": t, "groups": [], "rows": 0})
                if g["gid"] not in e["groups"]:
                    e["groups"].append(g["gid"])
                    e["rows"] += g.get("rows") or 0
    rec["capabilities"] = sorted(by_label.values(), key=lambda e: -e["rows"])
    rec["traits"] = sorted(by_trait.values(), key=lambda e: -e["rows"])
    rec["capabilities_pending"] = sum(
        1 for g in rec["groups"] if g.get("name") and not found.get(g["gid"])
    )


if __name__ == "__main__":
    a = [f"c:func:{i:032x}:{j}" for i in range(40) for j in range(10)]
    s_all, s_most = anchors(a), anchors(a[:360])  # a set that lost 4 of 40 files
    assert len(s_all) == K and similarity(s_all, s_all) == 1.0
    assert similarity(s_all, s_most) >= MATCH, similarity(s_all, s_most)
    other = anchors(
        [f"c:func:{i:032x}:{j}" for i in range(100, 140) for j in range(10)]
    )
    assert similarity(s_all, other) == 0.0
    assert short("c:func:abc:401000") == "abc:401000"

    class _R:  # the few redis calls used here, over dicts
        def __init__(self):
            self.h, self.s = {}, {}

        def pipeline(self, transaction=False):
            return self

        def execute(self):
            return []

        def hmget(self, k, fs):
            return [self.h.get(k, {}).get(f) for f in fs]

        def hget(self, k, f):
            return self.h.get(k, {}).get(f)

        def hset(self, k, f, v):
            self.h.setdefault(k, {})[f] = v

        def hdel(self, k, f):
            self.h.get(k, {}).pop(f, None)

        def get(self, k):
            return self.s.get(k)

        def set(self, k, v):
            self.s[k] = v

        def zadd(self, k, m):
            pass

    r = _R()
    sub = anchors(a[:200])  # a sub-lane: half of the lane's files
    rec = remember(r, "c", "lane", s_all, llm={"name": "scanner", "v": 10})
    remember(r, "c", "lane", sub, llm={"name": "scanner part", "v": 10})
    # the fresh lane (lost 4 files) gets the lane's name, the sub-lane its own
    hits = lookup(
        r,
        "c",
        [
            {"id": "L", "kind": "lane", "anchors": s_most},
            {"id": "S", "kind": "lane", "anchors": sub},
        ],
    )
    assert (
        hits["L"]["id"] == rec["id"] and hits["S"]["llm"]["name"] == "scanner part"
    ), hits
    assert not lookup(r, "c", [{"id": "R", "kind": "region", "anchors": s_all}])
    # a user name wins over the model and survives a later model answer
    remember(r, "c", "lane", s_most, user_name="Telnet scanner", user="t")
    again = remember(r, "c", "lane", s_all, llm={"name": "scan loop", "v": 11})
    assert again["id"] == rec["id"] and shown_name(again) == "Telnet scanner"
    assert shown_name(remember(r, "c", "lane", s_all, user_name="")) == "scan loop"
    print("ok")
