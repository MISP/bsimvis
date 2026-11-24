"""File cluster uuid history: a cluster's uuid outlives a rebuild.

A rebuild renumbers the tree and used to mint a fresh uuid for every node, so
URLs, bookmarks, tags and notes (all keyed by uuid) were orphaned. Now:

- the cluster about to be deleted is *snapshotted* (`snapshot`): uuid, name,
  size, a hash of its member set and a crc-ordered member sample (anchors);
- the next persist (`carry_uuids`) gives a new cluster with the exact same
  member set the old uuid back, so an unchanged cluster keeps everything;
- every other old uuid gets a history record with its successors (estimated
  Jaccard from the anchors) and a kind: reshaped / merged / split / dissolved.
  If it carried analyst data (tags, notes, a custom name) its best successor is
  flagged (`inherit`) so the UI can propose to copy that data across.

Old tags and notes stay on the old uuid's sidecar fields: nothing is moved
without the analyst, so nothing is lost.

Keys (`ns` = `{collection}:bin_cluster`, `a` = the algo namespace):
  {ns}:retired:{a}       hash uuid -> snapshot, consumed by the next persist
  {ns}:uuid_history:{a}  hash uuid -> history record
  {ns}:inherit:{a}       hash live uuid -> [old uuids with data to propose]
"""

import hashlib
import json
import time
import zlib

ANCHORS = 32
SUCCESSORS = 5
AXES = ("overall", "code", "library", "content")


def _key(collection, kind, algo_ns):
    return f"{collection}:bin_cluster:{kind}:{algo_ns}"


def _member_key(m):
    for sep in (":func:", ":file:"):
        if sep in m:
            return m.split(sep, 1)[1]
    return m


def fingerprint(members):
    """(member-set hash, anchors): anchors are the first members by crc32, a
    stable sample, so a share of anchors estimates a share of members."""
    keys = sorted({_member_key(str(m)) for m in members})
    digest = hashlib.sha1("\n".join(keys).encode()).hexdigest()[:20]
    anchors = sorted(keys, key=lambda k: zlib.crc32(k.encode()))[:ANCHORS]
    return digest, anchors, len(keys)


def snapshot(r, collection, algo_ns, meta, members):
    """Remember a cluster that is about to be deleted."""
    c_uuid = (meta or {}).get("cluster_uuid")
    if not c_uuid:
        return
    digest, anchors, size = fingerprint(members)
    rec = {
        "uuid": c_uuid,
        "cluster_id": meta.get("cluster_id"),
        "name": meta.get("cluster_name"),
        "custom_name": bool(meta.get("is_custom_name")),
        "size": size,
        "hash": digest,
        "anchors": anchors,
        "at": int(time.time() * 1000),
    }
    r.hset(_key(collection, "retired", algo_ns), c_uuid, json.dumps(rec))


def _loads(raw):
    if raw is None:
        return None
    return json.loads(raw.decode() if isinstance(raw, bytes) else raw)


def _decode(v):
    return v.decode() if isinstance(v, bytes) else v


def user_data(r, collection, algo_ns, c_uuid):
    field = f"bin_cluster:{algo_ns}:{c_uuid}"
    tags = _loads(r.hget(f"{collection}:cluster_tags", field)) or []
    notes = _loads(r.hget(f"{collection}:cluster_notes", field)) or []
    return {"tags": tags, "notes": notes}


def _successors(rec, anchor_hits, sizes):
    anchors = rec.get("anchors") or []
    out = []
    for label, hits in anchor_hits.items():
        share = hits / max(1, len(anchors))
        inter = share * rec["size"]
        jac = inter / max(1.0, rec["size"] + sizes[label] - inter)
        out.append((jac, share, label))
    out.sort(key=lambda t: (-t[0], sizes[t[2]]))
    return out[:SUCCESSORS]


def _kind(rec, best, sizes):
    if not best:
        return "dissolved"
    jac, share, label = best
    if jac >= 0.9:
        return "reshaped"
    if share >= 0.8 and sizes[label] > rec["size"]:
        return "merged"
    return "split"


def carry_uuids(r, collection, algo_ns, cluster_members, label_to_uuid, log=None):
    """Fill `label_to_uuid` for labels without a uuid: an old uuid when the
    member set is unchanged, else a fresh one. Then turn every unclaimed
    snapshot into a history record. Returns the number of records written."""
    import uuid as uuidlib

    retired_key = _key(collection, "retired", algo_ns)
    retired = {_decode(k): _loads(v) for k, v in (r.hgetall(retired_key) or {}).items()}
    by_hash = {}
    for rec in retired.values():
        by_hash.setdefault(rec["hash"], []).append(rec)
    taken = {u for u in label_to_uuid.values() if u}

    # Labels in id order, so parent/child with identical sets pair up stably.
    fresh = sorted(c for c in cluster_members if not label_to_uuid.get(c))
    for c in fresh:
        recs = by_hash.get(fingerprint(cluster_members[c])[0]) or []
        while recs and recs[0]["uuid"] in taken:
            recs.pop(0)
        if recs:
            label_to_uuid[c] = recs.pop(0)["uuid"]
        else:
            label_to_uuid[c] = uuidlib.uuid4().hex[:12]
        taken.add(label_to_uuid[c])

    dead = [rec for u, rec in retired.items() if u not in taken]
    hist_key = _key(collection, "uuid_history", algo_ns)
    inherit_key = _key(collection, "inherit", algo_ns)
    if taken:
        r.hdel(hist_key, *taken)  # back in use: never "dead" and "live" at once
    if dead:
        wanted = {a for rec in dead for a in rec.get("anchors") or []}
        holders = {}  # anchor -> labels holding it
        for c, members in cluster_members.items():
            for m in members:
                k = _member_key(str(m))
                if k in wanted:
                    holders.setdefault(k, []).append(c)
        sizes = {c: len(m) for c, m in cluster_members.items()}
        n_data = 0
        for rec in dead:
            hits = {}
            for a in rec.get("anchors") or []:
                for c in holders.get(a, ()):
                    hits[c] = hits.get(c, 0) + 1
            ranked = _successors(rec, hits, sizes)
            data = user_data(r, collection, algo_ns, rec["uuid"])
            has_data = bool(data["tags"] or data["notes"] or rec.get("custom_name"))
            hist = {
                k: rec.get(k) for k in ("uuid", "cluster_id", "name", "custom_name")
            }
            hist.update(
                size=rec["size"],
                retired_at=rec.get("at"),
                at=int(time.time() * 1000),
                kind=_kind(rec, ranked[0] if ranked else None, sizes),
                has_data=has_data,
                successors=[
                    {
                        "uuid": label_to_uuid[c],
                        "cluster_id": c,
                        "size": sizes[c],
                        "share": round(share, 3),
                        "jaccard": round(jac, 3),
                    }
                    for jac, share, c in ranked
                ],
            )
            r.hset(hist_key, rec["uuid"], json.dumps(hist))
            # Pending proposals follow the cluster they were waiting on.
            pending = _loads(r.hget(inherit_key, rec["uuid"])) or []
            if pending:
                r.hdel(inherit_key, rec["uuid"])
            if has_data:
                pending.append(rec["uuid"])
            if pending and ranked:
                to = label_to_uuid[ranked[0][2]]
                cur = _loads(r.hget(inherit_key, to)) or []
                r.hset(
                    inherit_key,
                    to,
                    json.dumps(cur + [u for u in pending if u not in cur]),
                )
            n_data += has_data
        if log:
            log(
                f"Cluster uuids: {len(taken & set(retired))} kept, {len(dead)} retired "
                f"({n_data} with analyst tags/notes/names, proposed on their successor)."
            )
    r.delete(retired_key)
    return len(dead)


def find(r, collection, c_uuid, algo_for):
    """History of `c_uuid` in any axis / node type namespace.
    `algo_for(axis, node_type)` gives the namespace. Follows dead successors
    until a live one, so an old link always lands somewhere current."""
    for axis in AXES:
        for node_type in ("file", "container"):
            algo_ns = algo_for(axis, node_type)
            rec = _loads(r.hget(_key(collection, "uuid_history", algo_ns), c_uuid))
            if rec:
                return _follow(r, collection, algo_ns, rec) | {
                    "axis": axis,
                    "node_type": node_type,
                    "algo": algo_ns,
                }
    return None


def _follow(r, collection, algo_ns, rec, depth=0):
    hist_key = _key(collection, "uuid_history", algo_ns)
    live = []
    for s in rec.get("successors") or []:
        nxt = _loads(r.hget(hist_key, s["uuid"]))
        if not nxt:
            live.append(s)
        elif depth < 8 and nxt.get("successors"):
            # ponytail: share multiplies down the chain only approximately
            live.extend(_follow(r, collection, algo_ns, nxt, depth + 1)["live"][:1])
    seen, uniq = set(), []
    for s in live:
        if s["uuid"] not in seen:
            seen.add(s["uuid"])
            uniq.append(s)
    return {**rec, "live": uniq}


def inherited(r, collection, algo_ns, c_uuid):
    """Old uuids with analyst data whose best successor is `c_uuid`."""
    hist_key = _key(collection, "uuid_history", algo_ns)
    out = []
    for old in _loads(r.hget(_key(collection, "inherit", algo_ns), c_uuid)) or []:
        rec = _loads(r.hget(hist_key, old)) or {"uuid": old}
        out.append({**rec, **user_data(r, collection, algo_ns, old)})
    return out


def carry_data(r, collection, algo_ns, src, dst, tags=True, notes=True):
    """Copy tags / notes of uuid `src` onto `dst` (kept on `src` too) and
    drop the pending proposal. Notes keep their id, so a second copy is a no-op."""
    data = user_data(r, collection, algo_ns, src)
    mine = user_data(r, collection, algo_ns, dst)
    f_dst = f"bin_cluster:{algo_ns}:{dst}"
    added = {"tags": 0, "notes": 0}
    if tags:
        new = [t for t in data["tags"] if t not in mine["tags"]]
        if new:
            r.hset(f"{collection}:cluster_tags", f_dst, json.dumps(mine["tags"] + new))
        added["tags"] = len(new)
    if notes:
        ids = {n.get("id") for n in mine["notes"]}
        new = [{**n, "from_uuid": src} for n in data["notes"] if n.get("id") not in ids]
        if new:
            r.hset(
                f"{collection}:cluster_notes", f_dst, json.dumps(mine["notes"] + new)
            )
        added["notes"] = len(new)
    dismiss(r, collection, algo_ns, src, dst)
    return added


def dismiss(r, collection, algo_ns, src, dst):
    inherit_key = _key(collection, "inherit", algo_ns)
    pending = [u for u in _loads(r.hget(inherit_key, dst)) or [] if u != src]
    if pending:
        r.hset(inherit_key, dst, json.dumps(pending))
    else:
        r.hdel(inherit_key, dst)


if __name__ == "__main__":

    class R(dict):
        def hset(self, k, f, v):
            self.setdefault(k, {})[f] = v

        def hget(self, k, f):
            return self.get(k, {}).get(f)

        def hdel(self, k, *fs):
            for f in fs:
                self.get(k, {}).pop(f, None)

        def hgetall(self, k):
            return dict(self.get(k, {}))

        def delete(self, k):
            self.pop(k, None)

    r, C, A = R(), "c", "algo:snn"
    files = lambda *ns: [f"c:file:{n:032x}" for n in ns]
    old = {  # uuid -> members
        "keep": files(*range(0, 40)),
        "merge": files(*range(40, 50)),
        "split": files(*range(50, 90)),
        "gone": files(900, 901),
    }
    for u, m in old.items():
        snapshot(r, C, A, {"cluster_uuid": u, "cluster_id": 1}, m)
    r.hset(f"{C}:cluster_tags", f"bin_cluster:{A}:merge", json.dumps(["bookmark"]))
    r.hset(
        f"{C}:cluster_notes",
        f"bin_cluster:{A}:split",
        json.dumps([{"id": "n1", "text": "hi"}]),
    )
    new = {
        10: files(*range(0, 40)),  # unchanged -> keeps "keep"
        11: files(*range(40, 50), *range(200, 260)),  # absorbs "merge"
        12: files(*range(50, 70)),  # half of "split"
        13: files(*range(70, 90)),
    }
    l2u = {}
    assert carry_uuids(r, C, A, new, l2u) == 3
    assert l2u[10] == "keep" and len({*l2u.values()}) == 4
    hist = {k: json.loads(v) for k, v in r.hgetall(_key(C, "uuid_history", A)).items()}
    assert hist["merge"]["kind"] == "merged", hist["merge"]
    assert hist["merge"]["successors"][0]["uuid"] == l2u[11]
    assert hist["split"]["kind"] == "split", hist["split"]
    assert {s["uuid"] for s in hist["split"]["successors"]} == {l2u[12], l2u[13]}
    assert hist["gone"]["kind"] == "dissolved" and not hist["gone"]["successors"]
    assert "keep" not in hist and not r.hgetall(_key(C, "retired", A))
    # data proposals sit on the best successor, then follow it when it dies
    assert inherited(r, C, A, l2u[11])[0]["tags"] == ["bookmark"]
    snapshot(r, C, A, {"cluster_uuid": l2u[11]}, new[11])
    l2u2 = {}
    carry_uuids(r, C, A, {20: new[11] + files(300)}, l2u2)
    assert [x["uuid"] for x in inherited(r, C, A, l2u2[20])] == ["merge"]
    found = find(r, C, "merge", lambda axis, nt: A)
    assert [s["uuid"] for s in found["live"]] == [l2u2[20]], found
    assert carry_data(r, C, A, "merge", l2u2[20]) == {"tags": 1, "notes": 0}
    assert user_data(r, C, A, l2u2[20])["tags"] == ["bookmark"]
    assert user_data(r, C, A, "merge")["tags"] == ["bookmark"]  # never moved away
    assert inherited(r, C, A, l2u2[20]) == []
    print("cluster_history self-check: OK")
