"""Containment lineage -- which file came out of which container.

Issue MISP/bsimvis#32 section 3. The relation lives in edge sets rather than in
fields on the file document, because the same child turns up in more than one
container (one `classes.dex`, two APKs) and a scalar field cannot hold that
without teaching the flat index layer about nested records. The edge also
carries the child's path inside its container, which the file name alone does
not give for every format.

The scalar fields stay exactly as they were -- `parent_md5` /
`parent_file_name` are what search, func/sim propagation and the bin_sim
denormalisation already read -- and `root_md5` joins them so "every function
under this upload" stays a single existing query rather than a graph walk.

Keys, per collection:

    {coll}:lineage:children:{md5}   SET   "{child_md5}|{path_in_parent}"
    {coll}:lineage:parents:{md5}    SET   "{parent_md5}|{path_in_parent}"
    {coll}:lineage:containers       SET   md5 of every file that is a container
    {coll}:lineage:funcs:{md5}      HASH  descendant md5 -> its function count
"""

import json
import logging

from bsimvis.app.services.index_service import save_file
from bsimvis.app.services.redis_client import get_redis

# ponytail: unpacking stops at unpack_service.MAX_DEPTH, so a real walk is two
# hops. This bound only stops a hand-declared parent cycle from spinning.
MAX_WALK = 16


def _txt(v):
    return v.decode() if isinstance(v, bytes) else str(v)


def _key(coll, kind, md5):
    return f"{coll}:lineage:{kind}:{md5}"


# ---------------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------------


def record(coll, parent_md5, child_md5, path="", r=None):
    """Record one containment edge. Idempotent; a self-edge is dropped.

    Called even when the child itself failed to ingest as a duplicate: the file
    was already known, but *this* container holding it is new information.
    """
    if not parent_md5 or not child_md5 or parent_md5 == child_md5:
        return
    r = r or get_redis()
    pipe = r.pipeline(transaction=False)
    pipe.sadd(_key(coll, "children", parent_md5), f"{child_md5}|{path}")
    pipe.sadd(_key(coll, "parents", child_md5), f"{parent_md5}|{path}")
    pipe.execute()


def _edges(key, r):
    out = []
    for m in r.smembers(key):
        md5, _, path = _txt(m).partition("|")
        if md5:
            out.append({"md5": md5, "path": path})
    return sorted(out, key=lambda e: (e["path"], e["md5"]))


def children(coll, md5, r=None):
    """Files extracted directly out of `md5`."""
    r = r or get_redis()
    return _edges(_key(coll, "children", md5), r)


def parents(coll, md5, r=None):
    """Containers `md5` was extracted from, one entry per container."""
    r = r or get_redis()
    return _edges(_key(coll, "parents", md5), r)


def _walk(coll, md5, kind, r):
    """Breadth-first walk in one direction, nearest first, cycle-safe."""
    r = r or get_redis()
    seen, frontier, out = {md5}, [md5], []
    for _ in range(MAX_WALK):
        nxt = []
        for m in frontier:
            for edge in _edges(_key(coll, kind, m), r):
                if edge["md5"] in seen:
                    continue
                seen.add(edge["md5"])
                out.append(edge)
                nxt.append(edge["md5"])
        if not nxt:
            break
        frontier = nxt
    return out


def ancestors(coll, md5, r=None):
    """Every container above `md5`, nearest first."""
    return _walk(coll, md5, "parents", r or get_redis())


def descendants(coll, md5, r=None):
    """Everything extracted out of `md5`, at any depth, nearest first."""
    return _walk(coll, md5, "children", r or get_redis())


def forget(coll, md5, r=None):
    """Drop every edge touching `md5`. For file deletion and backfill resets."""
    r = r or get_redis()
    former_parents = parents(coll, md5, r)
    pipe = r.pipeline(transaction=False)
    for edge in children(coll, md5, r):
        pipe.srem(_key(coll, "parents", edge["md5"]), f"{md5}|{edge['path']}")
    for edge in former_parents:
        pipe.srem(_key(coll, "children", edge["md5"]), f"{md5}|{edge['path']}")
        pipe.hdel(_key(coll, "funcs", edge["md5"]), md5)
    pipe.delete(_key(coll, "children", md5))
    pipe.delete(_key(coll, "parents", md5))
    pipe.delete(_key(coll, "funcs", md5))
    pipe.srem(f"{coll}:lineage:containers", md5)
    pipe.execute()

    # The containers above it were just told to forget this file's functions;
    # their stated count is wrong until it is summed again.
    for edge in former_parents:
        if is_container(coll, edge["md5"], r):
            _restate_container_count(coll, edge["md5"], r)


# ---------------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------------


def mark_container(coll, md5, r=None):
    """Flag `md5` as a container: it holds code but is not code itself."""
    (r or get_redis()).sadd(f"{coll}:lineage:containers", md5)


def is_container(coll, md5, r=None):
    return bool((r or get_redis()).sismember(f"{coll}:lineage:containers", md5))


def container_md5s(coll, r=None):
    """Every container md5 in the collection, as a set of str."""
    return {_txt(m) for m in (r or get_redis()).smembers(f"{coll}:lineage:containers")}


# ---------------------------------------------------------------------------
# Rolled-up function counts
# ---------------------------------------------------------------------------


def subtree_function_count(coll, md5, r=None):
    r = r or get_redis()
    total = 0
    for v in r.hvals(_key(coll, "funcs", md5)):
        try:
            total += int(_txt(v))
        except ValueError:
            continue
    return total


def record_function_count(coll, md5, count, r=None):
    """Roll this file's function count up into every container above it.

    Stored per contributing descendant rather than incremented, so re-indexing
    the same file cannot double-count it. Only containers are restated: a
    packed executable is a real binary whose own `function_count` must keep
    meaning "functions in this file", or sorting and collection totals lie.
    """
    r = r or get_redis()
    for edge in ancestors(coll, md5, r):
        if not is_container(coll, edge["md5"], r):
            continue
        r.hset(_key(coll, "funcs", edge["md5"]), md5, int(count or 0))
        _restate_container_count(coll, edge["md5"], r)


def _restate_container_count(coll, md5, r):
    """Write the summed subtree count onto the container's own document.

    ponytail: read-sum-write, so two children of one container finishing at the
    same instant can leave the stated total one child behind. The per-child
    hash stays correct either way, so the next child to land -- or a backfill
    run -- restates it. Make this a Lua sum-and-set if that ever shows.
    """
    meta_key = f"{coll}:file:{md5}:meta"
    raw = r.get(meta_key)
    if not raw:
        # A declared parent we were never given the bytes of has no document;
        # the edges still stand, there is just nothing to restate.
        return
    try:
        meta = json.loads(raw)
    except (ValueError, TypeError):
        logging.warning(f"[-] lineage: unreadable meta for container {md5}")
        return
    meta["function_count"] = subtree_function_count(coll, md5, r)
    pipe = r.pipeline(transaction=False)
    pipe.set(meta_key, json.dumps(meta))
    save_file(pipe, coll, md5, meta)
    pipe.execute()


def demo():
    """Self-check against a live Kvrocks, in a throwaway collection."""
    coll = "lineage_demo"
    r = get_redis()
    apk, dex, so, nested = "a" * 32, "d" * 32, "s" * 32, "n" * 32

    for m in (apk, dex, so, nested):
        forget(coll, m, r)

    record(coll, apk, dex, "classes.dex", r)
    record(coll, apk, so, "lib/arm64-v8a/libfoo.so", r)
    record(coll, so, nested, "libfoo.so.unpacked", r)
    record(coll, apk, apk, "self", r)  # self-edge, must be ignored

    assert [e["md5"] for e in children(coll, apk, r)] == [dex, so], children(coll, apk, r)
    assert children(coll, apk, r)[0]["path"] == "classes.dex"
    assert [e["md5"] for e in parents(coll, nested, r)] == [so]
    assert {e["md5"] for e in descendants(coll, apk, r)} == {dex, so, nested}
    assert [e["md5"] for e in ancestors(coll, nested, r)] == [so, apk]

    # A second container holding the same child: multi-parent must survive.
    other = "b" * 32
    record(coll, other, dex, "classes.dex", r)
    assert {e["md5"] for e in parents(coll, dex, r)} == {apk, other}

    # Counts roll up only into containers, and re-recording does not double.
    mark_container(coll, apk, r)
    record_function_count(coll, dex, 10, r)
    record_function_count(coll, nested, 5, r)
    record_function_count(coll, nested, 5, r)
    assert subtree_function_count(coll, apk, r) == 15, subtree_function_count(coll, apk, r)
    assert subtree_function_count(coll, so, r) == 0  # not a container, not restated

    for m in (apk, other, dex, so, nested):
        forget(coll, m, r)
    assert children(coll, apk, r) == []
    print("lineage_service demo OK")


if __name__ == "__main__":
    demo()
