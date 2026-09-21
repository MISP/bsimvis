"""Copy a collection between two Kvrocks instances.

Everything about a collection lives under the flat `{collection}:*` key prefix,
so a transfer is a prefix scan plus a prefix rewrite. Only three things sit
outside it and are handled separately: `global:collections`,
`global:collection:{name}:meta` and the per-batch `global:batch:{uuid}` blobs,
which are shared between collections and must be merged rather than replaced.

This talks to Kvrocks directly, not to the API -- there is nothing an HTTP layer
would add here except a second copy of the data in flight.
"""

import fnmatch
import json
import os
import time

import redis

# Keys read/written in one round trip. Kvrocks pipelines these fine; the cost of
# a transfer is round trips, not bytes.
BATCH = 1000
# A set/hash/zset larger than this is streamed with SCAN instead of being read
# whole -- `{coll}:sim:all` holds one member per similarity pair.
BIG = 10000
# Members written per command for a big key.
CHUNK = 5000

# Derived data: similarities, clusters, their LSH buckets and the indexes and
# registries that only exist to serve them. Dropped by --skip-sim, rebuilt on
# the target with `bsimvis sim build`.
DERIVED = [
    "sim:*",
    "all_bin_sims",
    "bin_sim:*",
    "cluster:*",
    "bin_cluster:*",
    "lsh:*",
    "built:functions:*",
    "idx:sim:*",
    "reg:sim:*",
    "idx:bin_sim:*",
    "reg:bin_sim:*",
    "idx:func:cluster_*",
    "reg:func:cluster_*",
    "idx:file:bin_cluster_*",
    "reg:file:bin_cluster_*",
    "*:clusters",
    "*:cluster_scores",
]


def parse_endpoint(spec, default_host, default_port):
    """`collection` or `host:port:collection` -> (host, port, collection)."""
    parts = spec.split(":")
    if len(parts) == 1:
        return default_host, default_port, parts[0]
    if len(parts) == 3:
        return parts[0], int(parts[1]), parts[2]
    raise ValueError(f"expected COLLECTION or HOST:PORT:COLLECTION, got '{spec}'")


def is_derived(key, collection):
    """True if key is recomputable similarity/cluster data."""
    suffix = key[len(collection) + 1 :]
    return any(fnmatch.fnmatch(suffix, g) for g in DERIVED)


class Rename:
    """Rewrites `{src}:` references to `{dst}:` when the name changes.

    Index members, call-graph edges, cluster ids and similarity back-references
    are all stored as *fully-qualified keys* (`{coll}:func:{md5}:{addr}`), and
    some live inside JSON blobs. So renaming a collection is not a key-prefix
    swap: the values carry the old name too, and a copy that leaves them alone
    points the new collection's indexes back at the old one.

    Rewriting is deliberately anchored rather than a substring replace -- a
    collection named `bash` or `http` would otherwise corrupt every string
    feature that happens to contain its name.
    """

    # JSON fields holding a display label built from an id, not an id itself.
    LABEL_FIELDS = ("cluster_name",)

    def __init__(self, src, dst):
        self.noop = src == dst
        self.src_s, self.dst_s = src, dst
        self.src = src.encode()
        self.dst = dst.encode()
        self.src_p = self.src + b":"
        self.dst_p = self.dst + b":"

    def key(self, suffix):
        """A cluster id is itself a node id, so it appears mid-key:
        `idx:func:cluster_id:{coll}:func:{md5}:{addr}`."""
        if self.noop:
            return suffix
        return suffix.replace(self.src_p, self.dst_p)

    def value(self, v):
        if self.noop or not isinstance(v, bytes) or self.src not in v:
            return v
        if v == self.src:
            return self.dst
        # A value that opens with the collection name is a key reference, and a
        # key reference is structural all the way through -- registry sets hold
        # whole index key names with a cluster id embedded in them. Rewriting
        # the whole string keeps them pointing at the keys `key()` produced.
        if v.startswith(self.src_p):
            return v.replace(self.src_p, self.dst_p)
        if v[:1] in (b"{", b"["):
            try:
                return json.dumps(self._json(json.loads(v))).encode()
            except (ValueError, UnicodeDecodeError):
                return v
        return v

    def _json(self, node, field=None):
        if isinstance(node, dict):
            return {k: self._json(v, k) for k, v in node.items()}
        if isinstance(node, list):
            return [self._json(v, field) for v in node]
        if isinstance(node, str):
            if field in self.LABEL_FIELDS:
                return node.replace(self.src_s + ":", self.dst_s + ":")
            if node == self.src_s:
                return self.dst_s
            if node.startswith(self.src_s + ":"):
                return self.dst_s + node[len(self.src_s) :]
        return node


def connect(host, port):
    # decode_responses=False: `{coll}:file:{md5}:raw` holds the sample bytes.
    return redis.Redis(
        host=host,
        port=port,
        decode_responses=False,
        socket_timeout=float(os.getenv("KVROCKS_SOCKET_TIMEOUT", 30)),
        socket_connect_timeout=5,
        socket_keepalive=True,
    )


def _read_small(src, keys, types):
    """Bulk-read keys in one round trip. Returns [(key, type, value)]."""
    pipe = src.pipeline(transaction=False)
    for key, t in zip(keys, types):
        if t == b"string":
            pipe.get(key)
        elif t == b"hash":
            pipe.hgetall(key)
        elif t == b"set":
            pipe.smembers(key)
        elif t == b"zset":
            pipe.zrange(key, 0, -1, withscores=True)
        elif t == b"list":
            pipe.lrange(key, 0, -1)
    return list(zip(keys, types, pipe.execute()))


def _write(pipe, key, t, value, rn):
    if value is None:
        return
    # The sample bytes are the one value that is not a reference to anything.
    tr = (lambda v: v) if key.endswith(b":raw") else rn.value
    if t == b"string":
        pipe.set(key, tr(value))
    elif t == b"hash":
        pipe.hset(key, mapping={tr(f): tr(v) for f, v in value.items()})
    elif t == b"set":
        members = [tr(m) for m in value]
        for i in range(0, len(members), CHUNK):
            pipe.sadd(key, *members[i : i + CHUNK])
    elif t == b"zset":
        items = list({tr(m): s for m, s in value}.items())
        for i in range(0, len(items), CHUNK):
            pipe.zadd(key, dict(items[i : i + CHUNK]))
    elif t == b"list":
        items = [tr(m) for m in value]
        for i in range(0, len(items), CHUNK):
            pipe.rpush(key, *items[i : i + CHUNK])


def _copy_big(src, dst, key, new_key, t, rn):
    """Stream one oversized key so neither side holds it whole."""
    buf = []
    if t == b"hash":
        it, write = src.hscan_iter(key, count=CHUNK), dst.hset
    elif t == b"set":
        it, write = src.sscan_iter(key, count=CHUNK), dst.sadd
    elif t == b"zset":
        it, write = src.zscan_iter(key, count=CHUNK), dst.zadd
    else:  # list
        n = src.llen(key)
        for i in range(0, n, CHUNK):
            chunk = [rn.value(m) for m in src.lrange(key, i, i + CHUNK - 1)]
            if chunk:
                dst.rpush(new_key, *chunk)
        return
    for item in it:
        buf.append(item)
        if len(buf) >= CHUNK:
            _flush_big(write, new_key, t, buf, rn)
            buf = []
    if buf:
        _flush_big(write, new_key, t, buf, rn)


def _flush_big(write, key, t, buf, rn):
    if t == b"set":
        write(key, *[rn.value(m) for m in buf])
    elif t == b"zset":
        write(key, {rn.value(m): s for m, s in buf})
    else:  # hash
        write(key, mapping={rn.value(f): rn.value(v) for f, v in buf})


def _card(pipe, key, t):
    if t == b"hash":
        pipe.hlen(key)
    elif t == b"set":
        pipe.scard(key)
    elif t == b"zset":
        pipe.zcard(key)
    elif t == b"list":
        pipe.llen(key)


def copy_keys(src, dst, src_coll, dst_coll, skip_sim, dry_run, log):
    """Scan `{src_coll}:*` and rewrite it under `{dst_coll}:` on dst."""
    src_prefix = f"{src_coll}:".encode()
    dst_prefix = f"{dst_coll}:".encode()
    rn = Rename(src_coll, dst_coll)
    copied = skipped = 0
    started = time.time()
    batch = []

    def flush(batch):
        nonlocal copied
        pipe = src.pipeline(transaction=False)
        for key in batch:
            pipe.type(key)
        kinds = dict(zip(batch, pipe.execute()))

        # Only a handful of keys per collection are huge, but reading one whole
        # would hold every similarity pair in memory at once.
        pipe = src.pipeline(transaction=False)
        sized = [(k, t) for k, t in kinds.items() if t not in (b"string", b"none")]
        for key, t in sized:
            _card(pipe, key, t)
        big = {k for (k, _), n in zip(sized, pipe.execute()) if n and n > BIG}

        small = [(k, t) for k, t in kinds.items() if k not in big and t != b"none"]
        if small:
            pipe = dst.pipeline(transaction=False)
            for key, t, value in _read_small(
                src, [k for k, _ in small], [t for _, t in small]
            ):
                _write(pipe, dst_prefix + rn.key(key[len(src_prefix) :]), t, value, rn)
            pipe.execute()
        for key in big:
            new_key = dst_prefix + rn.key(key[len(src_prefix) :])
            _copy_big(src, dst, key, new_key, kinds[key], rn)
        copied += len(small) + len(big)

    for key in src.scan_iter(match=src_prefix + b"*", count=BATCH):
        if skip_sim and is_derived(key.decode(errors="replace"), src_coll):
            skipped += 1
            continue
        if dry_run:
            copied += 1
            continue
        batch.append(key)
        if len(batch) >= BATCH:
            flush(batch)
            batch = []
            log(f"{copied} keys ({copied / max(time.time() - started, 1):.0f}/s)")
    if batch:
        flush(batch)
    return copied, skipped


def copy_globals(src, dst, src_coll, dst_coll, dry_run, log):
    """Collection registry, collection meta, and the shared batch blobs."""
    if dry_run:
        return
    meta = src.hgetall(f"global:collection:{src_coll}:meta".encode())
    if meta:
        dst.hset(f"global:collection:{dst_coll}:meta".encode(), mapping=meta)
    dst.sadd(b"global:collections", dst_coll.encode())

    uuids = src.smembers(f"{src_coll}:all_batches".encode())
    for raw in uuids:
        uuid = raw.decode()
        key = f"global:batch:{uuid}".encode()
        existing = dst.get(key)
        if existing:
            # The target already knows this batch, possibly through another
            # collection (or through the source, on a same-instance copy).
            # Add ourselves to its collections dict, never overwrite it --
            # delete_collection reads that dict to decide whether a batch is
            # still in use.
            blob = json.loads(existing)
        else:
            src_blob = src.get(key)
            if not src_blob:
                continue
            # A fresh blob on the target: the source's collections dict names
            # collections that live on the *source* instance, so it starts empty
            # here rather than carrying entries that do not exist.
            blob = json.loads(src_blob)
            blob["collections"] = {}
        blob.setdefault("collections", {})
        blob["collections"][dst_coll] = True
        dst.set(key, json.dumps(blob))
        dst.sadd(b"global:batches", raw)
    log(f"{len(uuids)} batch registrations merged")


def run_transfer(args):
    default_host = os.getenv("KVROCKS_HOST", "localhost")
    default_port = int(os.getenv("KVROCKS_PORT", 6666))
    try:
        s_host, s_port, s_coll = parse_endpoint(args.source, default_host, default_port)
        d_host, d_port, d_coll = parse_endpoint(args.dest, default_host, default_port)
    except ValueError as e:
        print(f"[!] {e}")
        return 1

    if (s_host, s_port, s_coll) == (d_host, d_port, d_coll):
        print("[!] Source and destination are the same collection.")
        return 1

    def log(msg):
        print(f"[*] {msg}", flush=True)

    src, dst = connect(s_host, s_port), connect(d_host, d_port)
    try:
        src.ping()
    except Exception as e:
        print(f"[!] Source {s_host}:{s_port} unreachable: {e}")
        return 1
    try:
        dst.ping()
    except Exception as e:
        print(f"[!] Destination {d_host}:{d_port} unreachable: {e}")
        return 1

    if not src.sismember(b"global:collections", s_coll.encode()):
        print(f"[!] Collection '{s_coll}' does not exist on {s_host}:{s_port}.")
        return 1
    if dst.sismember(b"global:collections", d_coll.encode()) and not args.force:
        print(
            f"[!] Collection '{d_coll}' already exists on {d_host}:{d_port}. "
            "Use --force to merge into it."
        )
        return 1

    log(
        f"{s_host}:{s_port}:{s_coll} -> {d_host}:{d_port}:{d_coll}"
        + (" (dry run)" if args.dry_run else "")
    )
    if args.skip_sim:
        log("Skipping similarities and clusters (rebuild with `bsimvis sim build`)")

    started = time.time()
    copied, skipped = copy_keys(
        src, dst, s_coll, d_coll, args.skip_sim, args.dry_run, log
    )
    copy_globals(src, dst, s_coll, d_coll, args.dry_run, log)

    verb = "Would copy" if args.dry_run else "Copied"
    print(f"[+] {verb} {copied} keys in {time.time() - started:.0f}s", end="")
    print(f", skipped {skipped} derived keys" if skipped else "")
    if not args.dry_run:
        print("[*] Milvus vectors are not transferred; run `sync_milvus` if enabled.")
    return 0


def _self_check():
    """Key rewriting and filtering, the two places a mistake corrupts a target."""
    assert parse_endpoint("mycoll", "h", 1) == ("h", 1, "mycoll")
    assert parse_endpoint("kv.lan:6666:mycoll", "h", 1) == ("kv.lan", 6666, "mycoll")
    try:
        parse_endpoint("kv.lan:mycoll", "h", 1)
        raise AssertionError("two-part endpoint should be rejected")
    except ValueError:
        pass

    derived = [
        "c:sim:unweighted_cosine:a::b",
        "c:sim:all",
        "c:bin_sim:involves:abc",
        "c:cluster:hdbscan:3:members",
        "c:bin_cluster:list:hdbscan",
        "c:lsh:bucket:0:deadbeef",
        "c:built:functions:unweighted_cosine",
        "c:idx:sim:score",
        "c:reg:sim:score",
        "c:idx:func:cluster_uuid:x",
        "c:idx:file:bin_cluster_name:y",
        "c:abc123:clusters",
        "c:abc123:cluster_scores",
        # A registry of bin-sim keys, useless once those keys are not copied.
        "c:all_bin_sims",
    ]
    kept = [
        "c:file:abc:raw",
        "c:file:abc:meta",
        "c:func:abc:1000",
        "c:feature:deadbeef:functions",
        "c:all_files",
        "c:all_functions",
        "c:batch:uuid:functions",
        "c:idx:file:file_name:a.exe",
        "c:reg:file:file_name",
        "c:idx:func:bsim_features_count",
        "c:indexed:functions",
        "c:tags_metadata",
        "c:funcid:deadbeef",
    ]
    for k in derived:
        assert is_derived(k, "c"), k
    for k in kept:
        assert not is_derived(k, "c"), k

    # A collection whose own name contains "sim" must not shadow the filters.
    assert not is_derived("simcoll:file:abc:raw", "simcoll")
    assert is_derived("simcoll:sim:all", "simcoll")

    rn = Rename("old", "new")
    # Fully-qualified references, the common case.
    assert rn.value(b"old:func:abc:1000") == b"new:func:abc:1000"
    assert rn.value(b"old") == b"new"
    # Cluster ids sit in the middle of a key, and registry sets hold those
    # whole key names as members -- both sides must land on the same string.
    assert rn.key(b"idx:func:cluster_id:old:func:abc:1000") == (
        b"idx:func:cluster_id:new:func:abc:1000"
    )
    assert rn.value(b"old:idx:func:cluster_id:old:func:abc:1000") == (
        b"new:idx:func:cluster_id:new:func:abc:1000"
    )
    assert rn.value(b"old:idx:file:bin_cluster_name:binary cluster old:file:abc") == (
        b"new:idx:file:bin_cluster_name:binary cluster new:file:abc"
    )
    # JSON: ids rewritten, labels rewritten, everything else untouched.
    blob = json.dumps(
        {
            "collection": "old",
            "batch_id": "old:batch:u1",
            "cluster_name": "Binary Cluster old:file:abc",
            "sample_members": [{"id": "old:file:abc", "name": "old times"}],
            "note": "mentions old but is not a reference",
            "count": 3,
        }
    ).encode()
    out = json.loads(rn.value(blob))
    assert out["collection"] == "new"
    assert out["batch_id"] == "new:batch:u1"
    assert out["cluster_name"] == "Binary Cluster new:file:abc"
    assert out["sample_members"][0]["id"] == "new:file:abc"
    assert out["sample_members"][0]["name"] == "old times"
    assert out["note"] == "mentions old but is not a reference"
    assert out["count"] == 3

    # Free text that merely contains the name is never touched: a collection
    # called "bash" must not corrupt every string feature holding "bash".
    bash = Rename("bash", "bash_copy")
    assert bash.value(b"/bin/bash: line 1") == b"/bin/bash: line 1"
    assert bash.value(b"run bash: now") == b"run bash: now"
    assert bash.value(b"bash:func:abc:1000") == b"bash_copy:func:abc:1000"

    # Same name on both ends: nothing is rewritten at all.
    same = Rename("c", "c")
    assert same.noop and same.value(b"c:func:abc") == b"c:func:abc"

    # Raw sample bytes must survive values that are not valid UTF-8.
    assert rn.value(b"\xb7\x00old\xff") == b"\xb7\x00old\xff"
    print("self-check OK")


if __name__ == "__main__":
    _self_check()
