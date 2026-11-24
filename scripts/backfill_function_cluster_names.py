"""Rename stored function clusters by the N-way naming rule, with no re-clustering.

Analyst name > most common non-default name (never FUN_/sub_/thunk_) > a name
carried by the members' tags > `Cluster <id>`. Only `cluster_name` in each
cluster meta and the `idx:func:cluster_name:*` buckets change.

Preview: ``uv run python scripts/backfill_function_cluster_names.py --collection more_mirai_yara``
Apply:   ``uv run python scripts/backfill_function_cluster_names.py --collection more_mirai_yara --apply``
"""

import argparse
import json
import logging
from collections import Counter

from bsimvis.app.services.cluster_utils import default_func_cluster_name
from bsimvis.app.services.collection_config import resolve_collection_algo
from bsimvis.app.services.nway_diff_service import _DEFAULT_NAME
from bsimvis.app.services.redis_client import get_redis

CHUNK = 1000
KEEP = (
    "function_name",
    "user_name",
    "user_name_source",
    "tags",
    "user_tags",
    "bsim_features_count",
    "entrypoint_address",
)


def txt(v):
    return v.decode() if isinstance(v, bytes) else v


def load_metas(r, fids):
    out = {}
    for i in range(0, len(fids), CHUNK):
        chunk = fids[i : i + CHUNK]
        pipe = r.pipeline(transaction=False)
        for f in chunk:
            pipe.get(f"{f}:meta")
        for f, raw in zip(chunk, pipe.execute()):
            try:
                m = json.loads(raw) if raw else {}
            except ValueError:
                m = {}
            out[f] = {k: m[k] for k in KEEP if k in m}
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--collection", required=True)
    ap.add_argument("--algo", help="default: the collection's similarity algo")
    ap.add_argument("--apply", action="store_true", help="write (default: preview)")
    ap.add_argument("--show", type=int, default=15, help="renames to print")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    r, coll = get_redis(), a.collection
    algo = a.algo or resolve_collection_algo(coll)
    cids = sorted(txt(c) for c in r.smembers(f"{coll}:cluster:list:{algo}"))
    logging.info("%s: %d function clusters (%s)", coll, len(cids), algo)

    pipe = r.pipeline(transaction=False)
    for c in cids:
        pipe.get(f"{coll}:cluster:{algo}:{c}:meta")
        pipe.smembers(f"{coll}:cluster:{algo}:{c}:members")
    raw = iter(pipe.execute())
    clusters = []
    for c in cids:
        meta, members = next(raw), next(raw)
        if meta:
            clusters.append((c, json.loads(meta), sorted(txt(m) for m in members)))

    metas = load_metas(r, sorted({f for _, _, ms in clusters for f in ms}))
    final, changed = {}, []
    for c, meta, members in clusters:
        new = default_func_cluster_name(members, metas, f"Cluster {c}")
        old = meta.get("cluster_name") or ""
        # two Ghidra defaults are a tie, not an improvement
        if _DEFAULT_NAME.match(new) and _DEFAULT_NAME.match(old):
            new = old
        elif old and new != old:
            # likewise a tie between two real names: keep the stored one
            count = Counter(metas[f].get("function_name") for f in members)
            if count[old] and count[old] == count[new]:
                new = old
        final[c] = (new, members)
        if new != old:
            changed.append((c, meta, new))

    logging.info("%d of %d clusters change name", len(changed), len(clusters))
    for c, meta, new in changed[: a.show]:
        logging.info("  %s: %r -> %r", c, meta.get("cluster_name"), new)
    if not a.apply:
        logging.info("preview only; pass --apply to write")
        return

    # A bucket holds the members of every cluster with that name: rebuild each
    # touched bucket from the final names instead of removing members one by one.
    touched = {m.get("cluster_name", "").lower() for _, m, _ in changed}
    touched |= {n.lower() for _, _, n in changed}
    touched.discard("")
    pipe = r.pipeline(transaction=False)
    for c, meta, new in changed:
        meta["cluster_name"] = new
        pipe.set(f"{coll}:cluster:{algo}:{c}:meta", json.dumps(meta))
    pipe.execute()

    reg = f"{coll}:reg:func:cluster_name"
    if not r.exists(reg):
        logging.info("no cluster_name index in this collection; metas updated")
        return
    buckets = {t: set() for t in touched}
    for new, members in final.values():
        if new.lower() in buckets:
            buckets[new.lower()].update(members)
    pipe = r.pipeline(transaction=False)
    for name, members in buckets.items():
        key = f"{coll}:idx:func:cluster_name:{name}"
        pipe.delete(key)
        if members:
            pipe.sadd(key, *members)
            pipe.sadd(reg, key)
        else:
            pipe.srem(reg, key)
    pipe.execute()
    logging.info(
        "renamed %d clusters, rebuilt %d index buckets", len(changed), len(buckets)
    )


if __name__ == "__main__":
    main()
