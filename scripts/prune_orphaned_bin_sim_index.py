#!/usr/bin/env python3
"""One-off cleanup: purge idx:bin_sim:*/reg:bin_sim:* entries left orphaned by
past clear_bin_sim runs that deleted the wrong key names (see
app/services/bin_sim_service.py clear_bin_sim, no-md5 branch).

Only removes index members whose backing `{collection}:bin_sim:{algo}:a::b`
doc no longer exists. Leaves live pairs untouched. Dry-run by default.

Usage:
    python3 scripts/prune_orphaned_bin_sim_index.py --host HOST --port PORT \
        --collection COLLECTION [--algo unweighted_cosine] [--apply]
"""
import argparse
import sys

import redis

# Mirrors BIN_SIM_NUM_FIELDS / BIN_SIM_TAG_FIELDS in
# app/services/bin_sim_service.py. Inlined so this script has no dependency
# on the Flask app package (which pulls in flask/etc just to read two tuples).
BIN_SIM_NUM_FIELDS = (
    "score",
    "score_code",
    "score_library",
    "score_content",
    "coverage_a",
    "coverage_b",
    "shared_clusters",
    "computed_at",
    "functions_count_a",
    "functions_count_b",
)

BIN_SIM_TAG_FIELDS = (
    "md5_a",
    "md5_b",
    "algo",
    "file_name_a",
    "file_tags_a",
    "file_user_tags_a",
    "architecture_a",
    "file_name_b",
    "file_tags_b",
    "file_user_tags_b",
    "architecture_b",
)


def _dec(x):
    return x.decode() if isinstance(x, bytes) else x


def prune(r, collection, algo, apply_):
    removed_num = 0
    removed_tag = 0
    checked = 0
    exist_cache = {}

    def doc_exists(sid):
        if sid not in exist_cache:
            exist_cache[sid] = bool(r.exists(sid))
        return exist_cache[sid]

    for field in BIN_SIM_NUM_FIELDS:
        zkey = f"{collection}:idx:bin_sim:{field}"
        members = [_dec(s) for s in r.zrange(zkey, 0, -1)]
        stale = [
            s for s in members if s.startswith(f"{collection}:bin_sim:{algo}:") and not doc_exists(s)
        ]
        checked += len(members)
        if stale:
            print(f"{zkey}: {len(stale)}/{len(members)} stale")
            removed_num += len(stale)
            if apply_ and stale:
                pipe = r.pipeline(transaction=False)
                for s in stale:
                    pipe.zrem(zkey, s)
                pipe.execute()

    for field in BIN_SIM_TAG_FIELDS:
        reg_key = f"{collection}:reg:bin_sim:{field}"
        buckets = [_dec(b) for b in r.smembers(reg_key)]
        for bucket in buckets:
            members = [_dec(s) for s in r.smembers(bucket)]
            stale = [
                s
                for s in members
                if s.startswith(f"{collection}:bin_sim:{algo}:") and not doc_exists(s)
            ]
            if not stale:
                continue
            print(f"{bucket}: {len(stale)}/{len(members)} stale")
            removed_tag += len(stale)
            if apply_:
                pipe = r.pipeline(transaction=False)
                for s in stale:
                    pipe.srem(bucket, s)
                pipe.execute()
                if len(stale) == len(members):
                    r.delete(bucket)
                    r.srem(reg_key, bucket)

    all_key = f"{collection}:all_bin_sims"
    all_members = [_dec(s) for s in r.smembers(all_key)]
    stale_all = [
        s
        for s in all_members
        if s.startswith(f"{collection}:bin_sim:{algo}:") and not doc_exists(s)
    ]
    if stale_all:
        print(f"{all_key}: {len(stale_all)}/{len(all_members)} stale")
        if apply_:
            pipe = r.pipeline(transaction=False)
            for s in stale_all:
                pipe.srem(all_key, s)
            pipe.execute()

    print(
        f"\n{'APPLIED' if apply_ else 'DRY RUN'}: "
        f"{removed_num} stale numeric-index members, {removed_tag} stale bucket members "
        f"({checked} score-index members inspected)."
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=6666)
    ap.add_argument("--collection", required=True)
    ap.add_argument("--algo", default="unweighted_cosine")
    ap.add_argument("--apply", action="store_true", help="Actually delete (default: dry run)")
    args = ap.parse_args()

    r = redis.Redis(host=args.host, port=args.port, decode_responses=False)
    try:
        r.ping()
    except redis.exceptions.ConnectionError as e:
        print(f"Cannot reach {args.host}:{args.port}: {e}", file=sys.stderr)
        sys.exit(1)

    prune(r, args.collection, args.algo, args.apply)
