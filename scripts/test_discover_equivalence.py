#!/usr/bin/env python3
"""Equivalence check for the vectorised _discover_find / _discover_minhash.

The discovery step was rewritten from a per-pair Python loop into a numpy
scatter-add (see doc/build-sim-discovery-perf.md). That is a pure performance
change: the candidate set and every score must be bit-identical to the old
implementation. This script keeps the original loop as a reference and asserts
both agree over randomised corpora.

Run:  uv run python scripts/test_discover_equivalence.py
No stack needed — it drives the service against an in-memory fake kvrocks.
"""

import math
import os
import random
import sys

# Import bsimvis from THIS checkout, not whatever an editable install points at
# (matters when running inside a git worktree).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- minimal in-memory stand-in for the kvrocks client -----------------------


class _Pipe:
    def __init__(self, store):
        self.store = store
        self.ops = []

    def zcard(self, key):
        self.ops.append(("zcard", key))
        return self

    def zrange(self, key, start, end, withscores=False):
        self.ops.append(("zrange", key, withscores))
        return self

    def zscore(self, key, member):
        self.ops.append(("zscore", key, member))
        return self

    def get(self, key):
        self.ops.append(("get", key))
        return self

    def execute(self):
        out = []
        for op in self.ops:
            if op[0] == "zcard":
                out.append(len(self.store.z.get(op[1], {})))
            elif op[0] == "zrange":
                items = list(self.store.z.get(op[1], {}).items())
                out.append(items if op[2] else [m for m, _ in items])
            elif op[0] == "zscore":
                out.append(self.store.z.get(op[1], {}).get(op[2]))
            elif op[0] == "get":
                out.append(self.store.kv.get(op[1]))
        self.ops = []
        return out


class FakeRedis:
    def __init__(self):
        self.z = {}
        self.kv = {}

    def pipeline(self, transaction=False):
        return _Pipe(self)

    def sunion(self, keys):
        out = set()
        for k in keys:
            out |= set(self.z.get(k, {}))
        return out


# --- reference implementation: the original per-pair loop, verbatim ----------


def reference_discover_find(store, args):
    target_id, collection, algo = args[0], args[1], args[2]
    threshold = float(args[3])
    target_total = float(args[4])
    target_norm = float(args[5])
    limit = int(args[6])
    min_features = float(args[7] or 0)

    target_features = {}
    for i in range(8, len(args), 2):
        target_features[args[i]] = float(args[i + 1])
    if not target_features:
        return []

    min_shared_norm_sq = 0.0
    if algo == "unweighted_cosine":
        min_shared_norm_sq = (threshold * target_norm) ** 2

    feats = list(target_features.items())
    features_sorted = sorted(
        (
            {
                "hash": h,
                "tf": tf,
                "key": f"{collection}:feature:{h}:functions",
                "size": len(store.z.get(f"{collection}:feature:{h}:functions", {})),
            }
            for h, tf in feats
        ),
        key=lambda x: x["size"],
    )

    intersection_counts = {}
    shared_target_norm_sq = {}
    target_norm_sq = target_norm * target_norm
    processed_norm_sq = 0.0
    processed_total = 0.0
    num_candidates = 0

    for feat in features_sorted:
        remaining_norm_sq = target_norm_sq - processed_norm_sq
        remaining_total = target_total - processed_total
        can_add_new = True
        if algo == "unweighted_cosine":
            if remaining_norm_sq < min_shared_norm_sq:
                can_add_new = False
        elif algo == "jaccard":
            if remaining_total < threshold * target_total:
                can_add_new = False
        if not can_add_new and num_candidates == 0:
            break

        target_tf_sq = feat["tf"] * feat["tf"] if algo == "unweighted_cosine" else 0
        for func_id, cand_tf in store.z.get(feat["key"], {}).items():
            if func_id == target_id:
                continue
            is_existing = func_id in intersection_counts
            if is_existing or can_add_new:
                if not is_existing:
                    intersection_counts[func_id] = 0.0
                    if algo == "unweighted_cosine":
                        shared_target_norm_sq[func_id] = 0.0
                    num_candidates += 1
                if algo == "jaccard":
                    intersection_counts[func_id] += min(feat["tf"], cand_tf)
                elif algo == "unweighted_cosine":
                    intersection_counts[func_id] += feat["tf"] * cand_tf
                    shared_target_norm_sq[func_id] += target_tf_sq

        processed_norm_sq += feat["tf"] * feat["tf"]
        processed_total += feat["tf"]

    kept = []
    for cid, intersect in intersection_counts.items():
        if algo == "jaccard":
            if intersect < threshold * target_total:
                continue
        elif algo == "unweighted_cosine":
            if shared_target_norm_sq.get(cid, 0) < min_shared_norm_sq:
                continue
        kept.append(cid)
    if not kept:
        return []

    count_idx = f"{collection}:idx:func:bsim_features_count"
    totals = [float(store.z.get(count_idx, {}).get(c) or 0) for c in kept]

    candidate_list = []
    if algo == "jaccard":
        for cid, cand_total in zip(kept, totals):
            if cand_total < min_features or cand_total <= 0:
                continue
            intersect = intersection_counts[cid]
            union = target_total + cand_total - intersect
            score = intersect / union if union > 0 else 0
            if score >= threshold and score > 0:
                candidate_list.append((cid, score, cand_total))
    else:
        need_norm = []
        for cid, cand_total in zip(kept, totals):
            if cand_total < min_features or cand_total <= 0:
                continue
            intersect = intersection_counts[cid]
            denom = threshold * target_norm
            max_cand_total = (intersect / denom) ** 2 if denom > 0 else 0
            if cand_total <= max_cand_total:
                need_norm.append((cid, intersect, cand_total))
        if need_norm:
            norms = [
                float(store.kv.get(f"{cid}:vec:norm") or 0) for cid, _, _ in need_norm
            ]
            for (cid, intersect, cand_total), cand_norm in zip(need_norm, norms):
                score = (
                    intersect / (target_norm * cand_norm)
                    if (target_norm > 0 and cand_norm > 0)
                    else 0
                )
                if score >= threshold and score > 0:
                    candidate_list.append((cid, score, cand_total))

    candidate_list.sort(key=lambda x: x[1], reverse=True)
    result = []
    for cid, score, cand_total in candidate_list[:limit]:
        result.extend([cid, str(score), str(cand_total)])
    return result


def reference_discover_minhash(store, args):
    target_id, collection = args[0], args[1]
    threshold = float(args[3])
    target_norm = float(args[5])
    limit = int(args[6])
    min_features = float(args[7] or 0)
    num_bands = int(args[8] or 10)

    bucket_keys = [
        f"{collection}:lsh:bucket:{band}:{args[9 + band]}" for band in range(num_bands)
    ]
    candidate_set = set()
    if bucket_keys:
        for cid in store.sunion(bucket_keys):
            if cid != target_id:
                candidate_set.add(cid)
    if not candidate_set:
        return []

    target_features = {}
    for i in range(9 + num_bands, len(args), 2):
        target_features[args[i]] = float(args[i + 1])

    intersection_counts = {}
    for f_hash in target_features:
        key = f"{collection}:feature:{f_hash}:functions"
        target_tf = target_features[f_hash]
        for func_id, cand_tf in store.z.get(key, {}).items():
            if func_id in candidate_set:
                intersection_counts[func_id] = (
                    intersection_counts.get(func_id, 0.0) + target_tf * cand_tf
                )
    if not intersection_counts:
        return []

    count_idx = f"{collection}:idx:func:bsim_features_count"
    ids = list(intersection_counts.keys())
    totals = [float(store.z.get(count_idx, {}).get(c) or 0) for c in ids]
    norms = [float(store.kv.get(f"{c}:vec:norm") or 0) for c in ids]

    candidate_list = []
    for cid, cand_total, cand_norm in zip(ids, totals, norms):
        if cand_total < min_features or cand_total <= 0:
            continue
        intersect = intersection_counts[cid]
        score = (
            intersect / (target_norm * cand_norm)
            if (target_norm > 0 and cand_norm > 0)
            else 0
        )
        if score >= threshold:
            candidate_list.append((cid, score, cand_total))

    candidate_list.sort(key=lambda x: x[1], reverse=True)
    result = []
    for cid, score, cand_total in candidate_list[:limit]:
        result.extend([cid, str(score), str(cand_total)])
    return result


# --- corpus generation -------------------------------------------------------


def build_corpus(rng, n_funcs, n_feats, coll="c", dup_rate=0.3, stopword=True):
    """Random corpus with duplicates and a stop-word feature, mirroring the shape
    measured on full_arbor (many near-identical functions, a few huge lists)."""
    store = FakeRedis()
    vectors = {}
    proto = None
    for i in range(n_funcs):
        fid = f"{coll}:func:{i:04d}:0"
        if proto and rng.random() < dup_rate:
            vec = dict(proto)
        else:
            k = rng.randint(1, min(8, n_feats))
            vec = {
                f"h{rng.randrange(n_feats)}": float(rng.randint(1, 9)) for _ in range(k)
            }
            proto = vec
        if stopword and rng.random() < 0.6:
            vec["h_stop"] = float(rng.randint(1, 4))
        vectors[fid] = vec

    cnt_key = f"{coll}:idx:func:bsim_features_count"
    store.z[cnt_key] = {}
    for fid, vec in vectors.items():
        store.z[f"{fid}:vec:tf"] = dict(vec)
        for h, tf in vec.items():
            store.z.setdefault(f"{coll}:feature:{h}:functions", {})[fid] = tf
        store.z[cnt_key][fid] = float(len(vec))
        store.kv[f"{fid}:vec:norm"] = str(math.sqrt(sum(t * t for t in vec.values())))
    return store, vectors


def args_for(coll, fid, vec, algo, threshold, limit, min_features=0):
    total = float(sum(vec.values()))
    norm = math.sqrt(sum(t * t for t in vec.values()))
    args = [
        fid,
        coll,
        algo,
        str(threshold),
        str(total),
        str(norm),
        str(limit),
        str(min_features),
    ]
    for h, tf in vec.items():
        args.extend([h, str(tf)])
    return args


def normalise(flat):
    """[(id, score, total), ...] sorted stably, so tie ORDER cannot mask a real
    difference in the candidate set or in any score."""
    trip = [
        (flat[i], round(float(flat[i + 1]), 12), round(float(flat[i + 2]), 12))
        for i in range(0, len(flat), 3)
    ]
    return sorted(trip)


def main():
    rng = random.Random(1337)
    svc_mod = __import__(
        "bsimvis.app.services.similarity_service", fromlist=["SimilarityService"]
    )
    SimilarityService = svc_mod.SimilarityService

    failures = 0
    cases = 0
    LIMIT = 10**9  # compare full candidate sets; tie-truncation is tested separately

    for trial in range(60):
        algo = "unweighted_cosine" if trial % 2 == 0 else "jaccard"
        threshold = rng.choice([0.5, 0.7, 0.9, 0.95])
        n_funcs = rng.randint(5, 120)
        n_feats = rng.randint(2, 25)
        coll = "c"
        store, vectors = build_corpus(rng, n_funcs, n_feats, coll)

        svc = SimilarityService.__new__(SimilarityService)
        svc.r = store
        svc._pl_budget = 10**9
        svc._reset_read_caches()

        for fid, vec in list(vectors.items())[:12]:
            if not vec:
                continue
            args = args_for(coll, fid, vec, algo, threshold, LIMIT)
            want = normalise(reference_discover_find(store, args))
            got = normalise(svc._discover_find(list(args)))
            cases += 1
            if want != got:
                failures += 1
                if failures <= 3:
                    print(
                        f"MISMATCH trial={trial} algo={algo} thr={threshold} fid={fid}"
                    )
                    w, g = dict((x[0], x[1]) for x in want), dict(
                        (x[0], x[1]) for x in got
                    )
                    only_w = set(w) - set(g)
                    only_g = set(g) - set(w)
                    diff = {k for k in set(w) & set(g) if w[k] != g[k]}
                    print(f"  only-reference={sorted(only_w)[:5]}")
                    print(f"  only-new      ={sorted(only_g)[:5]}")
                    print(
                        f"  score-diff    ={[(k, w[k], g[k]) for k in sorted(diff)[:5]]}"
                    )

    print(f"\n_discover_find: {cases - failures}/{cases} cases identical")

    # --- minhash_lsh path ----------------------------------------------------
    mh_cases = mh_failures = 0
    NUM_BANDS = 4
    for trial in range(30):
        coll = "c"
        store, vectors = build_corpus(rng, rng.randint(6, 80), rng.randint(2, 20), coll)
        fids = list(vectors)
        # Random LSH buckets over the corpus.
        bucket_of = {}
        for band in range(NUM_BANDS):
            for fid in fids:
                b = f"b{rng.randrange(3)}"
                bucket_of.setdefault((band, fid), b)
                store.z.setdefault(f"{coll}:lsh:bucket:{band}:{b}", {})[fid] = 1.0

        svc = SimilarityService.__new__(SimilarityService)
        svc.r = store
        svc._pl_budget = 10**9
        svc._reset_read_caches()

        for fid in fids[:10]:
            vec = vectors[fid]
            if not vec:
                continue
            norm = math.sqrt(sum(t * t for t in vec.values()))
            args = [
                fid,
                coll,
                "minhash_lsh",
                str(rng.choice([0.3, 0.6, 0.9])),
                str(sum(vec.values())),
                str(norm),
                str(LIMIT),
                "0",
                str(NUM_BANDS),
            ]
            args += [bucket_of[(band, fid)] for band in range(NUM_BANDS)]
            for h, tf in vec.items():
                args.extend([h, str(tf)])
            want = normalise(reference_discover_minhash(store, args))
            got = normalise(svc._discover_minhash(list(args)))
            mh_cases += 1
            if want != got:
                mh_failures += 1
                if mh_failures <= 3:
                    print(f"MINHASH MISMATCH trial={trial} fid={fid}")
                    print(f"  reference={want[:4]}")
                    print(f"  new      ={got[:4]}")

    print(f"_discover_minhash: {mh_cases - mh_failures}/{mh_cases} cases identical")

    if failures or mh_failures:
        print(f"FAIL: {failures + mh_failures} mismatches")
        return 1
    print("PASS: vectorised discovery is bit-identical to the reference loops")
    return 0


if __name__ == "__main__":
    sys.exit(main())
