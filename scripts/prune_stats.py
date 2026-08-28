#!/usr/bin/env python3
"""Measure how much each discovery optimization prunes, on live kvrocks.

Funnel per target function (unweighted_cosine, the default algo):

  N_all      collection function count       (brute-force cosine baseline)
  N_touched  functions sharing >=1 feature   (inverse-index reduction)
  inserted   candidates kept after rarest-first insertion bound (Phase 1a)
  kept       survive shared-norm filter       (Phase 2)
  need_norm  survive total-count upper bound  (Phase 3, = actual norm GETs)
  matched    final score >= threshold

Reuses SimilarityService caches/helpers; reimplements the loop with counters.
Run: KVROCKS_PORT=6667 uv run python scripts/prune_stats.py mirai7 [n_targets]
"""

import os, sys, math, random, time

os.environ.setdefault("KVROCKS_PORT", "6667")

from bsimvis.app.services.similarity_service import SimilarityService
from bsimvis.app.services.config_service import config_service


def discover_instrumented(
    svc, collection, target_id, features_raw, threshold, algo="unweighted_cosine"
):
    r = svc.r
    target_features = {h: float(tf) for h, tf in features_raw}
    target_total = sum(target_features.values())
    target_norm_sq = sum(v * v for v in target_features.values())
    target_norm = math.sqrt(target_norm_sq)
    min_features = float(config_service.get("similarity.min_features", 0) or 0)
    limit = int(config_service.get("similarity.top_k", 1000))

    min_shared_norm_sq = (threshold * target_norm) ** 2

    # 1. rarest-first sizing
    feats = list(target_features.items())
    pipe = r.pipeline(transaction=False)
    for f_hash, _ in feats:
        pipe.zcard(f"{collection}:feature:{f_hash}:functions")
    sizes = pipe.execute()
    features_sorted = sorted(
        (
            {
                "hash": h,
                "tf": tf,
                "key": f"{collection}:feature:{h}:functions",
                "size": sz,
            }
            for (h, tf), sz in zip(feats, sizes)
        ),
        key=lambda x: x["size"],
    )

    # N_touched = union of every posting list (all functions sharing >=1 feature)
    touched = set()
    for feat in features_sorted:
        for func_id, _ in svc._pl(feat["key"]):
            if func_id != target_id:
                touched.add(func_id)
    n_touched = len(touched)

    # 2. accumulate with pruning bounds (mirror of _discover_find)
    intersection_counts = {}
    shared_target_norm_sq = {}
    processed_norm_sq = 0.0
    num_candidates = 0
    blocked_by_insert_bound = 0  # distinct funcs seen only while insertion disabled

    for feat in features_sorted:
        remaining_norm_sq = target_norm_sq - processed_norm_sq
        can_add_new = remaining_norm_sq >= min_shared_norm_sq
        if not can_add_new and num_candidates == 0:
            break
        target_tf_sq = feat["tf"] * feat["tf"]
        for func_id, cand_tf in svc._pl(feat["key"]):
            if func_id == target_id:
                continue
            is_existing = func_id in intersection_counts
            if is_existing or can_add_new:
                if not is_existing:
                    intersection_counts[func_id] = 0.0
                    shared_target_norm_sq[func_id] = 0.0
                    num_candidates += 1
                intersection_counts[func_id] += feat["tf"] * cand_tf
                shared_target_norm_sq[func_id] += target_tf_sq
        processed_norm_sq += feat["tf"] * feat["tf"]

    inserted = num_candidates
    # candidates that exist in the touched universe but were never inserted
    blocked_by_insert_bound = n_touched - inserted

    # 3. Phase-2 shared-norm filter
    kept = [
        cid
        for cid in intersection_counts
        if shared_target_norm_sq.get(cid, 0) >= min_shared_norm_sq
    ]
    pruned_phase2 = inserted - len(kept)

    # 4. counts + Phase-3 total-count upper bound
    count_idx = f"{collection}:idx:func:bsim_features_count"
    totals = svc._counts(count_idx, kept) if kept else []
    need_norm = []
    for cid, cand_total in zip(kept, totals):
        if cand_total < min_features or cand_total <= 0:
            continue
        intersect = intersection_counts[cid]
        denom = threshold * target_norm
        max_cand_total = (intersect / denom) ** 2 if denom > 0 else 0
        if cand_total <= max_cand_total:
            need_norm.append((cid, intersect, cand_total))
    pruned_phase3 = len(kept) - len(need_norm)

    # 5. final score
    matched = 0
    if need_norm:
        norms = svc._norms([c for c, _, _ in need_norm])
        for (cid, intersect, _), cand_norm in zip(need_norm, norms):
            score = (
                intersect / (target_norm * cand_norm)
                if (target_norm > 0 and cand_norm > 0)
                else 0
            )
            if score >= threshold and score > 0:
                matched += 1

    return {
        "n_touched": n_touched,
        "inserted": inserted,
        "blocked_insert": blocked_by_insert_bound,
        "kept": len(kept),
        "pruned_phase2": pruned_phase2,
        "need_norm": len(need_norm),
        "pruned_phase3": pruned_phase3,
        "matched": matched,
        "target_feats": len(target_features),
    }


def discover_baseline(svc, collection, target_id, features_raw, threshold):
    """No math pruning: inverse index only. Insert every touched candidate,
    fetch a norm for each, full cosine. Same matches as the optimized path —
    this is the cost the Phase 1-3 bounds eliminate."""
    r = svc.r
    target_features = {h: float(tf) for h, tf in features_raw}
    target_norm = math.sqrt(sum(v * v for v in target_features.values()))
    min_features = float(config_service.get("similarity.min_features", 0) or 0)

    intersection = {}
    for h, tf in target_features.items():
        for func_id, cand_tf in svc._pl(f"{collection}:feature:{h}:functions"):
            if func_id == target_id:
                continue
            intersection[func_id] = intersection.get(func_id, 0.0) + tf * float(cand_tf)

    if not intersection:
        return 0
    ids = list(intersection)
    norms = svc._norms(ids)  # every touched candidate — no Phase-3 pre-filter
    matched = 0
    for cid, cand_norm in zip(ids, norms):
        if not cand_norm:
            continue
        score = (
            intersection[cid] / (target_norm * cand_norm)
            if (target_norm and cand_norm)
            else 0
        )
        if score >= threshold and score > 0:
            matched += 1
    return matched


def build_argv(collection, target_id, features_raw, threshold):
    tf = {h: float(t) for h, t in features_raw}
    total = sum(tf.values())
    norm = math.sqrt(sum(v * v for v in tf.values()))
    limit = int(config_service.get("similarity.top_k", 1000))
    minf = config_service.get("similarity.min_features", 0) or 0
    argv = [
        target_id,
        collection,
        "unweighted_cosine",
        threshold,
        total,
        norm,
        limit,
        minf,
    ]
    for h, t in tf.items():
        argv += [h, t]
    return argv


def run_timed(collection, n_targets):
    """Wall-time optimized production discovery vs no-pruning baseline."""
    threshold = float(config_service.get("similarity.min_score", 0.9))
    svc = SimilarityService()
    r = svc.r
    all_ids = sorted(r.smembers(f"{collection}:indexed:functions"))
    random.seed(1)
    sample = random.sample(all_ids, min(n_targets, len(all_ids)))

    # Warm the shared static index (posting lists) once so timing measures the
    # per-discovery compute + norm-fetch delta, not cold index reads both pay.
    svc._reset_read_caches()
    targets = []
    for fid in sample:
        feats = r.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
        if feats:
            targets.append((fid, feats))
            for h, _ in feats:
                pass
    # prime posting lists + counts by running each once (fills LRU/dict caches)
    for fid, feats in targets:
        svc._discover_find(build_argv(collection, fid, feats, threshold))

    t_opt = 0.0
    t_base = 0.0
    m_opt = m_base = 0
    for fid, feats in targets:
        svc._norm_cache.clear()  # cold norms: the DB round-trips pruning saves
        t0 = time.perf_counter()
        res = svc._discover_find(build_argv(collection, fid, feats, threshold))
        t_opt += time.perf_counter() - t0
        m_opt += len(res) // 3

        svc._norm_cache.clear()
        t0 = time.perf_counter()
        m_base += discover_baseline(svc, collection, fid, feats, threshold)
        t_base += time.perf_counter() - t0

    n = len(targets)
    print(f"collection={collection}  threshold={threshold}  targets={n}\n")
    print(
        f"  optimized (Phase 1-3 pruning)   {t_opt*1000:10.1f} ms total   {t_opt/n*1000:7.3f} ms/target   {m_opt} matches"
    )
    print(
        f"  baseline  (inverse index only)  {t_base*1000:10.1f} ms total   {t_base/n*1000:7.3f} ms/target   {m_base} matches"
    )
    print(
        f"\n  speedup   {t_base/t_opt:6.2f}x   ({(1-t_opt/t_base)*100:.1f}% wall-time saved by pruning math)"
    )
    print(f"  matches equal: {m_opt == m_base}")


def main():
    if "--time" in sys.argv:
        sys.argv.remove("--time")
        run_timed(
            sys.argv[1] if len(sys.argv) > 1 else "mirai7",
            int(sys.argv[2]) if len(sys.argv) > 2 else 300,
        )
        return
    collection = sys.argv[1] if len(sys.argv) > 1 else "mirai7"
    n_targets = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    threshold = float(config_service.get("similarity.min_score", 0.9))

    svc = SimilarityService()
    svc._reset_read_caches()
    r = svc.r

    n_all = r.scard(f"{collection}:indexed:functions")
    if not n_all:
        print(f"collection '{collection}' empty / not found")
        sys.exit(1)
    all_ids = sorted(r.smembers(f"{collection}:indexed:functions"))
    random.seed(1)
    sample = random.sample(all_ids, min(n_targets, len(all_ids)))

    print(
        f"collection={collection}  N_all={n_all}  threshold={threshold}  sampled={len(sample)}\n"
    )

    agg = {}
    used = 0
    for fid in sample:
        feats = r.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
        if not feats:
            continue
        used += 1
        st = discover_instrumented(svc, collection, fid, feats, threshold)
        st["n_all"] = n_all
        for k, v in st.items():
            agg[k] = agg.get(k, 0) + v

    if not used:
        print("no targets with vectors")
        sys.exit(1)

    # aggregate funnel (sum over targets)
    A = agg["n_all"]
    T = agg["n_touched"]
    I = agg["inserted"]
    K = agg["kept"]
    NN = agg["need_norm"]
    M = agg["matched"]

    def line(label, val, prev):
        pct_prev = f"-{100*(prev-val)/prev:5.1f}% vs prev" if prev else ""
        pct_all = f"{100*val/A:6.2f}% of N_all"
        print(f"  {label:<34}{val:>14,}   {pct_all}   {pct_prev}")

    print("FUNNEL (summed over %d targets):" % used)
    line("N_all (brute-force baseline)", A, 0)
    line("N_touched (inverse index)", T, A)
    line("inserted (Phase1 rarest-first)", I, T)
    line("kept (Phase2 shared-norm)", K, I)
    line("need_norm GET (Phase3 count)", NN, K)
    line("matched (final >= threshold)", M, NN)

    print(f"\nPer optimization — candidates eliminated (summed):")
    print(
        f"  inverse index skips        {A - T:>14,}  ({100*(A-T)/A:.2f}% of all pairs)"
    )
    print(f"  Phase1 rarest-first bound  {agg['blocked_insert']:>14,}")
    print(f"  Phase2 shared-norm filter  {agg['pruned_phase2']:>14,}")
    print(f"  Phase3 total-count bound   {agg['pruned_phase3']:>14,}")
    print(
        f"\n  norm GETs actually issued  {NN:>14,}  vs {A:>,} brute-force = "
        f"{100*NN/A:.3f}% ({A/NN:.0f}x fewer)"
        if NN
        else ""
    )
    print(f"  avg target features        {agg['target_feats']/used:>14.1f}")


if __name__ == "__main__":
    main()
