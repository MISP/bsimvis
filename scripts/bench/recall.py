#!/usr/bin/env python3
"""Does weighting find functions back better than unweighted?

Retrieval-first, because that is how the tool is actually used: for each
function in binary A, rank every function in binary B and ask where its true
counterpart landed. Threshold-free, so no tuning is involved.

Ground truth is the symbol name. Two builds of one source from different
compilers/architectures share function names, so a same-name pair is a known
match and a different-name pair is a known non-match -- no judgement call.

IMPORTANT: with a single project, every non-match is two functions from the
same program, so the false-positive numbers here are a floor, not an estimate.
A trustworthy FPR needs a second, unrelated program built with the same
toolchains. The `identical vectors` line reports the ceiling: distinct
functions that share a feature vector cannot be separated by ANY weighting
scheme, so they are an information limit of the signature, not an error.

Vectors come from either:
  --vectors FILE   JSON from `oracle_compare.py --dump-vectors` ({"A": ..., "B": ...})
  --collection C   read {coll}:func:*:vec:tf straight from kvrocks, grouping by
                   file md5 and labelling from each function's :meta

Usage:
    scripts/bench/recall.py --vectors vecs.json
    scripts/bench/recall.py --collection wtest --md5 <a> --md5 <b>
"""

import argparse
import json
import math
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from bsimvis.app.services import bsim_profiles, bsim_weights  # noqa: E402

MIN_FEATURES = 10


# --- scorers ----------------------------------------------------------------


def prep_unweighted(v):
    return {h: int(t) for h, t in v.items()}, math.sqrt(sum(int(t) ** 2 for t in v.values()))


def score_unweighted(pa, pb):
    a, na = pa
    b, nb = pb
    small, large = (a, b) if len(a) <= len(b) else (b, a)
    dot = sum(x * large[h] for h, x in small.items() if h in large)
    return dot / (na * nb) if na and nb else 0.0


def score_jaccard(pa, pb):
    a, _ = pa
    b, _ = pb
    small, large = (a, b) if len(a) <= len(b) else (b, a)
    inter = sum(min(x, large[h]) for h, x in small.items() if h in large)
    union = sum(a.values()) + sum(b.values()) - inter
    return inter / union if union else 0.0


def prep_weighted(v, table):
    """int-keyed tf + this function's own coefficients, length and hashcount.

    Per-function work, done once -- see scripts/bench/scoring_cost.py for why
    doing it inside the pair loop badly overstates the weighted cost.
    """
    tf = {int(h, 16): int(t) for h, t in v.items()}
    co = {h: table.idfweight[table.lookup.get(h, 0)] * table.tfweight[min(t, 64) - 1]
          for h, t in tf.items()}
    return tf, co, math.sqrt(sum(c * c for c in co.values())), sum(tf.values())


def score_weighted(pa, pb, table, want_sig=False):
    tfa, coa, la, hca = pa
    tfb, cob, lb, hcb = pb
    if len(tfa) <= len(tfb):
        s_tf, s_co, l_tf, l_co = tfa, coa, tfb, cob
    else:
        s_tf, s_co, l_tf, l_co = tfb, cob, tfa, coa
    dot = 0.0
    inter = 0
    for h, x in s_tf.items():
        y = l_tf.get(h)
        if y is None:
            continue
        # min-tf rule: the smaller side's own precomputed coefficient is correct.
        dot += (s_co[h] if x <= y else l_co[h]) ** 2
        inter += x if x < y else y
    sim = dot / (la * lb) if la > 0 and lb > 0 else 0.0
    if sim > 1.0:
        sim = 1.0
    if not want_sig:
        return sim
    lo, hi = (hca, hcb) if hca < hcb else (hcb, hca)
    sig = (dot - (lo - inter) * (table.probflip0 + table.probflip1 / hi)
           - (hi - lo) * (table.probdiff0 + table.probdiff1 / hi) + table.addend) if hi else table.addend
    return sim, sig


# --- loading ----------------------------------------------------------------


def load_from_json(path):
    d = json.load(open(path))
    return d["A"], d.get("B") or {}


def load_from_collection(collection, md5s):
    from bsimvis.app.services.redis_client import get_redis

    r = get_redis()
    groups = {}
    for key in r.scan_iter(match=f"{collection}:func:*:meta", count=5000):
        key = key.decode()
        base = key[: -len(":meta")]
        parts = base.split(":")
        md5 = parts[2]
        if md5s and md5 not in md5s:
            continue
        try:
            meta = json.loads(r.get(key))
        except Exception:
            continue
        if not isinstance(meta, dict):
            continue
        name = meta.get("function_name")
        vec = r.zrange(f"{base}:vec:tf", 0, -1, withscores=True)
        if not name or not vec:
            continue
        v = {h.decode() if isinstance(h, bytes) else h: int(t) for h, t in vec}
        if len(v) < MIN_FEATURES:
            continue
        # Keep the largest instance when a name repeats within one binary.
        g = groups.setdefault(md5, {})
        if name not in g or len(v) > len(g[name]):
            g[name] = v
    if len(groups) < 2:
        sys.exit(f"need functions from 2 binaries, found {len(groups)}")
    (_, a), (_, b) = sorted(groups.items())[:2]
    return a, b


# --- evaluation -------------------------------------------------------------


def evaluate(name, A, B, prep, score):
    """Rank all of B for each function of A; report where the true match landed."""
    pa = {n: prep(v) for n, v in A.items()}
    pb = {n: prep(v) for n, v in B.items()}
    shared = [n for n in pa if n in pb]

    ranks, true_scores, false_scores = [], [], []
    for n in shared:
        scored = sorted(((score(pa[n], pb[m]), m) for m in pb), reverse=True)
        for i, (s, m) in enumerate(scored, 1):
            if m == n:
                ranks.append(i)
                true_scores.append(s)
                break
        false_scores += [s for s, m in scored if m != n]

    n = len(ranks)
    if not n:
        return None
    return {
        "algo": name,
        "queries": n,
        "recall@1": sum(r == 1 for r in ranks) / n,
        "recall@5": sum(r <= 5 for r in ranks) / n,
        "mrr": sum(1 / r for r in ranks) / n,
        "true_median": statistics.median(true_scores),
        "true_min": min(true_scores),
        "false_median": statistics.median(false_scores),
        "false_max": max(false_scores),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vectors", help="JSON from oracle_compare.py --dump-vectors")
    ap.add_argument("--collection", help="read vectors from kvrocks instead")
    ap.add_argument("--md5", action="append", default=[], help="restrict to these binaries")
    ap.add_argument("--profile", default=None, help="BSim weights profile")
    args = ap.parse_args()

    if args.vectors:
        A, B = load_from_json(args.vectors)
    elif args.collection:
        A, B = load_from_collection(args.collection, set(args.md5))
    else:
        ap.error("need --vectors or --collection")

    table = bsim_weights.load(bsim_profiles.get_profile(args.profile).weights_path)
    print(f"binary A: {len(A)} functions   binary B: {len(B)} functions")
    shared = set(A) & set(B)
    print(f"shared names (ground-truth matches): {len(shared)}")
    if not shared:
        sys.exit("no shared function names -- are these builds of the same source?")

    rows = [
        evaluate("jaccard", A, B, prep_unweighted, score_jaccard),
        evaluate("unweighted_cosine", A, B, prep_unweighted, score_unweighted),
        evaluate("weighted_cosine", A, B,
                 lambda v: prep_weighted(v, table),
                 lambda x, y: score_weighted(x, y, table)),
    ]
    rows = [r for r in rows if r]

    print(f"\n{'algo':<20}{'recall@1':>10}{'recall@5':>10}{'MRR':>8}"
          f"{'true med':>10}{'true min':>10}{'false med':>11}{'false max':>11}")
    for r in rows:
        print(f"{r['algo']:<20}{r['recall@1']:>9.1%}{r['recall@5']:>10.1%}{r['mrr']:>8.3f}"
              f"{r['true_median']:>10.4f}{r['true_min']:>10.4f}"
              f"{r['false_median']:>11.4f}{r['false_max']:>11.4f}")

    best = max(rows, key=lambda r: r["recall@1"])
    print(f"\nbest recall@1: {best['algo']} ({best['recall@1']:.1%})")

    # Ceiling: pairs no weighting scheme could ever separate.
    ident = 0
    for n, v in A.items():
        for m, w in B.items():
            if n != m and v == w:
                ident += 1
    print(f"ceiling: {ident} distinct-name pairs have IDENTICAL feature vectors "
          f"(unfixable by any weighting)")
    print("NOTE: single project -> false-score columns are a floor, not an FPR. "
          "A real FPR needs a second unrelated program.")


if __name__ == "__main__":
    sys.exit(main())
