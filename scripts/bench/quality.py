#!/usr/bin/env python3
"""Retrieval quality on the full benchmark corpus: weighted vs unweighted vs jaccard.

Same question as scripts/bench/recall.py, but at corpus scale and with the two
things the small comparison in MISP/bsimvis#50 could not have:

  * **Real negatives.** The reference side is not one sibling binary, it is every
    binary of the same build variant across *all* projects. A wrong top-1 is then
    a genuine cross-program false positive, so the false-score column is an FPR
    and not a floor.
  * **Named difficulty axes.** Query/reference pairs are grouped by what differs
    -- architecture, optimisation level, or linkage -- so "weighting helps" can be
    attributed instead of averaged away.

Scoring is offline over `vectors/` produced by corpus/extract.py: no server, no
kvrocks, no Ghidra. Reruns are cheap; the expensive part happened once.

Usage:
    scripts/bench/quality.py [--out ~/data/bsim-bench-corpus] [--axis arch|opt|link|all]
                             [--sample 300] [--jobs 8] [--report quality.json]
"""

import argparse
import collections
import importlib.util
import json
import math
import multiprocessing
import os
import random
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, REPO)

from bsimvis.app.services import bsim_profiles, bsim_weights  # noqa: E402

# reuse the scorers rather than re-deriving them -- recall.py is the reference
_spec = importlib.util.spec_from_file_location("recall", os.path.join(HERE, "recall.py"))
recall = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(recall)

MIN_FEATURES = 10


def load_vectors(corpus, manifest):
    """{file -> {func_name: {hash: tf}}}, dropping functions too small for BSim."""
    out = {}
    for rec in manifest["binaries"]:
        path = os.path.join(corpus, "vectors", f"{rec['md5']}.json")
        if not os.path.exists(path):
            continue
        vecs = json.load(open(path))["vectors"]
        vecs = {n: v for n, v in vecs.items() if len(v) >= MIN_FEATURES}
        if vecs:
            out[rec["file"]] = vecs
    return out


def extracted(corpus, manifest):
    """Binaries that actually have vectors -- extraction may be partial."""
    return {r["file"] for r in manifest["binaries"]
            if os.path.exists(os.path.join(corpus, "vectors", f"{r['md5']}.json"))}


def pair_specs(manifest, axis, available=None):
    """(query, reference, axis) build pairs that differ in exactly one coordinate."""
    by_key = {r["file"]: r for r in manifest["binaries"]
              if available is None or r["file"] in available}
    pairs = []
    for a in by_key.values():
        for b in by_key.values():
            if a["file"] >= b["file"] or a["project"] != b["project"]:
                continue
            diff = [k for k in ("target", "opt", "link") if a[k] != b[k]]
            if len(diff) != 1:
                continue
            kind = {"target": "arch", "opt": "opt", "link": "link"}[diff[0]]
            if axis in ("all", kind):
                pairs.append((a["file"], b["file"], kind))
    return pairs


def reference_pool(manifest, ref_file, vectors):
    """Reference binary + distractors: same build variant, every other project.

    Those distractors are what make a wrong top-1 a real false positive: they are
    different programs, so no query function has a counterpart in them.
    """
    by_file = {r["file"]: r for r in manifest["binaries"]}
    ref = by_file[ref_file]
    pool = {}
    for rec in manifest["binaries"]:
        same_variant = (rec["target"] == ref["target"] and rec["opt"] == ref["opt"]
                        and rec["link"] == ref["link"])
        if rec["file"] != ref_file and not (same_variant and rec["project"] != ref["project"]):
            continue
        for name, vec in vectors.get(rec["file"], {}).items():
            # namespace by file: two projects can both define `main`
            pool[(rec["file"], name)] = vec
    return pool, ref["project"]


_STATE = {}


def _init(corpus, manifest):
    """Load the corpus vectors once per worker, not once per pair."""
    _STATE["manifest"] = manifest
    _STATE["vectors"] = load_vectors(corpus, manifest)


def evaluate_pair(job):
    query_file, ref_file, kind, sample, seed, profile = job
    manifest = _STATE["manifest"]
    vectors = _STATE["vectors"]
    A = vectors.get(query_file, {})
    pool, ref_project = reference_pool(manifest, ref_file, vectors)
    if not A or not pool:
        return None

    truth = {n: (ref_file, n) for n in A if (ref_file, n) in pool}
    if not truth:
        return None
    queries = sorted(truth)
    if sample and len(queries) > sample:
        queries = random.Random(seed).sample(queries, sample)

    table = bsim_weights.load(bsim_profiles.get_profile(profile).weights_path)
    algos = {
        "jaccard": (recall.prep_unweighted, recall.score_jaccard),
        "unweighted_cosine": (recall.prep_unweighted, recall.score_unweighted),
        "weighted_cosine": (lambda v: recall.prep_weighted(v, table),
                            lambda x, y: recall.score_weighted(x, y, table)),
    }

    result = {"query": query_file, "reference": ref_file, "axis": kind,
              "queries": len(queries), "pool": len(pool), "algos": {}}

    for algo, (prep, score) in algos.items():
        pa = {n: prep(A[n]) for n in queries}
        pp = {k: prep(v) for k, v in pool.items()}
        ranks, true_scores, top_false, cross_program_hits = [], [], [], 0
        for n in queries:
            scored = sorted(((score(pa[n], pp[k]), k) for k in pp), reverse=True)
            gold = truth[n]
            for i, (s, k) in enumerate(scored, 1):
                if k == gold:
                    ranks.append(i)
                    true_scores.append(s)
                    break
            best_wrong = next((s for s, k in scored if k != gold), 0.0)
            top_false.append(best_wrong)
            # top-1 from a different program = unambiguous false positive
            if scored and scored[0][1] != gold and scored[0][1][0] != ref_file:
                cross_program_hits += 1
        n = len(ranks)
        if not n:
            continue
        result["algos"][algo] = {
            "recall@1": sum(r == 1 for r in ranks) / n,
            "recall@5": sum(r <= 5 for r in ranks) / n,
            "mrr": sum(1 / r for r in ranks) / n,
            "true_median": statistics.median(true_scores),
            "true_min": min(true_scores),
            "false_top_median": statistics.median(top_false),
            "false_top_max": max(top_false),
            "cross_program_top1": cross_program_hits / n,
            "separable": sum(t > f for t, f in zip(true_scores, top_false)) / n,
        }
    return result


def aggregate(results):
    """Mean per algo, overall and per axis. Pairs are equally weighted."""
    agg = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in results:
        for algo, m in r["algos"].items():
            for k, v in m.items():
                agg[("all", algo)][k].append(v)
                agg[(r["axis"], algo)][k].append(v)
    return {f"{axis}|{algo}": {k: sum(v) / len(v) for k, v in metrics.items()}
            for (axis, algo), metrics in agg.items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.environ.get(
        "CORPUS_ROOT", os.path.expanduser("~/data/bsim-bench-corpus")))
    ap.add_argument("--axis", default="all", choices=["all", "arch", "opt", "link"])
    ap.add_argument("--sample", type=int, default=300,
                    help="query functions per pair (0 = all; cost is O(sample x pool))")
    ap.add_argument("--jobs", type=int, default=max(1, os.cpu_count() // 2))
    ap.add_argument("--limit-pairs", type=int, default=0)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--report", default=None)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()

    manifest = json.load(open(os.path.join(args.out, "manifest.json")))
    have = extracted(args.out, manifest)
    if not have:
        sys.exit(f"no vectors under {args.out}/vectors -- run corpus/extract.py first")
    pairs = pair_specs(manifest, args.axis, have)
    print(f"{len(have)}/{len(manifest['binaries'])} binaries extracted")
    if args.limit_pairs:
        pairs = random.Random(args.seed).sample(pairs, min(args.limit_pairs, len(pairs)))
    print(f"{len(pairs)} build pairs on axis={args.axis}, "
          f"sample={args.sample or 'all'} queries/pair, {args.jobs} workers")

    jobs = [(q, r, k, args.sample, args.seed, args.profile) for q, r, k in pairs]
    results = []
    with multiprocessing.Pool(args.jobs, initializer=_init,
                              initargs=(args.out, manifest)) as pool:
        for i, res in enumerate(pool.imap_unordered(evaluate_pair, jobs), 1):
            if res:
                results.append(res)
            if i % 10 == 0:
                print(f"  {i}/{len(jobs)} pairs", flush=True)

    if not results:
        sys.exit("no evaluable pairs -- did corpus/extract.py run?")

    summary = aggregate(results)
    header = (f"{'axis / algo':<32}{'recall@1':>10}{'recall@5':>10}{'MRR':>8}"
              f"{'true min':>10}{'false max':>11}{'sep':>8}{'xprog fp':>10}")
    print("\n" + header)
    print("-" * len(header))
    for key in sorted(summary):
        m = summary[key]
        print(f"{key:<32}{m['recall@1']:>9.1%}{m['recall@5']:>10.1%}{m['mrr']:>8.3f}"
              f"{m['true_min']:>10.4f}{m['false_top_max']:>11.4f}"
              f"{m['separable']:>7.1%}{m['cross_program_top1']:>10.2%}")
    print("\nsep      = queries where the true match outscored every wrong candidate")
    print("xprog fp = top-1 came from a different program entirely (real false positive)")

    if args.report:
        with open(args.report, "w") as fh:
            json.dump({"pairs": results, "summary": summary,
                       "config": vars(args)}, fh, indent=1, default=str)
        print(f"\nwrote {args.report}")


if __name__ == "__main__":
    main()
