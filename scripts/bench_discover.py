"""Benchmark sparse-matmul discovery against `discover_edges`, per algorithm.

Every algorithm's shared mass is a sum over hashes of some function of
min(tf_a, tf_b) (or of tf_a*tf_b), so one layered sparse product covers them:
    sum_h f(min(a,b)) = sum_k [a>=k][b>=k] * (f(k) - f(k-1))
Pairs: real binary pairs plus all-pairs of mutated variants of the biggest
binary (the Mirai-like case).

    uv run python scripts/bench_discover.py
"""

import glob
import json
import random
import time

from bsimvis.app.services.bin_sim_service import _discover_edges_py, discover_edges

MIN_SCORE = 0.4
ALGOS = ["jaccard", "unweighted_cosine", "binary_cosine", "weighted_cosine"]
random.seed(7)


def load():
    bins = {}
    for path in sorted(glob.glob("data/bench/*.json")):
        d = json.load(open(path))
        vecs = {}
        for i, f in enumerate(d["functions"]):
            tf = f["function_features"]["bsim_features_tf"]
            if tf:
                vecs[f"{d['file_md5']}:{i}"] = {t["hash"]: float(t["tf"]) for t in tf}
        if len(vecs) > 20:
            bins[d["file_md5"]] = vecs
    return bins


def mutate(vecs, tag, frac=0.1):
    out = {}
    for fid, v in vecs.items():
        keys = list(v)
        drop = set(random.sample(keys, int(len(keys) * frac)))
        nv = {h: c for h, c in v.items() if h not in drop}
        for j in range(len(drop)):
            nv[f"{random.getrandbits(32):x}"] = 1.0
        out[f"{tag}:{fid}"] = nv
    return out


def timed(fn):
    t = time.perf_counter()
    r = fn()
    return r, time.perf_counter() - t


def main():
    bins = load()
    names = list(bins)
    vectors = {}
    for v in bins.values():
        vectors.update(v)
    sets = [(m, set(bins[m])) for m in names]
    big = max(names, key=lambda m: len(bins[m]))
    for i in range(10):
        mv = mutate(bins[big], f"var{i}")
        vectors.update(mv)
        sets.append((f"var{i}", set(mv)))
    pairs = [(a, b) for i, a in enumerate(sets) for b in sets[i + 1 :]]
    print(f"{len(vectors)} functions, {len(pairs)} pairs")
    for algo in ALGOS:
        base_t = sp_t = 0.0
        for (_, fa), (_, fb) in pairs:
            base, t = timed(
                lambda: _discover_edges_py(vectors, fa, fb, algo, MIN_SCORE)
            )
            base_t += t
            sp, t = timed(lambda: discover_edges(vectors, fa, fb, algo, MIN_SCORE))
            sp_t += t
            key = lambda es: sorted((x, y, round(s, 6)) for x, y, s in es)
            assert key(sp) == key(base), f"{algo} mismatch"
        print(
            f"{algo:18} base {base_t:6.2f}s  sparse {sp_t:6.2f}s  {base_t / sp_t:.1f}x"
        )


if __name__ == "__main__":
    main()
