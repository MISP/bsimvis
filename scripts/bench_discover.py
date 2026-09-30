"""Benchmark discover_edges speed-ups on data/bench (jaccard).

Pairs: every real binary pair, plus each binary vs a mutated copy of itself
(the near-identical-variant case that makes discovery slow on Mirai-like sets).

    uv run python scripts/bench_discover.py
"""

import glob
import json
import random
import time

import numpy as np
from scipy import sparse

from bsimvis.app.services.bin_sim_service import discover_edges
from bsimvis.app.services.bin_sim_tags import greedy_match

MIN_SCORE = 0.4
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


def mutate(vecs, tag, frac=0.2):
    out = {}
    for fid, v in vecs.items():
        keys = list(v)
        drop = set(random.sample(keys, int(len(keys) * frac)))
        nv = {h: c for h, c in v.items() if h not in drop}
        for j in range(len(drop)):
            nv[f"new{tag}{fid}{j}"] = 1.0
        out[f"{tag}:{fid}"] = nv
    return out


# --- option 2: postings + totals built once per binary --------------------
def index_for(vectors, fids):
    fids = [f for f in sorted(fids) if vectors.get(f)]
    postings = {}
    for fid in fids:
        for h in vectors[fid]:
            postings.setdefault(h, []).append(fid)
    return postings, {f: sum(vectors[f].values()) for f in fids}


def discover_cached(vectors, fids_a, index_b, min_score):
    postings, totals_b = index_b
    fids_a = [f for f in sorted(fids_a) if vectors.get(f)]
    edges = []
    for fid_a in fids_a:
        vec_a = vectors[fid_a]
        total_a = sum(vec_a.values())
        shared_by_b = {}
        for h, value_a in vec_a.items():
            for fid_b in postings.get(h, ()):
                add = min(value_a, vectors[fid_b][h])
                shared_by_b[fid_b] = shared_by_b.get(fid_b, 0.0) + add
        for fid_b, shared in shared_by_b.items():
            score = shared / (total_a + totals_b[fid_b] - shared)
            if score >= min_score:
                edges.append((fid_a, fid_b, score))
    return edges


# --- option 3: layered sparse matmul, exact for integer tf ----------------
def discover_sparse(vectors, fids_a, fids_b, min_score):
    fa = [f for f in sorted(fids_a) if vectors.get(f)]
    fb = [f for f in sorted(fids_b) if vectors.get(f)]
    if not fa or not fb:
        return []
    col = {}
    for f in fa + fb:
        for h in vectors[f]:
            col.setdefault(h, len(col))

    def mat(fids):
        r, c, v = [], [], []
        for i, f in enumerate(fids):
            for h, x in vectors[f].items():
                r.append(i)
                c.append(col[h])
                v.append(int(x))
        return sparse.csr_matrix((v, (r, c)), shape=(len(fids), len(col)))

    A, B = mat(fa), mat(fb)
    ta = np.asarray(A.sum(axis=1)).ravel()
    tb = np.asarray(B.sum(axis=1)).ravel()
    shared = None
    for k in range(1, int(max(A.max(), B.max())) + 1):
        Ak = (A >= k).astype(np.float32)
        Bk = (B >= k).astype(np.float32)
        part = Ak @ Bk.T
        shared = part if shared is None else shared + part
    s = shared.tocoo()
    score = s.data / (ta[s.row] + tb[s.col] - s.data)
    keep = score >= min_score
    return [
        (fa[i], fb[j], float(x))
        for i, j, x in zip(s.row[keep], s.col[keep], score[keep])
    ]


class SparseIndex:
    """Per-binary CSR matrices over one shared hash->column map, built once."""

    def __init__(self, vectors):
        self.vectors = vectors
        self.col = {}
        self.cache = {}

    def get(self, fids):
        key = frozenset(fids)
        if key not in self.cache:
            fl = [f for f in sorted(fids) if self.vectors.get(f)]
            r, c, v = [], [], []
            for i, f in enumerate(fl):
                for h, x in self.vectors[f].items():
                    r.append(i)
                    c.append(self.col.setdefault(h, len(self.col)))
                    v.append(int(x))
            self.cache[key] = (fl, r, c, v)
        return self.cache[key]

    def discover(self, fids_a, fids_b, min_score):
        (fa, ra, ca, va), (fb, rb, cb, vb) = self.get(fids_a), self.get(fids_b)
        n = len(self.col)
        A = sparse.csr_matrix((va, (ra, ca)), shape=(len(fa), n))
        B = sparse.csr_matrix((vb, (rb, cb)), shape=(len(fb), n))
        ta = np.asarray(A.sum(axis=1)).ravel()
        tb = np.asarray(B.sum(axis=1)).ravel()
        # layer k keeps only tf >= k; pruning makes later layers nearly free
        Al, Bl = A.astype(np.float32), B.astype(np.float32)
        parts = []
        for k in range(1, int(min(A.max(), B.max())) + 1):
            for M in (Al, Bl):
                M.data[M.data < k] = 0
                M.eliminate_zeros()
            Ak = Al.copy()
            Ak.data[:] = 1
            Bk = Bl.copy()
            Bk.data[:] = 1
            part = (Ak @ Bk.T).tocoo()
            parts.append((part.row, part.col, part.data))
        # one sum of all layers; adding sparse layers one by one is the slow part
        shared = sparse.coo_matrix(
            (
                np.concatenate([p[2] for p in parts]),
                (
                    np.concatenate([p[0] for p in parts]),
                    np.concatenate([p[1] for p in parts]),
                ),
            ),
            shape=(len(fa), len(fb)),
        ).tocsr()
        s = shared.tocoo()
        score = s.data / (ta[s.row] + tb[s.col] - s.data)
        keep = score >= min_score
        return [
            (fa[i], fb[j], float(x))
            for i, j, x in zip(s.row[keep], s.col[keep], score[keep])
        ]


def timed(fn):
    t = time.perf_counter()
    r = fn()
    return r, time.perf_counter() - t


def summarize(edges):
    accepted, _, _ = greedy_match(edges)
    return len(edges), len(accepted)


def main():
    bins = load()
    names = list(bins)
    vectors = {}
    for m, v in bins.items():
        vectors.update(v)
    pairs = []
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            pairs.append((a, b, set(bins[a]), set(bins[b])))
        mv = mutate(bins[a], f"mut{i}")
        vectors.update(mv)
        pairs.append((a, f"{a}~mut", set(bins[a]), set(mv)))
    big = max(names, key=lambda m: len(bins[m]))
    variants = []
    for i in range(10):
        mv = mutate(bins[big], f"var{i}", frac=0.1)
        vectors.update(mv)
        variants.append((f"var{i}", set(mv)))
    for i, (a, fa) in enumerate(variants):
        for b, fb in variants[i + 1 :]:
            pairs.append((a, b, fa, fb))
    print(f"{len(names)} binaries, {len(vectors)} functions, {len(pairs)} pairs")

    sidx = SparseIndex(vectors)
    total = {"opt3b": 0.0, "base": 0.0, "opt2": 0.0, "opt3": 0.0, "opt1": 0.0}
    idx_cache = {}
    for a, b, fa, fb in pairs:
        base, t = timed(lambda: discover_edges(vectors, fa, fb, "jaccard", MIN_SCORE))
        total["base"] += t

        def run2():
            if b not in idx_cache:
                idx_cache[b] = index_for(vectors, fb)
            return discover_cached(vectors, fa, idx_cache[b], MIN_SCORE)

        e2, t = timed(run2)
        total["opt2"] += t
        e3, t = timed(lambda: discover_sparse(vectors, fa, fb, MIN_SCORE))
        total["opt3"] += t
        e3b, t = timed(lambda: sidx.discover(fa, fb, MIN_SCORE))
        total["opt3b"] += t
        e1, t = timed(
            lambda: discover_edges(vectors, fa, fb, "jaccard", MIN_SCORE, max_df=0.3)
        )
        total["opt1"] += t

        key = lambda es: sorted((x, y, round(s, 6)) for x, y, s in es)
        assert key(e2) == key(base), "opt2 mismatch"
        assert key(e3) == key(base), "opt3 mismatch"
        assert key(e3b) == key(base), "opt3b mismatch"
        nb, gb = summarize(base)
        n1, g1 = summarize(e1)
        print(
            f"{a[:6]}-{b[:6]:6} base={nb:6} edges greedy={gb:4} | "
            f"max_df=0.3: {n1:6} edges greedy={g1:4}"
        )
    for k, v in total.items():
        print(f"{k}: {v:.2f}s  ({total['base'] / v:.1f}x)")


main()
