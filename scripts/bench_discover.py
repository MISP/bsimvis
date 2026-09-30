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

import numpy as np
from scipy import sparse

from bsimvis.app.services import bsim_profiles, bsim_weights
from bsimvis.app.services.bin_sim_service import discover_edges

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


class SparseIndex:
    """CSR matrices per function set over one shared hash->column map."""

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

    def layered(self, A, B, layer_weight=None):
        """sum_k w_k[h] * [a>=k][b>=k], as one sparse (|A|,|B|) matrix."""
        Al, Bl = A.copy(), B.copy()
        hashes = {i: h for h, i in self.col.items()}
        parts = []
        for k in range(1, int(min(A.max(), B.max())) + 1):
            for M in (Al, Bl):
                M.data[M.data < k] = 0
                M.eliminate_zeros()
            Ak, Bk = Al.copy(), Bl.copy()
            Ak.data[:] = 1
            Bk.data[:] = 1
            if layer_weight:
                Ak.data = np.array([layer_weight(hashes[c], k) for c in Ak.indices])
            part = (Ak @ Bk.T).tocoo()
            parts.append((part.row, part.col, part.data))
        # one sum over all layers; adding sparse layers one by one is the slow part
        cat = [np.concatenate([p[i] for p in parts]) for i in range(3)]
        return sparse.coo_matrix(
            (cat[2], (cat[0], cat[1])), shape=(A.shape[0], B.shape[0])
        ).tocsr()

    def discover(self, fids_a, fids_b, algo, min_score):
        (fa, ra, ca, va), (fb, rb, cb, vb) = self.get(fids_a), self.get(fids_b)
        n = len(self.col)
        A = sparse.csr_matrix((va, (ra, ca)), shape=(len(fa), n), dtype=np.float64)
        B = sparse.csr_matrix((vb, (rb, cb)), shape=(len(fb), n), dtype=np.float64)

        def rows(M):
            return np.asarray(M.sum(axis=1)).ravel()

        if algo == "jaccard":
            ta, tb = rows(A), rows(B)
            s = self.layered(A, B).tocoo()
            score = s.data / (ta[s.row] + tb[s.col] - s.data)
        elif algo == "binary_cosine":
            Ab, Bb = A.copy(), B.copy()
            Ab.data[:] = 1
            Bb.data[:] = 1
            na, nb = rows(Ab), rows(Bb)
            s = (Ab @ Bb.T).tocoo()
            score = s.data / np.sqrt(na[s.row] * nb[s.col])
        elif algo == "unweighted_cosine":
            na = np.sqrt(rows(A.multiply(A)))
            nb = np.sqrt(rows(B.multiply(B)))
            s = (A @ B.T).tocoo()
            score = s.data / (na[s.row] * nb[s.col])
        else:
            _, prof = bsim_profiles.parse_algo(algo)
            table = bsim_weights.load(bsim_profiles.get_profile(prof).weights_path)

            def lw(h, k):
                c1 = table.coeff(h, k)
                c0 = table.coeff(h, k - 1) if k > 1 else 0.0
                return c1 * c1 - c0 * c0

            lens = lambda vs: np.array([table.stats(self.vectors[f])[0] for f in vs])
            la, lb = lens(fa), lens(fb)
            s = self.layered(A, B, lw).tocoo()
            score = np.minimum(s.data / (la[s.row] * lb[s.col]), 1.0)
        keep = (score >= min_score) & (score > 0)
        return [
            (fa[i], fb[j], float(x))
            for i, j, x in zip(s.row[keep], s.col[keep], score[keep])
        ]


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
    sidx = SparseIndex(vectors)
    for algo in ALGOS:
        base_t = sp_t = 0.0
        for (_, fa), (_, fb) in pairs:
            base, t = timed(lambda: discover_edges(vectors, fa, fb, algo, MIN_SCORE))
            base_t += t
            sp, t = timed(lambda: sidx.discover(fa, fb, algo, MIN_SCORE))
            sp_t += t
            key = lambda es: sorted((x, y, round(s, 6)) for x, y, s in es)
            assert key(sp) == key(base), f"{algo} mismatch"
        print(
            f"{algo:18} base {base_t:6.2f}s  sparse {sp_t:6.2f}s  {base_t / sp_t:.1f}x"
        )


main()
