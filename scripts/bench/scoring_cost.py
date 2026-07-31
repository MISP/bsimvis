#!/usr/bin/env python3
"""Cost of weighted vs unweighted scoring in the O(n^2) build-sim inner loop.

Only the per-pair work is timed. All per-function work (norms, lengths,
coefficient maps) is precomputed, because that is O(n) index-time work and
charging it to the pair loop overstates the weighted penalty several times over.

Also demonstrates the key property that makes weighting affordable: the min-tf
rule needs NO per-pair coefficient computation. When tf_a == tf_b each side's own
precomputed coefficient is already correct; when they differ the rule wants the
smaller side's coefficient, which is exactly that side's precomputed value. The
tail of this script asserts that against the reference compare().

Needs vectors dumped by:
    scripts/bench/oracle_compare.py <bin> --binary-b <bin> --dump-vectors vecs.json

Usage:
    VECS=vecs.json .venv/bin/python scripts/bench/scoring_cost.py
"""
import json, math, os, sys, time, random
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from bsimvis.app.services.bsim_weights import WeightTable

W = os.environ.get("WEIGHTS", os.path.join(
    os.environ.get("GHIDRA_INSTALL_DIR", "bin/ghidra_12.1_PUBLIC"),
    "Ghidra/Features/BSim/data/lshweights_nosize.xml"))
t = WeightTable.from_file(W)
d = json.load(open(os.environ.get("VECS", "vecs.json")))
A, B = list(d["A"].values()), list(d["B"].values())

def scale(vecs, factor, seed):
    rnd = random.Random(seed); out=[]
    for v in vecs:
        nv=dict(v)
        for _ in range(len(v)*(factor-1)):
            nv[f"{rnd.getrandbits(32):08x}"] = 1 if rnd.random()<0.85 else 2
        out.append(nv)
    return out

# ---- per-function precomputation (INDEX TIME, not counted) ----------------
def prep_unweighted(v):
    return v, math.sqrt(sum(x*x for x in v.values()))

def prep_weighted(v):
    """int-keyed tf + coeff at this function's own tf, plus length & hashcount."""
    tf = {int(h,16): int(x) for h,x in v.items()}
    co = {h: t.idfweight[t.lookup.get(h,0)] * t.tfweight[min(x,64)-1] for h,x in tf.items()}
    length = math.sqrt(sum(c*c for c in co.values()))
    return tf, co, length, sum(tf.values())

# ---- O(n^2) inner loops ---------------------------------------------------
def pair_unweighted(pa, pb):
    a,na = pa; b,nb = pb
    small,large = (a,b) if len(a)<=len(b) else (b,a)
    dot = 0.0
    for h,x in small.items():
        y = large.get(h)
        if y is not None: dot += x*y
    return dot/(na*nb) if na and nb else 0.0

def pair_weighted(pa, pb, want_sig):
    tfa,coa,la,hca = pa; tfb,cob,lb,hcb = pb
    if len(tfa) <= len(tfb): s_tf,s_co,l_tf,l_co = tfa,coa,tfb,cob
    else:                    s_tf,s_co,l_tf,l_co = tfb,cob,tfa,coa
    dot = 0.0; inter = 0
    for h,x in s_tf.items():
        y = l_tf.get(h)
        if y is None: continue
        if x == y:                 # overwhelmingly common: coeff already correct
            w = s_co[h]
        else:                      # min-tf rule needs the smaller side's coeff
            w = s_co[h] if x < y else l_co[h]
        dot += w*w
        inter += x if x < y else y
    sim = dot/(la*lb) if la>0 and lb>0 else 0.0
    if sim > 1.0: sim = 1.0
    if not want_sig: return sim
    lo,hi = (hca,hcb) if hca<hcb else (hcb,hca)
    sig = (dot - (lo-inter)*(t.probflip0 + t.probflip1/hi)
               - (hi-lo)*(t.probdiff0 + t.probdiff1/hi) + t.addend) if hi else t.addend
    return sim, sig

def bench(fn, pairs, reps=5):
    best=float("inf")
    for _ in range(reps):
        t0=time.perf_counter()
        for pa,pb in pairs: fn(pa,pb)
        best=min(best,time.perf_counter()-t0)
    return best/len(pairs)*1e6

for factor,label in ((1,"real (~20 feats)"),(6,"~120 feats"),(20,"~400 feats")):
    av,bv = (A,B) if factor==1 else (scale(A,factor,1),scale(B,factor,2))
    ua=[prep_unweighted(v) for v in av]; ub=[prep_unweighted(v) for v in bv]
    wa=[prep_weighted(v) for v in av];   wb=[prep_weighted(v) for v in bv]
    up=[(x,y) for x in ua for y in ub];  wp=[(x,y) for x in wa for y in wb]
    u  = bench(pair_unweighted, up)
    ws = bench(lambda a,b: pair_weighted(a,b,False), wp)
    wf = bench(lambda a,b: pair_weighted(a,b,True),  wp)
    print(f"\n{label}  ({len(up)} pairs, all per-function work precomputed)")
    print(f"  unweighted_cosine            : {u:7.2f} us/pair")
    print(f"  weighted, sim only           : {ws:7.2f} us/pair  ({ws/u:4.2f}x unweighted)")
    print(f"  weighted, sim + significance : {wf:7.2f} us/pair  ({wf/u:4.2f}x unweighted, "
          f"{100*(wf-ws)/ws:+.1f}% over sim-only)")

# --- correctness: optimized loop must equal the reference compare() ---------
worst_sim = worst_sig = 0.0
for x in A:
    for y in B:
        rs, rg = t.compare(x, y)
        os_, og = pair_weighted(prep_weighted(x), prep_weighted(y), True)
        worst_sim = max(worst_sim, abs(rs-os_)); worst_sig = max(worst_sig, abs(rg-og))
# force the min-tf branch: same hashes, deliberately differing tf
import copy
a = {h: 1 for h in list(A[0])[:15]}
b = {h: (7 if i%2 else 1) for i,h in enumerate(list(A[0])[:15])}
rs, rg = t.compare(a,b); os_, og = pair_weighted(prep_weighted(a), prep_weighted(b), True)
print(f"\ncorrectness vs reference compare(): max|dsim|={worst_sim:.3e} max|dsig|={worst_sig:.3e}")
print(f"  min-tf branch (differing tf): dsim={abs(rs-os_):.3e} dsig={abs(rg-og):.3e} "
      f"(sim {rs:.6f}, sig {rg:.4f})")
