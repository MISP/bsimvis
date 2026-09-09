# Cluster-job indexing cost: where the 28 minutes go

Reference run: job `84fade60` (`cluster_functions`, `hierarchical_uf`) on
`more_mirai_yara`, 165,247 functions / 10,933,336 sim edges.
Source: `logs/worker-3.log`, 2026-08-31 17:27:32 → 17:56:00.
Reported total: **1707.99 s**, peak RSS 3.03 GiB.

## Measured breakdown

| Phase | Wall | Share |
|---|---|---|
| Load edges from ZSET | 50 s | 3 % |
| Build single-linkage tree (306,110 rows) | 7 s | 0.4 % |
| `clear_clustering` (wipes 121,551 `cluster_uuid` buckets) | 12 s | 0.7 % |
| **Cohesion scoring, 140,863 candidate nodes** | **241 s** | **14 %** |
| **`_persist_hierarchical_clusters` (no phase logs)** | **1105 s** | **65 %** |
| **`_update_similarity_indexing`** | **291 s** | **17 %** |
| ↳ pre-fetch cluster metas | 10 s | |
| ↳ SMEMBERS on 140,863 cluster member sets | 208 s | |
| ↳ fetch cluster assignments, 165,247 funcs | 21 s | |
| ↳ involves-scan + propagate (781,049 sims indexed) | 52 s | |

82 % of the job is persistence and indexing. The actual clustering
(edges + tree) is 57 s.

## Issue 1 — cluster membership is written to Redis four times

`hierarchical_uf` promotes **every surviving tree node** to a cluster:
140,863 clusters for 165,247 functions. Only 24,384 of those are
cohesion-cut winners (a function's real "primary" cluster); the other
116,479 exist so the hierarchy view has a `:meta` doc to render.

`_persist_hierarchical_clusters` (`cluster_service.py:1664-1818`) writes each
node's **full subtree member list** four separate times:

| Key | Content |
|---|---|
| `{col}:cluster:{algo}:{c}:members` | subtree fids |
| `{col}:idx:func:cluster_id:{c}` | **identical set** |
| `{col}:idx:func:cluster_uuid:{uuid}` | **identical set** |
| `{col}:idx:func:cluster_name:{name}` | **identical set** |

Summed subtree size across the hierarchy is roughly 20 M entries (inferred
from the 241 s cohesion loop, which iterates once per member, and the 208 s
read-back of the same sets). Written 4× that is ~80 M `SADD` members against
kvrocks — which is what the unlogged 1105 s phase is doing.

The three `idx:func:cluster_*` buckets are pure duplication. Their only
consumers:

- `routes/cluster.py:868` — `/api/cluster/functions?cluster_uuid=…` reads
  `idx:func:cluster_uuid:{uuid}`. It needs a **uuid → label** lookup and
  nothing more; `:members` already holds the answer.
- `?cluster_uuid=` / `?cluster_name=` function filters (`function_filters.js:12`,
  `context_menu.js:441`) — same shape, one cluster at a time.
- `routes/cluster.py:402-430` already filters `cluster_name` **in Python over
  the cluster meta docs**, not via the index.

Worse, the `cluster_id` bucket is not even consistent with the rest of the
model: `{fid}:clusters` gets exactly one cluster per function on this path
(`member_of`), while `idx:func:cluster_id` holds the transitive closure of
every ancestor. Two different answers to "which cluster is this function in".

### Fix

Drop `cluster_id`, `cluster_uuid`, `cluster_name` from `INDEX_CONFIG["func"]`
(set to `[]` or remove). Replace the uuid lookup with one small hash written
once per job:

```python
# 140,863 short fields, one HSET — instead of 140,863 member-set copies
pipe.hset(f"{collection}:cluster:{algo}:uuid_map",
          mapping={label_to_uuid[c]: str(c) for c in write_nodes})
```

Then `cluster.py:868` becomes `HGET uuid_map <uuid>` → `SMEMBERS
{col}:cluster:{algo}:{label}:members`. Two round-trips instead of one, on a
route that serves ≤100 rows.

`cluster_name` filtering keeps the existing Python-side scan over metas.

**Estimated saving: ~750 s of the 1105 s persist phase, plus the 12 s wipe
in `clear_clustering`, plus 3× the Redis storage for cluster membership.**

## Issue 2 — the whole sim-index phase produces one thing, and it can be computed at read time

`_update_similarity_indexing` (291 s) starts by resolving which cluster fields
propagate func → sim:

```python
propagated = self.get_propagated_fields("sim")["func"]
cluster_prop = [p for p in propagated if p[0].startswith("cluster_")]
```

In `INDEX_CONFIG`, every `cluster_*` field on `func` is declared `["func"]` —
**no `"sim"` target**. So `cluster_prop` and `cluster_prop_num` are both empty,
and the `tag_buckets` / `reg_buckets` / `num_zsets` machinery in the hot loop
is dead code. The single surviving output is:

```python
best_cluster_map[sid] = best_cid      # → {col}:sim:best_cluster:{algo}
```

a hash with one field per clustered sim edge (781,049 written here; up to
10.9 M on a fully-clustered collection).

Its only reader is `routes/search_similarity.py:1152`:

```python
raw = r.hmget(f"{col}:sim:best_cluster:{bc_algo}", page_sids)
```

`page_sids` is one page — ~50 edges. And by the time that line runs, the route
has **already** loaded, in Phase 2 and Phase 4, exactly the inputs
`pick_best_shared_cluster` needs:

- `f_meta_map[fid]["scores"]` — `hgetall {fid}:cluster_scores` for both endpoints
- `cluster_meta_map` — the cluster meta docs for every cluster on the page

### Fix

Delete the build path of `_update_similarity_indexing` and compute the value
inline in the search route:

```python
# search_similarity.py, replacing the hmget block
cids1 = list(f_meta_map.get(id1, {}).get("scores", {}))
cids2 = list(f_meta_map.get(id2, {}).get("scores", {}))
best = pick_best_shared_cluster(cids1, cids2, cluster_meta_map)
shared_cid = str(best["cluster_id"]) if best else None
```

Zero extra Redis round-trips at read time — the data is already in hand. The
`min_cohesion` filter already applied to `cluster_meta_map` keeps working, and
in fact gets *more* correct: today a stale `best_cluster` hash survives until
the next full cluster job, whereas the inline version always reflects current
membership.

Keep the `is_clear` path (it still needs to delete the hash during migration),
and keep the incremental `_propagate_sim_indexes_for` only if you want the
index around for other reasons — otherwise it goes too.

**Estimated saving: the full 291 s, plus a 10.9 M-field hash removed from
kvrocks, plus the peak-RSS pressure this phase was measured to cause.**

## Issue 3 — exact cohesion over every hierarchy node (241 s)

`_run_clustering_hierarchical_uf` scores cohesion for all 140,863 candidate
nodes with `adj_sim.cohesion()`, which for `n >= 50` runs a Python loop over
every member and slices its CSR neighbour row. Total work is
`sum(subtree_size)` ≈ 20 M Python iterations, plus a fresh
`np.zeros(165247, bool)` allocation **per node**.

Cohesion drives one decision — `node_cohesion[c] >= cohesion_cut` (0.9) — and
is otherwise a display number in `:meta`.

### Fix (two independent, both cheap)

1. **Hoist the scratch buffer.** `member_set` is reallocated and zeroed
   140,863 times. Allocate once outside the loop, set the member bits, clear
   them at the end of each iteration. Pure win, no accuracy change.

2. **Sample large nodes.** Above some size the score is nowhere near the 0.9
   cut and exactness buys nothing:

   ```python
   COHESION_SAMPLE = 500   # ponytail: exact below this, sampled above.
   # Std error of the mean over sampled pairs is ~1/sqrt(k); at k=500 that is
   # well inside the margin for any node whose true cohesion isn't ~0.9.
   idx = member_indices
   if len(idx) > COHESION_SAMPLE:
       idx = random.sample(idx, COHESION_SAMPLE)   # seeded, for reproducibility
   ```

   Boundary risk is real: a node whose true cohesion sits at 0.90 ± 0.02 can
   flip the cut. Mitigate by re-scoring exactly any sampled node that lands in
   a band around the cut (e.g. 0.85–0.95) — that's a small minority of nodes,
   so the saving mostly survives.

**Estimated saving: 150–200 s of the 241 s, with a bounded and testable
accuracy cost.**

## Summary

| Change | Est. saving | Risk |
|---|---|---|
| Drop 3 duplicate `idx:func:cluster_*` bucket sets, add `uuid_map` hash | ~750 s | Low — one route to repoint |
| Delete sim-index build path, compute `best_cluster` inline in search | ~291 s | Low — inputs already loaded at read time |
| Hoist cohesion scratch buffer | ~10-20 s | None |
| Sample cohesion above 500 members, re-score near the cut | ~150 s | Medium — cut-boundary flips |

Projected: **1708 s → ~500 s** on this corpus, with Redis storage for cluster
membership cut by roughly 4× and the 10.9 M-field `best_cluster` hash gone.

Recommended order: issue 2 first (smallest diff, zero accuracy risk, and the
dead-code finding makes it self-evidently safe), then issue 1, then issue 3.

## Verification

Whichever lands, gate it on:

- `./scripts/wt-test.sh` with `Failed : 0` (RESULT: PASS alone is not enough).
- A cluster job on a corpus that actually forms clusters (the test fixture
  never does) — compare `cluster:list` cardinality, per-cluster `member_count`,
  and the `shared_clusters` field on a page of `/api/search/similarity` before
  and after.
