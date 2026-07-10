# Plan: Slim bin-sim diff + move sort/filter/paginate + cluster tagging to backend read

## Context

Binary similarity view (`binary_similarity.js`) loads the **entire** diff doc for a
pair via `/api/bin_sim/diff` and does all sorting + filtering client-side
(`applyFilters` js:1023, `sortItems` js:1075). Backend fetches metadata for **every**
matched + unique function on every request (`bin_sim.py:213-249`) and ships the whole
list to the browser → payload + Redis-GET bloat on large binaries.

Two coupled problems surfaced:

1. **Sort/filter/paginate is client-side** — should move to backend read.
2. **The stored diff doc is redundant and stale-prone.** Every matched row (bin_sim_service.py:450-479)
   and unique row (508-532) persists the full cluster block (`cluster_id/uuid/name/cohesion`)
   + `sim_rarity` **and** identical `collection_rarity`, all re-derived per row from one
   `best_label`. Clusters rebuild often → these frozen columns go stale.

Three changes collapse into one coherent design.

## Why no indexes (settled)

Bin-sim diff = **one pair = one Redis key**; matched count bounded by
`min(funcs_a, funcs_b)` (hundreds–low-thousands). Sorting that in-memory list in Python
is microseconds → **no new index for sort/filter of the view.** Mirror the existing
`_pool_page` in-memory path (search_bin_sim.py:302).

Index tiers that already exist (no new one contemplated):
- **Pair-doc indexes** `{collection}:idx:bin_sim:{field}` (bin_sim_service.py:19/28) —
  back the *pair search* API; granularity = pairs. Not touched here.
- **Per-file sim-edge index** `{collection}:sim:involves:file:{md5}` (365-378) —
  per-file (O(files)), all similarity edges touching a file regardless of partner; the
  function-search index extended to sim edges. `sinter(involves_a, involves_b)` = edges
  between two files, and this is already the **input to the greedy match** (edges list
  395-412).

Because Tier-2 already gives per-file edge lookup, the matched list is fully
reconstructable at read (`sinter` → fetch edges → greedy-match). We still **store the
slim triple** `{func_a, func_b, similarity}` rather than recompute, because greedy match
is O(E log E) per request while the stored triple is tiny and skips the match entirely.
No per-pair index — that was a mistaken idea; the real index is Tier-2 and it exists.

## Change 1 — Slim the stored diff doc

`build_bin_sim` (bin_sim_service.py) persists only the **stable, expensive** parts;
drop everything cluster-derived:

```
matched:        [{func_a, func_b, similarity}]
unique_to_a/b:  [func_id, ...]
+ score_*, coverage_*, functions_count_*, shared_clusters=len(matched)   (all cluster-independent, keep)
```

Rationale: greedy bipartite match (edges.sort 415, greedy 431) + aggregate scores
(weighted by `f_features`, 481-488) never touch clusters. Cluster label/name/cohesion/
rarity are **display-only** and volatile → derive at read, never persist.

## Change 2 — Best-shared cluster, resolved server-side at read (new util)

**New behavior (applies to BOTH bin-sim matched rows AND search-similarities API):** a
similarity pair's cluster column shows a **single best-shared cluster** — the
highest-cohesion cluster in `cids_a ∩ cids_b` that passes `min_cohesion`. **No shared
cluster → empty** (user decision). This replaces the current two per-function cluster
lists.

New shared util (dedupes the 3 existing copies of "pick highest-cohesion cluster":
bin_sim_service.py:244-256, similarity_service.py:2488-2493, unique-row loops):

```python
# home: cluster helper reused by both routes (new util module, or similarity_service)
def pick_best_shared_cluster(cids_a, cids_b, cluster_meta, min_cohesion=0.0):
    shared = (set(cids_a) & set(cids_b))
    best, best_coh = None, -1.0
    for cid in shared:
        m = cluster_meta.get(cid)
        if not m or float(m.get("cohesion_score", 0.0)) < min_cohesion:
            continue
        if float(m["cohesion_score"]) > best_coh:
            best, best_coh = m, float(m["cohesion_score"])
    return best            # cluster meta dict or None

def pick_best_cluster(cids, cluster_meta, min_cohesion=0.0):  # single-fn (unique rows)
    return pick_best_shared_cluster(cids, cids, cluster_meta, min_cohesion)
```

Inputs are already persistent keys — no new index:
- fid → clusters: `{fid}:clusters` / `{fid}:cluster_scores` (pools: `{collection}:{fid}:…`)
- cluster meta: `{collection}:cluster:{algo}:{lbl}:meta` → cohesion / name / uuid / `unique_files_count`

Resolved **server-side** (not client-side): the API returns one `shared_cluster` (uuid or
null) per pair + the referenced cluster meta in the response's cluster map. Rarity
(`get_col_rarity`, bin_sim_service.py:272) derives from that same shared cluster —
naturally consistent with the displayed cluster, and available as a sort/filter key.

- **matched rows** → `pick_best_shared_cluster(clusters(func_a), clusters(func_b))`
- **unique rows** (single function, no pair) → `pick_best_cluster(clusters(func_id))` (own best)

Always live — cluster rebuilds can't stale it, since nothing cluster-derived is persisted.

**Rejected — stamp best-cluster in the cluster→sim propagation pass**
(cluster_service.py:1127-1274) or inside BUILD_SIM: the first only pays off if bin-sim
reconstructs matched from sim docs; the second is impossible (clustering runs *on* the sim
graph → clusters don't exist at sim-index time) and would need an O(edges) re-tag per
cluster rebuild. Read-time derivation avoids both.

## Change 3 — search-similarities API returns best-shared cluster

`bsimvis/app/routes/search_similarity.py` (~1112-1250): today builds `clusters1`/
`clusters2` per-function lists (1182-1183) and embeds both in `meta1`/`meta2`. Change to:
- compute `shared_cluster = pick_best_shared_cluster(s1, s2, cluster_meta_map, min_cohesion)`
  per pair (s1/s2 already fetched at 1058-1060; `min_cohesion` at 193).
- return a single `shared_cluster` uuid (or null) on the pair; keep only referenced
  clusters in `clusters_response` (1279).
- drop `meta1.clusters` / `meta2.clusters`.

Frontend `bsimvis/app/static/js/dashboard.js:2171-2214`: replace the two
`renderClusterCard(clusters1/clusters2)` cells with **one** cell rendering the single
`shared_cluster` (empty when null). `renderClusterCard` (entity_renderer.js:199) already
takes a list → pass `[shared]` or `[]`.

## Change 4 — Move bin-sim sort/filter/paginate to backend read

Goal: `/api/bin_sim/diff` returns only the current page (+ its metadata), not the fat blob.
No index — bin-sim diff = **one pair = one Redis key**, matched bounded by
`min(funcs_a, funcs_b)`; Python sort/filter/slice of that in-memory list is microseconds.
Mirror `_pool_page` (search_bin_sim.py:302). Frontend `binary_similarity.js` renders one
best-shared cluster card per matched row (Change 2), mirroring the similarity view.

## Backend read endpoint

`bsimvis/app/routes/bin_sim.py` — `get_bin_sim()` (line 107):

1. Accept query params: `table` (matched|unique_to_a|unique_to_b), `q`, sort
   (`sort_col`, `sort_dir`), range filters (`sim_min/max`, `feat_min/max`,
   `rar_min/max`), note filters (`note_a`, `note_b`, `note`), `offset`, `limit`
   (reuse `DEFAULT_LIMIT` convention). Mirror JS param names (js:996-1006) → 1:1 port.
2. Load diff doc as today (line 155). Pick the requested `table` list.
3. **Resolve best-shared cluster** per row via `pick_best_shared_cluster` (matched) /
   `pick_best_cluster` (unique), from `{fid}:clusters` + cluster meta. Needed *before*
   filter/sort when `sim_rarity`/cluster is a filter/sort key; else defer to page-only.
   Return one `shared_cluster` uuid (or null) per item + referenced cluster meta once.
4. **Filter** — port `applyFilters` (js:1023-1073): text haystack, note-owner,
   similarity / avg_features / sim_rarity ranges. Applied pre-slice.
5. **Sort** — port `sortItems` (js:1075): numeric vs `localeCompare`, `dir`, `count`.
6. **Slice** `[offset:offset+limit]`.
7. Fetch **function metadata only for the sliced page's fids** (shrink the
   bin_sim.py:213-249 loop) → return `{items, total, offset, limit, functions_metadata}`
   with cluster columns attached to each item. Keep `file_metadata_a/b` + top-level
   scores returned once.

`ponytail: derive-all-fids (cluster + metadata) only when a text/note/rarity/cluster
filter or sort is active; page-only otherwise.` Numeric similarity/feature sort+filter
use fields already inline on the item — no per-fid fetch.

**Back-compat:** existing stored docs are fat (inline cluster fields). Read path uses
the inline value if present, else derives — so old docs keep working, new/rebuilt docs
are skinny. No big-bang rebuild. `ponytail: read tolerant of fat+skinny.`

Reuse: `normalize_tags`, metadata-normalization block (bin_sim.py:220-265), the
Change-2 cluster helper, and `_pool_page` filter/sort shape as template.

## Frontend changes

`bsimvis/app/static/js/binary_similarity.js`:

1. Delete client-side `applyFilters`, `sortItems`; keep the row **renderers**
   (`buildFuncObj` etc.). Cluster cell renders the single `shared_cluster`
   (`renderClusterCard([shared])`, empty when null) — no more per-function lists.
2. `setBinSimSort` (js:981) and `binSimFilterChange` (js:991) → instead of
   re-rendering from `binSimDataCache`, issue a fetch to `/api/bin_sim/diff` with the
   current table/sort/filter/offset params, replace that table's rows with the
   response page.
3. Add pager / infinite-scroll per table using `total` + `offset` + `limit`.
   Debounce filter inputs (they already funnel through `binSimFilterChange`).
4. Cache the static parts (file_metadata, scores) from the first load; only the
   per-table item list + its metadata refresh per page request.

## Verification

1. `/launch.sh` (or existing run flow), open a bin-sim pair view with a large binary.
2. **Parity (old fat doc):** on an already-built pair (fat doc), sort each column
   asc/desc, apply text/note/similarity/feature/rarity filters → same results as the
   pre-change client-side behavior.
3. **Skinny rebuild:** rebuild one pair → stored doc no longer contains per-row
   `cluster_*`/`sim_rarity`; view renders identically (columns derived at read).
4. **Cluster liveness:** rename a cluster / recompute rarity, reload the pair (no
   rebuild) → cluster column reflects the new value (proves derivation, not staleness).
5. **Paging:** scroll/next → next page loads, `total` correct, no dupes at boundary.
6. **Payload:** Network tab — `/api/bin_sim/diff` carries only page-size items + page
   metadata, not the full list.
7. **Similarity view (Change 3):** open the similarity search; each pair shows **one**
   cluster card = the best-shared cluster; pairs whose functions share no cluster show
   **empty** (not two per-function lists). Confirm a known shared pair shows the
   highest-cohesion common cluster.
