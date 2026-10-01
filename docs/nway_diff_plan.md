# N-way file diff — plan

Status: design agreed (grilling session, 2026-10-01). Nothing built yet.

## Goal

Generalise the pairwise file diff to N files. For a set of files, show:

- **Core**: functions present in every column.
- **Partial**: functions present in at least k columns (k is a runtime slider).
- **Unique**: functions present in one file only, listed per file.

The binary-cluster "common functions / unique in cluster" view is one entry point
into this feature. The other is an arbitrary multi-selection of files, clustered or
not, from any collections.

## Decisions

### Scope

- Binary clusters, file node type, collection and pool namespaces.
- Function clusters are out of scope. Container clusters are out of scope as cluster
  entry points, but a container selected in an ad-hoc set is expanded into its leaf
  children (see Members).

### Matching model: N-way constrained greedy

- Collect function edges `(fid_a, fid_b, score)` for every member pair.
- Drop edges below `min_edge`.
- Sort by score descending, then by fid for deterministic ties.
- Union-find over functions. Refuse a merge that would put two functions from the
  same file in one group.
- Each group is a **row** with at most one function per file. A singleton row is a
  unique function.
- With N = 2 the result must equal `bin_sim_tags.greedy_match` exactly.

Transitive drift (a~b, b~c, a≁c) is accepted (single linkage) and made visible per
row instead of prevented:

- `support` = edges present / possible pairs in the row.
- `cohesion` = mean edge score in the row.

Low-support rows are flagged and filterable. If support is routinely low on real
clusters, add an average-linkage gate inside the matcher later.

### Edge sources

| Member set | Source |
|:--- |:--- |
| One collection, or one pool | Stored bin_sim pair doc, `diff.matched` (A-first). |
| Pair with no stored doc | `cached_pair_from_stored_sims` on the fly; the view shows the count of such pairs. |
| Mixed collections, no pool | Virtual mode (R1), see below. |
| Any set with `mode=virtual` | Virtual mode (R1). |

- Pool clusters read the pool's stored pair doc for every pair, including two files
  from the same origin collection. `_build_pool_bin_sim_tile` builds those pairs too
  (discovery edges only under `only_cross_collection`), so the N-way view shows
  exactly what the file diff view shows for the same pair. No special case.
- `build_bin_sim` persists a doc for every pair, so a fully built collection needs no
  fallback.
- Stored edges are already pairwise-greedy winners. Runner-up edges are lost; this is
  accepted, because a row only forms where pairwise matches agree.

### Virtual mode (R1)

Definition: the result equals what a **fresh collection containing exactly these N
files**, built with the same parameters, would produce.

Per pair:

1. Function edges: `discover_edges(vectors, A, B, algo, similarity.min_score)` for
   functions at or above `min_features`, plus FunctionID exact-hash 1.0 edges for
   functions below it.
2. `greedy_match`, then bin-level `discover_edges` at `discovery_min_score` /
   `discovery_max_df` on the leftovers.
3. The resulting edges feed the N-way matcher like stored edges do.

Parameters come from explicit query params, else from the first member collection's
locked values. Every member collection whose locks or signature mask differ produces a
warning on the report, never a silent zero (same rule as scan).

Known ceilings, to be marked with `ponytail:` comments:

- `top_k` is ignored. It does not bind at N ≤ 12 files.
- `minhash_lsh` is not supported by `discover_edges`, so it is refused with a 400.

Nothing is written to Kvrocks in any mode.

### Members

- A member is `(coll, md5)`. The same md5 in two collections is two columns.
- Exact duplicate `(coll, md5)` entries are de-duplicated silently.
- A container md5 is expanded into its leaf children (`lineage_service`), labelled
  `container › child`. The expansion counts against `max_members`.

### Columns

- **Ad-hoc set**: one column per file.
- **Cluster**: adaptive, switchable at runtime.
  - File columns when the node has no children or ≤ 12 direct files.
  - Otherwise one column per child cluster, plus one "direct files" pseudo-column
    (`direct_members` minus the children's members).
  - A child column counts as present when ≥ x% of the child's files hold a function
    in the row. x is a runtime control.
- k counts columns, not files.
- Each row carries a presence mask over the columns. Grouping rows by mask answers
  "which functions does child A use that child B does not" (for example two versions
  inside one large cluster).

### Caps

Config keys in `bsimvis_config.toml`:

- `nway.max_members` (default 12, so 66 pairs).
- `nway.max_functions` (sum over members, default 60000).

Over a cap: HTTP 413 with
`{"error": "too_large", "members": n, "children": [...]}`. The UI offers the child
clusters as links. No sampling in v1: sampling changes what Unique means.

### Execution

Runtime (in the request) for now. The structure must let a worker job replace the
compute step later without touching the rest:

- `nway_diff_service.compute(r, scope, members, params) -> doc`: pure service, no
  Flask, JSON-serialisable output, pair docs streamed one at a time (only edges kept,
  never a hydrated doc).
- The route is `doc = compute(...)` followed by `page(doc, args)`. When the job lands,
  only the first half becomes "Redis cache hit, or 202 + job_id".
- Cache key, defined now: scope, cluster uuid or member hash, `bin_sim_rev`,
  `tags_rev`, `min_edge`, mode. In-process today (small, like `_DIFF_CACHE`), a TTL'd
  Redis key later.
- Pair edges are cached separately from the matcher output, so moving `min_edge`
  re-runs only the union-find.

### Library noise

- Row tags = union of member tags. A row is library when any member carries a
  library-origin tag (`is_library_tag`): stripped copies are where tags go missing.
- Code / Library / All toggle, default Code.
- Filtering and sorting reuse the bin-pair diff logic (`_page_diff` helpers), lifted
  rather than duplicated.
- Tab headers show code and library counts, so hidden rows are never invisible.

### Row presentation

- Label: the most frequent non-default name (skip `FUN_*`), else the heaviest member's
  address. "+n names" when member names disagree.
- Cells: file column = function address linking to `function_view`; child column =
  "present in m/n files".
- Metrics: `span`, `weight` (max `bsim_features_count`, as in `score_pair`),
  `support`, `cohesion`.
- Default sort: Core and Partial by `span` desc, then `weight` desc. Unique by
  `weight` desc.
- Picking two cells in a row opens the existing function diff view.

### API

`GET /api/bin_sim/nway`

- Members: `md5s=` (list of `coll:md5`, or bare md5 with `collection=`), or
  `cluster_uuid=` with `collection=` / `pool=`.
- Params: `min_edge` (default: the collection's locked `min_score`), `mode`
  (`stored` | `virtual`), `columns` (`auto` | `files` | `children`), `k`,
  `child_presence`, `tab` (`core` | `partial` | `unique`), `scope`
  (`code` | `library` | `all`), plus the bin-pair filter/sort/offset/limit params.
- Response: rows page, tab counts (code/library split), columns, fallback pair count,
  warnings.

### UI

- `nway_panel.js`: shared panel (tabs, sliders, column switch, table).
- Mounted as a Functions tab in `cluster_detail_view.js` (binary cluster, file node
  type), with deep-linkable state (`?tab=functions&min_edge=...`). Clicking a child in
  the tree re-targets the panel.
- Mounted in a standalone `/diff/nway?...` view, opened from a multi-select in the file
  tables.

### Member centrality

See slice 5b. `centrality` is persisted at cluster build time (rebuild needed);
`coverage` is derived live from the N-way rows.

### Out of v1

- CLI subcommand: none.
- LLM tool: one `nway_diff` tool in `llm_tools.py` (summary-first: tab counts and top
  Core code rows) after the worker job lands. MCP re-exports it for free.

## Slices

1. **Matcher.** Pure `nway_match(edges_by_pair, members, min_edge)` returning rows
   (mask, cells, span, weight, support, cohesion). `demo()` asserts:
   N = 2 equals `greedy_match`; one function per file under contention; drift chain
   gives one row with support < 1; deterministic ties; raising `min_edge` splits a
   bridged row.
2. **Service + API, stored path.** Single-namespace pair docs, on-the-fly fallback with
   its count, caps / 413, in-process cache, `GET /api/bin_sim/nway?md5s=` paged with
   the bin-pair filter/sort logic. Tests extend `test_api_endpoints.py` (fixture
   files, forced 413 via a low cap, paging, tabs, filters).
3. **Virtual path (R1).** Mixed sets, `mode=virtual`, container expansion,
   lock/signature warnings, `minhash_lsh` refused. Test: virtual mode on fixture files
   equals the stored path on a real collection of the same files.
4. **UI.** `nway_panel.js`, standalone `/diff/nway` view, multi-select in file tables.
4b. **Compare basket + set picker.**
   - *Basket:* right-click "Add to compare set" on file rows, kept in sessionStorage
     (`nway_basket.js`, deduped `coll:md5`), shown as a header chip with "Open N-way"
     and clear. The direct "Compare N Files" stays.
   - *Picker* on `/diff/nway`: an "Edit set" panel (always available, open when the set
     has fewer than 2 files) with removable members, a paste box (`coll:md5` or bare
     md5 + collection select), a batch UUID field, and "add from compare set".
   - *Backend:* `batch_uuid=` (+ `collection=`, else every collection) adds a batch's
     files through the transfer UI's own resolver (`_resolve_transfer_batch_sources`).
     Unknown or empty batch -> 404 `{"error":"unknown_batch"}`. Same dedupe, caps and
     container expansion; the 413 body carries a "narrow the set" message.
5. **Cluster entry.** `cluster_uuid=`, adaptive columns with the x% rule, runtime column
   switch, the tab in `cluster_detail_view.js`.
5b. **Member centrality + medoid.** Two parts, built after slice 5.
   - *Build time (user-approved DB change, needs a cluster rebuild):*
     `bin_cluster_service` already sums pair scores per node for `cohesion_score`;
     keep a per-member sum in the same pass and persist `centrality` (mean score to
     the node's other members) on each member. The medoid is the member with the
     highest centrality; store it on the cluster meta (`medoid`). No extra reads at
     request time, so it works on nodes over the N-way cap.
   - *UI:* sortable `centrality` column in the cluster member list, a main-file
     badge, and a jump button to the medoid. Existing clusters show nothing until
     rebuilt.
   - *N-way tab (free inside the cap):* a `coverage` column per file = share of the
     node's Core + Partial weight that the file holds. It measures function overlap,
     a different signal from file-score centrality; a disagreement between the two
     is informative.
6. **Later, after real use.** Worker job + Redis cache, `nway_diff` LLM tool, optional
   linkage gate, optional sampling for large clusters.

## Slice 5 as built

- `columns=auto` picks file columns when the node has no children or at most 12
  files in all (`AUTO_FILE_COLUMNS` in `routes/nway.py`), not "12 direct files": the
  matcher always runs on every file of the node, so the node's file count is what
  decides readability. With the default `nway.max_members` of 12 a node that needs
  child columns is over the cap, so child columns are reached with an explicit
  `columns=children` until the cap is raised or the job lands.
- A child column is `on` for a row when at least `child_presence` of its files hold a
  function in it. A row whose file span is above 1 but that is `on` in fewer than
  two columns appears in no tab under child columns (Core needs every column,
  Partial needs `k`). Unique is always per file (`file_span == 1`).
- The cluster resolver (`_cluster_node` in `routes/nway.py`) reads each node's
  `:members` set and its children's; the `direct` column is members minus the
  children's members. Pools whose cluster engine has no per-node member sets
  (not hierarchical) get a 400.
- Not merged: rows with the same child-column mask stay separate rows.

## Testing notes

- The `data/test/` fixture never forms binary clusters, so slices 1–4 are tested
  through `md5s=`. Slice 5 needs a check that does not depend on the fixture
  clustering (resolve members from a cluster created in the test, or test the resolver
  separately).
- `RESULT: PASS` from `wt-test.sh` is not enough: require `Failed : 0`.
