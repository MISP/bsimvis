# AGENTS.md

## Python

Use uv run to run

## Ports
Configurable via `.env` file:
- Kvrocks : `KVROCKS_PORT` (default: 6666) -> storage of functions, binaries and similarities
- Redis : `REDIS_PORT` (default: 6379) -> Job queue, plus the TTL'd scan cache
- API : `APP_PORT` (default: 5000) -> localhost:5000/api

Hosts are also configurable via `KVROCKS_HOST`, `REDIS_HOST`, and `APP_HOST`.

## Layout

- `bsimvis/app/routes/` — endpoint implementations, imported by `bsimvis/app/swagger.py`.
  `_list_query.py` holds the shared in-memory filter/sort/paginate helpers for the
  small registry listings (collections, pools).
- `bsimvis/app/services/` — all logic. `similarity_service.py` and `bin_sim_service.py`
  build similarities, `pool_service.py` owns pools, `job_service.py` owns the queue,
  `config_service.py` reads `bsimvis_config.toml`, `ghidra_service.py` owns the JVM.
- `bsimvis/app/lua/` — only `search_function.lua`, `search_similarity.lua` and
  `clear_similarity.lua`. Nothing else should become Lua: Kvrocks serializes every
  `EVAL` under one global lock, so a Lua build step blocks all other workers.
  Similarity candidate discovery is deliberately pure Python for that reason.
- `bsimvis/worker.py` — the queue worker. One process per worker, each with its own
  in-process Ghidra JVM.
- `bsimvis/cli/` — subcommands wired in `bsimvis/cli/main.py` (`bsimvis <sub>`).
  `bsimvis_bench.py` is a separate entry point (`bsimvis-bench`), not a subcommand.

## Jobs

`JobType` in `job_service.py` is the single source of truth for job types — add there,
never hardcode a string. Pipelines and groups are jobs too (`type` = `pipeline` /
`group`, children in `task_ids`).

- `enqueue_job` is idempotent via a `queued` latch field; do not bypass it.
- A claimed job holds a **lease** (`jobs:leased` ZSET, `LEASE_TTL` seconds), refreshed
  by a worker heartbeat thread. A worker that dies by SIGKILL/OOM stops refreshing,
  the lease expires, and `reap_expired()` requeues the job (up to `MAX_ATTEMPTS`,
  then fails it). Never sweep `jobs:processing` wholesale — that requeues jobs live
  workers are still running.
- `splice_tasks()` is the only correct way to add tasks to a running pipeline's
  `task_ids`; a plain read-modify-write loses concurrent splices.
- `jobs:paused` is the pause flag, read between claims. In-flight jobs finish.
- Chunked work re-enqueues itself as a *continuation* (`rpush` to the tail) so batches
  from different jobs interleave instead of one job starving the fleet.
- Clear jobs (`clear_sim`, `clear_features`, `clear_cluster`, `clear_bin_sim`,
  `sync_milvus`) go to `jobs:pending:high`.
- `scan` jobs go to `jobs:pending:scan`, which every worker drains first.
  `SCAN_WORKERS_COUNT` (default 1) reserved workers run `--scan-only` and claim
  nothing else, so an interactive scan never queues behind a build.

## Scan mode

`bsimvis scan` / `POST /api/scan` analyses a file and compares it to existing
collections **without writing a single Kvrocks key** (`services/scan_service.py`,
`routes/scan.py`). The analysis is cached in Redis under `scan:{id}:*`; the match is
read-only.

This works because discovery is vector-in: `SimilarityService.build_discovery_args`
turns an in-memory feature vector into the whole target side of `_discover`, which
reads only the *collection's* posting lists. Scan functions get synthetic ids
(`scan:{id}:func:{md5}:{addr}`), so they collide with nothing and self-matches survive.

The file score is the canonical one -- the same `bin_sim_tags.greedy_match` +
`score_pair` the bin_sim builder persists. Do not add a second scoring path. Cluster
membership is *simulated*: the nearest cluster comes from the matched partner, and
`would_join` is the real `clustering.uf_threshold` / `bin_uf_threshold` test.

Per-collection `min_features` / `min_score` locks are honoured by default so a scan's
number matches that collection's own UI; a caller-supplied param overrides everywhere.
A signature-mask mismatch is a warning on the report, never a silent zero.

`POST /api/scan/{id}/commit` promotes a scan into a real collection. It copies the
cached chunks into `{coll}:file:{md5}:chunk_data:{n}` and submits the same
INDEX_META -> INDEX_FUNCTIONS -> INDEX_FEATURES -> BUILD_SIM pipeline `upload_chunk`
builds, so **Ghidra never re-runs for the analysis itself**. Keep it that way. The one
exception is the tag top-up: a lean scan skipped modules whose output needs the live
program (YARA offset mapping, capa's `getFunctionContaining`), so commit queues a plain
`GHIDRA_ANALYZE` with `skip_sim` behind it. `topup=false` opts out.

An archive or packed file is unpacked in memory the way an upload is
(`_scan_tree` in `routes/scan.py` mirrors `_ingest_tree`, minus every write): one
`SCAN` job per code file inside it, then a `SCAN_CONTAINER` finalizer that rolls the
children up through `container_sim_service.aggregate` -- the same pure formula a stored
container pair uses, leaf children instead of functions. The container is never
analysed itself, so it has no chunks: commit its *children*, not the container.
`unpack=false` scans the posted bytes as they are, and `scan.max_cached_bytes` is
checked against the whole unpacked tree, not just the upload.

`POST /api/scan` takes either a raw body (one file, `file_name` in the query) or
multipart parts (`_uploads` in `routes/scan.py`): each part is queued by the same
`_queue_one`, so a batch answers with a `scans` list carrying a per-file error where
one failed. `max_cached_bytes` stays a per-file check, not a per-batch one.

Surfaces: `bsimvis scan --commit`, the `/scans` UI view (`static/js/views/scan_view.js`,
drop-zone + multi-file picker like the upload view, recent scans as a filter/sort table
whose columns are `SCAN_LIST_COLS`) and the `scan_file` tool in `llm_tools.py`, which the
MCP server re-exports for free.

## Strings

`string_service.py` owns the per-file string table (key layout in
`doc/kvrocks_database_structure.md`). `GhidraService.extract_strings` runs inside
`stream_bsim_data` and puts the table on the file meta as `strings`; the consumers
pop it -- `index_metadata` (and `_index_streamed_program`) write it, the scan cache
keeps it under `scan:{id}:strings`, and scan commit hands it back to INDEX_META.
Never leave it on the file doc. YARA hits are mapped to strings in
`GhidraAnalyzer._strings_by_offset` (same file-offset -> address walk as the function
mapping). Function search takes `strings=` through the `strings` field in
`index_config`; it is excluded from the bare `q` on purpose. Config: `[strings]` in
`bsimvis_config.toml`. Ghidra side check: `scripts/test_strings_extract.py`.

## YARA workbench

`/yara` (`static/js/views/yara_view.js`, `routes/yara_edit.py`, `services/yara_edit_service.py`)
is a live rule editor. A debounced `POST /api/yara/test` compiles the rule and scans the
stored `{coll}:file:{md5}:raw` bytes of the target sets and a capped sample of the negative
sets (`[yara_editor]` in the config), answering precision/recall, per-group and per-string
hit counts, and where hits land (file offset -> LIEF address -> stored function list / string
table; no Ghidra). Sets use the N-way column-set resolver (`nway.resolve_group`), plus
`files` and `rest`. Rules are compiled with `includes=False`.

Rule registry: `global:yara_rule:{uuid}` (+ `:v:{n}`, `:tags`) and `global:yara_rules` ZSET.
`YARA_RULE_RUN` is the chunked sweep/apply job; sweep records hits in the queue Redis
(`yara:sweep:{run}:*`, TTL) and writes nothing to Kvrocks; apply writes `yara:custom:<rule>`
file tags. rulezet-editor is AGPL and is *fetched* by `scripts/fetch_vendor_assets.sh`
(pinned sha), never committed; the view falls back to a textarea without it.

## Capabilities (experimental)

Plan G (`doc/internal/plan_g_capability_workbench.md`). A capability is analyst-owned: a
free-text name, optional MBC / ATT&CK ids, and evidence pointers (functions `md5:addr`,
an N-way group's anchors, a string, a yara / rulezet / capa rule hit). The definition is
global (`global:capability:{uuid}`), evidence per collection; the store is the "Analyst
capabilities" half of `capability_service.py`, the routes `routes/capabilities.py`
(`/api/capability/*`). Never write function tags for it: a tag write bumps `tags_rev`.
The model only *suggests* (`build_suggestions`, `capsugg:` in the queue Redis): on the
groups the analyst selects (`POST /api/capability/suggest`), and as a silent pre-cache in
`_lane_caps` (lane naming, `NWAY_SEM_BUILD`). Nothing reaches Kvrocks before Accept.
Every source ends in `nway_group_service.suggest_capabilities`, which caches the answer
and indexes it by the files of the group's anchors (`capsugg:{ns}:f:{md5}`): the tab
lists every suggestion on the cluster's files, also those whose group the stored set no
longer holds (`earlier`), so a re-save of the set or another lens never loses one, and
selecting groups never hides any.
**One build, one evidence.** Only the table's default view (`SEM_ARGS`, `_canonical` in
`routes/nway.py`) is stored as a cluster's set (`canon: true`); any other view (lens,
columns, `min_edge`, filters) only answers the stored hash. Beside the set,
`{ns}:nway_sem:ev:{hash}` keeps per group what the model reads (`_group_evidence`: the
naming payload, plus `fid_rows` / `fid_names`, each read function's N-way row as its
copies). Suggest (tab or drawer) resolves its groups onto the stored ones (`_resolve`:
name id, else anchors >= MATCH; the rest come back in `skipped`) and reads that stored
evidence through `_clean_lane`, the cleaning naming uses, so it reads exactly what the
Build's pre-cache read. The pre-cache runs only in `NWAY_SEM_BUILD` (`lane_names(...,
caps=True)`), never from an opened table. A suggestion's functions pointer lists its
functions as rows (`rows` / `row_names`, under the group's `label`). The N-way page's
Build (`NWAY_BUILD`) of a cluster's default view queues `NWAY_SEM_BUILD` after it; the
tab's "Build N-way" is that job. What runs in the background (Build queued /
running, naming, suggesting) is marked in `capsugg:{ns}:run` (`run_mark` / `run_done` /
`running`) and shown on the pane (`running` on `GET /api/capability/suggestions`).
UI: the cluster Capabilities tab (`cluster_capabilities.js`, groups | suggestions +
accepted), the shared editor and bottom dock tray (`capability_tray.js`, fed by the
right-click "Capability" entry), and the N-way table's "Select groups" mode and
suggestions drawer (`nway_panel.js`). Every surface carries the Experimental badge.

## Pools

A pool (`pool_service.py`, `routes/pools.py`) is a named union of collections with its
own similarity/cluster namespace under `global:pool:{id}`. Its config carries
`only_cross_collection` — when set, matches inside a single member collection are
dropped, keeping only cross-collection pairs. Pool builds run as a pipeline:
`init_pool_build` -> `build_pool_sim` chunks -> `finalize_pool_build`, then
`cluster_pool` / `build_pool_bin_sim` / `cluster_pool_binaries`. Tags and notes stay
written against the origin collection; only clusters are namespace-local.

## Similarity

`similarity.min_features` in `bsimvis_config.toml` is a BSim floor. Functions below it
skip BSim entirely and get a deterministic 1.0/0 match from their exact Ghidra
FunctionID hash (`{fid}:funcid` pointer, `{coll}:funcid:{hash}` buckets) — BSim is
false-positive-prone on tiny functions. Both the collection and pool build paths do this.

Below the stored edges' floor, the function view's Similar tab can discover live:
`GET /api/similarity/discover` (`SimilarityService.discover_live`, read-only,
clamped to `similarity.discovery_min_score`) and `POST /api/similarity/discover/persist`
(recomputes the score, writes through `_persist_and_index_batch`, user tag
`discovered`). Partner filters reuse `/api/function/search` via its `ids=` param
(`filter_ids_by_groups`) -- no second filter path.

Binary similarity diff tables are paged, filtered and sorted **server-side**
(`_page_diff` in `routes/bin_sim.py`). Don't reintroduce full-table client loads.

## N-way lineage

`GET /api/bin_sim/nway/lineage` builds the tree from every row and sends them as one
compact column-wise `rows` list (fid, mask over `nodes`, role, region, `match` = the
`/nway` row filter via `_filter_rows`), so the page paints any lens, filters and tracks
without moving the tree. `/nway?order=lineage` puts the file columns in that tree's DFS
order (`column_parents`, each column's default index as `base`). Both pages share one URL
state: `track` (`fn:<fid>` or `<lens>:<key>`; lane hex bits and colours are over the
default column order, `NwayColors` in `nway_colors.js`), and the lineage page keeps its
own `layout` (tree / chain / star) and `lview`: Tree, Tracks (one lens group through the
tree, or six small multiples), Ring, Timeline or Births. `table=1` (the toolbar's Table button) puts a real
`NwayPanel` under the chart in any view, wired by `setTrack` / `onTrack` / `onRowHover`; a hovered row or the
picked column is outlined on the Tree and ringed on the Ring's and Births' file dots (`markFiles`). An old
`lview=split` link opens the Tree with the table. With the table's `group` (the lens preset) the route
also sends the first-level `groups` that `/nway` builds over the same filtered rows (same
`group_rows` call, so same ids, meta and LLM names) and `rows.group` (each matching row's
deepest one; a tier or fold is a header only, never a row's group: what it holds outside its lanes keeps the
module / "Other functions" group the table put it in, `CONTAINER_KINDS` in `routes/nway.py`, so no view paints
"Core" beside its own lanes); both pages draw them with `nway_groups.js` (`NwayGroups`: header pieces, track
keys, and one lane-name cache keyed by content, so a lane is never named twice).
**Merge files** on the lineage page: `columns=files`, `twins` (Same code) or `children` (Cluster, needs
a `cluster_uuid` or a shared cluster; the same availability rule as the table's menu). `children` and
`twins` docs both go through `nway_lineage_service.group_nodes` (a node holds a row when any member file
does, `members` / `path` come from the group). Twins keep the first member's pair edges (they share
their rows); child clusters differ, so the edge between two nodes is the strongest one over all their
member pairs (most matched functions at `min_edge`), the same `PairEdges` value, and a node is named by
its cluster. `columns=by` is refused (400). The view (`cols()`) falls back to files when the server
answers 400 for children (old server, no shared cluster). Opening a cluster node uses the same
`col_open` rule as twins: the `path` group ids on the nodes plus the node to open.
Node check: `node scripts/test_nway_lineage.js`; the service self-check: `uv run python -I
bsimvis/app/services/nway_lineage_service.py`.

**Timeline (`lview=timeline`).** The Tree renderer over `order=time`: the server makes each node's parent the
most similar node dated before it (`nway_lineage_service.time_order` / `time_parents`, ties: the latest, equal dates
allowed), scored on the N-way rows the two share (`row_similarity`, Jaccard), not on pair-edge counts: pair edges are
many-to-many candidates and scored files sharing a handful of rows near 1, and a node under `TIME_FLOOR` (0.05) to every earlier one starts its own root, so the doc can be a forest
(`buildModel` walks every root). `tfield` = `first_seen` (default), `last_seen` or `entry_date`, read off the live file
metas in the route (`_lineage_times`, the cached doc has no dates; a node takes its earliest file, latest for
`last_seen`); undated nodes rank last. Files of one UTC day are a tie (one build seen on several architectures):
`time_order(columns, w)` places next the one most similar to what is already placed. Nodes get `t`, `rank`,
`time_inversion` and `time_move` (`time_moves`): the date is probably late because moving the file to the start of an
earlier day adds at least `MOVE_GAIN` (0.15) to the tree's total parent similarity (only earlier days: `first_seen` only
bounds a birth from above; O(n^2), checked against a brute-force re-score in the demo); `{before, gain}`. A child closer
than its parent is normal divergence and is never flagged. `tmove=a,b;c,d` (node id, member file id or md5) places a at
the last millisecond of the day before b's, in the answer only (`_move_times`; stored dates never change); a moved node
gets `t_moved` `{before, t0}`. The `⚠ ←` / `↺` button (`moveBtn`, right of a Timeline node, under a Births dot, in a top
layer over every ribbon) and the File inspector's Move earlier / Undo move set one move per file in the page URL's
`tmove`; a dashed arrow (`moveArrows`) runs from a hollow dot at the file's own date to where it is now; a state bar chip
clears them; stripped from table links. In `layout()`, `doc.order ===
'time'` sets `n.d` to the date rank (one column per node, `T_GAP` apart), so reuse arcs, slots and anchors are the tree's;
`timeLanes` puts a parent's first child in its lane when they share at least `LANE_KEEP` (0.5) of the bigger file
(else it starts a lane of its own) and every other child or root in the free lane nearest it; `timeTicks`
draws the date where the day changes, a dashed `+N` mark for gaps of `GAP_DAYS` (30) or more and an "undated" mark. Never
auto-collapsed, never auto-chosen; `tfield` is stripped from table links.

**Births (`lview=births`).** `static/js/nway_births.js` (`window.NwayBirths`) over the same `order=time` doc, in the
lineage canvas like the Ring (own `g`, own toolbar box, the left pane is its panel). A row is born in its holder with the
lowest date rank (`model()`, not the tree's `bornIn`; undated nodes rank last; rows failing `rows.match` are left out);
a birth is the rows born in one file, week or month (`bgrp`). Each Timeline lane (`n.lane`, split into its chains) is a
stream: height = the file's matching functions, layers = the birth they come from, oldest at the bottom. A chain that forks
off another lane is fed by fork ribbons (`bl-fork`, always drawn): each layer its first file shares with its parent flows
from the parent's band into the fork's, so inherited code (the big core lanes) visibly carries across forks. `bcol=lens` (default) splits every birth by the lens's groups and paints them in the group colour, so
all groups show at every date with no selection (no lens chips in the toolbar, as the Ring; a tracked group keeps its layers lit, the rest fade);
`bcol=birth` paints one layer per birth on an old-to-new ramp. Hovering a layer lights the same group (or birth)
in every lane. Reuse ribbons (`reuse()`, dashed violet) join a file to its nearest earlier holder in another chain for
code it holds that its parent lacks and that was not born in it (reuse, independent birth or matcher miss); at most
`RIB_MAX` (400), biggest first, with a note on how many are left out. The lineage's Reuse layer switch (in view, as in
Tree) shows or hides them (`g.bl-ribs`, `applyReuse`). A strip under the lanes counts rows born per file (the biggest is cut). Selecting a birth (`bsel`,
the cohort key) greys the others (before / after), splits it by the current lens's groups, marks `+N` where it is born and
`+N from X` (violet) where it shows up off its parent; the selection is a chip in the state bar, and the left pane stays the
group selector (Births has no card of its own). A layer click tracks that group; a pane hover lights the group's layers,
forks and ribbons (`births.hoverTrack`), a layer hover lights its lane in the pane (`ctx.onHover`).
`bsel` is lineage-only, not a track key: the table does not know it. `bgrp` / `baxis` (`rank` | `cal`) / `bsel` / `bcol` are
stripped from table and API links. The model is checked in `scripts/test_nway_lineage.js`.

**Panes (every view).** Left: a selector of the lens's groups, nothing else (`groupList` / `navRow` in
`nway_lineage.js`): a search, Core / Common / Rare sections (tiered lens) with Collapse / Expand all, one row per group
(swatch, name, holders, count; the name is `plainName`: the model's name, the label, the fallback, the group's function names, else `N-file lane`, never an empty placeholder) and its sub-groups under a chevron. A click selects the group (`NAV.focus`,
also the track) and opens it in the Inspector; a second click or Esc unselects. A group key covers its sub-groups
(`catUnder`: a lane lights its regions). Cohesion: a pane row hover lights the group in the Tree (`focusHover`), Ring
(`ring.hoverTrack`) and Births (`births.hoverTrack`); a group hovered there lights its pane rows (`paneHl`, via
`ctx.onHover`); a hover in a view lights the whole lane there, as the pane does (`graphHover` / `laneOf`: the deepest
pane group holding the hovered cat that is not a region), and the Tree fades the other node segments (`rect.lin-sg`).
A group selected on the page goes to the table with `NwayPanel.focusTrack` (rows brought in, scrolled,
pulsed); one tracked in the table selects its pane row (revealed, scrolled) and opens the Inspector. A table row click
(`onRowPick` -> `markRow`) marks its files on the chart until another row or Esc; a function picked on the chart
scrolls the table to its row (`onFn` -> `NwayPanel.showFn`, loaded rows only). No cards, tabs or function lists here: a
view never adds its own card (Births has none; Tracks' One group / Small multiples is a toolbar switch). Right: the Inspector (`inspHead`, docked in `.lin-body`,
never over the canvas): the selected group (`inspGroup`: tier, numbers, what its files are made of, its files, its
functions as one table, `fnTr`: the app's `showCodePreview` on hover and `showGraphContextMenu` on the kebab or right
click, `NAV_ROWS` at a time; In table / Call graph / Functions pivots), File / Cluster / Same-code files (`dock`), Function (`inspectFnDraw`: code, every copy with its
address), Edge and Reuse (`inspOther`), each with pivots; an edge has a file diff (`buildFileDiffUrl`). Closing it clears
the outline. Under the toolbar the state bar (`syncBar`) shows every non-default state as a chip with x (focus, tracked
function, inspector, search, sections, grouped subtrees, small multiples, reuse layer, per-lane ribbons, Ring focus /
opened / closed clusters via `ring.state` / `clear`, the selected birth and Rare lanes via `births.state` / `clear`, the
table via `opts.chips`), Back (a snapshot stack of the page's own state) and Reset view (as the page opened). One page Esc
(`onEsc`, never inside a field) closes the newest: inspector, tracked function, Ring focus, group, birth. Every tooltip
that names a set of files carries `fileMetaHtml` (architecture, format, size, batch, collection, tags; bar = share of
those files, tick = share of all). The reuse layer starts off.

**Lane tiers (Lanes lens).** `lensCats` gives every Lanes-lens group a `tier` (its `tier:*` band from the table, else its
lane's file share with the Ring's cuts: Core >= 90%, Common >= 50%, Rare) and its lane `mask`; rows in no lane are split by
their own `rows.tier` (`~core`, `~common`, `~rare`, "Core functions in no lane") instead of one grey "Other functions". Lanes
are coloured by tier hue family (`tierColor`: Core greens, Common ambers, Rare blues), Common / Rare fill with the Ring's
zebra (`tierFill`, `tierCss`). The trees stack every node Core, Common, Rare; The trees draw what the left pane shows
(`lensPaint(..., { groupOf: paneGroupOf })`, node segments and ribbons alike): a closed tier section is one striped band
per node and edge, an open one a band per lane, a lane opened with its chevron a band per sub-lane; a merged cat keeps its
cats in `members` (`catHas`), so a hover on it lights exactly its nodes. Opening or closing a section or a lane redraws
(`paneSig` / `st.paintSig`); the search does not. "Lane ribbons" (More row, Lanes lens only, not in the URL): As the pane
(default) or Every group. Births
stacks a selected birth's lane layers Core at the bottom, draws a line and a label over each tier's band, folds more
than one Rare lane into one layer (a click on it or the pane button opens them) and puts a tier rail beside each file.
A lane tooltip carries the tier badge, the files holding the lane as the table's `.nway-sq` squares in each column's colour, and a Rare / 50% / 90% scale (`tierTip`).

**Deltas in the Ring.** The Ring's Deltas lens uses the server's delta lanes (`doc.group` `delta,region`,
`lensCats(..., { server: true })`): `Added in X`, `Only in X`, `A to B`, `Kept`, ... with the table's labels, coloured by the
file they start in (`added-N`, `dropped-N`, `span-N-M` take node N's Tree colour; kept / scattered / rewritten are grey).
Tree and Tracks keep painting Deltas by the node a row is born in (`st.C` is keyed `deltas:ring` for the Ring and
rebuilt when the view changes).

**Open in table / Call graph** (the lane card): the URL carries `track`, `group` and the lineage page's `columns` /
`col_open` plus `tfocus=1` (lineage-only: stripped from table and API links). The table (`nway_panel.js`) consumes it
once (`_takePivot`, the URL loses it): `track_only=1` (default on after a pivot, in the URL otherwise off), then
`_trackFocusStep`: a load with every header (`_keep` = no page limit, groups closed) so the tracked group is known
whichever page it is on, then a load that opens only that group and its sub-groups (`_gdef = 'none'` + `_gflip`, the
table's own group open/close mechanism, all their rows), then `_trackShow`: scroll the first tracked row to the middle,
pulse the tracked rows 1.5 s (`nway-pulse`, no animation under `prefers-reduced-motion`). The Tracking chip has "Only
these" / "Show all" (`nway-only` on the table hides every row and header that is not tracked, client side) and "N of M
rows". Lane track keys (`lanes:<hex>`, `tier:*`, `delta:*`) are computed over the default column order on both pages and
were checked equal on a twins and a children table.

**Default view.** With no `lview` in the URL and no remembered pick (`localStorage` `nway.lview`; the Ring's
settings are `nway.ring`; a URL param always wins) the page chooses once its doc is here, before the first
draw: `NwayRing.autoView(doc)` = Ring with at least `RING.AUTO.FAMILIES` (3) families, or `FILES` (30)
files in at least `FILES_FAMILIES` (2) families (the "Other files" family never counts), else Tree. The
automatic choice is not written to the URL or remembered. The table's Lineage button (among its controls) opens
the page in the remembered view; the page's back link always goes to the N-way table (`NwayPanel.tableUrl`).

### N-way semantics (names that survive rebuilds)

`nway_sem_service.py`: `_annotate` gives every named group (lane, region, presence, delta, born) `anchors`, a
bottom-20 crc32 sketch of its `md5:addr` ids; `/nway` and `/nway/lineage` look them up in the collection's
`{ns}:nway_names` registry (`attach`: `name_id`, `user_name`, `stored_name`, `ns`). `lane_names` answers stored
names first (also with no LLM; `sources` = user | llm) and writes every model answer through. `POST
/nway/group_name` is the analyst's rename (the pencil from `NwayGroups.renameBtn` / `bindRename`); it never loses
to a model answer. An unfiltered lane view saves the set's summary (`set_hash` on the answer, `GET
/nway/semantics`). Never key stored semantics on `doc["key"]` or a group id: both change with every rebuild.
Plan: `doc/internal/plan_f_nway_semantics_overview.md`.

### Ring view (`lview=ring`)

`static/js/nway_ring.js` (`window.NwayRing`) draws clusters of files as circles, the modules
they hold as a ring of segments, shared modules as outer ribbons and lanes down the nested
levels. It lives in the lineage canvas (toolbar, filters, dock, tooltip, Tracks list are the
lineage's); pin = the table's `track`. The first half of the file is DOM-free
(`clusterTree`, `modulesOf`, `buildModel`, `plan`, `geometry`, `newSim`) and checked in
Node; `geometry` returns one frame as a flat element list, the renderer only diffs attributes.
Clusters come from `node.path`, else the lineage tree is cut at weak edges. Modules are the
current lens's groups. **A file holds a module when it has at least one of its functions**
(no threshold, no control). Counts come from one pass over `rows.mask` (never
`rowModel.held`): per module `rowsIn[family]` and `pair[x|y]`, the rows held by a file of
both families. A cross-family segment is sized by the rows shared with its partner, so both
ends of a ribbon are equal; a single-family module by its rows in the family. URL keys
`rnest`, `rarc`, `rmin` (lineage-only, stripped from table and API links, as is a legacy `rfocus`).

**Min size gate** (toolbar select, `rmin` in percent, default 0.5, 0 = off): the one deliberate
exception to "nothing is dropped". A module (lane) whose own rows in the lens group, before
any split between families, are fewer than `rmin`% of **all functions of the set (`R.n`)** is
left out of everything (segments, ribbons, lanes, fans, colours) and never merged into an
"other" segment. The denominator is the whole set, not a family or the module, so one gate
means the same everywhere. The canvas note and the select's title always say how many modules
are hidden and how many are shown. Scale and floor widths are computed after the gate.

**Toolbar (every view).** One pattern: the View picker (the table's pill dropdown, `NwayPanel._dd`: Tree,
Tracks, Ring, Timeline, Births, an "auto" badge while the page chose for you; arrows / Enter / Esc), the file search, zoom
(floating: - + Fit) and at most two view essentials, then one "More" `.lin-btn` (`aria-expanded`, chevron) that
opens a second row. Visible per view: the Table toggle, then Tree = Reuse layer; Tracks = nothing extra; Ring = Min size and
Reset layout. Behind More (every view): the tree's Layout and Root, Collapse deep, Expand all, 100%, Selection,
Reuse layer (Tracks), Lane ribbons (trees, Lanes lens), and for the Ring Arcs, Nested clusters, Bundle ribbons, Tier buckets, Module list
(the lens chip legend, hidden by default). More starts open when the URL carries a non-default advanced key
(`layout`, `root`, `rarc`, `rnest=0`, `rlist`, `rtier=hide`, `rbundle=0`). The on/off switches are `.lin-seg`
groups of buttons with `aria-pressed`, never selects. Defaults: Arcs = grouped, Min size 0.5%, Nested on,
Module list off; a key equal to its default stays out of the URL (`rarc`, `rnest`, `rmin`, `rlist`, `rtier`,
`rbundle`; a legacy `rfocus` is stripped too), and none of them reaches the table or API links.

**Arcs** (`rarc=partner|group|one`, default `RING.ARC_DEFAULT` = group): how many ring
segments a cross-family module gets on a circle. `partner`: one per partner family. `group`: partners
seen from the circle's home position closer than `GROUP_DEG` (40) degrees share one arc.
`one`: one arc for all. A shared arc is sized by the sum of its pair rows, its ribbons leave it as
consecutive slots ordered by partner angle (ends stay equal), and the lane chain below starts at
that arc. `ORDER_SWEEPS` (6; 0 = order by direction only): the arcs of each ring are re-ordered
by the barycentre of the arcs they connect to on the other rings. Measured on the saved real sets
(yara modules/lanes, alois): per partner has the fewest crossings (yara modules 2219 against 3325 grouped
and 4100 one-arc), grouped cuts the segments about 40-50% for about 50% more crossings, the sweeps help
grouped and one-arc by 5-35% and are neutral for per partner. Grouped is the default by the user's choice; per partner has the fewest crossings.

**Colours:** a module or lane is painted exactly with its lens group's colour (`cat.color`, the
same string the Tree uses; grey stays grey where the lens says so), never tinted by cluster; a
golden-angle hue only when the lens gives none. The cluster colour is only on the circle, its
label and the file dots. No lane shading.

**Where a module sits.** The outer family ring carries only cross-family modules (the ones with
outer ribbons). A module held inside one cluster has no ring segment: its lane sits on the lowest
cluster node that contains all its holders (the lane ring of a sub-cluster, or, when that is the
family root or nesting is off, a lane ring just inside the circle's edge, `ROOT_LANE_OFF` -6),
and its chain goes down only: lane -> lanes of the sub-clusters that hold it -> files, no band
from the outer ring. Its width is its functions in the family times `lanePx`; the ring's
px-per-function scale is computed from the cross-family arcs only, in the part of the ring not
reserved for the outward side: `OUTWARD_KEEP` (0.4) of every ring, away from the other clusters, stays
free of cross arcs so their ribbons do not wrap round the circle (0 gave 1051 crossings on yara lanes,
0.4 gives 581). The arc order keeps the better of the by-direction order and the barycentre sweeps
(by a crossing count), so sweeps never add crossings. Its tooltip says "Only in
cluster X".

**Nothing is dropped.** Every module is drawn (cross-family: ring arc, lanes at each level, files;
single-cluster: lanes inside) and every cross-family module has one outer ribbon per family pair it spans. A segment thinner than the scale gives is
drawn `FLOOR` (2 px, less on a genuinely full ring); the px-per-function scale shrinks
instead. Only `RIBBON_SAFE` (5000 ribbons) can omit ribbons, with a note in the canvas.
Constants are all in `RING`: `CUT` [0.3, 0.6, 0.85], `DEPTH` 3 (2 above 120 files),
`LEAF_MAX` 60 (larger leaves are one circle until clicked), `FAM_MAX` 24, `PX_MAX` 2.5,
`LANE_W` 0.12 (shrunk by `lanePx` to fit), `LANE_FIT` 0.85, `BUDGET`
[6000, 12000] estimated elements (above: no parent-to-lane bands; above the second:
clusters and family ribbons only; the mode is shown in the canvas note). The redraw loop
stops after 240 frames or 4 s.

## File cluster uuid history, notes and bookmarks

A file cluster's tags (bookmarks are the `bookmark` user tag) and notes live in the
`{coll}:cluster_tags` / `{coll}:cluster_notes` sidecars under `bin_cluster:{algo}:{uuid}`,
never on the meta. Cluster rows carry `cluster_key` (`{coll}:bin_cluster:{algo}:{uuid}`),
the id `/api/notes/bin_cluster/*` and the notes panel take. A score axis cluster is its own
`{algo}:{axis}` namespace whose ids overlap the overall ones, so tag calls send `axis`.

`services/cluster_history.py` keeps a uuid across rebuilds: `clear_clusters`, an incremental
retire and a full build that skipped the clear `snapshot` the clusters they delete; the next
persist (`carry_uuids`, both hierarchical and threshold_uf paths) gives a cluster with the
same member set its old uuid back and writes a history record (kind reshaped / merged /
split / dissolved, successors by anchor Jaccard) for the rest. A retired uuid that held
analyst data is proposed on its best successor (`inherit`); `POST /api/bin_cluster/history/carry`
copies tags and notes (kept on the old uuid too). The detail view shows a retired uuid's page
and the proposal banner (`renderRetired` / `loadInherited`). Names already survive by member
anchors (`ClusterNameKeeper`). MCP: `get_cluster_info` takes `cluster_uuid`, `add_cluster_note`
is in `llm_tools.WRITE_TOOLS` (MCP only; the in-app agent stays read-only).
Self-check: `uv run python -I bsimvis/app/services/cluster_history.py`; API: `test_bin_cluster_uuid_history`.

## Cluster timeline

A binary cluster's meta carries `timeline` (`cluster_utils.timeline_stats`): per member date
field (`entry_date` = upload, `first_seen` / `last_seen` = in the wild, both optional) the
first / last date, the dated member count and per-UTC-day member counts. Written by both
build paths and by metadata propagation; `BACKFILL_CLUSTER_TIMELINE` (`POST
/api/bin_cluster/timeline/backfill`, `bsimvis metadata backfill-timeline`) fills older
clusters without a rebuild. Everything relative to now (`new_members` in `active_days`,
`last_added`, ...) is `timeline_summary` on read, so `/api/bin_cluster/list` sorts
(`active`, `last_added`, `first_added`, `first_seen`, `last_seen`) and filters on it like
relevance. The collection overview ranks its Families by it (`home.rank_activity`). It is
rebuilt from today's members: merges and splits leave no history. An upload takes
`first_seen` / `last_seen` params; a re-ingest keeps the earliest / latest.

## Homepage and unified search

`routes/home.py` backs the instance-wide homepage (`/`, view key `home`,
`static/js/views/home_view.js`) and the Ctrl+K palette
(`static/js/search_palette.js`). The collections table moved to `/collections`.

Nothing here keeps its own index. `_call(fn, **args)` runs an existing route
function inside a synthetic request context, so every search reuses that route's
own `request.args` parsing — add a new entity to `unified_search` by calling its
search route, never by writing a second query path. `/api/index/home/insights`
(top tags, biggest clusters, recent batches) is the expensive half and is cached
120s in-process; `/api/index/home/stats` stays cheap and uncached.

## Workers and Ghidra memory

Each worker holds its own JVM, so heap limits multiply by worker count.
`ghidra.max_heap_mb` sets an absolute `-Xmx` per worker (keep
`max_heap_mb * WORKERS_COUNT` under ~60% of host RAM); `ghidra.max_ram_percent` is only
for the single-JVM `bsimvis upload` CLI. `ghidra.jvm_args` enables periodic G1 GC so an
idle worker gives its heap back. `launch_tmux.sh` additionally caps `WORKERS_COUNT` by
host RAM (~3 GB each) and wraps each worker in a systemd scope with
`MemoryMax=$WORKER_MEMORY_MAX`, so a runaway worker is OOM-killed and its job requeued.

## Databases tip

Never use `keys` or other commands that might freeze the Kvrocks database.
The database holds millions of similarities. 
Dont whipeout the database.
Dont change database structure unless user asks.

### Kvrocks db
Indexes, and inverse indexes are stored in [collection]:idx:[level]:[field]:[value]
Like `main_collection:idx:file:file_name:file.exe`

Registries hold all the key of indexes, for quick search : [collection]:reg:[level]:[field]
Like `main_collection:reg:file:file_name`

This doesnt apply to global indexes and registries which : 

### Redis db

Jobs, plus the scan cache (the one non-job thing that lives here, because a scan
deliberately writes nothing to Kvrocks). Everything below carries a TTL except the
job keys.

| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `job:{id}` | **Hash** | Status, payload and metadata for a job, pipeline or group. |
| `jobs:pending:scan` / `jobs:pending:high` / `jobs:pending` | **List** | Work queues, in claim order; workers pop from the tail. `job_service.PENDING_QUEUES` is the list — never name one by hand. |
| `jobs:processing` | **List** | In-flight job IDs; a worker `LMOVE`s here when it claims a job. |
| `jobs:leased` | **ZSET** | Claimed job ID -> lease expiry (unix seconds). Expired = its worker died. |
| `jobs:paused` | **String** | Present while the fleet is paused. |
| `jobs:global` / `jobs:collection:{c}` | **List** | Recent job IDs, trimmed to 1000. |
| `scan:{id}:doc` | **String** | The scan document: status, scopes, per-scope summary. |
| `scan:{id}:raw` | **String** | The scanned bytes (binary connection: `get_raw_queue_redis`). |
| `scan:{id}:meta` / `scan:{id}:chunk:{n}` | **String** | Cached Ghidra output, one chunk per 100 functions. |
| `scan:{id}:rows:{coll}:{md5}` | **String** | One scored pair's diff rows, served paged. |
| `scans:recent` | **ZSET** | Scan id -> creation time, trimmed to 1000. The listing index; the docs expire on their own. |
| `nway:progress:{pid}` / `nway:cancel:{pid}` | **String** | A running `/nway` call's progress, and its cancel flag (checked at each progress step). 120 s TTL. |
| `nway:built:base:{key}` / `nway:built:rows:{key}` | **String** | zlib-pickled base / matched rows of a set over `nway.auto_build_max_members` files, written by the `nway_build` job (or a later min_edge). 7 day TTL. Such a set is never built by a request: `/nway` answers 428 `needs_build`. |
| `nway:build_job:{key}` | **String** | That set's last `nway_build` job id, so a reload re-attaches to it. 7 day TTL. |
| `nway:depth:{set}` | **String** | The `pair_k` a large set was last asked to build at (fast / balanced / deep; 0 = every pair), so every `/nway` call without `pair_k` reads that build or re-attaches to its job. 7 day TTL. |
| `nway:titles:{sha1}` | **String** | LLM titles of an N-way row's reference blocks (JSON), 30 day TTL. Written by a background thread in the app, never by a job. |
| `nway:lane:{sha1}` | **String** | LLM name of a lane by its prompt content (JSON `{name, tags, strings, calls}`), 30 day TTL. Also written through to the Kvrocks name registry (`{ns}:nway_names`), which outlives it. |
| `nway:lane:{sha1}` | **String** | An LLM lane name and the evidence it came from, keyed by its prompt item. 30 day TTL. |
| `capsugg:{ns}:{key}` / `capsugg:{ns}:{key}:pending` | **String** | Capability suggestions of one N-way group (`key` = its name record id, else `a`+anchor hash; JSON with `v` = `CAPS_V`), and the flag while it is being asked. 30 day / 10 min TTL. |
| `capsugg:{ns}:f:{md5}` | **Set** | Suggestion cache keys whose group has an anchor in that file, so a cluster finds them by its files. 30 day TTL, refreshed on write. |
| `capsugg:{ns}:run` | **Hash** | `{what}:{key}` -> JSON `{what, label, files, cluster, job, at, ttl}`: a background step (`build`, `name`, `suggest`) the capability pane shows. Dropped on read past its own ttl (10 min, a Build 6 h). |
| `nway:colset:{sha1}` | **String** | Saved N-way compare: the members and column groups that `POST /api/bin_sim/nway/columns` resolved from file queries (JSON), read back by `col_set`. 1 h TTL. |

## Worktree testing

Never read `data/kvrocks/` or `hs_err_pid*.log` — confidential (real binary md5s /
function data). Tests use only the git-tracked `data/test/` fixtures.

In a linked worktree, run `./scripts/wt-test.sh` before committing. Do NOT commit if
it prints `RESULT: FAIL` or the run was skipped. Show the output.

- **Do NOT run the whole suite every time.** Use the `--only <substring>` filter to run only the specific test steps related to your changes (e.g., `./scripts/wt-test.sh --only pool`).
- **For UI changes only:** Do not run `wt-test.sh` (as there are no UI tests). Instead, simply run `node --check <file>` on the modified JavaScript files to verify they compile.
- **For UI changes only:** also run `node scripts/test_xss_escaping.js`. It fails if a value that came off an uploaded sample reaches `innerHTML` without `escapeHtml` / `escapeAttr`.

`bsimvis/app/static/vendor` and `bin/` are symlinks in a worktree and gitignored: never `git add -f` them. `scripts/githooks/pre-commit` (enabled via `git config core.hooksPath scripts/githooks`, done by `wt-setup.sh`) rejects them.

Three scripts, all refusing to run outside a linked worktree so they can never touch
the main stack's `.env`, ports or confidential DB:

| Script | Does |
|:--- |:--- |
| `scripts/wt-setup.sh` | Brings the isolated stack up and leaves it running. Symlinks `bin/` from the main repo (never recompiled — 1.4G of downloaded tools), writes an isolated `.env` if missing (own `PROJECT_NAME` + offset ports + fresh local data dir), aborts if any of its ports is already held, launches via `launch_tmux.sh --clear`, waits for the app port. Idempotent — tears down an existing session of its own first. |
| `scripts/wt-teardown.sh` | Shuts redis + kvrocks down and kills the worktree's tmux session. Reads the worktree's own `.env` for ports; no-ops if there is none. |
| `scripts/wt-test.sh` | `wt-setup.sh` → `test_api_endpoints.py` (with `API_URL` pointed at the worktree's app port) → `wt-teardown.sh`, then `RESULT: PASS`/`FAIL`. |

Use `wt-setup.sh` directly when you want a live stack to poke at by hand, and
`wt-teardown.sh` when done — the dashboard URL is printed as a clickable link at the
end of launch. Ports come from the worktree name, so two worktrees can run at once.

Test files live at the repo root: `test_api_endpoints.py` (the broad suite — endpoints,
pools, filtering and sorting sweeps; the old `test_pools.py` was absorbed into it),
`test_bin_sim_file_tags.py`, `test_chunk_job_priority.py`, `test_origin_collection.py`.

## Benchmarking

`uv run bsimvis-bench` runs the ingest + similarity pipeline against the git-tracked
JSON fixtures in `data/bench/` (`--dir` to override) into the `test_bench` collection.
`--bench-pools` benchmarks the pool paths instead. Save runs with
`--save data/bench_results/<name>.json` and regress against one with `--compare`;
`data/bench_results/` is gitignored, `data/bench/` is not.

## Contributions

Always minimal code change unless user asks drastic change.
Dont be destructive of features when building new.
Comments must be simple, they are only required for complex code
Use `uv run black .` to clear up python synthax.

## API Development

The backend API uses Flask-RESTX for routing and Swagger documentation.
- **Serialization**: Do not mix `jsonify()` and Flask-RESTX `api.model` marshaling. When using Flask-RESTX `@api.response`, simply return a Python dictionary (e.g., `return {"status": "success"}`) instead of `jsonify(...)` to avoid double-serialization bugs or missing headers.
- **Endpoints**: Routes are defined in `bsimvis/app/swagger.py` and import their implementation from `bsimvis/app/routes/`.
- **Validation**: Rely on Swagger doc and `@api.expect` for parameter validation and schema definition.
