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
tree, or six small multiples) or Split (compact tree over a real `NwayPanel`, wired by
`setTrack` / `onTrack` / `onRowHover`). With the table's `group` (the lens preset) the route
also sends the first-level `groups` that `/nway` builds over the same filtered rows (same
`group_rows` call, so same ids, meta and LLM names) and `rows.group` (each matching row's
deepest one); both pages draw them with `nway_groups.js` (`NwayGroups`: header pieces, track
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

**Default view.** With no `lview` in the URL and no remembered pick (`localStorage` `nway.lview`; the Ring's
settings are `nway.ring`; a URL param always wins) the page chooses once its doc is here, before the first
draw: `NwayRing.autoView(doc)` = Ring with at least `RING.AUTO.FAMILIES` (3) families, or `FILES` (30)
files in at least `FILES_FAMILIES` (2) families (the "Other files" family never counts), else Tree. The
automatic choice is not written to the URL or remembered. The table's Lineage button (among its controls) opens
the page in the remembered view; the page's back link always goes to the N-way table (`NwayPanel.tableUrl`).

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

**Toolbar.** The Ring controls use the lineage page's own switches: `.lin-seg` groups of buttons with
`aria-pressed` (on/off switches are two buttons, Off / On), never selects. Visible: Min size
(Off, 0.25 ... 5%), Reset layout and a "More" `.lin-btn` (`aria-expanded`). Behind "More" (a row that
wraps below): Arcs, Nested clusters, Bundle ribbons, Tier buckets, Module list (the lens chip legend, hidden by default). More
starts open when the URL carries a non-default advanced key. Defaults: Arcs = grouped, Min size 0.5%,
Nested on, Module list off; a key equal to its default stays out of the URL
(`rarc`, `rnest`, `rmin`, `rlist`, `rtier`, `rbundle`; a legacy `rfocus` is stripped too), and none of them reaches the table or API links.

**Focus by click.** A click on a cluster circle's own body (not an arc, ribbon, lane, band, fan, file
dot; a drag still drags) zooms the page's d3 camera to it and fades the other top-level clusters and
the ribbons that do not touch it (`ring-dim`, opacity .15; hover and pin still light their chain
at full strength). A breadcrumb (All > cluster > sub-cluster, `textContent`) shows the path; Esc,
the All crumb or a click on empty canvas bring back the camera stored at the *first* focus. Deeper,
in order: the focused cluster's files get labels even above `HIDE_FILES`; leaf circles above
`LEAF_MAX` are added to `S.open`; merged (twin) files are opened by the server. The server opens a
merged column when its id is in `col_open`; with an explicit list the open groups are exactly the
`path` ids on the nodes, so the view (`openNodes` in `nway_lineage_view.js`) sends the union of those
ids plus the merged node ids (a `*fit:N` wildcard would not, its budget is spent). The answer
replaces each opened node by one node per member file, with `path` extended by the opened id; the
other nodes keep their paths and relative order. The answer is merged with `setDoc(doc, lens,
{ keep: true })`: same SVG root, camera, pin, `S.open`, breadcrumb and the physics state: `sim.pts` is
keyed by cluster id and by node id (never by index), and `sim.homes` / `sim.fhomes` remember where
every cluster and file was placed, so old things do not move and new files appear (fade-in) inside
their new cluster. While it runs the focused circle pulses and the note says "opening N files...";
a failure keeps the old doc and shows the error; a response for a cluster that is no longer focused is
ignored; a double click is one request. The new `col_open` is written with `history.replaceState`.

**Tier buckets** (More row, `rtier=hide`, default Show): in the Lanes lens the server's `tier` / `fold`
groups that hold rows of their own (rows in no lane) are buckets (`b.bucket`): neutral grey (the one
exception to the lens-colour rule), fainter and thinner than lanes, drawn behind them, labelled
"<Tier>: rows in no lane" / "slim lanes folded". They take part in hover and pin. Hide removes
them like the Min size gate (never merged into "other") and the canvas note counts them. The switch is
disabled on lenses without tier groups.

**Bundle ribbons** (More row, `rbundle=0` for Off, default On, `RING.BUNDLE_DEFAULT`): the outer ribbons of one cluster pair are drawn
as one bundle: each module's stripe leaves its slot, joins a straight parallel run on the axis between
the two centres (`min(0.25 * distance, 80)` px beyond each ring), and diverges to its slot on the other
ring; ends are arc-exact and equal; stripe order is by lateral slot position. A stripe is still the
module's `o:` element (same id and `data-m`); one drag handle per pair (`rb:X:Y`); the tooltip adds
"X + Y: N modules, M shared functions". Pairs too close for a bundle fall back to plain ribbons.
Stripes of a pair can still cross where the two rings order their slots differently (see the numbers
in the commit message).

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
