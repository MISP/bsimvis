# Plan F: overview snapshot and persistent N-way semantics

Status: design, agreed in review on 2026-11-18. Nothing is built yet.

Two problems, one theme. Each is computed at runtime, held in memory and lost on restart:

1. The collection overview (`/collections/<coll>`) is slow while a build runs.
2. Each N-way set's high-level metadata has to be rebuilt from the full row doc every time. That
   covers tiers, lane, sub-lane and region names, LLM summaries and capabilities. The row doc is
   tens to hundreds of MB and lives in a TTL'd cache.

**The fix is to store the small derived layer durably and leave the big layer as a rebuildable
cache.**

Decisions taken in review:

| Question | Decision |
|---|---|
| Overview fix | Fix the wasted reads; stored snapshot deferred (decided after the deploy) |
| Overview cache | Serve stale while one background thread refreshes; single-flight on a miss |
| Lane identity | Anchors: ~20 function ids (`md5:addr`), matched at >= 50% overlap |
| Name scope | Per collection (per pool for pools), user renames included |
| Storage | Kvrocks, new keys (explicitly approved schema change) |
| Stored record | Sub-lanes + regions by default; per-row index behind a config flag |
| Capabilities | Own entity; MBC and ATT&CK ids picked independently, plus free text; curated id list, committed catalog snapshot; capa is one evidence source among several |
| Auto-generation | TopN (activity + size, N = 50), user-opened N-way sets, changed clusters that are stored or in the TopN |

---

## PART 1: collection overview

### What is slow

I mapped one `/api/index/collection/overview` build (`routes/home.py:355`). It uses no Lua. It
does several O(files) reads that compete with the running build's writes:

- `search_files_by_cluster(per_cluster=0)` calls `sort_doc_ids(every, "file_name")` with no
  `slice_end` (`search_file_clusters.py:208`). That is an SSCAN of the file-name registry plus
  one SMEMBERS per name. With `per_cluster=0` nothing reads that order.
- `get_cluster_metas` (`home.py:375`) re-reads the meta blobs that `_load` already fetched
  (`search_file_clusters.py:198`). These blobs can be ~30 KB each.
- Recent (`home.py:417`) reads all of `all_files` and `ZRANGE entry_date 0 -1` to show 8 rows.
- Rare (`home.py:426`) reads `all_files`, HGETALLs the whole display hash
  (`cluster_display.py:221`), then walks name buckets until it finds 8 unclustered files.
- A recluster changes the display tag, so the next request runs `_build_display` inline: O(files)
  SMEMBERS plus HSET/SADD (`cluster_display.py:267`).
- The 120 s cache is per process and has no single-flight. During a build each miss starts
  several full rebuilds at once.

The page also waits for 5 parallel calls before it sends the overview call
(`collection_detail_view.js:33-39, 79`). It could send all 6 at once.

### Measured (2026-11-18, live 1.2k-file collection, build job running)

I timed each sub-call of one overview build with curl:

| Call | Time |
|---|---|
| Overview build (`refresh=true`) | 3.9 s |
| Families (`/api/file/search/by_cluster`, `per_cluster=0`) | 0.23 s |
| Rare (`unclustered=1`) | 0.31 s |
| Recent (`/api/file/search`, `sort_by=entry_date`, `limit=8`) | 3.25 s |
| Same, `limit=1` | 3.03 s |
| Same, `sort_by=file_name`, `limit=1` | 0.69 s |
| Same, `sort_by=function_count`, `limit=1` | 2.99 s |

**Recent is ~80% of the overview, and the cost is the ZSET sort, not the page size.**
`sort_doc_ids` loaded the whole `{coll}:idx:file:{field}` ZSET with `ZRANGE 0 -1` and ignored
`slice_end`. The text-field path already stopped early. My guesses above about Families and Rare
were wrong at this size: both are well under a second.

Done in this branch: the ZSET path walks the ZSET in growing pages and stops once `slice_end`
ids are ordered. This fixes every ZSET-sorted file search (file table pages too), not just the
card.

### Slice 1: fix the reads (no schema change)

Remaining items, all smaller than the ZSET fix. Re-measure first, and drop any item that no
longer shows up.

- Skip the file-name sort when `per_cluster=0`.
- Pass the already loaded metas through instead of calling `get_cluster_metas` again.
- Recent: `ZREVRANGE {coll}:idx:file:entry_date 0 7`.
- Rare: stop scanning after 8 hits from the newest end of `entry_date`, checking each file with
  `HMGET` on the display hash instead of HGETALL.
- Single-flight on the in-process cache: one builder per key, the other callers wait for its
  result.
- Send the overview request in parallel with the other 5.

Measure before and after on a test stack with a build running. Slice 2 is only worth doing if
it is still slow after this.

### Slice 2: background refresh (done), snapshot deferred

After the ZSET fix was deployed, a rebuild took 1.8 s and a cached call 0.06 s, with Recent still
1.1 s of the rebuild. A snapshot would only remove that 1.8 s, once every 2 minutes. Done
instead:

- `_cached` (`routes/home.py`) serves a stale value while one background thread rebuilds it.
  Callers arriving during a miss wait for that one build instead of each starting their own.
  Only the first request after an app restart waits.
- `bin_clusters=false` on `/api/file/search`, read in `hydrate_files` so `by_cluster` takes it
  too. The Recent and Rare cards no longer load the meta of every cluster their files sit in
  (121 metas for 8 files).

Build the snapshot below only when a rebuild passes ~3 s (re-time at 5k+ files) or the app runs
more than one process, since the in-process cache is not shared.

Deferred design:

- Key: `{coll}:overview:{axis}` (JSON, Kvrocks), with `built_at` and the display tag it was built
  from.
- `JobType.OVERVIEW_SNAPSHOT`, on `jobs:pending:high`, appended as the last task of the cluster,
  bin_cluster and upload pipelines through `splice_tasks()`. `enqueue_job`'s `queued` latch
  dedupes bursts.
- The job also runs `_build_display`, so no request ever rebuilds the display assignment inline.
- The route serves the snapshot with its age, and falls back to the live path when the snapshot
  is missing or its display tag is stale.
- Pools: same key under `global:pool:{id}`.

---

## PART 2: persistent N-way semantics

### Why nothing survives today

- `base_key` (`nway_diff_service.py:1673`) hashes the member list in caller order, plus the
  bin_sim and tags revs. Any tag write invalidates every stored build.
- Group ids are structural. `lane:{mask}` depends on column order, `region:r{n}` is a Louvain
  size rank, and `/same:0` is an index (`nway_group_service.py:198, 918`,
  `nway_region_service.py:112`).
- LLM lane names sit in `nway:lane:{sha1(prompt item)}` (30 d TTL, queue Redis,
  `routes/nway.py:1508-1574`). The key changes whenever the prompt input changes. It is also
  missing from the AGENTS.md Redis table.
- Function-cluster uuids are new on every rebuild (`cluster_service.py:687`). Bin-cluster uuids
  are stable only on the incremental UF path.

The pattern that already solves this is `save_cluster_name` / `ClusterNameKeeper`
(`cluster_utils.py:90-140`): a name anchored on ~20 member keys, matched back to a rebuilt
cluster that holds at least half of them. Lanes reuse it with function ids as anchors.

### Identity

- **Set:** `set_hash = md5(scope + sorted member md5s)`. It ignores order and revs.
- **Group** (lane, sub-lane, region, module, tier): ~20 anchor function ids, `md5:addr`, picked
  as the group's most-supported rows' fids, crc-sorted as in `ClusterNameKeeper`. A stored group
  matches a fresh group when >= 50% of its anchors are in the fresh group.
- **Limit:** `md5:addr` anchors only match across sets that share files. That covers nested
  parent/child clusters, ad hoc N-way sets over a cluster's files, reclusters of the same files
  and sets that gained a file. Two families with no shared file never share a name, even for
  identical libc code. BSim discovery of lanes and modules (vector-in, like scan mode) is the
  later upgrade for that case.

### Keys (Kvrocks)

| Key | Type | Holds |
|---|---|---|
| `{coll}:nway_sem:set:{set_hash}` | String (JSON) | Members (sorted md5s), build params (`pair_k`, `min_edge`, lens), `built_at`, tier counts, groups (see below) |
| `{coll}:nway_sem:sets` | ZSET | `set_hash` -> `built_at` |
| `{coll}:nway_sem:by_cluster` | Hash | bin cluster uuid -> `set_hash`, to detect a changed cluster |
| `{coll}:nway_names` | Hash | anchor key -> `{name, source: llm\|user, user, history, anchors, prompt_v}` |
| `{coll}:capability:{uuid}` | String (JSON) | See Capabilities |
| `{coll}:capabilities` | ZSET | capability uuid -> updated time |
| `{coll}:idx:capability:label:{id}` | Set | capability uuids per ATT&CK / MBC id |
| `{coll}:nway_sem:rows:{set_hash}` | String (zlib) | Per-row index, only when `[nway_sem] store_rows = true` |

Pools use the same keys under `global:pool:{id}:`. Document all of them in
`doc/kvrocks_database_structure.md`, and add `nway:lane:{sha1}` to the AGENTS.md Redis table.

One stored group:

```json
{"kind": "lane", "parent": null, "tier": "core", "mask": "<bits over sorted members>",
 "anchors": ["<md5>:<addr>", "..."], "rows": 412, "files": 37,
 "tags": ["yara:...", "capa:..."], "caps": ["<capability uuid>"]}
```

The name is not stored on the group. It is looked up in `{coll}:nway_names` by anchors, so a
rename shows in every stored set that holds the group.

### Size

Live numbers on the main collection: 1,159 files, 498,959 functions (~430 per file), 780 file
clusters. Bin clusters nest up to 3 levels, so a function sits in ~3 sets.

| Record | Today (1.2k files) | Target (10k files, thousands of clusters) |
|---|---|---|
| Sub-lanes + regions (~140 groups per set, ~1 KB each, anchors dominate) | a few MB | ~10-50 MB |
| Per-row index, packed (4 B crc32(fid) + 2 B group id per function, plus an n/8 B mask per row), zlib | ~5-10 MB | ~40-80 MB |
| Per-row index as JSON | ~40 MB | ~300+ MB |

The rows-per-set count behind the row-index line is an assumption (~2x the functions per file).
I did not measure it, so as not to build an N-way set on the live server while a job ran. The
per-row index is also only valid for its exact member set, while the anchored groups survive a
change. Hence the default: sub-lanes + regions, with `store_rows = false`. Only ever store the
row index as one packed zlib blob per set, never as JSON.

### Write path

- **User-opened N-way:** after `group_rows` (`nway_group_service.py:1619`), write the set record
  inline. It is small, so no job is needed. Names arrive later from `_name_lane`'s thread, which
  then writes through to `{coll}:nway_names`.
- **Read before LLM:** `lane_names` (`routes/nway.py:1531`) looks up `{coll}:nway_names` by
  anchors first and only prompts for groups with no match. This is where the LLM saving comes
  from.
- **Rename:** `POST /api/nway/group/rename` `{collection, anchors, name}` -> `{coll}:nway_names`
  with WATCH/MULTI and history, as `note_service.set_user_name` does. A user name always beats an
  LLM name, and a re-run of the LLM never overwrites it.
- Writes go to `{coll}:nway_names` and `nway_sem:*` only. They never touch function tags, so
  `tags_rev` and the cached N-way builds stay valid.

### Capabilities

A capability is its own entity, a named set of functions per collection:

```json
{"uuid": "...", "mbc": ["<B or C id>"], "attack": ["T1498"],
 "text": "free-text analyst or LLM description",
 "anchors": ["<md5>:<addr>", "..."],
 "evidence": {"tags": ["yara:...", "capa:..."], "strings": ["..."], "calls": ["..."]},
 "groups": ["<anchor key of a lane or region>"], "source": "llm|user"}
```

- Vocabulary: MBC and MITRE ATT&CK ids, both stored and picked independently, plus free text for
  deeper analysis. A capability JSON carries `mbc: [...]` and `attack: [...]` instead of one
  `ids` list. capa is evidence, not the vocabulary: it covers too few architectures to rely on.
- Level follows group size. Lanes and regions get MBC behaviours (`B*`) and ATT&CK techniques.
  Sub-lanes and small groups get MBC micro-behaviours (`C*`, e.g. socket communication, crypto
  primitives). A capability keeps both, and its `C*` ids roll up to the behaviours of the lane
  that holds it. The cut is a config value: `[capabilities] micro_below` (rows), default open.
- One capability can span several lanes, and one lane can hold several capabilities.
- Evidence comes from what `_lane_evidence` (`routes/nway.py:1329`) already gathers: yara and
  capa tags, strings, calls, code snippets. No new analysis pass.
- Generation: the same LLM call that names the lane (prompt `v:11`) also returns MBC ids, ATT&CK
  ids and a sentence. The prompt lists only the curated ids for that group's level. Any id
  outside the curated list is dropped, not stored.
- Dedupe: same id + >= 50% anchor overlap -> one capability with both groups linked.
- Search: `{coll}:idx:capability:label:{id}` for MBC and ATT&CK ids alike. Later, the
  `unified_search` palette adds it by calling its route (`_call`), as the homepage does.

Vocabulary, decided 2026-11-18:

- **Curated list.** `[capabilities]` in `bsimvis_config.toml` holds ~40-60 ids only:
  `behaviours`, `micro_behaviours` and `attack` lists, aimed at Linux/IoT and general malware
  (scanning, brute force, DoS kinds, C2, persistence, process kill, obfuscation, crypto,
  anti-analysis). The full catalog is too wide for a local model on stripped code. Editable.
- **Catalog snapshot, committed.** `data/capabilities/` holds a trimmed JSON of the MBC and
  ATT&CK catalogs: id -> name, objective/tactic, and MBC's own ATT&CK mapping (used for display
  only, never to fill `attack`). Add a `!data/capabilities/` exception to `.gitignore`. Put a
  `VENDORED_FROM.md` beside it, as `data/yara_rules/` does: upstream repo, commit or release, date,
  the trim script, and the upstream copyright and terms notices. Check both licences before the
  first commit.
- A config id missing from the snapshot fails at startup, so the curated list and the catalog
  cannot drift silently.

Open: the actual curated ids. Draft them from the snapshot once it is committed, then run them
against the lane-naming truth set (`data/lane_eval`) to check the LLM picks them reliably.

### Auto-generation

`JobType.NWAY_SEM_BUILD`, one cluster per job, normal queue, chained as a continuation so it
interleaves with other work.

Triggers:

1. **TopN:** after a recluster, the top `[nway_sem] top_n` (default 50) bin clusters by
   `home.rank_activity`, ties broken by size. This is the same ranking as the overview's
   Families card, so what the homepage shows is precomputed.
2. **User-opened N-way:** the inline write above. No job.
3. **Changed clusters:** after a recluster, a cluster whose sorted-member `set_hash` no longer
   matches `{coll}:nway_sem:by_cluster`. It is rebuilt only if it already had stored semantics
   or is in the TopN. Change detection uses the member hash, not the uuid.

Memory rules for the job:

- Build at `pair_k` = fast. Do not insert into `_DOC_CACHE`, `_MATCH_CACHE` or the `group_rows`
  cache. Drop the rows as soon as the record is written.
- A set over `nway.auto_build_max_members` goes through the existing `nway_build` path.
- LLM calls only for groups with no anchor match in `{coll}:nway_names`, so a re-run after a
  recluster costs mostly reads.

---

## Slices, in order

1. Overview read fixes plus single-flight. Measure. Done: ZSET walk, background refresh,
   `bin_clusters=false`.
2. Overview snapshot job and key: deferred, see Slice 2.
3. N-way semantic store: set record, name registry with anchor lookup, inline write on open,
   LLM write-through, read-before-LLM. Done (`nway_sem_service.py`). Anchors are a bottom-20
   crc32 sketch, matched at a sketch Jaccard >= 0.5, one stored name per group. The per-set
   `mask` over sorted members was left out: files and rows per group cover what reads it.
4. Group rename endpoint and UI (lineage pane and table group headers). Done:
   `POST /nway/group_name`, pencil in both places.
5. Capability entity: catalog snapshot in `data/capabilities/`, curated `[capabilities]` list,
   index. Done (`capability_service.py`), with two changes from the plan. The capability ids
   come from a second, short model call after the name, not from the naming prompt: that prompt
   is tuned against the lane-eval truth set, so it stays at `v:10`. MBC's own ATT&CK mapping
   was dropped: the STIX bundle holds it only as text in descriptions. The chips show on lane
   headers; there is no Inspector section yet.
6. `NWAY_SEM_BUILD` job with TopN and changed-cluster triggers. Done: after a bin clustering
   of `nway_sem.axis`, the top `nway_sem.top_n` families plus every stored set. A job skips a
   cluster whose member files equal the stored ones, and sets above
   `nway.auto_build_max_members` unless `force`. `POST /nway/semantics/build` queues by hand.
7. Optional: `store_rows` packed row index; BSim discovery anchors for naming across families.
   Not built.

Each slice gets `./scripts/wt-test.sh --only <area>` with `Failed : 0`, and API checks are added
to `test_api_endpoints.py`. The test fixture never forms clusters, so the TopN and
changed-cluster triggers need a fixture or a direct service self-check.
