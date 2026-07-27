# BSimVis API Documentation

This document describes the BSimVis backend REST API. The API is built with Flask-RESTX; interactive Swagger UI is available at `/api/`.

All endpoints are prefixed with `/api`. Unless noted, `collection` defaults to `main`; the `file`, `function`, `similarity` and `bin_sim` search endpoints instead require either `collection` or `pool` and return 400 without one. Most read endpoints accept `offset`/`limit` pagination and a `format` (`json` or `csv`) export parameter.

## Table of Contents
- [Index & Config](#index--config)
- [Jobs](#jobs)
- [Collections & Batches](#collections--batches)
- [Files](#files)
- [Functions](#functions)
- [Features](#features)
- [Feature Indexing](#feature-indexing)
- [Search Utilities](#search-utilities)
- [Similarity Engine](#similarity-engine)
- [Function Clusters](#function-clusters)
- [Binary Similarity](#binary-similarity)
- [Binary Clusters](#binary-clusters)
- [Diff](#diff)
- [Tags](#tags)
- [Notes](#notes)
- [LLM](#llm)
- [Pools (Cross-Collection)](#pools-cross-collection)

---

## Index & Config

### `GET /api/index/status`
Database index statistics and counts.
- **Params:** `collection`, `details` (`true`/`false`).
- **Returns:** `num_files`, `num_functions`, `num_indexed`, `num_missing`, `num_features`, `num_sim_meta`, `indexing_ratio`. With `details=true`, adds a `components` breakdown (key pattern, count, average size).

### `GET /api/index/config`
Returns default configuration values from `bsimvis_config.toml`.
- **Returns:** `clustering` (`epsilon`, `min_cluster_size`, `min_samples`, `selection_method`, `min_sim`, `min_features`, `min_cohesion`) and `similarity` (`top_k`, `min_score`, `min_features`, `algo`).

---

## Jobs

### `GET /api/jobs`
Lists recent and active background jobs.
- **Params:** `limit` (default 100), `offset`, `collection`, `pool` (alias `pool_id`), `status`, `type`.
- **Returns:** `items`, `total`.

### `GET /api/jobs/stats`
Aggregate metrics across all jobs (total, completed, failed, pending).

### `GET /api/jobs/<job_id>`
Detailed status and logs for a job or pipeline.

### `POST /api/jobs/<job_id>/cancel`
Cancels a pending or running job/pipeline.

### `POST /api/jobs/all/cancel`
Cancels all pending or running jobs and pipelines.

### `POST /api/jobs/<job_id>/retry`
Retries a failed or cancelled job/pipeline (pipelines reset all sub-tasks).

---

## Collections & Batches

### `GET /api/collection/search`
Lists and searches collections with filtering and CSV/JSON export.
- **Params:** `q`, `name`, `sort_by` (`name` | `total_files` | `total_functions` | `total_batches` | `last_updated`), `sort_order`, `min_files`/`max_files`, `min_functions`/`max_functions`, `min_batches`/`max_batches`, `min_last_updated`/`max_last_updated`, `offset`, `limit`, `format`.

### `POST /api/collection/delete`
Wipes and deletes a collection entirely (async background job).
- **Body:** `collection` (required).

### `POST /api/collection/clean`
Cleans up temporary raw/JSON upload keys in a collection (async background job).
- **Body:** `collection` (required).

### `GET /api/batch/search`
Lists and searches ingestion batches in a collection.
- **Params:** `collection` (required), `q`, `offset`, `limit`, `format`.

---

## Files

### `GET /api/file/search`
Searches files with rich filtering, sorting, and export. Accepts `pool` to target a cross-collection pool instead of a `collection`.
- **Core:** `collection` or `pool`, `q`, `file_name`, `file_md5` (alias `md5`), `language_id` (alias `language`), `batch_uuid`.
- **Threat-intel metadata:** `first_seen`, `last_seen`, `filetype`, `avtype`, `yara`, `cc_ip`, `file_names`, `note_owner` (alias `note_owners`), and the clustering-derived `inferred_yara`, `inferred_avtype`, `inferred_filetype`, `inferred_ccip`, `inferred_filename`, `inferred_md5`.
- **Cluster:** `bin_cluster_uuid`, `bin_cluster_name`, `min_cohesion`/`max_cohesion`, `algo`.
- **Tags:** `tag`, `static_tag`, `user_tag`, `file_tag`, `file_static_tag`, `file_user_tag`, plus an `exclude_`-prefixed variant of each. All tag filters are repeatable.
- **Ranges:** `min_function_count`/`max_function_count`, `min_bsim_features`/`max_bsim_features`, `min_entry_date`/`max_entry_date`.
- **Paging/sort:** `sort_by` (numeric: `function_count` | `bsim_features_count` | `cohesion_score` | `entry_date` | `file_date`; text: `file_name` (default) | `parent_file_name` | `related_file_name` | `language_id` | `filetype` | `avtype`), `sort_order` (default `asc`), `offset`, `limit`, `format`.

`file_md5`/`md5` and `file_name` match the file's own value *or* its `parent_md5`/`related_md5` and `parent_file_name`/`related_file_name`, so a lookup by a parent archive hash or name also returns its children.

### `GET /api/file/details/<file_md5>`
Full metadata for a file including its clusters.
- **Params:** `collection`, `algo`.

### `GET /api/file/call_graph`
Full call graph for a file.
- **Params:** `collection`, `file_md5`.

### `POST /api/file/upload`
Uploads a raw binary for server-side Ghidra analysis. Params accepted as query or form.
- **Config:** `collection`, `file_name`, `profile` (`fast`/`full`), `min_func_len` (default 10), `processor` (force Ghidra Language ID), `cspec` (force Compiler Spec ID).
- **Similarity:** `algo` (`jaccard`/`unweighted_cosine`/`milvus_sparse`), `top_k`, `min_score`, `min_features`, `skip_sim`.
- **Metadata:** `batch_uuid` (generated server-side if omitted), `batch_name` (default `Ghidra Batch`), `tags` (repeatable), `related_md5` (repeatable), `file_metadata_extra` (JSON object merged into the file document — this is how `parent_md5`, `parent_file_name` and `related_file_name` are supplied; all four parent/related fields are indexed and searchable at file, function and similarity level).
- **Scheduling:** `enqueue` (default `true`; `false` creates the pipeline without starting it, for batch uploads finalized later).
- **Returns:** `status`, `file_md5`, `pipeline_id`, `batch_uuid`.

### `POST /api/file/upload_file_data`
Uploads pre-analyzed JSON metadata + function feature maps from client-side extractors.
- **Body:** `collection`, `file_md5` (optional, computed if missing or read from `file_metadata`), `top_k`, `min_score`, `min_features`, `algo`, `skip_sim`, `skip_write`, `batch_uuid`, `enqueue`, plus the standard extractor payload. Similarity defaults fall back to `bsimvis_config.toml` (`similarity.*`) when omitted.
- `file_metadata` carries the file document: `file_name`, `language_id`, `parent_md5`, `parent_file_name`, `related_md5`, `related_file_name`, threat-intel fields (`yara`, `avtype`, `filetype`, `cc_ip`, `first_seen`, `last_seen`, `file_names`), …
- **Params:** `enqueue` (query param overrides the body; defaults to `false` when a `batch_uuid` is present, `true` otherwise).

### `POST /api/file/upload_chunk`
Uploads a chunk of function analysis data (streaming path to avoid memory bloat).

### `POST /api/file/upload/batch_finalize`
Finalizes a multi-file batch upload by orchestrating a master pipeline.
- **Body:** `pipeline_ids` (required), `batch_uuid`, `collection`, `algo`, `skip_sim`, `min_cohesion`.

### `PATCH /api/file/<file_md5>/metadata`
Partially updates metadata for a file and triggers propagation.
- **Body:** `collection`, `metadata` (dict of fields to update).

### `POST /api/file/metadata/propagate`
Bulk metadata update + propagation.
- **Body:** `collection`, `updates` (map of MD5 → metadata dict).

---

## Functions

### `GET /api/function/search`
Searches functions with rich filtering, sorting, and export. Accepts `pool` to target a cross-collection pool.
- **Core:** `collection` or `pool`, `q`, `function_name` (alias `name`), `file_md5` (alias `md5`), `file_name`, `language_id` (alias `language`), `namespace`, `return_type` (alias `ret_type`), `entrypoint_address` (alias `address`), `calling_convention`, `parameters`, `decompiler_id`, `batch_uuid`, `note_owners` (alias `note_owner`).
- **Cluster:** `cluster_id`, `cluster_uuid`, `cluster_name`, `cluster_stability`.
- **Inherited file metadata:** `first_seen`, `last_seen`, `filetype`, `avtype`, `yara`, `cc_ip`, `file_names`, `type`, `entry_date`, `file_date` — indexed at function level and filterable here.
- **Tags:** `tag`, `static_tag`, `user_tag`, and `func_`/`file_`-scoped variants, each with an `exclude_` counterpart. Repeatable.
- **Filters:** `min_features`, `min_cohesion` (default 0.95 — clusters below the threshold are dropped from the response).
- **Paging/sort:** `sort_by` (`id` (default) or a numeric function-level index: `bsim_features_count` | `instruction_count` | `entry_date` | `file_date` | `batch_order` | `cluster_stability`), `sort_order` (default `desc`), `offset`, `limit`, `pool_limit` (default 1000000), `format`.

As on `file/search`, `file_md5`/`md5` and `file_name` also match the file's `parent_md5`/`related_md5` and `parent_file_name`/`related_file_name`.

### `GET /api/function/code`
Decompiler tokens and metadata for a function.
- **Params:** `id` (`idx:coll:func:md5:addr`).
- **Returns:** `rows` (line objects with tokens), `tips` (features per token), `meta`.

### `GET /api/function/diff`
Unified diff endpoint (alias of `/api/diff`). Without `addr_a`/`addr_b` returns the file-level bin_sim doc; with them, a side-by-side aligned function diff. See [Diff](#diff) for the full parameter list.

### `GET /api/function/features`
Lists all BSim features for a function with their code context.
- **Params:** `id`.

---

## Features

### `GET /api/feature/search`
Searches BSim features and their frequency across a collection.
- **Params:** `collection`, `q`, `hash` (hex prefix), `type`, `op`, `min_frequency`/`max_frequency`, `min_tf_score`/`max_tf_score`, `sort_by` (alias `sort`, default `tf_score`), `sort_order` (alias `order`, default `desc`), `offset`, `limit` (default 20), `format`.

### `GET /api/feature/details/<f_hash>`
All function occurrences for a specific feature hash.
- **Params:** `collection`, `offset`, `limit` (default 1000).

---

## Feature Indexing

Manages the searchable global feature index (distinct from per-function feature vectors).

### `GET /api/features/status`
Feature indexing status. Params: `collection`, `details`.

### `GET /api/features/files`
Indexing status per file. Params: `collection`.

### `POST /api/features/index`
Enqueues a feature indexing job. Body: `collection` (required), `md5` or `batch`.

### `POST /api/features/clear`
Enqueues a feature clear job. Body: `collection` (required), `md5` or `batch`.

---

## Search Utilities

### `GET /api/search/autocomplete`
Autocomplete for indexed metadata field values.
- **Params:** `collection` or `pool`, `level` (`func`/`file`/`sim`), `field` (e.g. `function_name`), `q` (prefix), `limit` (default 50).

### `GET /api/search/fields`
Cardinality stats for metadata fields.
- **Params:** `collection`, `level`, `field` (list).

---

## Similarity Engine

### `GET /api/similarity/search`
Main function-level similarity search with rich filtering, cross-binary detection, caching, and export. Accepts `pool`.
- **Core:** `collection` or `pool`, `algo` (default `unweighted_cosine`), `min_score` (default from `similarity.min_score`, 0.9), `max_score` (default 1.0), `q`.
- **Metadata:** `name`, `file_name`, `md5` (repeatable), `id`, `language`, `namespace`, `ret_type`, `address`, `batch_uuid`. As on the other search endpoints, `md5` and `file_name` also match the file's parent/related md5 and file names.
- **Tags:** `tag`, `static_tag`, `user_tag`, plus `sim_`/`func_`/`file_`-scoped variants, each with an `exclude_` counterpart. Repeatable.
- **Behavior:** `cross_binary` (`true`/`false`), `match_mode` (`any`/`both`, default `any`), `min_features`, `min_cohesion` (default 0.95), `pool_limit`.
- **Paging/sort/cache:** `sort_by` (`score` (default) / `feat_count`), `sort_order` (default `desc`), `offset`, `limit`, `use_cache` (default `false`), `format`.
- **Returns:** `pairs` (with `meta1`/`meta2`), `total`, `offset`, `limit`.

### `GET /api/similarity`
Similarity score and tags for a specific function pair.
- **Params:** `id1` (required), `id2` (required).

### `GET /api/similarity/list`
Lists pre-calculated similarity results for a file or batch.
- **Params:** `collection`, `md5` (required unless `batch`), `batch` (required unless `md5`), `algo`, `limit` (default 20), `offset`.

### `GET /api/similarity/status`
Build status (total vs built) for a target.
- **Params:** `collection` (required), `md5` or `batch`, `algo`.

### `GET /api/similarity/batches`
Build status grouped by batch or file.
- **Params:** `collection` (required), `by` (`batch`/`md5`, default `batch`), `algo`.

### `POST /api/similarity/build`
Enqueues a job to pre-calculate similarity pairs.
- **Body:** `collection` (required), `md5`, `batch`, `algo`, `min_score`, `top_k`, `min_features`, `all` (default `false`), `skip_write`. One of `md5`, `batch` or `all` is required; omitted algo/score/top_k/min_features fall back to `bsimvis_config.toml` (`similarity.*`).

### `POST /api/similarity/rebuild`
Enqueues a clear + build pipeline. Same body as build.

### `POST /api/similarity/clear`
Enqueues a similarity clear job.
- **Body:** `collection` (required), `md5`, `batch`, `algo`.

### `POST /api/similarity/tag` / `POST /api/similarity/untag`
Adds/removes a user tag on a similarity pair.
- **Body:** `collection`, `id1`, `id2`, `algo`, `tag`.

---

## Function Clusters

HDBSCAN-based clustering of functions.

### `POST /api/cluster/build`
Enqueues a clustering job.
- **Body:** `collection`, `algo`, `min_cluster_size` (default 2), `min_samples` (default 1), `epsilon` (default 0.1), `selection_method` (default `eom`), `min_sim` (default 0.0), `min_features` (default 0).

### `POST /api/cluster/rebuild`
Clear + cluster pipeline. Same body as build.

### `POST /api/cluster/rebuild_all`
Full re-analysis pipeline: function clusters + binary similarity. Same body as build.

### `POST /api/cluster/clear`
Enqueues a cluster clear job. Body: `collection`, `algo`.

### `GET /api/cluster/list`
Lists clusters with metadata and filtering.
- **Params:** `collection` or `pool`, `algo`, `min_stability`/`max_stability`, `min_count`/`max_count`, `min_features`/`max_features`, `min_cohesion`/`max_cohesion`, `sort_by` (`count`/`stability`/`features`/`cohesion`), `sort_order`, `q`, `cluster_id`, `cluster_uuid`, `cluster_name`, `func_name`, `func_addr`, `file_name`, `show_members`, `show_parents`, `show_children`, `offset`, `limit`, `format`.
- Cluster membership excludes points shed as noise by HDBSCAN.

### `GET /api/cluster/tree`
Condensed dendrogram tree for D3 visualization. Params: `collection`, `algo`.

### `GET /api/cluster/members`
Function IDs in a cluster. Params: `collection`, `algo`, `cluster_id`, `limit`, `offset`.

### `GET /api/cluster/functions`
Sample of function metadata for a cluster UUID. Params: `collection`, `algo`, `cluster_uuid`, `limit`, `offset`.

### `POST /api/cluster/meta`
Updates cluster metadata (e.g. rename).
- **Body:** `collection`, `algo`, `cluster_id`, `cluster_name`.

---

## Binary Similarity

File-level similarity derived from shared function clusters.

### `POST /api/bin_sim/build`
Enqueues a job to build binary similarities.
- **Body:** `collection`, `algo`, `md5_a`, `md5_b`, `min_cohesion` (default 0.5).

### `POST /api/bin_sim/rebuild`
Clear + build pipeline. Same body as build.

### `POST /api/bin_sim/clear`
Clears binary similarities. Body: `collection`, `algo`, `md5`.

### `GET /api/bin_sim/list`
Pre-calculated similar binaries for a given MD5.
- **Params:** `collection`, `algo`, `md5`, `limit`, `offset`.

### `GET /api/bin_sim/search`
Searches binary similarity pairs with filtering and sorting. Accepts `pool`.
- **Core:** `collection` or `pool`, `algo` (default `unweighted_cosine`), `q`, `md5` (matches either side, and also the sides' `parent_md5`/`related_md5`), `file_name` (either side, plus parent/related file names), `arch` (architecture / language ID, either side).
- **Ranges:** `min_score`/`max_score`, `min_coverage`/`max_coverage`, `min_shared`/`max_shared`, `min_funcs`/`max_funcs` (function count).
- **Tags:** `file_tag`, `exclude_file_tag`, `exclude_file_static_tag`, `exclude_file_user_tag` (resolved through the live file tag index, so tag edits take effect without a rebuild), plus similarity-level `tag`, `exclude_tag`, `exclude_static_tag`, `exclude_user_tag` (applied on the page). All repeatable.
- **Paging/sort:** `sort_by` (alias `sort`; `score` (default), `score_sim_weighted`, `score_collection_weighted`, `coverage`, `shared_clusters`, `functions_count`, `computed_at`, `architecture`), `sort_order` (default `desc`), `offset`, `limit` (default 50).
- **Returns:** `total`, `offset`, `limit`, `results` — each pair enriched with `md5_a`/`md5_b`, `coll_a`/`coll_b`, `file_name_a`/`file_name_b`, the `file_parent_*`/`file_related_*` md5 and name fields, `file_tags_*`/`file_user_tags_*`, `architecture_*`, `functions_count_*`, `compiler_*`, `entry_date_*`, `coverage_a`/`coverage_b`, `shared_clusters`.
- Pool search uses the pool's own bin_sim index when present; otherwise it falls back to a slower full scan (run `bin_sim/reindex` with `pool_id` to build it).

### `GET /api/bin_sim/diff`
Same unified diff behavior as `/api/diff`, plus `algo`. See [Diff](#diff) for filters, `table` paging and `view=sankey`.

### `POST /api/bin_sim/reindex`
Rebuilds secondary indexes for existing binary similarity pairs (backfill).
- **Body:** `collection`, `algo`, `pool_id` (optional — index a pool to enable fast pool search).

---

## Binary Clusters

HDBSCAN-based clustering of binaries.

### `POST /api/bin_cluster/build`
Enqueues a binary clustering job.
- **Body:** `collection`, `algo`, `min_cluster_size` (default 2), `min_samples` (default 1), `epsilon` (default 0.1), `selection_method` (default `eom`), `min_sim` (default 0.0).

### `POST /api/bin_cluster/rebuild`
Clear + cluster pipeline. Same body as build.

### `POST /api/bin_cluster/clear`
Clears binary clusters. Body: `collection`, `algo`.

### `GET /api/bin_cluster/list`
Lists binary clusters. Params: `collection` or `pool`, `algo`, `min_stability`/`max_stability`, `min_count`/`max_count`, `min_cohesion`/`max_cohesion`, `sort_by` (`count`/`stability`/`cohesion`), `sort_order`, `q`, `cluster_id`, `cluster_uuid`, `cluster_name`, `file_name`, `file_md5`, `show_members`, `show_parents`, `show_children`, `offset`, `limit`, `format`.

### `GET /api/bin_cluster/tree`
Condensed tree for binary clustering. Params: `collection`, `algo`.

### `GET /api/bin_cluster/members`
File IDs in a binary cluster. Params: `collection`, `algo`, `cluster_id`, `limit`, `offset`.

### `GET /api/bin_cluster/files`
Sample of file metadata for a cluster UUID. Params: `collection`, `algo`, `cluster_uuid`, `limit`, `offset`.

### `POST /api/bin_cluster/meta`
Updates binary cluster metadata (e.g. rename).
- **Body:** `collection`, `algo`, `cluster_id`, `cluster_name`.

---

## Diff

### `GET /api/diff`
Unified diff endpoint. Without `addr_a`/`addr_b` returns the file-level bin_sim document; with them returns a side-by-side aligned function code diff. `/api/function/diff` and `/api/bin_sim/diff` are aliases of this endpoint (the latter also reads `algo`, default `unweighted_cosine`).
- **Params:** `collection_a` (alias `collection`, default `main`), `collection_b` (alias `coll_b`, defaults to `collection_a`), `md5_a`, `md5_b`, `addr_a`, `addr_b`, `pool` (alias `pool_id`). `md5A`/`md5B`/`addrA`/`addrB` and the legacy `id1`/`id2` function-ID pair are also accepted.
- **Function diff returns:** `rows` (aligned left/right), `left_tips`/`right_tips`, `meta1`/`meta2`.
- **File diff returns:** `score`, `score_sim_weighted`, `score_collection_weighted`, `file_metadata_a`/`file_metadata_b`, `functions_metadata` (per function ID), and `diff` with the `matched`, `unique_to_a` and `unique_to_b` tables. Matched/unique rows carry `cluster_uuid`, `cluster_name`, `cohesion`, `similarity`, `avg_features`, `sim_rarity`, `is_clustered`.

#### Server-side table paging (file diff)
Adding `table` returns one filtered/sorted page of a single diff table instead of the whole document.
- **`table`:** `matched` | `unique_to_a` | `unique_to_b`.
- **Filters:** `q` (function name, namespace, address, tags), `cl_q` (cluster name; unclustered rows match `unclustered`), `note_a`/`note_b`/`note` (note owner — `note_a`/`note_b` on matched rows, `note` on unique rows), `sim_min`/`sim_max`, `feat_min`/`feat_max` (average features), `rar_min`/`rar_max` (similarity rarity).
- **Sort/paging:** `sort_col` (any row field, plus `func_name` which resolves the name from metadata), `sort_dir` (`asc`/`desc`, default `desc`), `offset`, `limit` (default 100; `0` or less returns everything from `offset`).
- **Returns:** `items`, `total`, `offset`, `limit`, `table`, `functions_metadata` (page rows only), `file_metadata_a`/`file_metadata_b`.

#### Sankey projection (file diff)
`view=sankey` returns a compact projection for the Sankey visualization: `score`, `score_sim_weighted`, `score_collection_weighted`, `file_metadata_a`/`file_metadata_b`, `counts` (per table), and `sankey` with cluster fields plus inlined feature counts (`feat_a`/`feat_b` for matched rows, `feat` for unique rows) — no names, tags or notes, so large binaries stay renderable. Ignored when `table` is present.

---

## Tags

Tags apply to files, functions, and similarities. Each entity carries `static` (analysis-derived) and `user` tags. The read endpoints (`/api/tags`, `/api/tags/metadata`, `/api/tags/stats`) accept `pool` (alias `pool_id`) in place of `collection`. Tags and notes are stored on the origin collection and mirrored into every pool containing it.

### `GET /api/tags`
Global tag index (all tags with colors and priorities). Params: `collection` (required).

### `POST /api/tags/add` / `POST /api/tags/remove`
Adds/removes a tag on one entity.
- **Body:** `collection`, `entity_type` (`file`/`function`/`similarity`), `entity_id`, `tag`.

### `POST /api/tags/bulk_add` / `POST /api/tags/bulk_remove`
Same as above but `entity_ids` is a list.

### `GET /api/tags/metadata`
All tag metadata for a collection. Params: `collection`.

### `GET /api/tags/stats`
Statistics for a specific tag. Params: `collection`, `tag`.

### `POST /api/tags/color`
Sets a tag color. Body: `collection`, `tag`, `color` (e.g. `#ff0000`).

### `POST /api/tags/priority`
Sets a tag priority. Body: `collection`, `tag`, `priority` (int).

---

## Notes

Analyst notes on functions and files. The `list` endpoints accept `pool` (alias `pool_id`) in place of `collection`.

### Function notes
- `POST /api/notes/add` — Body: `collection`, `func_id`, `text`, `owner`.
- `PUT /api/notes/update` — Body: `collection`, `func_id`, `note_id`, `text`.
- `DELETE /api/notes/remove` — Body: `collection`, `func_id`, `note_id`.
- `GET /api/notes/list` — Params: `collection`, `func_id`.

### File notes
- `POST /api/notes/file/add` — Body: `collection`, `file_id`, `text`, `owner`.
- `PUT /api/notes/file/update` — Body: `collection`, `file_id`, `note_id`, `text`.
- `DELETE /api/notes/file/remove` — Body: `collection`, `file_id`, `note_id`.
- `GET /api/notes/file/list` — Params: `collection`, `file_id`.

---

## LLM

Local LLM integration (Ollama).

### `POST /api/llm/summarize`
Generates a summary for a function.
- **Body:** `func_id` (required), `prompt`, `code`, `func_name` (all optional).

### `POST /api/llm/chat`
Continues a discussion about a function.
- **Body:** `messages` (list of `{role, content}`).

### `POST /api/llm/summarize_file`
Streams a threat-intel summary for a binary using all available metadata.
- **Body:** `file_id`.

---

## Pools (Cross-Collection)

A **pool** groups multiple collections so similarity and clustering can run across their combined function/binary set. Search endpoints that accept a `pool` parameter (`file/search`, `function/search`, `similarity/search`, `bin_sim/search`, `search/autocomplete`, `cluster/list`, `bin_cluster/list`, `tags`, `notes`, `diff`) target the pool instead of a single collection. Passing both `pool` and `collection` scopes the request to that one member collection within the pool.

Clustering artifacts (`cluster_*`, `bin_cluster_*`, `inferred_*`) are namespace-local: a pool's clusters are computed from the pool's own similarity graph and are never merged in from member collections. Tags and notes are the opposite — they live on the origin collection and are mirrored into every pool that contains it.

### `GET /api/pool`
Lists and searches pools.
- **Params:** `collection` (membership filter), `q` (name / id / member collections / sync status), `name`, `id`, `sync_status` (`current`/`outdated`/`created`), `sort_by` (`name`/`id`/`created_at`/`last_built_at`/`sync_status`/count fields), `sort_order`, `offset`, `limit` (default 100), `refresh_sync` (1 = recompute live status, slower), `min_created_at`/`max_created_at`, `min_last_built_at`/`max_last_built_at`, plus `min_`/`max_` ranges on the count fields (`total_func_similarities`, `total_func_clusters`, `total_file_similarities`, `total_file_clusters`, `total_files`, `total_functions`).
- **Returns:** `pools`, `total`, `offset`, `limit`. Default order is `created_at` descending.

### `POST /api/pool`
Creates a pool definition **and** enqueues the full build pipeline (per-file similarity build → finalize → function clustering → binary similarity → binary clustering → index).
- **Body:** `pool_id` (optional, generated if missing), `name` (required), `collections` (required list), `config` (optional):
  - `only_cross_collection` (default `false`) — when true, pool similarity keeps only pairs whose two functions come from **different** member collections. Use it to identify known functions across collections (e.g. label an unknown binary against a reference corpus) instead of paying for full cross-correlation, including the within-collection pairs each collection already has.
  - `func_sim_params`: `algo` (default `unweighted_cosine`), `top_k` (default 1000), `min_score` (default 0.3), `min_features` (default 0), `skip_write` (benchmark-only: compute without persisting pairs)
  - `func_cluster_params`: `cluster_algo` (default `hdbscan`), `min_cluster_size` (default 2), `min_samples` (default 1), `epsilon` (default 0.1), `selection_method` (default `eom`)
  - `file_sim_params`: `enabled` (default `true`), `min_cohesion` (default 0.5)
  - `file_cluster_params`: `min_cluster_size` (default 2), `min_samples` (default 1), `epsilon` (default 0.1), `selection_method` (default `eom`)
  - Legacy flat fallbacks, still written and read when the structured params are absent: `algo`, `top_k`, `min_score`, `cluster_algo`, `cluster_params`, `skip_write`.
- **Returns:** `message`, `pool_id`, `job_id` (the pipeline).

### `GET /api/pool/<pool_id>`
Pool details: `name`, `collections`, `status`, `sync_status`, `created_at`, `last_built_at`, `only_cross_collection`, the four `*_params` objects, `sync_snapshots`, and the `total_*` counts.

### `PUT /api/pool/<pool_id>`
Renames a pool. Body: `name`.

### `DELETE /api/pool/<pool_id>`
Deletes a pool and all its data.

### `POST /api/pool/<pool_id>/build`
Enqueues the same build pipeline as pool creation (similarities → clusters → binary similarities → binary clusters → index) without wiping existing data. Returns `job_id`.

### `POST /api/pool/<pool_id>/cluster`
Clears the pool's function clusters, binary similarities and binary clusters, then enqueues clustering → binary similarity → binary clustering → index. Returns `job_id`.

### `POST /api/pool/<pool_id>/rebuild`
Wipes all computed pool data and enqueues the full build pipeline. Returns `message`, `pool_id`, `job_id`.

### `GET /api/pool/<pool_id>/sync_check`
Checks whether the pool is outdated compared to its source collections.
