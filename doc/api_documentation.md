# BSimVis API Documentation

This document describes the BSimVis backend REST API. The API is built with Flask-RESTX; interactive Swagger UI is available at `/api/`.

All endpoints are prefixed with `/api`. Unless noted, `collection` defaults to `main`. Most read endpoints accept `offset`/`limit` pagination and a `format` (`json` or `csv`) export parameter.

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
- **Returns:** `file_count`, `function_count`, `feature_count`, `similarity_pairs`, `last_updated`.

### `GET /api/index/config`
Returns default configuration values from `bsimvis_config.toml`.

---

## Jobs

### `GET /api/jobs`
Lists recent and active background jobs.
- **Params:** `limit` (default 100), `offset`, `collection`, `pool`, `status`, `type`.

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
- **Core:** `collection` or `pool`, `q`, `file_name`, `file_md5`, `language_id`, `batch_uuid`.
- **Cluster:** `bin_cluster_uuid`, `bin_cluster_name`, `min_cohesion`/`max_cohesion`, `algo`.
- **Tags:** `tag`, `static_tag`, `user_tag`, `file_tag`, `file_static_tag`, `file_user_tag`, plus an `exclude_`-prefixed variant of each.
- **Ranges:** `min_function_count`/`max_function_count`, `min_entry_date`/`max_entry_date`, `min_file_date`/`max_file_date`.
- **Paging/sort:** `sort_by` (`entry_date` | `file_date` | `function_count`), `sort_order`, `offset`, `limit`, `format`.

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
- **Metadata:** `batch_uuid`, `batch_name` (default `Ghidra Batch`), `tags`.

### `POST /api/file/upload_file_data`
Uploads pre-analyzed JSON metadata + function feature maps from client-side extractors.
- **Body:** `collection`, `file_md5` (optional, computed if missing), `top_k`, `min_score`, `min_features`, `algo`, `skip_sim`, plus standard extractor payload (`file_name`, `functions`, `features`, `language_id`, …).

### `POST /api/file/upload_chunk`
Uploads a chunk of function analysis data (streaming path to avoid memory bloat).

### `POST /api/file/upload/batch_finalize`
Finalizes a multi-file batch upload by orchestrating a master pipeline.
- **Params:** `pipeline_ids`, `batch_uuid`, `collection`, `algo`, `skip_sim`.

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
- **Core:** `collection` or `pool`, `q`, `function_name`, `file_md5`, `file_name`, `language_id`, `namespace`, `return_type`, `entrypoint_address`.
- **Tags:** `tag`, `static_tag`, `user_tag`, and `func_`/`file_`-scoped variants, each with an `exclude_` counterpart.
- **Filters:** `min_features`, `min_cohesion`.
- **Paging/sort:** `sort_by` (`id` | `function_name` | `bsim_features_count`), `sort_order`, `offset`, `limit`, `pool_limit` (default 1000000), `format`.

### `GET /api/function/code`
Decompiler tokens and metadata for a function.
- **Params:** `id` (`idx:coll:func:md5:addr`).
- **Returns:** `rows` (line objects with tokens), `tips` (features per token), `meta`.

### `GET /api/function/diff`
Unified diff endpoint (alias of `/api/diff`). Without `addr_a`/`addr_b` returns the file-level bin_sim doc; with them, a side-by-side aligned function diff.
- **Params:** `collection_a`, `collection_b` (defaults to `collection_a`), `md5_a`, `md5_b`, `addr_a`, `addr_b`, `pool`.

### `GET /api/function/features`
Lists all BSim features for a function with their code context.
- **Params:** `id`.

---

## Features

### `GET /api/feature/search`
Searches BSim features and their frequency across a collection.
- **Params:** `collection`, `q`, `hash` (hex prefix), `type`, `op`, `sort_by` (`tf_score`/`default`), `sort_order`, `offset`, `limit`, `format`.

### `GET /api/feature/details/<f_hash>`
All function occurrences for a specific feature hash.
- **Params:** `collection`, `offset`, `limit`.

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
- **Core:** `collection` or `pool`, `algo` (default `unweighted_cosine`), `min_score` (default 0.95), `max_score` (default 1.0), `q`.
- **Metadata:** `name`, `file_name`, `md5`, `language`, `namespace`, `ret_type`, `address`.
- **Tags:** `tag`, `static_tag`, `user_tag`, plus `sim_`/`func_`/`file_`-scoped variants, each with an `exclude_` counterpart.
- **Behavior:** `cross_binary` (`true`/`false`), `match_mode` (`any`/`both`, default `any`), `min_features`, `min_cohesion`.
- **Paging/sort/cache:** `sort_by` (`score`/`feat_count`), `sort_order`, `offset`, `limit`, `use_cache` (default `false`), `format`.
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
- **Body:** `collection` (required), `md5`, `batch`, `algo` (default `unweighted_cosine`), `min_score` (default 0.95), `top_k` (default 20), `min_features` (default 0), `all` (default `false`).

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
- **Params:** `collection`, `algo`, `min_stability`, `min_count`, `min_features`, `min_cohesion`, `sort_by` (`count`/`stability`/`features`/`cohesion`), `sort_order`, `q`, `cluster_id`, `cluster_uuid`, `cluster_name`, `show_members`, `format`.

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
- **Params:** `collection` or `pool`, `algo`, `q`, `md5`, `md5_a`, `md5_b`, `file_name`, `file_tag`, `min_score`/`max_score`, `min_coverage_a`/`max_coverage_a`, `min_coverage_b`/`max_coverage_b`, `min_shared`/`max_shared`, `sort_by` (`score`/`coverage_a`/`coverage_b`/`shared_clusters`/`computed_at`), `sort_order`, `offset`, `limit`.

### `GET /api/bin_sim/diff`
Same unified diff behavior as `/api/diff`. Params: `collection_a`, `collection_b`, `md5_a`, `md5_b`, `addr_a`, `addr_b`, `algo`, `pool`.

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
Lists binary clusters. Params: `collection`, `algo`, `min_stability`, `min_count`, `min_cohesion`, `sort_by` (`count`/`stability`/`cohesion`), `sort_order`, `q`, `cluster_id`, `cluster_uuid`, `cluster_name`, `show_members`, `format`.

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
Unified diff endpoint. Without `addr_a`/`addr_b` returns the file-level bin_sim document; with them returns a side-by-side aligned function code diff.
- **Params:** `collection_a`, `collection_b` (defaults to `collection_a`), `md5_a`, `md5_b`, `addr_a`, `addr_b`, `pool`.
- **Function diff returns:** `rows` (aligned left/right), `left_tips`/`right_tips`, `meta1`/`meta2`.

---

## Tags

Tags apply to files, functions, and similarities. Each entity carries `static` (analysis-derived) and `user` tags.

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

Analyst notes on functions and files.

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

A **pool** groups multiple collections so similarity and clustering can run across their combined function/binary set. Search endpoints that accept a `pool` parameter (`file/search`, `function/search`, `similarity/search`, `bin_sim/search`, `search/autocomplete`, `diff`) target the pool instead of a single collection.

### `GET /api/pool`
Lists and searches pools.
- **Params:** `collection` (membership filter), `q`, `name`, `sync_status` (`current`/`outdated`/`created`), `sort_by` (`name`/`id`/`created_at`/`last_built_at`/`sync_status`/count fields), `sort_order`, `offset`, `limit`, `refresh_sync` (1 = recompute live status, slower), `min_created_at`/`max_created_at`, `min_last_built_at`/`max_last_built_at`.

### `POST /api/pool`
Creates a pool definition.
- **Body:** `pool_id` (optional), `name` (required), `collections` (required list), `config` (optional):
  - `only_cross_collection` (default `false`)
  - `func_sim_params`: `algo`, `top_k` (default 1000), `min_score` (default 0.3), `min_features`
  - `func_cluster_params`: `cluster_algo` (default `hdbscan`), `min_cluster_size`, `min_samples`, `epsilon`, `selection_method`
  - `file_sim_params`: `enabled` (default `true`), `min_cohesion` (default 0.5)
  - `file_cluster_params`: `min_cluster_size`, `min_samples`, `epsilon`, `selection_method`

### `GET /api/pool/<pool_id>`
Pool details.

### `PUT /api/pool/<pool_id>`
Renames a pool. Body: `name`.

### `DELETE /api/pool/<pool_id>`
Deletes a pool and all its data.

### `POST /api/pool/<pool_id>/build`
Builds/rebuilds function similarities for the pool.

### `POST /api/pool/<pool_id>/cluster`
Runs clustering for the pool.

### `POST /api/pool/<pool_id>/rebuild`
Wipes all pool data and enqueues rebuild of similarities + clusters.

### `GET /api/pool/<pool_id>/sync_check`
Checks whether the pool is outdated compared to its source collections.
