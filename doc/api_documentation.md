# BSimVis API Documentation

This document describes the primary API endpoints for the BSimVis backend. The API is built using Flask-RESTX, and interactive Swagger documentation is available at `/api/`.

## Function Data APIs

### Function Code
**`GET /api/function/code`**
Retrieves decompiled code and semantic tokens for a function.
- **Parameters:**
  - `id`: (string, required) Function ID (format: `idx:coll:func:md5:addr`).
- **Returns:**
  - `rows`: List of line objects with tokens.
  - `tips`: Tooltip data (features associated with each tokens).
  - `meta`: Function metadata.

### Function Diff
**`GET /api/diff`**
Computes an aligned diff between two functions.
- **Parameters:**
  - `id1`: (string, required) First function ID.
  - `id2`: (string, required) Second function ID.
- **Returns:**
  - `rows`: List of aligned diff rows (left/right).
  - `left_tips`, `right_tips`: Tooltip data for each side.
  - `meta1`, `meta2`: Metadata for both functions.

---

## Search APIs

### Collection & Batch Search
- **`GET /api/collection/search`**: Lists available collections.
  - Params: `offset`, `limit`.
- **`GET /api/batch/search`**: Lists batches for a specific collection.
  - Params: `collection` (required), `offset`, `limit`.

### File & Function Search
- **`GET /api/file/search`**: Searches for files with filters.
  - Params: `collection` (required), `file_name`, `tag`, `file_md5`, `batch_uuid`, `offset`, `limit`.
- **`GET /api/function/search`**: Searches for functions with comprehensive filters.
  - Params: `collection` (required), `function_name`, `file_name`, `tag`, `file_md5`, `batch_uuid`, `language_id`, `decompiler_id`, `return_type`, `calling_convention`, `entrypoint_address`, `offset`, `limit`.

### Global Feature Search
- **`GET /api/feature/search`**: Searches for BSim features globally.
  - Params: `collection`, `hash`, `sort`
- **`GET /api/feature/details/<f_hash>`**: Returns all function occurrences for a specific feature.

### Similarity Search
**`GET /api/similarity/search`**
High-performance similarity search with advanced filtering.
- **Parameters:**
  - `collection`: (required)
  - `algo`: (default: `unweighted_cosine`)
  - `min_score`: (default: `0.95`) Minimum similarity score (0.0 to 1.0).
  - `max_score`: (default: `1.0`) Maximum similarity score.
  - `min_features`: Filter by minimum number of BSim features.
  - `q`: Global keyword search (searches name, tags, IDs).
  - `name`, `tag`, `language`: Specific metadata filters.
  - `md5`: Binary MD5 filter (can be specified multiple times).
  - `cross_binary`: (boolean) Filter for similarities between different binaries.
  - `match_mode`: `any` or `both`.
  - `pool_limit`: Maximum number of candidates to process in DB.
  - `sort_by`: `score` or `feat_count`.
  - `sort_order`: `desc` or `asc`.
- **Returns:**
  - `pairs`: List of similar function pairs with metadata.
  - `metrics`: Performance metrics for the search operation.
  - `total`, `pool_truncated`: Search result statistics.

---

## Clustering APIs
- **`POST /api/cluster/build`**: Enqueues a clustering job.
- **`GET /api/cluster/list`**: Lists discovered clusters with metadata and filtering.
- **`GET /api/cluster/dendrogram`**: Returns a hierarchical tree of clusters for D3 visualization.

---

## Tag Management APIs
Tags can be applied to files, functions, and similarities.
- **`GET /api/tags`**: Returns the global tag index for a collection.
- **`POST /api/tags/add`** / **`POST /api/tags/remove`**: Add/remove a tag from an entity.
- **`POST /api/tags/bulk_add`** / **`POST /api/tags/bulk_remove`**: Add/remove tags from multiple entities.
- **`POST /api/tags/color`**: Sets a custom color for a tag.
- **`POST /api/tags/priority`**: Sets a custom priority for a tag.

---

## Upload API
**`POST /api/file/upload`**
Uploads a raw binary file for server-side analysis.
- **Configuration Params**: `collection`, `profile` (fast/full), `processor` (Ghidra Language ID), `cspec` (Ghidra Compiler Spec ID), `min_func_len`.
- **Similarity Config**: `algo`, `top_k`, `min_score`, `min_features`, `skip_sim`.
- **Metadata**: `batch_uuid`, `batch_name`, `tags`.

---

## Job & Worker APIs

### List & Stats
- **`GET /api/jobs`**: Lists recent and active background jobs.
- **`GET /api/jobs/stats`**: Returns aggregate metrics across all jobs.

### Job Management
- **`GET /api/jobs/<job_id>`**: Returns detailed status and logs for a specific job or pipeline.
- **`POST /api/jobs/<job_id>/cancel`**: Cancels a pending or running job.
- **`POST /api/jobs/<job_id>/retry`**: Retries a failed or cancelled job/pipeline.
