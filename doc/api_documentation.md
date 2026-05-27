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

### Similarity Calculation & Management

#### Similarity List
**`GET /api/similarity/list`**
Lists pre-calculated similarity results for a file or ingestion batch.
- **Parameters:**
  - `collection`: (default: `main`)
  - `md5`: File MD5 (required unless `batch` is provided).
  - `batch`: Batch UUID (required unless `md5` is provided).
  - `algo`: Similarity algorithm (default: `unweighted_cosine`).
  - `limit`: Max results to return (default: `20`).
  - `offset`: Pagination offset (default: `0`).

#### Build Similarity
**`POST /api/similarity/build`**
Enqueues a background job to pre-calculate similarity pairs.
- **Request Body (JSON):**
  - `collection`: (required)
  - `md5`: File MD5.
  - `batch`: Batch UUID.
  - `algo`: (default: `unweighted_cosine`)
  - `min_score`: (default: `0.95`)
  - `top_k`: (default: `20`)
  - `min_features`: (default: `0`)
  - `all`: (default: `false`)

#### Rebuild Similarity
**`POST /api/similarity/rebuild`**
Enqueues a clear + build pipeline to recalculate similarity pairs.
- **Request Body (JSON):** (Same schema as Build Similarity)

#### Clear Similarity
**`POST /api/similarity/clear`**
Enqueues a background job to delete calculated similarity pairs.
- **Request Body (JSON):**
  - `collection`: (required)
  - `md5`: File MD5.
  - `batch`: Batch UUID.
  - `algo`: (default: `unweighted_cosine`)

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

## Ingestion & Upload APIs

### Raw Binary Upload
**`POST /api/file/upload`**
Uploads a raw binary file for server-side analysis.
- **Configuration Params (Query or Form):**
  - `collection`: Collection name (default: `main`).
  - `profile`: Ghidra analysis profile: `fast` or `full` (default: `fast`).
  - `processor`: Force a specific Ghidra Language ID (e.g., `x86:LE:64:default`).
  - `cspec`: Force a specific Ghidra Compiler Spec ID (e.g., `gcc`).
  - `min_func_len`: Minimum function length in instructions (default: `10`).
- **Similarity Config (Query or Form):**
  - `algo`: Similarity algorithm (`jaccard`, `unweighted_cosine`, `milvus_sparse`).
  - `top_k`: Top K matches per function.
  - `min_score`: Minimum similarity score threshold.
  - `min_features`: Minimum feature count required.
  - `skip_sim`: Set to `true` to skip building similarities.
- **Metadata (Query or Form):**
  - `batch_uuid`: Ingestion batch UUID.
  - `batch_name`: Ingestion batch name.
  - `tags`: Optional tags to associate with the uploaded file.

### Pre-analyzed Ingestion Data Upload
**`POST /api/file/upload_file_data`**
Uploads JSON metadata and function feature maps directly from client-side extractor tools.
- **Request Body (JSON):**
  - `collection`: Collection name (default: `main`).
  - `file_md5`: File MD5 (optional, will be calculated if missing).
  - `top_k`: Top K matches per function.
  - `min_score`: Minimum similarity score threshold.
  - `min_features`: Minimum feature count required.
  - `algo`: Ingestion/similarity algorithm.
  - `skip_sim`: Set to `true` to skip similarity calculation step.
  - Plus standard Ghidra feature extraction payload fields (e.g. `file_name`, `functions`, `features`, `language_id`).

---

## Job & Worker APIs

### List & Stats
- **`GET /api/jobs`**: Lists recent and active background jobs.
- **`GET /api/jobs/stats`**: Returns aggregate metrics across all jobs.

### Job Management
- **`GET /api/jobs/<job_id>`**: Returns detailed status and logs for a specific job or pipeline.
- **`POST /api/jobs/<job_id>/cancel`**: Cancels a pending or running job.
- **`POST /api/jobs/<job_id>/retry`**: Retries a failed or cancelled job/pipeline.
