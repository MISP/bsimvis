# BSimVis API Documentation

This document describes the primary API endpoints for the BSimVis backend.

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
  - `pool_limit`: (default: 1,000,000) Maximum number of candidates to process in DB.
  - `sort_by`: `score` or `feat_count`.
  - `sort_order`: `desc` or `asc`.
- **Returns:**
  - `pairs`: List of similar function pairs with metadata.
  - `metrics`: Performance metrics for the search operation.
  - `total`, `pool_truncated`: Search result statistics.

---

## Job & Worker APIs

### List Jobs
**`GET /api/jobs`**
Lists recent and active background jobs.
- **Parameters:**
  - `limit`: (default: 50) Max jobs to return.

### Global Stats
**`GET /api/jobs/stats`**
Returns aggregate metrics across all jobs.

### Job Status
**`GET /api/jobs/<job_id>`**
Returns detailed status and logs for a specific job or pipeline.

### Cancel Job
**`POST /api/jobs/<job_id>/cancel`**
Cancels a pending or running job.
