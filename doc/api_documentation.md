# BSimVis API Documentation

This document describes the primary API endpoints for the BSimVis backend.

## Function Data APIs

### Function Code
**`GET /api/function/code`**
Retrieves decompiled code and semantic tokens for a function.
- **Parameters:**
  - `id`: (string, required) Function ID (format: `coll:type:md5:addr`).
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

### Function Features
**`GET /api/function/features`**
Retrieves raw features enriched with C-code context lines.
- **Parameters:**
  - `id`: (string, required) Function ID.
- **Returns:**
  - `features`: List of enriched feature objects.
  - `tips`: Tooltip data.

### Function Similarity
**`GET /api/similarity`**
Retrieves pre-calculated similarity scores between two functions.
- **Parameters:**
  - `id1`, `id2`: (string, required) Function IDs.
- **Returns:**
  - `scores`: Map of algorithms (e.g., `jaccard`, `unweighted_cosine`) to scores.

---

## Search APIs

### Collection & Batch Search
- **`GET /api/collection/search`**: Lists available collections.
  - Params: `offset`, `limit`.
- **`GET /api/batch/search`**: Lists batches for a specific collection.
  - Params: `collection` (required), `offset`, `limit`.

### Feature Search
- **`GET /api/feature/search`**: Searches for features within a collection.
  - Params: `collection` (required), `hash` (prefix match), `sort` (`tf` or `default`), `offset`, `limit`.
- **`GET /api/feature/details/<f_hash>`**: Retrieves details and occurrences for a specific feature.
  - Params: `collection` (required), `offset`, `limit`.

### File & Function Search
- **`GET /api/file/search`**: Searches for files with filters (metadata, tags).
  - Params: `collection` (required), `file_name`, `tag`, `file_md5`, `batch_uuid`, `offset`, `limit`.
- **`GET /api/function/search`**: Searches for functions with comprehensive filters.
  - Params: `collection` (required), `function_name`, `file_name`, `tag`, `file_md5`, `batch_uuid`, `language_id`, `decompiler_id`, `return_type`, `calling_convention`, `entrypoint_address`, `offset`, `limit`.

### Similarity Search
**`GET /api/similarity/search`**
High-performance similarity search with advanced filtering.
- **Parameters:**
  - `collection` (required), `algo` (default: `unweighted_cosine`), `threshold` (default: `0.95`).
  - Filters: `q` (keyword), `name`, `tag`, `language`, `md5` (list), `min_features`, `cross_binary` (boolean).
  - Control: `offset`, `limit`, `pool_limit`, `sort_by` (`score`, `feat_count`, `name`), `sort_order` (`desc`, `asc`).
- **Returns:**
  - `pairs`: List of similar function pairs with metadata summary.
  - `total`, `truncated`: Search result statistics.
