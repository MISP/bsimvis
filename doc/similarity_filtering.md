# Similarity Search Filtering Architecture

The `/api/similarity/search` endpoint implements a "Deep Selection" strategy. Filtering is primarily performed within Redis using a specialized Lua script (`search_similarity.lua`) to ensure atomic operations and minimal data transfer.

## 1. Database-Side Filtering (Lua / Redis)
The system avoids loading full similarity lists into Python. Instead, it works with indices:

### Metadata Filtering
For fields like `name`, `tags`, and `language_id`, the system resolves values to document-level Sets:
1. **Registry Lookup**: The system uses `{coll}:reg:{level}:{field}` to find matching bucket keys (e.g., `main:reg:func:function_name`).
2. **Union**: If a filter matches multiple buckets (e.g., wildcard search), they are combined in Lua.
3. **Verification**: The Lua script verifies similarity candidate pairs against these pre-resolved membership Sets.

### Range Filtering
- **Scores**: Filters by `min_score` and `max_score` using `ZRANGEBYSCORE` on the algorithm's scoreboard (`{coll}:sim:score:{algo}`).
- **Features**: Filters by BSim feature count using the `{coll}:sim:min_features` ZSet.

### Pool Limit
To maintain performance on large collections, a `pool_limit` (default: 10,000) is enforced during the intersection process. Only the top candidates matching the core filters are processed for final enrichment.

## 2. Python-Side Enrichment
Once the Lua engine returns a page of similarity IDs (SIDs), Python performs:
- **Metadata Fetching**: Pipelines JSON fetches for the metadata of both entities in the pair using `idx:{coll}:file:{md5}` or `idx:{coll}:func:{md5}:{addr}`.
- **Timestamp Parsing**: Normalizes ISO/Unix dates for the UI.

## 3. Sorting and Pagination
- **Sorting**: Sorting by `score` or `feat_count` is performed directly in the database layer during selection.
- **Pagination**: `offset` and `limit` are applied during the SID extraction in Lua.

## 4. Performance & Caching
- **Caching**: Full search results are cached in the standard Redis instance (`cache:search:sim:{hash}`) for 60 minutes.
- **Pre-filtering**: Specialized indices like `{coll}:idx:sim:is_cross_binary:true` are used to speed up common queries.
