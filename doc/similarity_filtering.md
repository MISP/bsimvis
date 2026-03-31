# Similarity Search Filtering Architecture

The `/api/similarity/search` endpoint implements a hybrid filtering strategy to ensure performance while maintaining flexibility. Filtering is split between the database layer (Redis/Kvrocks) and the application layer (Python).

## 1. Database-Side Filtering (Redis/Kvrocks)
To avoid loading massive amounts of data into Python, primary filtering is handled in Redis using Sets and Sorted Sets.

### Subset Resolution
For fields like `name`, `tags`, and `language_id`, the system uses `SCAN` to find matching index keys (e.g., `idx:{col}:sim:name:*substring*`).
- **Union**: Matching keys for a single field are combined using `SUNIONSTORE`.
- **Intersection**: Multiple filters are combined using `SINTERSTORE`.

### MD5 Filtering
Function pairs are indexed by the MD5 of their parent binaries. Multiple MD5 filters are combined using `SUNIONSTORE`.

### Keyword Search (`q`)
The global search query splits terms and intersects them. Each term is searched across `name`, `tags`, `id`, and `language_id`.

### Pool Limit
To prevent "OOM" or extreme latency, a `pool_limit` (default 1,000,000) is enforced. Only the top candidates matching the database filters are fetched for further processing.

## 2. Python-Side Post-Processing
Certain filters are more efficient to apply in Python once the candidate pool is reduced.

- **Similarity Score Thresholds**: While scores are stored in a Sorted Set, if other filters are present, the system fetches all candidate scores and filters by `min_score` and `max_score` in Python.
- **Min Features**: The system pre-computes `min_features = min(feat_count1, feat_count2)` during the similarity bake process and indexes it in a ZSET `idx:{col}:sim:min_features`. This allows for extremely efficient range queries (e.g., `ZRANGEBYSCORE ... min_features +inf`) avoiding Python-side overhead.
- **Cross-Binary Logic**: The `cross_binary` flag (which filters out pairs from the same binary) is applied by parsing the similarity IDs in Python.

## 3. Sorting and Pagination
All sorting (`score`, `feat_count`, `name`) and pagination (`offset`, `limit`) are performed in Python after the final filtered list is constructed.

## 4. Performance & Caching
- **Temporary Keys**: Filter intersections use temporary Redis keys with a 60-second TTL.
- **Caching**: Queries taking longer than `CACHE_TIME_THRESHOLD` (0.5s) are cached in Redis for 60 seconds based on a hash of their parameters.
