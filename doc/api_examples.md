# BSimVis API Examples

Example `curl` commands for the primary endpoints, using a `test_api` collection. Default port is `5000` (set by `APP_PORT` in `.env`). See [api_documentation.md](api_documentation.md) for the full parameter reference.

## Index & Jobs
```bash
# Database statistics
curl -s "http://localhost:5000/api/index/status?collection=test_api"

# List background jobs
curl -s "http://localhost:5000/api/jobs?limit=10"

# Job / pipeline status
curl -s "http://localhost:5000/api/jobs/<job_id>"
```

## Collections & Batches
```bash
curl -s "http://localhost:5000/api/collection/search?q=test"
curl -s "http://localhost:5000/api/batch/search?collection=test_api"
```

## Files
```bash
# Search files
curl -s "http://localhost:5000/api/file/search?collection=test_api"

# File details (with clusters)
curl -s "http://localhost:5000/api/file/details/59281a167473ca9b98515b11cb709f82?collection=test_api"

# Call graph
curl -s "http://localhost:5000/api/file/call_graph?collection=test_api&file_md5=59281a167473ca9b98515b11cb709f82"
```

### Upload a raw binary
```bash
curl -X POST --data-binary "@/path/to/file" \
  "http://localhost:5000/api/file/upload?collection=test_api&file_name=my_binary&profile=fast"

# With provenance metadata: parent archive + siblings
curl -X POST --data-binary "@/path/to/file" \
  "http://localhost:5000/api/file/upload?collection=test_api&file_name=my_binary&tags=dropper&related_md5=<md5b>&file_metadata_extra=%7B%22parent_md5%22%3A%22<md5parent>%22%2C%22parent_file_name%22%3A%22sample.zip%22%7D"

# Search by the parent hash: returns the children too
curl -s "http://localhost:5000/api/file/search?collection=test_api&md5=<md5parent>"
```

## Functions
```bash
# Search functions
curl -s "http://localhost:5000/api/function/search?collection=test_api&function_name=main"

# Decompiled code + tokens
curl -s "http://localhost:5000/api/function/code?id=test_api:func:59281a167473ca9b98515b11cb709f82:00101144"

# Features for a function
curl -s "http://localhost:5000/api/function/features?id=test_api:func:59281a167473ca9b98515b11cb709f82:00101144"

# Aligned function diff
curl -s "http://localhost:5000/api/diff?md5_a=<md5a>&addr_a=<addra>&md5_b=<md5b>&addr_b=<addrb>&collection_a=test_api"
```

## Features (Global)
```bash
curl -s "http://localhost:5000/api/feature/search?collection=test_api&sort_by=tf_score"
curl -s "http://localhost:5000/api/feature/details/<f_hash>?collection=test_api"
```

## Search Utilities
```bash
curl -s "http://localhost:5000/api/search/autocomplete?collection=test_api&level=func&field=function_name&q=aes"
curl -s "http://localhost:5000/api/search/fields?collection=test_api&level=func&field=function_name"
```

## Similarity Engine
```bash
# Main similarity search
curl -s "http://localhost:5000/api/similarity/search?collection=test_api&min_score=0.9&cross_binary=true"

# List pre-calculated similarities for a file
curl -s "http://localhost:5000/api/similarity/list?collection=test_api&md5=59281a167473ca9b98515b11cb709f82"

# Build status for a target
curl -s "http://localhost:5000/api/similarity/status?collection=test_api&md5=59281a167473ca9b98515b11cb709f82"

# Build similarities for a file
curl -X POST -H "Content-Type: application/json" \
  -d '{"collection": "test_api", "md5": "59281a167473ca9b98515b11cb709f82"}' \
  http://localhost:5000/api/similarity/build

# Tag a similarity pair
curl -X POST -H "Content-Type: application/json" \
  -d '{"collection":"test_api","id1":"test_api:func:<md5a>:<addra>","id2":"test_api:func:<md5b>:<addrb>","tag":"interesting"}' \
  http://localhost:5000/api/similarity/tag
```

## Tagging
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"collection": "test_api", "entity_type": "function", "entity_id": "test_api:func:59281a167473ca9b98515b11cb709f82:00101144", "tag": "important"}' \
  http://localhost:5000/api/tags/add
```

## Function Clustering
```bash
# List clusters
curl -s "http://localhost:5000/api/cluster/list?collection=test_api"

# Dendrogram tree (D3)
curl -s "http://localhost:5000/api/cluster/tree?collection=test_api"

# Full re-analysis (clusters + binary similarity)
curl -X POST -H "Content-Type: application/json" \
  -d '{"collection": "test_api", "algo": "unweighted_cosine"}' \
  http://localhost:5000/api/cluster/rebuild_all
```

## Binary Similarity & Clustering
```bash
# Build binary similarities
curl -X POST -H "Content-Type: application/json" \
  -d '{"collection": "test_api", "algo": "unweighted_cosine"}' \
  http://localhost:5000/api/bin_sim/build

# Search binary similarity pairs
curl -s "http://localhost:5000/api/bin_sim/search?collection=test_api&min_score=0.5&sort_by=shared_clusters"

# One page of the matched table of a file diff, filtered and sorted server-side
curl -s "http://localhost:5000/api/bin_sim/diff?collection_a=test_api&md5_a=<md5a>&md5_b=<md5b>&table=matched&cl_q=crypto&sim_min=0.9&sort_col=func_name&sort_dir=asc&limit=50"

# Compact projection for the Sankey view
curl -s "http://localhost:5000/api/bin_sim/diff?collection_a=test_api&md5_a=<md5a>&md5_b=<md5b>&view=sankey"

# Similar binaries for one MD5
curl -s "http://localhost:5000/api/bin_sim/list?collection=test_api&md5=59281a167473ca9b98515b11cb709f82"

# List binary clusters
curl -s "http://localhost:5000/api/bin_cluster/list?collection=test_api"
```

## Notes
```bash
# Add a function note
curl -X POST -H "Content-Type: application/json" \
  -d '{"collection":"test_api","func_id":"test_api:func:<md5>:<addr>","text":"input validation","owner":"analyst"}' \
  http://localhost:5000/api/notes/add

# List file notes
curl -s "http://localhost:5000/api/notes/file/list?collection=test_api&file_id=test_api:file:59281a167473ca9b98515b11cb709f82"
```

## LLM (Ollama)
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"func_id":"test_api:func:<md5>:<addr>"}' \
  http://localhost:5000/api/llm/summarize
```

## Pools (Cross-Collection)
```bash
# Create a pool over two collections (also enqueues the full build pipeline)
curl -X POST -H "Content-Type: application/json" \
  -d '{"name":"My Pool","collections":["test_api","bench"]}' \
  http://localhost:5000/api/pool

# Cross-collection only: keep pairs spanning two different collections, to label
# an unknown corpus against a reference one
curl -X POST -H "Content-Type: application/json" \
  -d '{"name":"Known vs Unknown","collections":["reference","unknown"],
       "config":{"only_cross_collection":true,
                 "func_sim_params":{"min_score":0.95,"top_k":50},
                 "file_sim_params":{"min_cohesion":0.6}}}' \
  http://localhost:5000/api/pool

# List pools
curl -s "http://localhost:5000/api/pool?refresh_sync=1&sort_by=last_built_at&sort_order=desc"

# Build + cluster a pool
curl -X POST "http://localhost:5000/api/pool/<pool_id>/rebuild"

# Search functions within a pool
curl -s "http://localhost:5000/api/function/search?pool=<pool_id>&function_name=main"

# Restrict a pool query to one member collection
curl -s "http://localhost:5000/api/function/search?pool=<pool_id>&collection=test_api"

# Index a pool's binary similarities so bin_sim/search uses the fast path
curl -X POST -H "Content-Type: application/json" \
  -d '{"pool_id":"<pool_id>"}' \
  http://localhost:5000/api/bin_sim/reindex
```
