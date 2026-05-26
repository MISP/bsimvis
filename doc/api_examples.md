# BsimVis API Examples

This document provides example `curl` commands for the primary API endpoints, using the `test_api` collection.

## 1. Index Statistics
Get database statistics for a collection.
```bash
curl -s "http://localhost:5000/api/index/status?collection=test_api"
```

## 2. Jobs Management
List background jobs.
```bash
curl -s "http://localhost:5000/api/jobs?limit=10"
```
Get job status.
```bash
curl -s "http://localhost:5000/api/jobs/<job_id>"
```

## 3. Collections and Batches
Search collections.
```bash
curl -s "http://localhost:5000/api/collection/search?q=test"
```
Search batches in a collection.
```bash
curl -s "http://localhost:5000/api/batch/search?collection=test_api"
```

## 4. File Operations
Search for files.
```bash
curl -s "http://localhost:5000/api/file/search?collection=test_api"
```
Get call graph for a file.
```bash
curl -s "http://localhost:5000/api/file/call_graph?collection=test_api&file_md5=59281a167473ca9b98515b11cb709f82"
```

## 5. Function Analysis
Search for functions.
```bash
curl -s "http://localhost:5000/api/function/search?collection=test_api&function_name=main"
```
Get decompiler code for a function.
```bash
curl -s "http://localhost:5000/api/function/code?id=test_api:func:59281a167473ca9b98515b11cb709f82:00101144"
```
Aligned diff between two functions.
```bash
curl -s "http://localhost:5000/api/function/diff?id1=<id1>&id2=<id2>"
```

## 6. Similarity Engine
Main similarity search.
```bash
curl -s "http://localhost:5000/api/similarity/search?collection=test_api&min_score=0.9"
```
Build similarities for a file.
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"collection": "test_api", "md5": "59281a167473ca9b98515b11cb709f82"}' \
  http://localhost:5000/api/similarity/build
```

## 7. Tagging
Add a tag to a function.
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"collection": "test_api", "entity_type": "function", "entity_id": "test_api:func:59281a167473ca9b98515b11cb709f82:00101144", "tag": "important"}' \
  http://localhost:5000/api/tags/add
```

## 8. Clustering
List discovered clusters.
```bash
curl -s "http://localhost:5000/api/cluster/list?collection=test_api"
```
Get cluster tree (D3 format).
```bash
curl -s "http://localhost:5000/api/cluster/dendrogram?collection=test_api"
```
