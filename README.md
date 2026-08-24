# BSimVis

<p align="center">
  <img src="/img/logo/logo_dark_bg.svg" alt="Repository Logo" width="200"/>
</p>

BSimVis is a tool to analyze similarities across a collection of binaries, based on [Ghidra](https://github.com/nationalsecurityagency/ghidra) analyzers and the BSim (Behavioral Similarity) plugin. It provides an API and Web interface to upload large quantities of decompiled binaries and BSim feature vectors to a Kvrocks database for similarity analysis, function diffing, and family clustering.


BSimVis uses a custom database because Ghidra's BSim databases don't store decompiled code and other metadata. This alternative BSim database and API provide filtering and visualization of this additional data across multiple binaries at once. It doesn't aim to replace Ghidra's BSim plugin, but to enable more advanced analysis and visualization of the similarities on a large scale (family clustering, etc.).


![alt text](img/sim_view.png)
# Features

### Analysis
- Upload decompiled functions and BSim vectors from Ghidra
- Similarity search with score filtering across multiple binaries
- Function diffing based on BSim features
- BSim feature correlation with decompiled C tokens / Pcode blocks
- Call graph navigation (callers and callees)

### Clustering
- HDBSCAN-based binary family clustering and interactive file dendrogram visualization
- Cluster search view with dendrogram and packing diagram
- Stability and parent cluster filtering

### Cross-Collection Pools
- Combine multiple collections into a pool for similarity search and clustering across their union
- Search endpoints accept a `pool` parameter; per-pool build/cluster jobs with sync-status tracking

### Search & Filtering
- Full text search on files and features with sorting, filtering, and pagination
- Search history and caching

### Web Interface
- Similarity graph
- Dynamic window management for multiple code previews
- Tag management for files, functions, and similarities
- Quick preview tooltips for clusters and diffs
- Table selection and copy across all views

### Analyst Notes & AI Insights
- Analyst notes system for files and functions
- Local LLM assistant for file and function summaries

### API
- REST API with Swagger documentation
- Upload API: processor/compiler config, profiling, batch metadata, and similarity params

# Screenshots




## Web UI Similarity Search Graph view

![alt text](img/new_sim_view.png)

## Web UI Diffing

![alt text](img/diff.png)

## Web UI Cluster Dendrogram 

![alt text](img/function_cluster_view.png)

## Call graph

![alt text](/img/call_graph.png)

# Requirements

- Ghidra and pyghidra install
- Redis and Kvrocks databases

# Installation

Copy the example configuration files and customize them:

```bash
cp .env.example .env
cp bsimvis_config.toml.example bsimvis_config.toml
```

Run the install script to set up portable Redis, Kvrocks, and optionally Ghidra:

```bash
./install.sh
```

Milvus support is optional and can be enabled via the `.env` file (`ENABLE_MILVUS=true`).

# Running

Use the launch script to start all services in screen sessions:

```bash
./launch.sh
```

Use `--clear` to kill stale sessions before restarting:

```bash
./launch.sh --clear
```

`launch_tmux.sh` is the tmux equivalent, and additionally caps the worker count by host
RAM and runs each worker under a memory-limited systemd scope.

Services are configured via `.env` (see `.env.example`). Key variables:

| Variable | Default | Description |
|---|---|---|
| `KVROCKS_PORT` | `6666` | Kvrocks database port |
| `REDIS_PORT` | `6379` | Redis job queue port |
| `APP_PORT` | `5000` | API / Web UI port |
| `WORKERS_COUNT` | `5` | Number of background workers |
| `DATA_BASE_DIR` | `./data` | Storage path for all service data |
| `ENABLE_MILVUS` | `false` | Enable optional Milvus vector DB |
| `WORKER_MEMORY_MAX` | `3G` | Per-worker memory cap (`launch_tmux.sh` only) |

# Test script

```
uv run test_api_endpoints.py
```

# Benchmark

```bash
# Ingest + similarity pipeline over the JSON fixtures in data/bench/
uv run bsimvis-bench --clear

# Benchmark the pool paths, save metrics, compare against a baseline
uv run bsimvis-bench --bench-pools --save data/bench_results/run.json
uv run bsimvis-bench --compare data/bench_results/run.json
```

# API

Full endpoint reference in [doc/api_documentation.md](doc/api_documentation.md); `curl` examples in [doc/api_examples.md](doc/api_examples.md). Interactive Swagger UI is at `/api/` when running.

## Binary upload

One call is the whole story: analysis, indexing, and per-file similarity all
run as part of this one job, and the collection's function/binary clusters
rebuild automatically a short while after uploads to it go quiet
(`clustering.idle_debounce_seconds` in `bsimvis_config.toml`, default 30s) --
see [Batch upload](#batch-upload-optional) below for why you don't need to
call anything else, even for many files at once.

```
# upload to /api/file/upload
curl -X POST --data-binary "@/path/to/file" \
  "http://localhost:5000/api/file/upload?collection=main&file_name=my_binary&profile=fast"

# Response
{
    "status": "processing",
    "file_md5": "b7680c697c69aff3cd8f44fffcb7d683",
    "pipeline_id": "b2e1a4c0-...",
    "message": "Binary uploaded. Analysis started."
}
```

Add `&priority=high` to jump this file's analysis ahead of other pending jobs
on the shared worker pool -- useful when another collection has a long-running
job tying up workers and you need one file processed fast regardless. It does
not preempt a job already running, only reorders what an idle worker picks up
next.

## Ghidra project upload 

Files in ghidra projects won´t get reanalyzed to not overwrite analyst work, meaning if no analyzers were ran in this project, no functions will be found

```
# Zip your .gpr and .rep project
gpr_name="my_project" # .gpr
zip -r "$gpr_name.gpr.zip" "$gpr_name.gpr" "$gpr_name.rep"

# upload to /api/file/upload
curl -X POST --data-binary @$gpr_name.gpr.zip \
  "http://localhost:5000/api/file/upload?file_name=$gpr_name&collection=main&profile=fast"
```

## Batch upload (optional)

Just upload each file with plain `POST /api/file/upload` calls, in a loop or
in parallel -- no `batch_uuid`, no finalize call needed. Every collection has
its own lane: all the files you upload analyze fully in parallel across
workers, and once uploads to that collection go quiet, the lane
automatically groups everything uploaded in that window, clears old
cluster/bin_sim results, and rebuilds function clusters, binary similarity,
and binary clusters -- once, covering the whole batch:

```
for f in /path/to/*.bin; do
  curl -s -X POST --data-binary "@$f" \
    "http://localhost:5000/api/file/upload?collection=main&file_name=$(basename $f)"
done
# each upload analyzes immediately; clustering fires on its own ~30s after the last one
```

`POST /api/cluster/rebuild_all` and `POST /api/file/upload/batch_finalize`
(explicit `pipeline_ids` list, `batch_uuid` grouping) still work for forcing
a rebuild right now instead of waiting, or for scripts that already manage
their own batching -- they queue behind whatever's currently active for that
collection instead of racing it (a collection's lane only ever runs one
clear/rebuild at a time; two overlapping requests used to corrupt each
other's results). Add `"priority": "high"` to jump an explicit request ahead
of other rebuilds already queued for that same collection. See
[doc/api_documentation.md](doc/api_documentation.md#post-apifileuploadbatch_finalize)
for `batch_finalize`'s full parameters.

## Follow pipeline progress

```
curl -s "http://localhost:5000/api/jobs/pipe_f4f87081-ab7d-4077"

# Wait for completed status 
{
...
    "id": "pipe_f4f87081-ab7d-4077",
    "progress": "100",
    "status": "completed",
...
}
```

## Build function clusters and Binary similarities

Runs automatically after uploads to a collection go quiet (see
[Batch upload](#batch-upload-optional) above). Call this directly to force it
right now instead of waiting -- it queues behind whatever's currently active
for that collection rather than running concurrently with it:

```
curl -X POST -H "Content-Type: application/json" \
 -d '{"collection": "main", "algo": "unweighted_cosine"}' \
 http://localhost:5000/api/cluster/rebuild_all

# Response -- pipeline_id is pollable via GET /api/jobs/{pipeline_id} either way,
# whether it started immediately or is waiting behind another active rebuild
{
    "job_id": "b2e1a4c0-...",
    "pipeline_id": "b2e1a4c0-...",
    "status": "queued"
}
```

# CLI tool

## Upload BSIM data

Assuming you have the API running, upload binary or ghidra project (files in ghidra projects won´t get reanalyzed to not overwrite analyst work, meaning if no analyzers were ran in this project, no functions will be found)

```bash
uv run bsimvis upload <target1> <target2> ... <targetN> -c <collection_name>
```

See `bsimvis_config.toml` for an example config file.

## Job management

```bash
# List all jobs
uv run bsimvis job list

# View logs of a specific job
uv run bsimvis job status <job_id>

# Cancel a job
uv run bsimvis job cancel <job_id>
```

## Worker management

```bash
# Start workers
uv run bsimvis worker start --count 5
```

## Building clusters

For now clustering is a manual job to run after ingesting all binaries. 

```bash
uv run bsimvis cluster build -c <collection_name>
```

## Binary similarity management

```bash
# Build binary similarities
uv run bsimvis binsim build -c <collection_name>
```

## Collection management

```bash
# Wipe and delete a collection completely
uv run bsimvis collection delete -c <collection_name>

# Clean up temporary raw/JSON upload keys in a collection
uv run bsimvis collection clean -c <collection_name>
```

## Metadata propagation

```bash
# Propagate metadata from a pipe-delimited CSV file
uv run bsimvis metadata propagate -m <metadata_csv_path> -c <collection_name>
```

## Full CLI reference

```
usage: bsimvis [-h] [-H HOST] {features,index,sim,cluster,binsim,job,worker,upload,collection,metadata} ...

Unified BSimVis CLI

positional arguments:
  {features,index,sim,cluster,binsim,job,worker,upload,collection,metadata}
    features            BSim Feature management (Indexing)
    index               Index health and statistics
    sim                 Similarity management
    cluster             Unsupervised clustering management
    binsim              Binary-level similarity management
    job                 Job & Pipeline management
    worker              Worker management
    upload              Upload binaries to redis/kvrocks
    collection          Collection management
    metadata            Metadata management and propagation

options:
  -h, --help            show this help message and exit
  -H, --host HOST       API host:port (default: localhost:5000, or from .env, or from bsimvis_config.toml)
```
