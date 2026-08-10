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
- LLM assistant for file and function summaries (local or remote via Ollama)

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
- Function ID databases in the Ghidra install (see below) — without them, function library tags stay empty
- Ollama (optional) for LLM analyst insights and function summaries. Can be run locally (default port `11434`) or remotely (configurable via `ollama_url` in `bsimvis_config.toml`).
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

## Function ID databases

Ghidra's Function ID (FID) analyzer is what produces the `lib:<library>:<version>:<name>` tags BSimVis puts
on functions: standard library code identified by exact and operand-masked instruction hashes. Stock Ghidra
ships FID databases for Visual Studio runtimes only, so on ELF samples nothing gets identified until you add
databases to `$GHIDRA_HOME/Ghidra/Features/FunctionID/data/`.

`install.sh` fetches the open-source [threatrack/ghidra-fidb-repo](https://github.com/threatrack/ghidra-fidb-repo)
(MIT — 67 MB download, 215 MB unpacked, 47 databases: glibc, uClibc/OpenWrt, gcc, OpenSSL, Qt5, SDL,
libsodium, CentOS 6/7, across x86 32/64, ARM, AARCH64, MIPS, PowerPC, SPARC, SuperH, m68k). Skip it with
`SKIP_FIDB=1 ./install.sh`.

To install them by hand, into your own Ghidra:

```bash
curl -LO https://github.com/threatrack/ghidra-fidb-repo/releases/download/20200530/ghidra-fidb-repo_20200530.zip
unzip ghidra-fidb-repo_20200530.zip -d "$GHIDRA_HOME/Ghidra/Features/FunctionID/data/"
```

Those are the pre-unpacked `.fidbf` files, so no Ghidra rebuild is needed. Restart the workers afterwards —
Ghidra reads the data directory at startup.

Coverage is per-architecture and per-toolchain. No database exists for PowerPC e500, MIPS64r6, RISC-V or
LoongArch, and a statically linked binary built with a different compiler or `-O` level than the one the FIDB
was built from misses every hash even where a database exists. Build your own with Ghidra's
`Tools -> Function ID -> Populate Function ID Database` when a library matters and nothing matches it.

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

```
# upload to /api/file/upload
curl -X POST --data-binary "@/path/to/file" \
  "http://localhost:5000/api/file/upload?collection=main&file_name=my_binary&profile=fast"

# Follow pipeline with response
{
    "status": "processing",
    "file_md5": "b7680c697c69aff3cd8f44fffcb7d683",
    "pipeline_id": "pipe_f4f87081-ab7d-4077",
    "message": "Binary uploaded. Analysis pipeline started."
}
```

## Supported upload formats

`/api/file/upload` takes the raw bytes of whatever you have. Containers are unpacked server-side and every binary inside is analyzed as its own file, tagged with the format it came from and carrying the container's md5 in `parent_md5`.

| Upload | What gets analyzed | Tag |
|---|---|---|
| Raw executable (ELF, PE, Mach-O, ...) | The file itself | — |
| `.zip`, `.tar`, `.tar.gz`, `.tar.bz2`, `.tar.xz` | Every member, one file each | `container:archive` |
| Encrypted zip (ZipCrypto or AES) | Same, password defaults to `infected` | `container:archive` |
| `.apk`, `.aab` | Only `.dex`, `.so` and `.jar` members — resources and assets are skipped | `container:apk` |
| Fat/universal Mach-O | One file per architecture slice, named `<file>:x86_64`, `<file>:arm64`, ... | `container:macho-fat` |
| UPX-packed executable | **Both** the packed file and its unpacked child, so packed-vs-unpacked is a normal binary diff | `packer:upx` |
| `.gpr.zip` (Ghidra project) | Imported whole, never unpacked — see below | — |

Notes:

- Archive password: `--archive-password` on the CLI, `archive_password=` on the API. Default is `infected`, the usual convention for shipped malware samples.
- Unpacking UPX needs the `upx` binary, installed into `bin/` by `install.sh`. Without it a packed sample is still analyzed, just packed.
- Containers are followed 2 levels deep, capped at 200 children.
- `--no-unpack` (`unpack=false`) analyzes the upload exactly as-is.
- Unpacked something with your own tooling? Declare the lineage yourself with `--parent-md5` / `--parent-name` (`parent_md5=` / `parent_file_name=`).

Adding another format means one entry in `HANDLERS` in [`bsimvis/app/services/unpack_service.py`](bsimvis/app/services/unpack_service.py).

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

```
# With all binaries uploaded and ingestion pipeline completed
# Or periodically schedule 

curl -X POST -H "Content-Type: application/json" \
 -d '{"collection": "main", "algo": "unweighted_cosine"}' \
 http://localhost:5000/api/cluster/rebuild_all
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
