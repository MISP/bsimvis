# AGENTS.md

## Python

Use uv run to run


## Ports
Configurable via `.env` file:
- Kvrocks : `KVROCKS_PORT` (default: 6666) -> storage of functions, binaries and similarities
- Redis : `REDIS_PORT` (default: 6379) -> Job queue only
- API : `APP_PORT` (default: 5000) -> localhost:5000/api

Hosts are also configurable via `KVROCKS_HOST`, `REDIS_HOST`, and `APP_HOST`.

## Layout

- `bsimvis/app/routes/` — endpoint implementations, imported by `bsimvis/app/swagger.py`.
  `_list_query.py` holds the shared in-memory filter/sort/paginate helpers for the
  small registry listings (collections, pools).
- `bsimvis/app/services/` — all logic. `similarity_service.py` and `bin_sim_service.py`
  build similarities, `pool_service.py` owns pools, `job_service.py` owns the queue,
  `config_service.py` reads `bsimvis_config.toml`, `ghidra_service.py` owns the JVM.
- `bsimvis/app/lua/` — only `search_function.lua`, `search_similarity.lua` and
  `clear_similarity.lua`. Nothing else should become Lua: Kvrocks serializes every
  `EVAL` under one global lock, so a Lua build step blocks all other workers.
  Similarity candidate discovery is deliberately pure Python for that reason.
- `bsimvis/worker.py` — the queue worker. One process per worker, each with its own
  in-process Ghidra JVM.
- `bsimvis/cli/` — subcommands wired in `bsimvis/cli/main.py` (`bsimvis <sub>`).
  `bsimvis_bench.py` is a separate entry point (`bsimvis-bench`), not a subcommand.

## Jobs

`JobType` in `job_service.py` is the single source of truth for job types — add there,
never hardcode a string. Pipelines and groups are jobs too (`type` = `pipeline` /
`group`, children in `task_ids`).

- `enqueue_job` is idempotent via a `queued` latch field; do not bypass it.
- Chunked work re-enqueues itself as a *continuation* (`rpush` to the tail) so batches
  from different jobs interleave instead of one job starving the fleet.
- Clear jobs (`clear_sim`, `clear_features`, `clear_cluster`, `clear_bin_sim`,
  `sync_milvus`) go to `jobs:pending:high`.

## Pools

A pool (`pool_service.py`, `routes/pools.py`) is a named union of collections with its
own similarity/cluster namespace under `global:pool:{id}`. Its config carries
`only_cross_collection` — when set, matches inside a single member collection are
dropped, keeping only cross-collection pairs. Pool builds run as a pipeline:
`init_pool_build` -> `build_pool_sim` chunks -> `finalize_pool_build`, then
`cluster_pool` / `build_pool_bin_sim` / `cluster_pool_binaries`. Tags and notes stay
written against the origin collection; only clusters are namespace-local.

## Similarity

`similarity.min_features` in `bsimvis_config.toml` is a BSim floor. Functions below it
skip BSim entirely and get a deterministic 1.0/0 match from their exact Ghidra
FunctionID hash (`{fid}:funcid` pointer, `{coll}:funcid:{hash}` buckets) — BSim is
false-positive-prone on tiny functions. Both the collection and pool build paths do this.

Binary similarity diff tables are paged, filtered and sorted **server-side**
(`_page_diff` in `routes/bin_sim.py`). Don't reintroduce full-table client loads.

## Workers and Ghidra memory

Each worker holds its own JVM, so heap limits multiply by worker count.
`ghidra.max_heap_mb` sets an absolute `-Xmx` per worker (keep
`max_heap_mb * WORKERS_COUNT` under ~60% of host RAM); `ghidra.max_ram_percent` is only
for the single-JVM `bsimvis upload` CLI. `ghidra.jvm_args` enables periodic G1 GC so an
idle worker gives its heap back. `launch_tmux.sh` additionally caps `WORKERS_COUNT` by
host RAM (~3 GB each) and wraps each worker in a systemd scope with
`MemoryMax=$WORKER_MEMORY_MAX`, so a runaway worker is OOM-killed and its job requeued.

## Databases tip

Never use `keys` or other commands that might freeze the Kvrocks database.
The database holds millions of similarities. 
Dont whipeout the database.
Dont change database structure unless user asks.

### Kvrocks db
Indexes, and inverse indexes are stored in [collection]:idx:[level]:[field]:[value]
Like `main_collection:idx:file:file_name:file.exe`

Registries hold all the key of indexes, for quick search : [collection]:reg:[level]:[field]
Like `main_collection:reg:file:file_name`

This doesnt apply to global indexes and registries which : 

### Redis db

Since its only for jobs, the jobs are in : 

| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `job:{id}` | **Hash** | Status, payload and metadata for a job, pipeline or group. |
| `jobs:pending` / `jobs:pending:high` | **List** | Work queues; workers pop from the tail. |
| `jobs:processing` | **List** | In-flight job IDs; a worker `LMOVE`s here when it claims a job. |
| `jobs:global` / `jobs:collection:{c}` | **List** | Recent job IDs, trimmed to 1000. |

## Worktree testing

Never read `data/kvrocks/` or `hs_err_pid*.log` — confidential (real binary md5s /
function data). Tests use only the git-tracked `data/test/` fixtures.

In a linked worktree, run `./scripts/wt-test.sh` before committing. Do NOT commit if
it prints `RESULT: FAIL` or the run was skipped. Show the output.

Three scripts, all refusing to run outside a linked worktree so they can never touch
the main stack's `.env`, ports or confidential DB:

| Script | Does |
|:--- |:--- |
| `scripts/wt-setup.sh` | Brings the isolated stack up and leaves it running. Symlinks `bin/` from the main repo (never recompiled — 1.4G of downloaded tools), writes an isolated `.env` if missing (own `PROJECT_NAME` + offset ports + fresh local data dir), aborts if any of its ports is already held, launches via `launch_tmux.sh --clear`, waits for the app port. Idempotent — tears down an existing session of its own first. |
| `scripts/wt-teardown.sh` | Shuts redis + kvrocks down and kills the worktree's tmux session. Reads the worktree's own `.env` for ports; no-ops if there is none. |
| `scripts/wt-test.sh` | `wt-setup.sh` → `test_api_endpoints.py` (with `API_URL` pointed at the worktree's app port) → `wt-teardown.sh`, then `RESULT: PASS`/`FAIL`. |

Use `wt-setup.sh` directly when you want a live stack to poke at by hand, and
`wt-teardown.sh` when done — the dashboard URL is printed as a clickable link at the
end of launch. Ports come from the worktree name, so two worktrees can run at once.

Test files live at the repo root: `test_api_endpoints.py` (the broad suite — endpoints,
pools, filtering and sorting sweeps; the old `test_pools.py` was absorbed into it),
`test_bin_sim_file_tags.py`, `test_chunk_job_priority.py`, `test_origin_collection.py`.

## Benchmarking

`uv run bsimvis-bench` runs the ingest + similarity pipeline against the git-tracked
JSON fixtures in `data/bench/` (`--dir` to override) into the `test_bench` collection.
`--bench-pools` benchmarks the pool paths instead. Save runs with
`--save data/bench_results/<name>.json` and regress against one with `--compare`;
`data/bench_results/` is gitignored, `data/bench/` is not.

`scripts/bench/` is the BSim scoring benchmark suite, built on a reproducible
open-source corpus (180 cross-compiled binaries, 313k functions) that is *not* in
git — see `doc/bench_corpus.md` for how to rebuild it under `$CORPUS_ROOT`.

| Script | Answers |
|:--- |:--- |
| `corpus/build_corpus.sh`, `corpus/manifest.py`, `corpus/extract.py` | build the corpus, describe it, Ghidra-extract it once |
| `quality.py` | retrieval accuracy per algorithm, offline over extracted vectors |
| `pipeline_bench.py` | ingest + build-sim throughput per algorithm (needs the stack) |
| `scoring_cost.py`, `recall.py`, `oracle_compare.py`, `idf_coverage.py` | per-pair cost, small-scale recall, Ghidra oracle, weighting go/no-go |
| `bsim_baseline.py` + `bsim/BSimQueryAll.java` | Ghidra's own BSim database as a retrieval baseline |

The BSim baseline is benchmark-only: it drives `support/bsim` and
`support/analyzeHeadless` from the vendored `bin/` Ghidra. Do not add a BSim
database dependency to the application.

## Contributions

Always minimal code change unless user asks drastic change.
Dont be destructive of features when building new.
Comments must be simple, they are only required for complex code
Use `uv run black .` to clear up python synthax.

## API Development

The backend API uses Flask-RESTX for routing and Swagger documentation.
- **Serialization**: Do not mix `jsonify()` and Flask-RESTX `api.model` marshaling. When using Flask-RESTX `@api.response`, simply return a Python dictionary (e.g., `return {"status": "success"}`) instead of `jsonify(...)` to avoid double-serialization bugs or missing headers.
- **Endpoints**: Routes are defined in `bsimvis/app/swagger.py` and import their implementation from `bsimvis/app/routes/`.
- **Validation**: Rely on Swagger doc and `@api.expect` for parameter validation and schema definition.
