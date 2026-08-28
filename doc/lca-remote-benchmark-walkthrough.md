# Deploying LCA on a remote box and benchmarking rust_cpu vs wgpu

Goal: stand up a second, LCA-enabled BsimVis instance next to an existing
production/dev instance on the same remote machine, pull two real collections
across (files + functions only, no similarity/cluster data), then run the
full pipeline (similarity build, function clustering, binary similarity,
binary clustering) once under `rust_cpu` and once under `wgpu`, and keep
whichever result you decide to keep as a normal, working collection -- not a
throwaway benchmark environment.

Three scripts back this walkthrough, all in `scripts/`:

| script | does |
|---|---|
| `migrate_collection.py` | kvrocks-to-kvrocks copy of files+functions for named collections, then rebuilds every derived index on the target using the app's own real indexing code (not a raw key dump) |
| `switch_backend.sh` | edits `discovery_backend` in `bsimvis_config.toml` and restarts the stack (`launch_tmux.sh --clear`) so it actually takes effect |
| `benchmark_pipeline.py` | drives the real HTTP API (`/api/similarity/rebuild`, `/api/cluster/rebuild_all`) end to end, polls jobs, reports wall time + summed per-stage compute time |
| `quick_bench_backends.py` | optional pre-check: runs `legacy_proxy`/`rust_cpu`/`wgpu` head-to-head on a throwaway copy of one collection, in-process (no job queue, no config edit needed), `N` repeats with mean/median. Nothing it does persists. Good for "does this even move the needle" before committing to the full deploy-and-migrate flow below |

All three were exercised locally against a real collection (migrate ->
verify via search/API -> full rebuild -> counts match source) before being
handed over. See [[lca-hierarchical-uf-fix]] memory for the clustering-engine
prerequisite this branch needed fixed first.

## 0. What you need decided before starting

- **Two source collection names** on the existing instance, and what you want
  them called on the new instance (same name is simplest).
- **Network path** from the new box to the existing instance's Kvrocks port.
  Direct copy needs a live connection to that port for the duration of the
  migration -- open the firewall or `ssh -L 7777:localhost:<old_kvrocks_port>
  user@old-host` and point `--source-port` at the tunnel's local port.
- **GPU**, if you want to actually test `wgpu`: check what's there and which
  backend `wgpu` will pick.
  ```
  .venv/bin/python3 -c "import bsimvis_similarity_native as sn; print(sn.wgpu_adapter_info())"
  ```
  An integrated GPU still runs it, but on a small corpus (low thousands of
  functions) the dispatch/transfer overhead can beat rust_cpu outright --
  ran `scripts/quick_bench_backends.py` on this branch's own local
  1419-function test collection and got rust_cpu ~0.95s mean vs wgpu ~1.34s
  mean over 4 repeats (GPU *slower*, on an Intel integrated GPU). Don't
  assume wgpu wins going in on a small corpus -- run
  `quick_bench_backends.py` first if you're unsure it's worth the full
  deploy; that's the whole point of benchmarking it on your actual data.

## 1. Deploy the new LCA instance on the remote machine

Give it its own directory and its own ports -- it runs next to the existing
instance, not in place of it.

```bash
# on the remote machine
git clone <this-repo-url> bsimvis-lca
cd bsimvis-lca
git checkout worktree-lca-acceleration   # or whatever ref you're deploying

cp bsimvis_config.toml.example bsimvis_config.toml
```

Edit `bsimvis_config.toml`:
```toml
[clustering]
    engine = "hierarchical_uf"
    bin_engine = "hierarchical_uf"
[similarity]
    discovery_backend = "rust_cpu"   # start on CPU; switch later
```
(These are already the `.example` defaults on this branch as of this
commit -- confirm rather than assume if you're on a different ref.)

Write `.env` with ports that don't collide with the existing instance:
```bash
cat > .env <<'EOF'
GHIDRA_INSTALL_DIR=/path/to/bin/ghidra_12.1_PUBLIC
APP_HOST=0.0.0.0
APP_PORT=5500
REDIS_HOST=localhost
REDIS_PORT=7500
KVROCKS_HOST=localhost
KVROCKS_PORT=7999
WORKERS_COUNT=5
PROJECT_NAME=bsimvis-lca
DATA_BASE_DIR=/path/to/bsimvis-lca/data
EOF
```

Build the native similarity extension. `rust_cpu` alone doesn't need the
`gpu` Cargo feature; `wgpu` does, and needs Vulkan/Metal/DX12 drivers +
loader present on the box (Linux: `vulkan-tools`, `mesa-vulkan-drivers` or
the vendor driver; check with `vulkaninfo`).

```bash
uv run maturin develop --release \
  --manifest-path native/bsimvis_similarity/Cargo.toml --features gpu
```
(Building with `--features gpu` doesn't force GPU use -- `discovery_backend`
in config controls that. Building it once with gpu enabled just means you
can switch to `wgpu` later without rebuilding.)

Install the rest of the Python deps (`uv sync` or your usual flow), then
bring the stack up:

```bash
./launch_tmux.sh --clear
```
Wait for `Dashboard: http://localhost:5500` to print. Confirm the backend
actually loaded:
```bash
.venv/bin/python3 -c "
import sys; sys.path.insert(0,'.')
from bsimvis.app.services.config_service import config_service
print(config_service.get('similarity.discovery_backend'))
print(config_service.get('clustering.engine'), config_service.get('clustering.bin_engine'))
"
```

## 2. Migrate the two collections

Dry-run first -- it only counts and prints, writes nothing:
```bash
.venv/bin/python3 scripts/migrate_collection.py \
  --source-host <old-instance-host-or-tunnel> --source-port <old-kvrocks-port> \
  --target-host localhost --target-port 7999 \
  --collection <coll_a> --collection <coll_b>
```
Then for real:
```bash
.venv/bin/python3 scripts/migrate_collection.py \
  --source-host <old-instance-host-or-tunnel> --source-port <old-kvrocks-port> \
  --target-host localhost --target-port 7999 \
  --collection <coll_a> --collection <coll_b> --apply
```
The last step it runs (vector-class / feature-registry rebuild) is CPU-bound
and proportional to function count -- it repeats the indexing cost the
*source* instance already paid once, per function. Budget real time for a
large collection (thousands to tens of thousands of functions); it's a
one-time cost, not part of either backend benchmark.

`--force` is required if the target collection name already has data
(refuses by default, so a mistyped `--collection` can't silently merge into
something that's already there). Rename during migration with
`--collection old_name:new_name`.

**What this does NOT bring over**, by design (scope was "files and functions,
no similarity"): similarity pairs, function/binary clusters, bin_sim docs,
LLM analyses, notes, job history. Section 3 rebuilds the similarity/cluster
layer fresh on the target -- that's the whole point of the benchmark.

Sanity-check before benchmarking: open `http://<new-host>:5500` and confirm
the two collections list their files/functions with correct names (not the
source collection's name leaking through), and that Cluster / Bin Sim tabs
are empty (nothing migrated there yet). Or via API:
```bash
curl -s "http://localhost:5500/api/search/unified?collection=<coll_a>&q=<some_known_function_name>"
curl -s "http://localhost:5500/api/cluster/list?collection=<coll_a>" | python3 -c "import json,sys;print(json.load(sys.stdin)['total'])"
# should print 0 -- nothing clustered yet
```

## 3. Benchmark rust_cpu

Backend should already be `rust_cpu` from step 1. Confirm, then run:
```bash
.venv/bin/python3 scripts/benchmark_pipeline.py \
  --base-url http://localhost:5500 \
  --collection <coll_a> --collection <coll_b> \
  --backend-label rust_cpu \
  --out bench_rust_cpu.json
```
This leaves `<coll_a>`/`<coll_b>` fully built (similarity, function
clusters, binary similarity, binary clusters) under rust_cpu -- a real,
browsable state, not cleaned up after.

## 4. Benchmark wgpu

Switch backend and restart (a few seconds of downtime; kvrocks/redis
shutdown is graceful, data on disk is untouched):
```bash
./scripts/switch_backend.sh wgpu
```
Then rerun the exact same benchmark, which clears and rebuilds everything
fresh under wgpu:
```bash
.venv/bin/python3 scripts/benchmark_pipeline.py \
  --base-url http://localhost:5500 \
  --collection <coll_a> --collection <coll_b> \
  --backend-label wgpu \
  --out bench_wgpu.json
```

## 5. Compare and decide

Both runs printed a summary table (wall time, summed per-stage compute time,
final function/binary cluster counts). Diff the two JSON reports for exact
numbers:
```bash
python3 -c "
import json
a = json.load(open('bench_rust_cpu.json'))
b = json.load(open('bench_wgpu.json'))
for coll in a['collections']:
    ra, rb = a['collections'][coll], b['collections'][coll]
    print(coll)
    for stage in ('similarity_rebuild', 'cluster_rebuild_all'):
        print(f\"  {stage}: rust_cpu={ra[stage]['compute_seconds']:.2f}s wgpu={rb[stage]['compute_seconds']:.2f}s\")
"
```
Also compare `final_counts` between the two reports -- they should match
closely (same functions, same thresholds); a large divergence in
`func_clusters`/`bin_clusters` between backends on the *same* data is a
correctness signal worth chasing before trusting either number, not just a
speed one (this happened at small scale locally: LCA discovery found ~9%
fewer pairs than the pre-LCA path on the same corpus).

Whichever backend wins, that's naturally what's left active (you just ran it
last). If you want the *other* one active instead, run
`./scripts/switch_backend.sh <backend>` one more time and rerun
`benchmark_pipeline.py` for that backend so the live collections match the
backend actually configured -- don't leave rust_cpu-built cluster data
sitting under a `wgpu`-configured instance, they can drift silently on the
next incremental upload.

## Caveats

- `migrate_collection.py` needs the source Kvrocks port reachable for the
  whole run (SSH tunnel if it isn't directly). It does not touch the source
  instance at all -- read-only `GET`/`SMEMBERS`/`ZRANGE`, no writes.
- `switch_backend.sh` restarts the *whole* stack (redis+kvrocks+app+workers),
  not just the workers -- simpler and already proven safe (data on disk
  survives kvrocks's graceful `SHUTDOWN`), but it's a few seconds of
  downtime each switch, and it will drop any job that happens to be running
  at that moment (fine here since nothing else should be running on this
  instance during a dedicated benchmark).
- `benchmark_pipeline.py`'s wall time includes time spent queued behind
  other lane work; compute time (summed `perf_total` across sub-tasks) is
  what actually isolates backend speed. Report both, trust compute for the
  backend comparison.
