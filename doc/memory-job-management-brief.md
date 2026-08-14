# Memory and job management: what to fix

Written 2026-08-03 after an evening that OOM-killed two full worker fleets while
bridging SightHouse's library corpus (6531 files) into the `stdlib-ref`
collection. Everything below is either an existing tracker item or something
observed directly during that run, with the evidence attached.

Intended as the working brief for a branch covering issue #36 items 2-6, plus
the new findings in part B.

## Starting point

- `dev` at `29bb87e` already contains the merge of **PR #51**
  (`feat/job-system-rework`) — lease-based claims, heartbeat, reaper,
  pause/resume. That is #36 item 1 and it works (see "what already got fixed").
  Merged locally, **not pushed**.
- Tests from that PR pass on the merge: `test_job_leases.py` (uses a fake redis,
  never touches a live queue) and `test_worker_lrem.py`.

## The outage, as evidence

Two fleets of 5 workers, both wiped by cgroup OOM kills, `MemoryMax=3G`:

```
17:30:40  fleet 1 started (5 scopes)
19:28:37  oom-kill   (5min 10s CPU consumed)
19:30:50  oom-kill   (10min 21s CPU consumed)
19:33:40  oom-kill  x2
21:19:20  oom-kill   → fleet 1 gone, queue frozen at 6041 pending
22:09:54  fleet 2 started (5 scopes)
22:37:52  oom-kill
22:40:53  oom-kill
22:43:21  oom-kill
22:47:40  oom-kill
22:56:30  oom-kill   → fleet 2 gone, queue frozen at 378 pending
```

Source: `journalctl --user -b | grep oom-kill`.

Two facts that shape the fixes:

1. **The host was never short of memory.** 19-22 GB free the whole evening. Every
   kill was the *per-worker* cgroup limit, not the kernel reclaiming under
   pressure. Raising or bounding is a per-worker question, not a host question.
2. **The dying job type was `enrich_features`**, not clustering. Its progress
   updates stop at exactly the kill timestamps. The cheap fixes drafted under
   #36 item 5 all target `cluster_service.py` and raw uploads, so **as drafted,
   item 5 would not have prevented this outage.**

## What already got fixed, and what it proves

PR #51's reaper cleared the historical leak on its first pass. Before the merge:
9 jobs marked `running` with no process behind them, including 3
`build_pool_sim` from Jul 6 and 1 `cluster_pool` from Jul 29. After: all
requeued to `pending`. That closes #26 and confirms item 1 works in production,
not just in tests.

It also shows the limit of leases: they recover *jobs*, not the *fleet*. With all
5 workers dead there was nobody left to run the reaper (`worker.py:68`), so the
queue sat frozen until a human relaunched. Twice.

Despite that, the run made progress — the corpus went from 2799 files / 7958
functions to **8997 files / 86559 functions**, 95.8% indexed. The OOMs cost
time, not data.

---

# Part A — existing tracker items

Ordered as in #36, which is the live issue; #43 and #44 were consolidated into it
on Jul 29 and closed as redundant (not as shipped).

### A1. Kvrocks socket timeout — #36 item 2, was #45

`socket_timeout` of 1000 s means a hung command looks like a stuck job for 17
minutes. Lease timings are meaningless while a single socket call can block
longer than the lease TTL, so this gates the value of item 1.

### A2. Crash-safe handlers — #36 item 3, was #37

`INDEX_FUNCTIONS` deletes chunk data before indexing commits, losing functions
silently. Retries are unsafe until handlers are idempotent, so this gates the
reaper's requeue path: a requeued job must not destroy data on its second run.

### A3. Ghidra out-of-process — #36 item 4, was #38

Run analysis in a subprocess, retry twice, then flag the file unanalyzed. The
worker's JVM is also the reason each worker carries a ~1.3-2.4 GB floor, so this
interacts directly with A4 below.

There are 27 `hs_err_pid*.log` JVM crash dumps sitting in the repo root, newest
Jul 27 — evidence this path fires regularly.

### A4. Bounded memory and admission control — #36 item 5, was #44

Verified still present on `dev`:

- `cluster_service.py:304` and `:1613` — `np.ones((size, size), dtype=np.float32)`
  followed by `clusterer.fit(sub_dist.astype(np.float64))`, i.e. a 100 MB
  allocation plus a 200 MB copy, both alive at `size=5000`.
- No `admission`, `job_cost` or memory-token code anywhere in `bsimvis/*.py`.

The drafted sub-items 1-4 (float64 directly, lower the dense threshold,
vectorise the `iterrows()` walk, cap upload size) are all worth doing and all
irrelevant to this outage. Sub-item 5 (memory tokens) is the structurally right
answer but needs the amendment in B2/B3 below.

### A5. Process supervision — #36 item 6, was #43

Nothing restarts a dead worker. `launch_tmux.sh:233` runs each worker as
`bash -c` inside a tmux window, so an OOM kill ends the process, ends the shell,
and the window vanishes — leaving no trace in the UI and no log to read. This is
the single reason the outage lasted hours instead of a minute.

Note the diagnostic cost: because the windows were gone, the only evidence of
what happened was `journalctl --user`. Worker output should survive the worker.

### A6. Kvrocks is the preferred OOM victim — #49

Every process inherits `oom_score_adj = +200`, so under real host pressure the
kernel kills the datastore first. We got lucky: the per-worker cgroup fired
before host pressure ever built. Fixing A4 without fixing this just moves the
target.

---

# Part B — raised by this run, not yet on the tracker

### B1. `WORKER_MEMORY_MAX=3G` leaves almost no room above the JVM

`launch_tmux.sh:117` defaults to 3G. The comment eight lines above budgets
"~2.5 GB RSS per worker (1536 MB heap plus JVM native overhead, measured 2.4 GB
peak)". So the cap sits ~0.5-1.7 GB above a worker's own floor, and any
memory-hungry handler crosses it.

Measured on fleet 3 (fresh workers, cheap `ghidra_analyze` jobs only):
`memory.peak` 1.33 GB / 1.39 GB against `memory.max` 3.0 GB, `oom_kill 0`.

**Do not just raise the number.** Measure first: run the 10 queued
`enrich_features` jobs with a higher cap and read `memory.peak` off the scope
cgroup. If it wants 3.5 GB this is a config fix; if it wants 8 GB, B2 is a real
bounding job. The number decides the work.

Also worth reconciling: `WORKERS_MAX_BY_RAM` at `launch_tmux.sh:109` derives a
worker count from `(MemTotal-8)/2.5`, i.e. a 2.5 GB assumption, while the cgroup
enforces 3 GB. Two different numbers for the same budget.

### B2. `enrich_features` is unbounded and not chunkable

```python
# feature_service.py:682
feature_hashes = list(self.r.smembers(pending_key))
```

Every pending feature hash into one Python list. `stdlib-ref` currently holds
375,755 features. The Redis work below it *is* chunked at 500
(`index_global_features:418`), so the per-chunk structures are bounded — meaning
the initial `SMEMBERS` (~35 MB of strings) does not by itself explain 3 GB, and
the real allocation still needs to be found by measurement.

Two things to fix regardless:

- stream the pending set (`SSCAN`) instead of materialising it;
- make the job resumable. Today it either finishes or dies and restarts from
  zero, which is #36 item 5 sub-item 6. Five kills over two fleets meant the
  same work was attempted repeatedly.

### B3. `enrich_features` is missing from the heavy-token list

The admission-control draft names `cluster_*`, `build_bin_sim` and
over-threshold binary analysis as heavy. The job that actually killed 10 workers
is not on that list. Whatever weight scheme lands should be derived from
measured peaks, not from a hand-picked list of suspects.

And a caveat for whoever implements it: tokens bound *concurrency*, not
*per-job* footprint. With each worker in its own 3 GB cgroup, serialising five
`enrich_features` jobs would not have saved any single one of them. Tokens are
necessary but not sufficient.

### B4. `active_workers` is not a worker count

```python
# job_service.py:771
"active_workers": active_jobs_count,
```

It reports the number of jobs in an active state. During the outage it read
**13 while zero worker processes existed** — 9 of those were the leaseless
zombies. This is the metric that makes a total fleet death look like a busy
system, and it is why the freeze went unnoticed for hours. A real count needs
worker registration (cheap now that leases carry an owner).

### B5. `/api/jobs/stats` throughput fields are dead

`avg_speed: 0.0`, `total_speed: 0.0`, `remaining_items: 0`, `global_eta: 0` —
constant, including while thousands of jobs were draining. So the one obvious
signal for "is the queue moving" is unusable, and detecting the stall took
polling `pending_jobs` by hand every 20 s. Either populate them or drop them.

### B6. Status endpoints silently return zeros without `collection`

`GET /api/index/status` and `/api/features/status` with no `collection`
parameter return an all-zero body and HTTP 200:

```json
{"num_files": 0, "num_functions": 0, "num_indexed": 0, "indexing_ratio": 0}
```

Indistinguishable from a genuinely empty instance. Should be a 400.

### B7. `bsimvis upload` cannot distinguish dedupe from failure

Two symptoms, one root cause — the CLI reports only an aggregate success rate
and exits 0 regardless:

- A group where every file was already present by MD5 reports
  `Success rate: 0.00% (0/19)` and looks like total failure. Two zlib groups hit
  this; both were fine.
- Conversely, a run where every file failed also exits 0. The SightHouse bridge
  works around this by regex-scraping the printed success rate
  (`scripts/sighthouse_bridge.py`), which is not an interface.

Skipped-as-duplicate should be its own count in the summary, and the exit code
should reflect real failures.

---

## Suggested order

Dependencies first, then the thing that actually broke:

1. **A5 + B4** — supervision and an honest worker count. Cheapest, and it means
   the next occurrence self-heals and is visible. Highest value per line.
2. **B1 measurement** — instrument `memory.peak` per scope, run the queued
   `enrich_features` jobs, get the real number. Everything about sizing depends
   on this and it is an afternoon's work.
3. **A1** — kvrocks socket timeout, so lease TTLs mean something.
4. **A2** — crash-safe handlers, so the reaper's requeues are safe.
5. **B2** — stream and checkpoint `enrich_features`, informed by step 2.
6. **A4 + B3** — the cluster allocation fixes and admission control by measured
   weight.
7. **A6** — `oom_score_adj`, before A4 makes the host the binding constraint
   again.
8. **B5, B6, B7** — observability and CLI honesty. Independent, small, do them
   whenever.

## Open queue state, for whoever picks this up

378 jobs pending, no workers running, `stdlib-ref` at 8997 files / 86559
functions / 95.8% indexed (2809 functions unindexed). 10 `enrich_features`, 3
`build_pool_sim` and 1 `cluster_pool` are queued and will be the first heavy
jobs to run — which makes them a ready-made test case for B1 and B2.

The queue is durable and does not need rebuilding: it lives in redis/kvrocks,
and the reaper requeues anything a killed worker was holding.
