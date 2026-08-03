# Memory and job management: what was fixed, and what it measured

Companion to `doc/memory-job-management-brief.md`. The brief said what was
wrong; this says what changed, what was verified, and the one measurement still
outstanding.

Branch `feat/memory-job-management`, off `dev` at `29bb87e`. Six commits, local
only, not pushed.

## Headline

The worker no longer carries a Ghidra JVM.

That single change explains the outage better than anything else in the brief.
`ghidra_service` was the only in-process user of the JVM, and it was started in
`Worker.__init__` for every worker regardless of what that worker went on to
do. Under a 3 GB cgroup cap, a 1.3-2.4 GB resident JVM left roughly 0.6 GB for
the actual job. `enrich_features` needs no Ghidra at all and was OOM-killed ten
times while holding one.

Measured on the worktree stack, before vs after:

| | before (from the brief) | after |
|---|---|---|
| worker process RSS | 1.3-2.4 GB (JVM resident) | **0.18 GiB** |
| scope peak, cheap `ghidra_analyze` | 1.33-1.39 GiB | **0.70-0.86 GiB** |
| headroom under `MemoryMax=3G` | ~1.6 GB | **~2.2 GB** |

The "after" scope peak still includes the JVM, because the Ghidra child runs
inside the worker's systemd scope. The difference is that it is now resident
only during analysis instead of for the worker's whole life.

## What changed

Ordered as the brief's suggested order, not as committed.

### A5 + B4 — supervision and an honest worker count (`60e1064`)

- `scripts/worker-supervisor.sh` owns each worker. The restart loop sits
  *outside* the systemd scope, so a MemoryMax kill takes the worker and leaves
  the supervisor to start the next one. Previously the worker ran as `bash -c`
  directly in a tmux window: the kill ended the process, ended the shell, and
  the window vanished with no log and no trace in the UI.
- Output is tee'd to `logs/<name>.log`, so evidence survives the window.
- The supervisor samples the scope's `memory.peak` and reports it on every exit,
  including OOM kills. That number is otherwise unrecoverable, because the
  cgroup dies with the scope.
- Worker scopes are named `bsimvis-<project>-worker-N.scope`. They are not
  children of the tmux shell, so `kill-session` used to leave them running and
  still draining the queue. Teardown now stops them explicitly.
- `launch_tmux.sh` never passed `--name`, so all five workers self-identified as
  `worker-1`. Workers now get a name plus their pid.
- Workers register in a `workers:alive` ZSET from the existing heartbeat and age
  out when they stop refreshing, exactly as leases do. `active_workers` is that
  count; the old value moved to `active_jobs_count`.
- `WORKERS_MAX_BY_RAM` assumed 2.5 GB/worker while the cgroup enforced 3 GB.
  Both now derive from `WORKER_MEMORY_MAX`.

### B5 — throughput fields (same commit)

Only `similarity_service` ever wrote `total_items`/`processed_items`, so every
speed and ETA field read zero during an `enrich_features` drain.
`update_progress` now records the counters and derives `speed` centrally, and
`global_eta` falls back to elapsed-vs-percent for handlers that report no counts.

### A1 — kvrocks socket timeout (`84c8c64`)

`socket_timeout` was 1000 **seconds**. A hung command blocked ~17 minutes,
far longer than the 60s lease it held, so the reaper requeued work underneath a
job that was still technically alive. Now 30s, connect timeout 5s,
`KVROCKS_SOCKET_TIMEOUT` to override.

### A2 — crash-safe INDEX_FUNCTIONS (same commit)

The handler deleted the functions chunk from kvrocks *before* indexing
committed. A crash in that window lost the functions silently and made the
reaper's requeue actively destructive: the retry found no chunk and returned
success with nothing indexed. The delete now happens only after a successful
commit. Verified safe to replay: `index_functions` writes `SET`/`SADD` keyed by
function id with no counters.

### A6 — kvrocks as preferred OOM victim (same commit, corrected in `b603a17`)

Every process inherits `oom_score_adj=+200` and kvrocks is the largest RSS, so
under host pressure the kernel would kill the datastore first. An unprivileged
process may only *raise* its own score, so kvrocks cannot protect itself — the
worker volunteers instead, at 1000. A killed worker is recoverable; kvrocks is
not. The Ghidra child inherits the score across fork+exec (verified).

### B6 — status endpoints (same commit)

`/api/index/status` and `/api/features/status` defaulted to collection `main`,
so an omitted parameter returned an all-zero body with HTTP 200,
indistinguishable from an empty instance. Now 400.

### B7 — upload dedupe vs failure (same commit, plus `run_upload` fix)

A file already present by MD5 was counted as a failure. Duplicates are now
their own outcome and their own count, and the exit code reflects real failures.

A second bug surfaced during live testing: `run_upload` called an inner
`main(args)` without returning it, so the new exit code never reached the
caller. Fixed.

### B2 — stream and checkpoint enrich_features (`da73ff3`)

`SMEMBERS` of the whole pending set became `SSCAN` in batches of 5000, and each
batch is `SREM`ed once indexed, so the set itself is the checkpoint. A kill now
costs at most one batch instead of restarting from zero — five kills across two
fleets previously meant the same work was attempted five times and never
finished.

### A4 + B3 — cluster allocations and admission control (`62ed89c`)

- `run_clustering` and `run_pool_clustering` built the distance matrix as
  float32 then called `fit(sub_dist.astype(np.float64))`, keeping both the
  100 MB original and its 200 MB copy alive. Allocated as float64 up front.
- Admission control weights are **measured**, not hand-picked. Workers record
  the peak RSS actually observed per job type (kernel `VmHWM`, reset per job via
  `clear_refs`), and admission reserves that cost against a fleet budget.
  Refused jobs are requeued, not failed. A job whose cost alone exceeds the
  budget still runs when nothing else is reserved, or one expensive type would
  deadlock the queue forever.
- Reservations are released with the lease, and the reaper drops reservations
  for jobs no longer in flight and rebuilds the counter — otherwise a worker
  killed between reserving and releasing leaks budget permanently, and enough
  leaks starve the fleet into a standstill that looks exactly like the outage.

This is the brief's point restated: tokens bound *concurrency*, not per-job
footprint. Each worker has its own cgroup, so serialising five
`enrich_features` jobs would not have saved any single one. Necessary, not
sufficient.

### A3 — Ghidra out-of-process (`bff150c`)

The `GHIDRA_ANALYZE` branch moved to `bsimvis/ghidra_job.py`, run as a child
process. Streaming already went over HTTP to the app rather than through the
worker's memory, so the child needs nothing back but an exit code. The worker
retries (`GHIDRA_ANALYZE_ATTEMPTS`, default 3) then flags the file in
`<collection>:files:unanalyzed`. Crash dumps go to `logs/` via `-XX:ErrorFile`
and are gitignored; the 27 in the repo root are no longer reproduced there.

## Tests

New, all runnable standalone with `uv run python <file>`:

| file | tests | covers |
|---|---|---|
| `test_worker_registry.py` | 9 | worker registration, expiry, the 13-jobs/0-workers outage shape, throughput fields, oom_score_adj |
| `test_enrich_resumable.py` | 5 | batched SSCAN, checkpoint-after-index ordering, kill-and-resume |
| `test_job_admission.py` | 11 | measured weights, budget enforcement, rollback, reaper reclaiming leaked reservations |
| `test_ghidra_subprocess.py` | 6 | child process invocation, JVM crash retry, retry budget, flag-unanalyzed, no JVM in worker |

Existing suites still pass: `test_job_leases.py` (17), `test_worker_lrem.py`,
`test_ghidra_project_leak.py`.

**One existing test was found hollow.** `test_worker_lrem.py`'s fake job service
lacked `try_admit`, so the worker loop threw before dispatching and every
claim-release assertion passed while executing nothing at all. Added the stub
and `test_jobs_actually_reach_the_executor`, which asserts a job reaches the
executor — the guard for the guard.

### Integration

`./scripts/wt-test.sh` on the isolated worktree stack: **317/317 passed,
RESULT: PASS**. Teardown correctly stopped all five worker scopes.

### Live verification against a running stack

Real binaries from `~/data/versioned_c/bin`:

- `/api/jobs/stats` returned `active_workers: 5` against `active_jobs_count: 3`
  — the B4 fix, visible.
- `global_eta: 7` during a drain, where it was previously always 0.
- `/api/index/status` and `/api/features/status` returned **400** without a
  collection.
- Full pipeline through out-of-process Ghidra: 2 files, 57/57 functions indexed,
  100% ratio.
- Upload, fresh: `Uploaded 2 / Skipped (dup) 0 / Failed 0`, exit 0.
- Upload, all duplicates: `Uploaded 0 / Skipped (dup) 2 / Failed 0`, exit 0.
  This is the exact case that used to read `Success rate: 0.00% (0/19)` and look
  like a total wipeout.
- Upload to an unreachable host: `Failed 1`, **exit 1**. Previously exit 0.

### A failure worth recording

The first live run crash-looped every worker **123 times**. `OOMScoreAdjust` is
an exec property and systemd rejects it on a *scope* unit with "Unknown
assignment", so nothing started. Two things came out of it:

1. The fix — set `oom_score_adj` from inside the worker process.
2. The supervisor now backs off exponentially (to 120s) when a worker dies
   within 30s, and says plainly that repeated fast failure looks like a
   misconfiguration rather than a crash. At the flat 5s delay the real error was
   buried under thousands of restart lines.

Unit tests would not have caught this. It only appears when the stack runs.

## Tooling

`scripts/job_memory_report.py` prints measured peak RSS per job type against the
fleet budget:

```
fleet memory budget : 22.77 GiB
workers alive       : 5

job type                      measured peak   share of budget
cluster_functions                  0.19 GiB     0.8%
enrich_features                    0.18 GiB     0.8%
ghidra_analyze                     0.18 GiB     0.8%
...
```

## Still outstanding

**The decisive `enrich_features` measurement has not been taken.** The numbers
above come from a 2-file test collection with 265 features. `stdlib-ref` holds
375,755. Those figures show the JVM floor is gone; they do *not* prove what
`enrich_features` costs at real scale.

`scripts/wt-setup.sh` deliberately never touches the main repo's 17G
`data/kvrocks`, so this cannot be answered from the worktree. To answer it, run
the branch against the real collection and read the report:

```bash
uv run python scripts/job_memory_report.py     # after some enrich_features jobs
grep -h 'exited\|OOM-KILLED' logs/*.log        # scope peaks, incl. OOM kills
```

The 351-job queue in the main stack's redis (`jobs:global`) is still intact and
untouched, with the `enrich_features`, `build_pool_sim` and `cluster_pool` jobs
the brief flagged as the ready-made test case.

Only after that number exists should `WORKER_MEMORY_MAX` be changed. The brief's
rule still holds: if it wants 3.5 GB this is a config fix; if it wants 8 GB,
`enrich_features` needs more bounding than streaming gave it.

## Not addressed

- The `iterrows()` walks in `cluster_service.py` (10 sites) and the upload size
  cap — brief sub-items under A4, worth doing, irrelevant to this outage.

Found and fixed along the way: `bsimvis_config.toml` was absent from a fresh
worktree and the upload CLI hard-fails without it. `wt-setup.sh` now seeds it
from the example.
