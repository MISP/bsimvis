# Job system: diagnosis and a simpler redesign

Written 2026-08-28. Scope: `bsimvis/app/services/job_service.py` (1685 lines),
`bsimvis/worker.py`, `bsimvis/app/routes/jobs.py`, `scripts/worker-supervisor.sh`,
`launch_tmux.sh`. Companion to the memory-management docs (`memory-job-management-*.md`,
`BUG-jobs-processing-leak.md`) — those fixed *reliability* (leaks, OOM, lease/reaper).
This is about *complexity*: too many ways to do the same thing, and operations
(stop, retry, restart) that don't behave the way a human expects them to.

---

## 1. What's actually in there today

`JobService` implements, on raw Redis lists/hashes/ZSETs, all of the following
**independently**:

| Concern | Mechanism | Where |
|---|---|---|
| Sequencing (A then B) | `create_pipeline` / `task_ids` list, `advance_parent` | :183, :599 |
| Fan-out/fan-in (A,B,C then D) | `create_group` + barrier count, `create_group(...)+ tasks=[group_id]+rebuild` | :257, :439 |
| Insert work into a running pipeline | `splice_tasks` (WATCH/MULTI on the JSON blob) | :1046 |
| Per-collection serialization ("one thing at a time per collection") | `submit_to_lane` / `advance_lane`, CAS'd `lane:<coll>:active` key | :361, :385 |
| Debounced auto-clustering after a batch upload | `open_or_extend_wave` / `seal_wave`, a *second* deadline+list state machine | :397, :411 |
| Priority | a second Redis list (`jobs:pending:high`) + a hardcoded type allowlist | :508 |
| Crash recovery | leases (ZSET + TTL) + a reaper + `MAX_ATTEMPTS` | :58, :824 |
| Fleet memory protection | measured-peak admission control, its own reserved/used HASH | :67-77, :789 |
| Pause | fleet-wide flag *and* per-job/per-ancestor flag, each walked separately | :929, :940 |
| Retry | recursive hash reset across descendants, skip-if-completed, walk-to-ancestor | `routes/jobs.py:98`, and the `jobsystem-lane-retry` worktree fix |
| Cancel | recursive status flip, **not observed by running code** except in 3 handlers | :1210 |
| Idempotent enqueue | a `queued` latch field, cleared in 4 different places | :495, worker.py:242,279 |

That's 12 orthogonal state machines sharing one job hash, each with its own
flag (`queued`, `paused_queued`, `barrier_fired`, `lease_owner`, `attempts`,
`lane_collection`, `wave_deadline`...). They interact: the
`jobsystem-lane-retry` worktree (commit `90ef2d2`, 2026-08-26) exists because
retrying a job could replay a terminal transition and corrupt the lane
pointer — a CAS (`expected_unit_id`) had to be threaded through
`advance_lane`/`complete_job`/`fail_job` to fix it. That's the pattern: each
bug fix adds another parameter or another guard flag to the same state
machine, because the state machine was never given a formal shape to begin
with.

This matches exactly what you're describing as "too many ways to split big
tasks": **pipeline**, **group**, **splice into a running pipeline**, **wave/
debounce grouping**, and **lane queueing** are five different answers to "how
do I structure related work," built up over time rather than designed at
once, plus job creation that happens ad hoc from request handlers alongside
job creation that happens from pre-planned "build the whole graph up front"
call sites (`build_rebuild_all_tasks`, etc).

---

## 2. Your specific complaints, traced to code

### "Too many ways to split big tasks"

Confirmed above. Concretely: a big upload creates individual leaf jobs
(md5-targeted, one per file), which get spliced into a group mid-flight
(`splice_tasks`, because chunks arrive in parallel over HTTP as the upload
streams), which get sealed into a wave-group on a debounce timer
(`seal_wave`), which gets wrapped in a pipeline with rebuild steps appended,
which gets submitted to a per-collection lane. **Five layers of indirection
for "run these files, then re-cluster."**

### "API and UI slow with too many subtasks"

`get_job_status` (job_service.py:1087) does one `hgetall` per sub-task,
**unpipelined**, in a Python loop (:1117-1148). A pipeline with hundreds of
spliced md5-targeted children (exactly what a big upload produces) means
hundreds of sequential round-trips to Redis to render one detail page.
(`list_jobs`/`get_global_stats` were already fixed to pipeline their fetches —
this one wasn't.) The UI then presumably re-polls this on an interval for any
open job panel, multiplying the cost by however many panels are open.

### "Stopping a job isn't immediate"

`cancel_job` (:1210) only ever flips a Redis field. Nothing makes the worker
executing that job aware of it *unless the handler code polls
`is_cancelled()` itself* — and only **3 of the ~32 job types** do
(`ghidra_job.py:258`, `llm_batch_service.py:267`, `analysis_orchestrator.py:854`).
Every other type — `build_sim`, every `cluster_*`, every `index_*`,
`enrich_features` — runs to completion regardless of cancellation, and
`complete_job` at the end will happily overwrite the `cancelled` status back
to `completed`. "Cancel" today means "don't start this if it hasn't started
yet" for 29 of 32 job types, not "stop what's running."

### "Restarting has to wait a long time for workers"

Two separate mechanisms, both by design, neither configurable per-situation:

1. `Worker.stop()` (worker.py:131) only sets `self.running = False`. That flag
   is checked at the top of `_run_loop`'s `while` — **never inside
   `_execute_job`**. A SIGTERM sent while a worker is mid-job (a
   `cluster_pool` on an 11M-pair pool, an `enrich_features` batch, a Ghidra
   analysis) waits for that job to finish naturally before the process exits.
   There is no hard-stop path.
2. `scripts/worker-supervisor.sh` restarts a dead worker with exponential
   backoff up to `WORKER_MAX_RESTART_DELAY=120` seconds if it dies within
   `WORKER_FAST_FAIL_SECONDS=30` of starting (lines ~85-140). This exists to
   stop a genuinely crash-looping worker from spamming logs — but it also
   fires during normal iterate-and-restart-to-pick-up-code-changes
   development, where the previous exit *looks* like a fast failure.

Neither of these is a queue problem — jobs sit in Redis fine — it's that the
worker process itself has only one gear (cooperative, whole-job-granularity
stop) and the supervisor can't distinguish "developer restarting" from
"crash loop."

### "I want to rerun a specific failed/frozen job and continue the pipeline"

This mostly exists (`retry_job`, `routes/jobs.py:135`, improved further by the
uncommitted `jobsystem-lane-retry` worktree to skip already-completed
descendants and un-stall cascade-cancelled siblings) — but "frozen" is the
gap. A frozen (not crashed, not exception-raising, just stuck) job holds its
lease indefinitely via the heartbeat thread refreshing it every `LEASE_TTL/3`
regardless of whether the *job* is making progress — the lease only proves
the *process* is alive, not that the job is moving. There's no operator lever
between "wait for the 60s lease to expire on its own after killing the
process" and "kill the whole worker and hope the supervisor doesn't
backoff-delay the restart."

### "Not enough flexibility, workers don't always pick it up"

Falls out of the pause/lane/admission stack: a retried or requeued job can
land behind (a) a fleet-wide pause flag, (b) its own or an ancestor's
per-job pause flag, (c) a lane that's still "active" pointing at a stale or
already-finished unit (the exact bug `90ef2d2` patches one instance of), or
(d) admission control deferring it because reserved memory wasn't released
(mitigated, not eliminated, by the reaper). Four independent reasons a
"ready" job might not run, each requiring separate operator knowledge to
diagnose.

---

## 3. Root cause

This isn't a series of unrelated bugs. It's a **hand-rolled distributed task
queue** — broker, worker pool, retry policy, task composition (chain/group),
crash recovery, and resource admission — built incrementally on raw Redis
primitives, where every new requirement (debounced batch clustering, MD5
dedup, per-collection serialization, memory-aware scheduling) got its own
bespoke state machine instead of being expressed in terms of the ones already
there. Mature task-queue libraries solve this exact problem, and mostly
already have.

---

## 4. Is Celery a fit? Yes — you've already reinvented most of its shape

Map current concepts to Celery's canvas primitives directly:

| bsimvis today | Celery equivalent |
|---|---|
| `create_pipeline` (sequential `task_ids`) | `chain(a.s(), b.s(), c.s())` |
| `create_group` (parallel, barrier) | `group(a.s(), b.s(), c.s())` |
| `seal_wave` (group + rebuild-after) | `chord(group(...), rebuild.s())` — literally what a chord *is* |
| `splice_tasks` (insert into a running pipeline) | not needed the same way — a chord's callback fires once every group member reports in, so late-arriving members join the group before it's dispatched instead of being surgically spliced into a JSON list mid-flight |
| md5-targeted per-file leaf jobs | ordinary `.s(file_md5=...)` task signatures — no special-casing |
| Lane (one top-level unit per collection at a time) | a low-concurrency queue per collection, or `celery.contrib.abortable` + a Redis lock — the one piece with no first-class primitive, see §6 |
| Leases + reaper + `MAX_ATTEMPTS` | native: `task_acks_late=True` + broker visibility timeout requeues a task whose worker died; `autoretry_for` + `retry_backoff` replaces the hand-rolled attempts counter |
| `cancel_job` (flag only) | `AsyncResult.revoke(terminate=True, signal='SIGTERM')` — with the **prefork** pool this sends a real signal to the OS process running the task, killing it immediately, not just marking a flag three handlers might check |
| Fleet memory protection | `worker_max_memory_per_child` recycles a worker child once its RSS crosses a threshold — same effect as the custom admission control, without a second Redis-backed budget ledger |
| Per-job timeout / stuck-job detection | `task_time_limit` (hard, SIGKILL after) / `task_soft_time_limit` (raises in-process) — replaces "wait for the 60s lease to expire" with an actual bound you set per task |
| `/api/jobs/*` status polling | Celery result backend (can stay Redis) + `AsyncResult.state`; Flower gives you a working monitoring UI for free instead of hand-building progress aggregation |

You are not choosing *whether* to have a broker, a worker pool, retries, and
task composition — you already built all of those. The question is whether
to keep maintaining a bespoke implementation of them (with its own new bugs,
like `90ef2d2`) or use one with 15 years of exactly these edge cases already
closed.

### What doesn't map cleanly, and has to stay deliberate

- **Per-collection lane serialization.** Celery has no "one thing at a time
  for this dynamic key" primitive. Cleanest fit: route lane-sensitive task
  types to a queue named `lane.<collection>` and run exactly one consumer
  against it (Celery lets you set `-Q` per worker, or use `task_routes` with
  a dynamic queue name), or keep a small Redis lock (`SET NX EX`) around
  dispatch. Either is a fraction of the current lane/wave code.
- **Measured (not hand-picked) per-type memory weights.** Worth keeping as
  observability (`scripts/job_memory_report.py` already does this well) even
  after moving admission itself to `worker_max_memory_per_child` — use the
  measured peaks to *set* that number per queue/worker pool rather than to
  run a live reservation ledger.
- **Ghidra subprocess isolation** (`ghidra_job.py`) carries over unchanged —
  it's just what a Celery task's body does, same as today.
- **`bsimvis_job.py` CLI** and the upload MD5-dedup logic are orthogonal to
  the queue implementation and don't need to change.

### Why Celery specifically over RQ / Dramatiq

- RQ has no native chain/group/chord — you'd be rebuilding exactly the
  pipeline/group layer you're trying to delete.
- Dramatiq's pipelines/groups exist but its ecosystem and revoke/terminate
  story are thinner; Celery's `terminate=True` + prefork is the direct fix
  for "stop isn't immediate," which is a headline complaint here.
- You already run Redis for the current queue — Celery's Redis broker needs
  no new infrastructure.

---

## 5. What this fixes, directly

- **Stop isn't immediate** → `revoke(terminate=True)` sends SIGTERM (or
  SIGKILL) to the actual OS process running the task. No more auditing every
  handler for an `is_cancelled()` poll.
- **Restart waits too long** → Celery worker shutdown is a first-class
  concept: warm (`TERM`, finish current tasks — same as today) vs cold
  (`QUIT`/second `TERM`, stop immediately) is one signal choice, not a
  115-line bash supervisor guessing intent from restart timing. `celery
  multi restart` / systemd `Restart=on-failure` replaces
  `worker-supervisor.sh`'s hand-rolled backoff outright.
- **Rerun a specific job and continue the pipeline** → this is what
  `chain`/`chord` error handling is *for* natively (`link_error`, retry the
  failed signature, the chain resumes from there) instead of a recursive
  hand-written descendant-reset that needed a follow-up patch to stop
  corrupting sibling state.
- **API/UI slow with subtasks** → Flower (or a thin custom UI over the
  result backend) doesn't N+1 Redis per subtask on every poll; Celery tracks
  group/chord membership itself.
- **Too many ways to split work** → one vocabulary: task, chain, group,
  chord. Delete `splice_tasks`, the lane/wave dual state machine, and the
  five-layer upload-to-cluster indirection in favor of "upload tasks → group
  → chord callback rebuilds," which is one concept instead of three.

---

## 6. What I'd actually do — not a big-bang rewrite

The memory-management work already proved incremental works here and
big-outage rewrites don't need to happen twice. Suggested order:

1. **Spike, not migrate.** Stand up Celery against the existing Redis
   instance for one job type end-to-end (e.g. `ghidra_analyze`, already
   subprocess-isolated so it's the cleanest boundary) behind a feature flag.
   Confirm `terminate=True` really does hard-kill a Ghidra child, confirm
   `worker_max_memory_per_child` behaves sanely under the same load the
   memory-management docs measured against.
2. **Port the lane primitive** as a small, separate, well-tested module
   (queue-per-collection or lock-based) — this is the one piece with no
   Celery equivalent, so it deserves to be the one deliberately-designed
   custom piece rather than an emergent one.
3. **Migrate job types one at a time**, oldest/simplest first
   (`build_sim`, `idx_*`), each replacing its slice of `job_service.py` and
   deleting the corresponding dispatch branch in `worker.py`.
4. **Migrate the pipeline/group/wave call sites last** — `seal_wave` becomes
   one `chord()` call; `build_rebuild_all_tasks` becomes a chain; delete
   `splice_tasks`, `submit_to_lane`'s bespoke queue, `open_or_extend_wave`.
5. **Keep** `job_memory_report.py`-style measurement throughout — it's
   infrastructure-agnostic and is exactly what should inform
   `worker_max_memory_per_child` and per-queue concurrency once Celery owns
   admission.

Each step is independently shippable and reversible, same shape as the
memory-management branches that already worked. The payoff compounds: every
step deletes bespoke state-machine code instead of adding another flag to it.
