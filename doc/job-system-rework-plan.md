# Job system rework: implementation plan

Companion to `doc/job-system-diagnosis-and-redesign.md` (root-cause diagnosis,
Celery mapping, why). This is the concrete plan: what ships, in what order,
against what branch, with what acceptance bar. Decisions below were locked
via two rounds of design questions on 2026-08-28; each item cites which
answer drove it.

**Target branch: `dev-lca-merged`**, not `dev`. It's `dev` plus the
LCA-acceleration work (native Rust/GPU similarity engine, LCA same-class
matching) merged in, and it introduces a new monolithic-job source this plan
must cover. Verified `job_service.py`/`worker.py` are byte-identical between
`dev` and `dev-lca-merged` as of this writing, so the diagnosis doc's
file:line references hold unchanged — only the base for new work moves.

---

## 0. Decisions locked

| Question | Answer |
|---|---|
| Architecture | Migrate to Celery (chain/group/chord), phased, type-by-type |
| Stop semantics | Cooperative checkpoint first, hard-kill (`revoke(terminate=True)`) after a grace period |
| Logging | Rich per-job structured logs, live tail, **plus** speed/ETA/processed-total progress fields (your pasted `sim/s` log line is the target shape) |
| Upload wave (dynamic splice case) | Keep the idle-timeout heuristic — no upload-client protocol change |
| Chunked `build_bin_sim` (dynamic splice case) | Replace self-splicing with a single resumable job, internal loop, checkpointed — same shape as `enrich_features` |
| Failure states to distinguish | Crashed/worker-died, timed-out/frozen, retries-exhausted, logic/data error |
| Live UI updates | Push (SSE/websocket), not polling |
| Monolithic-job splitting | **Full scope** — split every known offender in this plan, not just a reference case |
| Split-job performance | Must not regress throughput materially — measured, not assumed (§7) |

---

## 1. Target architecture

Unchanged from the diagnosis doc's §4/§6 mapping — restated briefly:

- `create_pipeline` → `chain`; `create_group` → `group`; `seal_wave` → `chord`
  (group + callback is literally what a chord is).
- `splice_tasks` is **deleted**, not ported. Its only two callers are handled
  differently: the wave path never used it (already collect-then-group); the
  `build_bin_sim` chunk path becomes a single resumable task (§7.2).
- Per-collection lane serialization has no Celery primitive — ported as a
  small dedicated module: one queue per collection (`lane.<collection>`),
  exactly one consumer, or a `SET NX EX` lock around dispatch. This is the
  one deliberately-custom piece, kept small and separately tested.
- Leases + reaper + `MAX_ATTEMPTS` → native `task_acks_late` (broker
  visibility timeout requeues a dead worker's task) + `autoretry_for` +
  `retry_backoff`.
- Fleet memory protection → `worker_max_memory_per_child`, tuned from
  `job_memory_report.py`'s measured peaks instead of a live reservation
  ledger.
- `cancel_job` → `AsyncResult.revoke()`, cooperative first (§4), then
  `terminate=True` (real SIGTERM/SIGKILL to the task's OS process via the
  prefork pool).

---

## 2. Job API redesign

Replace `bsimvis/app/routes/jobs.py` + the relevant slice of `job_service.py`
with a thinner layer over Celery's result backend plus the lane module.

**Endpoints** (REST shape unchanged where it already made sense, semantics
fixed):

- `GET /api/jobs` — flat list of top-level units (chain/group/chord roots),
  paginated, filterable by collection/pool/status/type. Same shape as
  today's `list_jobs`, already pipelined — keep as is.
- `GET /api/jobs/<id>` — one unit's detail. **No more recursive N+1
  `hgetall`** (diagnosis doc §2) — subtask status comes from Celery's own
  group/chord result tracking, fetched pipelined.
- `GET /api/jobs/<id>/stream` — **new**, SSE. Pushes status, progress
  (`processed/total`, `speed_avg`, `speed_current`, `eta`), and log lines as
  they happen. Job view subscribes to this instead of polling (§3, §5).
- `POST /api/jobs/<id>/stop` — cooperative cancel; escalates to hard kill
  after `STOP_GRACE_SECONDS` (config, default proposed 15s) if the task
  hasn't exited. Returns immediately; terminal state arrives over the
  stream.
- `POST /api/jobs/<id>/restart` — **restart this step only**, pipeline
  resumes after it succeeds. This is `retry_job`'s already-fixed semantics
  (`jobsystem-lane-retry` worktree, commit `90ef2d2`) reimplemented as:
  re-invoke the failed task's signature, `link()` it back into the
  remaining chain from that point. Already-completed steps are untouched.
- `POST /api/jobs/<id>/restart-all` — **new, distinct** from the above:
  reset the whole top-level unit and every descendant, re-run from the
  start. For when the failure indicates upstream data was bad, not just
  that one step.
- `POST /api/jobs/<id>/skip` — **new**. Marks a permanently-broken step
  `skipped` and advances the pipeline past it, for the "logic/data error,
  not worth retrying, don't block everything downstream" case surfaced by
  the failure-taxonomy work.
- `POST /api/jobs/pause` / `resume` (fleet-wide) and per-job pause/resume —
  kept, same semantics as today.

**Job/task schema** (returned by `GET /api/jobs/<id>` and pushed over the
stream):

```
{
  "id": "...", "type": "cluster_pool", "status": "failed",
  "failure_reason": "crashed",       // crashed | frozen | retries_exhausted | error | null
  "failure_detail": "OOM-killed (rc=137), attempt 3/3",
  "attempts": 3, "max_attempts": 3,
  "progress": 62,
  "processed_items": 6996000, "total_items": 11300000,
  "speed_avg": 3820.3, "speed_current": 8968.1,   // items/s
  "eta_seconds": 917,
  "phase": "adj_sim",                // current internal checkpoint, see §5
  "peak_rss_bytes": 4364838912,
  ...
}
```

`failure_reason` is derived, not hand-set per call site: the reaper/worker
classify it at the moment a job becomes terminal (lease expiry with no clean
`fail_job` call → `crashed`; lease held but `processed_items`/`updated_at`
stalled past a threshold → `frozen`; `attempts >= max_attempts` → wraps
whichever of the above caused the last attempt as `retries_exhausted`; a
clean exception/`False` return → `error`). This directly answers your pasted
log ("Abandoned after 3 attempts... Lease expired 3 times") — that becomes
`failure_reason: retries_exhausted`, `failure_detail: "crashed x3 (worker
died each time)"` instead of an opaque final log line.

---

## 3. Job view UI

Two-level layout, not a fully recursive tree:

- **Unit list** (top-level chains/groups/chords): flat, paginated, filtered
  — same as today's list, already reasonably fast.
- **Unit detail panel**: opens via SSE subscription (`/stream`), not a
  poll-on-interval fetch. Subtasks render from the pushed group/chord state;
  a unit with hundreds of members (a sealed upload wave) virtualizes the
  list client-side rather than rendering every row.
- **Controls per unit/task**: Stop, Restart (this step), Restart all, Skip,
  Pause — wired directly to the §2 endpoints. Buttons reflect what's
  actually legal for the current `status`/`failure_reason` (e.g. "Restart
  all" is not offered for a job that's still running).
- **Status badges** use the failure taxonomy, not a single red "failed" —
  distinct icon/color for crashed vs frozen vs retries-exhausted vs
  logic-error, so a scan of the job list tells you which failures are worth
  a code fix vs which are worth a click-to-retry.
- **Progress** shows `processed/total`, `speed_current`/`speed_avg`, ETA,
  and the current `phase` string when a handler reports one (§5) — the
  format you pasted, generalized to every job type that reports counts.
- **Log panel** streams live, leveled (debug/info/warn/error), filterable by
  level client-side.

---

## 4. Stop / restart semantics, precisely

Per the locked "cooperative, then hard-kill" answer:

1. `POST .../stop` sets the Celery revoke flag and (for the prefork pool)
   sends the configured signal. Handlers that already checkpoint (the
   resumable ones from §7) see it on their next checkpoint and exit cleanly,
   preserving partial progress.
2. If the task hasn't transitioned to a terminal state within
   `STOP_GRACE_SECONDS`, escalate to `terminate=True` — the OS process is
   killed outright. This is the actual fix for "stopping isn't immediate":
   today only 3 of ~32 handlers poll `is_cancelled()` at all, so this is the
   backstop for the other 29 without requiring every handler to be audited
   first.
3. A hard-killed task's partial state is whatever its last checkpoint wrote
   — this is *why* §7's chunking work matters beyond memory: a job with no
   checkpoints loses all progress on hard-kill, one with checkpoints resumes
   near where it stopped via `restart` (§2).

Restart-this-step vs restart-all are two distinct, separately-tested code
paths per §2 — this directly satisfies "restart, and restart and keep on
pipeline" as two different buttons instead of one overloaded action.

---

## 5. Logging & progress infrastructure

The current per-job log is a capped Redis **list** (`job_log:<id>`, `LTRIM`
to 100 lines). Your pasted example — one line every ~1-2s for a
20M-similarity indexing run — would blow through 100 lines in under 3
minutes and lose everything before that. Fix:

- **Redis Stream** (`XADD`) per job instead of a capped list. Natural fit
  for the SSE tail (`XREAD` from the last-seen id), trims by time+count
  instead of a hard line cap, and doesn't require polling to detect new
  entries.
- **Structured entries**: `{ts, level, message, processed, total, speed_avg,
  speed_current, phase}` — the progress fields ride the same stream as the
  log lines, so "more logs" and "more progress/speed" are the same piece of
  work, not two.
- **`phase()` checkpoints, standardized**: `cluster_service.py` already has
  an informal version of this (`mem_util.phase(label, job_service, job_id)`,
  lines 184/208/220/234/681/2160/2500). Promote it to the shared progress
  API every handler uses — one call updates phase, progress %, item counts,
  and RSS in one write, and it's what feeds both the job-view progress bar
  and the "why did this die" post-mortem.
- **Worker-death diagnostics**: today "worker kept dying" (your pasted log)
  has no recorded reason per attempt. The reaper should record *why* each
  lease expired if determinable (OOM rc=137 from the supervisor, vs. no
  signal at all i.e. genuinely frozen) into `failure_detail`, not just
  increment a counter.

This phase of work stands on its own — it's valuable even before any Celery
migration lands, since it's the thing that makes diagnosing the next
`enrich_features`-style outage fast instead of "read `journalctl` and
reconstruct it by hand" (as the memory-management brief had to do).

---

## 6. Splice fixes

- **Upload wave**: no change to the trigger mechanism (idle-timeout kept per
  the locked answer). What changes is only the *representation* — becomes a
  literal `chord(group(file_tasks), rebuild_callback)` once sealed, instead
  of a hand-rolled group+pipeline-wrapping dance through `seal_wave`.
- **Chunked `build_bin_sim`**: delete the self-splice entirely
  (`bin_sim_service.py:354` and the `splice_tasks` method). Total pair count
  is already known before the first chunk runs (`total_pairs`, computed at
  line ~300) — replace with one task that loops `offset` internally,
  checkpointing via the §5 progress API every `CHUNK_SIZE` pairs. One job
  row instead of N, no splice race possible because there's nothing to
  splice.
- Once both are gone, `splice_tasks`, its `WATCH`/`MULTI` retry loop, and
  the "resolve once up front to avoid duplicate orphaned children on
  `WatchError` retry" comment it required all delete cleanly — there are no
  other callers (verified by repo-wide grep).

---

## 7. Splitting the monolithic jobs — full scope

Every handler below either has a measured peak near/over the memory cap, or
(new, from the LCA merge) has no chunking/progress/logging at all. Each gets
the same treatment: identify the natural checkpoint boundary, chunk around
it, wire it through the §5 progress API, and validate against §7.5's
performance budget before considering it done.

### 7.1 `cluster_pool` / `bin_cluster_service.run_pool_bin_clustering`

Already has partial phase instrumentation (§5) and the CSR `adj_sim`
optimization (1.71 GiB down from 2.78 at the `adj_sim` phase). Still peaks
at **4.06 GiB** on the largest real pool (11.3M pairs) — over the 3 GB cap —
with ~1.3 GiB unattributed between the metadata-enrichment loop and the
sim-index propagation stage (documented as the one open question in
`memory-job-management-remaining.md`). Plan:

1. Use the phase instrumentation already in place (`MEM_PHASE_LOG=1`) to get
   the attribution that was never captured (the instrumented run that would
   have gotten it was interrupted by a reboot).
2. Chunk whichever stage dominates — almost certainly the metadata
   enrichment loop or sim-index propagation, both of which iterate over a
   bounded structure (clusters, or propagated edges) and are natural
   checkpoint boundaries.
3. Wire checkpoints through §5's progress API so a hard-kill (§4) resumes
   from the last completed chunk of clusters, not from zero.

### 7.2 `build_bin_sim` chunked path

Covered in §6 — becomes a single resumable job. Listed here too because it's
also a memory concern (each chunk's pair computation), not just a splicing
one: chunking bounds peak memory the same way `ENRICH_CHUNK_SIZE` does for
`enrich_features`.

### 7.3 Native/GPU similarity discovery (new, from `dev-lca-merged`)

`similarity_service.py` calls into `bsimvis_similarity_native` (Rust,
`discovery_backend` config: `rust_cpu` or GPU) as a single opaque FFI call
per job — **no chunking, no progress, no logging** inside the native call at
all today. This is the newest and least-visible monolithic-job source: if it
OOMs or hangs, there's currently nothing to look at.

Plan: chunk on the **Python side** — batch the input pair/feature set fed to
the native call, call it per batch, checkpoint progress between batches
through §5. This does not require changing the Rust code's internals (no
native-side streaming needed) — only how much work Python hands it per call.
Batch size gets tuned the same way `ENRICH_CHUNK_SIZE` was: measure peak RSS
and throughput per candidate size, pick from the curve (§7.5).

### 7.4 `idx_functions` / `idx_features`

Currently under the cap (1.66 / 1.13 GiB measured) but still monolithic in
the sense of no phase-level progress reporting — included because "no
progress, no logging" was called out as its own complaint independent of
OOM risk. Lower priority than 7.1/7.3, same treatment: wire through §5.

### 7.5 Performance guardrail — mandatory for every job in this section

Chunking adds fixed overhead per boundary (a progress write, a checkpoint,
possibly a smaller native-call batch with its own per-call fixed cost).
`enrich_features`'s own tuning already proved this can be made free — 100 →
50 → 25 chunk size measured **flat throughput (44-49 features/s) at every
size** — but that was verified, not assumed, and every job in §7 gets the
same discipline:

1. Benchmark the **unsplit** handler's throughput on a real corpus first
   (extend `scripts/benchmark_pipeline.py`, which already exists on this
   branch, rather than building a new harness).
2. Implement chunking with a config-exposed chunk/batch size.
3. Benchmark across 2-3 candidate sizes. Regression budget: **no more than
   5% throughput loss** at the chosen size versus the unsplit baseline: if
   every candidate loses more, the checkpoint granularity is wrong (too
   fine) and needs to be coarser, re-measure rather than ship it.
4. Record the measured curve in the job's docstring/comment (matching the
   existing `ENRICH_CHUNK_SIZE` precedent) so the next person tuning it
   isn't guessing either.
5. `job_memory_report.py` output for the job type must show peak RSS
   comfortably under `worker_max_memory_per_child` after the change — this
   is the acceptance bar, not just "it completed once."

---

## 8. Benchmarking

Reuse and extend what's already on `dev-lca-merged`, don't build parallel
infrastructure:

- `scripts/benchmark_pipeline.py` — extend with a mode per §7 job type,
  before/after comparison output.
- `scripts/job_memory_report.py` — already the source of truth for measured
  peaks vs budget; keep using it as the acceptance check for every §7 item.
- `scripts/quick_bench_backends.py` — already exists for the native
  `rust_cpu`/GPU backend comparison; extend to also report throughput at
  each candidate batch size from §7.3/§7.5 rather than adding a new script.

---

## 9. Execution order

Each phase independently shippable, same incremental discipline that worked
for the memory-management branches. Later phases depend on earlier ones only
where noted.

1. **Logging/progress infrastructure (§5).** No architecture change
   required — ships value immediately, and every later phase (stop/restart,
   splitting, Celery migration) depends on it for observability. Do first.
2. **Splice fixes (§6).** Independent of Celery; deletes `splice_tasks`
   outright. Uses §5's progress API for the new resumable `build_bin_sim`
   job.
3. **Monolithic job splitting (§7)**, ordered 7.3 (native/GPU — newest,
   least visible today) → 7.1 (`cluster_pool` — known worst offender,
   attribution already half-done) → 7.2 (folded into phase 2) → 7.4 (lowest
   priority). Each gated by §7.5's performance budget before merge.
4. **Celery spike** on `ghidra_analyze` (already subprocess-isolated,
   cleanest boundary) — validate `revoke(terminate=True)` really hard-kills,
   validate `worker_max_memory_per_child` against the peaks §7/§8 measured.
5. **Migrate leaf job types** to Celery tasks one at a time, reusing the §5
   progress API and §7's chunking inside each task body unchanged — the
   migration is about *dispatch*, not re-touching the handler logic just
   rewritten in phase 3.
6. **Migrate orchestration** (chain/group/chord replacing
   pipeline/group/lane/wave) — last, because it's the highest-blast-radius
   change and every job type it touches should already be simplified and
   instrumented by this point.
7. **New Job API + Job view UI (§2, §3)** built against the Celery-backed
   system.
8. **Decommission** the old `JobService` orchestration code once nothing
   references it.

---

## 10. Risks / open items

- **Two engines running concurrently during phases 4-6.** The job list/API
  needs to read from both the legacy `JobService` and Celery's result
  backend until migration completes. Keep this window short — migrate leaf
  types in one focused stretch, not spread over months.
- **Lane module is genuinely new code**, not a port — it's the one piece
  with no Celery equivalent and deserves its own test suite before
  orchestration migration (phase 6) depends on it.
- **Native similarity chunking (7.3)** is the least-understood item —
  nobody has profiled the FFI call's internal memory behavior yet. Budget
  time for a measurement pass before committing to a batch size, same as
  `cluster_pool`'s phase attribution had to be measured rather than guessed.
- **`STOP_GRACE_SECONDS` default (15s proposed)** is a judgment call — worth
  confirming against real job types once §7's chunking lands, since a
  well-checkpointed job should exit cooperatively well inside that window
  and rarely need the hard-kill escalation at all.
