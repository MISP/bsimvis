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
| Search | Real secondary indexes (collection, file md5, status, type, pool), not a full-scan-and-filter (§2.1) |

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

### 1.1 Standards to adopt instead of inventing more of our own

Asked directly: yes, there's a real standard for most of what's left, and
using it is less work than what we'd otherwise hand-roll.

- **Task states.** Stop maintaining a bespoke `JobStatus` enum once Celery
  owns dispatch — use Celery's own states (`PENDING`, `STARTED`, `RETRY`,
  `FAILURE`, `SUCCESS`, `REVOKED`) directly, with `failure_reason` (§2) as a
  bsimvis-specific *refinement* layered on `FAILURE`, not a replacement
  taxonomy. One less thing to keep in sync with the library we're already
  adopting.
- **Timing and nested-pipeline structure: OpenTelemetry's span model.** This
  is the direct fix for both new asks below (§3.8) — a *unified ETA across
  nested pipelines* and *correct subjob durations* are exactly what
  OTel's data model is for, and we'd otherwise be re-inventing it badly:
  - A span has exactly one `start_time` and one `end_time`, set once each,
    by construction — there's no way to accidentally compute duration from
    the wrong field (today's bug: `jobs.js:447` uses `created_at`→`updated_at`,
    conflating queue-wait with execution and drifting from trailing writes).
  - Spans nest (`parent_span_id`), and a parent's duration is *derived* from
    its children automatically in every trace viewer — nobody hand-writes a
    one-hop, unweighted aggregate like today's `_update_pipeline_aggregate_progress`
    (job_service.py:1294) that breaks on anything deeper than one level.
  - Celery already has first-class OTel instrumentation
    (`opentelemetry-instrumentation-celery`) that emits a span per task and
    threads parent/child through chain/group/chord automatically — this is
    largely wiring an existing integration, not building tracing from
    scratch.
  - Doesn't require standing up Jaeger/Tempo on day one to get value: adopt
    the *data model* (every unit — task, chain, group, chord — gets
    `trace_id`/`span_id`/`parent_span_id`/`start_time`/`end_time`, span
    *attributes* for `processed_items`/`total_items`/`speed_*`, span
    *events* for the §5 log lines) even while storage stays Redis for v1.
    Exporting to a real trace backend later is additive, not a rework.
- **The SSE event envelope** (§2's `/api/jobs/stream`) — shape pushed
  deltas as [CloudEvents](https://cloudevents.io) (`id`, `source`, `type`,
  `time`, `data`) instead of an ad hoc JSON blob. Costs nothing extra since
  the shape has to be defined either way, and means any future consumer
  (a Slack notifier, an external dashboard) doesn't need bsimvis-specific
  parsing.

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
- `GET /api/jobs/stream?collection=&pool=&md5=&status=` — **new**, SSE,
  scoped instead of single-job. Same filter vocabulary as §2.1's indexed
  query, pushes deltas (job created/status-changed/completed) for whatever
  matches the filter. This is the one feed every cross-view widget in §3
  subscribes to — a global "everything" subscription for the bottom-right
  box, a `collection=X` one for that collection's page, an `md5=Y` one for
  that file's detail page. v1 implementation: the server re-runs the §2.1
  indexed query for the connection's filter every 1-2s and pushes only the
  diff — still O(matches) per connection thanks to the indexes, not O(all
  jobs) like every current client-side poll is. Real Redis-pub/sub push
  (publish on every `_set_status` write, §2.1) is the natural v2 upgrade if
  connection count ever makes the polling variant expensive; not needed to
  ship v1.
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

### 2.1 Search & indexing

Today's "filtering" isn't indexed at all. `list_jobs` (job_service.py:1447)
fetches every id from `jobs:global` (or `jobs:collection:<coll>`, capped at
1000 each by `LTRIM`) and only applies `status`/`type`/`tier` filters
**after** hydrating and JSON-parsing each one in Python (:1616-1644) — a
status or type filter is a full scan every time, and gets more expensive as
the cap grows or is raised. There is **no index at all** for file md5 —
"every job that touched this file" isn't answerable without scanning every
payload's `md5`/`file_md5`/`file_id` keys, which aren't even consistently
named (the same three-way inconsistency `worker.py:290-296`'s
`_mark_file_status` already has to work around).

Fix: real secondary indexes, maintained incrementally instead of scanned.

**Indexes** (Redis SETs, one per dimension value):

- `jobs:idx:collection:<coll>`
- `jobs:idx:pool:<pool_id>`
- `jobs:idx:status:<status>`
- `jobs:idx:type:<jtype>`
- `jobs:idx:md5:<file_md5>` — **new**, doesn't exist today

Plus one timeline: `jobs:timeline` — a ZSET, member = job id, score =
`created_at`. Replaces `jobs:global`'s `LPUSH`+`LTRIM`-to-1000 with a real
sorted structure that doesn't need an arbitrary cap to stay usable.

**Canonical md5 field.** Write `file_md5` onto the job hash itself at
creation time (`create_job`, normalizing whichever of `md5`/`file_md5`/
`file_id` the payload used), instead of leaving it buried three different
ways inside the JSON `payload` blob. This is what makes the md5 index (and
`worker.py`'s existing lookup workaround) both simpler — one field, one
name, indexed and hash-readable without a JSON parse.

**Maintenance.** Collection/pool/type/md5 set membership is written once at
`create_job` and never changes. Status is the one that moves — every status
transition (`start_job`, `complete_job`, `fail_job`, `cancel_job`, the
`pending`↔`running` flips) currently calls `self.r.hset(..., "status",
...)` directly from ~6 separate call sites. Centralize these into one
`_set_status(job_id, new_status)` helper that writes the hash field **and**
does the `SREM` old-status-set / `SADD` new-status-set move atomically (Lua,
same pattern as `_ADVANCE_LANE_LUA`). This both builds the index correctly
and removes another instance of the "same state change, six call sites"
smell the diagnosis doc flagged elsewhere (§1's `queued` latch, `advance_lane`
CAS).

**Query.** For N active filters (e.g. `collection=X&status=failed`):
`ZINTERSTORE tmp 2 jobs:timeline jobs:idx:status:failed WEIGHTS 1 0` (or
chain through each active filter's SET) intersects `jobs:timeline`'s
recency ordering against every filter set at once, giving a
correctly-paginated, correctly-sorted result in one round trip —
`ZCARD tmp` for total, `ZREVRANGE tmp offset offset+limit-1` for the page —
instead of hydrating and filtering the whole timeline in Python. `md5` and
`pool` filters slot into the same intersection, so `GET /api/jobs?md5=<hash>`
("every job that ever touched this file") becomes a real indexed query
instead of a request nobody could serve before.

**Engine-agnostic.** This indexing layer sits on the job *metadata* record,
not on Celery's result backend — it's needed whether the underlying executor
is today's `JobService` or the Celery-backed one from later phases (§9),
since Celery's own result store has no notion of collection/md5/pool at all.
Build it against the current job hash now; it carries forward unchanged
across the phase-4/5 migration.

---

## 3. Unified job-status UI

You called this out directly: "unify both job success pop up, job status in
the different views etc (they are a lot)." Confirmed by grep across the
current frontend — there are **5 independent `setInterval` polling loops**
hitting job endpoints separately (`pool_detail_view.js:72`,
`collection_detail_view.js:68`, `jobs.js:755`, `binary_similarity.js:1948`,
`dashboard.js:3495`'s `window.jobPollInterval`), **3 separate hand-rolled
poll-until-terminal-then-toast flows** duplicating the same logic
(`tag_manager.js:275-298`, `binary_similarity.js:1840-1869` and
`:1948-1966`), and `showToast` called ad hoc from ~15 call sites across
`dashboard.js`/`tag_manager.js`/`binary_similarity.js`, several guarded by
`typeof showToast === 'function'` — evidence the function's availability
isn't even reliably assumed. None of them share state; each re-fetches and
re-derives its own view of "is anything running."

The fix is one client-side store everything else is a thin view over.

### 3.1 The Job Status Store — single source of truth

One JS module (`job_status_store.js`), instantiated once, mounted at app
init:

- Owns **one** SSE connection per distinct scope actually in use (global
  fleet-wide, plus one per currently-open collection/pool/md5-scoped view) —
  against the §2 `/api/jobs/stream` filtered endpoint. Views never call
  `fetch('/api/jobs...')` in a loop themselves again.
- Maintains an in-memory map of job state keyed by id, and derived
  per-scope aggregates (running count, failed count, by collection/pool) —
  the same shape §2.1's indexes provide server-side, mirrored client-side
  for zero-latency reads between pushes.
- Exposes a small pub/sub API: `subscribe(scope, callback)` where `scope` is
  `{collection}`, `{pool}`, `{md5}`, `{jobId}`, or `{}` for global. Every
  widget in §3.2-3.6 is a `subscribe()` call plus a render function — no
  widget owns a fetch, a timer, or a "have I already shown this toast"
  flag.
- Fires **one** set of lifecycle events (`job:started`, `job:progress`,
  `job:completed`, `job:failed`) that both the unified toast (§3.6) and any
  view's own refresh logic hang off of.

### 3.2 Full job view (unit list + detail panel)

Two-level layout, not a fully recursive tree:

- **Unit list** (top-level chains/groups/chords): flat, paginated, filtered
  by collection, pool, status, type, and **file md5** (§2.1's new index) —
  a search box that accepts a pasted md5 and jumps straight to every job
  that touched that file is the concrete deliverable here.
- **Unit detail panel**: subscribes to the store (§3.1) at `{jobId}` scope
  instead of its own poll. Subtasks render from the pushed group/chord
  state; a unit with hundreds of members (a sealed upload wave) virtualizes
  the list client-side rather than rendering every row.
- **Controls per unit/task**: Stop, Restart (this step), Restart all, Skip,
  Pause — wired directly to the §2 endpoints. Buttons reflect what's
  actually legal for the current `status`/`failure_reason` (e.g. "Restart
  all" is not offered for a job that's still running).
- **Status badges** use the failure taxonomy, not a single red "failed" —
  distinct icon/color for crashed vs frozen vs retries-exhausted vs
  logic-error, so a scan of the job list tells you which failures are worth
  a code fix vs which are worth a click-to-retry.
- **Progress** shows `processed/total`, `speed_current`/`speed_avg`, ETA,
  and the current `phase` string when a handler reports one (§5, §3.8) —
  the format you pasted, generalized to every job type that reports counts.
- **Log panel** streams live, leveled (debug/info/warn/error), filterable by
  level client-side.

### 3.3 Global status widget (bottom-right)

Small, always-mounted, subscribes to the store at global scope:

- Collapsed: a badge with the running/queued count, broken down by
  collection/pool when more than one is active ("3 running — stdlib-ref,
  2 — pool:f4ea…").
- Expanded: the same §2.1 search box (collection/md5/status/type), scoped to
  recent/active jobs — a docked, lightweight entry point into the full job
  view (§3.2) rather than a second, different search implementation.
- Click-through on any row opens §3.2 filtered to that unit.

### 3.4 Per-view "partial / empty because a job is running" banner

One reusable component, not per-view bespoke logic: given an entity
(`collection`, `pool`, or `md5`) and the job type(s) relevant to what that
view is displaying (e.g. a collection's function-search view cares about
`idx_functions`/`build_sim`/`cluster_functions`; a file detail page cares
about `ghidra_analyze`/`idx_functions` for that one md5), it subscribes to
the store at that scope and renders one of:

- up to date — nothing relevant is running,
- **partial** — N relevant jobs still running, showing results so far,
- **empty** — analysis in progress, no results yet (distinct from "there
  really is nothing here," which today looks identical from the UI).

This is a direct consumer of §2.1's md5/collection indexes — "is anything
still running against this file/collection" wasn't answerable at all before
those existed.

### 3.5 Entity activity panel

A generic, embeddable "jobs for this thing" panel — given an entity
(collection, pool, file md5, batch), renders its job history via the same
`GET /api/jobs?collection=`/`md5=`/`pool=` query as §3.2's list, just scoped
and inlined. Drop it into the file detail page, the collection page, the
pool page — one component instead of each page hand-building its own
mini-table (which mostly doesn't exist consistently today; that gap is part
of what's being asked for here).

### 3.6 Unified toast / notification

One call site. `job:completed`/`job:failed` events from the store (§3.1)
drive a single toast dispatcher — replaces `tag_manager.js`'s own
poll-then-toast (:275-298) and `binary_similarity.js`'s two separate ones
(:1840-1869, :1948-1966). A view that starts a job does:
`store.subscribe({jobId}, onDone)` and nothing else — it no longer owns a
timer, a terminal-state check, or its own `showToast` call for that job's
outcome. The toast itself becomes taxonomy-aware (§2's `failure_reason`) —
"failed: crashed (worker died)" reads differently from "failed: logic
error," instead of every failure producing the same generic red toast.

### 3.7 Progress, ETA, and duration — unified, correct for nested pipelines

Two real bugs today, both traced to code:

- **Progress aggregation is flat and one-hop.**
  `_update_pipeline_aggregate_progress` (job_service.py:1294) does
  `total_p // len(tids)` — an *unweighted* average of immediate children's
  percentages only. A pipeline with 3 fast `idx_*` jobs and 1 huge
  `cluster_pool` job reports 25% "done" the instant the first three finish,
  regardless of how much real work the cluster job has left, and a
  grandchild's progress never reaches a grandparent — the update only walks
  one `parent_id` hop.
- **Duration uses the wrong fields, and breaks across retries.**
  `jobs.js:447` computes duration as `formatDuration(job.created_at,
  job.updated_at, status)`. `created_at` is enqueue time — includes queue/
  lane wait, not just execution. `updated_at` drifts (bumped by trailing
  log writes after completion, and by the parent-progress bug above
  touching a *pipeline's* `updated_at` on every child tick). No
  `started_at`/`completed_at`/`duration` field is ever set on composite
  units (pipeline/group) at all — only leaf jobs get `started_at`, written
  in `worker.py:_execute_job`. And `_reset_job_recursive` (routes/jobs.py:98)
  resets status/progress/attempts for a retry but **never clears
  `started_at`** — a job that fails after running 20 minutes, then
  completes in 30 seconds on retry, displays a 20-minute-plus duration,
  mixing two attempts into one wrong number.

Fix, per §1.1's OTel model — this is exactly the problem span
start/end + parent/child aggregation solves structurally instead of by hand:

1. Every unit (leaf task, chain, group, chord) gets its own
   `start_time`/`end_time`, set exactly once each: `start_time` when that
   unit's *first* leaf actually begins executing (not at enqueue), `end_time`
   when it reaches any terminal state. A retry creates a **new** attempt
   record (new `start_time`), not a mutation of the old one — so "this
   attempt's duration" and "total time across all attempts" (sum of past
   attempts' recorded durations + the current one) are both available and
   neither lies.
2. Progress/ETA aggregation becomes weighted and recursive: a parent's
   progress is `Σ(child.total_items * child.progress) / Σ(child.total_items)`
   when children report sizes, falling back to equal weighting only when
   none do — and it recomputes by walking all the way to the top-level unit,
   not one hop, the same way `advance_parent` already walks the full
   ancestor chain for status.
3. **ETA** is computed the same way at every level, not just once fleet-wide
   as `get_global_stats` does today: `remaining_items / speed_avg` from the
   weighted aggregate, falling back to elapsed-vs-percent extrapolation
   (today's fleet-only fallback, generalized to per-unit) when no unit in
   the subtree reports item counts. A top-level pipeline's ETA is the
   recursive sum of its not-yet-complete children's remaining work at their
   current combined speed — not a separate number someone has to reconcile
   against the per-job ones.
4. `duration` (and `eta_seconds`, already in §2's schema) become computed
   fields returned by the API, not something every view re-derives from raw
   timestamps client-side — `jobs.js:447`'s bespoke `formatDuration` call
   goes away along with the fields it was misusing.

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

1. **Logging/progress infrastructure (§5) + search indexes (§2.1) + the
   timing/duration/ETA model (§3.7).** None need an architecture change —
   all ship value immediately against today's `JobService`, and every later
   phase (stop/restart, splitting, Celery migration, the new UI) depends on
   them for observability. Do first, together: the `_set_status`
   centralization §2.1 needs touches the same call sites §5's worker-death
   diagnostics and §3.7's `start_time`/`end_time` writes do.
2. **Unified frontend: store + widgets (§3.1, §3.3-§3.6).** Also no Celery
   dependency — consumes phase 1's stream endpoint and indexed queries
   against the current backend. This is what actually deletes the 5 polling
   loops and 3 duplicate poll-to-toast flows (§3 intro) and ships the
   bottom-right widget and per-view partial/empty banners. Doing this before
   the Celery migration means the highest-visibility user-facing win lands
   early instead of waiting on the riskiest phase (6-7).
3. **Splice fixes (§6).** Independent of Celery; deletes `splice_tasks`
   outright. Uses §5's progress API for the new resumable `build_bin_sim`
   job.
4. **Monolithic job splitting (§7)**, ordered 7.3 (native/GPU — newest,
   least visible today) → 7.1 (`cluster_pool` — known worst offender,
   attribution already half-done) → 7.2 (folded into phase 3) → 7.4 (lowest
   priority). Each gated by §7.5's performance budget before merge.
5. **Celery spike** on `ghidra_analyze` (already subprocess-isolated,
   cleanest boundary) — validate `revoke(terminate=True)` really hard-kills,
   validate `worker_max_memory_per_child` against the peaks §7/§8 measured.
6. **Migrate leaf job types** to Celery tasks one at a time, reusing the §5
   progress API and §7's chunking inside each task body unchanged — the
   migration is about *dispatch*, not re-touching the handler logic just
   rewritten in phase 4.
7. **Migrate orchestration** (chain/group/chord replacing
   pipeline/group/lane/wave) — last, because it's the highest-blast-radius
   change and every job type it touches should already be simplified and
   instrumented by this point. Also where the §1.1 Celery-native task states
   replace the bespoke `JobStatus` enum.
8. **Full job view (§3.2)** reconciled against the Celery-backed
   chain/group/chord model — the store/widgets from phase 2 mostly carry
   forward unchanged (they consume the same API shape); this phase is
   specifically the tree-aware unit list/detail rendering that depends on
   real chain/group/chord semantics.
9. **Decommission** the old `JobService` orchestration code once nothing
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
