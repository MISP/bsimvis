# What is left to finish the memory/job-management work

Handoff brief. Companion to two existing docs, read both first:

- `doc/memory-job-management-brief.md` — the original problem statement, written
  after an evening that OOM-killed two full worker fleets.
- `doc/memory-job-management-results.md` — what was changed, measured and
  verified, including numbers you should not need to re-derive.

Everything in the original brief (items A1–A6, B1–B7) is implemented, committed
and passing. State: local `dev`, **15 commits ahead of `origin/dev`, nothing
pushed**. Two of those 15 (`1eb9e48`, `29bb87e`) predate this work and were
already unpushed.

Test baseline to preserve — all of this passes today:

```bash
for t in test_sim_edges test_job_leases test_worker_registry \
         test_enrich_resumable test_job_admission test_ghidra_subprocess \
         test_worker_lrem; do uv run python $t.py; done   # 53 tests

cd .claude/worktrees/memjob && ./scripts/wt-test.sh        # 317/317, RESULT: PASS
```

`wt-test.sh` must be run from a linked worktree — it refuses otherwise, and it
never touches the main repo's 17G `data/kvrocks`.

---

## 1. Validate the clustering rewrite at real scale — the one real gap

**This is the most important item and the reason this brief exists.**

`perf(cluster): stream similarity pairs…` (`f569793`) cut clustering peak from
2.75 GiB to 0.43 GiB on a real 5.4M-pair pool. That result is backed by:

- offline phase-by-phase measurement against real data ✅
- `test_sim_edges.py`, 10 tests including parity against a reference copy of the
  old inline algorithm ✅
- the 317-test integration suite ✅ — but that runs on a 2-file collection

It is **not** backed by an actual `cluster_pool` or `build_pool_bin_sim` job at
scale. Neither has executed since the rewrite. Confirm with:

```bash
redis-cli -p 6380 hgetall jobs:mem:peak     # neither type appears yet
```

Before the rewrite these were the only two job types that exceeded the 3 GB cap
outright, measured at **3.03 GiB** and **3.02 GiB**.

### How to close it

A pool build is draining right now (~4,400 `build_pool_sim` jobs pending). When
it finishes, `cluster_pool` and `build_pool_bin_sim` run on their own. Watch:

```bash
watch -n30 'redis-cli -p 6380 hgetall jobs:mem:peak | paste - -'
grep -h 'OOM-KILLED' logs/worker-*.log | tail
uv run python scripts/job_memory_report.py
```

Expected: both well under 1 GiB. If either is still near 3 GiB, the fix did not
cover the path that actually runs — find out which allocation dominates before
changing anything (see "How to measure" below).

Don't wait passively if you want it sooner: the largest pool is
`global:pool:1551edc1-bf5d-47c7-a332-fdcbf292cb02` with **11.3M pairs**, roughly
2x the pool the 0.43 GiB figure came from. Clustering that one is the strongest
available test.

**Also verify correctness, not just memory.** The rewrite changed how edges and
components are built. Compare cluster assignments before/after on the same pool
— same number of clusters, same membership. Parity tests cover the edge loader;
they do not cover the full clustering output.

---

## 2. `enrich_features` has almost no headroom

Measured peak on the live collection: **2.75 GiB against a 3 GB cap** — 92%.

It completes (the 419,617-feature `stdlib-ref` backlog fully drained,
`pending_enrichment` is now 0) and no OOM kill has been recorded since, but the
margin is thin and some batches are clearly heavier than others. An earlier
sample of the same job measured 1.93 GiB, so the variance across batches is
large.

Peak is governed by `ENRICH_CHUNK_SIZE` (`feature_service.py:437`, default 100),
not by `ENRICH_BATCH_SIZE` (`:727`, default 1000) — the batch is only the
checkpoint interval. Each chunk pipelines one `HRANDFIELD … 100 withvalues` per
feature and holds every response at once.

Suggested: drop the default to 50 and measure. Halving the chunk should roughly
halve the peak, at some cost in round-trips. Confirm the runtime cost is
acceptable before committing to it — this job processes hundreds of thousands of
features and the throughput matters.

I left it at 100 deliberately: it passes today, and picking the number is a
judgement call the owner should make with the runtime trade-off visible.

---

## 3. `adj_sim` — the largest remaining structure

Measured: **0.89 GiB for 10.8M entries** on the 5.4M-pair pool. It is a
dict-of-dicts holding two entries per edge, built *after* clustering:

- `cluster_service.py:613` (function clustering, collections and pools)
- `cluster_service.py:1682` (`run_pool_bin_clustering`)
- `bin_cluster_service.py:528` (binary clustering, collections)

Consumers are lookup and iteration, `cluster_service.py:662` and `:666` plus the
equivalents in the other two:

```python
total_sim += adj_sim[u].get(v, 0.0)
for v, sim in adj_sim[u].items():
```

A symmetric CSR matrix would cut this to roughly 65 MB and serves both access
patterns (`indptr` slicing gives the neighbours of `u` directly). It is an
optimisation, not a fix — total peak still lands near 1.1 GiB, inside the cap —
so **do item 1 first** and only do this if item 1 shows clustering still tight.

If you do it: all three sites are near-identical, so put the replacement in
`bsimvis/app/services/sim_edges.py` next to `group_edges_by_component`, which is
already the shared home for this kind of thing.

---

## 4. `MAX_ATTEMPTS` does not account for progress

`job_service.py:79` — a job is failed permanently after 3 lease expiries
(`:686`). That predates jobs being resumable.

`enrich_features` is now resumable and checkpoints every batch, so a job can be
killed three times while making real forward progress each time and still be
abandoned. This was observed: a full-scale enrich run was failed by the reaper
after three OOM kills, having permanently enriched several thousand features.

Suggested: reset `attempts` when a requeued job demonstrably advanced (progress
percent or `processed_items` increased since the previous claim), so the counter
targets poison jobs rather than slow ones. Careful not to reintroduce the
infinite-retry loop `MAX_ATTEMPTS` exists to prevent — `test_job_leases.py`'s
`test_poison_job_fails_instead_of_looping_forever` pins that and must keep
passing.

---

## 5. Brief items deliberately not done

From the original brief's A4, worth doing, irrelevant to the outage:

- **`iterrows()` walks** — 10 sites in `cluster_service.py`. Row-wise pandas
  iteration over the condensed tree. Vectorising is a speed win; none of them
  showed up as a memory problem in the phase measurements.
- **Upload size cap** — never implemented. A single enormous upload is still
  unbounded.

---

## How to measure (use this before changing anything)

The whole of this work was driven by measurement, and twice the obvious guess
was wrong — streaming the `enrich_features` pending set fixed nothing because
the real cost was per-chunk, and a component cap for function clustering would
have solved an already-solved problem. Measure first.

**Per-job peaks**, recorded automatically by every worker:

```bash
uv run python scripts/job_memory_report.py
```

**Per-phase inside a job** — set `MEM_PHASE_LOG=1` in `.env`, restart workers,
and read `[mem]` lines from `logs/worker-*.log`. `mem_util.phase(label)` marks a
boundary; add more where you need them. Remember tmux windows inherit the tmux
*server* environment, so `MEM_PHASE_LOG=1 ./launch_tmux.sh` does **not** reach
the workers — it has to go in `.env`.

**Offline against real data, no side effects** — the most useful technique here.
Replicate the phases in a standalone script reading the same keys and print
`mem_util.current_rss()` between them. That is how the four-copies finding came
out, without mutating anything or disturbing the fleet.

**Whole-scope peak including the Ghidra child**: `logs/<worker>.peak`, and the
supervisor prints it on every worker exit including OOM kills.

## Operational notes

- Restarting workers: kill the tmux windows **and** stop the systemd scopes —
  scopes are not children of the tmux shell and survive `kill-session`:
  ```bash
  for w in 1 2 3 4 5; do tmux kill-window -t bsimvis:worker-$w; done
  systemctl --user list-units --plain --no-legend 'bsimvis-bsimvis-worker-*.scope' \
    | awk '{print $1}' | xargs -r -n1 systemctl --user stop
  ./launch_tmux.sh          # adds missing windows, does not clear the stack
  ```
  `./launch_tmux.sh --clear` tears everything down — don't, unless you mean it.
- The app must be restarted to pick up service changes; workers are separate.
- Worker count settles to the true value ~60s after a restart (registration TTL);
  a transient inflated `active_workers` right after a restart is expected.
- Main stack: app `:5001`, redis (jobs) `:6380`, kvrocks (data) `:6667`. The
  `jobs:global` list on `:6379` is a different, unrelated instance — it is a job
  *history* list, not the queue. I got this wrong once.
