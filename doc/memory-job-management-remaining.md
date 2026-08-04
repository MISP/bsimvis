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

## STATUS — all five items worked, 2026-08-04

Branch `worktree-memjob-remaining`, five commits on top of `12f7f6a` (four
code, one this doc). Test
baseline held: 317/317 integration, and the unit suite is now 61 tests (53 plus
8 new). Everything below was measured against real data, not derived.

| # | Item | Outcome |
|---|------|---------|
| 1 | Validate clustering at scale | **Done, and it found a real problem.** See below. |
| 2 | `enrich_features` headroom | Done. `ENRICH_CHUNK_SIZE` 100 → 50: ~1/3 of the peak back, no throughput cost. |
| 3 | `adj_sim` | **Done, and it turned out to be needed.** CSR: 2.78 → 1.71 GiB at that phase. |
| 4 | `MAX_ATTEMPTS` vs progress | Done. Retry counter resets on a forward-only progress watermark. |
| 5 | `iterrows()`, upload cap | Both done. 17 sites → `itertuples`; the two caps were `* 1024` typos. |

### The finding that matters

Item 1 is closed twice over, and the second answer is worse than the first.

**The good half.** The fleet drained and ran real jobs, so the two types that
used to exceed the cap now have live numbers in `jobs:mem:peak`:

```
cluster_pool           0.39 GiB   (was 3.03)
build_pool_bin_sim     0.42 GiB   (was 3.02)
```

No OOM kill since 01:45 on 2026-08-04, all of which predate the fixes and were
the old `enrich_features` path. Correctness is confirmed too: against a
reference copy of the pre-rewrite code on a full 1.27M-pair pool, 5475 clusters
both sides, identical membership, cohesion matching to 7.1e-08. That covers the
full clustering output, which the parity tests did not.

**The bad half.** Clustering the largest pool —
`global:pool:1551edc1-bf5d-47c7-a332-fdcbf292cb02`, 11.3M pairs — peaked at
**4.06 GiB**, over the 3 GB cap. A `cluster_pool` job on that pool would be
OOM-killed. The rewrite is not what fails: streaming 11.3M pairs costs 0.29 GiB,
exactly as designed. The peak arrives later, and `adj_sim` (now CSR) is no
longer the cause.

Measured phases on that pool, dict version:

```
after streaming 11.3M pairs   0.29 GiB
after comp_to_edges           0.91 GiB
after adj_sim                 2.78 GiB   <- CSR now holds this at 1.71
...                           4.06 GiB   <- final, somewhere after this
```

**So the one open question is where the last ~1.3 GiB goes**, between the
cluster-metadata enrichment loop and the sim-index propagation stage. That
stage runs after the edge structures are freed, so it always looked cheap and
was never instrumented. It is instrumented now (`mem_util.phase` through the
sim-index `phase()` helper, plus every 500k propagated), so the next real
`cluster_pool` run on a large pool prints the attribution with
`MEM_PHASE_LOG=1` in `.env` — no special run needed.

I did not get that attribution myself: the instrumented re-run was still in the
metadata loop when the machine locked up under the combined load of that run,
the worker fleet and kvrocks, and rebooted. Which is its own lesson — do not
run a full 11.3M-pair clustering alongside a busy fleet.

**Two loose ends from that reboot:**

- The 11.3M pool's cluster keys were left half-written when the run died mid
  metadata-enrichment. Requeue a `cluster_pool` job for it to make them
  consistent; that also produces the phase attribution above.
- Services were not restarted after the reboot (`:6380` was down when I
  checked). Bring the stack back up before reading any of these numbers again.

### How the measurements were taken

Two throwaway harnesses, both against real data, neither committed:

- **Parity** — copies a real pool's sim ZSET into two scratch pool ids with the
  member prefix rewritten, clusters one with the pre-rewrite code and one with
  current, and compares membership and cohesion. Every write lands under
  `global:pool:<scratch-uuid>:*`, so it cannot disturb anything real, and it
  deletes those keys afterwards. This is the technique to reuse for any future
  change to clustering output.
- **Enrich bench** — re-enriches a `ZRANDMEMBER` sample of already-enriched
  features at a given `ENRICH_CHUNK_SIZE`, reporting peak RSS and throughput.
  Enrichment recomputes from the same source data, so a re-run writes back the
  same values.

---

## 1. Validate the clustering rewrite at real scale — the one real gap

**This is the most important item and the reason this brief exists.**

> **Closed 2026-08-04** — see STATUS above. Both job types now have live peaks
> well under the cap, and correctness is confirmed against the old code. But
> the 11.3M-pair pool peaks at 4.06 GiB, which is a new problem, not this one.

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

> **Closed 2026-08-04** — default is now 50. Measured 100 -> 2.07/2.40 GiB,
> 50 -> 1.60 GiB, 25 -> 1.38 GiB, with throughput flat at 44-49 features/s
> across all three. The round-trip cost this was expected to pay is not there.


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

> **Closed 2026-08-04** — done, and item 1 showed it was needed rather than
> optional. `sim_edges.SimAdjacency`, all three sites. 2.78 -> 1.71 GiB at that
> phase on the 11.3M-pair pool. Duplicate pairs take the last value, not the
> sum, which is the one way scipy would have got this wrong.


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

> **Closed 2026-08-04** — resets against a forward-only `processed_items`
> watermark. The poison-job test still passes, plus two new tests covering a job
> that advances every attempt and one that advances once and then stalls.


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

> **Closed 2026-08-04** — both done. 17 `iterrows()` sites converted (25.9s ->
> 17.2s on a 1.27M-pair parity run), and both upload caps were off by one
> `* 1024` from their own comments.


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
