# Bug report: `jobs:processing` leaks entries; workers leak JVM memory until the host freezes

**Date:** 2026-07-28
**Severity:** High — caused two host-level freezes requiring hard restart (2026-07-27, 2026-07-28)
**Components:** `bsimvis/worker.py` (queue loop), Ghidra/JVM lifecycle in worker process
**Status:** Root causes A, B and D fixed (`410423b`, `672fe5d`). Queue manually drained
2026-07-28 13:10. Still open: C (no orphan recovery after SIGKILL/reboot) and E (fleet
sizing ignores milvus and ollama); no `earlyoom` installed.

---

## Summary

Two distinct but compounding defects:

1. **`jobs:processing` is never reliably drained.** It holds 253 entries, of which only
   8 are unique non-terminal jobs. 241 are terminal (`cancelled`/`completed`) records
   that were dequeued but never removed. The list also contains 37 duplicate entries.
   There is no lease, no heartbeat, and no orphan recovery, so any worker that dies or
   wedges leaks its claim permanently.

2. **Worker processes leak Ghidra/JVM resources.** RSS grows monotonically and thread
   count accumulates (21 `File System Lis` threads in a single worker). Idle workers
   also burn ~0.7 CPU each continuously. With five workers the memory growth consumes
   host RAM until the kernel livelocks in page reclaim.

These are independent. Defect 1 stalls jobs; defect 2 freezes the host. Neither causes
the other.

---

## Evidence

### Queue state (redis on port 6380)

```
llen jobs:processing   253
llen jobs:pending        0
llen jobs:pending:high   0
```

Status histogram of the 253 entries:

| status | count |
|---|---|
| cancelled | 225 |
| completed | 16 |
| running | 12 |

216 unique IDs / 253 total entries → **37 duplicate entries**, one ID appearing 6 times.

### The 8 unique non-terminal jobs

Every one started **before the 10:30:26 reboot**. None were claimed by the currently
running workers.

| job_id | type | started_at |
|---|---|---|
| f3f79a19-bd72-40d9-aa3f-cd4a3fcf5fc9 | build_sim | 2026-07-28 10:26:44 |
| 34d5cbaa-273b-46b6-b819-55601aa0b001 | build_sim | 2026-07-28 10:26:38 |
| 017926c7-8fb8-4c3d-a6bc-a827be1ffb81 | ghidra_analyze | 2026-07-28 10:26:31 |
| 295b41ce-1d08-4441-ac4a-7c3a5331d1fd | build_sim | 2026-07-28 10:22:56 |
| 715650fa-8be0-4c0f-a276-7c22a56517d6 | ghidra_analyze | 2026-07-28 09:54:29 |
| 095710b0-ef25-4b09-9e04-3199c6e398d1 | build_pool_sim | 2026-07-06 14:56:43 |
| 587cfb3c-f719-4e10-8c48-37c9c169de8b | build_pool_sim | 2026-07-06 14:56:21 |
| 1e7494ec-aebe-4234-8fc0-9a64483c0761 | build_pool_sim | 2026-07-06 14:55:52 |

The three `build_pool_sim` entries are **22 days stale** and are the ones appearing
multiple times in the list.

### Worker process state

Sampled twice, 10 s apart, while `jobs:pending` was empty:

| process | utime+stime delta / 10 s | voluntary ctxt delta / 10 s |
|---|---|---|
| worker 8719 | +71 ticks (~0.7 CPU) | +10 |
| worker 8729 | +76 ticks | +10 |
| worker 8739 | +52 ticks | +10 |
| worker 8749 | +76 ticks | +10 |
| worker 8759 | +81 ticks | +10 |
| kvrocks 8658 | **0** | **0** |

Workers burn ~0.7 CPU each **with an empty queue and nothing claimed**, while making
roughly one syscall per second and issuing zero requests to kvrocks. This is not job
execution.

**Important:** this idle CPU burn is *not* what stalled the jobs. Verified by requeueing
the five recent orphans: workers claimed them via `BLMOVE` within milliseconds, and two
`build_sim` jobs ran to `completed` in under 5 seconds. The workers are healthy and
polling normally. The stalled jobs were orphaned entries in `jobs:processing` that no
worker held and nothing would ever re-claim — root causes A and C below, not a GC spiral.

The idle CPU burn and the RSS growth remain real and unexplained defects worth fixing
(root cause D), but they degrade the host rather than the queue.

Thread composition of worker 8749 (83 threads):

```
 27 python3
 21 File System Lis      <- Ghidra filesystem listeners, all in futex_do_wait
  1 VM Thread
  1 VM Periodic Tas
  1 Signal Dispatch
  1 Service Thread
  1 Reference Handl
  1 Python Referenc
  1 process reaper
  1 Notification Th
```

The JVM is embedded in the Python worker process (pyhidra/jpype), so its heap is
counted inside `worker.py` RSS and is invisible to any per-process `java` accounting.

RSS growth over 2 h 13 m of uptime:

| pid | at 10 min | at 2 h 13 m | growth |
|---|---|---|---|
| 8749 | 1.17 G | 2.11 G | +81 % |
| 8739 | 1.04 G | 1.82 G | +75 % |
| 8759 | 1.35 G | 1.81 G | +34 % |
| 8719 | 1.14 G | 1.62 G | +42 % |
| 8729 | 1.47 G | 1.46 G | ~0 |
| 8658 (kvrocks) | 2.75 G | 2.66 G | ~0 |

kvrocks is flat and is not implicated.

### Host impact

`sar -r`, 2026-07-28 morning:

```
10:00:05  kbmemfree 4845240
10:10:02  kbmemfree 3054352
10:20:15  kbmemfree  316628
```

Journal, same boot:

```
10:29:03  systemd-journald: Under memory pressure, flushing caches.
10:29:24  systemd-journald: Under memory pressure, flushing caches.
10:29:33  systemd-journald: Under memory pressure, flushing caches.
[log ends — hard restart at 10:30:43]
```

**No OOM kill was ever issued on 2026-07-28.** The kernel still had ~11 G of reclaimable
page cache, so `__alloc_pages_may_oom` never concluded that reclaim had failed. It kept
evicting and re-faulting executable pages instead — technically making progress, so the
OOM killer never fired, but at disk speed. That is why the machine had to be
power-cycled rather than recovering on its own.

By contrast, on 2026-07-27 the killer did fire twice and the host survived both:

```
Jul 27 10:34:28  Out of memory: Killed process 6515 (kvrocks) total-vm:8608372kB, anon-rss:1435792kB
Jul 27 19:35:37  Out of memory: Killed process 24442 (Isolated Web Co) total-vm:13203472kB, anon-rss:9876832kB
```

---

## Root causes

### A. Non-terminal cleanup paths skip `LREM`

`bsimvis/worker.py:66-106` implements a reliable-queue pattern:

```python
job_id = self.r_queue.execute_command(
    "LMOVE", "jobs:pending:high", "jobs:processing", "RIGHT", "LEFT"
)
...
self._execute_job(job_id, job_data)
self.r_queue.lrem("jobs:processing", 1, job_id)   # only on the success path
```

`LREM` runs after `_execute_job` returns normally. If `_execute_job` raises, control
goes to the `except Exception` handler at line 101, which logs and sleeps — it never
removes the entry. If the process is killed or wedges, likewise. The claim leaks.

This accounts for the 241 terminal entries: those jobs reached `cancelled`/`completed`
via some other path (API cancellation, or a crash after the status write but before the
`LREM`) and their list entries were orphaned.

### B. `LREM ... 1` cannot clean duplicates

`lrem("jobs:processing", 1, job_id)` removes a single occurrence. Once an ID is enqueued
more than once — as the three `build_pool_sim` jobs were — a successful completion
removes one copy and leaves the rest permanently. 37 duplicate entries are stuck this way.

### C. No lease or orphan recovery

Nothing expires a `jobs:processing` entry. There is no `started_at` watchdog, no worker
heartbeat, and no startup sweep. A job orphaned on 2026-07-06 was still sitting in the
list 22 days later, across many restarts.

### D. `GhidraProject.createProject` is never closed

**Corrected 2026-07-28.** An earlier draft of this report blamed `program.release(project)`
being inside conditional branches. That was wrong — every `program.release` call *is*
already in a `finally`. The actual leak is one level up: the **project** is never closed.

The codebase has two shapes. Every `openProject` path closes correctly:

| site | creates | closes |
|---|---|---|
| `worker.py:355` | `openProject` | ✅ `project.close()` at :437 |
| `ghidra_service.py:792` | `openProject` | ✅ `project.close()` at :817 |
| `bsimvis_upload.py:346` | `openProject` | ✅ `project.close()` at :406 |
| `worker.py:448` | `createProject` | ❌ **none** |
| `ghidra_service.py:753` | `createProject` | ❌ **none** |
| `bsimvis_upload.py:411` | `createProject` | ❌ **none** |

All three `createProject` sites release the program and then let the project object fall
out of scope unclosed. `worker.py:448` is the hot one — it is the single-file
`ghidra_analyze` path that workers execute for every uploaded binary.

Why this leaks a thread per job: `GhidraProject.createProject` builds a
`ProjectFileManager` over a `LocalFileSystem`, which constructs a
`FileSystemEventManager`. That class starts a dispatch thread — confirmed by extracting
`Ghidra/Framework/FileSystem/lib/FileSystem.jar`, where the string `File System Listener`
lives in `ghidra/framework/store/FileSystemEventManager$FileSystemEventProcessingThread`.
`FileSystemEventManager.dispose()` is what stops it, and it is only reached via
`project.close()`. No close, no dispose, thread runs forever.

This matches the observed evidence exactly: **21 `File System Lis` threads parked in
`futex_do_wait`** in worker 8749 — one per single-file analysis that worker had run,
each pinning its project, filesystem, and associated Ghidra object graph in the JVM heap.

Note the `TemporaryDirectory` context manager at `worker.py:445` deletes the project
directory on exit while the project is still open, so the leaked object graph outlives
the files it refers to.

Secondary issue at the same sites: `if "program" in locals() and program:`
(`worker.py:482`, `ghidra_service.py:783`) — `locals()` is function-scoped, so on a
second call within the same frame a stale `program` from an earlier iteration could be
released twice. Not the leak, but worth tightening to `program = None` before the `try`.

### E. Fleet sizing ignores co-tenants

`launch_tmux.sh` sizes the fleet as `(MemTotal - 8) / 2.5`, reserving 8 GB for "kvrocks,
redis and the desktop". On this host **milvus** (5+ processes) and **ollama** also run
outside the fleet and are unaccounted. At 30 GB the formula permits 8 workers; 5 already
exhausts RAM once the leak has run for a couple of hours.

---

## Suggested fixes

Ordered by cost/benefit.

1. **Move `LREM` into a `finally`.** Guarantees the claim is released on every exit path
   from `_execute_job`. Smallest possible change, fixes root cause A.

2. **Use `LREM ... 0`** to remove all occurrences, fixing root cause B. There is no case
   where the same job ID should legitimately remain claimed after completion.

3. **Sweep `jobs:processing` on worker startup.** Re-queue or fail any entry whose
   `job:{id}` status is terminal or whose `started_at` exceeds a threshold. Fixes C, and
   would have auto-healed the 2026-07-06 orphans.

4. ~~**Add `project.close()` at all three `createProject` sites.**~~ **Done** — see
   commit `672fe5d`. Note the fix *replaces* the manual `program.release(project)`
   rather than following it: `GhidraProject.close()` already iterates `openPrograms`,
   ends their transactions and calls `p.release(this)`, and `importProgram` registers
   programs there. Releasing first makes `close()` throw
   `IllegalArgumentException: Attempted to release domain object with unknown consumer`.

   Verified by `test_ghidra_project_leak.py`, counting `File System Lis` threads over
   three consecutive analyses:

   | | round 1 | round 2 | round 3 |
   |---|---|---|---|
   | before fix | 1 | 2 | 3 |
   | after fix | 0 | 0 | 0 |

5. **Install `earlyoom`.** Kills on a free-memory threshold rather than waiting for
   reclaim to fail, which is the exact condition the kernel never reached on 2026-07-28.
   This converts an unrecoverable freeze into a single killed process.

6. **Run the fleet under a cgroup:**
   `systemd-run --user --scope -p MemoryMax=10G ./launch.sh`
   Bounds the blast radius regardless of the leak, and does not depend on the heuristic
   in E being correct.

Items 5 and 6 are containment and are worth doing even before 1–4, because they make the
failure survivable without a power cycle.

---

## Remediation applied (2026-07-28 13:09–13:11)

Manual, data only — no code changed.

Backups taken first:
- `data/redis/dump.rdb.bak-20260728-preclean` (BGSAVE, `rdb_last_bgsave_status:ok`)
- `data/redis/jobs-processing-20260728.txt` (all 253 original list entries)

Actions:
1. Requeued the 5 recent orphans — `LREM 0`, reset status to `pending`, dropped
   `started_at`, `LPUSH jobs:pending`. Workers claimed all 5 immediately. Both
   `build_sim` jobs completed within seconds; the two `ghidra_analyze` and one
   `build_sim` were still running at time of writing.
2. Removed 208 unique terminal IDs (`completed`/`cancelled`/`failed`) from
   `jobs:processing` with `LREM 0`.

Result: `llen jobs:processing` 253 → 10.

**Still outstanding:** 7 of those 10 entries are duplicates of the three 2026-07-06
`build_pool_sim` jobs (`095710b0` ×3, `587cfb3c` ×2, `1e7494ec` ×2). They were left
untouched pending a decision on whether `pool_id f4ea5194-6d2a-45ee-9736-c439e420fc6a`
is still wanted. The other 3 entries are jobs legitimately in flight.

---

## Reproduction

1. Start the fleet with `WORKERS_COUNT=5` on a host with other memory consumers running.
2. Submit a batch of `ghidra_analyze` / `build_sim` jobs against a large collection.
3. Watch `RSS` of `bsimvis/worker.py` processes and the `File System Lis` thread count:
   both grow monotonically and never recover between jobs.
4. Watch `llen jobs:processing` grow and never return to 0.
5. After ~2 h, free memory approaches zero and the workers enter a GC spiral
   (high CPU, no kvrocks traffic).
