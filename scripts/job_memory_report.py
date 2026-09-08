#!/usr/bin/env python3
"""What each job type actually costs in memory.

    uv run python scripts/job_memory_report.py

Workers record the peak RSS they observed per job type (see Worker._execute_job),
so this is measurement, not estimation. Use it to answer the sizing question the
brief left open: is WORKER_MEMORY_MAX too low, or is a handler unbounded?

  peak <= ~1.5 GB   the cap is fine, look elsewhere
  peak near the cap  raise WORKER_MEMORY_MAX, or bound the handler
  peak > the cap     that job type cannot run under the current cap at all

Supervisor logs carry the other half of the picture: scripts/worker-supervisor.sh
prints the cgroup's memory.peak on every worker exit, including OOM kills.
"""

import sys

from bsimvis.app.services.job_service import JobService, MEM_PEAK_KEY, MEM_USED_KEY


def gib(n):
    return f"{int(n) / 1024**3:.2f} GiB"


def main():
    svc = JobService()
    budget = svc.memory_budget()
    peaks = svc.r.hgetall(MEM_PEAK_KEY) or {}
    peaks = {
        (k.decode() if isinstance(k, bytes) else k): int(v) for k, v in peaks.items()
    }

    print(f"fleet memory budget : {gib(budget)}")
    print(f"live reservations   : {gib(svc.r.get(MEM_USED_KEY) or 0)}")
    print(f"workers alive       : {svc.count_active_workers()}")
    print()

    if not peaks:
        print("No measurements yet. Run some jobs; every completed job records one.")
        return 0

    print(f"{'job type':<28} {'measured peak':>14}   share of budget")
    print("-" * 64)
    for jtype, peak in sorted(peaks.items(), key=lambda kv: -kv[1]):
        share = peak / budget * 100 if budget else 0
        flag = "  <-- exceeds budget alone" if peak > budget else ""
        print(f"{jtype:<28} {gib(peak):>14}   {share:5.1f}%{flag}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
