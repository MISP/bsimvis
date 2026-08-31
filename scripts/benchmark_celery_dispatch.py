"""§8/§7.5 discipline applied to the §9 phase 6 dispatch change: "must not
regress throughput materially -- measured, not assumed."

ghidra_analyze's own cost is dominated by the JVM (seconds), so the thing
worth measuring in isolation is what Celery adds ON TOP of that: one
broker publish + one worker consume + one result-backend round trip for
`.get()`. This measures exactly that, against a trivial no-op task, so the
number isn't muddied by real Ghidra runtime either direction.

Requires a Celery worker already running against this process's
REDIS_HOST/REDIS_PORT (see scripts/test_celery_hard_kill.py's docstring
for the command; same isolated-stack requirement applies here).

Run: uv run python scripts/benchmark_celery_dispatch.py [n]
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bsimvis.celery_app import app
from bsimvis.tasks.test_hang_task import noop_task


def bench_inprocess(n):
    def noop():
        return True

    start = time.perf_counter()
    for _ in range(n):
        noop()
    return time.perf_counter() - start


def bench_celery(n):
    start = time.perf_counter()
    for _ in range(n):
        noop_task.delay().get(timeout=10)
    return time.perf_counter() - start


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 50

    insp = app.control.inspect(timeout=5)
    if not insp.ping():
        print("No Celery worker responding -- start one against this stack first.")
        sys.exit(1)

    inproc = bench_inprocess(n)
    celery = bench_celery(n)

    print(f"n={n}")
    print(f"in-process : {inproc*1000/n:.3f} ms/call  (total {inproc:.3f}s)")
    print(f"celery     : {celery*1000/n:.3f} ms/call  (total {celery:.3f}s)")
    print(f"added cost : {(celery-inproc)*1000/n:.3f} ms/call")
    print()
    print(
        "For comparison: ghidra_analyze's own JVM runtime is seconds, and "
        "the pre-existing worker.py comment already measures its subprocess "
        "boot floor at ~0.8 GiB / multi-second JVM startup -- so this added "
        "cost is the relevant number, not a percentage of Ghidra's own time."
    )
