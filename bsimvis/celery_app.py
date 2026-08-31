"""Celery app for the job-system migration (doc/job-system-rework-plan.md §9).

Broker/backend reuse the existing job Redis (REDIS_HOST/REDIS_PORT --
job_service.py's plain-Redis instance, not kvrocks) on dedicated DB indices
so Celery's own keys never collide with the `job:*`/`jobs:*` keyspace the
legacy JobService still owns during the dual-engine window (§10 risk).

task_acks_late + worker_max_memory_per_child are the native replacements for
the hand-rolled lease/reaper/MAX_ATTEMPTS machinery (plan §1). Per §4,
STOP_GRACE_SECONDS backs the cooperative-then-hard-kill escalation: routes
call `AsyncResult.revoke()` first, then `revoke(terminate=True)` if the task
hasn't reached a terminal state within this window.
"""

import os

from celery import Celery

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
BROKER_DB = int(os.getenv("CELERY_BROKER_DB", 1))
BACKEND_DB = int(os.getenv("CELERY_BACKEND_DB", 2))

STOP_GRACE_SECONDS = int(os.getenv("STOP_GRACE_SECONDS", 15))

app = Celery(
    "bsimvis",
    broker=f"redis://{REDIS_HOST}:{REDIS_PORT}/{BROKER_DB}",
    backend=f"redis://{REDIS_HOST}:{REDIS_PORT}/{BACKEND_DB}",
    include=["bsimvis.tasks.ghidra_task", "bsimvis.tasks.test_hang_task"],
)

app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_track_started=True,
    # ghidra_analyze's own worker footprint measures ~0.8 GiB (the JVM runs
    # isolated in a subprocess and is NOT counted here -- see ghidra_job.py's
    # peak-RSS comment: it shares the parent's cgroup/systemd MemoryMax
    # instead, a mechanism this setting does not supersede). Recycle
    # generously above that measured floor; tune per job type once more
    # leaf tasks migrate (§9 phase 6).
    worker_max_memory_per_child=int(
        os.getenv("CELERY_WORKER_MAX_MEMORY_KB", 1_500_000)
    ),
)
