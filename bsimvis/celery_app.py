"""Celery app for the job-system migration (doc/job-system-rework-plan.md §9).

Scope, deliberately narrower than the plan's literal §1 wording: Celery is
adopted as the EXECUTION SANDBOX for the one handler that spawns a real
subprocess (ghidra_analyze, via leaf_task.py) -- not as a full replacement
for job_service.py's lease/reaper/admission-control system. That system
already does two things worker_max_memory_per_child structurally cannot:
a fleet-wide MEASURED memory budget across concurrent jobs of different
types (try_admit), and watermark-based retry abandonment that tells a job
still making progress apart from one that's genuinely poison
(reap_expired). Both are real, tested, already-tuned-from-production-OOMs
machinery; replacing them with a coarser per-child recycle threshold would
be a regression, not a migration. See job_service.py's admission-control
comment and worker.py's _dispatch_via_celery for the actual split.

Broker/backend reuse the existing job Redis (REDIS_HOST/REDIS_PORT --
job_service.py's plain-Redis instance, not kvrocks) on dedicated DB indices
so Celery's own keys never collide with the `job:*`/`jobs:*` keyspace
job_service.py owns.

STOP_GRACE_SECONDS backs the cooperative-then-hard-kill escalation (§4):
job_service.cancel_job calls kill_utils.stop_task (SIGTERM) immediately,
then schedules kill_utils.escalate_stop_task after this many seconds to
SIGKILL + killpg if the task hasn't reached a terminal state by then.
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
    include=[
        "bsimvis.tasks.leaf_task",
        "bsimvis.tasks.kill_utils",
        "bsimvis.tasks.test_hang_task",
    ],
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
    # generously above that measured floor.
    worker_max_memory_per_child=int(
        os.getenv("CELERY_WORKER_MAX_MEMORY_KB", 1_500_000)
    ),
)
