"""Celery task that actually executes a job's handler logic (plan §9 phase
6). Currently only worker.py's `_dispatch_via_celery` routes work here --
ghidra_analyze specifically, the one handler that spawns a real subprocess
and so is the one where a hard-kill needs kill_utils.py's process-group
tracking to avoid orphaning it. The task itself is generic (it just runs
whatever `_execute_job` would have run in-process), so widening which job
types get routed through here later is a one-line change in worker.py, not
a rewrite of this module.

The dispatcher (worker.py's Worker._run_loop) still owns claiming work off
the priority queues -- unchanged, including leases, admission control and
pause/cancel checks, none of which a Celery-native queue replaces for free
(see job_service.py's admission-control comment: fleet-wide measured memory
budget and watermark-based retry abandonment are not what
worker_max_memory_per_child gives you). What moves to Celery is only the
execution: the dispatcher hands off and blocks on the result, buying a
hard-kill that survives a subprocess boundary and per-child memory
recycling for the handler code that actually does the heavy lifting.
"""

import logging

from bsimvis.app.services.redis_client import get_queue_redis
from bsimvis.celery_app import app

_worker = None


def _get_worker():
    global _worker
    if _worker is None:
        # Local import: worker.py pulls in a large slice of the app's
        # services, and importing it at module load time would slow down
        # every Celery process, including ones that never run this task.
        from bsimvis.worker import Worker

        _worker = Worker(name="celery-exec")
    return _worker


@app.task(
    name="bsimvis.execute_job",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def execute_job_task(self, job_id):
    worker = _get_worker()
    job_data = get_queue_redis().hgetall(f"job:{job_id}")
    if not job_data:
        logging.warning(f"[celery] Job {job_id} metadata missing at execution time.")
        return
    worker._execute_job(job_id, job_data)
