"""Celery task for ghidra_analyze (plan §9 phase 5 spike, §9 phase 6 target).

Mirrors worker.py's `Worker._run_ghidra_out_of_process` exactly: same retry
budget, same `python -m bsimvis.ghidra_job` invocation, same unanalyzed-flag
fallback. The one addition is process-group tracking (`start_new_session`
+ a Redis-recorded pgid) -- without it, a hard-killed Celery task leaves its
Ghidra JVM child orphaned instead of dead, because `revoke(terminate=True)`
only signals the OS process actually running the task, not any subprocess
it spawned. See `kill_utils.hard_kill_task`, which is what actually kills
the group; this module only records where to aim.
"""

import logging
import os
import signal
import subprocess
import sys

from celery import current_task

from bsimvis.app.services.redis_client import get_queue_redis
from bsimvis.celery_app import app

CHILD_PGID_KEY = "celery:child_pgid:{task_id}"


def _record_child_pgid(r, task_id, pgid):
    if task_id:
        r.set(CHILD_PGID_KEY.format(task_id=task_id), pgid, ex=3600)


def _clear_child_pgid(r, task_id):
    if task_id:
        r.delete(CHILD_PGID_KEY.format(task_id=task_id))


@app.task(name="bsimvis.ghidra_analyze", bind=True)
def ghidra_analyze_task(self, job_id, name="ghidra-child", attempts=None):
    """Runs one Ghidra analysis out-of-process, with retries.

    Return value matches the legacy dispatcher's convention: True on
    success, False once the retry budget is exhausted (job_service.py's
    fail_job / _mark_file_status callers key off this).
    """
    r = get_queue_redis()
    attempts = attempts or int(os.getenv("GHIDRA_ANALYZE_ATTEMPTS", 3))
    task_id = current_task.request.id if current_task else None
    cmd = [sys.executable, "-m", "bsimvis.ghidra_job", "--job-id", job_id, "--name", name]

    for attempt in range(1, attempts + 1):
        logging.info(f"[celery] Ghidra analysis attempt {attempt}/{attempts} for {job_id}")
        # start_new_session puts the JVM child in its own process group, so
        # a hard-kill can take the whole group out via killpg instead of
        # orphaning it (see module docstring).
        proc = subprocess.Popen(cmd, cwd=os.getcwd(), start_new_session=True)
        _record_child_pgid(r, task_id, proc.pid)
        try:
            rc = proc.wait()
        finally:
            _clear_child_pgid(r, task_id)

        if rc == 0:
            return True

        reason = f"signal {-rc}" if rc < 0 else f"exit {rc}"
        logging.warning(f"[celery] Ghidra analysis for {job_id} failed ({reason}), attempt {attempt}/{attempts}")

    logging.warning(f"[celery] Giving up on {job_id} after {attempts} attempts.")
    return False
