"""Stop/restart escalation (plan §4): cooperative signal, then hard kill.

`AsyncResult.revoke(terminate=True, signal=...)` only reaches the single OS
process Celery is running the task in. A task that spawned a subprocess
(worker.py's `_run_ghidra_out_of_process`, the one handler this migration
actually routes through Celery -- see leaf_task.py) leaves that subprocess
orphaned on hard-kill unless something explicitly kills its process group
too -- this module is that something.
"""

import errno
import logging
import os
import signal

from bsimvis.celery_app import app

CHILD_PGID_KEY = "celery:child_pgid:{task_id}"


def record_child_pgid(r, task_id, pgid):
    if task_id:
        r.set(CHILD_PGID_KEY.format(task_id=task_id), pgid, ex=3600)


def clear_child_pgid(r, task_id):
    if task_id:
        r.delete(CHILD_PGID_KEY.format(task_id=task_id))


def _killpg_if_recorded(r, task_id, sig):
    pgid = r.get(CHILD_PGID_KEY.format(task_id=task_id))
    if not pgid:
        return
    try:
        os.killpg(int(pgid), sig)
    except ProcessLookupError:
        pass
    except OSError as e:
        if e.errno != errno.ESRCH:
            logging.warning(f"[celery] killpg({pgid}) failed: {e}")


def stop_task(task_id):
    """Step 1 (§4): cooperative -- SIGTERM the task process."""
    app.control.revoke(task_id, terminate=True, signal="SIGTERM")


def hard_kill_task(task_id, r):
    """Step 2 (§4): STOP_GRACE_SECONDS elapsed with no terminal state.

    SIGKILL both the task's own OS process and any recorded subprocess
    group -- the task process dying does not take an orphaned grandchild
    with it.
    """
    _killpg_if_recorded(r, task_id, signal.SIGKILL)
    app.control.revoke(task_id, terminate=True, signal="SIGKILL")


@app.task(name="bsimvis.escalate_stop")
def escalate_stop_task(task_id, job_id):
    """Scheduled (countdown=STOP_GRACE_SECONDS) by job_service.cancel_job.

    Cooperative stop already fired (stop_task, or a handler's own
    is_cancelled() checkpoint). If the *task process itself* hasn't reached
    a terminal Celery state by the time this fires, hard-kill is the
    backstop for the ~29 handlers that don't checkpoint at all (plan §4
    item 2).

    Checks the Celery result, not job:<id>'s own status field -- cancel_job
    stamps CANCELLED/completed_at synchronously the instant the user clicks
    stop, before the underlying process has necessarily exited, so that
    field can't distinguish "asked to stop" from "actually stopped."
    """
    from celery.result import AsyncResult

    from bsimvis.app.services.redis_client import get_queue_redis

    res = AsyncResult(task_id, app=app)
    if res.state in ("SUCCESS", "FAILURE", "REVOKED"):
        return  # cooperative stop already won the race
    hard_kill_task(task_id, get_queue_redis())
