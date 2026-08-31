"""Stop/restart escalation (plan §4): cooperative signal, then hard kill.

`AsyncResult.revoke(terminate=True, signal=...)` only reaches the single OS
process Celery is running the task in. A task that spawned a subprocess
(ghidra_task.py) leaves that subprocess orphaned on hard-kill unless
something explicitly kills its process group too -- this is that something.
"""

import errno
import logging
import os
import signal

from bsimvis.celery_app import app
from bsimvis.tasks.ghidra_task import CHILD_PGID_KEY


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
