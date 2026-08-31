"""Task-side half of the hard-kill regression test (scripts/test_celery_hard_kill.py).

Spawns scripts/_celery_spike_hang_child.py -- a process that ignores
SIGTERM -- the same way worker.py's _run_ghidra_out_of_process spawns the
real Ghidra subprocess (start_new_session + pgid recorded via kill_utils'
shared key). Proves `kill_utils.hard_kill_task` actually reaps a subprocess
a plain revoke() cannot touch. Not wired into any real job type; exists
only for that test.
"""

import os
import subprocess
import sys

from celery import current_task

from bsimvis.app.services.redis_client import get_queue_redis
from bsimvis.celery_app import app
from bsimvis.tasks.kill_utils import clear_child_pgid, record_child_pgid

_CHILD_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "scripts",
    "_celery_spike_hang_child.py",
)


@app.task(name="bsimvis.test_hang", bind=True)
def hang_task(self):
    r = get_queue_redis()
    task_id = current_task.request.id if current_task else None
    proc = subprocess.Popen([sys.executable, _CHILD_SCRIPT], start_new_session=True)
    record_child_pgid(r, task_id, proc.pid)
    try:
        proc.wait()
    finally:
        clear_child_pgid(r, task_id)
    return True


@app.task(name="bsimvis.test_noop")
def noop_task():
    """Pure dispatch-overhead probe (scripts/benchmark_celery_dispatch.py,
    job-system-rework-plan.md §8): does nothing, so a delay()+get() round
    trip measures Celery's broker/backend cost in isolation from any real
    handler's own runtime.
    """
    return True


@app.task(name="bsimvis.test_mem_hog")
def mem_hog_task(mb):
    """Allocates `mb` megabytes in the worker process itself (no subprocess).

    Used to trigger worker_max_memory_per_child recycling -- unlike
    ghidra_task's JVM, this pressure is real Python-process RSS, which is
    the only kind that setting can see.
    """
    block = bytearray(mb * 1024 * 1024)
    for i in range(0, len(block), 4096):
        block[i] = 1
    return os.getpid()
