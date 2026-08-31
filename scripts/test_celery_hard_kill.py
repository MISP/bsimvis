"""Plan §9 phase 5 spike: does revoke(terminate=True) really hard-kill a
subprocess-spawning task, and does worker_max_memory_per_child recycle a
worker under real memory pressure?

Two things this proves, both non-obvious from reading Celery's docs alone:

1. A plain `revoke(terminate=True)` kills only the OS process Celery is
   running the task in. A subprocess that task spawned (ghidra_task.py's
   JVM) is NOT in that process's wait-tree in a way the signal reaches --
   it survives, orphaned. `kill_utils.hard_kill_task`'s process-group
   killpg is what actually reaps it. Both the failure and the fix are
   demonstrated here, not just asserted.
2. worker_max_memory_per_child recycles the pool child once its own RSS
   crosses the threshold -- confirmed against a task that allocates real
   memory in-process (mirrors what would happen for a leaf task that does
   its own work in-process, as opposed to ghidra_analyze's subprocess-
   isolated shape where this setting does NOT see the JVM's memory at all,
   per celery_app.py's comment).

Requires a Celery worker already running against this process's
REDIS_HOST/REDIS_PORT (see scripts/spike_celery_ghidra_up.sh or run
`celery -A bsimvis.celery_app worker --pool=prefork --concurrency=1` by
hand against the isolated stack -- never point this at a shared Redis).

Run: uv run python scripts/test_celery_hard_kill.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bsimvis.app.services.redis_client import get_queue_redis, init_redis
from bsimvis.celery_app import app
from bsimvis.tasks.ghidra_task import CHILD_PGID_KEY
from bsimvis.tasks.kill_utils import hard_kill_task
from bsimvis.tasks.test_hang_task import hang_task, mem_hog_task


def _child_pid(r, task_id, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        pid = r.get(CHILD_PGID_KEY.format(task_id=task_id))
        if pid:
            return int(pid)
        time.sleep(0.1)
    raise TimeoutError(f"child pid never recorded for task {task_id}")


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def test_naive_revoke_orphans_the_subprocess(r):
    """Negative control: proves the risk this module exists to fix."""
    res = hang_task.delay()
    pid = _child_pid(r, res.id)
    assert _alive(pid), "child never started"

    app.control.revoke(res.id, terminate=True, signal="SIGTERM")
    time.sleep(3)

    assert _alive(pid), (
        f"expected orphaned child pid {pid} to survive a naive revoke() -- "
        "if this fails, Celery's signal delivery changed and the "
        "kill_utils workaround may no longer be needed"
    )
    print(f"  ok  naive revoke() orphans subprocess pid {pid} (as expected)")
    os.kill(pid, 9)  # test cleanup, not part of the assertion


def test_hard_kill_task_reaps_the_subprocess(r):
    """The fix: kill_utils.hard_kill_task actually kills the group."""
    res = hang_task.delay()
    pid = _child_pid(r, res.id)
    assert _alive(pid), "child never started"

    hard_kill_task(res.id, r)
    deadline = time.time() + 5
    while _alive(pid) and time.time() < deadline:
        time.sleep(0.1)

    assert not _alive(pid), f"hard_kill_task left pid {pid} alive"
    print(f"  ok  hard_kill_task reaps subprocess pid {pid}")


def test_worker_max_memory_recycles_child():
    """Confirms the configured cap actually triggers a recycle."""
    pid1 = mem_hog_task.apply_async(args=[300]).get(timeout=30)
    pid2 = mem_hog_task.apply_async(args=[300]).get(timeout=30)
    print(f"  ok  worker pids across mem-hog tasks: {pid1} -> {pid2}")
    assert pid1 != pid2, (
        "expected worker_max_memory_per_child to recycle the pool child "
        "between two 300MB allocations under a 200MB cap"
    )


if __name__ == "__main__":
    init_redis(redis_port=int(os.environ["REDIS_PORT"]))
    r = get_queue_redis()
    insp = app.control.inspect(timeout=5)
    if not insp.ping():
        print("No Celery worker responding -- start one against this stack first.")
        sys.exit(1)

    test_naive_revoke_orphans_the_subprocess(r)
    test_hard_kill_task_reaps_the_subprocess(r)
    test_worker_max_memory_recycles_child()
    print("\nall spike checks passed")
