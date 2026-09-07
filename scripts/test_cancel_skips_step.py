"""Cancelling one job must not freeze the pipeline it belongs to.

Cancelling by hand is how you fast-forward a pipeline past a step you don't
want. It used to do the opposite: cancel_job marked the job and stopped there,
so the parent kept waiting on a member that would never report again and the
whole pipeline stalled.

Run: uv run python scripts/test_cancel_skips_step.py
"""

import json

from bsimvis.app.services.job_service import JobStatus

from test_job_leases import make_service


def build(svc, parent_id, ptype, child_ids, status=JobStatus.RUNNING.value):
    svc.r.hset(
        f"job:{parent_id}",
        mapping={
            "id": parent_id,
            "type": ptype,
            "status": status,
            "task_ids": json.dumps(child_ids),
        },
    )
    for cid in child_ids:
        svc.r.hset(
            f"job:{cid}",
            mapping={
                "id": cid,
                "type": "idx_features",
                "jtype": "idx_features",
                "status": JobStatus.PENDING.value,
                "parent_id": parent_id,
            },
        )


def test_cancelling_a_group_member_fires_the_barrier():
    svc = make_service()
    build(svc, "grp", "group", ["a", "b"])
    svc.complete_job("a")

    svc.cancel_job("b")

    assert svc.r.hget("job:grp", "status") == JobStatus.COMPLETED.value


def test_cancelling_the_running_step_starts_the_next_one():
    svc = make_service()
    build(svc, "pipe", "pipeline", ["s1", "s2"])
    svc.r.hset("job:s1", "status", JobStatus.RUNNING.value)

    svc.cancel_job("s1")

    assert svc.r.lrange("jobs:pending", 0, -1) == ["s2"]


def test_a_late_completion_does_not_start_the_next_step_twice():
    # The worker keeps running a job cancelled mid-flight and still reports it
    # done. That must not re-enqueue the step the cancel already started.
    svc = make_service()
    build(svc, "pipe", "pipeline", ["s1", "s2"])
    svc.r.hset("job:s1", "status", JobStatus.RUNNING.value)
    svc.cancel_job("s1")

    svc.complete_job("s1")

    assert svc.r.hget("job:s1", "status") == JobStatus.CANCELLED.value
    assert svc.r.lrange("jobs:pending", 0, -1) == ["s2"]


def test_a_cancelled_step_is_skipped_when_its_turn_comes():
    svc = make_service()
    build(svc, "pipe", "pipeline", ["s1", "s2", "s3"])
    svc.r.hset("job:s1", "status", JobStatus.RUNNING.value)
    svc.cancel_job("s2")  # queued step, cancelled ahead of its turn

    svc.complete_job("s1")

    assert svc.r.hget("job:s2", "status") == JobStatus.CANCELLED.value
    assert svc.r.lrange("jobs:pending", 0, -1) == ["s3"]


def test_cancelling_the_pipeline_still_stops_everything():
    svc = make_service()
    build(svc, "pipe", "pipeline", ["s1", "s2"])
    svc.r.hset("job:s1", "status", JobStatus.RUNNING.value)

    svc.cancel_job("pipe")

    assert svc.r.hget("job:pipe", "status") == JobStatus.CANCELLED.value
    assert svc.r.hget("job:s2", "status") == JobStatus.CANCELLED.value
    assert svc.r.lrange("jobs:pending", 0, -1) == []


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("all passed")
