"""Worker registry and the throughput fields on /api/jobs/stats.

During the Aug 2026 OOM outage the dashboard reported `active_workers: 13` while
zero worker processes existed -- the field was really a count of jobs in an
active state, and nine of those were leaseless zombies. Every speed/ETA field
read 0.0 at the same time, because only similarity_service ever wrote the item
counters they were computed from. Between them that is why a total fleet death
looked like a busy system for hours.

These tests pin both: workers are counted by registration, and the throughput
fields move when work is moving.

Run: uv run python test_worker_registry.py
"""

import time

from bsimvis.app.services.job_service import JobStatus, WORKERS_KEY
from test_job_leases import make_service


def running_job(svc, job_id, **fields):
    base = {"id": job_id, "type": "enrich_features", "status": JobStatus.RUNNING.value}
    base.update({k: str(v) for k, v in fields.items()})
    svc.r.hset(f"job:{job_id}", mapping=base)
    svc.r.lpush("jobs:processing", job_id)


# --------------------------------------------------------------------------
# registry
# --------------------------------------------------------------------------


def test_registered_workers_are_counted():
    svc = make_service()
    for i in range(1, 6):
        svc.register_worker(f"worker-{i}-{1000 + i}")

    assert svc.count_active_workers() == 5
    assert len(svc.list_active_workers()) == 5


def test_expired_registration_drops_out():
    """An OOM-killed worker stops refreshing; nothing else has to notice."""
    svc = make_service()
    svc.register_worker("alive-1", ttl=60)
    svc.register_worker("killed-1", ttl=-1)  # heartbeat stopped a while ago

    assert svc.count_active_workers() == 1
    assert svc.list_active_workers() == ["alive-1"]
    # and the dead entry is actually gone, not merely filtered out
    assert svc.r.zcard(WORKERS_KEY) == 1


def test_clean_shutdown_unregisters():
    svc = make_service()
    svc.register_worker("worker-1-42")
    svc.unregister_worker("worker-1-42")
    assert svc.count_active_workers() == 0


def test_dead_fleet_holding_running_jobs_reports_zero_workers():
    """The exact outage shape: jobs look active, nobody is alive to run them."""
    svc = make_service()
    for i in range(13):
        running_job(svc, f"zombie-{i}")

    stats = svc.get_global_stats()

    assert stats["active_workers"] == 0, "a dead fleet must not look busy"
    assert stats["active_jobs_count"] == 13, "the job count is still reported"


# --------------------------------------------------------------------------
# throughput fields
# --------------------------------------------------------------------------


def test_item_counters_drive_speed_and_eta():
    svc = make_service()
    svc.register_worker("worker-1-42")
    running_job(svc, "j1", speed="10", total_items="1000", processed_items="400")

    stats = svc.get_global_stats()

    assert stats["remaining_items"] == 600
    assert stats["total_speed"] == 10.0
    assert stats["avg_speed"] == 10.0
    assert stats["global_eta"] == 60  # 600 items / 10 per second


def test_eta_falls_back_to_progress_when_no_item_counts():
    """enrich_features reports no counters, and used to yield a flat zero ETA."""
    svc = make_service()
    svc.register_worker("worker-1-42")
    started_ms = int((time.time() - 60) * 1000)  # 60s ago, 25% done
    running_job(svc, "enrich", progress="25", started_at=started_ms)

    stats = svc.get_global_stats()

    assert stats["remaining_items"] == 0  # genuinely unknown, not faked
    # 60s bought 25%, so ~180s remain. Wide bounds: wall clock moves under us.
    assert 150 <= stats["global_eta"] <= 210, stats["global_eta"]


def test_update_progress_records_counters_and_speed():
    svc = make_service()
    svc.r.hset("job:j1", mapping={"started_at": str(int((time.time() - 10) * 1000))})

    svc.update_progress("j1", 50, processed=500, total=1000)

    job = svc.r.hgetall("job:j1")
    assert job["progress"] == "50"
    assert job["total_items"] == "1000"
    assert job["processed_items"] == "500"
    assert 40 <= float(job["speed"]) <= 60, job["speed"]  # ~500 items / 10s


def test_update_progress_without_counts_still_works():
    """Most handlers pass a percent only; they must not start failing."""
    svc = make_service()
    svc.update_progress("j2", 80)
    assert svc.r.hgetall("job:j2")["progress"] == "80"
    assert "speed" not in svc.r.hgetall("job:j2")


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
            passed += 1
    print(f"\n{passed} passed")
