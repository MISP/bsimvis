"""Memory admission control, with weights taken from measurement.

The draft version of admission control named cluster_*, build_bin_sim and
over-threshold binary analysis as the heavy job types. enrich_features -- the
job that actually OOM-killed ten workers -- was not on that list. So weights
here are the peak RSS a worker actually observed for a job type, and an
unmeasured type calibrates itself after one run.

The failure mode that matters most is the leak: a worker killed between
reserving and releasing would hold budget forever, and enough of those starve
the fleet into a standstill indistinguishable from the outage being fixed.

Run: uv run python test_job_admission.py
"""

import os

from bsimvis.app.services.job_service import (
    JobStatus,
    MEM_DEFAULT_COST,
    MEM_RESERVED_KEY,
    MEM_USED_KEY,
)
from test_job_leases import make_service

GB = 1024**3


def budgeted(gb):
    """A service with a fixed budget, independent of the host's real RAM."""
    os.environ["JOB_MEMORY_BUDGET_MB"] = str(int(gb * 1024))
    return make_service()


def teardown():
    os.environ.pop("JOB_MEMORY_BUDGET_MB", None)


# --------------------------------------------------------------------------
# weights come from measurement
# --------------------------------------------------------------------------


def test_unmeasured_job_type_uses_the_default():
    svc = budgeted(16)
    assert svc.job_cost("enrich_features") == MEM_DEFAULT_COST


def test_measured_peak_becomes_the_cost():
    svc = budgeted(16)
    svc.record_job_peak("enrich_features", 3 * GB)
    assert svc.job_cost("enrich_features") == 3 * GB


def test_only_the_largest_peak_is_kept():
    svc = budgeted(16)
    svc.record_job_peak("enrich_features", 3 * GB)
    svc.record_job_peak("enrich_features", 1 * GB)  # a cheap run later
    assert svc.job_cost("enrich_features") == 3 * GB, "a cheap run must not lower it"


# --------------------------------------------------------------------------
# admission
# --------------------------------------------------------------------------


def test_fleet_cannot_overcommit_the_budget():
    svc = budgeted(8)
    svc.record_job_peak("enrich_features", 3 * GB)

    assert svc.try_admit("j1", "enrich_features") is True
    assert svc.try_admit("j2", "enrich_features") is True
    # 9 GB > 8 GB budget
    assert svc.try_admit("j3", "enrich_features") is False

    assert int(svc.r.get(MEM_USED_KEY)) == 6 * GB, "refusal must roll its cost back"


def test_releasing_frees_the_budget_again():
    svc = budgeted(8)
    svc.record_job_peak("enrich_features", 3 * GB)
    svc.try_admit("j1", "enrich_features")
    svc.try_admit("j2", "enrich_features")
    assert svc.try_admit("j3", "enrich_features") is False

    svc.release_admission("j1")
    assert svc.try_admit("j3", "enrich_features") is True


def test_release_is_idempotent():
    svc = budgeted(8)
    svc.try_admit("j1", "cheap")
    svc.release_admission("j1")
    svc.release_admission("j1")
    assert int(svc.r.get(MEM_USED_KEY)) == 0


def test_a_job_bigger_than_the_whole_budget_still_runs():
    """Otherwise one expensive job type deadlocks the queue permanently."""
    svc = budgeted(2)
    svc.record_job_peak("enrich_features", 8 * GB)
    assert svc.try_admit("j1", "enrich_features") is True


def test_lease_release_returns_the_reservation():
    svc = budgeted(8)
    svc.record_job_peak("enrich_features", 3 * GB)
    svc.try_admit("j1", "enrich_features")
    svc.release_lease("j1")
    assert int(svc.r.get(MEM_USED_KEY)) == 0


# --------------------------------------------------------------------------
# leaks
# --------------------------------------------------------------------------


def test_reaper_reclaims_a_killed_workers_reservation():
    """The starvation case: reserved budget with no job left in flight."""
    svc = budgeted(8)
    svc.record_job_peak("enrich_features", 3 * GB)
    svc.try_admit("ghost", "enrich_features")
    # The worker died; nothing is in jobs:processing any more.
    assert int(svc.r.get(MEM_USED_KEY)) == 3 * GB

    svc.reap_expired()

    assert int(svc.r.get(MEM_USED_KEY)) == 0, "leaked reservation starved the fleet"
    assert svc.r.hkeys(MEM_RESERVED_KEY) == []


def test_reaper_leaves_a_live_reservation_alone():
    svc = budgeted(8)
    svc.record_job_peak("enrich_features", 3 * GB)
    svc.r.hset(
        "job:live",
        mapping={"id": "live", "type": "enrich_features", "status": JobStatus.RUNNING.value},
    )
    svc.r.lpush("jobs:processing", "live")
    svc.claim_lease("live", "worker-1", ttl=300)
    svc.try_admit("live", "enrich_features")

    svc.reap_expired()

    assert int(svc.r.get(MEM_USED_KEY)) == 3 * GB, "a running job lost its reservation"


def test_resync_rebuilds_the_counter_from_reality():
    svc = budgeted(8)
    svc.try_admit("j1", "a")
    svc.try_admit("j2", "b")
    svc.r.set(MEM_USED_KEY, 99 * GB)  # drifted

    assert svc.resync_admissions() == 2 * MEM_DEFAULT_COST
    assert int(svc.r.get(MEM_USED_KEY)) == 2 * MEM_DEFAULT_COST


if __name__ == "__main__":
    passed = 0
    try:
        for name, fn in sorted(list(globals().items())):
            if name.startswith("test_") and callable(fn):
                fn()
                print(f"  ok  {name}")
                passed += 1
    finally:
        teardown()
    print(f"\n{passed} passed")
