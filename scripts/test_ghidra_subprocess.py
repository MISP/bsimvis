"""Ghidra analysis runs out-of-process, retries, then gives up visibly.

Ghidra's embedded JVM crashes -- 27 hs_err_pid*.log dumps were sitting in the
repo root. In-process, one of those crashes killed the worker outright along
with whatever else it was doing. Out-of-process a crash costs one child, and a
binary that crashes the JVM every single time gets flagged rather than being
retried until the end of time.

Run: uv run python test_ghidra_subprocess.py
"""

import logging
import types

from bsimvis.worker import Worker


class FakeJobService:
    def __init__(self):
        self.logs = []

    def add_log(self, job_id, msg):
        self.logs.append(msg)


class FakeData:
    def __init__(self):
        self.unanalyzed = set()

    def sadd(self, key, val):
        self.unanalyzed.add((key, val))
        return 1


def make_worker(returncodes):
    """Worker whose subprocess call yields `returncodes` in order."""
    w = object.__new__(Worker)
    w.name = "test"
    w.id = "test-0"
    w.job_service = FakeJobService()
    w.r_data = FakeData()

    calls = []
    seq = list(returncodes)

    def fake_run(cmd, cwd=None):
        calls.append(cmd)
        return types.SimpleNamespace(returncode=seq[len(calls) - 1])

    w._fake_calls = calls
    import bsimvis.worker as mod

    mod.subprocess.run = fake_run
    return w


PAYLOAD = {"collection": "stdlib-ref", "file_md5": "deadbeef"}


def test_analysis_runs_in_a_child_process():
    w = make_worker([0])
    assert w._run_ghidra_out_of_process("j1", PAYLOAD) is True
    assert len(w._fake_calls) == 1
    cmd = w._fake_calls[0]
    assert "bsimvis.ghidra_job" in cmd, cmd
    assert "--job-id" in cmd and "j1" in cmd


def test_a_jvm_crash_is_retried():
    # -11 is SIGSEGV: the JVM dying, which is exactly what retrying is for.
    w = make_worker([-11, 0])
    assert w._run_ghidra_out_of_process("j1", PAYLOAD) is True
    assert len(w._fake_calls) == 2


def test_gives_up_after_the_retry_budget():
    w = make_worker([-11, -6, 1])
    assert w._run_ghidra_out_of_process("j1", PAYLOAD) is False
    assert len(w._fake_calls) == 3, "should not retry forever"


def test_a_file_that_always_crashes_is_flagged_unanalyzed():
    """Otherwise the file is silently missing from the collection."""
    w = make_worker([-11, -11, -11])
    w._run_ghidra_out_of_process("j1", PAYLOAD)
    assert ("stdlib-ref:files:unanalyzed", "deadbeef") in w.r_data.unanalyzed


def test_failure_is_recorded_in_the_job_log():
    w = make_worker([-11, -11, -11])
    w._run_ghidra_out_of_process("j1", PAYLOAD)
    joined = " ".join(w.job_service.logs)
    assert "signal 11" in joined, joined
    assert "flagged unanalyzed" in joined, joined


def test_worker_no_longer_starts_a_jvm():
    """The whole memory point: no JVM floor in a process that may never analyse.

    A 1.3-2.4 GB JVM inside a 3 GB cgroup left enrich_features about 0.6 GB.
    """
    import inspect

    src = inspect.getsource(Worker.__init__)
    assert "ensure_launcher" not in src, "worker still boots a JVM at startup"

    import bsimvis.worker as mod

    assert not hasattr(mod, "ghidra_service"), "worker still imports ghidra_service"


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    passed = 0
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
            passed += 1
    print(f"\n{passed} passed")
