#!/usr/bin/env python3
"""Self-check for the kvrocks stall policy in the bin-sim file walk
(_stall_action / _stall_delay in bsimvis/app/services/bin_sim_service.py).

No redis, no server: the policy is pure arithmetic over the attempt counters.
Run: python3 scripts/test_bin_sim_stall.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bsimvis.app.services.bin_sim_service import (  # noqa: E402
    _stall_action,
    _stall_delay,
)


def test_a_busy_server_gets_the_file_redone():
    for attempt in (1, 2, 3):
        assert _stall_action(attempt, 4, 0, 3, False) == "retry", attempt


def test_a_file_that_never_answers_is_skipped_not_fatal():
    # Out of attempts, but no run of stalls behind it: the build goes on.
    assert _stall_action(4, 4, 0, 3, False) == "skip"


def test_a_run_of_stalls_fails_the_job():
    # Third skip in a row -- the server is down, not busy. Fail loudly rather
    # than walk the rest of the collection into a hole.
    assert _stall_action(4, 4, 2, 3, False) == "fail"


def test_one_pair_builds_never_skip():
    # /api/bin_sim/diff asked for a specific doc; there is nothing to skip to.
    assert _stall_action(4, 4, 0, 3, True) == "fail"
    assert _stall_action(1, 4, 0, 3, True) == "retry"


def test_delay_escalates_and_is_capped():
    delays = [_stall_delay(a) for a in (1, 2, 3, 4)]
    assert delays == [5, 15, 45, 60], delays


if __name__ == "__main__":
    for fn in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        fn()
        print("ok  %s" % fn.__name__)
    print("all passed")
