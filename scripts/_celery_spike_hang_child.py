"""Stand-in for a truly stuck Ghidra JVM: ignores SIGTERM and sleeps.

Used only by scripts/spike_celery_ghidra.py to prove the hard-kill path
(kill_utils.hard_kill_task) reaps a subprocess that a plain SIGTERM cannot
touch -- exactly the worst case a hung/looping Ghidra JVM represents.
"""

import signal
import time

signal.signal(signal.SIGTERM, signal.SIG_IGN)
time.sleep(300)
