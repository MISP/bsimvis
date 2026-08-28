"""Reading this process's memory, in one place.

Peak RSS drives three separate things now -- the worker's admission weights, the
Ghidra child's self-report, and phase-level profiling of the clustering jobs --
and every one of them had grown its own copy of the same /proc parsing.

`VmHWM` is the kernel's own high-water mark, which beats sampling: a phase that
allocates and frees between two samples is invisible to a sampler and exact
here.
"""

import logging
import os

_LOG_PHASES = os.getenv("MEM_PHASE_LOG", "0") not in ("0", "", "false", "False")


def peak_rss():
    """Peak RSS in bytes since the last reset, or 0 if unavailable."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


def current_rss():
    """Resident set size right now, in bytes."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


def reset_peak():
    """Clears VmHWM so the next read measures from here.

    Linux exposes this via clear_refs. Best-effort: kernels that refuse simply
    leave the previous high-water mark in place, which over-reports rather than
    under-reports, so callers never get a falsely low number.
    """
    try:
        with open("/proc/self/clear_refs", "w") as f:
            f.write("5")
        return True
    except OSError:
        return False


def phase(label, job_service=None, job_id=None):
    """Logs current and peak RSS at a phase boundary.

    Off unless MEM_PHASE_LOG is set, because this is diagnostic: it answers
    "which part of this job holds the memory", which is not something you can
    infer from a single per-job peak. Enable it, run the job once, read the log.

    This is the shared checkpoint every handler calls (job-system-rework-plan.md
    §5): routes through JobService.update_progress so phase, message and RSS
    land in one write on the job_log stream, instead of a separate add_log
    call with the RSS baked into the message text.
    """
    if not _LOG_PHASES:
        return
    cur, peak = current_rss(), peak_rss()
    msg = f"[mem] {label}: rss={cur / 1024**3:.2f} GiB peak={peak / 1024**3:.2f} GiB"
    logging.info(msg)
    if job_service and job_id:
        try:
            job_service.update_progress(
                job_id, message=msg, phase=label, rss_current=cur, rss_peak=peak
            )
        except Exception:
            pass
