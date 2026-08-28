#!/bin/bash
# Keep one bsimvis worker alive, and record what it cost in memory.
#
#   scripts/worker-supervisor.sh worker-3
#
# Run as the command of a tmux window by launch_tmux.sh. Two jobs:
#
#  1. Restart the worker when it dies. The restart loop deliberately lives
#     OUTSIDE the systemd scope: `systemd-run --scope` moves only the worker
#     into the new cgroup, so a MemoryMax OOM kill takes the worker and leaves
#     this loop running to start the next one. Before this existed an OOM kill
#     ended the worker, ended the window's shell, and the tmux window vanished
#     -- the whole fleet could die with no trace and no restart.
#
#  2. Sample the scope's memory.peak while the worker runs, and print it on
#     exit. That is the number that decides whether MemoryMax is too low, and
#     it is unreadable after the fact because the cgroup dies with the scope.
#
# Output goes to both the tmux window and $LOG_DIR/<name>.log, so evidence
# survives the window.
set -uo pipefail

NAME=${1:?usage: worker-supervisor.sh <worker-name>}

LOG_DIR=${LOG_DIR:-"$(pwd)/logs"}
PYTHON_CMD=${PYTHON_CMD:-uv run python}
PROJECT_NAME=${PROJECT_NAME:-bsimvis}
WORKER_MEMORY_MAX=${WORKER_MEMORY_MAX:-}
WORKER_RESTART_DELAY=${WORKER_RESTART_DELAY:-5}
# Under real host pressure the kernel picked kvrocks -- the datastore -- as its
# first victim, because every process here inherits oom_score_adj=+200 and
# kvrocks is the biggest RSS in the session. An unprivileged process can only
# RAISE its own oom_score_adj, so kvrocks cannot protect itself; making workers
# the maximum-score victim achieves the same ordering without root. A worker
# dying is recoverable (the lease reaper requeues its job); kvrocks dying is not.
WORKER_OOM_SCORE_ADJ=${WORKER_OOM_SCORE_ADJ:-1000}
# 0 = restart forever. A crash-looping worker still backs off by the delay.
WORKER_MAX_RESTARTS=${WORKER_MAX_RESTARTS:-0}
# Same connection the app/reaper use (bsimvis/app/services/redis_client.py),
# so the reaper can read what this script saw about a dead worker.
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}

mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/${NAME}.log"

UNIT="bsimvis-${PROJECT_NAME}-${NAME}.scope"
PEAK_FILE="$(mktemp -t "bsimvis-peak-${NAME}.XXXXXX")"
trap 'rm -f "$PEAK_FILE"' EXIT

# Resolve the scope's cgroup and sample memory.peak into $PEAK_FILE until the
# scope goes away. systemctl reports the cgroup path relative to the v2 mount,
# which avoids guessing the user.slice/user@.service nesting.
sample_peak() {
    local peak_path="" rel="" i=0
    while [ $i -lt 30 ]; do
        rel=$(systemctl --user show -p ControlGroup --value "$UNIT" 2>/dev/null)
        if [ -n "$rel" ] && [ -r "/sys/fs/cgroup${rel}/memory.peak" ]; then
            peak_path="/sys/fs/cgroup${rel}/memory.peak"
            break
        fi
        sleep 1
        i=$((i + 1))
    done
    [ -n "$peak_path" ] || return 0
    while [ -r "$peak_path" ]; do
        cat "$peak_path" > "$PEAK_FILE" 2>/dev/null
        # Also outside the tmpdir: teardown kills this session before the
        # supervisor can print its exit line, and the cgroup dies with the
        # scope, so without this the peak of a clean run is unrecoverable.
        cat "$peak_path" > "$LOG_DIR/${NAME}.peak" 2>/dev/null
        sleep 5
    done
}

human() {  # bytes -> GiB, 2dp
    awk -v b="${1:-0}" 'BEGIN { if (b+0 <= 0) print "n/a"; else printf "%.2f GiB", b/1073741824 }'
}

# Records this exit for the job reaper (job_service.py:_classify_worker_death)
# to read -- otherwise a SIGKILL'd worker leaves no trace of *why* the lease
# expired, and an OOM kill is indistinguishable from a genuinely frozen
# process. Best-effort: a Redis hiccup here must never block the restart loop.
record_worker_exit() {
    local rc="$1" peak="$2" ts
    ts=$(($(date +%s%N) / 1000000))
    local args=(rc "$rc" ts "$ts")
    [ -n "$peak" ] && args+=(peak "$peak")
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" HSET "worker_exit:${NAME}" "${args[@]}" > /dev/null 2>&1
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" EXPIRE "worker_exit:${NAME}" 3600 > /dev/null 2>&1
    return 0
}

WORKER_FAST_FAIL_SECONDS=${WORKER_FAST_FAIL_SECONDS:-30}
WORKER_MAX_RESTART_DELAY=${WORKER_MAX_RESTART_DELAY:-120}

restarts=0
fast_failures=0
while true; do
    echo "[supervisor] starting ${NAME} (restart ${restarts}, MemoryMax=${WORKER_MEMORY_MAX:-none})"
    started_at=$SECONDS

    if [ -n "$WORKER_MEMORY_MAX" ] && command -v systemd-run > /dev/null; then
        # A named unit is what lets teardown stop the scope and lets us find
        # its cgroup. An anonymous scope survives `tmux kill-session` forever.
        systemctl --user reset-failed "$UNIT" 2>/dev/null
        : > "$PEAK_FILE"
        sample_peak &
        sampler=$!
        # OOMScoreAdjust is NOT a valid property for a scope unit -- systemd
        # rejects it with "Unknown assignment" and nothing starts. The worker
        # sets its own oom_score_adj from WORKER_OOM_SCORE_ADJ instead, which
        # is allowed because raising your own score never needs privilege.
        WORKER_OOM_SCORE_ADJ="$WORKER_OOM_SCORE_ADJ" \
        systemd-run --user --scope -q --unit="$UNIT" \
            -p MemoryMax="$WORKER_MEMORY_MAX" -p MemoryAccounting=yes \
            $PYTHON_CMD bsimvis/worker.py --name "$NAME"
        rc=$?
        kill "$sampler" 2> /dev/null
        wait "$sampler" 2> /dev/null
        peak=$(cat "$PEAK_FILE" 2> /dev/null)
    else
        $PYTHON_CMD bsimvis/worker.py --name "$NAME"
        rc=$?
        peak=""
    fi

    record_worker_exit "$rc" "$peak"

    # 137 = SIGKILL, which under a MemoryMax scope means the cgroup OOM killer.
    if [ "$rc" -eq 137 ]; then
        echo "[supervisor] ${NAME} OOM-KILLED (rc=137) peak=$(human "$peak") limit=${WORKER_MEMORY_MAX:-none}"
    else
        echo "[supervisor] ${NAME} exited rc=${rc} peak=$(human "$peak")"
    fi

    # Clean exit means we asked it to stop (SIGTERM handler sets running=False).
    if [ "$rc" -eq 0 ]; then
        echo "[supervisor] ${NAME} stopped cleanly; supervisor exiting."
        break
    fi

    restarts=$((restarts + 1))
    if [ "$WORKER_MAX_RESTARTS" -gt 0 ] && [ "$restarts" -ge "$WORKER_MAX_RESTARTS" ]; then
        echo "[supervisor] ${NAME} hit WORKER_MAX_RESTARTS=${WORKER_MAX_RESTARTS}; giving up."
        break
    fi

    # Back off when the worker dies immediately. A misconfiguration that stops
    # the worker booting at all would otherwise spin at the base delay forever
    # and bury the actual error under thousands of restart lines -- which is
    # exactly what a bad systemd property did on the first run of this script.
    if [ "$((SECONDS - started_at))" -lt "$WORKER_FAST_FAIL_SECONDS" ]; then
        fast_failures=$((fast_failures + 1))
    else
        fast_failures=0
    fi
    delay=$WORKER_RESTART_DELAY
    i=0
    while [ "$i" -lt "$fast_failures" ] && [ "$delay" -lt "$WORKER_MAX_RESTART_DELAY" ]; do
        delay=$((delay * 2))
        i=$((i + 1))
    done
    [ "$delay" -gt "$WORKER_MAX_RESTART_DELAY" ] && delay=$WORKER_MAX_RESTART_DELAY

    # An OOM kill is a real crash, not a config error. Conflating the two sent
    # me hunting a broken systemd property while the actual cause was a job
    # genuinely exceeding MemoryMax.
    if [ "$fast_failures" -ge 3 ]; then
        if [ "$rc" -eq 137 ]; then
            echo "[supervisor] ${NAME} OOM-killed ${fast_failures}x in a row -- a job is exceeding MemoryMax=${WORKER_MEMORY_MAX}. See scripts/job_memory_report.py."
        else
            echo "[supervisor] ${NAME} has failed ${fast_failures}x within ${WORKER_FAST_FAIL_SECONDS}s with no OOM kill -- this looks like a misconfiguration."
        fi
    fi
    echo "[supervisor] restarting ${NAME} in ${delay}s..."
    sleep "$delay"
done 2>&1 | tee -a "$LOG"
