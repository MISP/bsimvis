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
# 0 = restart forever. A crash-looping worker still backs off by the delay.
WORKER_MAX_RESTARTS=${WORKER_MAX_RESTARTS:-0}

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
        sleep 5
    done
}

human() {  # bytes -> GiB, 2dp
    awk -v b="${1:-0}" 'BEGIN { if (b+0 <= 0) print "n/a"; else printf "%.2f GiB", b/1073741824 }'
}

restarts=0
while true; do
    echo "[supervisor] starting ${NAME} (restart ${restarts}, MemoryMax=${WORKER_MEMORY_MAX:-none})"

    if [ -n "$WORKER_MEMORY_MAX" ] && command -v systemd-run > /dev/null; then
        # A named unit is what lets teardown stop the scope and lets us find
        # its cgroup. An anonymous scope survives `tmux kill-session` forever.
        systemctl --user reset-failed "$UNIT" 2>/dev/null
        : > "$PEAK_FILE"
        sample_peak &
        sampler=$!
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
    echo "[supervisor] restarting ${NAME} in ${WORKER_RESTART_DELAY}s..."
    sleep "$WORKER_RESTART_DELAY"
done 2>&1 | tee -a "$LOG"
