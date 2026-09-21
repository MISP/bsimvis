#!/bin/bash
# Deploy isolated workers for one collection, pool, or pipeline.
# Usage: scripts/deploy_workers.sh --collection NAME [--count N]
set -euo pipefail

TARGET_KIND=""
TARGET=""
COUNT=1
while [[ $# -gt 0 ]]; do
    case "$1" in
        --collection|--pool|--pipeline)
            [[ -z "$TARGET_KIND" ]] || { echo "Choose one target." >&2; exit 2; }
            TARGET_KIND=${1#--}
            TARGET=${2:?Missing value for $1}
            shift 2
            ;;
        -n|--count)
            COUNT=${2:?Missing count}
            shift 2
            ;;
        *) echo "Usage: $0 --collection NAME | --pool ID | --pipeline UUID [--count N]" >&2; exit 2 ;;
    esac
done
[[ -n "$TARGET_KIND" ]] || { echo "One target is required." >&2; exit 2; }
[[ "$COUNT" =~ ^[1-9][0-9]*$ ]] || { echo "Count must be positive." >&2; exit 2; }
[[ "$TARGET" =~ ^[A-Za-z0-9_.:-]+$ ]] || { echo "Target contains unsafe characters." >&2; exit 2; }

if [[ -f .env ]]; then
    export $(grep -v '^#' .env | xargs)
fi
PROJECT_NAME=${PROJECT_NAME:-bsimvis}
PROJECT_NAME=${PROJECT_NAME//./_}
PYTHON_CMD="uv run python"
if [[ -d .venv ]]; then PYTHON_CMD="$(pwd)/.venv/bin/python3"; fi

case "$TARGET_KIND" in
    collection) FILTER="WORKER_COLLECTION='$TARGET'"; LABEL="collection-${TARGET}" ;;
    pool) FILTER="WORKER_COLLECTION='pool:${TARGET}'"; LABEL="pool-${TARGET}" ;;
    pipeline) FILTER="WORKER_PIPELINE='$TARGET'"; LABEL="pipeline-${TARGET}" ;;
esac
SESSION="${PROJECT_NAME}-dedicated-${LABEL}"

command -v tmux >/dev/null || { echo "Error: tmux is required." >&2; exit 1; }
for i in $(seq 1 "$COUNT"); do
    name="dedicated-${LABEL}-${i}"
    command_line="${FILTER} PROJECT_NAME='${SESSION}' PYTHON_CMD='${PYTHON_CMD}' bash scripts/worker-supervisor.sh '${name}'"
    if tmux has-session -t "$SESSION" 2>/dev/null; then
        if tmux list-windows -t "$SESSION" -F '#{window_name}' | grep -Fxq "$name"; then
            echo "Window $name already running."
        else
            tmux new-window -t "$SESSION" -n "$name" bash -c "$command_line"
        fi
    else
        tmux new-session -d -s "$SESSION" -n "$name" bash -c "$command_line"
    fi
done

echo "Deployed $COUNT dedicated worker(s) for $TARGET_KIND=$TARGET in tmux session $SESSION."
echo "Stop them: tmux kill-session -t $SESSION"
