#!/bin/bash

# Function to start a tmux window or create the session
start_tmux() {
    window_name=$1
    command=$2
    # Build env prefix string for tmux command
    local env_prefix=""
    if [ -f .env ]; then
        env_prefix=$(grep -v '^#' .env | xargs)
        env_prefix="$env_prefix "
    fi
    if tmux has-session -t "${PROJECT_NAME}" 2>/dev/null; then
        # Session exists, create a new window
        # Check if window already exists
        if tmux list-windows -t "${PROJECT_NAME}" -F '#{window_name}' | grep -q "^${window_name}$"; then
            echo "Window ${window_name} is already running."
        else
            echo "Starting window ${window_name}..."
            tmux new-window -t "${PROJECT_NAME}" -n "${window_name}" bash -c "${env_prefix}${command}"
        fi
    else
        # Create new session with the first window
        echo "Starting session ${PROJECT_NAME} with window ${window_name}..."
        tmux new-session -d -s "${PROJECT_NAME}" -n "${window_name}" bash -c "${env_prefix}${command}"
    fi
}

# Print a terminal-clickable hyperlink (OSC 8; plain URL fallback in dumb terms)
hyperlink() {
    local url=$1
    local label=${2:-$1}
    printf '\033]8;;%s\033\\%s\033]8;;\033\\' "$url" "$label"
}

# Wait until a TCP port is accepting connections (max 30s)
wait_for_port() {
    local port=$1
    local name=$2
    local max=30
    local i=0
    echo -n "  Waiting for ${name} on port ${port}..."
    while ! (echo > /dev/tcp/localhost/${port}) 2>/dev/null; do
        sleep 1
        i=$((i+1))
        if [ $i -ge $max ]; then
            echo " TIMEOUT (${max}s). Service may not be ready."
            return 1
        fi
    done
    echo " ready (${i}s)."
}

# Wait until a TCP port is no longer accepting connections (max 10s before force kill)
wait_for_port_free() {
    local port=$1
    local name=$2
    local max=10
    local i=0
    echo -n "  Waiting for ${name} port ${port} to be freed..."

    # Check if any process is listening on the port
    while lsof -i :"$port" >/dev/null 2>&1; do
        sleep 1
        i=$((i+1))
        if [ $i -ge $max ]; then
            echo " TIMEOUT. Force killing..."
            # Use fuser or lsof to kill specifically the process on that port
            fuser -k -n tcp "$port" 2>/dev/null || lsof -t -i tcp:"$port" | xargs kill -9 2>/dev/null
            sleep 1
            break
        fi
    done
    echo " free."
}

# Check for tmux
if ! command -v tmux > /dev/null; then
    echo "Error: 'tmux' is not installed. Please install it first."
    exit 1
fi

# Parse optional flags
CLEAR=false
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --clear) CLEAR=true ;;
        *) echo "Unknown option: $1" ; exit 1 ;;
    esac
    shift
done

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Defaults & Config
APP_HOST=${APP_HOST:-0.0.0.0}
APP_PORT=${APP_PORT:-5000}
REDIS_PORT=${REDIS_PORT:-6379}
KVROCKS_PORT=${KVROCKS_PORT:-6666}
# Each worker holds a Ghidra JVM (1536 MB heap plus JVM native overhead,
# measured 2.4 GB peak). Run each in its own systemd scope so a runaway worker
# is OOM-killed instead of thrashing the host into a freeze. Empty = no
# backstop. scripts/worker-supervisor.sh prints the scope's memory.peak on every
# exit, which is how you find out whether this number is too low.
WORKER_MEMORY_MAX=${WORKER_MEMORY_MAX:-3G}

# The fleet budget must be the SAME number the cgroup enforces. It used to be
# hardcoded at 2.5 while the cgroup enforced 3G, so the host could be sold
# 20% more worker memory than the kernel would ever allow -- reserve 8 GB for
# kvrocks, redis and the desktop, then divide by the real per-worker cap.
WORKER_BUDGET_GB=$(awk -v v="${WORKER_MEMORY_MAX:-2.5G}" 'BEGIN {
    n = v + 0
    if (v ~ /[Mm]$/) n /= 1024
    else if (v ~ /[Kk]$/) n /= 1048576
    print (n > 0 ? n : 2.5)
}')
WORKERS_MAX_BY_RAM=$(awk -v b="$WORKER_BUDGET_GB" '/MemTotal/ {m=$2/1024/1024; n=int((m-8)/b); print (n>1?n:1)}' /proc/meminfo)
WORKERS_COUNT=${WORKERS_COUNT:-5}
if [ "$WORKERS_COUNT" -gt "$WORKERS_MAX_BY_RAM" ]; then
    echo "Capping WORKERS_COUNT ${WORKERS_COUNT} -> ${WORKERS_MAX_BY_RAM} (host RAM, ${WORKER_BUDGET_GB} GB/worker)"
    WORKERS_COUNT=$WORKERS_MAX_BY_RAM
fi
ENABLE_MILVUS=${ENABLE_MILVUS:-false}
DATA_BASE_DIR=${DATA_BASE_DIR:-"$(pwd)/data"}
PROJECT_NAME=${PROJECT_NAME:-bsimvis}
PROJECT_NAME="${PROJECT_NAME//./_}"

# Optional tmux cleanup (default off, enable with --clear or CLEAN_TMUX=true)
CLEAN_TMUX=${CLEAN_TMUX:-$CLEAR}

if [ "$CLEAN_TMUX" = "true" ]; then
    # Worker scopes are separate systemd units, not children of the tmux shell:
    # kill-session alone leaves them running and still eating the queue.
    #
    # Stop them BEFORE the datastores, and in a single systemctl call. One call
    # per unit inside a loop is what made teardown take N x (SIGTERM -> exit):
    # `systemctl stop` blocks until that unit is gone, so ten workers stopped
    # one after another instead of all at once. Passing every unit to one
    # invocation enqueues independent stop jobs that systemd runs in parallel,
    # so the wall time is the slowest worker, not their sum.
    UNITS=$(systemctl --user list-units --plain --no-legend "bsimvis-${PROJECT_NAME}-worker-*.scope" 2>/dev/null | awk '{print $1}')
    if [ -n "$UNITS" ]; then
        echo "Stopping leftover worker scopes: $(echo $UNITS | tr '\n' ' ')"
        systemctl --user stop $UNITS 2>/dev/null || true
    fi

    # Datastores go last. Killing Redis/Kvrocks first left every worker
    # spinning on connection errors (1s sleep per failed loop) while it was
    # still being asked to shut down -- slower teardown and a log full of
    # noise for a shutdown that was going fine.
    if command -v redis-cli > /dev/null; then
        echo "Sending shutdown commands to Redis and Kvrocks..."
        redis-cli -p "${REDIS_PORT}" shutdown 2>/dev/null || true
        redis-cli -p "${KVROCKS_PORT}" shutdown 2>/dev/null || true
    fi

    if tmux has-session -t "${PROJECT_NAME}" 2>/dev/null; then
        echo "Cleaning up tmux session ${PROJECT_NAME}..."
        tmux kill-session -t "${PROJECT_NAME}"

        # Wait for ports to be freed
        wait_for_port_free "${REDIS_PORT}" "Redis"
        wait_for_port_free "${KVROCKS_PORT}" "Kvrocks"
        if [ "$ENABLE_MILVUS" = "true" ]; then
            ETCD_PORT=${ETCD_PORT:-2379}
            MINIO_PORT=${MINIO_PORT:-9000}
            wait_for_port_free "${ETCD_PORT}" "Etcd"
            wait_for_port_free "${MINIO_PORT}" "Minio"
        fi
    fi
fi

# Add local bin to PATH
export PATH="$(pwd)/bin:$PATH"

# Check if core binaries exist
REQUIRED_BINS=("redis-server" "kvrocks")
for bin in "${REQUIRED_BINS[@]}"; do
    if ! command -v "$bin" > /dev/null; then
        echo "Error: Required binary '$bin' not found in PATH or bin/ directory."
        echo "Please run ./install.sh first."
        exit 1
    fi
done

# Check Milvus binaries only if enabled
if [ "$ENABLE_MILVUS" = "true" ]; then
    MILVUS_BINS=("etcd" "minio" "milvus")
    for bin in "${MILVUS_BINS[@]}"; do
        if ! command -v "$bin" > /dev/null; then
            echo "Warning: Milvus binary '$bin' not found. Disabling Milvus for this session."
            ENABLE_MILVUS="false"
            break
        fi
    done
fi


echo "--- Launching Services (Data: ${DATA_BASE_DIR}) ---"

# Ensure directories exist
mkdir -p "${DATA_BASE_DIR}/redis"
mkdir -p "${DATA_BASE_DIR}/kvrocks"
if [ "$ENABLE_MILVUS" = "true" ]; then
    mkdir -p "${DATA_BASE_DIR}/etcd"
    mkdir -p "${DATA_BASE_DIR}/minio"
fi

# Start Redis
start_tmux "redis" "redis-server --port ${REDIS_PORT} --dir ${DATA_BASE_DIR}/redis"

# Start Kvrocks
start_tmux "kvrocks" "kvrocks -c kvrocks.conf --port ${KVROCKS_PORT} --dir ${DATA_BASE_DIR}/kvrocks"

# Wait for both datastores to be ready before launching dependent services
wait_for_port "${REDIS_PORT}" "Redis"
wait_for_port "${KVROCKS_PORT}" "Kvrocks"

# Start Milvus stack if enabled
if [ "$ENABLE_MILVUS" = "true" ]; then
    ETCD_PORT=${ETCD_PORT:-2379}
    MINIO_PORT=${MINIO_PORT:-9000}
    MINIO_CONSOLE_PORT=${MINIO_CONSOLE_PORT:-9001}
    MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
    MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}

    echo "Launching Milvus stack (Etcd, Minio, Milvus)..."
    start_tmux "etcd" "etcd --data-dir ${DATA_BASE_DIR}/etcd --advertise-client-urls http://127.0.0.1:${ETCD_PORT} --listen-client-urls http://0.0.0.0:${ETCD_PORT}"
    
    start_tmux "minio" "MINIO_ROOT_USER=${MINIO_ACCESS_KEY} MINIO_ROOT_PASSWORD=${MINIO_SECRET_KEY} minio server ${DATA_BASE_DIR}/minio --address \":${MINIO_PORT}\" --console-address \":${MINIO_CONSOLE_PORT}\""
    
    # Give a moment for etcd and minio to start
    sleep 2
    
    start_tmux "milvus" "ETCD_ENDPOINTS=localhost:${ETCD_PORT} MINIO_ADDRESS=localhost:${MINIO_PORT} milvus run standalone"
fi

# Determine python command
PYTHON_CMD="python3"
if [ -d ".venv" ]; then
    PYTHON_CMD="$(pwd)/.venv/bin/python3"
elif command -v uv > /dev/null; then
    PYTHON_CMD="uv run python"
fi

# Start App
start_tmux "app" "${PYTHON_CMD} app.py"

# Start Workers
# Each window runs the supervisor, not the worker: the supervisor owns the
# systemd scope, restarts the worker after an OOM kill, and reports the scope's
# memory.peak. Running the worker directly meant a kill took the window with it.
echo "Starting ${WORKERS_COUNT} workers..."
LOG_DIR=${LOG_DIR:-"$(pwd)/logs"}
mkdir -p "$LOG_DIR"
for i in $(seq 1 $WORKERS_COUNT); do
    # --name is what makes the worker identifiable: without it every worker
    # registered as "worker-1" and the fleet looked like a single worker.
    start_tmux "worker-${i}" \
        "LOG_DIR='${LOG_DIR}' PYTHON_CMD='${PYTHON_CMD}' PROJECT_NAME='${PROJECT_NAME}' WORKER_MEMORY_MAX='${WORKER_MEMORY_MAX}' bash scripts/worker-supervisor.sh worker-${i}"
done

echo "--------------------------"
wait_for_port "${APP_PORT}" "App"
APP_URL="http://localhost:${APP_PORT}"
echo -n "Dashboard: "; hyperlink "${APP_URL}"; echo
echo "All services started in tmux session '${PROJECT_NAME}'."
echo "Use 'tmux attach -t ${PROJECT_NAME}' to view the session."
echo "Inside tmux, use Ctrl+b then n/p to switch between windows (services)."
echo "To stop all services and close the session: tmux kill-session -t ${PROJECT_NAME}"
