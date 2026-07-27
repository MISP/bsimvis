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
REDIS_PORT=${REDIS_PORT:-6379}
KVROCKS_PORT=${KVROCKS_PORT:-6666}
# Each worker holds a Ghidra JVM (ghidra.max_heap_mb). Cap the fleet by RAM as
# well as cores: ~3 GB of host RAM per worker.
WORKERS_MAX_BY_RAM=$(awk '/MemTotal/ {print int($2/1024/1024/3)}' /proc/meminfo)
WORKERS_COUNT=${WORKERS_COUNT:-5}
if [ "$WORKERS_COUNT" -gt "$WORKERS_MAX_BY_RAM" ]; then
    echo "Capping WORKERS_COUNT ${WORKERS_COUNT} -> ${WORKERS_MAX_BY_RAM} (host RAM)"
    WORKERS_COUNT=$WORKERS_MAX_BY_RAM
fi
# Hard backstop: run each worker in its own systemd scope so a runaway worker is
# OOM-killed instead of thrashing the host into a freeze. Empty = no backstop.
WORKER_MEMORY_MAX=${WORKER_MEMORY_MAX:-3G}
ENABLE_MILVUS=${ENABLE_MILVUS:-false}
DATA_BASE_DIR=${DATA_BASE_DIR:-"$(pwd)/data"}
PROJECT_NAME=${PROJECT_NAME:-bsimvis}

# Optional tmux cleanup (default off, enable with --clear or CLEAN_TMUX=true)
CLEAN_TMUX=${CLEAN_TMUX:-$CLEAR}

if [ "$CLEAN_TMUX" = "true" ]; then
    # Try sending clean shutdown commands first
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
echo "Starting ${WORKERS_COUNT} workers..."
WORKER_WRAP=""
if [ -n "$WORKER_MEMORY_MAX" ] && command -v systemd-run > /dev/null; then
    WORKER_WRAP="systemd-run --user --scope -q -p MemoryMax=${WORKER_MEMORY_MAX} "
fi
for i in $(seq 1 $WORKERS_COUNT); do
    start_tmux "worker-${i}" "${WORKER_WRAP}${PYTHON_CMD} bsimvis/worker.py"
done

echo "--------------------------"
echo "All services started in tmux session '${PROJECT_NAME}'."
echo "Use 'tmux attach -t ${PROJECT_NAME}' to view the session."
echo "Inside tmux, use Ctrl+b then n/p to switch between windows (services)."
echo "To stop all services and close the session: tmux kill-session -t ${PROJECT_NAME}"
