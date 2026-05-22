#!/bin/bash

# Function to start a screen session
start_screen() {
    session_name=$1
    command=$2
    if screen -list | grep -q "\.${session_name}"; then
        echo "Session ${session_name} is already running."
    else
        echo "Starting session ${session_name}..."
        screen -dmS "${session_name}" bash -c "${command}"
    fi
}

# Check for screen
if ! command -v screen > /dev/null; then
    echo "Error: 'screen' is not installed. Please install it first."
    exit 1
fi

# Load environment variables
if [ -f .env ]; then
    # Filter out comments and export
    export $(grep -v '^#' .env | xargs)
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

# Defaults
REDIS_PORT=${REDIS_PORT:-6379}
KVROCKS_PORT=${KVROCKS_PORT:-6666}
WORKERS_COUNT=${WORKERS_COUNT:-5}
ENABLE_MILVUS=${ENABLE_MILVUS:-false}
DATA_BASE_DIR=${DATA_BASE_DIR:-"$(pwd)/data"}

echo "--- Launching Services (Data: ${DATA_BASE_DIR}) ---"

# Start Redis
start_screen "bsimvis-redis" "redis-server --port ${REDIS_PORT} --dir ${DATA_BASE_DIR}/redis"

# Start Kvrocks
start_screen "bsimvis-kvrocks" "kvrocks -c kvrocks.conf --port ${KVROCKS_PORT} --dir ${DATA_BASE_DIR}/kvrocks"

# Start Milvus stack if enabled
if [ "$ENABLE_MILVUS" = "true" ]; then
    ETCD_PORT=${ETCD_PORT:-2379}
    MINIO_PORT=${MINIO_PORT:-9000}
    MINIO_CONSOLE_PORT=${MINIO_CONSOLE_PORT:-9001}
    MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
    MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}

    echo "Launching Milvus stack (Etcd, Minio, Milvus)..."
    start_screen "bsimvis-etcd" "etcd --data-dir ${DATA_BASE_DIR}/etcd --advertise-client-urls http://127.0.0.1:${ETCD_PORT} --listen-client-urls http://0.0.0.0:${ETCD_PORT}"
    
    start_screen "bsimvis-minio" "MINIO_ROOT_USER=${MINIO_ACCESS_KEY} MINIO_ROOT_PASSWORD=${MINIO_SECRET_KEY} minio server ${DATA_BASE_DIR}/minio --address \":${MINIO_PORT}\" --console-address \":${MINIO_CONSOLE_PORT}\""
    
    # Give a moment for etcd and minio to start
    sleep 2
    
    start_screen "bsimvis-milvus" "ETCD_ENDPOINTS=localhost:${ETCD_PORT} MINIO_ADDRESS=localhost:${MINIO_PORT} milvus run standalone"
fi

# Determine python command
PYTHON_CMD="python3"
if [ -d ".venv" ]; then
    PYTHON_CMD="$(pwd)/.venv/bin/python3"
elif command -v uv > /dev/null; then
    PYTHON_CMD="uv run python"
fi

# Start App
start_screen "bsimvis-app" "${PYTHON_CMD} app.py"

# Start Workers
echo "Starting ${WORKERS_COUNT} workers..."
for i in $(seq 1 $WORKERS_COUNT); do
    start_screen "bsimvis-worker-${i}" "${PYTHON_CMD} bsimvis/worker.py"
done

echo "--------------------------"
echo "All services started in screen sessions."
echo "Use 'screen -ls' to see running sessions."
echo "Use 'screen -r <name>' to attach to a session."
echo "To stop all sessions: screen -ls | grep bsimvis- | cut -d. -f1 | awk '{print \$1}' | xargs -I{} screen -X -S {} quit"
