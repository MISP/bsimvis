#!/bin/bash
set -e

# Configuration
REDIS_VERSION="7.2.4"

# Directories
PROJECT_ROOT=$(pwd)
BIN_DIR="${PROJECT_ROOT}/bin"
SCRATCH_DIR="${PROJECT_ROOT}/scratch_build"

# Check for build dependencies
MISSING_DEPS=()
# Critical dependencies
for cmd in gcc g++ make cmake git unzip curl; do
    if ! command -v $cmd > /dev/null; then
        MISSING_DEPS+=($cmd)
    fi
done

if [ ${#MISSING_DEPS[@]} -ne 0 ]; then
    echo "Warning: Missing critical build dependencies: ${MISSING_DEPS[*]}"
    echo "Attempting to continue, but the build might fail."
    echo "If you are root, you can install them with:"
    echo "sudo apt update && sudo apt install -y build-essential cmake git unzip curl"
fi

# Optional/Helper dependencies (often present but maybe named differently or not strictly needed for all steps)
for cmd in autoconf automake libtoolize; do
    if ! command -v $cmd > /dev/null; then
        echo "Note: Optional dependency '$cmd' not found. This might be needed for some source builds."
    fi
done

# Load .env early to get DATA_BASE_DIR
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Set default data directory if not provided
DATA_BASE_DIR=${DATA_BASE_DIR:-"${PROJECT_ROOT}/data"}

mkdir -p "${BIN_DIR}"
mkdir -p "${SCRATCH_DIR}"
mkdir -p "${DATA_BASE_DIR}/etcd" "${DATA_BASE_DIR}/minio" "${DATA_BASE_DIR}/milvus" "${DATA_BASE_DIR}/kvrocks" "${DATA_BASE_DIR}/redis"

echo "--- Setting up Python environment ---"
if command -v uv > /dev/null; then
    echo "Using uv for dependency management..."
    uv sync
else
    echo "Using pip for dependency management..."
    if [ ! -d ".venv" ]; then
        python3 -m venv .venv
    fi
    source .venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
fi

echo "--- Installing Redis ---"
if [ ! -f "${BIN_DIR}/redis-server" ]; then
    echo "Building Redis from source..."
    cd "${SCRATCH_DIR}"
    curl -L "https://download.redis.io/releases/redis-${REDIS_VERSION}.tar.gz" -o redis.tar.gz
    tar -xzf redis.tar.gz
    cd "redis-${REDIS_VERSION}"
    make -j$(nproc 2>/dev/null || echo 4)
    cp src/redis-server src/redis-cli "${BIN_DIR}/"
    cd "${PROJECT_ROOT}"
else
    echo "Redis already installed in bin/"
fi

echo "--- Installing Kvrocks ---"
if [ ! -f "${BIN_DIR}/kvrocks" ]; then
    echo "Building Kvrocks from source..."
    cd "${SCRATCH_DIR}"
    if [ ! -d "kvrocks" ]; then
        git clone https://github.com/apache/kvrocks.git
    fi
    cd kvrocks
    # Use x.py to build
    ./x.py build -j$(nproc 2>/dev/null || echo 4)
    cp build/kvrocks "${BIN_DIR}/"
    cd "${PROJECT_ROOT}"
else
    echo "Kvrocks already installed in bin/"
fi

echo "--- Installing UPX ---"
# Used to unpack UPX-packed samples before analysis (see unpack_service.py).
# Optional: without it packed samples are still analyzed, just packed.
UPX_VERSION="5.2.0"
if [ ! -f "${BIN_DIR}/upx" ]; then
    case "$(uname -m)" in
        x86_64)  UPX_ARCH="amd64" ;;
        aarch64) UPX_ARCH="arm64" ;;
        armv7l)  UPX_ARCH="arm" ;;
        i686)    UPX_ARCH="i386" ;;
        *)       UPX_ARCH="" ;;
    esac
    UPX_DIR="upx-${UPX_VERSION}-${UPX_ARCH}_linux"
    if [ -z "$UPX_ARCH" ]; then
        echo "Warning: no UPX build for $(uname -m); packed samples stay packed."
    elif ! (
        cd "${SCRATCH_DIR}" \
        && curl -fsSL "https://github.com/upx/upx/releases/download/v${UPX_VERSION}/${UPX_DIR}.tar.xz" -o upx.tar.xz \
        && tar -xJf upx.tar.xz \
        && cp "${UPX_DIR}/upx" "${BIN_DIR}/upx" \
        && chmod +x "${BIN_DIR}/upx"
    ); then
        echo "Warning: UPX download failed; packed samples stay packed."
    fi
else
    echo "UPX already installed in bin/"
fi

echo "--- Installing Ghidra ---"
./scripts/install_ghidra.sh

# Load .env to check for Milvus
if [ -f .env ]; then
    # Simple export for the script
    export $(grep -v '^#' .env | xargs)
fi

if [ "$ENABLE_MILVUS" = "true" ]; then
    ./scripts/install_milvus.sh
fi

echo "--- Installation Complete ---"
echo "Binaries are located in ${BIN_DIR}"
echo "You can now run ./launch.sh to start the services."
