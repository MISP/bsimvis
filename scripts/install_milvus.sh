#!/bin/bash
set -e

# Configuration
ETCD_VERSION="v3.5.5"
MILVUS_VERSION="v2.4.15"

# Directories
PROJECT_ROOT=$(pwd)
BIN_DIR="${PROJECT_ROOT}/bin"
SCRATCH_DIR="${PROJECT_ROOT}/scratch_build"

mkdir -p "${BIN_DIR}"
mkdir -p "${SCRATCH_DIR}"
mkdir -p data/etcd data/minio data/milvus

echo "--- Installing Milvus dependencies (Etcd, Minio) ---"

# Etcd
if [ ! -f "${BIN_DIR}/etcd" ]; then
    echo "Downloading Etcd..."
    cd "${SCRATCH_DIR}"
    curl -L "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz" -o etcd.tar.gz
    tar -xzf etcd.tar.gz
    cp "etcd-${ETCD_VERSION}-linux-amd64/etcd" "etcd-${ETCD_VERSION}-linux-amd64/etcdctl" "${BIN_DIR}/"
    cd "${PROJECT_ROOT}"
else
    echo "Etcd already installed in bin/"
fi

# Minio
if [ ! -f "${BIN_DIR}/minio" ]; then
    echo "Downloading Minio..."
    curl -L "https://dl.min.io/server/minio/release/linux-amd64/minio" -o "${BIN_DIR}/minio"
    chmod +x "${BIN_DIR}/minio"
else
    echo "Minio already installed in bin/"
fi

# Milvus
if [ ! -f "${BIN_DIR}/milvus" ]; then
    echo "Downloading Milvus ${MILVUS_VERSION}..."
    cd "${SCRATCH_DIR}"
    
    # Note: Milvus releases follow a specific naming convention. 
    # For v2.4.x, the binary is often distributed as milvus-standalone.
    # We will try the most likely URL and fallback if needed.
    # The previous attempt failed with a 9-byte file, likely a "Not Found".
    
    # Correcting the URL based on common Milvus release patterns
    MILVUS_URL="https://github.com/milvus-io/milvus/releases/download/${MILVUS_VERSION}/milvus-standalone-linux-amd64.tar.gz"
    
    echo "URL: ${MILVUS_URL}"
    if ! curl -L "${MILVUS_URL}" -o milvus.tar.gz; then
        echo "Error: Failed to download Milvus from ${MILVUS_URL}"
        exit 1
    fi
    
    if [ ! -s milvus.tar.gz ]; then
        echo "Error: Downloaded file is empty."
        exit 1
    fi

    # Check if it's actually a gzip file (avoid the 9-byte "Not Found" issue)
    if ! file milvus.tar.gz | grep -q "gzip compressed data"; then
        echo "Error: Downloaded file is not a valid gzip archive. The version ${MILVUS_VERSION} might be incorrect or the asset name has changed."
        echo "Contents of the downloaded file:"
        cat milvus.tar.gz
        exit 1
    fi

    tar -xzf milvus.tar.gz
    # Search for the milvus binary in the extracted files
    find . -name milvus -type f -exec cp {} "${BIN_DIR}/" \;
    cd "${PROJECT_ROOT}"
else
    echo "Milvus already installed in bin/"
fi

echo "--- Milvus Installation Complete ---"
