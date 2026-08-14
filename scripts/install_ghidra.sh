#!/bin/bash
set -e

# Configuration
GHIDRA_VERSION="12.1"
GHIDRA_DATE="20260513"
GHIDRA_URL="https://github.com/NationalSecurityAgency/ghidra/releases/download/Ghidra_${GHIDRA_VERSION}_build/ghidra_${GHIDRA_VERSION}_PUBLIC_${GHIDRA_DATE}.zip"

# Directories
PROJECT_ROOT=$(pwd)
BIN_DIR="${PROJECT_ROOT}/bin"
SCRATCH_DIR="${PROJECT_ROOT}/scratch_build"

mkdir -p "${BIN_DIR}"
mkdir -p "${SCRATCH_DIR}"

echo "--- Installing Ghidra ${GHIDRA_VERSION} ---"

# Check for Java 21+ (Ghidra 12 requires it); install a portable Temurin JDK if missing.
java_major() {
    local out
    out=$("$1" -version 2>&1 | head -n 1) || return 1
    [[ $out =~ \"([0-9]+) ]] && echo "${BASH_REMATCH[1]}"
}

JDK_DIR=$(ls -d "${BIN_DIR}"/jdk-21* 2>/dev/null | head -n 1)
if [ -n "$JDK_DIR" ] && [ -x "$JDK_DIR/bin/java" ]; then
    JAVA_HOME_PATH="$JDK_DIR"
    echo "Using portable JDK in ${JAVA_HOME_PATH}"
elif command -v java >/dev/null && [ "$(java_major java || echo 0)" -ge 21 ] 2>/dev/null; then
    echo "Found system Java $(java_major java)."
else
    # ponytail: Adoptium "latest 21 ga" redirect, no version pinning. Pin if reproducibility matters.
    case "$(uname -m)" in
        x86_64) JDK_ARCH="x64" ;;
        aarch64|arm64) JDK_ARCH="aarch64" ;;
        *) JDK_ARCH="" ;;
    esac
    if [ -z "$JDK_ARCH" ]; then
        echo "Warning: no prebuilt JDK for $(uname -m). Install OpenJDK 21+ manually."
    else
        echo "Java 21+ not found. Installing portable Temurin JDK 21 into ${BIN_DIR}..."
        JDK_URL="https://api.adoptium.net/v3/binary/latest/21/ga/linux/${JDK_ARCH}/jdk/hotspot/normal/eclipse"
        if (
            cd "${SCRATCH_DIR}" \
            && curl -fsSL "${JDK_URL}" -o jdk21.tar.gz \
            && tar xzf jdk21.tar.gz -C "${BIN_DIR}"
        ); then
            JAVA_HOME_PATH=$(ls -d "${BIN_DIR}"/jdk-21* | head -n 1)
            echo "Installed JDK to ${JAVA_HOME_PATH}"
        else
            echo "Warning: JDK download failed. Ghidra needs OpenJDK 21+; install it manually."
        fi
    fi
fi

# Check for unzip
if ! command -v unzip >/dev/null; then
    echo "Error: unzip is not installed. Please install it."
    echo "On Ubuntu/Debian: sudo apt update && sudo apt install -y unzip"
    exit 1
fi

GHIDRA_DIR_NAME="ghidra_${GHIDRA_VERSION}_PUBLIC"
GHIDRA_PATH="${BIN_DIR}/${GHIDRA_DIR_NAME}"

if [ ! -d "${GHIDRA_PATH}" ]; then
    echo "Downloading Ghidra..."
    cd "${SCRATCH_DIR}"
    if [ ! -f "ghidra.zip" ]; then
        if ! curl -L "${GHIDRA_URL}" -o ghidra.zip; then
            echo "Error: Failed to download Ghidra from ${GHIDRA_URL}"
            exit 1
        fi
    fi
    
    echo "Extracting Ghidra..."
    if ! unzip -q ghidra.zip; then
        echo "Error: Failed to unzip Ghidra"
        rm -f ghidra.zip
        exit 1
    fi
    
    # Check if the extracted directory exists
    if [ ! -d "${GHIDRA_DIR_NAME}" ]; then
        # Some versions might have a different naming convention in the zip
        ACTUAL_DIR=$(ls -d ghidra_*_PUBLIC 2>/dev/null | head -n 1)
        if [ -n "$ACTUAL_DIR" ]; then
            GHIDRA_DIR_NAME="$ACTUAL_DIR"
        else
            echo "Error: Could not find extracted Ghidra directory"
            exit 1
        fi
    fi

    mv "${GHIDRA_DIR_NAME}" "${BIN_DIR}/"
    GHIDRA_PATH="${BIN_DIR}/${GHIDRA_DIR_NAME}"
    cd "${PROJECT_ROOT}"
else
    echo "Ghidra already installed in ${GHIDRA_PATH}"
fi

# Update .env if it exists
if [ -f .env ]; then
    if grep -q "GHIDRA_INSTALL_DIR" .env; then
        # Use a different delimiter for sed since path contains slashes
        sed -i "s|GHIDRA_INSTALL_DIR=.*|GHIDRA_INSTALL_DIR=${GHIDRA_PATH}|" .env
    else
        echo "" >> .env
        echo "# Ghidra Configuration" >> .env
        echo "GHIDRA_INSTALL_DIR=${GHIDRA_PATH}" >> .env
    fi
    echo "Updated .env with GHIDRA_INSTALL_DIR"
else
    echo "GHIDRA_INSTALL_DIR=${GHIDRA_PATH}" > .env
    echo "Created .env with GHIDRA_INSTALL_DIR"
fi

if [ -n "${JAVA_HOME_PATH:-}" ]; then
    if grep -q "^JAVA_HOME=" .env; then
        sed -i "s|^JAVA_HOME=.*|JAVA_HOME=${JAVA_HOME_PATH}|" .env
    else
        echo "JAVA_HOME=${JAVA_HOME_PATH}" >> .env
    fi
    echo "Updated .env with JAVA_HOME"
fi

echo "--- Ghidra Installation Complete ---"
echo "Ghidra path: ${GHIDRA_PATH}"
