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

# Check for Java
if command -v java >/dev/null; then
    # Improved Java version check
    JAVA_VERSION_STR=$(java -version 2>&1 | head -n 1)
    if [[ $JAVA_VERSION_STR =~ \"([0-9]+) ]]; then
        JAVA_VERSION="${BASH_REMATCH[1]}"
        if [ "$JAVA_VERSION" -lt 21 ]; then
            echo "Warning: Ghidra 12+ requires Java 21 or higher. Found version $JAVA_VERSION."
            echo "You might need to provide a newer JDK for Ghidra to run correctly."
        else
            echo "Found Java version $JAVA_VERSION."
        fi
    else
        echo "Warning: Could not determine Java version. Please ensure Java 21+ is installed for Ghidra."
    fi
else
    echo "Warning: Java is not installed. Ghidra requires OpenJDK 21 or higher to run."
    echo "You can download a portable JDK and add it to your PATH later."
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

echo "--- Ghidra Installation Complete ---"
echo "Ghidra path: ${GHIDRA_PATH}"
