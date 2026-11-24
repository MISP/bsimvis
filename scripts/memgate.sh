#!/bin/bash
# Refuse to start a heavy job (browser, stack, bulk fetch) when the host is short on memory.
# Usage: scripts/memgate.sh [min_available_gb]   (default 10). Exit 1 = do not start.
need=${1:-10}
have=$(awk '/MemAvailable/ {printf "%d", $2/1048576}' /proc/meminfo)
if [ "$have" -lt "$need" ]; then
    echo "memgate: only ${have} GB available, need ${need} GB. Not starting." >&2
    exit 1
fi
echo "memgate: ${have} GB available (need ${need})"
