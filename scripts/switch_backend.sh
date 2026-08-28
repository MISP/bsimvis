#!/bin/bash
# Switch this deployment's LCA discovery backend (rust_cpu <-> wgpu) and
# restart the stack so every worker picks up the new config.toml value --
# config_service is a singleton loaded once at process start, so an edit to
# bsimvis_config.toml alone does nothing until the workers restart.
#
# Run from the bsimvis checkout root (where bsimvis_config.toml and
# launch_tmux.sh live):
#
#   ./scripts/switch_backend.sh rust_cpu
#   ./scripts/switch_backend.sh wgpu
#
# Restarts via `launch_tmux.sh --clear`, same as wt-teardown/wt-setup use:
# stops worker scopes, then redis/kvrocks (graceful SHUTDOWN, data persists
# to disk), then relaunches the full stack. A few seconds of downtime; no
# data loss.
set -euo pipefail

BACKEND=${1:?"usage: $0 <rust_cpu|wgpu>"}
case "$BACKEND" in
    rust_cpu|wgpu) ;;
    *) echo "unknown backend '$BACKEND' -- expected rust_cpu or wgpu"; exit 1 ;;
esac

CONFIG_FILE="bsimvis_config.toml"
[ -f "$CONFIG_FILE" ] || { echo "no $CONFIG_FILE here -- run from the bsimvis checkout root"; exit 1; }

if grep -q '^\s*discovery_backend\s*=' "$CONFIG_FILE"; then
    sed -i -E "s/^(\s*discovery_backend\s*=\s*).*/\1\"$BACKEND\"/" "$CONFIG_FILE"
else
    # No line yet -- insert right after [similarity].
    sed -i "/^\[similarity\]/a\\    discovery_backend = \"$BACKEND\"" "$CONFIG_FILE"
fi

echo "[*] $CONFIG_FILE: discovery_backend = \"$BACKEND\""
grep -n 'discovery_backend' "$CONFIG_FILE"

echo "[*] restarting stack (launch_tmux.sh --clear)..."
./launch_tmux.sh --clear
