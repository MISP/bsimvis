#!/bin/bash
# Prep + launch a git worktree's isolated stack. Leaves it running.
#
# Run from inside a linked worktree:  ./scripts/wt-setup.sh
# Idempotent: symlinks bin/ to the main repo, writes an isolated .env
# (own PROJECT_NAME + offset ports + LOCAL fresh data dir) if missing,
# launches the full stack via launch_tmux.sh and waits for the app port.
#
# Tear down with ./scripts/wt-teardown.sh. Used by wt-test.sh.
#
# Confidentiality: the main repo's 17G data/kvrocks is NEVER symlinked or
# read. Each worktree gets its own empty data dir; tests build their own
# collections from the git-tracked data/test/ binaries.
set -uo pipefail

# --- locate worktree + main repo -------------------------------------------
WT_ROOT="$(git rev-parse --show-toplevel)"
GIT_DIR="$(git rev-parse --git-dir)"
case "$GIT_DIR" in
  *"/worktrees/"*) ;;  # linked worktree, good
  *) echo "Refusing: not a linked worktree (protects main repo .env/ports)."; exit 1 ;;
esac
MAIN_ROOT="$(dirname "$(git rev-parse --git-common-dir)")"
WT_NAME="$(basename "$WT_ROOT")"
cd "$WT_ROOT" || exit 1

# A non-default ENV_FILE (wt-test.sh sets WT_ENV_FILE=.env.wttest) gets its
# own session name/ports/data dir, so an ephemeral test run can never
# collide with -- or tear down -- a persistent stack launched by hand
# against the plain .env in this same worktree.
ENV_FILE="${WT_ENV_FILE:-.env}"
SESSION_TAG="${ENV_FILE#.env}"; SESSION_TAG="${SESSION_TAG#.}"
WT_ID="$WT_NAME${SESSION_TAG:+-$SESSION_TAG}"

# --- 1. bin/ symlink -> main repo (never rebuilt; 1.4G of downloaded tools) --
if [ ! -L bin ] || [ "$(readlink -f bin)" != "$MAIN_ROOT/bin" ]; then
  echo "Linking bin/ -> $MAIN_ROOT/bin"
  rm -rf bin
  ln -s "$MAIN_ROOT/bin" bin
fi

# --- 1b. config from the example (the upload CLI hard-fails without it) -----
if [ ! -f bsimvis_config.toml ] && [ -f bsimvis_config.toml.example ]; then
  echo "Seeding bsimvis_config.toml from the example"
  cp bsimvis_config.toml.example bsimvis_config.toml
fi

# --- 2. isolated env file (offset ports so it never collides with main/others) --
if [ ! -f "$ENV_FILE" ]; then
  # Bands kept clear of main (.env: 5001/6380/6667) and of each other.
  # Hash only spreads names; the busy-port guard below is what guarantees safety.
  OFF=$(( ( $(cksum <<<"$WT_ID" | cut -d' ' -f1) % 50 ) * 10 ))
  echo "Writing isolated $ENV_FILE (offset $OFF)"
  cat > "$ENV_FILE" <<EOF
GHIDRA_INSTALL_DIR=$WT_ROOT/bin/ghidra_12.1_PUBLIC
APP_HOST=0.0.0.0
APP_PORT=$((5100 + OFF))
REDIS_HOST=localhost
REDIS_PORT=$((6900 + OFF))
KVROCKS_HOST=localhost
KVROCKS_PORT=$((7400 + OFF))
WORKERS_COUNT=5
PROJECT_NAME=bsimvis-$WT_ID
DATA_BASE_DIR=$WT_ROOT/data${SESSION_TAG:+-$SESSION_TAG}
EOF
fi
# shellcheck disable=SC1091
set -a; . "./$ENV_FILE"; set +a
PROJECT_NAME="${PROJECT_NAME//./_}"
APP_PORT=${APP_PORT:-5100}

# --- 3. launch full stack (launch_tmux.sh reads the env file above) ---------
# Clean up any existing session/services for this worktree+ENV_FILE first.
if tmux has-session -t "$PROJECT_NAME" 2>/dev/null; then
  echo "Session $PROJECT_NAME already exists. Cleaning up..."
  WT_ENV_FILE="$ENV_FILE" "$WT_ROOT/scripts/wt-teardown.sh" >/dev/null
  sleep 1
fi

# Fail loud if any of our ports is already held (another worktree/main stack).
# Sharing a kvrocks data dir across two live processes corrupts it.
for p in "$APP_PORT" "$REDIS_PORT" "$KVROCKS_PORT"; do
  if (echo > /dev/tcp/localhost/"$p") 2>/dev/null; then
    echo "Port $p already in use. Another stack is running on this worktree's"
    echo "ports. Tear it down first (./scripts/wt-teardown.sh)."
    exit 1
  fi
done
echo "=== launching stack (session $PROJECT_NAME, app :$APP_PORT) ==="
ENV_FILE="$ENV_FILE" ./launch_tmux.sh --clear || { echo "launch failed"; exit 1; }

# app.py needs a moment after datastores; poll its port (max 40s)
echo -n "  waiting for app on :$APP_PORT..."
for i in $(seq 1 40); do
  (echo > /dev/tcp/localhost/"$APP_PORT") 2>/dev/null && break
  sleep 1
  [ "$i" -eq 40 ] && { echo " TIMEOUT"; }
done
echo " up."
