#!/bin/bash
# Prep + launch + test a git worktree in isolation, then tear down.
#
# Run from inside a linked worktree:  ./scripts/wt-test.sh
# Idempotent: symlinks bin/ to the main repo, writes an isolated .env
# (own PROJECT_NAME + offset ports + LOCAL fresh data dir) if missing,
# launches the full stack via launch_tmux.sh, runs the API test suites,
# reports, and kills its own tmux session.
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

# --- 1. bin/ symlink -> main repo (never rebuilt; 1.4G of downloaded tools) --
if [ ! -L bin ] || [ "$(readlink -f bin)" != "$MAIN_ROOT/bin" ]; then
  echo "Linking bin/ -> $MAIN_ROOT/bin"
  rm -rf bin
  ln -s "$MAIN_ROOT/bin" bin
fi

# --- 2. isolated .env (offset ports so it never collides with main/others) --
if [ ! -f .env ]; then
  # Bands kept clear of main (.env: 5001/6380/6667) and of each other.
  # Hash only spreads names; the busy-port guard below is what guarantees safety.
  OFF=$(( ( $(cksum <<<"$WT_NAME" | cut -d' ' -f1) % 50 ) * 10 ))
  echo "Writing isolated .env (offset $OFF)"
  cat > .env <<EOF
GHIDRA_INSTALL_DIR=$WT_ROOT/bin/ghidra_12.1_PUBLIC
APP_HOST=0.0.0.0
APP_PORT=$((5100 + OFF))
REDIS_HOST=localhost
REDIS_PORT=$((6900 + OFF))
KVROCKS_HOST=localhost
KVROCKS_PORT=$((7400 + OFF))
WORKERS_COUNT=5
PROJECT_NAME=bsimvis-$WT_NAME
DATA_BASE_DIR=$WT_ROOT/data
EOF
fi
# shellcheck disable=SC1091
set -a; . ./.env; set +a
APP_PORT=${APP_PORT:-5100}

# --- 3. launch full stack (launch_tmux.sh reads the .env above) -------------
# Clean up any existing session/services for this worktree first.
if tmux has-session -t "$PROJECT_NAME" 2>/dev/null; then
  echo "Session $PROJECT_NAME already exists. Cleaning up..."
  redis-cli -p "$REDIS_PORT"   shutdown nosave 2>/dev/null || true
  redis-cli -p "$KVROCKS_PORT" shutdown 2>/dev/null || true  # kvrocks SHUTDOWN takes no args
  tmux kill-session -t "$PROJECT_NAME" 2>/dev/null || true
  sleep 1
fi

# Fail loud if any of our ports is already held (another worktree/main stack).
# Sharing a kvrocks data dir across two live processes corrupts it.
for p in "$APP_PORT" "$REDIS_PORT" "$KVROCKS_PORT"; do
  if (echo > /dev/tcp/localhost/"$p") 2>/dev/null; then
    echo "Port $p already in use. Another stack is running on this worktree's"
    echo "ports. Tear it down first (tmux kill-session -t $PROJECT_NAME)."
    exit 1
  fi
done
echo "=== launching stack (session $PROJECT_NAME, app :$APP_PORT) ==="
./launch_tmux.sh --clear || { echo "launch failed"; exit 1; }

# app.py needs a moment after datastores; poll its port (max 40s)
echo -n "  waiting for app on :$APP_PORT..."
for i in $(seq 1 40); do
  (echo > /dev/tcp/localhost/"$APP_PORT") 2>/dev/null && break
  sleep 1
  [ "$i" -eq 40 ] && { echo " TIMEOUT"; }
done
echo " up."

# --- 4. run the test suite -------------------------------------------------
# test_pools.py was absorbed into test_api_endpoints.py (step 3d).
export API_URL="http://localhost:$APP_PORT"   # test_api_endpoints reads this
rc=0
echo "=== test_api_endpoints.py ==="; uv run python test_api_endpoints.py "$@" || rc=1

# --- 5. teardown this worktree's session (leaves main stack untouched) ------
echo "=== teardown $PROJECT_NAME ==="
redis-cli -p "$REDIS_PORT"   shutdown nosave 2>/dev/null || true
redis-cli -p "$KVROCKS_PORT" shutdown 2>/dev/null || true  # kvrocks SHUTDOWN takes no args
tmux kill-session -t "$PROJECT_NAME" 2>/dev/null || true

[ $rc -eq 0 ] && echo "RESULT: PASS" || echo "RESULT: FAIL"
exit $rc
