#!/bin/bash
# Tear down a git worktree's isolated stack (leaves the main stack untouched).
#
# Run from inside a linked worktree:  ./scripts/wt-teardown.sh
# Reads the worktree's own .env for its ports/session name.
set -uo pipefail

WT_ROOT="$(git rev-parse --show-toplevel)"
GIT_DIR="$(git rev-parse --git-dir)"
case "$GIT_DIR" in
  *"/worktrees/"*) ;;  # linked worktree, good
  *) echo "Refusing: not a linked worktree (would kill the main stack)."; exit 1 ;;
esac
cd "$WT_ROOT" || exit 1

[ -f .env ] || { echo "No .env here; nothing to tear down."; exit 0; }
# shellcheck disable=SC1091
set -a; . ./.env; set +a
PROJECT_NAME="${PROJECT_NAME//./_}"

echo "=== teardown $PROJECT_NAME ==="
# Workers live in systemd scopes, which are NOT children of the tmux shell and
# survive kill-session. Left behind they keep draining this worktree's queue.
# One systemctl call for all of them: `stop` blocks until the unit is gone, so
# a loop costs the sum of every worker's shutdown instead of the slowest one.
# Stopped before the datastores so they exit on their own SIGTERM rather than
# spinning on connection errors first.
UNITS=$(systemctl --user list-units --plain --no-legend "bsimvis-${PROJECT_NAME}-worker-*.scope" 2>/dev/null | awk '{print $1}')
if [ -n "$UNITS" ]; then
  echo "  stopping $(echo $UNITS | tr '\n' ' ')"
  systemctl --user stop $UNITS 2>/dev/null || true
fi
redis-cli -p "$REDIS_PORT"   shutdown nosave 2>/dev/null || true
redis-cli -p "$KVROCKS_PORT" shutdown 2>/dev/null || true  # kvrocks SHUTDOWN takes no args
tmux kill-session -t "$PROJECT_NAME" 2>/dev/null || true
echo "done."
