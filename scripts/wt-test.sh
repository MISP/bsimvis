#!/bin/bash
# Prep + launch + test a git worktree in isolation, then tear down.
#
# Run from inside a linked worktree:  ./scripts/wt-test.sh
# To run specific tests:              ./scripts/wt-test.sh --only <substring>
# For UI-only changes:                Do not run this script. Use `node --check <file>` instead.
#
# Thin wrapper: wt-setup.sh brings the isolated stack up, the API suite
# runs against it, wt-teardown.sh kills it again.
set -uo pipefail

WT_ROOT="$(git rev-parse --show-toplevel)"
cd "$WT_ROOT" || exit 1

# Own env file/session/ports/data dir, distinct from a persistent stack a
# developer launched by hand against the plain .env -- so this throwaway
# test run can never collide with, or tear down, that stack.
export WT_ENV_FILE=.env.wttest

"$WT_ROOT/scripts/wt-setup.sh" || exit 1

# shellcheck disable=SC1091
set -a; . "./$WT_ENV_FILE"; set +a
APP_PORT=${APP_PORT:-5100}

# --- run the test suite ----------------------------------------------------
# test_pools.py was absorbed into test_api_endpoints.py (step 3d).
export API_URL="http://localhost:$APP_PORT"   # test_api_endpoints reads this
rc=0

# Pure-function checks: stdlib + cluster_utils only, no redis, no fixtures.
# They live here because the API fixture never forms clusters, so the suite
# below cannot reach the cluster-metadata summary at all.
echo "=== cluster_utils unit checks ==="
uv run python scripts/test_default_bin_cluster_name.py || rc=1
uv run python scripts/test_cluster_meta_freq.py || rc=1
uv run python scripts/test_bsim_weights.py || rc=1

echo "=== test_api_endpoints.py ==="; uv run python scripts/test_api_endpoints.py "$@" || rc=1

"$WT_ROOT/scripts/wt-teardown.sh"

[ $rc -eq 0 ] && echo "RESULT: PASS" || echo "RESULT: FAIL"
exit $rc
