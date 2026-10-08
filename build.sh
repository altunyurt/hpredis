#!/usr/bin/env bash
# Build the hpredis extension from the tracked Mojo source.
#
#   ./build.sh                  uses .venv/bin/mojo, then $PATH
#   MOJO=/path/to/mojo ./build.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

if [ -z "${MOJO:-}" ]; then
    for candidate in "$ROOT/.venv/bin/mojo" "$(command -v mojo 2>/dev/null || true)"; do
        if [ -n "$candidate" ] && [ -x "$candidate" ]; then
            MOJO="$candidate"
            break
        fi
    done
fi
if [ -z "${MOJO:-}" ]; then
    echo "mojo not found: set MOJO=/path/to/mojo (pip install mojo)" >&2
    exit 1
fi

# the Mojo Python bindings set PYTHONPATH/PYTHONEXECUTABLE at the C level;
# children must not inherit them
env -u PYTHONPATH -u PYTHONEXECUTABLE -u PYTHONHOME \
    "$MOJO" build "$ROOT/src/hpredis_core.mojo" \
    --emit shared-lib -o "$ROOT/src/hpredis/hpredis_core.so"

echo "built $ROOT/src/hpredis/hpredis_core.so"
