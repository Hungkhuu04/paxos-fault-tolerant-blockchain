#!/usr/bin/env bash
set -e


ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
echo "[RUN] Killing all node processes (anything running src.node)"
PIDS=$(pgrep -f "src.node" || true)

if [ -z "$PIDS" ]; then
  echo "[RUN] No node processes found."
  exit 0
fi

echo "[RUN] Found node PIDs: $PIDS"
kill $PIDS || true

# Give them a moment to die
sleep 1

# If anything survived, murder it with -9
PIDS_STILL=$(pgrep -f "src.node" || true)
if [ -n "$PIDS_STILL" ]; then
  echo "[RUN] Forcing kill on: $PIDS_STILL"
  kill -9 $PIDS_STILL || true
fi

echo "[RUN] Done."
