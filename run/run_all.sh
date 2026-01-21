#!/usr/bin/env bash
set -e


ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[RUN] Starting all 5 nodes in background"
./run/run_p1.sh &
PID1=$!
./run/run_p2.sh &
PID2=$!
./run/run_p3.sh &
PID3=$!
./run/run_p4.sh &
PID4=$!
./run/run_p5.sh &
PID5=$!

echo "[RUN] Nodes PIDs: $PID1 $PID2 $PID3 $PID4 $PID5"
echo "[RUN] Use ./run/kill_all.sh to stop them."

wait
