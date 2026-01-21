
set -e


ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[RUN] Starting node 1"
python3 -m src.node --id 1
