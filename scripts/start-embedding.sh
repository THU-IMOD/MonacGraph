#!/usr/bin/env bash
# Creates embedding-service/.venv if needed and serves http://127.0.0.1:8099
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=env.sh
source "$ROOT/scripts/env.sh"

SERVICE="$ROOT/embedding-service"
VENV="$SERVICE/.venv"
PORT="${EMBEDDING_PORT:-8099}"

host_python() {
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
  elif command -v python >/dev/null 2>&1; then
    command -v python
  else
    echo "Python 3.10+ is required. Install Python and retry." >&2
    exit 1
  fi
}

if [[ -x "$VENV/bin/python" ]]; then
  VENV_PYTHON="$VENV/bin/python"
elif [[ -x "$VENV/Scripts/python.exe" ]]; then
  VENV_PYTHON="$VENV/Scripts/python.exe"
else
  echo "Creating embedding virtualenv at $VENV"
  "$(host_python)" -m venv "$VENV"
  if [[ -x "$VENV/bin/python" ]]; then
    VENV_PYTHON="$VENV/bin/python"
  else
    VENV_PYTHON="$VENV/Scripts/python.exe"
  fi
fi

if ! "$VENV_PYTHON" -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('fastapi') and importlib.util.find_spec('sentence_transformers') and importlib.util.find_spec('torch') else 1)"; then
  echo "Installing CPU PyTorch and embedding-service requirements into $VENV"
  "$VENV_PYTHON" -m pip install --upgrade pip
  "$VENV_PYTHON" -m pip install --cache-dir "$PIP_CACHE_DIR" torch --index-url https://download.pytorch.org/whl/cpu
  "$VENV_PYTHON" -m pip install --cache-dir "$PIP_CACHE_DIR" -r "$SERVICE/requirements.txt"
fi

echo "Embedding caches: HF_HOME=$HF_HOME"
echo "Starting embedding service on http://127.0.0.1:$PORT (first run downloads the model)"
cd "$SERVICE"
exec "$VENV_PYTHON" -m uvicorn app:app --host 127.0.0.1 --port "$PORT"
