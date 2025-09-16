#!/usr/bin/env bash
set -euo pipefail

# Usage: ./build.sh [name]
NAME=${1:-nfa95}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
SERVER_DIR="$SCRIPT_DIR"
ROOT_DIR=$(cd "$SERVER_DIR/.." && pwd)

# Ensure PyInstaller
if ! command -v pyinstaller >/dev/null 2>&1; then
  echo "[INFO] Installing PyInstaller..."
  python3 -m pip install --user pyinstaller
  export PATH="$HOME/.local/bin:$PATH"
fi

# Compose add-data arguments (Linux/mac uses src:dest)
ADD_DATA=()
STATIC_DIR="$SERVER_DIR/static"
MAPPING_FILE="$SERVER_DIR/mapping.json"
if [[ -d "$STATIC_DIR" ]]; then ADD_DATA+=("--add-data" "$STATIC_DIR:static"); fi
if [[ -f "$MAPPING_FILE" ]]; then ADD_DATA+=("--add-data" "$MAPPING_FILE:mapping.json"); fi

ENTRY="$SERVER_DIR/serve.py"
if [[ ! -f "$ENTRY" ]]; then
  echo "[ERROR] Entry not found: $ENTRY" >&2
  exit 1
fi

# Clean previous build artifacts
rm -rf "$SERVER_DIR/build" "$SERVER_DIR/__pycache__" 2>/dev/null || true

# Build
ARGS=(
  "--name" "$NAME"
  "--onefile"
  "--clean"
  "--noconfirm"
  "--paths" "$ROOT_DIR"  # ensure 'server' package import works
  ${ADD_DATA[@]:-}
  "$ENTRY"
)

echo "[INFO] Running: pyinstaller ${ARGS[*]}"
pyinstaller "${ARGS[@]}"

EXE_PATH="$ROOT_DIR/dist/$NAME"
# Copy env example into dist for distribution
ENV_EXAMPLE="$SERVER_DIR/.env.example"
if [[ -f "$ENV_EXAMPLE" ]]; then
  cp -f "$ENV_EXAMPLE" "$ROOT_DIR/dist/.env.example"
fi
if [[ -f "$EXE_PATH" ]]; then
  echo "[SUCCESS] Build succeeded: $EXE_PATH"
  echo "[TIP] Deploy by copying the binary next to a .env file. Logs and storage will auto-create alongside the binary."
else
  echo "[WARN] Build seems to have failed. Check the output above." >&2
fi
