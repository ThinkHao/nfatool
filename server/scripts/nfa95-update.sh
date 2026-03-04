#!/usr/bin/env bash
set -euo pipefail

# External updater for nfa95 (Linux).
# Features:
# - lock to avoid concurrent updates
# - staged download into releases/
# - atomic symlink switch for target executable
# - restart + health check
# - automatic rollback to previous binary when health check fails

ASSET_URL=""
VERSION=""
TARGET=""
SERVICE="nfa95.service"
HEALTH_URL="http://127.0.0.1:8000/api/health"
HEALTH_TIMEOUT=45
CA_BUNDLE=""
INSECURE=0
NO_RESTART=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --asset-url) ASSET_URL="${2:-}"; shift 2 ;;
    --version) VERSION="${2:-}"; shift 2 ;;
    --target) TARGET="${2:-}"; shift 2 ;;
    --service) SERVICE="${2:-}"; shift 2 ;;
    --health-url) HEALTH_URL="${2:-}"; shift 2 ;;
    --health-timeout) HEALTH_TIMEOUT="${2:-45}"; shift 2 ;;
    --ca-bundle) CA_BUNDLE="${2:-}"; shift 2 ;;
    --insecure) INSECURE=1; shift ;;
    --no-restart) NO_RESTART=1; shift ;;
    *) echo "[ERR] unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$ASSET_URL" || -z "$VERSION" || -z "$TARGET" ]]; then
  echo "[ERR] required args: --asset-url --version --target" >&2
  exit 2
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "[ERR] curl is required" >&2
  exit 2
fi
if ! command -v systemctl >/dev/null 2>&1; then
  echo "[ERR] systemctl is required" >&2
  exit 2
fi

TARGET_DIR="$(cd "$(dirname "$TARGET")" && pwd)"
TARGET_NAME="$(basename "$TARGET")"
RELEASES_DIR="$TARGET_DIR/releases"
DOWNLOAD_DIR="$RELEASES_DIR/.downloads"
LOCK_FILE="$TARGET_DIR/.update.lock"

mkdir -p "$RELEASES_DIR" "$DOWNLOAD_DIR"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[ERR] another update is in progress" >&2
  exit 3
fi

TS="$(date +%Y%m%d%H%M%S)"
SANITIZED_VER="${VERSION//\//_}"
NEW_DIR="$RELEASES_DIR/${SANITIZED_VER}-${TS}"
NEW_BIN="$NEW_DIR/$TARGET_NAME"
mkdir -p "$NEW_DIR"

TMP_DL="$DOWNLOAD_DIR/${TARGET_NAME}.${SANITIZED_VER}.${TS}.download"

CURL_ARGS=(-fL --connect-timeout 10 --max-time 300 "$ASSET_URL" -o "$TMP_DL")
if [[ "$INSECURE" == "1" ]]; then
  CURL_ARGS=(-k "${CURL_ARGS[@]}")
fi
if [[ -n "$CA_BUNDLE" ]]; then
  CURL_ARGS=(--cacert "$CA_BUNDLE" "${CURL_ARGS[@]}")
fi

echo "[INFO] downloading $ASSET_URL"
curl "${CURL_ARGS[@]}"
chmod 755 "$TMP_DL"
mv -f "$TMP_DL" "$NEW_BIN"

OLD_RESOLVED=""
if [[ -e "$TARGET" ]]; then
  if [[ -L "$TARGET" ]]; then
    OLD_RESOLVED="$(readlink -f "$TARGET" || true)"
  else
    LEGACY_DIR="$RELEASES_DIR/legacy-${TS}"
    mkdir -p "$LEGACY_DIR"
    cp -a "$TARGET" "$LEGACY_DIR/$TARGET_NAME"
    chmod 755 "$LEGACY_DIR/$TARGET_NAME" || true
    OLD_RESOLVED="$LEGACY_DIR/$TARGET_NAME"
  fi
fi
if [[ -z "$OLD_RESOLVED" || ! -f "$OLD_RESOLVED" ]]; then
  echo "[ERR] previous target binary not found for rollback: $OLD_RESOLVED" >&2
  exit 4
fi

TMP_LINK="$TARGET.tmp-link"
ln -sfn "$NEW_BIN" "$TMP_LINK"
mv -Tf "$TMP_LINK" "$TARGET"
echo "[INFO] switched target -> $NEW_BIN"

if [[ "$NO_RESTART" == "1" ]]; then
  echo "[INFO] no-restart mode enabled; finish after switch"
  exit 0
fi

echo "[INFO] restarting service $SERVICE"
if ! systemctl restart "$SERVICE"; then
  echo "[ERR] restart failed, rolling back"
  ln -sfn "$OLD_RESOLVED" "$TMP_LINK"
  mv -Tf "$TMP_LINK" "$TARGET"
  systemctl restart "$SERVICE" || true
  exit 5
fi

DEADLINE=$(( $(date +%s) + HEALTH_TIMEOUT ))
OK=0
while [[ $(date +%s) -le "$DEADLINE" ]]; do
  if curl -fsS --max-time 3 "$HEALTH_URL" >/dev/null 2>&1; then
    OK=1
    break
  fi
  sleep 1
done

if [[ "$OK" == "1" ]]; then
  echo "[INFO] health check passed: $HEALTH_URL"
  exit 0
fi

echo "[ERR] health check failed, rolling back to $OLD_RESOLVED"
ln -sfn "$OLD_RESOLVED" "$TMP_LINK"
mv -Tf "$TMP_LINK" "$TARGET"
if ! systemctl restart "$SERVICE"; then
  echo "[ERR] rollback restart failed; manual intervention required" >&2
  exit 6
fi
echo "[INFO] rollback completed"
exit 7
