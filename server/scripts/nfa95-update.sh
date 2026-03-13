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
STATE_FILE=""
DOWNLOAD_MAX_TIME=1800
DOWNLOAD_RETRY=8
DOWNLOAD_RETRY_DELAY=3
DOWNLOAD_LOW_SPEED_TIME=30
DOWNLOAD_LOW_SPEED_LIMIT=10240
STARTED_AT=""
FINAL_STATE=""

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
    --state-file) STATE_FILE="${2:-}"; shift 2 ;;
    --download-max-time) DOWNLOAD_MAX_TIME="${2:-1800}"; shift 2 ;;
    --download-retry) DOWNLOAD_RETRY="${2:-8}"; shift 2 ;;
    --download-retry-delay) DOWNLOAD_RETRY_DELAY="${2:-3}"; shift 2 ;;
    --download-low-speed-time) DOWNLOAD_LOW_SPEED_TIME="${2:-30}"; shift 2 ;;
    --download-low-speed-limit) DOWNLOAD_LOW_SPEED_LIMIT="${2:-10240}"; shift 2 ;;
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

now_iso() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

write_state() {
  local st="${1:-running}"
  local step="${2:-unknown}"
  local msg="${3:-}"
  [[ -z "$STATE_FILE" ]] && return 0
  local safe_msg="${msg//$'\n'/ }"
  safe_msg="${safe_msg//=/:}"
  mkdir -p "$(dirname "$STATE_FILE")"
  {
    echo "status=$st"
    if [[ "$st" == "queued" || "$st" == "running" ]]; then
      echo "running=1"
    else
      echo "running=0"
    fi
    echo "step=$step"
    echo "version=$VERSION"
    echo "target=$TARGET"
    echo "updated_at=$(now_iso)"
    [[ -n "$STARTED_AT" ]] && echo "started_at=$STARTED_AT"
    if [[ "$st" == "succeeded" || "$st" == "failed" ]]; then
      echo "finished_at=$(now_iso)"
    fi
    [[ -n "$safe_msg" ]] && echo "message=$safe_msg"
    if [[ "$st" == "failed" && -n "$safe_msg" ]]; then
      echo "last_error=$safe_msg"
    fi
  } > "${STATE_FILE}.tmp"
  mv -f "${STATE_FILE}.tmp" "$STATE_FILE"
}

on_err() {
  local code="$1"
  local cmd="$2"
  if [[ "$FINAL_STATE" != "succeeded" ]]; then
    FINAL_STATE="failed"
    write_state "failed" "error" "exit=${code}, cmd=${cmd}"
  fi
}

trap 'on_err "$?" "$BASH_COMMAND"' ERR

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
STARTED_AT="$(now_iso)"
write_state "running" "prepare" "updater started"

TMP_DL="$DOWNLOAD_DIR/${TARGET_NAME}.${SANITIZED_VER}.${TS}.download"

CURL_ARGS=(
  -fL
  --connect-timeout 10
  --max-time "$DOWNLOAD_MAX_TIME"
  --retry "$DOWNLOAD_RETRY"
  --retry-delay "$DOWNLOAD_RETRY_DELAY"
  --continue-at -
  --speed-time "$DOWNLOAD_LOW_SPEED_TIME"
  --speed-limit "$DOWNLOAD_LOW_SPEED_LIMIT"
  "$ASSET_URL"
  -o "$TMP_DL"
)
if curl --help all 2>/dev/null | grep -q -- '--retry-all-errors'; then
  CURL_ARGS=(--retry-all-errors "${CURL_ARGS[@]}")
fi
if [[ "$INSECURE" == "1" ]]; then
  CURL_ARGS=(-k "${CURL_ARGS[@]}")
fi
if [[ -n "$CA_BUNDLE" ]]; then
  CURL_ARGS=(--cacert "$CA_BUNDLE" "${CURL_ARGS[@]}")
fi

echo "[INFO] downloading $ASSET_URL"
write_state "running" "download" "downloading release binary"
curl "${CURL_ARGS[@]}"
chmod 755 "$TMP_DL"
mv -f "$TMP_DL" "$NEW_BIN"
write_state "running" "switch" "download completed; switching binary"

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
  FINAL_STATE="failed"
  write_state "failed" "precheck" "previous target binary not found for rollback"
  exit 4
fi

TMP_LINK="$TARGET.tmp-link"
ln -sfn "$NEW_BIN" "$TMP_LINK"
mv -Tf "$TMP_LINK" "$TARGET"
echo "[INFO] switched target -> $NEW_BIN"

if [[ "$NO_RESTART" == "1" ]]; then
  echo "[INFO] no-restart mode enabled; finish after switch"
  FINAL_STATE="succeeded"
  write_state "succeeded" "done" "switch completed in no-restart mode"
  exit 0
fi

echo "[INFO] restarting service $SERVICE"
write_state "running" "restart" "restarting service"
if ! systemctl restart "$SERVICE"; then
  echo "[ERR] restart failed, rolling back"
  ln -sfn "$OLD_RESOLVED" "$TMP_LINK"
  mv -Tf "$TMP_LINK" "$TARGET"
  systemctl restart "$SERVICE" || true
  FINAL_STATE="failed"
  write_state "failed" "restart" "restart failed; rollback attempted"
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
  FINAL_STATE="succeeded"
  write_state "succeeded" "health" "health check passed"
  exit 0
fi

echo "[ERR] health check failed, rolling back to $OLD_RESOLVED"
ln -sfn "$OLD_RESOLVED" "$TMP_LINK"
mv -Tf "$TMP_LINK" "$TARGET"
if ! systemctl restart "$SERVICE"; then
  echo "[ERR] rollback restart failed; manual intervention required" >&2
  FINAL_STATE="failed"
  write_state "failed" "rollback" "rollback restart failed; manual intervention required"
  exit 6
fi
echo "[INFO] rollback completed"
FINAL_STATE="failed"
write_state "failed" "rollback" "health check failed; rollback completed"
exit 7
