#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_ENV_FILE="${RUNTIME_ENV_FILE:-${SCRIPT_DIR}/runtime.env}"
if [[ -f "${RUNTIME_ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${RUNTIME_ENV_FILE}"
fi

REMOTE_HOST="${REMOTE_HOST:?REMOTE_HOST is required}"
CONTAINER_NAME="${CONTAINER_NAME:-bifrost-dynamic}"
IMAGE_NAME="${IMAGE_NAME:?IMAGE_NAME is required}"
HOST_PORT="${HOST_PORT:-18080}"
CONTAINER_PORT="${CONTAINER_PORT:-8080}"
DATA_DIR="${DATA_DIR:?DATA_DIR is required}"
PLUGINS_DIR="${PLUGINS_DIR:?PLUGINS_DIR is required}"
NETWORK_MODE="${NETWORK_MODE:-bridge}"
RESTART_POLICY="${RESTART_POLICY:-unless-stopped}"

ssh "${REMOTE_HOST}" "\
  mkdir -p '${DATA_DIR}' '${PLUGINS_DIR}' && \
  docker rm -f '${CONTAINER_NAME}' >/dev/null 2>&1 || true && \
  docker run -d \
    --name '${CONTAINER_NAME}' \
    --restart '${RESTART_POLICY}' \
    --network '${NETWORK_MODE}' \
    -p '${HOST_PORT}:${CONTAINER_PORT}' \
    -e APP_PORT='${CONTAINER_PORT}' \
    -e APP_HOST='0.0.0.0' \
    -e LOG_LEVEL='info' \
    -e LOG_STYLE='json' \
    -v '${DATA_DIR}:/app/data' \
    -v '${PLUGINS_DIR}:/app/plugins' \
    '${IMAGE_NAME}'"
