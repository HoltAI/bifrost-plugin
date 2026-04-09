#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <plugin-name>" >&2
  exit 1
fi

PLUGIN_NAME="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LIVE_DEPLOY_ROOT="${LIVE_DEPLOY_ROOT:-/home/azureuser/bifrost-dynamic}"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-${REPO_ROOT}/.build/live-bifrost-1.4.19}"
CONTAINER_NAME="${CONTAINER_NAME:-bifrost-dynamic}"
SO_PATH="${WORKSPACE_ROOT}/out/${PLUGIN_NAME}.so"
DEPLOY_SO="${LIVE_DEPLOY_ROOT}/plugins/${PLUGIN_NAME}.so"

if [[ ! -f "${SO_PATH}" ]]; then
  echo "missing built artifact: ${SO_PATH}" >&2
  exit 1
fi

mkdir -p "${LIVE_DEPLOY_ROOT}/plugins"

if [[ -f "${DEPLOY_SO}" ]]; then
  cp "${DEPLOY_SO}" "${DEPLOY_SO}.bak-$(date -u +%Y%m%dT%H%M%SZ)"
fi

cp "${SO_PATH}" "${DEPLOY_SO}"
docker restart "${CONTAINER_NAME}" >/dev/null
sleep "${WAIT_SECS:-15}"

echo "deployed: ${DEPLOY_SO}"
docker logs --since 2m "${CONTAINER_NAME}" 2>&1 | egrep "${PLUGIN_NAME}|failed to load plugin|plugin status" | tail -80 || true
curl -sS "http://127.0.0.1:18080/health"
