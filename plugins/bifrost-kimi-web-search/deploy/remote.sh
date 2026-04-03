#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNTIME_ENV_FILE="${RUNTIME_ENV_FILE:-${SCRIPT_DIR}/runtime.env}"
CONFIG_FILE="${CONFIG_FILE:-${SCRIPT_DIR}/plugin-config.json}"
if [[ -f "${RUNTIME_ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${RUNTIME_ENV_FILE}"
fi

REMOTE_HOST="${REMOTE_HOST:-azureuser@57.154.32.127}"
REMOTE_DIR="${REMOTE_DIR:-/home/azureuser/bifrost-dynamic}"
CONTAINER_NAME="${CONTAINER_NAME:-bifrost-dynamic}"
HOST_PORT="${HOST_PORT:-18080}"
PLUGIN_NAME="bifrost-kimi-web-search"
PLUGIN_FILE="${PROJECT_DIR}/build/${PLUGIN_NAME}.so"
PLUGIN_PATH="/app/plugins/${PLUGIN_NAME}.so"
PLUGIN_PLACEMENT="${PLUGIN_PLACEMENT:-pre_builtin}"
PLUGIN_EXEC_ORDER="${PLUGIN_EXEC_ORDER:-2}"
CONTROL_PATH="/tmp/bifrost-plugin-ssh-%C"
SSH_OPTS=(
  -o ControlMaster=auto
  -o ControlPersist=60
  -o ControlPath="${CONTROL_PATH}"
)

cleanup() {
  ssh "${SSH_OPTS[@]}" -O exit "${REMOTE_HOST}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

echo "[1/6] build linux/amd64 plugin"
make -C "${PROJECT_DIR}" build-linux-amd64

if [[ ! -f "${PLUGIN_FILE}" ]]; then
  echo "missing plugin artifact: ${PLUGIN_FILE}" >&2
  exit 1
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "missing config file: ${CONFIG_FILE}" >&2
  exit 1
fi

CONFIG_JSON="$(tr -d '\n' < "${CONFIG_FILE}")"
CONFIG_JSON_ESCAPED="$(sql_escape "${CONFIG_JSON}")"

echo "[2/6] upload plugin"
scp "${SSH_OPTS[@]}" "${PLUGIN_FILE}" "${REMOTE_HOST}:${REMOTE_DIR}/plugins/${PLUGIN_NAME}.so"

LOCAL_SHA="$(shasum -a 256 "${PLUGIN_FILE}" | awk '{print $1}')"
REMOTE_SHA="$(ssh "${SSH_OPTS[@]}" "${REMOTE_HOST}" "shasum -a 256 '${REMOTE_DIR}/plugins/${PLUGIN_NAME}.so' | awk '{print \$1}'")"
if [[ "${LOCAL_SHA}" != "${REMOTE_SHA}" ]]; then
  echo "sha mismatch after upload: local=${LOCAL_SHA} remote=${REMOTE_SHA}" >&2
  exit 1
fi

echo "[3/6] backup config db"
ssh "${SSH_OPTS[@]}" "${REMOTE_HOST}" "\
  cp '${REMOTE_DIR}/data/config.db' '${REMOTE_DIR}/data/config.db.bak-'\"\$(date +%Y%m%dT%H%M%SZ)\""

echo "[4/6] upsert plugin config"
ssh "${SSH_OPTS[@]}" "${REMOTE_HOST}" "sqlite3 '${REMOTE_DIR}/data/config.db' <<SQL
INSERT INTO config_plugins (name, enabled, path, config_json, created_at, version, updated_at, is_custom, placement, exec_order)
VALUES (
  '${PLUGIN_NAME}',
  1,
  '${PLUGIN_PATH}',
  '${CONFIG_JSON_ESCAPED}',
  CURRENT_TIMESTAMP,
  1,
  CURRENT_TIMESTAMP,
  1,
  '${PLUGIN_PLACEMENT}',
  ${PLUGIN_EXEC_ORDER}
)
ON CONFLICT(name) DO UPDATE SET
  enabled = excluded.enabled,
  path = excluded.path,
  config_json = excluded.config_json,
  updated_at = excluded.updated_at,
  is_custom = excluded.is_custom,
  placement = excluded.placement,
  exec_order = excluded.exec_order,
  version = config_plugins.version + 1;
SQL"

echo "[5/6] restart container"
ssh "${SSH_OPTS[@]}" "${REMOTE_HOST}" "docker restart '${CONTAINER_NAME}' >/dev/null"

echo "[6/6] verify plugin load"
ssh "${SSH_OPTS[@]}" "${REMOTE_HOST}" "\
  for i in \$(seq 1 30); do \
    if curl -fsS 'http://127.0.0.1:${HOST_PORT}/health' >/dev/null 2>&1; then \
      exit 0; \
    fi; \
    sleep 1; \
  done; \
  echo 'health check did not become ready in time' >&2; \
  exit 1"

ssh "${SSH_OPTS[@]}" "${REMOTE_HOST}" "\
  docker logs '${CONTAINER_NAME}' 2>&1 | grep -E '${PLUGIN_NAME}|loading custom plugin|plugin status' | tail -n 60 && \
  printf '\n--- plugin rows ---\n' && \
  sqlite3 -json '${REMOTE_DIR}/data/config.db' \"select name,enabled,path,placement,exec_order,config_json from config_plugins where name='${PLUGIN_NAME}';\""
