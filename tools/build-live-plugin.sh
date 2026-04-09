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
LIVE_SRC="${LIVE_DEPLOY_ROOT}/src"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-${REPO_ROOT}/.build/live-bifrost-1.4.19}"
OUT_DIR="${WORKSPACE_ROOT}/out"
GO_IMAGE="${GO_IMAGE:-golang:1.26.1-bookworm}"

PLUGIN_SRC="${REPO_ROOT}/plugins/${PLUGIN_NAME}"
WORKSPACE_PLUGIN="${WORKSPACE_ROOT}/plugins/${PLUGIN_NAME}"

if [[ ! -d "${PLUGIN_SRC}" ]]; then
  echo "missing plugin source: ${PLUGIN_SRC}" >&2
  exit 1
fi

if [[ ! -d "${LIVE_SRC}/core" || ! -d "${LIVE_SRC}/framework" || ! -d "${LIVE_SRC}/transports" ]]; then
  echo "live source snapshot is incomplete: ${LIVE_SRC}" >&2
  exit 1
fi

bootstrap_workspace() {
  rm -rf "${WORKSPACE_ROOT}"
  mkdir -p "${WORKSPACE_ROOT}" "${OUT_DIR}"
  cp -a "${LIVE_SRC}/core" "${WORKSPACE_ROOT}/core"
  cp -a "${LIVE_SRC}/framework" "${WORKSPACE_ROOT}/framework"
  cp -a "${LIVE_SRC}/plugins" "${WORKSPACE_ROOT}/plugins"
  cp -a "${LIVE_SRC}/transports" "${WORKSPACE_ROOT}/transports"
}

if [[ "${REFRESH_WORKSPACE:-0}" == "1" || ! -d "${WORKSPACE_ROOT}/core" ]]; then
  bootstrap_workspace
fi

mkdir -p "${OUT_DIR}"
rm -rf "${WORKSPACE_PLUGIN}"
cp -a "${PLUGIN_SRC}" "${WORKSPACE_PLUGIN}"

cat > "${WORKSPACE_ROOT}/go.work" <<EOF
go 1.26.1

use (
  ./core
  ./framework
  ./plugins/governance
  ./plugins/jsonparser
  ./plugins/litellmcompat
  ./plugins/logging
  ./plugins/maxim
  ./plugins/mocker
  ./plugins/otel
  ./plugins/semanticcache
  ./plugins/telemetry
  ./plugins/${PLUGIN_NAME}
  ./transports
)
EOF

docker run --rm \
  -v "${WORKSPACE_ROOT}:/src" \
  -w "/src/plugins/${PLUGIN_NAME}" \
  "${GO_IMAGE}" \
  bash -lc "
    set -euo pipefail
    export PATH=/usr/local/go/bin:\$PATH
    apt-get update >/dev/null
    apt-get install -y --no-install-recommends gcc libc6-dev libsqlite3-dev >/dev/null
    export CGO_ENABLED=1 GOOS=linux GOARCH=amd64 GOAMD64=v1
    go work sync
    go mod download
    go build -a -trimpath -buildmode=plugin \
      -o '/src/out/${PLUGIN_NAME}.so' \
      './cmd/${PLUGIN_NAME}'
  "

ls -lh "${OUT_DIR}/${PLUGIN_NAME}.so"
echo "built: ${OUT_DIR}/${PLUGIN_NAME}.so"
