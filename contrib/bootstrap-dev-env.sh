#!/usr/bin/env bash
# Bootstrap the minimal host tooling needed to build and run the devcontainer.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

export PATH="$(cmn_strip_windows_paths)"

cmn_ensure_host_packages
cmn_ensure_docker

image="$(jq -er '.image | select(type == "string" and length > 0)' "${REPOSITORY_ROOT}/.devcontainer/devcontainer.json")"
build_image=false
if docker image inspect "${image}" >/dev/null 2>&1; then
    cmn_log bootstrap "Using existing devcontainer image: ${image}"
else
    pull_log="$(mktemp)"
    if cmn_pull_devcontainer_image "${REPOSITORY_ROOT}" >"${pull_log}" 2>&1; then
        cat "${pull_log}"
    else
        pull_status=$?
        cat "${pull_log}" >&2
        if grep -Eiq 'unauthorized|authentication required|access denied|denied: requested access|401 unauthorized|403 forbidden' "${pull_log}"; then
            build_image=true
            cmn_log bootstrap "Registry access denied; building the pinned image from source."
        else
            rm -f "${pull_log}"
            exit "${pull_status}"
        fi
    fi
    rm -f "${pull_log}"
fi

cmn_ensure_node
cmn_npm_ci "${REPOSITORY_ROOT}"

if [[ "${build_image}" == true ]]; then
    (
        cd "${REPOSITORY_ROOT}"
        npx --no-install nx run devcontainer:tag
        tag="$(tr -d '\r\n' < .devcontainer/.image-tag)"
        if [[ "${image##*:}" != "${tag}" ]]; then
            cmn_log bootstrap "FATAL: configured image does not match the source-derived tag ${tag}." >&2
            exit 1
        fi
        npx --no-install nx run devcontainer:test
        docker image inspect "${image}" >/dev/null
    )
fi

cat <<EOF

Host bootstrap complete.
  Node:   $(node --version)
  npm:    $(npm --version)
  Docker: $(docker --version)
  Buildx: $(docker buildx version | head -1)
  Compose: $(docker compose version)

Build the prebuilt image:
  npx --no-install nx run devcontainer:build

Start the repository headlessly:
  npx --no-install nx run devcontainer:up
EOF
