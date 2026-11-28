#!/usr/bin/env bash
# Installs the pinned agent tooling the Darwin lanes need on a fresh devbox/devcontainer:
#   * the Copilot CLI that Darwin 0.3.0's github-copilot-sdk 1.0.5 drives (CLI 1.0.5 itself is NOT usable: it
#     fails with "Unhandled method connect/runtime.shutdown"; the SDK needs 1.0.67);
#   * optionally the Darwin 0.3.0 wheel when DARWIN_WHEEL=<path> is set (sha256 verified).
# Identity never comes from here: Darwin reads COPILOT_GITHUB_TOKEN / COPILOT_GH_HOST / COPILOT_GH_ACCOUNT from the
# repo's gitignored .env that repo-bootstrapper syncs (sync/<repo>); this script neither reads nor prints them.
#
# Usage: bash experiments/darwin/provision-agent-tools.sh   (idempotent; DARWIN_TOOLS_DIR overrides ~/.darwin-tools)
set -euo pipefail

DARWIN_WHEEL_SHA256=e5432cc6457dc6c65272d872082f71cbbd439d869db2d591b773e92cbc915f0e # darwin_evolution-0.3.0, source commit c50a954
COPILOT_VERSION=1.0.67
COPILOT_TARBALL_SHA256=c6d251de20d14415ebd6af00bfb6a8e3854094b49fa49fb7f3df2bfa64f6db3b # copilot-linux-x64.tar.gz of v1.0.67
COPILOT_BINARY_SHA256=a78b6e40e447fbadf500ac04e05e7f1378e73ffe96c67d3fff46f27200e34bc1

tools="${DARWIN_TOOLS_DIR:-$HOME/.darwin-tools}"
copilot_dir="$tools/copilot-cli/$COPILOT_VERSION"
copilot_bin="$copilot_dir/bin/copilot"

verify() { # <sha256> <file>
  echo "$1  $2" | sha256sum --check --quiet - || { echo "sha256 mismatch for $2" >&2; exit 1; }
}

if [[ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ]]; then
  echo "only linux-x64 is pinned" >&2
  exit 1
fi

if [[ ! -x "$copilot_bin" ]] || ! "$copilot_bin" --version 2>/dev/null | grep -q "$COPILOT_VERSION"; then
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT
  tarball="${COPILOT_TARBALL:-$work/copilot.tar.gz}"
  if [[ -z "${COPILOT_TARBALL:-}" ]]; then
    curl -fsSL "https://github.com/github/copilot-cli/releases/download/v${COPILOT_VERSION}/copilot-linux-x64.tar.gz" -o "$tarball"
  fi
  verify "$COPILOT_TARBALL_SHA256" "$tarball"
  rm -rf "$copilot_dir"
  mkdir -p "$copilot_dir/bin"
  tar -xzf "$tarball" -C "$copilot_dir/bin" copilot
  chmod 0755 "$copilot_bin"
fi
verify "$COPILOT_BINARY_SHA256" "$copilot_bin"

darwin_bin="$(command -v darwin || true)"
if [[ -n "${DARWIN_WHEEL:-}" ]]; then
  verify "$DARWIN_WHEEL_SHA256" "$DARWIN_WHEEL"
  venv="$tools/venv"
  if [[ ! -x "$venv/bin/python" ]]; then
    python3 -m venv --without-pip "$venv"
    python3 -m pip --python "$venv/bin/python" install pip
  fi
  if ! "$venv/bin/darwin" --version 2>/dev/null | grep -q " 0.3.0$"; then
    "$venv/bin/python" -m pip install --quiet "$DARWIN_WHEEL"
  fi
  darwin_bin="$venv/bin/darwin"
fi

echo "copilot: $("$copilot_bin" --version | head -1)"
if [[ -n "$darwin_bin" ]]; then
  echo "darwin: $("$darwin_bin" --version)"
  echo "export PATH=\"$(dirname "$darwin_bin"):\$PATH\""
else
  echo "darwin is not installed: rerun with DARWIN_WHEEL=<path to darwin_evolution-0.3.0-py3-none-any.whl>" >&2
fi
echo "export COPILOT_CLI_PATH=\"$copilot_bin\""
