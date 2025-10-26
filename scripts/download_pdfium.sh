#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--help" ]]; then
  cat <<'USAGE'
Download the prebuilt Pdfium binaries for the current platform.

Usage:
  scripts/download_pdfium.sh [version]

Arguments:
  version   Optional release tag (default: chromium/7483)

The archive is fetched from https://github.com/bblanchon/pdfium-binaries and
extracted into third_party/pdfium/.
USAGE
  exit 0
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT}/third_party/pdfium"
VERSION="${1:-${PDFIUM_VERSION:-chromium/7483}}"

OS="$(uname -s)"
ARCH="$(uname -m)"

case "${OS}" in
  Linux)
    platform="linux"
    ;;
  Darwin)
    platform="mac"
    ;;
  MINGW*|MSYS*|CYGWIN*)
    platform="win"
    ;;
  *)
    echo "Unsupported operating system: ${OS}" >&2
    exit 1
    ;;
esac

case "${ARCH}" in
  x86_64|amd64)
    arch="x64"
    ;;
  arm64|aarch64)
    arch="arm64"
    ;;
  *)
    echo "Unsupported CPU architecture: ${ARCH}" >&2
    exit 1
    ;;
esac

ARCHIVE="pdfium-${platform}-${arch}.tgz"
URL="https://github.com/bblanchon/pdfium-binaries/releases/download/${VERSION}/${ARCHIVE}"

echo "Downloading ${URL}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

curl -L --fail --show-error --output "${tmp_dir}/${ARCHIVE}" "${URL}"

echo "Extracting to ${OUT_DIR}"
rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"
tar -xzf "${tmp_dir}/${ARCHIVE}" -C "${OUT_DIR}"

echo "Pdfium installed under ${OUT_DIR}"
