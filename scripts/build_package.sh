#!/usr/bin/env bash
# build.sh — Build pgmqtt extension tarballs for one or more Postgres versions.
#
# Usage:
#   ./build.sh              # builds for PG 16 (default)
#   ./build.sh 15 16 17     # builds for PG 15, 16, and 17
#
# Output:
#   dist/pgmqtt-pg<VER>-<ARCH>.tar.gz  (one per version, arch detected from Docker host)
#
# Prerequisites:
#   - Docker must be running
#   - Run from the repository root

set -euo pipefail

VERSIONS=("${@:-16}")

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIST_DIR="${SCRIPT_DIR}/dist"
mkdir -p "${DIST_DIR}"

# Detect the target architecture from the active Docker host.
# ARCH can be overridden: ARCH=arm64 ./build.sh
if [[ -z "${ARCH:-}" ]]; then
    RAW_ARCH=$(docker info --format '{{.Architecture}}' 2>/dev/null || echo "x86_64")
    case "${RAW_ARCH}" in
        aarch64|arm64) ARCH="arm64" ;;
        *)             ARCH="amd64" ;;
    esac
fi

echo "=== pgmqtt build ==="
echo "Versions: ${VERSIONS[*]}"
echo "Architecture: ${ARCH}"
echo ""

built=()

for v in "${VERSIONS[@]}"; do
    echo "──────────────────────────────────────────────"
    echo "Building for PostgreSQL ${v}..."
    echo "──────────────────────────────────────────────"

    IMAGE_TAG="pgmqtt-builder-pg${v}-${ARCH}"
    TARBALL_NAME="pgmqtt-pg${v}-${ARCH}"
    STAGING_DIR="${DIST_DIR}/${TARBALL_NAME}"

    # Build the Docker image
    docker build \
        --build-arg PG_MAJOR="${v}" \
        --platform "linux/${ARCH}" \
        -f docker/Dockerfile.build \
        -t "${IMAGE_TAG}" \
        .

    # Extract artifacts from the image
    # Create a temporary container (never started) and docker cp from it
    CONTAINER_ID=$(docker create --platform "linux/${ARCH}" "${IMAGE_TAG}" none)
    trap "docker rm -f ${CONTAINER_ID} >/dev/null 2>&1 || true" EXIT

    rm -rf "${STAGING_DIR}"
    mkdir -p "${STAGING_DIR}"
    docker cp "${CONTAINER_ID}:/dist/lib" "${STAGING_DIR}/lib"
    docker cp "${CONTAINER_ID}:/dist/extension" "${STAGING_DIR}/extension"
    docker rm -f "${CONTAINER_ID}" >/dev/null 2>&1
    trap - EXIT

    # Create the tarball
    tar -czf "${DIST_DIR}/${TARBALL_NAME}.tar.gz" \
        -C "${DIST_DIR}" \
        "${TARBALL_NAME}"

    # Clean up staging directory
    rm -rf "${STAGING_DIR}"

    TARBALL_SIZE=$(du -h "${DIST_DIR}/${TARBALL_NAME}.tar.gz" | cut -f1)
    echo "✓ Built: dist/${TARBALL_NAME}.tar.gz (${TARBALL_SIZE})"
    echo ""
    built+=("dist/${TARBALL_NAME}.tar.gz")
done

echo "══════════════════════════════════════════════"
echo "Build complete. Artifacts:"
for b in "${built[@]}"; do
    echo "  • ${b}"
done
echo "══════════════════════════════════════════════"
