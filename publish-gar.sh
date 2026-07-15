#!/usr/bin/env bash
# Publish the metrics agent to Google Artifact Registry from a local checkout.
#
# CI (.github/workflows/publish.yml) publishes to ghcr.io only; the xano
# platform pulls from GAR, and nothing bridges the two — this script is that
# bridge, run deliberately by a human with gcloud auth already configured
# (`gcloud auth configure-docker us-docker.pkg.dev`).
#
# Usage:
#   ./publish-gar.sh              # version from the v* tag on HEAD
#   ./publish-gar.sh 0.1.2        # explicit version (still requires clean tree)
#   GAR_IMAGE=us-docker.pkg.dev/xano-registry/public/mesh0-agent ./publish-gar.sh
#
# Defaults to the `platform` repo — that's what the master's mesh0-agent
# component references, and repo-level IAM allows human pushes there (the
# `public` repo only lets the bobthebuilder pipeline SA write).
#
# Publishes $GAR_IMAGE:<version> for linux/amd64 + linux/arm64. No `latest`
# tag on purpose: deploy registries get pinned versions only, so every bump
# is explicit (a cached `latest` is how stale agents linger — see
# cloud-client#3042).
set -euo pipefail

GAR_IMAGE="${GAR_IMAGE:-us-docker.pkg.dev/xano-registry/platform/mesh0-agent}"

if ! git diff --quiet HEAD 2>/dev/null; then
  echo "error: working tree is dirty; publish only committed code" >&2
  exit 1
fi

if [ $# -ge 1 ]; then
  VERSION="${1#v}"
  BUILD_VERSION="v${VERSION}"
else
  TAG="$(git describe --tags --exact-match 2>/dev/null || true)"
  case "$TAG" in
    v*) ;;
    *)
      echo "error: HEAD has no v* tag; check out a release tag or pass a version" >&2
      exit 1
      ;;
  esac
  VERSION="${TAG#v}"
  BUILD_VERSION="$TAG"   # matches CI, which passes github.ref_name (e.g. v0.1.1)
fi

DEST="${GAR_IMAGE}:${VERSION}"

if docker manifest inspect "$DEST" >/dev/null 2>&1; then
  echo "error: ${DEST} already exists; bump the version instead of overwriting" >&2
  exit 1
fi

echo "publishing ${DEST} (VERSION=${BUILD_VERSION}, commit $(git rev-parse --short HEAD))"

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --build-arg "VERSION=${BUILD_VERSION}" \
  --tag "$DEST" \
  --push \
  .

echo "published ${DEST}"
