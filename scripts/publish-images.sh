#!/usr/bin/env bash
# Builds and publishes the three images for a Kubernetes release. Run explicitly after review.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$#" -ne 2 ]; then
  printf 'Usage: %s REGISTRY_PREFIX VERSION\n' "$0" >&2
  printf 'Example: %s ghcr.io/ningocnguyen/wiki-realtime-analytics-dashboard v1.0.0\n' "$0" >&2
  exit 2
fi
prefix="$1"
version="$2"
if [[ ! "$prefix" =~ ^[a-zA-Z0-9._/-]+$ || ! "$version" =~ ^[a-zA-Z0-9._-]+$ ]]; then
  printf 'Registry prefix or version contains invalid characters.\n' >&2
  exit 2
fi
docker buildx version >/dev/null
docker info >/dev/null

for component in backend frontend wikipedia; do
  case "$component" in
    backend) context=. ; dockerfile=backend/Dockerfile ;;
    frontend) context=frontend ; dockerfile=frontend/Dockerfile ;;
    wikipedia) context=connectors ; dockerfile=connectors/Dockerfile ;;
  esac
  image="${prefix}-${component}:${version}"
  printf 'Publishing %s\n' "$image"
  docker buildx build --platform linux/amd64,linux/arm64 \
    -f "$dockerfile" -t "$image" --push "$context"
done
printf 'Published all images. Set the matching tags in k8s/kustomization.yaml before deployment.\n'
