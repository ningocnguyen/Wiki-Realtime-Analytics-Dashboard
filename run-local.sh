#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

build=true
open_page=false
observability=false
for argument in "$@"; do
  case "$argument" in
    --no-build) build=false ;;
    --open) open_page=true ;;
    --observability) observability=true ;;
    --help)
      printf 'Usage: ./run-local.sh [--no-build] [--open] [--observability]\n'
      printf 'Starts PostgreSQL, Redis, two backend replicas, frontend, and the Wikipedia connector.\n'
      printf 'Rebuilds the Redis cache when needed. Dashboard: http://localhost:8080\n'
      exit 0 ;;
    *) printf 'Unknown option: %s\n' "$argument" >&2; exit 2 ;;
  esac
done

command -v docker >/dev/null || { printf 'Docker is required. Install and start Docker Desktop.\n' >&2; exit 1; }
command -v curl >/dev/null || { printf 'curl is required for the readiness check.\n' >&2; exit 1; }
docker info >/dev/null 2>&1 || { printf 'Docker is not running. Start Docker Desktop and try again.\n' >&2; exit 1; }

compose=(docker compose)
if "$observability"; then compose+=(--profile observability); fi
up=(up -d)
if "$build"; then up+=(--build); fi
"${compose[@]}" "${up[@]}"
# nginx resolves the backend Service when it starts; refresh after backend replacement.
"${compose[@]}" up -d --no-deps --force-recreate frontend

ready=false
for ((attempt=0; attempt<90; attempt++)); do
  if curl --fail --silent --max-time 2 http://localhost:8080/readyz >/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
if ! "$ready"; then
  printf 'Backend did not become ready. Inspect: docker compose logs backend postgres redis\n' >&2
  exit 1
fi

"${compose[@]}" exec -T backend python scripts/rebuild_live.py --if-needed
printf 'Dashboard ready: http://localhost:8080\n'
if "$observability"; then printf 'Prometheus: http://localhost:9090\n'; fi
if "$open_page"; then
  if command -v open >/dev/null; then open http://localhost:8080
  elif command -v xdg-open >/dev/null; then xdg-open http://localhost:8080
  fi
fi
