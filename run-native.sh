#!/usr/bin/env bash
# Run the development stack without Docker, using local PostgreSQL and Redis.
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" == "--help" ]]; then
  printf 'Usage: ./run-native.sh [--open]\n'
  printf 'Requires Python 3.12, Node/npm, PostgreSQL at localhost:55432, and Redis at localhost:6379.\n'
  printf 'Set DATABASE_URL and REDIS_URL to use other local addresses.\n'
  exit 0
fi
if [[ "$#" -gt 1 || ( "$#" -eq 1 && "$1" != "--open" ) ]]; then
  printf 'Usage: ./run-native.sh [--open]\n' >&2
  exit 2
fi

command -v python3 >/dev/null || { printf 'Python 3 is required.\n' >&2; exit 1; }
command -v npm >/dev/null || { printf 'Node.js and npm are required.\n' >&2; exit 1; }
command -v curl >/dev/null || { printf 'curl is required.\n' >&2; exit 1; }

export DATABASE_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:55432/analytics}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
export PORT="${PORT:-5050}" WEB_CONCURRENCY="${WEB_CONCURRENCY:-2}"
frontend_port="${FRONTEND_PORT:-8080}"
export BACKEND_URL="http://127.0.0.1:$PORT"
export PROMETHEUS_MULTIPROC_DIR="$PWD/.run/prometheus"
mkdir -p .run "$PROMETHEUS_MULTIPROC_DIR"

if [[ ! -x .venv/bin/python ]]; then python3 -m venv .venv; fi
if ! .venv/bin/python -c 'import flask, gunicorn, aiohttp, redis, psycopg2, requests' 2>/dev/null; then
  .venv/bin/python -m pip install -r backend/requirements.txt -r connectors/requirements.txt
fi
if [[ ! -d frontend/node_modules ]]; then (cd frontend && npm ci); fi
export PATH="$PWD/.venv/bin:$PATH"

# Refuse to reuse an unrelated server on either dashboard port.
if .venv/bin/python -c 'import socket,sys; s=socket.socket(); s.settimeout(.2); sys.exit(0 if s.connect_ex(("127.0.0.1", int(sys.argv[1]))) == 0 else 1)' "$PORT"; then
  printf 'Port %s is already in use. Stop the existing server first.\n' "$PORT" >&2
  exit 1
fi
if .venv/bin/python -c 'import socket,sys; s=socket.socket(); s.settimeout(.2); sys.exit(0 if s.connect_ex(("127.0.0.1", int(sys.argv[1]))) == 0 else 1)' "$frontend_port"; then
  printf 'Port %s is already in use. Stop the existing server first.\n' "$frontend_port" >&2
  exit 1
fi

backend_pid='' frontend_pid='' connector_pid=''
cleanup() {
  for pid in "$connector_pid" "$frontend_pid" "$backend_pid"; do
    if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; fi
  done
  for pid in "$connector_pid" "$frontend_pid" "$backend_pid"; do
    if [[ -n "$pid" ]]; then wait "$pid" 2>/dev/null || true; fi
  done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

(cd backend && exec sh ../scripts/start-backend.sh) >.run/backend.log 2>&1 &
backend_pid=$!
ready=false
for ((attempt=0; attempt<40; attempt++)); do
  if curl --fail --silent --max-time 1 "$BACKEND_URL/readyz" >/dev/null; then ready=true; break; fi
  if ! kill -0 "$backend_pid" 2>/dev/null; then break; fi
  sleep 1
done
if ! "$ready"; then
  printf 'Backend not ready. Check PostgreSQL, Redis, DATABASE_URL, REDIS_URL, and .run/backend.log.\n' >&2
  exit 1
fi

(cd frontend && exec ./node_modules/.bin/vite --host 127.0.0.1 --port "$frontend_port" --strictPort) >.run/frontend.log 2>&1 &
frontend_pid=$!
ready=false
for ((attempt=0; attempt<30; attempt++)); do
  if curl --fail --silent --max-time 1 "http://127.0.0.1:$frontend_port/" >/dev/null; then ready=true; break; fi
  if ! kill -0 "$frontend_pid" 2>/dev/null; then break; fi
  sleep 1
done
if ! "$ready"; then printf 'Frontend not ready. See .run/frontend.log.\n' >&2; exit 1; fi

.venv/bin/python connectors/wikipedia.py --api "$BACKEND_URL" --checkpoint "$PWD/.run/wikipedia.json" >.run/wikipedia.log 2>&1 &
connector_pid=$!
.venv/bin/python scripts/rebuild_live.py --if-needed
printf 'Dashboard ready: http://127.0.0.1:%s\nLogs: .run/backend.log, .run/frontend.log, .run/wikipedia.log\nPress Ctrl+C to stop.\n' "$frontend_port"
if [[ "${1:-}" == "--open" ]]; then
  if command -v open >/dev/null; then open "http://127.0.0.1:$frontend_port"
  elif command -v xdg-open >/dev/null; then xdg-open "http://127.0.0.1:$frontend_port"
  fi
fi
while kill -0 "$backend_pid" 2>/dev/null && kill -0 "$frontend_pid" 2>/dev/null && kill -0 "$connector_pid" 2>/dev/null; do
  sleep 2
done
printf 'A native stack process stopped. Check the logs in .run/.\n' >&2
exit 1
