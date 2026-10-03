# Real-Time Analytics Dashboard

A step-by-step rebuild of [JiajunWang23/realtime-analytics-dashboard](https://github.com/JiajunWang23/realtime-analytics-dashboard), referenced at commit `f84b289`.

The dashboard will display Wikipedia only. Website tracker and simulator events will remain API-only.

## Implementation workflow

Implement and verify one milestone at a time, then pause for the owner to review and commit. Do not create commits automatically. Ask before starting the next milestone.

1. Project scaffold and local development setup (implemented and verified; awaiting owner review/commit).
2. Event contract, PostgreSQL schema, ingestion, rollups, and initial REST reads.
3. Wikipedia connector and offline replay.
4. Redis statistics and WebSocket fan-out.
5. Wikipedia dashboard.
6. Website tracker and collection endpoint.
7. Full deployment support, launchers, and observability.
8. Correctness checks, benchmarks, and final documentation.

## Current behavior

The React shell checks `/api/config`. Flask exposes `/healthz` and `/readyz`; readiness checks PostgreSQL and Redis. Ingestion and live analytics are not implemented yet.

## Docker

From the repository root:

```sh
docker compose up --build
```

Open http://localhost:8080. Backend health endpoints are also available on http://localhost:5050. Local database credentials are for development.

PostgreSQL is exposed on `127.0.0.1:55432` to avoid conflicting with existing databases. Redis uses `127.0.0.1:6379`. Containers connect to PostgreSQL on its internal port 5432.

## Development

Requires Python 3.12, Node.js 22.12+ or 24, npm, and a running Docker engine (or your own PostgreSQL and Redis).

Start dependencies:

```sh
docker compose up -d postgres redis
python3 -m venv .venv
.venv/bin/python -m pip install -r backend/requirements.txt
```

Start Flask from the root in one terminal:

```sh
.venv/bin/gunicorn -c backend/gunicorn.conf.py --chdir backend wsgi:app
```

In another terminal:

```sh
cd frontend
npm ci
npm run dev
```

Open http://localhost:5173. Vite proxies API and health requests to port 5050, avoiding macOS AirPlay's common port 5000 conflict.

`.env.example` documents configuration. Compose supplies container settings directly; for manual runs, export any overrides in your shell before starting the backend. No dotenv loader is installed.

## Checks

```sh
docker compose config --quiet
curl --fail http://localhost:5050/healthz
curl --fail http://localhost:5050/readyz
cd frontend
npm run build
npm run lint
```

Readiness returns HTTP 503 when either dependency is unavailable. The source repository's reported benchmarks have not been reproduced here.

Step 1 verification passed: frontend production build and lint, backend configuration and endpoint smoke checks, Python dependency consistency, Docker builds, and HTTP requests through nginx. The running stack returned readiness with PostgreSQL and Redis both healthy. Visual browser verification has not been performed.
