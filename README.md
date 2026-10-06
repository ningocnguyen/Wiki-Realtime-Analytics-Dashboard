# Wikipedia Real-Time Analytics Dashboard

A React dashboard for the live [Wikimedia Recent Change stream](https://stream.wikimedia.org/). A Python connector reads Server-Sent Events, Flask stores raw events and minute rollups in PostgreSQL, Redis maintains live counters and publishes updates, and WebSockets deliver those updates to the browser. The dashboard shows **Wikipedia only**. The website tracker (`source=site`) and synthetic producer (`source=demo`) remain API-only.

## Run locally

Start Docker Desktop, then run:

```bash
./run-local.sh
```

Open <http://localhost:8080>. This starts PostgreSQL, Redis, two backend replicas, the React frontend, and one Wikipedia connector. On macOS, double-click `start.command`: it starts Docker Desktop if needed and uses the same stack. If Docker is unavailable, it invokes `run-native.sh`, which requires Python 3.12, Node/npm, and locally running PostgreSQL and Redis. The native runner uses PostgreSQL at `localhost:55432` and Redis at `localhost:6379` by default; `DATABASE_URL` and `REDIS_URL` can override them.

To include Prometheus:

```bash
./run-local.sh --observability
```

Prometheus is at <http://localhost:9090>. Stop the stack with `docker compose --profile observability down`; its PostgreSQL and connector volumes remain available for the next run.

The connector needs outbound access to Wikimedia EventStreams. For an offline connector check, replay the included JSONL fixture with `python connectors/wikipedia.py --replay connectors/fixtures/wikimedia_recentchange_sample.jsonl --api http://localhost:8080`.

## Architecture

```mermaid
flowchart LR
  WM[Wikimedia EventStreams] --> C[Wikipedia SSE connector]
  C -->|POST /api/events| API[Flask ingest API]
  T[Website tracker] -->|POST /api/collect| API
  S[Simulator] -->|POST /api/events| API
  API -->|raw events + minute rollups| PG[(PostgreSQL)]
  API -->|counters, unique users, publish| R[(Redis)]
  R --> H[one subscriber per backend worker]
  H -->|source-filtered WebSockets| UI[Wikipedia dashboard]
  PG -->|REST snapshots| UI
  R -->|live snapshots| UI
```

The connector checkpoints the SSE event ID and resumes with `Last-Event-ID` after reconnecting. Event IDs deduplicate retries. Ingest writes raw rows and minute rollups in one database transaction, then updates Redis. PostgreSQL remains authoritative: the Redis cache can be rebuilt with `docker compose exec backend python scripts/rebuild_live.py`. Each backend worker subscribes once to Redis, coalesces updates for 10 ms, and fans out only the source each socket requested. REST snapshots resynchronize the dashboard after reconnects.

`tracker/rta.js` sends first-party page views, selected link clicks, and 30-second engagement beacons to `/api/collect`. It is **not installed on any website by this repository**. The collection API accepts only configured origins (`COLLECT_ORIGINS`), applies input validation and rate limiting, and forces those events to `source=site`. For a separate HTTPS website, set `COLLECT_ORIGINS` to that site's origin and add:

```html
<script defer src="https://YOUR-DASHBOARD-HOST/rta.js"
        data-endpoint="https://YOUR-DASHBOARD-HOST/api/collect"></script>
```

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/api/events` | Trusted producer: one event or `{ "events": [...] }`; returns `202` and an accepted count. |
| `POST` | `/api/collect` | Validated browser beacons from configured origins. |
| `GET` | `/api/stats/summary` | Per-source counts and unique-user estimates. |
| `GET` | `/api/stats/timeseries?source=wikipedia&minutes=60` | Completed-minute chart rows. |
| `GET` | `/api/stats/breakdown?source=wikipedia&minutes=60` | Counts by event type. |
| `GET` | `/api/stats/dims?source=wikipedia&key=wiki&minutes=60` | Top dimension values (`wiki`, `bot`, `ref_source`, `path`, `target`). |
| `GET` | `/api/events/recent?source=wikipedia&limit=50` | Recent events. |
| `GET` | `/api/live?source=wikipedia&after=0` | Cursor-based polling for serverless mode. |
| `POST` | `/api/ingest/wikipedia?seconds=18` | On-demand SSE pull in Vercel mode only. |
| `WS` | `/ws?source=wikipedia` | Live `hello`, `events`, `stats`, `resync`, and `ping` frames. |
| `GET` | `/healthz`, `/readyz`, `/metrics` | Liveness, dependency readiness, and Prometheus metrics. |

Example trusted event:

```json
{"source":"demo","event_id":"unique-event-id","type":"page_view","user_id":"visitor-1","value":1,"props":{"path":"/"}}
```

`event_id` is optional but should be supplied by producers that may retry: uniqueness is enforced per source. The dashboard deliberately ignores `site` and `demo` events.

## Verify and benchmark

Set up the Python test environment once:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r backend/requirements.txt -r connectors/requirements.txt
cd frontend && npm ci && cd ..
```

Run correctness checks against the local Docker services. The integration suite creates and removes isolated PostgreSQL schemas:

```bash
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:55432/analytics \
TEST_REDIS_URL=redis://localhost:6379/0 \
.venv/bin/python -m unittest discover -s tests
cd frontend && npm test && npm run lint && npm run build
```

The benchmark scripts report JSON so their outputs can be saved and compared. `bench_query.py` uses temporary tables; the other two writers persist **demo-source** events in the configured application database.

```bash
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:55432/analytics \
  .venv/bin/python scripts/bench_query.py --rows 1000000 --minutes 1440 --repeats 5
.venv/bin/python scripts/simulate.py --total 5000 --rate 1000 --batch 100
.venv/bin/python scripts/bench_ws.py --clients 10 --events 50
.venv/bin/python scripts/smoke_ws.py --require-replicas
.venv/bin/python scripts/uptime_probe.py --seconds 60 --min-uptime 99.8
```

Results measured locally on October 6, 2026 with Docker Desktop, two backend containers, and the Wikipedia connector running:

| Check | Observation |
| --- | --- |
| Backend and frontend tests | 47 Python tests and 4 frontend tests passed; frontend lint and build passed. |
| Query benchmark, 1 million generated rows, five warm 24-hour reads | Raw without index: 457.7 ms; raw with index: 480.8 ms; minute rollup: 0.27 ms mean server execution time. The index does not help a near-full-table scan. |
| Ingest, 5,000 synthetic demo events | 5,000 accepted in 7.90 s, about 633 events/s. This is a short burst, not a sustained capacity result. |
| WebSocket, 1 client × 100 events | 100/100 delivered; HTTP-start-to-frame latency p50 31.9 ms, p95 168.2 ms. |
| WebSocket, 10 clients × 50 events | 500/500 delivered; latency p50 33.0 ms, p95 183.9 ms. |
| Readiness, 10 seconds | 10/10 `/readyz` checks succeeded. This does not establish a production uptime percentage. |

Latency varied across runs: a later 10-client × 10-event check delivered 100/100 frames but reached 791 ms p95. The original performance goals of sub-100 ms p95 latency, 1,000 concurrent viewers, 5 million records, and 99.8% Kubernetes uptime remain **unverified** here. A burst of 50 WebSocket handshakes exposed an expensive per-connection statistics read; the handshake now sends a lightweight `hello` and lets the client fetch its snapshot. The 50/1,000-client benchmark must be rerun on a rebuilt stack before making a concurrency claim.

## Deploy

### Kubernetes

The manifests in `k8s/` define PostgreSQL with a persistent volume, Redis, two backend replicas with an HPA of 2–10, a single `Recreate` Wikipedia connector, two frontend replicas with an HPA of 2–4, a cache bootstrap Job, and an ingress. The backend also has readiness/liveness probes and a disruption budget. The manifests have been rendered and checked against strict Kubernetes schemas; a live cluster has **not** been tested.

1. Publish the three images with `scripts/publish-images.sh REGISTRY_PREFIX VERSION`, then set the corresponding immutable tags in `k8s/kustomization.yaml`. The script pushes images, so run it only after reviewing its destination.
2. Set your dashboard hostname in `k8s/40-ingress.yaml` and your tracker origin in `k8s/01-config.yaml`.
3. Create namespace `analytics` and a Secret named `analytics-secrets` with `POSTGRES_PASSWORD` and `DATABASE_URL`. The latter should point to `postgres:5432/analytics` and include a URL-encoded password. Keep credentials out of Git.
4. Provide a TLS Secret named `analytics-tls`, an `nginx` ingress controller, a storage class for the volume claims, and metrics-server for the HPAs.
5. Apply with `kubectl apply -k k8s`, then inspect `kubectl -n analytics get pods,hpa,ingress` and `kubectl -n analytics logs deploy/wikipedia-connector`.

The default image tag `unpublished` and host `analytics.example.invalid` are placeholders; they must be replaced before a deployment.

### Vercel

`vercel.json` builds the React app and serves Flask from `api/index.py`. In this mode Redis and WebSockets are optional: the dashboard polls `/api/live` every two seconds, and a visible tab asks the backend to pull Wikimedia SSE for short periods. PostgreSQL advisory locks ensure that concurrent tabs share one pull. Set both `DATABASE_URL` and a **direct, unpooled** `DATABASE_URL_UNPOOLED`; the latter holds the session lock. `WIKI_RETENTION_HOURS` defaults to 48 in Vercel mode and prunes old raw Wikipedia rows while keeping minute rollups. If using the website tracker, set `COLLECT_ORIGINS` to its HTTPS origin.

To deploy, connect this GitHub repository to a Vercel project with the repository root as the project root. Attach a hosted PostgreSQL database, set `DATABASE_URL` to its pooled URL and `DATABASE_URL_UNPOOLED` to its direct URL in the project environment, then deploy. The repo pins Python 3.12 in `.python-version`; `vercel.json` builds `frontend/dist` and routes API requests to the Python function. Visit `/api/config` on the deployment to confirm `"realtime":"poll"` and `"wiki_pull":true`, then open the dashboard and check `/readyz` for PostgreSQL readiness. In Vercel mode, anonymous `POST /api/events` is disabled; set `INGEST_TOKEN` to enable trusted producers and send it as an `Authorization: Bearer` token. The website tracker continues to use its separately validated `/api/collect` route.

The Vercel entrypoint has been exercised in PostgreSQL-only mode locally, but no Vercel project has been deployed or verified yet.

## Operational limits

`POST /api/events` is currently unauthenticated and is intended for trusted connectors and local load tests. Restrict that route at the network edge or add producer authentication before exposing a public deployment. `COLLECT_ORIGINS` protects the browser tracker route; it does not authenticate `/api/events`. Redis pub/sub does not replay missed frames, so the dashboard refreshes from PostgreSQL-backed APIs after reconnecting. The single-node PostgreSQL and Redis manifests are a learning-project deployment, not a high-availability database setup.
