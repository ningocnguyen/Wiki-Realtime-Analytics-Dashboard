# Real-Time Analytics Dashboard

The dashboard will display Wikipedia only. Website tracker and simulator events will remain API-only.

## Implementation workflow

Implement and verify one milestone at a time, then pause for the owner to review and commit. Do not create commits automatically. Ask before starting the next milestone.

1. Project scaffold and local development setup (completed; owner committed).
2. Event contract, PostgreSQL schema, ingestion, rollups, and initial REST reads (completed; owner committed).
3. Wikipedia connector and offline replay (completed; owner committed).
4. Redis statistics and WebSocket fan-out (implemented; awaiting owner review/commit).
5. Wikipedia dashboard.
6. Website tracker and collection endpoint.
7. Full deployment support, launchers, and observability.
8. Correctness checks, benchmarks, and final documentation.

## Current behavior

The React shell checks `/api/config`. Flask accepts individual events and batches, stores raw events and minute rollups atomically, and exposes historical queries and PostgreSQL-backed summaries. The schema is initialized under an advisory lock at backend startup. `/healthz` checks process liveness and `/readyz` checks PostgreSQL and Redis.

The Wikipedia connector sends the public recentchange stream into the backend. Redis supplies live counters, estimated unique users, dimension breakdowns, recent events, and pub/sub delivery to WebSocket workers. The frontend charts arrive in Step 5. PostgreSQL must be available when the backend starts. Each Gunicorn worker has its own database pool; account for both workers and replicas when setting `DB_POOL_MAX`.

## Event contract and API

`POST /api/events` accepts one event or `{ "events": [...] }`, with at most 1,000 events and a 5 MiB request body. Producers are trusted in this local implementation; this endpoint has no producer authentication yet.

```json
{
  "source": "wikipedia",
  "event_id": "example-edit-1",
  "type": "edit",
  "user_id": "example-editor",
  "value": 1,
  "props": { "wiki": "enwiki", "bot": "human", "title": "Example" },
  "ts": "2026-10-03T18:00:00Z"
}
```

- `source`: `wikipedia`, `site`, or `demo`; defaults to `demo`.
- `type` and `user_id`: required non-empty strings, at most 64 and 128 characters.
- `value`: finite number, defaults to zero.
- `props`: JSON object, at most 4,096 UTF-8 bytes.
- `ts`: ISO-8601 or epoch seconds/milliseconds; defaults to server UTC time. A timestamp without an offset is interpreted as UTC. Historical timestamps are preserved.
- `event_id`: optional stable identifier, at most 256 characters. The first stored event wins for each `(source, event_id)`. A retry does not update that event or inflate rollups. Events without IDs are counted on every submission.

Success returns HTTP 202 with `{ "accepted": N }`, where N is the number of newly inserted events. The database transaction is committed before Redis is updated or the request is acknowledged. A failed Redis update preserves the committed events, acknowledges ingestion, and makes statistics fall back to PostgreSQL. Invalid batches return HTTP 400 without storing any events; excessive batches/bodies return HTTP 413. Database failures return HTTP 503. Retry with stable event IDs when the outcome of a request is uncertain.

| Endpoint | Behavior |
| --- | --- |
| `GET /api/sources` | All three supported sources; this does not create dashboard tabs |
| `GET /api/stats/summary` | Redis live counts and HLL estimates; exact PostgreSQL fallback |
| `GET /api/stats/timeseries?source=wikipedia&minutes=60` | Rollup rows for the last 60 completed UTC minutes; sparse empty minutes |
| `GET /api/stats/breakdown?source=wikipedia&minutes=60` | Rollup totals by event type, including the current minute |
| `GET /api/events/recent?source=wikipedia&limit=50` | Latest inserted events, newest ID first |
| `GET /api/stats/dims?source=wikipedia&key=wiki&minutes=60` | Top values for `wiki`, `bot`, `ref_source`, `path`, or `target` |
| `WS /ws?source=wikipedia` | Source-filtered event batches, statistics, and recovery notices |
| `GET /metrics` | Prometheus counters, client gauge, ingest histogram, and drop counts for the responding worker |
| `GET /api/config` | Current implementation stage and dashboard source |

Read endpoints default to `wikipedia`. `minutes` must be 1–10,080; recent-event `limit` must be 1–200 and dimension `limit` 1–50. Invalid query parameters return HTTP 400. Charts and type breakdowns always read rollups. Dimension windows longer than 180 minutes use PostgreSQL. Future minute buckets are excluded from chart/summary queries; statistics temporarily use PostgreSQL while any future minute events exist and automatically return to Redis when their buckets start. Rebuilds retain those events in their future Redis buckets. Small clock differences within the current minute are included in Redis bucket statistics.

## Live delivery and cache recovery

Each Gunicorn worker owns one Redis subscription shared by its viewers. It coalesces publications for 10 ms, serializes once per watched source, and queues frames independently for each viewer. Client queues hold 256 frames; the hub inbox holds 10,000 publications. A slow client drops frames and receives a `resync` notice without blocking other clients.

The WebSocket protocol sends `hello`, `events`, `stats` (once per second), `ping`, and `resync`. Event frames contain only the subscribed source; hello/statistics frames include all source summaries. Clients must refresh REST snapshots on reconnect and `resync`, since Redis pub/sub cannot replay missed frames. The public dashboard will continue to show only Wikipedia; site and demo subscriptions are available for API clients and tests.

Redis keys are scoped by `REDIS_NAMESPACE` (default `rta`), generation, source, and UTC bucket. Minute keys expire after approximately three hours, daily keys after three days, and recent feeds hold 200 events. Unique users are HyperLogLog estimates; active users union the current and four preceding minute buckets, so that metric is an approximate bucketed window. Summary responses identify `stats_backend` and `unique_users_approximate`.

The cache begins in PostgreSQL fallback until you rebuild it. Rebuild once after deploying Step 4, and again after a Redis reset or an interrupted cache update:

```sh
docker compose exec -T backend python scripts/rebuild_live.py
```

For a manually started backend:

```sh
.venv/bin/python scripts/rebuild_live.py
```

Rebuild streams retained events into a fresh cache generation and replaces the active generation when complete. Application ingests take a shared PostgreSQL session advisory lock across their commit/Redis update; rebuild takes the exclusive lock. This briefly pauses ingestion during rebuild and avoids counting the same event twice. Use a direct PostgreSQL connection when Redis is enabled, rather than a transaction-pooling proxy.

A durable pending marker detects interruption between database commit and Redis completion. Redis failures, pending updates, or a missing generation cause REST statistics to use PostgreSQL. Rebuild clears those markers after reconstructing the cache. It does not replay past WebSocket frames or provide durable messaging; PostgreSQL snapshots supply recovery. `/readyz` requires Redis when configured; `REDIS_URL=''` disables live transport and uses PostgreSQL-only reads.

Worker heartbeats provide a shared `ws_clients` total when Redis summaries are active. `/metrics` describes the responding worker; aggregation across Gunicorn workers is future observability work.

Verify WebSocket delivery through two backend replicas:

```sh
.venv/bin/python scripts/smoke_ws.py --require-replicas
```

This connects demo viewers, submits a uniquely identified API-only event, checks delivery and source isolation, and retries the event to check deduplication. It is a correctness smoke check, not a throughput/latency benchmark.

Try a local event (omit `ts` to use the current time):

```sh
curl --fail http://localhost:8080/api/events \
  -H 'Content-Type: application/json' \
  -d '{"source":"demo","event_id":"local-example-1","type":"page_view","user_id":"visitor-1","value":1}'
curl --fail 'http://localhost:8080/api/stats/breakdown?source=demo'
```

## Docker

From the repository root:

```sh
docker compose up --build
```

Open http://localhost:8080. The frontend proxy also listens on http://localhost:5050 for API/development compatibility; it forwards to two backend replicas, each with two Gunicorn workers by default. Compose includes one Wikipedia connector, with a persistent `wiki_state` volume for its checkpoint. Local database credentials are for development.

To change the backend replica count, recreate the frontend proxy afterward so nginx resolves the updated service addresses:

```sh
docker compose up -d --scale backend=3
docker compose up -d --no-deps --force-recreate frontend
```

PostgreSQL is exposed on `127.0.0.1:55432` to avoid conflicting with existing databases. Redis uses `127.0.0.1:6379`. Containers connect to PostgreSQL on its internal port 5432.

## Development

Requires Python 3.12, Node.js 22.12+ or 24, npm, and a running Docker engine (or your own PostgreSQL and Redis).

Start dependencies:

```sh
docker compose up -d postgres redis
python3 -m venv .venv
.venv/bin/python -m pip install -r backend/requirements.txt -r connectors/requirements.txt
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

## Wikipedia connector

After starting the backend, run the connector from the repository root:

```sh
.venv/bin/python connectors/wikipedia.py
```

The default API is `http://localhost:5050`. Use `--api` or `INGEST_API_URL` for another backend. Configure `WIKI_USER_AGENT` with your project URL/contact when deploying. `WIKI_STREAM_URL` or `--url` can select a test stream. The connector uses a certificate bundle and retains HTTPS verification.

Every 250 ms or 200 events, it sends a batch to `/api/events`. Upstream `meta.id` identifies individual events; SSE `id` is the separate resume cursor. Stable event IDs make overlapping stream replay and uncertain HTTP acknowledgements safe to retry without inflating database counts. Reconnects use `Last-Event-ID`, exponential backoff with jitter, and upstream `retry:` hints. Canary records are filtered; malformed records are skipped and counted.

The checkpoint defaults to `.run/wikipedia-checkpoint.json`. It is replaced atomically only after a batch is acknowledged. A process restart reloads that acknowledged position, while an in-process reconnect uses the last queued position. A file lock prevents two processes from sharing the same checkpoint; deploy one live connector. Checkpoints are scoped to their stream URL. `--no-checkpoint` disables persistence for temporary experiments.

The buffer holds at most 20,000 events, including the pinned in-flight batch. During extended outages it drops the oldest queued events and counts them. If all capacity is in flight, new arrivals are dropped. This is a bounded-memory design, not a guarantee of lossless collection during unlimited outages. Stream replay history is also finite; see the [Wikimedia documentation](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams).

HTTP 429, server errors, and connection failures retry the same in-flight batch. Other rejected batches stop the connector visibly, without advancing its checkpoint. Logs report received, acknowledged, newly inserted acknowledgements, buffered, dropped, skipped, and retry counts. An acknowledgement can report zero newly inserted events when the earlier attempt already committed.

For a finite live check:

```sh
.venv/bin/python connectors/wikipedia.py --duration 15
```

SIGINT/SIGTERM stop collection and allow up to 10 seconds to flush pending events. A drain timeout exits with an error; unacknowledged live events can be retried from the saved checkpoint on restart, subject to upstream retention and any counted drops.

Offline replay needs the local backend but no Wikimedia connection:

```sh
.venv/bin/python connectors/wikipedia.py \
  --replay connectors/fixtures/wikimedia_recentchange_sample.jsonl --rate 30
```

Replay sends the 200 recorded events once and exits after flushing. Add `--loop` for continuous replay. Each run gets fresh timestamps and unique replay IDs, preserving IDs within HTTP retries; events have `props.replay=true` and remain `source=wikipedia`. Use replay when the live connector is stopped if you want unmixed fixture statistics.

Check stored changes:

```sh
curl --fail 'http://localhost:8080/api/events/recent?source=wikipedia&limit=5'
docker compose logs --tail=20 wikipedia
```

The frontend remains the setup shell until Step 5.

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

Run validation and connector tests (PostgreSQL tests skip without `TEST_DATABASE_URL`):

```sh
.venv/bin/python -m unittest discover -s tests -v
```

Run all tests, including PostgreSQL and Redis integration checks:

```sh
TEST_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/analytics \
  TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
  .venv/bin/python -m unittest discover -s tests -v
```

Integration tests create a uniquely named schema and remove it afterward. They do not truncate or modify application tables. The supplied database user needs permission to create schemas. Coverage includes malformed batches, source isolation, completed-minute windows, deduplication, concurrent writes, atomic rollback, and repeat schema initialization.

Redis integration tests use uniquely named key prefixes and clean up only their own keys. They cover cache rebuilds, post-commit failures, interruption markers, concurrent ingestion/rebuild, two independent subscribers, and slow-client recovery.

Step 2 verification passed: all 12 tests, rebuilt Docker services, and API checks through nginx. Two `demo` events (`step2-verification-1` and `step2-verification-2`) were inserted for the deployed smoke check; retrying accepted zero additional events, and their rollup count/value total was 2/5. These remain API-only.

Step 3 verification passed: 23 tests, including repeated mock SSE disconnects with overlapping resume positions, checkpoint timing/locking, lost-ack retries, bounded buffering, normalization of all 200 fixture records, and offline replay. A replay into the running backend acknowledged/inserted 200 events with zero drops. The Docker connector collected live Wikimedia changes, survived container recreation using the saved checkpoint (`resuming=True`), and automatically recovered from a subsequent upstream disconnect. The connector remains running; Wikipedia storage currently includes both marked replay records and live events.

Step 4 verification passed: all 38 tests, Docker builds, and checks through nginx. The WebSocket smoke check delivered one event to all 12 viewers across four workers in two backend containers, verified source filtering, and retried without inserting a duplicate. A cache rebuild restored 60,197 stored events; subsequent summaries reported `stats_backend=redis`. Readiness and the worker metrics endpoint passed. Integration coverage includes Redis failures, interrupted updates, concurrent rebuilds, bounded queues, recent-feed ordering/capping, and clock skew. This is a correctness check; throughput and latency benchmarks remain Step 8 work.
