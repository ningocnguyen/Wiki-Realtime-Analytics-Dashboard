# Real-Time Analytics Dashboard

The dashboard will display Wikipedia only. Website tracker and simulator events will remain API-only.

## Implementation workflow

Implement and verify one milestone at a time, then pause for the owner to review and commit. Do not create commits automatically. Ask before starting the next milestone.

1. Project scaffold and local development setup (completed; owner committed).
2. Event contract, PostgreSQL schema, ingestion, rollups, and initial REST reads (completed; owner committed).
3. Wikipedia connector and offline replay (implemented; awaiting owner review/commit).
4. Redis statistics and WebSocket fan-out.
5. Wikipedia dashboard.
6. Website tracker and collection endpoint.
7. Full deployment support, launchers, and observability.
8. Correctness checks, benchmarks, and final documentation.

## Current behavior

The React shell checks `/api/config`. Flask accepts individual events and batches, stores raw events and minute rollups atomically, and exposes historical queries and PostgreSQL-backed summaries. The schema is initialized under an advisory lock at backend startup. `/healthz` checks process liveness and `/readyz` checks PostgreSQL and Redis.

The Wikipedia connector now sends the public recentchange stream into the backend. Redis counters, WebSockets, and dashboard charts are coming in later steps. PostgreSQL must be available when the backend starts. Each Gunicorn worker has its own database pool; account for the worker count when setting `DB_POOL_MAX`.

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

Success returns HTTP 202 with `{ "accepted": N }`, where N is the number of newly inserted events. The database transaction is committed before responding. Invalid batches return HTTP 400 without storing any events; excessive batches/bodies return HTTP 413. Database failures return HTTP 503. Retry with stable event IDs when the outcome of a request is uncertain.

| Endpoint | Behavior |
| --- | --- |
| `GET /api/sources` | All three supported sources; this does not create dashboard tabs |
| `GET /api/stats/summary` | Per-source counts and exact unique-user counts from PostgreSQL |
| `GET /api/stats/timeseries?source=wikipedia&minutes=60` | Rollup rows for the last 60 completed UTC minutes; sparse empty minutes |
| `GET /api/stats/breakdown?source=wikipedia&minutes=60` | Rollup totals by event type, including the current minute |
| `GET /api/events/recent?source=wikipedia&limit=50` | Latest inserted events, newest ID first |
| `GET /api/config` | Current implementation stage and dashboard source |

Read endpoints default to `wikipedia`. `minutes` must be 1–10,080; `limit` must be 1–200. Invalid query parameters return HTTP 400. Unique-user summaries currently scan raw events; Redis estimates will replace those live reads in Step 4. Future buckets are excluded from chart/summary queries.

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

Open http://localhost:8080. Backend health endpoints are also available on http://localhost:5050. Compose includes one Wikipedia connector, with a persistent `wiki_state` volume for its checkpoint. Local database credentials are for development.

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

Run all tests, including PostgreSQL integration checks:

```sh
TEST_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/analytics \
  .venv/bin/python -m unittest discover -s tests -v
```

Integration tests create a uniquely named schema and remove it afterward. They do not truncate or modify application tables. The supplied database user needs permission to create schemas. Coverage includes malformed batches, source isolation, completed-minute windows, deduplication, concurrent writes, atomic rollback, and repeat schema initialization.

Step 2 verification passed: all 12 tests, rebuilt Docker services, and API checks through nginx. Two `demo` events (`step2-verification-1` and `step2-verification-2`) were inserted for the deployed smoke check; retrying accepted zero additional events, and their rollup count/value total was 2/5. These remain API-only.

Step 3 verification passed: 23 tests, including repeated mock SSE disconnects with overlapping resume positions, checkpoint timing/locking, lost-ack retries, bounded buffering, normalization of all 200 fixture records, and offline replay. A replay into the running backend acknowledged/inserted 200 events with zero drops. The Docker connector collected live Wikimedia changes, survived container recreation using the saved checkpoint (`resuming=True`), and automatically recovered from a subsequent upstream disconnect. The connector remains running; Wikipedia storage currently includes both marked replay records and live events.
