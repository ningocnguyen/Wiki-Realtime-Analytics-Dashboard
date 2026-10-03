import os
import json
import queue
import time
from collections import Counter

import psycopg2
from flask import Flask, jsonify, request
from psycopg2.pool import PoolError
from redis.exceptions import RedisError
from flask_sock import Sock
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import stats
from .db import Database
from .ingest import ingest
from .hub import Hub
from .live import DIM_KEYS, LiveStore
from .metrics import Metrics
from .validation import SOURCES, ValidationError, source, validate


def create_app(overrides=None):
    app = Flask(__name__)
    app.config.update(
        DATABASE_URL=os.getenv(
            "DATABASE_URL", "postgresql://postgres:postgres@localhost:55432/analytics"
        ),
        REDIS_URL=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        DB_POOL_MIN=int(os.getenv("DB_POOL_MIN", "1")),
        DB_POOL_MAX=int(os.getenv("DB_POOL_MAX", "10")),
        MAX_BATCH=1000,
        MAX_CONTENT_LENGTH=5 * 1024 * 1024,
        REDIS_NAMESPACE=os.getenv("REDIS_NAMESPACE", "rta"),
        START_HUB=True,
        WS_QUEUE_SIZE=256,
        HUB_INBOX_SIZE=10000,
        BROADCAST_WINDOW_MS=10,
        WS_PING_SECONDS=20,
    )
    app.config.update(overrides or {})
    database = Database(app.config)
    try:
        database.apply_schema()
    except Exception:
        database.close()
        raise
    app.extensions["database"] = database
    metrics = Metrics()
    live = LiveStore(app.config) if app.config["REDIS_URL"] else None
    app.extensions.update(metrics=metrics, live=live)

    def cached(method, fallback, *args):
        if live:
            try:
                result = getattr(live, method)(database, *args)
                if result is not None:
                    return result
            except RedisError:
                app.logger.warning("Redis read unavailable; using PostgreSQL")
        return fallback(database, *args)

    def live_summary():
        return cached("summary", stats.summary)

    hub = Hub(live, metrics, live_summary, app.config) if live and app.config["START_HUB"] else None
    app.extensions["hub"] = hub
    if hub:
        hub.start()

    def close_resources():
        if hub:
            hub.close()
        if live:
            live.client.close()
        database.close()

    app.extensions["close_resources"] = close_resources

    @app.errorhandler(ValidationError)
    def invalid_request(error):
        return jsonify(error=str(error)), 400

    @app.errorhandler(413)
    def too_large(error):
        return jsonify(error="request body or batch exceeds the configured limit"), 413

    @app.errorhandler(psycopg2.Error)
    @app.errorhandler(PoolError)
    def database_error(error):
        app.logger.exception("Database operation failed")
        return jsonify(error="database unavailable; retry the request"), 503

    def integer_argument(name, default, minimum, maximum):
        try:
            result = int(request.args.get(name, default))
        except ValueError:
            raise ValidationError(f"{name} must be an integer") from None
        if not minimum <= result <= maximum:
            raise ValidationError(f"{name} must be between {minimum} and {maximum}")
        return result

    def selected_source():
        return source(request.args.get("source", "wikipedia"))

    @app.post("/api/events")
    def post_events():
        body = request.get_json(silent=True)
        raw = body.get("events") if isinstance(body, dict) and "events" in body else [body]
        if not isinstance(raw, list) or not raw:
            raise ValidationError("body must be an event or {events: [non-empty list]}")
        if len(raw) > app.config["MAX_BATCH"]:
            return too_large(None)
        # Validate the entire batch before starting a transaction.
        events = [validate(event) for event in raw]
        started = time.perf_counter()
        inserted = ingest(database, events, live, metrics)
        metrics.ingest_seconds.observe(time.perf_counter() - started)
        for src, count in Counter(event["source"] for event in inserted).items():
            metrics.events.labels(source=src).inc(count)
        return jsonify(accepted=len(inserted)), 202

    @app.get("/api/sources")
    def sources():
        return jsonify(list(SOURCES))

    @app.get("/api/stats/timeseries")
    def timeseries():
        return jsonify(stats.timeseries(database, selected_source(), integer_argument("minutes", 60, 1, 10080)))

    @app.get("/api/stats/breakdown")
    def breakdown():
        return jsonify(stats.breakdown(database, selected_source(), integer_argument("minutes", 60, 1, 10080)))

    @app.get("/api/events/recent")
    def recent():
        return jsonify(cached("recent", stats.recent, selected_source(), integer_argument("limit", 50, 1, 200)))

    @app.get("/api/stats/dims")
    def dims():
        key = request.args.get("key", "wiki")
        if key not in DIM_KEYS:
            raise ValidationError(f"key must be one of {', '.join(DIM_KEYS)}")
        return jsonify(cached("dims", stats.dims, selected_source(), key,
                              integer_argument("minutes", 60, 1, 10080), integer_argument("limit", 10, 1, 50)))

    @app.get("/api/stats/summary")
    def summary():
        return jsonify(live_summary())

    @app.get("/api/config")
    def config():
        return jsonify(stage="realtime", dashboard_source="wikipedia", realtime="ws" if hub else "disabled", wiki_pull=False)

    if hub:
        app.config["SOCK_SERVER_OPTIONS"] = {"ping_interval": app.config["WS_PING_SECONDS"]}
        sock = Sock(app)

        @sock.route("/ws")
        def websocket(connection):
            selected = source(request.args.get("source", "wikipedia"))
            client = hub.register(selected)
            try:
                connection.send(json.dumps({"kind": "hello", "worker_id": hub.worker_id, **live_summary()}))
                metrics.frames.inc()
                while not hub.stopping.is_set():
                    reason = client.take_resync()
                    if reason:
                        connection.send(json.dumps({"kind": "resync", "reason": reason}))
                        metrics.frames.inc()
                    try:
                        frame = client.q.get(timeout=app.config["WS_PING_SECONDS"])
                    except queue.Empty:
                        frame = json.dumps({"kind": "ping"})
                    connection.send(frame)
                    metrics.frames.inc()
            except Exception:
                app.logger.debug("WebSocket disconnected", exc_info=True)
            finally:
                hub.unregister(client)

    @app.get("/metrics")
    def prometheus_metrics():
        return generate_latest(metrics.registry), 200, {"Content-Type": CONTENT_TYPE_LATEST}

    @app.get("/healthz")
    def health():
        return jsonify(status="ok")

    @app.get("/readyz")
    def ready():
        dependencies = {}
        try:
            with database.connection() as conn, conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            dependencies["postgres"] = "ok"
        except (psycopg2.Error, PoolError):
            dependencies["postgres"] = "unavailable"
        if live:
            try:
                live.client.ping()
                dependencies["redis"] = "ok"
            except RedisError:
                dependencies["redis"] = "unavailable"
        healthy = all(value == "ok" for value in dependencies.values())
        return jsonify(
            status="ready" if healthy else "not-ready", dependencies=dependencies
        ), 200 if healthy else 503

    return app
