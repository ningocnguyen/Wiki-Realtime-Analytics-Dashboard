import os

import psycopg2
from flask import Flask, jsonify, request
from psycopg2.pool import PoolError
from redis import Redis
from redis.exceptions import RedisError

from . import stats
from .db import Database
from .ingest import ingest
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
    )
    app.config.update(overrides or {})
    database = Database(app.config)
    try:
        database.apply_schema()
    except Exception:
        database.close()
        raise
    app.extensions["database"] = database

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
        return jsonify(accepted=ingest(database, events)), 202

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
        return jsonify(stats.recent(database, selected_source(), integer_argument("limit", 50, 1, 200)))

    @app.get("/api/stats/summary")
    def summary():
        return jsonify(stats.summary(database))

    @app.get("/api/config")
    def config():
        return jsonify(stage="storage", dashboard_source="wikipedia", realtime="disabled", wiki_pull=False)

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
        try:
            with Redis.from_url(
                app.config["REDIS_URL"], socket_connect_timeout=2, socket_timeout=2
            ) as client:
                client.ping()
            dependencies["redis"] = "ok"
        except RedisError:
            dependencies["redis"] = "unavailable"
        healthy = all(value == "ok" for value in dependencies.values())
        return jsonify(
            status="ready" if healthy else "not-ready", dependencies=dependencies
        ), 200 if healthy else 503

    return app
