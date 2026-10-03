import os

import psycopg2
from flask import Flask, jsonify
from redis import Redis
from redis.exceptions import RedisError


def create_app():
    app = Flask(__name__)
    app.config.update(
        DATABASE_URL=os.getenv(
            "DATABASE_URL", "postgresql://postgres:postgres@localhost:55432/analytics"
        ),
        REDIS_URL=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    )

    @app.get("/api/config")
    def config():
        return jsonify(stage="scaffold", dashboard_source="wikipedia")

    @app.get("/healthz")
    def health():
        return jsonify(status="ok")

    @app.get("/readyz")
    def ready():
        dependencies = {}
        try:
            conn = psycopg2.connect(app.config["DATABASE_URL"], connect_timeout=2)
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                dependencies["postgres"] = "ok"
            finally:
                conn.close()
        except psycopg2.Error:
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
