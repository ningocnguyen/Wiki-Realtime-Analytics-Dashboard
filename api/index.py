"""Vercel's WSGI entrypoint for the PostgreSQL-only dashboard mode."""
import sys
from pathlib import Path

from flask import Flask, jsonify

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))


def build_app():
    try:
        from app import create_app
        return create_app()
    except Exception as error:
        fallback = Flask(__name__)
        message = f"{type(error).__name__}: {error}".split("\n")[0][:300]

        @fallback.route("/", defaults={"path": ""}, methods=["GET", "POST"])
        @fallback.route("/<path:path>", methods=["GET", "POST"])
        def startup_error(path):
            return jsonify(status="startup-error", error=message,
                           hint="Configure DATABASE_URL and DATABASE_URL_UNPOOLED, then redeploy"), 503

        return fallback


# Vercel's Python runtime detects this module-level app assignment.
app = build_app()
