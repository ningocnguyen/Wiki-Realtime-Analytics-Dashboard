"""Rebuild Redis from PostgreSQL, excluding concurrent app ingests."""
import json
import argparse
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / "backend" if (root / "backend").is_dir() else root))
# This one-shot process is not a Gunicorn worker and must not leave metric files behind.
os.environ.pop("PROMETHEUS_MULTIPROC_DIR", None)

from app import create_app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--if-needed", action="store_true", help="skip when the current generation is healthy")
    args = parser.parse_args()
    app = create_app({"START_HUB": False})
    try:
        live = app.extensions["live"]
        if live is None:
            raise SystemExit("REDIS_URL is required for a live cache rebuild")
        if args.if_needed and live.valid_generation(app.extensions["database"]):
            print(json.dumps({"ready": True, "rebuild_skipped": True}))
        else:
            print(json.dumps(live.rebuild(app.extensions["database"])))
    finally:
        app.extensions["close_resources"]()
