"""Rebuild Redis from PostgreSQL, excluding concurrent app ingests."""
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / "backend" if (root / "backend").is_dir() else root))

from app import create_app


if __name__ == "__main__":
    app = create_app({"START_HUB": False})
    try:
        live = app.extensions["live"]
        if live is None:
            raise SystemExit("REDIS_URL is required for a live cache rebuild")
        print(json.dumps(live.rebuild(app.extensions["database"])))
    finally:
        app.extensions["close_resources"]()
