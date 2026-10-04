"""Short Wikipedia SSE pulls for deployments without an always-on connector."""
import json
import sys
import time
from pathlib import Path

import psycopg2
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from connectors.wikipedia import SSEParser, to_event  # noqa: E402

from .ingest import ingest
from .validation import ValidationError, validate

LOCK_KEY = 7_770_001


def pull(config, database, seconds):
    # A pooled URL may use transaction pooling, which cannot preserve a session lock.
    direct_url = config.get("DIRECT_DATABASE_URL")
    if not direct_url:
        raise RuntimeError("DATABASE_URL_UNPOOLED is required for on-demand pulls")
    connection = psycopg2.connect(direct_url, connect_timeout=5)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
            if not cursor.fetchone()[0]:
                return {"status": "busy", "ingested": 0}
            try:
                cursor.execute("""SELECT last_id FROM connector_state
                    WHERE name = 'wikipedia' AND updated_at > now() - interval '120 seconds'""")
                row = cursor.fetchone()
                last_id = row[0] if row else None
                result = _read_stream(config, database, cursor, seconds, last_id)
                if config["WIKI_RETENTION_HOURS"] > 0:
                    cursor.execute("""DELETE FROM events WHERE id IN (
                        SELECT id FROM events WHERE source = 'wikipedia'
                        AND occurred_at < now() - make_interval(hours => %s)
                        ORDER BY occurred_at LIMIT 20000)""", (config["WIKI_RETENTION_HOURS"],))
                    result["pruned"] = cursor.rowcount
                return result
            finally:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
    finally:
        connection.close()


def _read_stream(config, database, cursor, seconds, last_id):
    parser = SSEParser(last_id)
    headers = {"Accept": "text/event-stream", "User-Agent": config["WIKI_USER_AGENT"]}
    if last_id:
        headers["Last-Event-ID"] = last_id
    deadline = time.monotonic() + seconds
    last_flush = time.monotonic()
    saved_id = last_id
    acknowledged_id = last_id
    batch = []
    ingested = 0

    def flush():
        nonlocal batch, ingested, saved_id, last_flush
        if batch:
            ingested += len(ingest(database, batch))
            batch = []
        if acknowledged_id and acknowledged_id != saved_id:
            cursor.execute("""INSERT INTO connector_state (name, last_id, updated_at)
                VALUES ('wikipedia', %s, now()) ON CONFLICT (name)
                DO UPDATE SET last_id = EXCLUDED.last_id, updated_at = now()""", (acknowledged_id,))
            saved_id = acknowledged_id
        last_flush = time.monotonic()

    try:
        with requests.get(config["WIKI_STREAM_URL"], headers=headers, stream=True,
                          timeout=(5, 5)) as response:
            response.raise_for_status()
            for line in response.iter_lines(chunk_size=256):
                frame = parser.feed(line)
                if frame:
                    try:
                        event = to_event(json.loads(frame["data"]))
                        if event:
                            batch.append(validate(event))
                    except (ValueError, TypeError, ValidationError, OverflowError):
                        pass
                    acknowledged_id = frame["id"]
                now = time.monotonic()
                if len(batch) >= 200 or now - last_flush >= 1:
                    flush()
                if now >= deadline:
                    break
    finally:
        flush()
    return {"status": "ok", "ingested": ingested, "resumed": bool(last_id), "seconds": seconds}
