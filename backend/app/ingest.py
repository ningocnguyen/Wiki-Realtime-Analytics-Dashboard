"""Commit new raw events and their minute rollups atomically."""
import time
from collections import defaultdict

from psycopg2.errors import DeadlockDetected, SerializationFailure
from psycopg2.extras import Json, execute_values


def ingest(database, events):
    # A consistent insert order also reduces conflict-lock inversions on retried IDs.
    ordered = sorted(events, key=lambda event: (event["source"], event["event_id"] or ""))
    rows = [
        (e["event_id"], e["source"], e["type"], e["user_id"], e["value"], Json(e["props"]), e["occurred_at"])
        for e in ordered
    ]
    for attempt in range(3):
        try:
            with database.connection() as conn, conn.cursor() as cursor:
                inserted = execute_values(
                    cursor,
                    """INSERT INTO events
                       (event_id, source, event_type, user_id, value, props, occurred_at)
                       VALUES %s ON CONFLICT (source, event_id) DO NOTHING
                       RETURNING source, occurred_at, event_type, value""",
                    rows, page_size=len(rows), fetch=True,
                )
                rollups = defaultdict(lambda: [0, 0.0])
                for src, occurred_at, event_type, value in inserted:
                    aggregate = rollups[(src, occurred_at.replace(second=0, microsecond=0), event_type)]
                    aggregate[0] += 1
                    aggregate[1] += value
                if rollups:
                    execute_values(
                        cursor,
                        """INSERT INTO event_rollup_minute
                           (source, bucket, event_type, cnt, value_sum) VALUES %s
                           ON CONFLICT (source, bucket, event_type) DO UPDATE SET
                             cnt = event_rollup_minute.cnt + EXCLUDED.cnt,
                             value_sum = event_rollup_minute.value_sum + EXCLUDED.value_sum""",
                        sorted((s, b, t, count, total) for (s, b, t), (count, total) in rollups.items()),
                    )
            return len(inserted)
        except (DeadlockDetected, SerializationFailure):
            if attempt == 2:
                raise
            time.sleep(0.01 * 2 ** attempt)
