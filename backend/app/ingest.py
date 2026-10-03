"""Commit events/rollups, then update Redis while excluding cache rebuilds."""
import logging
import time
import uuid
from collections import defaultdict

from psycopg2.errors import DeadlockDetected, SerializationFailure
from psycopg2.extras import Json, RealDictCursor, execute_values
from redis.exceptions import RedisError

from .stats import payload

log = logging.getLogger(__name__)


def ingest(database, events, live=None, metrics=None):
    ordered = sorted(events, key=lambda event: (event["source"], event["event_id"] or ""))
    rows = [(e["event_id"], e["source"], e["type"], e["user_id"], e["value"], Json(e["props"]), e["occurred_at"])
            for e in ordered]
    for attempt in range(3):
        try:
            with database.connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
                locked = False
                try:
                    generation = None
                    if live:
                        cursor.execute("SELECT pg_advisory_lock_shared(%s)", (live.lock_id,))
                        locked = True
                        cursor.execute("SELECT generation, dirty FROM live_cache_state WHERE name = %s", (live.namespace,))
                        state = cursor.fetchone()
                        generation = state["generation"] if state and not state["dirty"] else None
                    inserted = execute_values(cursor, """INSERT INTO events
                        (event_id, source, event_type, user_id, value, props, occurred_at)
                        VALUES %s ON CONFLICT (source, event_id) DO NOTHING RETURNING *""",
                                              rows, page_size=len(rows), fetch=True)
                    rollups = defaultdict(lambda: [0, 0.0])
                    for row in inserted:
                        aggregate = rollups[(row["source"], row["occurred_at"].replace(second=0, microsecond=0), row["event_type"])]
                        aggregate[0] += 1
                        aggregate[1] += row["value"]
                    if rollups:
                        execute_values(cursor, """INSERT INTO event_rollup_minute
                            (source, bucket, event_type, cnt, value_sum) VALUES %s
                            ON CONFLICT (source, bucket, event_type) DO UPDATE SET
                              cnt = event_rollup_minute.cnt + EXCLUDED.cnt,
                              value_sum = event_rollup_minute.value_sum + EXCLUDED.value_sum""",
                                       sorted((s, b, t, count, total) for (s, b, t), (count, total) in rollups.items()))
                    result = [payload(row) for row in inserted]
                    pending_id = uuid.uuid4().hex if live and result else None
                    if pending_id:
                        cursor.execute("INSERT INTO live_cache_pending (id, name) VALUES (%s, %s)", (pending_id, live.namespace))
                    conn.commit()
                    if live and result:
                        try:
                            live.record(result, generation)
                        except RedisError:
                            log.exception("Redis update failed after commit; statistics require a rebuild")
                            if metrics:
                                metrics.cache_errors.inc()
                            cursor.execute("""INSERT INTO live_cache_state (name, dirty) VALUES (%s, TRUE)
                                ON CONFLICT (name) DO UPDATE SET dirty = TRUE""", (live.namespace,))
                            conn.commit()
                        else:
                            cursor.execute("DELETE FROM live_cache_pending WHERE id = %s", (pending_id,))
                            conn.commit()
                finally:
                    if locked and not conn.closed:
                        conn.rollback()
                        cursor.execute("SELECT pg_advisory_unlock_shared(%s)", (live.lock_id,))
            return result
        except (DeadlockDetected, SerializationFailure):
            if attempt == 2:
                raise
            time.sleep(0.01 * 2 ** attempt)
