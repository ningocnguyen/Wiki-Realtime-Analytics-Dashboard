"""PostgreSQL read APIs; chart queries use minute rollups, never raw scans."""
from datetime import datetime, timezone

from psycopg2.extras import RealDictCursor

from .validation import SOURCES


def query(database, sql, params):
    with database.connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def timeseries(database, source, minutes):
    rows = query(database, """
        SELECT bucket, event_type AS type, cnt AS count, value_sum
        FROM event_rollup_minute
        WHERE source = %s
          AND bucket >= date_trunc('minute', now()) - make_interval(mins => %s)
          AND bucket < date_trunc('minute', now())
        ORDER BY bucket, event_type
    """, (source, minutes))
    for row in rows:
        row["bucket"] = row["bucket"].astimezone(timezone.utc).isoformat()
    return rows


def breakdown(database, source, minutes):
    return query(database, """
        SELECT event_type, SUM(cnt)::bigint AS count, SUM(value_sum) AS value_sum
        FROM event_rollup_minute
        WHERE source = %s
          AND bucket >= date_trunc('minute', now()) - make_interval(mins => %s)
          AND bucket <= date_trunc('minute', now())
        GROUP BY event_type ORDER BY count DESC, event_type
    """, (source, minutes))


def recent(database, source, limit):
    rows = query(database, """
        SELECT id, event_id, source, event_type AS type, user_id, value, props,
               occurred_at, ingested_at
        FROM events WHERE source = %s ORDER BY id DESC LIMIT %s
    """, (source, limit))
    return [payload(row) for row in rows]


def payload(row):
    return {"id": row["id"], "event_id": row["event_id"], "source": row["source"],
            "type": row.get("type", row.get("event_type")), "user_id": row["user_id"],
            "value": row["value"], "props": row["props"],
            "occurred_at": row["occurred_at"].astimezone(timezone.utc).isoformat(),
            "ingested_at": int(row["ingested_at"].timestamp() * 1000)}


def dims(database, source, name, minutes, limit):
    return query(database, """
        SELECT left(props->>%s, 128) AS value, count(*)::bigint AS count
        FROM events WHERE source = %s
          AND occurred_at >= date_trunc('minute', now()) - make_interval(mins => %s)
          AND occurred_at <= now()
          AND jsonb_typeof(props->%s) IN ('string', 'number', 'boolean')
          AND props->>%s <> ''
        GROUP BY 1 ORDER BY count DESC, value LIMIT %s
    """, (name, source, minutes, name, name, limit))


def summary(database):
    # Exact fallback while Redis is unavailable or awaiting a rebuild.
    rows = query(database, """
        WITH bounds AS (
            SELECT date_trunc('minute', now()) AS minute,
                   date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS day
        ), counts AS (
            SELECT source,
              COALESCE(SUM(cnt) FILTER (WHERE bucket = minute - interval '1 minute'), 0)::bigint AS events_last_minute,
              COALESCE(SUM(cnt) FILTER (WHERE bucket = minute), 0)::bigint AS events_this_minute,
              COALESCE(SUM(cnt) FILTER (WHERE bucket >= day), 0)::bigint AS events_today
            FROM event_rollup_minute CROSS JOIN bounds
            WHERE bucket >= LEAST(day, minute - interval '1 minute') AND bucket <= minute
            GROUP BY source
        ), users AS (
            SELECT source,
              COUNT(DISTINCT user_id) FILTER (WHERE occurred_at >= now() - interval '5 minutes') AS active_users_5m,
              COUNT(DISTINCT user_id) FILTER (WHERE occurred_at >= day) AS unique_users_today
            FROM events CROSS JOIN bounds
            WHERE occurred_at >= LEAST(day, now() - interval '5 minutes') AND occurred_at <= now()
            GROUP BY source
        )
        SELECT COALESCE(counts.source, users.source) AS source,
               COALESCE(events_last_minute, 0) AS events_last_minute,
               COALESCE(events_this_minute, 0) AS events_this_minute,
               COALESCE(events_today, 0) AS events_today,
               COALESCE(active_users_5m, 0) AS active_users_5m,
               COALESCE(unique_users_today, 0) AS unique_users_today
        FROM counts FULL OUTER JOIN users USING (source)
    """, ())
    empty = dict(events_last_minute=0, events_this_minute=0, events_today=0,
                 active_users_5m=0, unique_users_today=0)
    sources = {source: dict(empty) for source in SOURCES}
    for row in rows:
        source = row.pop("source")
        sources[source] = row
    return {"sources": sources, "ws_clients": None,
            "server_time": int(datetime.now(timezone.utc).timestamp() * 1000),
            "stats_backend": "postgres", "unique_users_approximate": False}
