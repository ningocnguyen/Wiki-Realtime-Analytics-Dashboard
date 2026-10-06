"""Compare raw and rollup query time in temporary PostgreSQL tables."""
import argparse
import json
import os
import statistics
from contextlib import closing

import psycopg2

RAW_SQL = """SELECT date_trunc('minute', occurred_at) AS bucket, event_type,
                   count(*) AS count FROM benchmark_events
             WHERE occurred_at >= date_trunc('minute', now()) - make_interval(mins => %s)
               AND occurred_at < date_trunc('minute', now())
             GROUP BY 1, 2 ORDER BY 1, 2"""
ROLLUP_SQL = """SELECT bucket, event_type, cnt AS count FROM benchmark_rollup
                WHERE bucket >= date_trunc('minute', now()) - make_interval(mins => %s)
                  AND bucket < date_trunc('minute', now())
                ORDER BY bucket, event_type"""


def measure(cursor, sql, minutes, repeats):
    cursor.execute(sql, (minutes,))
    expected = cursor.fetchall()
    samples = []
    for _ in range(repeats):
        cursor.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, (minutes,))
        samples.append(cursor.fetchone()[0][0]["Execution Time"])
    return expected, round(statistics.mean(samples), 3)


def run(args):
    url = args.database_url or os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit("Set TEST_DATABASE_URL or DATABASE_URL, or pass --database-url")
    with closing(psycopg2.connect(url, connect_timeout=5)) as connection, connection, connection.cursor() as cursor:
        cursor.execute("""CREATE TEMP TABLE benchmark_events AS
            SELECT date_trunc('minute', now()) - make_interval(mins => gs %% 1440) AS occurred_at,
                   (ARRAY['edit', 'new', 'log'])[(gs %% 3) + 1] AS event_type
            FROM generate_series(1, %s) AS gs""", (args.rows,))
        cursor.execute("ANALYZE benchmark_events")
        raw, no_index_ms = measure(cursor, RAW_SQL, args.minutes, args.repeats)
        cursor.execute("CREATE INDEX ON benchmark_events (occurred_at, event_type)")
        cursor.execute("ANALYZE benchmark_events")
        indexed, indexed_ms = measure(cursor, RAW_SQL, args.minutes, args.repeats)
        cursor.execute("""CREATE TEMP TABLE benchmark_rollup AS
            SELECT date_trunc('minute', occurred_at) AS bucket, event_type, count(*) AS cnt
            FROM benchmark_events GROUP BY 1, 2""")
        cursor.execute("CREATE UNIQUE INDEX ON benchmark_rollup (bucket, event_type)")
        cursor.execute("ANALYZE benchmark_rollup")
        rolled, rollup_ms = measure(cursor, ROLLUP_SQL, args.minutes, args.repeats)
        if raw != indexed or raw != rolled:
            raise RuntimeError("raw and rollup queries returned different results")
        print(json.dumps({"rows": args.rows, "window_minutes": args.minutes, "result_rows": len(raw),
                          "repeats": args.repeats, "mean_execution_ms": {
                              "raw_no_index": no_index_ms, "raw_indexed": indexed_ms,
                              "rollup": rollup_ms}}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--minutes", type=int, default=60)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if not 1000 <= args.rows <= 5000000 or not 1 <= args.minutes <= 1440 or not 1 <= args.repeats <= 100:
        parser.error("rows must be 1,000–5,000,000; minutes 1–1,440; repeats 1–100")
    run(args)
