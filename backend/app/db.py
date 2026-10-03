"""A connection pool owned by each application/worker, with transaction cleanup."""
from contextlib import contextmanager
from pathlib import Path

from psycopg2.pool import ThreadedConnectionPool


class Database:
    def __init__(self, config):
        self.pool = ThreadedConnectionPool(
            config["DB_POOL_MIN"], config["DB_POOL_MAX"],
            config["DATABASE_URL"], connect_timeout=3,
        )

    @contextmanager
    def connection(self):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            yield conn
            conn.commit()
        except BaseException:
            if not conn.closed:
                conn.rollback()
            raise
        finally:
            self.pool.putconn(conn, close=bool(conn.closed))

    def apply_schema(self):
        with self.connection() as conn, conn.cursor() as cursor:
            # Multiple workers may initialize simultaneously.
            cursor.execute("SELECT pg_advisory_xact_lock(424242)")
            cursor.execute((Path(__file__).parent / "schema.sql").read_text())

    def close(self):
        self.pool.closeall()
