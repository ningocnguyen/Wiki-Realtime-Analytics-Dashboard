"""Integration tests create and remove their own schema; existing data is untouched."""
import importlib
import os
import sys
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app import create_app

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")


@unittest.skipUnless(TEST_DATABASE_URL, "set TEST_DATABASE_URL to run PostgreSQL integration tests")
class StorageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = "rta_test_" + uuid.uuid4().hex
        cls.admin = psycopg2.connect(TEST_DATABASE_URL)
        cls.admin.autocommit = True
        with cls.admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(cls.schema)))
        try:
            cls.app = create_app({
                "TESTING": True,
                "DATABASE_URL": make_dsn(TEST_DATABASE_URL, options=f"-c search_path={cls.schema}"),
                "DB_POOL_MAX": 8,
                "REDIS_URL": "",
            })
            cls.database = cls.app.extensions["database"]
        except Exception:
            cls.remove_schema()
            raise

    @classmethod
    def remove_schema(cls):
        with cls.admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(cls.schema)))
        cls.admin.close()

    @classmethod
    def tearDownClass(cls):
        cls.database.close()
        cls.remove_schema()

    def setUp(self):
        self.client = self.app.test_client()
        with self.database.connection() as conn, conn.cursor() as cursor:
            cursor.execute("TRUNCATE events, event_rollup_minute RESTART IDENTITY")
        self.minute = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    def event(self, **changes):
        return {"source": "wikipedia", "type": "edit", "user_id": "editor",
                "ts": (self.minute - timedelta(minutes=2)).isoformat(), **changes}

    def counts(self):
        with self.database.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM events")
            raw = cursor.fetchone()[0]
            cursor.execute("SELECT COALESCE(sum(cnt), 0) FROM event_rollup_minute")
            return raw, cursor.fetchone()[0]

    def test_batch_rollups_values_and_source_isolation(self):
        response = self.client.post("/api/events", json={"events": [
            self.event(value=2), self.event(value=3), self.event(source="demo", value=9),
        ]})
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json, {"accepted": 3})
        self.assertEqual(self.counts(), (3, 3))
        rows = self.client.get("/api/stats/timeseries?source=wikipedia").json
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["count"], rows[0]["value_sum"]), (2, 5))
        self.assertEqual(self.client.get("/api/stats/breakdown?source=demo").json[0]["count"], 1)
        recent = self.client.get("/api/events/recent?source=wikipedia&limit=1").json
        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0]["source"], "wikipedia")

    def test_retry_and_duplicates_within_batch_do_not_inflate_counts(self):
        batch = {"events": [self.event(event_id="same"), self.event(event_id="same")]}
        self.assertEqual(self.client.post("/api/events", json=batch).json, {"accepted": 1})
        self.assertEqual(self.client.post("/api/events", json=batch).json, {"accepted": 0})
        self.assertEqual(self.counts(), (1, 1))
        self.assertEqual(self.client.post("/api/events", json=self.event(source="demo", event_id="same")).json,
                         {"accepted": 1})

    def test_invalid_batch_is_rejected_before_any_write(self):
        response = self.client.post("/api/events", json={"events": [self.event(), self.event(type="")]})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.counts(), (0, 0))

    def test_database_failure_rolls_back_raw_insert(self):
        module = importlib.import_module("app.ingest")
        original = module.execute_values
        calls = 0

        def fail_rollup(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise psycopg2.OperationalError("simulated rollup failure")
            return original(*args, **kwargs)

        with patch.object(module, "execute_values", side_effect=fail_rollup), self.assertLogs(self.app.logger, level="ERROR"):
            response = self.client.post("/api/events", json=self.event())
        self.assertEqual(response.status_code, 503)
        self.assertEqual(self.counts(), (0, 0))
        self.assertEqual(self.client.post("/api/events", json=self.event()).status_code, 202)

    def test_concurrent_retries_and_rollup_updates(self):
        def write(index):
            with self.app.test_client() as client:
                return client.post("/api/events", json={"events": [
                    self.event(event_id=f"unique-{index}"), self.event(event_id="shared"),
                ]})

        with ThreadPoolExecutor(max_workers=6) as executor:
            responses = list(executor.map(write, range(12)))
        self.assertTrue(all(response.status_code == 202 for response in responses))
        self.assertEqual(sum(response.json["accepted"] for response in responses), 13)
        self.assertEqual(self.counts(), (13, 13))

    def test_completed_minute_window_and_summary(self):
        self.client.post("/api/events", json={"events": [
            self.event(ts=(self.minute - timedelta(minutes=61)).isoformat()),
            self.event(ts=(self.minute - timedelta(minutes=1)).isoformat()),
            self.event(ts=self.minute.isoformat()),
        ]})
        rows = self.client.get("/api/stats/timeseries?minutes=60").json
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["bucket"], (self.minute - timedelta(minutes=1)).isoformat())
        summary = self.client.get("/api/stats/summary").json["sources"]
        self.assertEqual(summary["wikipedia"]["events_last_minute"], 1)
        self.assertEqual(summary["wikipedia"]["events_this_minute"], 1)
        self.assertEqual(summary["wikipedia"]["active_users_5m"], 1)
        self.assertEqual(summary["site"]["events_today"], 0)

    def test_malformed_requests_and_query_bounds(self):
        for body in [None, {}, {"events": []}, {"events": "bad"}]:
            self.assertEqual(self.client.post("/api/events", json=body).status_code, 400)
        self.assertEqual(self.client.post("/api/events", data="not JSON", content_type="application/json").status_code, 400)
        response = self.client.post("/api/events", json={"events": [self.event()] * 1001})
        self.assertEqual(response.status_code, 413)
        with patch.dict(self.app.config, {"MAX_CONTENT_LENGTH": 32}):
            response = self.client.post("/api/events", json=self.event())
            self.assertEqual(response.status_code, 413)
        for url in ["/api/stats/timeseries?minutes=no", "/api/stats/timeseries?minutes=0",
                    "/api/stats/breakdown?source=unknown", "/api/events/recent?limit=201"]:
            self.assertEqual(self.client.get(url).status_code, 400)
        self.assertEqual(self.counts(), (0, 0))

    def test_multiple_initializations_preserve_existing_data(self):
        self.client.post("/api/events", json=self.event())
        self.database.apply_schema()
        self.assertEqual(self.counts(), (1, 1))
