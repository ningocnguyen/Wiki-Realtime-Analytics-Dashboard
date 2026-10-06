"""Serverless polling and on-demand pulls against an isolated PostgreSQL schema."""
import json
import os
import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from app import create_app
from app.wikipull import LOCK_KEY


class FakeStream:
    def __init__(self, lines):
        self.lines = lines

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        pass

    def iter_lines(self, chunk_size):
        yield from self.lines


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "set TEST_DATABASE_URL for PostgreSQL integration tests")
class ServerlessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = "rta_serverless_" + uuid.uuid4().hex
        cls.admin = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
        cls.admin.autocommit = True
        with cls.admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(cls.schema)))
        cls.dsn = make_dsn(os.environ["TEST_DATABASE_URL"], options=f"-c search_path={cls.schema}")
        try:
            cls.app = create_app({"TESTING": True, "DATABASE_URL": cls.dsn,
                                  "DIRECT_DATABASE_URL": cls.dsn, "REDIS_URL": "",
                                  "START_HUB": False, "WIKI_PULL": True})
        except Exception:
            cls.admin.close()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.app.extensions["close_resources"]()
        with cls.admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(cls.schema)))
        cls.admin.close()

    def setUp(self):
        self.client = self.app.test_client()
        with self.app.extensions["database"].connection() as conn, conn.cursor() as cursor:
            cursor.execute("TRUNCATE events, event_rollup_minute, connector_state RESTART IDENTITY")

    def record(self, identity):
        return {"meta": {"id": identity, "dt": datetime.now(timezone.utc).isoformat(),
                          "domain": "en.wikipedia.org"}, "type": "edit", "wiki": "enwiki",
                "user": "tester", "title": "Test", "length": {"old": 1, "new": 3}}

    def stream(self, *records, unfinished=None):
        lines = []
        for identity, record in records:
            lines.extend([f"id: {identity}".encode(),
                          ("data: " + json.dumps(record)).encode(), b""])
        if unfinished:
            lines.extend([f"id: {unfinished}".encode(), b"data: {"])
        return FakeStream(lines)

    def test_polling_cursor_and_resume_only_committed_sse_frames(self):
        self.assertEqual(self.client.get("/api/config").json["realtime"], "poll")
        with patch("app.wikipull.requests.get", return_value=self.stream(
                ("cursor-1", self.record("one")), ("cursor-2", self.record("two")),
                unfinished="cursor-3")) as get:
            response = self.client.post("/api/ingest/wikipedia?seconds=1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["ingested"], 2)
        self.assertNotIn("Last-Event-ID", get.call_args.kwargs["headers"])
        feed = self.client.get("/api/live?source=wikipedia&after=0").json
        self.assertEqual(len(feed["events"]), 2)
        self.assertEqual(feed["cursor"], feed["events"][-1]["id"])
        self.assertEqual(self.client.get(f"/api/live?source=wikipedia&after={feed['cursor']}").json["events"], [])

        with patch("app.wikipull.requests.get", return_value=self.stream(
                ("cursor-2", self.record("two")))) as get:
            response = self.client.post("/api/ingest/wikipedia?seconds=1")
        self.assertEqual(get.call_args.kwargs["headers"]["Last-Event-ID"], "cursor-2")
        self.assertEqual(response.json["ingested"], 0)
        self.assertEqual(response.json["resumed"], True)

    def test_concurrent_pull_returns_busy(self):
        owner = psycopg2.connect(self.dsn)
        owner.autocommit = True
        try:
            with owner.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
            with patch("app.wikipull.requests.get") as get:
                response = self.client.post("/api/ingest/wikipedia?seconds=1")
            self.assertEqual(response.json["status"], "busy")
            get.assert_not_called()
        finally:
            owner.close()

    def test_public_serverless_ingest_requires_a_producer_token(self):
        original_serverless = self.app.config["SERVERLESS"]
        original_token = self.app.config["INGEST_TOKEN"]
        event = {"source": "demo", "event_id": "serverless-auth", "type": "test", "user_id": "tester"}
        try:
            self.app.config["SERVERLESS"] = True
            self.app.config["INGEST_TOKEN"] = ""
            self.assertEqual(self.client.post("/api/events", json=event).status_code, 403)
            self.app.config["INGEST_TOKEN"] = "test-secret"
            self.assertEqual(self.client.post("/api/events", json=event).status_code, 401)
            accepted = self.client.post("/api/events", json=event,
                                        headers={"Authorization": "Bearer test-secret"})
            self.assertEqual((accepted.status_code, accepted.json["accepted"]), (202, 1))
        finally:
            self.app.config["SERVERLESS"] = original_serverless
            self.app.config["INGEST_TOKEN"] = original_token

    def test_retention_prunes_raw_rows_but_keeps_rollups(self):
        old = self.record("old")
        old["meta"]["dt"] = "2020-01-01T00:00:00Z"
        self.app.config["WIKI_RETENTION_HOURS"] = 48
        try:
            with patch("app.wikipull.requests.get", return_value=self.stream(("cursor-old", old))):
                result = self.client.post("/api/ingest/wikipedia?seconds=1")
            self.assertEqual(result.json["ingested"], 1)
            self.assertEqual(result.json["pruned"], 1)
            with self.app.extensions["database"].connection() as conn, conn.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM events")
                self.assertEqual(cursor.fetchone()[0], 0)
                cursor.execute("SELECT sum(cnt) FROM event_rollup_minute")
                self.assertEqual(cursor.fetchone()[0], 1)
        finally:
            self.app.config["WIKI_RETENTION_HOURS"] = 0
