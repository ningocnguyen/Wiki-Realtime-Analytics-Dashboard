"""Website beacon contract and isolation tests using an independent PostgreSQL schema."""
import json
import os
import sys
import unittest
import uuid
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from app import create_app
from app.collector import CollectLimiter

DATABASE_URL = os.getenv("TEST_DATABASE_URL")
REDIS_URL = os.getenv("TEST_REDIS_URL")
ORIGIN = "https://portfolio.example"
HEADERS = {"Origin": ORIGIN, "User-Agent": "Mozilla/5.0 Example Browser"}


@unittest.skipUnless(DATABASE_URL, "set TEST_DATABASE_URL to run collector integration tests")
class CollectorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = "rta_collect_test_" + uuid.uuid4().hex
        cls.admin = psycopg2.connect(DATABASE_URL)
        cls.admin.autocommit = True
        with cls.admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(cls.schema)))
        try:
            cls.app = create_app({"TESTING": True, "START_HUB": False, "REDIS_URL": "",
                                  "DATABASE_URL": make_dsn(DATABASE_URL, options=f"-c search_path={cls.schema}"),
                                  "COLLECT_ORIGINS": (ORIGIN,), "COLLECT_RATE_PER_MIN": 100})
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
        cls.app.extensions["close_resources"]()
        cls.remove_schema()

    def setUp(self):
        self.client = self.app.test_client()
        with self.database.connection() as conn, conn.cursor() as cursor:
            cursor.execute("TRUNCATE events, event_rollup_minute RESTART IDENTITY")
        self.app.extensions["collect_limiter"].local.clear()
        self.app.extensions["collect_limiter"].limit = 100

    def event(self, **changes):
        return {"event_id": uuid.uuid4().hex, "type": "page_view", "user_id": "v_" + "f" * 32,
                "source": "wikipedia", "props": {"path": "/work", "title": "Work", "ref_source": "linkedin"},
                **changes}

    def post(self, events, headers=None, content_type="text/plain"):
        return self.client.post("/api/collect", data=json.dumps({"events": events}),
                                content_type=content_type, headers=headers or HEADERS)

    def test_forces_site_source_and_deduplicates_browser_retries(self):
        event = self.event()
        response = self.post([event])
        self.assertEqual((response.status_code, response.json), (202, {"accepted": 1}))
        self.assertEqual(response.headers["Access-Control-Allow-Origin"], ORIGIN)
        self.assertEqual(self.post([event]).json, {"accepted": 0})
        self.assertEqual(self.client.get("/api/events/recent?source=wikipedia").json, [])
        recent = self.client.get("/api/events/recent?source=site").json
        self.assertEqual(len(recent), 1)
        self.assertEqual((recent[0]["source"], recent[0]["props"]["ref_source"]), ("site", "linkedin"))

    def test_origin_cors_bot_and_payload_guards(self):
        script = self.client.get("/rta.js")
        self.assertEqual(script.status_code, 200)
        self.assertIn(b"sendBeacon", script.get_data())
        script.close()
        preflight = self.client.options("/api/collect", headers=HEADERS)
        self.assertEqual(preflight.status_code, 204)
        self.assertEqual(preflight.headers["Access-Control-Allow-Origin"], ORIGIN)
        denied = self.post([self.event()], {"Origin": "https://untrusted.example", "User-Agent": HEADERS["User-Agent"]})
        self.assertEqual(denied.status_code, 403)
        self.assertNotIn("Access-Control-Allow-Origin", denied.headers)
        self.assertEqual(self.client.options("/api/collect", headers={"Origin": "https://untrusted.example"}).status_code, 403)
        self.assertEqual(self.post([self.event()], {**HEADERS, "User-Agent": "Googlebot"}).status_code, 204)
        self.assertEqual(self.post([self.event()] * 21).status_code, 413)
        self.assertEqual(self.post([self.event(props={"path": "/x?token=secret", "title": "X", "ref_source": "direct"})]).status_code, 400)
        self.assertEqual(self.post([self.event(props={"path": "/", "title": "X", "ref_source": "direct", "email": "x@y"})]).status_code, 400)
        self.assertEqual(self.post([self.event(type="other")]).status_code, 400)
        self.assertEqual(self.post([self.event(user_id="somebody@example.com")]).status_code, 400)
        self.assertEqual(self.post([self.event(event_id="a personal identifier")]).status_code, 400)
        self.assertEqual(self.post([self.event()], content_type="application/xml").status_code, 415)
        self.assertEqual(self.client.post("/api/collect", data=b"x" * (96 * 1024 + 1),
                                          content_type="text/plain", headers=HEADERS).status_code, 413)
        self.assertEqual(self.client.get("/api/events/recent?source=site").json, [])

    def test_rate_limit_and_all_site_types(self):
        self.app.extensions["collect_limiter"].limit = 3
        events = [self.event(), self.event(type="link_click", props={"path": "/", "ref_source": "direct", "target": "github"}),
                  self.event(type="engaged", props={"path": "/", "ref_source": "direct", "seconds": 30})]
        self.assertEqual(self.post(events).json, {"accepted": 3})
        self.assertEqual(self.post([self.event()]).status_code, 202)
        self.assertEqual(self.post([self.event()]).status_code, 202)
        self.assertEqual(self.post([self.event()]).status_code, 429)
        recent = self.client.get("/api/events/recent?source=site").json
        self.assertEqual(len(recent), 5)
        self.assertEqual({item["type"] for item in recent}, {"page_view", "link_click", "engaged"})


@unittest.skipUnless(REDIS_URL, "set TEST_REDIS_URL to run shared rate-limit test")
class SharedLimiterTests(unittest.TestCase):
    def test_limit_shared_between_instances_and_keys_expire(self):
        from redis import Redis
        namespace = "rta_collect_rate_test_" + uuid.uuid4().hex
        client = Redis.from_url(REDIS_URL)
        class Store:
            pass
        store = Store()
        store.client = client
        try:
            first = CollectLimiter(store, namespace, 2)
            second = CollectLimiter(store, namespace, 2)
            self.assertTrue(first.allowed("203.0.113.1"))
            self.assertTrue(second.allowed("203.0.113.1"))
            self.assertFalse(first.allowed("203.0.113.1"))
            self.assertTrue(second.allowed("203.0.113.2"))
            keys = list(client.scan_iter(f"{namespace}:*"))
            self.assertEqual(len(keys), 2)
            self.assertTrue(all(client.ttl(key) > 0 for key in keys))
            self.assertTrue(all(b"203.0.113" not in key for key in keys))
        finally:
            keys = list(client.scan_iter(f"{namespace}:*"))
            if keys:
                client.delete(*keys)
            client.close()
