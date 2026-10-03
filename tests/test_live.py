import json
import os
import queue
import sys
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn
from redis.exceptions import ConnectionError as RedisConnectionError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app import create_app, stats
from app.hub import Client, Hub
from app.metrics import Metrics

DATABASE_URL = os.getenv("TEST_DATABASE_URL")
REDIS_URL = os.getenv("TEST_REDIS_URL")


class HubUnitTests(unittest.TestCase):
    def test_slow_consumer_is_bounded_and_can_request_resync(self):
        metrics = Metrics()
        client = Client("wikipedia", 1, metrics)
        client.offer("first")
        client.offer("second")
        self.assertEqual(client.q.qsize(), 1)
        self.assertEqual(client.dropped, 1)
        self.assertEqual(client.take_resync(), "slow_consumer")
        self.assertTrue(client.q.empty())
        client.offer("fresh")
        self.assertEqual(client.q.get_nowait(), "fresh")

    def test_source_filtering_and_slow_client_do_not_block_fast_client(self):
        class Store:
            namespace = "unit"

        metrics = Metrics()
        hub = Hub(Store(), metrics, lambda: {},
                  {"HUB_INBOX_SIZE": 10, "WS_QUEUE_SIZE": 1, "BROADCAST_WINDOW_MS": 10})
        fast = hub.register("wikipedia")
        slow = hub.register("wikipedia")
        demo = hub.register("demo")
        for index in range(2):
            hub.flush([json.dumps({"events": [{"source": "wikipedia", "event_id": str(index)}]})])
            self.assertEqual(json.loads(fast.q.get_nowait())["events"][0]["event_id"], str(index))
        self.assertEqual(fast.dropped, 0)
        self.assertEqual(slow.dropped, 1)
        self.assertTrue(demo.q.empty())
        hub.flush([json.dumps({"events": [{"source": "demo", "event_id": "demo-event"}]})])
        self.assertEqual(json.loads(demo.q.get_nowait())["events"][0]["source"], "demo")


@unittest.skipUnless(DATABASE_URL and REDIS_URL, "set TEST_DATABASE_URL and TEST_REDIS_URL for live integration tests")
class LiveIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = "rta_live_test_" + uuid.uuid4().hex
        cls.admin = psycopg2.connect(DATABASE_URL)
        cls.admin.autocommit = True
        with cls.admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(cls.schema)))
        cls.app = create_app({"TESTING": True, "START_HUB": False, "DB_POOL_MAX": 8,
                              "DATABASE_URL": make_dsn(DATABASE_URL, options=f"-c search_path={cls.schema}"),
                              "REDIS_URL": REDIS_URL, "REDIS_NAMESPACE": cls.schema})
        cls.database = cls.app.extensions["database"]
        cls.store = cls.app.extensions["live"]
        cls.store.client.ping()

    @classmethod
    def clear_redis(cls):
        keys = list(cls.store.client.scan_iter(cls.schema + ":*"))
        if keys:
            cls.store.client.delete(*keys)

    @classmethod
    def tearDownClass(cls):
        cls.clear_redis()
        cls.app.extensions["close_resources"]()
        with cls.admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(cls.schema)))
        cls.admin.close()

    def setUp(self):
        self.clear_redis()
        with self.database.connection() as conn, conn.cursor() as cursor:
            cursor.execute("TRUNCATE events, event_rollup_minute, live_cache_state, live_cache_pending RESTART IDENTITY")
        self.client = self.app.test_client()

    def event(self, **changes):
        return {"source": "wikipedia", "event_id": uuid.uuid4().hex, "type": "edit", "user_id": "editor",
                "props": {"wiki": "enwiki", "bot": "human"}, **changes}

    def post(self, event):
        response = self.client.post("/api/events", json=event)
        self.assertEqual(response.status_code, 202)
        return response.json["accepted"]

    def test_current_minute_clock_skew_does_not_disable_cache(self):
        self.store.rebuild(self.database)
        timestamp = datetime.now(timezone.utc).replace(second=59, microsecond=999999)
        self.post(self.event(ts=timestamp.isoformat()))
        self.assertFalse(self.store.state(self.database)["dirty"])
        summary = self.client.get("/api/stats/summary").json
        self.assertEqual(summary["stats_backend"], "redis")
        self.assertEqual(summary["sources"]["wikipedia"]["events_today"], 1)
        self.assertTrue(self.store.rebuild(self.database)["ready"])
        self.assertEqual(self.client.get("/api/stats/summary").json["sources"]["wikipedia"]["events_today"], 1)

    def test_future_minute_preserves_postgres_fallback(self):
        self.store.rebuild(self.database)
        self.post(self.event(ts=(datetime.now(timezone.utc) + timedelta(days=1)).isoformat()))
        self.assertTrue(self.store.state(self.database)["dirty"])
        self.assertEqual(self.client.get("/api/stats/summary").json["stats_backend"], "postgres")
        self.assertFalse(self.store.rebuild(self.database)["ready"])
        with self.database.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT dirty FROM live_cache_state WHERE name = %s", (self.store.namespace,))
            self.assertFalse(cursor.fetchone()[0])  # Clock skew does not require another rebuild.

    def test_rebuild_restores_existing_counts_dimensions_and_recent_feed(self):
        self.post(self.event())
        self.post(self.event(user_id="another"))
        self.post(self.event(source="demo"))
        self.assertEqual(self.client.get("/api/stats/summary").json["stats_backend"], "postgres")
        result = self.store.rebuild(self.database)
        self.assertTrue(result["ready"])
        self.assertEqual(result["events_rebuilt"], 3)
        summary = self.client.get("/api/stats/summary").json
        self.assertEqual(summary["stats_backend"], "redis")
        self.assertEqual(summary["sources"]["wikipedia"]["events_today"], 2)
        self.assertEqual(summary["sources"]["wikipedia"]["unique_users_today"], 2)
        self.assertEqual(summary["sources"]["demo"]["events_today"], 1)
        self.assertEqual(self.client.get("/api/stats/dims?key=wiki").json, [{"value": "enwiki", "count": 2}])
        recent = self.client.get("/api/events/recent").json
        self.assertEqual(len(recent), 2)
        self.assertGreater(recent[0]["id"], recent[1]["id"])

    def test_retries_do_not_increment_redis_or_publish_again(self):
        self.store.rebuild(self.database)
        with self.store.client.pubsub() as subscriber:
            subscriber.subscribe(self.store.channel)
            subscriber.get_message(timeout=1)
            event = self.event()
            self.assertEqual(self.post(event), 1)
            frame = subscriber.get_message(timeout=1)
            self.assertEqual(json.loads(frame["data"])["events"][0]["event_id"], event["event_id"])
            self.assertEqual(self.post(event), 0)
            self.assertIsNone(subscriber.get_message(timeout=0.1))
        summary = self.client.get("/api/stats/summary").json
        self.assertEqual(summary["sources"]["wikipedia"]["events_today"], 1)

    def test_redis_failure_preserves_commit_and_falls_back_until_rebuild(self):
        self.store.rebuild(self.database)
        event = self.event()
        with patch.object(self.store, "record", side_effect=RedisConnectionError("simulated failure")), self.assertLogs("app.ingest", level="ERROR"):
            self.assertEqual(self.post(event), 1)
        self.assertEqual(self.post(event), 0)
        summary = self.client.get("/api/stats/summary").json
        self.assertEqual(summary["stats_backend"], "postgres")
        self.assertEqual(summary["sources"]["wikipedia"]["events_today"], 1)
        self.assertTrue(self.store.state(self.database)["dirty"])
        self.store.rebuild(self.database)
        self.assertFalse(self.store.state(self.database)["dirty"])
        self.assertEqual(self.client.get("/api/stats/summary").json["sources"]["wikipedia"]["events_today"], 1)

    def test_interrupted_post_commit_update_is_detected_by_pending_marker(self):
        self.store.rebuild(self.database)
        event = self.event()
        with patch.object(self.store, "record", side_effect=RuntimeError("simulated process interruption")):
            with self.assertRaises(RuntimeError):
                self.client.post("/api/events", json=event)
        self.assertTrue(self.store.state(self.database)["dirty"])
        self.assertEqual(self.post(event), 0)
        self.assertEqual(self.client.get("/api/stats/summary").json["stats_backend"], "postgres")
        self.store.rebuild(self.database)
        self.assertEqual(self.client.get("/api/stats/summary").json["sources"]["wikipedia"]["events_today"], 1)

    def test_missing_cache_and_long_dimension_windows_use_postgres(self):
        self.post(self.event(ts=(datetime.now(timezone.utc) - timedelta(minutes=181)).isoformat()))
        self.store.rebuild(self.database)
        self.assertEqual(self.client.get("/api/stats/dims?key=wiki&minutes=200").json,
                         [{"value": "enwiki", "count": 1}])
        self.store.client.delete(self.store.pointer)
        self.assertEqual(self.client.get("/api/stats/summary").json["stats_backend"], "postgres")
        self.assertEqual(len(self.client.get("/api/events/recent").json), 1)
        self.assertEqual(self.client.get("/api/stats/dims?key=invalid").status_code, 400)

    def test_redis_read_failure_falls_back(self):
        self.post(self.event())
        self.store.rebuild(self.database)
        with patch.object(self.store.client, "get", side_effect=RedisConnectionError("unavailable")), self.assertLogs(self.app.logger, level="WARNING"):
            response = self.client.get("/api/stats/summary")
        self.assertEqual(response.json["stats_backend"], "postgres")
        self.assertEqual(response.json["sources"]["wikipedia"]["events_today"], 1)

    def test_rebuild_coordinates_with_concurrent_ingestion(self):
        self.post(self.event())
        self.store.rebuild(self.database)
        rebuilding, release, write_done = threading.Event(), threading.Event(), threading.Event()
        original = self.store.record

        def gated(events, generation, publish=True):
            if not publish:
                rebuilding.set()
                if not release.wait(3):
                    raise RuntimeError("test rebuild gate timed out")
            return original(events, generation, publish)

        def write():
            with self.app.test_client() as client:
                response = client.post("/api/events", json=self.event())
            write_done.set()
            return response

        with patch.object(self.store, "record", side_effect=gated), ThreadPoolExecutor(max_workers=2) as executor:
            rebuild = executor.submit(self.store.rebuild, self.database)
            try:
                self.assertTrue(rebuilding.wait(2))
                writer = executor.submit(write)
                self.assertFalse(write_done.wait(0.1))
            finally:
                release.set()
            self.assertTrue(rebuild.result(timeout=3)["ready"])
            self.assertEqual(writer.result(timeout=3).status_code, 202)
        self.assertEqual(self.client.get("/api/stats/summary").json["sources"]["wikipedia"]["events_today"], 2)

    def test_independent_hubs_receive_the_same_source_filtered_publication(self):
        self.store.rebuild(self.database)
        config = {"HUB_INBOX_SIZE": 100, "WS_QUEUE_SIZE": 10, "BROADCAST_WINDOW_MS": 10}
        hubs = [Hub(self.store, Metrics(), lambda: stats.summary(self.database), config) for _ in range(2)]
        try:
            for hub in hubs:
                hub.start()
                self.assertTrue(hub.ready.wait(3))
            viewers = [hub.register("wikipedia") for hub in hubs]
            demo = hubs[1].register("demo")
            event = self.event()
            self.post(event)
            for viewer in viewers:
                frame = json.loads(viewer.q.get(timeout=2))
                self.assertEqual(frame["kind"], "events")
                self.assertEqual([e["event_id"] for e in frame["events"]], [event["event_id"]])
            while not demo.q.empty():
                self.assertNotEqual(json.loads(demo.q.get_nowait())["kind"], "events")
        finally:
            for hub in hubs:
                hub.close()

    def test_metrics_endpoint_reports_ingestion(self):
        self.post(self.event())
        response = self.client.get("/metrics")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"rta_events_ingested_total", response.data)
        self.assertIn(b"rta_ingest_seconds_count", response.data)

    def test_out_of_order_cache_updates_preserve_recent_event_order(self):
        self.store.rebuild(self.database)
        started, release = threading.Event(), threading.Event()
        original = self.store.record

        def delayed(events, generation, publish=True):
            if events[0]["event_id"] == "older":
                started.set()
                if not release.wait(3):
                    raise RuntimeError("test gate timed out")
            return original(events, generation, publish)

        def older():
            with self.app.test_client() as client:
                return client.post("/api/events", json=self.event(event_id="older"))

        with patch.object(self.store, "record", side_effect=delayed), ThreadPoolExecutor(max_workers=1) as executor:
            first = executor.submit(older)
            try:
                self.assertTrue(started.wait(2))
                self.post(self.event(event_id="newer"))
            finally:
                release.set()
            self.assertEqual(first.result(timeout=3).status_code, 202)
        recent = self.client.get("/api/events/recent").json
        self.assertEqual([event["event_id"] for event in recent], ["newer", "older"])
        self.assertEqual(self.client.get("/api/stats/summary").json["sources"]["wikipedia"]["events_today"], 2)

    def test_recent_feed_is_capped_without_losing_counter_history(self):
        self.store.rebuild(self.database)
        response = self.client.post("/api/events", json={"events": [self.event() for _ in range(250)]})
        self.assertEqual(response.json, {"accepted": 250})
        recent = self.client.get("/api/events/recent?limit=200").json
        self.assertEqual(len(recent), 200)
        self.assertEqual([event["id"] for event in recent], sorted([event["id"] for event in recent], reverse=True))
        generation = self.store.state(self.database)["generation"]
        self.assertEqual(self.store.client.zcard(self.store.key(generation, "wikipedia:recent")), 200)
        self.assertEqual(self.client.get("/api/stats/summary").json["sources"]["wikipedia"]["events_today"], 250)
