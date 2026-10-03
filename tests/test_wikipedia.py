import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import aiohttp
from aiohttp import web

from app.validation import validate
from connectors.wikipedia import Checkpoint, Forwarder, IngestRejected, SSEParser, consume_sse, replay, to_event

FIXTURE = Path(__file__).resolve().parents[1] / "connectors/fixtures/wikimedia_recentchange_sample.jsonl"


def record(index=0):
    return {"meta": {"id": f"mock-{index}", "dt": "2026-10-03T12:00:00Z", "domain": "en.wikipedia.org"},
            "type": "edit", "user": "Editor", "wiki": "enwiki", "title": "Example",
            "bot": False, "length": {"old": 10, "new": 5}, "server_name": "en.wikipedia.org"}


class WikipediaUnitTests(unittest.TestCase):
    def test_sse_multiline_comments_ids_and_retry(self):
        parser = SSEParser()
        frame = None
        for line in [b"\xef\xbb\xbf: heartbeat\r\n", b"retry: 60000\n", b"id: cursor-1\n",
                     b"data: {\n", b'data: "a": 1}\n', b"\n"]:
            frame = parser.feed(line)
        self.assertEqual(json.loads(frame["data"]), {"a": 1})
        self.assertEqual(frame["id"], "cursor-1")
        self.assertEqual(parser.retry_s, 60)
        parser.feed(b"id: bad\x00id\n")
        parser.feed(b"retry: nope\n")
        parser.feed(b"data: {}\n")
        self.assertEqual(parser.feed(b"\n")["id"], "cursor-1")
        parser.feed(b"id:\n")
        parser.feed(b"data: {}\n")
        self.assertEqual(parser.feed(b"\n")["id"], "")

    def test_incomplete_sse_event_is_not_dispatched(self):
        parser = SSEParser()
        self.assertIsNone(parser.feed(b"id: 1\n"))
        self.assertIsNone(parser.feed(b"data: {}\n"))

    def test_normalization_and_all_recorded_events_match_contract(self):
        event = to_event(record())
        self.assertEqual(event["event_id"], "wm:mock-0")
        self.assertEqual(event["value"], 5)
        self.assertEqual(event["props"]["delta"], -5)
        self.assertEqual(event["props"]["bot"], "human")
        self.assertEqual(event["props"]["url"], "https://en.wikipedia.org/wiki/Example")
        for line in FIXTURE.read_text().splitlines():
            event = to_event(json.loads(line))
            if event:
                self.assertEqual(validate(event)["source"], "wikipedia")

    def test_canary_filter_and_identity_fallback(self):
        self.assertIsNone(to_event({"meta": {"domain": "canary"}}))
        sample = record()
        del sample["meta"]["id"]
        sample["id"] = 123
        self.assertEqual(to_event(sample)["event_id"], "wm:enwiki:123")
        del sample["id"]
        self.assertEqual(to_event(sample)["event_id"], to_event(sample)["event_id"])

    def test_checkpoint_persistence_lock_and_stream_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cursor.json"
            checkpoint = Checkpoint(path, "https://stream.example/changes")
            try:
                checkpoint.save("cursor-70")
                with self.assertRaises(BlockingIOError):
                    Checkpoint(path, "https://stream.example/changes")
            finally:
                checkpoint.close()
            restored = Checkpoint(path, "https://stream.example/changes")
            self.assertEqual(restored.last_id, "cursor-70")
            restored.close()
            with self.assertRaises(ValueError):
                Checkpoint(path, "https://different.example/changes")

    def test_bounded_buffer_drops_oldest_queued_events(self):
        forwarder = Forwarder("http://localhost", batch=2, max_buffer=3)
        for index in range(5):
            forwarder.put(to_event(record(index)), str(index))
        self.assertEqual(forwarder.buffered, 3)
        self.assertEqual(forwarder.dropped, 2)
        self.assertEqual([cursor for _, cursor in forwarder.buf], ["2", "3", "4"])
        forwarder.inflight = [forwarder.buf.popleft(), forwarder.buf.popleft()]
        forwarder.put(to_event(record(5)), "5")
        self.assertEqual([cursor for _, cursor in forwarder.inflight], ["2", "3"])
        self.assertEqual(forwarder.buffered, 3)


class WikipediaHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.committed = set()
        self.posts = []
        self.cursors = []
        self.failures = 0
        self.commit_before_failure = False
        self.reject_status = None
        self.gate = None
        self.post_started = asyncio.Event()
        app = web.Application()
        app.router.add_get("/stream", self.stream)
        app.router.add_post("/api/events", self.ingest)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.api = f"http://127.0.0.1:{self.runner.addresses[0][1]}"
        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self):
        if self.gate:
            self.gate.set()
        await self.session.close()
        await self.runner.cleanup()
        self.directory.cleanup()

    async def ingest(self, request):
        body = await request.json()
        self.posts.append(body["events"])
        self.post_started.set()
        if self.gate:
            await self.gate.wait()
        if self.reject_status:
            return web.json_response({"error": "rejected"}, status=self.reject_status)
        failed = self.failures > 0
        accepted = 0
        if not failed or self.commit_before_failure:
            for event in body["events"]:
                if event["event_id"] not in self.committed:
                    self.committed.add(event["event_id"])
                    accepted += 1
        if failed:
            self.failures -= 1
            return web.json_response({"error": "temporarily unavailable"}, status=503)
        return web.json_response({"accepted": accepted}, status=202)

    async def stream(self, request):
        cursor = request.headers.get("Last-Event-ID", "0")
        self.cursors.append(cursor)
        # Timestamp-style resume can overlap; repeat one event on subsequent pulls.
        start = max(0, int(cursor) - (1 if int(cursor) else 0))
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(b"retry: 50\n\n")
        for index in range(start, min(start + 70, 200)):
            data = f"id: {index + 1}\ndata: {json.dumps(record(index))}\n\n"
            await response.write(data.encode())
        await response.write_eof()
        return response

    async def wait_until(self, predicate, timeout=4):
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0.01)

    async def test_disconnect_resume_and_overlapping_events_are_deduplicated(self):
        checkpoint = Checkpoint(Path(self.directory.name) / "resume.json", self.api + "/stream")
        forwarder = Forwarder(self.api, batch=20, flush_ms=10, checkpoint=checkpoint)
        sender = asyncio.create_task(forwarder.run(self.session))
        consumer = asyncio.create_task(consume_sse(self.api + "/stream", forwarder, self.session))
        try:
            await self.wait_until(lambda: len(self.committed) == 200)
            self.assertEqual(self.cursors[:3], ["0", "70", "139"])
            self.assertEqual(self.committed, {f"wm:mock-{index}" for index in range(200)})
        finally:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
            forwarder.close()
            await asyncio.wait_for(sender, 3)
            checkpoint.close()
        self.assertEqual(json.loads(checkpoint.path.read_text())["last_id"], "200")
        self.assertEqual(forwarder.inserted, 200)
        self.assertEqual(forwarder.dropped, 0)

    async def test_checkpoint_waits_for_acknowledgement(self):
        checkpoint = Checkpoint(Path(self.directory.name) / "ack.json", self.api + "/stream")
        forwarder = Forwarder(self.api, batch=1, checkpoint=checkpoint)
        self.gate = asyncio.Event()
        forwarder.put(to_event(record()), "cursor-1")
        sender = asyncio.create_task(forwarder.run(self.session))
        try:
            await asyncio.wait_for(self.post_started.wait(), 2)
            self.assertFalse(checkpoint.path.exists())
            self.gate.set()
            forwarder.close()
            await asyncio.wait_for(sender, 2)
            self.assertEqual(checkpoint.last_id, "cursor-1")
        finally:
            self.gate.set()
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)
            checkpoint.close()

    async def test_committed_batch_with_lost_ack_retries_same_ids(self):
        self.failures = 1
        self.commit_before_failure = True
        forwarder = Forwarder(self.api, batch=2, flush_ms=10)
        for index in range(2):
            forwarder.put(to_event(record(index)))
        forwarder.close()
        with self.assertLogs("wikipedia", level="WARNING"):
            await asyncio.wait_for(forwarder.run(self.session), 3)
        self.assertEqual(self.posts[0], self.posts[1])
        self.assertEqual(len(self.committed), 2)
        self.assertEqual(forwarder.sent, 2)
        self.assertEqual(forwarder.inserted, 0)  # retry acknowledged existing events
        self.assertEqual(forwarder.retries, 1)

    async def test_rejected_batch_fails_without_acknowledging(self):
        self.reject_status = 400
        checkpoint = Checkpoint(Path(self.directory.name) / "reject.json", self.api + "/stream")
        forwarder = Forwarder(self.api, batch=1, checkpoint=checkpoint)
        forwarder.put(to_event(record()), "1")
        try:
            with self.assertRaises(IngestRejected):
                await forwarder.run(self.session)
            self.assertFalse(checkpoint.path.exists())
            self.assertEqual(forwarder.sent, 0)
        finally:
            checkpoint.close()

    async def test_replay_once_uses_fresh_timestamps_and_unique_ids(self):
        forwarder = Forwarder(self.api, batch=200, max_buffer=400)
        with patch("connectors.wikipedia.asyncio.sleep", new=self.no_sleep):
            await replay(FIXTURE, 1000, forwarder)
            await replay(FIXTURE, 1000, forwarder)
        self.assertEqual(forwarder.received, 400)
        ids = [event["event_id"] for event, _ in forwarder.buf]
        self.assertEqual(len(set(ids)), 400)
        self.assertTrue(all(event["props"]["replay"] for event, _ in forwarder.buf))
        forwarder.close()
        await asyncio.wait_for(forwarder.run(self.session), 3)
        self.assertEqual(len(self.committed), 400)

    @staticmethod
    async def no_sleep(seconds):
        pass
