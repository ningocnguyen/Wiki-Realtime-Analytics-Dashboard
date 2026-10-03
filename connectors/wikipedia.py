"""Consume Wikimedia SSE or replay JSONL, forwarding bounded batches to Flask."""
import argparse
import asyncio
import fcntl
import hashlib
import json
import logging
import math
import os
import random
import signal
import ssl
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse

import aiohttp
import certifi

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "RealTimeAnalyticsDashboard/0.1 (learning project; Python aiohttp)"
log = logging.getLogger("wikipedia")


class SSEParser:
    """Parse LF/CRLF SSE lines; IDs persist across messages and EOF never dispatches."""
    def __init__(self, last_id=None):
        self.last_id = last_id
        self.retry_s = 1.0
        self.data = []
        self.event = "message"
        self.first_line = True

    def feed(self, raw):
        line = raw.decode("utf-8", "replace").rstrip("\r\n")
        if self.first_line:
            line = line.removeprefix("\ufeff")
            self.first_line = False
        if line == "":
            frame = {"data": "\n".join(self.data), "id": self.last_id, "event": self.event} if self.data else None
            self.data = []
            self.event = "message"
            return frame
        if line.startswith(":"):
            return None
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "data":
            self.data.append(value)
        elif field == "id" and "\x00" not in value:
            self.last_id = value
        elif field == "event":
            self.event = value or "message"
        elif field == "retry" and value.isascii() and value.isdigit():
            self.retry_s = max(int(value) / 1000, 0.05)
        return None


class Checkpoint:
    """Atomically store only acknowledged cursors; prevent simultaneous file owners."""
    def __init__(self, path, url):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.url = url
        self.last_id = None
        self.lock = self.path.with_name(self.path.name + ".lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if self.path.exists():
                state = json.loads(self.path.read_text())
                if state.get("url") != url or not isinstance(state.get("last_id"), str):
                    raise ValueError("checkpoint is invalid or belongs to a different stream")
                self.last_id = state["last_id"]
        except BaseException:
            self.close()
            raise

    def save(self, last_id):
        if last_id is None:
            return
        temporary = self.path.with_name(self.path.name + ".tmp")
        with temporary.open("w") as handle:
            json.dump({"url": self.url, "last_id": last_id}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(self.path)
        self.last_id = last_id

    def close(self):
        self.lock.close()


def clipped(value, limit):
    if not isinstance(value, str):
        raise ValueError("expected text in recentchange record")
    value = value.replace("\x00", "")[:limit]
    value.encode("utf-8")
    return value


def to_event(record):
    """Use upstream identity for retry deduplication; SSE ID is a separate cursor."""
    if not isinstance(record, dict):
        raise ValueError("recentchange must be an object")
    meta = record.get("meta") or {}
    if not isinstance(meta, dict):
        raise ValueError("meta must be an object")
    if meta.get("domain") == "canary":
        return None
    wiki = clipped(record.get("wiki", "unknown"), 128)
    identity = meta.get("id")
    if identity:
        identity = "wm:" + clipped(identity, 200)
    elif isinstance(record.get("id"), int):
        identity = f"wm:{wiki}:{record['id']}"
    else:
        identity = "wm:sha256:" + hashlib.sha256(
            json.dumps(record, sort_keys=True, allow_nan=False).encode("utf-8")
        ).hexdigest()
    length = record.get("length") or {}
    if not isinstance(length, dict):
        raise ValueError("length must be an object")
    old, new = length.get("old") or 0, length.get("new") or 0
    if any(not isinstance(n, int) or isinstance(n, bool) for n in (old, new)):
        raise ValueError("length values must be integers")
    delta = new - old
    title = clipped(record.get("title") or "", 200)
    url = record.get("title_url") or meta.get("uri")
    if not url and record.get("server_name"):
        url = f"https://{record['server_name']}/wiki/{quote(title.replace(' ', '_'))}"
    url = clipped(url, 2048) if url else None
    if url and urlparse(url).scheme not in ("http", "https"):
        url = None
    stamp = meta.get("dt") or record.get("timestamp")
    if isinstance(stamp, str):
        parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        parsed = parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    elif isinstance(stamp, (int, float)) and not isinstance(stamp, bool):
        parsed = datetime.fromtimestamp(stamp, timezone.utc)
    else:
        raise ValueError("record has no valid timestamp")
    props = {"wiki": wiki, "bot": "bot" if record.get("bot") else "human", "title": title,
             "url": url, "delta": delta, "ns": record.get("namespace")}
    if record.get("log_type"):
        props["log_type"] = clipped(record["log_type"], 64)
    if len(json.dumps(props, ensure_ascii=False, allow_nan=False).encode("utf-8")) > 4096:
        raise ValueError("record properties exceed ingest limit")
    event_type = clipped(record.get("type") or "unknown", 64)
    user = clipped(record.get("user") or "anonymous", 128)
    if not event_type.strip() or not user.strip() or not math.isfinite(delta):
        raise ValueError("invalid event fields")
    return {"event_id": identity, "source": "wikipedia", "type": event_type,
            "user_id": user, "value": abs(delta), "props": props,
            "ts": parsed.astimezone(timezone.utc).isoformat()}


class IngestRejected(RuntimeError):
    pass


class Forwarder:
    def __init__(self, api, batch=200, flush_ms=250, max_buffer=20000, checkpoint=None):
        if not 1 <= batch <= 1000 or max_buffer < batch or flush_ms <= 0:
            raise ValueError("batch must be 1–1000, buffer >= batch, and flush-ms > 0")
        self.api = api.rstrip("/") + "/api/events"
        self.batch = batch
        self.flush_s = flush_ms / 1000
        self.max_buffer = max_buffer
        self.checkpoint = checkpoint
        self.last_received_id = checkpoint.last_id if checkpoint else None
        self.buf = deque()
        self.inflight = []
        self.wake = asyncio.Event()
        self.closing = False
        self.received = self.sent = self.inserted = self.dropped = self.skipped = self.retries = 0

    @property
    def buffered(self):
        return len(self.buf) + len(self.inflight)

    def put(self, event, cursor=None):
        self.received += 1
        if cursor is not None:
            self.last_received_id = cursor
        if self.buffered >= self.max_buffer:
            self.dropped += 1
            if self.buf:
                self.buf.popleft()  # in-flight requests are pinned for safe retry
            else:
                return
        self.buf.append((event, cursor))
        if len(self.buf) >= self.batch:
            self.wake.set()

    def close(self):
        self.closing = True
        self.wake.set()

    async def run(self, session):
        attempt = 0
        while not self.closing or self.buffered:
            if not self.inflight:
                if not self.closing and len(self.buf) < self.batch:
                    try:
                        await asyncio.wait_for(self.wake.wait(), self.flush_s)
                    except asyncio.TimeoutError:
                        pass
                self.wake.clear()
                self.inflight = [self.buf.popleft() for _ in range(min(self.batch, len(self.buf)))]
            if not self.inflight:
                continue
            retry_after = 0
            try:
                async with session.post(self.api, json={"events": [event for event, _ in self.inflight]},
                                        timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 429 or response.status >= 500:
                        hint = response.headers.get("Retry-After", "0")
                        retry_after = min(float(hint), 60) if hint.isdigit() else 0
                        raise aiohttp.ClientResponseError(response.request_info, (), status=response.status)
                    if response.status != 202:
                        raise IngestRejected(f"ingest rejected batch: HTTP {response.status}; {(await response.text())[:240]}")
                    result = await response.json()
                    accepted = result.get("accepted") if isinstance(result, dict) else None
                    if isinstance(accepted, bool) or not isinstance(accepted, int) or not 0 <= accepted <= len(self.inflight):
                        raise IngestRejected("ingest returned an invalid acknowledgement")
                if self.checkpoint:
                    self.checkpoint.save(self.inflight[-1][1])
                self.sent += len(self.inflight)
                self.inserted += accepted
                self.inflight = []
                attempt = 0
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                self.retries += 1
                delay = max(retry_after, min(0.5 * 2 ** min(attempt, 6), 15) * random.uniform(0.5, 1.5))
                attempt += 1
                log.warning("ingest unavailable (%s), retry in %.2fs", exc, delay)
                await asyncio.sleep(delay)


async def consume_sse(url, forwarder, session, user_agent=USER_AGENT):
    attempt = 0
    retry_s = 1.0
    while True:
        headers = {"Accept": "text/event-stream", "User-Agent": user_agent}
        if forwarder.last_received_id:
            headers["Last-Event-ID"] = forwarder.last_received_id
        received_before = forwarder.received
        try:
            async with session.get(url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=60)) as response:
                response.raise_for_status()
                if response.content_type != "text/event-stream":
                    raise aiohttp.ClientError("upstream did not return an SSE stream")
                log.info("connected to Wikimedia stream (resuming=%s)", bool(headers.get("Last-Event-ID")))
                parser = SSEParser(forwarder.last_received_id)
                parser.retry_s = retry_s
                async for raw in response.content:
                    frame = parser.feed(raw)
                    retry_s = parser.retry_s
                    if not frame:
                        continue
                    if frame["event"] == "error":
                        raise aiohttp.ClientError("upstream SSE error event")
                    if frame["event"] != "message":
                        continue
                    try:
                        event = to_event(json.loads(frame["data"]))
                    except (ValueError, TypeError, OverflowError, OSError):
                        forwarder.skipped += 1
                        log.warning("skipped malformed recentchange record")
                        continue
                    if event:
                        forwarder.put(event, frame["id"])
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            log.warning("stream disconnected: %s", exc)
        if forwarder.received > received_before:
            attempt = 0
        # Positive jitter never reconnects earlier than the upstream retry hint.
        delay = min(retry_s * 2 ** min(attempt, 10), max(30, retry_s)) * random.uniform(1, 1.25)
        attempt += 1
        log.info("reconnecting in %.2fs", delay)
        await asyncio.sleep(delay)


async def replay(path, rate, forwarder, loop=False):
    records = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    if not records:
        raise ValueError("replay fixture is empty")
    run_id = uuid.uuid4().hex
    index = 0
    while True:
        for record in records:
            event = to_event(record)
            if event:
                event["event_id"] = f"replay:{run_id}:{index}"
                event["ts"] = datetime.now(timezone.utc).isoformat()
                event["props"]["replay"] = True
                forwarder.put(event)
                index += 1
                await asyncio.sleep(1 / rate)
        if not loop:
            return


async def report(forwarder):
    while True:
        await asyncio.sleep(10)
        log.info("received=%d acknowledged=%d inserted=%d buffered=%d dropped=%d skipped=%d retries=%d",
                 forwarder.received, forwarder.sent, forwarder.inserted, forwarder.buffered,
                 forwarder.dropped, forwarder.skipped, forwarder.retries)


async def main(args):
    checkpoint = Checkpoint(args.checkpoint, args.url) if not args.replay and not args.no_checkpoint else None
    try:
        forwarder = Forwarder(args.api, args.batch, args.flush_ms, args.max_buffer, checkpoint)
        stopping = asyncio.Event()
        event_loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            event_loop.add_signal_handler(sig, stopping.set)
        if args.duration:
            event_loop.call_later(args.duration, stopping.set)
        connector = aiohttp.TCPConnector(ssl=ssl.create_default_context(cafile=certifi.where()))
        async with aiohttp.ClientSession(connector=connector) as session:
            producer = asyncio.create_task(replay(args.replay, args.rate, forwarder, args.loop) if args.replay
                                           else consume_sse(args.url, forwarder, session, args.user_agent))
            sender = asyncio.create_task(forwarder.run(session))
            reporter = asyncio.create_task(report(forwarder))
            stop_task = asyncio.create_task(stopping.wait())
            tasks = [producer, sender, reporter, stop_task]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task is not stop_task:
                        task.result()  # fail visibly on rejected batches or bad fixtures
                producer.cancel()
                await asyncio.gather(producer, return_exceptions=True)
                forwarder.close()
                try:
                    await asyncio.wait_for(asyncio.shield(sender), args.drain_seconds)
                except asyncio.TimeoutError:
                    log.warning("shutdown drain timed out; %d events remain unacknowledged", forwarder.buffered)
                    raise RuntimeError("connector stopped with unacknowledged events") from None
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            log.info("finished: received=%d acknowledged=%d inserted=%d dropped=%d skipped=%d buffered=%d",
                     forwarder.received, forwarder.sent, forwarder.inserted, forwarder.dropped,
                     forwarder.skipped, forwarder.buffered)
    finally:
        if checkpoint:
            checkpoint.close()


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default=os.getenv("INGEST_API_URL", "http://localhost:5050"))
    parser.add_argument("--url", default=os.getenv("WIKI_STREAM_URL", STREAM_URL))
    parser.add_argument("--user-agent", default=os.getenv("WIKI_USER_AGENT", USER_AGENT))
    parser.add_argument("--batch", type=int, default=200)
    parser.add_argument("--flush-ms", type=int, default=250)
    parser.add_argument("--max-buffer", type=int, default=20000)
    parser.add_argument("--checkpoint", default=".run/wikipedia-checkpoint.json")
    parser.add_argument("--no-checkpoint", action="store_true")
    parser.add_argument("--replay", help="JSONL fixture; replay once with fresh timestamps")
    parser.add_argument("--loop", action="store_true", help="repeat the replay fixture continuously")
    parser.add_argument("--rate", type=float, default=30, help="replay events per second")
    parser.add_argument("--duration", type=float, help="stop after this many seconds")
    parser.add_argument("--drain-seconds", type=float, default=10)
    args = parser.parse_args()
    if not math.isfinite(args.rate) or args.rate <= 0 or args.drain_seconds <= 0 or not math.isfinite(args.drain_seconds):
        parser.error("rate and drain-seconds must be finite positive numbers")
    if args.duration is not None and (not math.isfinite(args.duration) or args.duration <= 0):
        parser.error("duration must be a finite positive number")
    if args.loop and not args.replay:
        parser.error("--loop requires --replay")
    return args


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main(arguments()))
    except (ValueError, OSError, RuntimeError) as exc:
        log.error("connector stopped: %s", exc)
        raise SystemExit(1)
