"""One Redis subscriber per worker; serialize once per watched source."""
import json
import logging
import os
import queue
import socket
import threading
import time
import uuid

from redis.exceptions import RedisError

log = logging.getLogger(__name__)


class Client:
    def __init__(self, source, size, metrics):
        self.source = source
        self.q = queue.Queue(maxsize=size)
        self.metrics = metrics
        self.dropped = 0
        self.resync_reason = None

    def offer(self, frame):
        try:
            self.q.put_nowait(frame)
        except queue.Full:
            self.dropped += 1
            self.metrics.drops.labels(queue="client").inc()
            self.resync_reason = "slow_consumer"

    def take_resync(self):
        reason = self.resync_reason
        if reason:
            self.resync_reason = None
            while True:
                try:
                    self.q.get_nowait()
                except queue.Empty:
                    break
        return reason


class Hub:
    def __init__(self, store, metrics, summary, config):
        self.store, self.metrics, self.summary = store, metrics, summary
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.clients = set()
        self.lock = threading.Lock()
        self.inbox = queue.Queue(maxsize=config["HUB_INBOX_SIZE"])
        self.client_size = config["WS_QUEUE_SIZE"]
        self.window = config["BROADCAST_WINDOW_MS"] / 1000
        self.stopping = threading.Event()
        self.ready = threading.Event()
        self.threads = []
        self.heartbeat = f"{store.namespace}:ws:clients:{self.worker_id}"

    def register(self, source):
        client = Client(source, self.client_size, self.metrics)
        with self.lock:
            self.clients.add(client)
            self.metrics.clients.set(len(self.clients))
        return client

    def unregister(self, client):
        with self.lock:
            self.clients.discard(client)
            self.metrics.clients.set(len(self.clients))

    def mark_resync(self, reason):
        with self.lock:
            for client in self.clients:
                client.resync_reason = reason

    def start(self):
        if self.threads:
            return
        for target in (self._listen, self._flush, self._stats):
            thread = threading.Thread(target=target, daemon=True)
            self.threads.append(thread)
            thread.start()

    def close(self):
        self.stopping.set()
        for thread in self.threads:
            thread.join(timeout=3)
        try:
            self.store.client.delete(self.heartbeat)
        except RedisError:
            pass

    def _listen(self):
        while not self.stopping.is_set():
            try:
                with self.store.client.pubsub() as subscription:
                    subscription.subscribe(self.store.channel)
                    while not self.stopping.is_set():
                        message = subscription.get_message(timeout=1)
                        if not message:
                            continue
                        if message["type"] == "subscribe":
                            self.mark_resync("connection_recovery")
                            self.ready.set()
                        elif message["type"] == "message":
                            try:
                                self.inbox.put_nowait(message["data"])
                            except queue.Full:
                                self.metrics.drops.labels(queue="inbox").inc()
                                self.mark_resync("server_backpressure")
            except RedisError as error:
                self.ready.clear()
                self.mark_resync("connection_recovery")
                log.warning("Redis subscriber reconnecting: %s", error)
                self.stopping.wait(1)

    def flush(self, messages):
        by_source = {}
        for raw in messages:
            try:
                message = json.loads(raw)
                for event in message.get("events", []):
                    by_source.setdefault(event["source"], []).append(event)
            except (ValueError, TypeError, KeyError, AttributeError):
                log.warning("ignored malformed pub/sub frame")
        with self.lock:
            clients = list(self.clients)
        wanted = {client.source for client in clients}
        for source in wanted:
            events = by_source.get(source)
            if events:
                frame = json.dumps({"kind": "events", "events": events, "sent_at": int(time.time() * 1000)}, ensure_ascii=False)
                for client in clients:
                    if client.source == source:
                        client.offer(frame)

    def _flush(self):
        while not self.stopping.is_set():
            try:
                messages = [self.inbox.get(timeout=0.5)]
            except queue.Empty:
                continue
            deadline = time.monotonic() + self.window
            while (left := deadline - time.monotonic()) > 0:
                try:
                    messages.append(self.inbox.get(timeout=left))
                except queue.Empty:
                    break
            self.flush(messages)

    def _stats(self):
        while not self.stopping.wait(1):
            try:
                with self.lock:
                    clients = list(self.clients)
                self.store.client.set(self.heartbeat, len(clients), ex=5)
                if clients:
                    frame = json.dumps({"kind": "stats", **self.summary()})
                    for client in clients:
                        client.offer(frame)
            except Exception:
                log.exception("live statistics tick failed")
