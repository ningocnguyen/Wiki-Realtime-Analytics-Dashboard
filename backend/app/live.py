"""Redis live cache and pub/sub, with a coordinated PostgreSQL-backed rebuild."""
import hashlib
import json
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from redis import Redis

from .stats import payload, query
from .validation import SOURCES

DIM_KEYS = ("wiki", "bot", "ref_source", "path", "target")
MINUTE_RETENTION = 181


def dimension(value):
    if isinstance(value, str):
        return value[:128] if value else None
    if isinstance(value, (bool, int, float)):
        return json.dumps(value)[:128]
    return None


class LiveStore:
    def __init__(self, config):
        self.namespace = config["REDIS_NAMESPACE"]
        self.client = Redis.from_url(config["REDIS_URL"], decode_responses=True,
                                     socket_connect_timeout=2, socket_timeout=2)
        self.pointer = f"{self.namespace}:live:generation"
        self.channel = f"{self.namespace}:events"
        self.lock_id = int.from_bytes(hashlib.sha256(self.namespace.encode()).digest()[:8], "big", signed=True)

    def key(self, generation, suffix):
        return f"{self.namespace}:live:{generation}:{suffix}"

    def state(self, database):
        rows = query(database, """SELECT generation, dirty OR EXISTS(
            SELECT 1 FROM live_cache_pending WHERE name = %s) OR EXISTS(
            SELECT 1 FROM events WHERE occurred_at >= date_trunc('minute', now()) + interval '1 minute') AS dirty
            FROM live_cache_state WHERE name = %s""", (self.namespace, self.namespace))
        return rows[0] if rows else {"generation": None, "dirty": True}

    def valid_generation(self, database):
        state = self.state(database)
        if state["dirty"] or not state["generation"]:
            return None
        return state["generation"] if self.client.get(self.pointer) == state["generation"] else None

    def record(self, events, generation, publish=True):
        if not events:
            return
        pipeline = self.client.pipeline(transaction=False)
        if generation:
            counts, days, types, dims = Counter(), Counter(), Counter(), Counter()
            users, day_users, feeds = defaultdict(set), defaultdict(set), defaultdict(list)
            now = datetime.now(timezone.utc)
            current_minute = int(now.timestamp() // 60)
            for event in sorted(events, key=lambda item: item["id"]):
                occurred = datetime.fromisoformat(event["occurred_at"])
                minute = int(occurred.timestamp() // 60)
                src = event["source"]
                day = occurred.strftime("%Y%m%d")
                if minute >= current_minute - MINUTE_RETENTION + 1:
                    counts[(src, minute)] += 1
                    users[(src, minute)].add(event["user_id"])
                    for name in DIM_KEYS:
                        value = dimension(event["props"].get(name))
                        if value is not None:
                            dims[(src, minute, name, value)] += 1
                if occurred.date() >= (now - timedelta(days=2)).date():
                    days[(src, day)] += 1
                    day_users[(src, day)].add(event["user_id"])
                    types[(src, day, event["type"])] += 1
                feeds[src].append((event["id"], json.dumps(event, ensure_ascii=False)))
            for (src, minute), count in counts.items():
                key = self.key(generation, f"{src}:minute:{minute}:count")
                pipeline.incrby(key, count)
                pipeline.expireat(key, (minute + MINUTE_RETENTION) * 60)
                key = self.key(generation, f"{src}:minute:{minute}:users")
                pipeline.pfadd(key, *users[(src, minute)])
                pipeline.expireat(key, (minute + MINUTE_RETENTION) * 60)
            for (src, day), count in days.items():
                expires = int((datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc) + timedelta(days=3)).timestamp())
                key = self.key(generation, f"{src}:day:{day}:count")
                pipeline.incrby(key, count)
                pipeline.expireat(key, expires)
                key = self.key(generation, f"{src}:day:{day}:users")
                pipeline.pfadd(key, *day_users[(src, day)])
                pipeline.expireat(key, expires)
            for (src, day, event_type), count in types.items():
                key = self.key(generation, f"{src}:day:{day}:types")
                pipeline.hincrby(key, event_type, count)
                expires = int((datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc) + timedelta(days=3)).timestamp())
                pipeline.expireat(key, expires)
            for (src, minute, name, value), count in dims.items():
                key = self.key(generation, f"{src}:minute:{minute}:dim:{name}")
                pipeline.hincrby(key, value, count)
                pipeline.expireat(key, (minute + MINUTE_RETENTION) * 60)
            for src, entries in feeds.items():
                key = self.key(generation, f"{src}:recent")
                pipeline.zadd(key, {entry: event_id for event_id, entry in entries[-200:]})
                pipeline.zremrangebyrank(key, 0, -201)
                pipeline.expire(key, 3 * 86400)
        if publish:
            pipeline.publish(self.channel, json.dumps({"kind": "events", "events": events}, ensure_ascii=False))
        pipeline.execute()

    def summary(self, database):
        generation = self.valid_generation(database)
        if not generation:
            return None
        now = datetime.now(timezone.utc)
        minute = int(now.timestamp() // 60)
        day = now.strftime("%Y%m%d")
        pipeline = self.client.pipeline(transaction=False)
        for src in SOURCES:
            pipeline.get(self.key(generation, f"{src}:minute:{minute - 1}:count"))
            pipeline.get(self.key(generation, f"{src}:minute:{minute}:count"))
            pipeline.get(self.key(generation, f"{src}:day:{day}:count"))
            pipeline.pfcount(*[self.key(generation, f"{src}:minute:{m}:users") for m in range(minute - 4, minute + 1)])
            pipeline.pfcount(self.key(generation, f"{src}:day:{day}:users"))
        values = pipeline.execute()
        fields = ("events_last_minute", "events_this_minute", "events_today", "active_users_5m", "unique_users_today")
        sources = {src: dict(zip(fields, [int(v or 0) for v in values[i * 5:(i + 1) * 5]]))
                   for i, src in enumerate(SOURCES)}
        keys = list(self.client.scan_iter(f"{self.namespace}:ws:clients:*", count=100))
        clients = sum(int(value or 0) for value in self.client.mget(keys)) if keys else 0
        return {"sources": sources, "ws_clients": clients, "server_time": int(now.timestamp() * 1000),
                "stats_backend": "redis", "unique_users_approximate": True}

    def dims(self, database, source, name, minutes, limit):
        generation = self.valid_generation(database) if minutes <= 180 else None
        if not generation:
            return None
        minute = int(datetime.now(timezone.utc).timestamp() // 60)
        pipeline = self.client.pipeline(transaction=False)
        for bucket in range(minute - minutes, minute + 1):
            pipeline.hgetall(self.key(generation, f"{source}:minute:{bucket}:dim:{name}"))
        counts = Counter()
        for row in pipeline.execute():
            counts.update({value: int(count) for value, count in row.items()})
        return [{"value": value, "count": count} for value, count in
                sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]]

    def recent(self, database, source, limit):
        generation = self.valid_generation(database)
        if not generation:
            return None
        return [json.loads(value) for value in self.client.zrevrange(self.key(generation, f"{source}:recent"), 0, limit - 1)]

    def rebuild(self, database):
        """Exclude concurrent app ingests, build a fresh generation, then activate it."""
        generation = uuid.uuid4().hex
        count = 0
        with database.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_lock(%s)", (self.lock_id,))
            # Establish snapshot time after waiting for earlier ingests to finish.
            conn.commit()
            try:
                with conn.cursor(name="live_rebuild") as cursor:
                    cursor.execute("""SELECT * FROM events
                        WHERE occurred_at >= LEAST(
                            date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
                            date_trunc('minute', now()) - interval '180 minutes')
                        ORDER BY id""")
                    while rows := cursor.fetchmany(1000):
                        columns = [column.name for column in cursor.description]
                        events = [payload(dict(zip(columns, row))) for row in rows]
                        self.record(events, generation, publish=False)
                        count += len(events)
                with conn.cursor() as cursor:
                    for src in SOURCES:
                        cursor.execute("SELECT * FROM events WHERE source = %s ORDER BY id DESC LIMIT 200", (src,))
                        columns = [column.name for column in cursor.description]
                        events = [payload(dict(zip(columns, row))) for row in cursor.fetchall()]
                        key = self.key(generation, f"{src}:recent")
                        pipeline = self.client.pipeline(transaction=False)
                        pipeline.delete(key)
                        if events:
                            pipeline.zadd(key, {json.dumps(event, ensure_ascii=False): event["id"] for event in events})
                            pipeline.expire(key, 3 * 86400)
                        pipeline.execute()
                    cursor.execute("SELECT EXISTS(SELECT 1 FROM events WHERE occurred_at >= date_trunc('minute', now()) + interval '1 minute')")
                    future_events = cursor.fetchone()[0]
                    self.client.set(self.pointer, generation)
                    cursor.execute("""INSERT INTO live_cache_state (name, generation, dirty) VALUES (%s, %s, %s)
                        ON CONFLICT (name) DO UPDATE SET generation = EXCLUDED.generation, dirty = EXCLUDED.dirty""",
                                   (self.namespace, generation, False))
                    cursor.execute("DELETE FROM live_cache_pending WHERE name = %s", (self.namespace,))
                conn.commit()
            finally:
                conn.rollback()
                with conn.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(%s)", (self.lock_id,))
        return {"generation": generation, "events_rebuilt": count, "ready": not future_events}
