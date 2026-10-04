"""Validate untrusted browser beacons before they enter the shared event pipeline."""
import hashlib
import json
import logging
import re
import threading
import time

from redis.exceptions import RedisError

from .validation import ValidationError, validate

log = logging.getLogger(__name__)
EVENT_TYPES = {"page_view", "link_click", "engaged"}
TARGETS = {"github", "linkedin", "resume", "email", "project", "external"}
PROPS = {"page_view": {"path", "title", "ref_source"},
         "link_click": {"path", "ref_source", "target"},
         "engaged": {"path", "ref_source", "seconds"}}
BOT_MARKERS = ("bot", "crawler", "spider", "headless", "python-requests", "curl/", "wget/")
MAX_BEACON_BYTES = 96 * 1024
MAX_BEACON_EVENTS = 20


def validate_beacon(raw):
    if not isinstance(raw, dict) or not isinstance(raw.get("events"), list) or not raw["events"]:
        raise ValidationError("body must be {events: [non-empty list]}")
    if len(raw["events"]) > MAX_BEACON_EVENTS:
        raise ValidationError("max 20 events per beacon")
    events = []
    for item in raw["events"]:
        if not isinstance(item, dict) or item.get("type") not in EVENT_TYPES:
            raise ValidationError("invalid site event type")
        kind = item["type"]
        if not isinstance(item.get("event_id"), str) or not re.fullmatch(r"[0-9a-f-]{32,36}", item["event_id"]):
            raise ValidationError("site event_id must be a random identifier")
        if not isinstance(item.get("user_id"), str) or not re.fullmatch(r"v_[0-9a-f-]{32,36}", item["user_id"]):
            raise ValidationError("site user_id must be a pseudonymous visitor identifier")
        props = item.get("props")
        if not isinstance(props, dict) or set(props) - PROPS[kind]:
            raise ValidationError("invalid site event properties")
        path = props.get("path")
        if not isinstance(path, str) or not path.startswith("/") or len(path) > 256 or "?" in path or "#" in path:
            raise ValidationError("path must be a pathname of at most 256 characters")
        ref = props.get("ref_source")
        if not isinstance(ref, str) or not re.fullmatch(r"[a-z0-9._-]{1,40}", ref):
            raise ValidationError("ref_source must be a short source label")
        if kind == "page_view" and (not isinstance(props.get("title"), str) or len(props["title"]) > 120):
            raise ValidationError("title must be a string of at most 120 characters")
        if kind == "link_click" and props.get("target") not in TARGETS:
            raise ValidationError("invalid link target")
        if kind == "engaged" and props.get("seconds") != 30:
            raise ValidationError("engaged seconds must equal 30")
        events.append(validate({**item, "source": "site", "value": 0, "props": props}))
    return events


def parse_beacon(data):
    if len(data) > MAX_BEACON_BYTES:
        raise ValidationError("beacon exceeds 96 KiB")
    try:
        return validate_beacon(json.loads(data))
    except (UnicodeError, ValueError, TypeError) as error:
        if isinstance(error, ValidationError):
            raise
        raise ValidationError("beacon must contain valid JSON") from None


class CollectLimiter:
    """Shared Redis limit, with a per-worker fallback when Redis is unavailable."""
    def __init__(self, live, namespace, limit):
        self.live = live
        self.namespace = namespace
        self.limit = limit
        self.lock = threading.Lock()
        self.local = {}

    def allowed(self, address):
        minute = int(time.time() // 60)
        digest = hashlib.sha256(address.encode("utf-8", errors="replace")).hexdigest()[:24]
        key = f"{self.namespace}:collect:rate:{digest}:{minute}"
        if self.live:
            try:
                with self.live.client.pipeline(transaction=True) as pipe:
                    pipe.incr(key)
                    pipe.expire(key, 120)
                    count = pipe.execute()[0]
                return count <= self.limit
            except RedisError:
                log.warning("Redis rate limit unavailable; using worker-local limit")
        with self.lock:
            if len(self.local) > 10000:
                self.local = {k: v for k, v in self.local.items() if k.endswith(f":{minute}")}
            count = self.local.get(key, 0) + 1
            self.local[key] = count
            return count <= self.limit
