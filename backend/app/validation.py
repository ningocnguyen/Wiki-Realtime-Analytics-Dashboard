"""The shared event contract for trusted producers and the future tracker."""
import json
import math
from datetime import datetime, timezone

SOURCES = ("wikipedia", "site", "demo")


class ValidationError(ValueError):
    pass


def text(value, field, limit):
    if not isinstance(value, str) or not value.strip() or len(value) > limit or "\x00" in value:
        raise ValidationError(f"{field} must be a non-empty string of at most {limit} characters")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise ValidationError(f"{field} must contain valid Unicode") from None
    return value


def source(value):
    if not isinstance(value, str) or value not in SOURCES:
        raise ValidationError("source must be wikipedia, site, or demo")
    return value


def timestamp(value):
    if value is None:
        return datetime.now(timezone.utc)
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            parsed = datetime.fromtimestamp(value / 1000 if value > 1e11 else value, timezone.utc)
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            raise ValueError()
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError, OSError):
        raise ValidationError("ts must be an ISO-8601 timestamp or epoch seconds/milliseconds") from None


def validate(raw):
    if not isinstance(raw, dict):
        raise ValidationError("each event must be an object")
    value = raw.get("value", 0)
    try:
        finite = math.isfinite(value)
    except (TypeError, OverflowError):
        finite = False
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not finite:
        raise ValidationError("value must be a finite number")
    props = raw.get("props", {})
    try:
        encoded = json.dumps(props, allow_nan=False, ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError):
        raise ValidationError("props must contain valid JSON values") from None
    if not isinstance(props, dict) or len(encoded) > 4096:
        raise ValidationError("props must be a JSON object of at most 4096 UTF-8 bytes")
    # PostgreSQL text/JSONB cannot represent the NUL character, even in valid JSON.
    pending = [props]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
        elif isinstance(item, str) and "\x00" in item:
            raise ValidationError("props cannot contain NUL characters")
    event_id = raw.get("event_id")
    if event_id is not None:
        event_id = text(event_id, "event_id", 256)
    return {
        "event_id": event_id,
        "source": source(raw.get("source", "demo")),
        "type": text(raw.get("type"), "type", 64),
        "user_id": text(raw.get("user_id"), "user_id", 128),
        "value": float(value),
        "props": props,
        "occurred_at": timestamp(raw.get("ts")),
    }
