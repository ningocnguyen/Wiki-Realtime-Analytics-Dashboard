import sys
import unittest
from datetime import timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.validation import ValidationError, validate


class ValidationTests(unittest.TestCase):
    def event(self, **changes):
        return {"type": "edit", "user_id": "editor", **changes}

    def test_defaults_and_timezone_normalization(self):
        event = validate(self.event(ts="2026-10-03T12:00:00-04:00"))
        self.assertEqual(event["source"], "demo")
        self.assertEqual(event["value"], 0)
        self.assertEqual(event["occurred_at"].hour, 16)
        self.assertEqual(event["occurred_at"].tzinfo, timezone.utc)

    def test_epoch_seconds_and_milliseconds_match(self):
        self.assertEqual(validate(self.event(ts=1750000000))["occurred_at"],
                         validate(self.event(ts=1750000000000))["occurred_at"])

    def test_invalid_values(self):
        for value in [True, "1", float("nan"), float("inf"), 10 ** 1000]:
            with self.subTest(value=type(value).__name__), self.assertRaises(ValidationError):
                validate(self.event(value=value))

    def test_invalid_fields(self):
        for changes in [{"type": " "}, {"user_id": ""}, {"source": "unknown"},
                        {"source": []}, {"event_id": ""}, {"props": []},
                        {"props": {"n": float("nan")}}, {"props": {"x": "漢" * 1400}},
                        {"ts": True}, {"ts": "invalid"}, {"user_id": "a\x00b"},
                        {"user_id": "\ud800"}, {"props": {"x": "\ud800"}},
                        {"props": {"nested": ["a\x00b"]}}]:
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                validate(self.event(**changes))
