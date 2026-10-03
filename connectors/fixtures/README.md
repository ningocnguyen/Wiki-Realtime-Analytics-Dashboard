# Wikimedia recentchange sample

`wikimedia_recentchange_sample.jsonl` contains 200 recorded public Wikimedia EventStreams records from October 1, 2026, bundled with the reference project's fixture at commit `f84b289`:

https://github.com/JiajunWang23/realtime-analytics-dashboard/blob/f84b289/connectors/fixtures/wikimedia_recentchange_sample.jsonl

These records retain public editor identifiers and page titles. Replay assigns fresh timestamps and replay-specific event IDs; it does not claim to represent current edits. Live collection uses https://stream.wikimedia.org/v2/stream/recentchange.
