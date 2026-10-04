import assert from "node:assert/strict";
import test from "node:test";
import { buildSeries, mergeFeed, safePageUrl } from "../src/dashboardData.js";
import { wikiName } from "../src/presentation.js";

test("REST/WS overlap deduplicates, sorts and isolates the bounded feed", () => {
  const event = (id, source = "wikipedia") => ({ id, source });
  const previous = Array.from({ length: 80 }, (_, i) => event(i + 1));
  const rows = mergeFeed(previous, [event(80), event(83), event(82), event(84, "demo"), event(85, "site")]);
  assert.equal(rows.length, 80);
  assert.deepEqual(rows.slice(0, 3).map((row) => row.id), [83, 82, 80]);
  assert.equal(new Set(rows.map((row) => row.id)).size, 80);
  assert.ok(rows.every((row) => row.source === "wikipedia"));
});

test("chart combines event types and fills only 60 completed minute buckets", () => {
  const now = Date.parse("2026-10-03T12:00:30Z");
  const rows = buildSeries([
    { bucket: "2026-10-03T11:59:00Z", type: "edit", count: 7 },
    { bucket: "2026-10-03T11:59:00Z", type: "new", count: 2 },
    { bucket: "2026-10-03T12:00:00Z", type: "edit", count: 99 },
    { bucket: "2026-10-03T10:59:00Z", type: "edit", count: 99 },
  ], now);
  assert.equal(rows.length, 60);
  assert.equal(rows[0].t, Date.parse("2026-10-03T11:00:00Z"));
  assert.equal(rows[0].total, 0);
  assert.deepEqual(rows.at(-1), { t: Date.parse("2026-10-03T11:59:00Z"), total: 9, byType: { edit: 7, new: 2 } });
});

test("event links allow HTTP pages and reject executable or malformed URLs", () => {
  assert.equal(safePageUrl("https://en.wikipedia.org/wiki/Test"), "https://en.wikipedia.org/wiki/Test");
  for (const value of ["javascript:alert(1)", "data:text/html,test", "/relative", undefined]) {
    assert.equal(safePageUrl(value), undefined);
  }
});

test("project labels distinguish languages, sister projects and unknown codes", () => {
  assert.equal(wikiName("enwiki"), "English Wikipedia");
  assert.equal(wikiName("enwiktionary"), "English Wiktionary");
  assert.equal(wikiName("commonswiki"), "Wikimedia Commons");
  assert.equal(wikiName("unknown-project"), "unknown-project");
});
