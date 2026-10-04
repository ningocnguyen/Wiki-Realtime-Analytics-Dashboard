export const SOURCE = "wikipedia";
export const FEED_LIMIT = 80;

export function mergeFeed(previous, incoming) {
  const events = new Map();
  for (const event of [...previous, ...incoming]) {
    if (event.source !== SOURCE || !Number.isSafeInteger(event.id)) continue;
    events.set(event.id, event);
  }
  return [...events.values()].sort((a, b) => b.id - a.id).slice(0, FEED_LIMIT);
}

export function buildSeries(rows, serverTime = Date.now()) {
  const minute = Math.floor(serverTime / 60000) * 60000;
  const buckets = new Map();
  for (const row of rows) {
    const t = Date.parse(row.bucket);
    const bucket = buckets.get(t) || { t, total: 0, byType: {} };
    const count = Number(row.count);
    bucket.total += count;
    bucket.byType[row.type] = (bucket.byType[row.type] || 0) + count;
    buckets.set(t, bucket);
  }
  return Array.from({ length: 60 }, (_, i) => {
    const t = minute - (60 - i) * 60000;
    return buckets.get(t) || { t, total: 0, byType: {} };
  });
}

export function safePageUrl(value) {
  try {
    const url = new URL(value);
    return ["https:", "http:"].includes(url.protocol) ? url.href : undefined;
  } catch {
    return undefined;
  }
}
