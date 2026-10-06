import { useEffect, useState } from "react";
import { buildSeries, FEED_LIMIT, mergeFeed, SOURCE } from "./dashboardData";

// Windowed aggregates are server snapshots; only the recent feed is merged from WS.
// This avoids counting an event twice when REST and live updates overlap.
export function useLiveStream() {
  const [state, setState] = useState({ status: "connecting", summary: null, series: [],
    breakdown: {}, dims: {}, feed: [], latency: { p50: null }, error: null,
    approximate: false, backend: null, updatedAt: null });

  useEffect(() => {
    let disposed = false, socket, reconnectTimer, pullTimer, pollTimer;
    let refreshing = false, refreshAgain = false, polling = false, pollCursor = 0;
    let attempts = 0, lastFrame = 0, lastSnapshot = 0, clockOffset = 0;
    let buffer = [], stats = null, delaySamples = [], live = false, mode = "ws";
    const controllers = new Set();
    const update = (fn) => { if (!disposed) setState(fn); };
    async function get(path, options = {}, timeoutMs = 10000) {
      const controller = new AbortController();
      controllers.add(controller);
      const timeout = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const response = await fetch(path, { ...options, signal: controller.signal });
        if (!response.ok) throw new Error(`Request failed (${response.status})`);
        return await response.json();
      } finally {
        clearTimeout(timeout);
        controllers.delete(controller);
      }
    }
    function summaryPatch(message) {
      clockOffset = Date.now() - message.server_time;
      return { summary: message.sources?.[SOURCE] ?? null,
        approximate: message.unique_users_approximate === true,
        backend: message.stats_backend, updatedAt: message.server_time };
    }
    async function refresh() {
      if (disposed) return;
      if (refreshing) { refreshAgain = true; return; }
      refreshing = true;
      try {
        const [summary, series, breakdown, wiki, bot, recent] = await Promise.all([
          get("/api/stats/summary"),
          get(`/api/stats/timeseries?source=${SOURCE}&minutes=60`),
          get(`/api/stats/breakdown?source=${SOURCE}&minutes=60`),
          get(`/api/stats/dims?source=${SOURCE}&key=wiki&minutes=60&limit=50`),
          get(`/api/stats/dims?source=${SOURCE}&key=bot&minutes=60&limit=50`),
          get(`/api/events/recent?source=${SOURCE}&limit=${FEED_LIMIT}`),
        ]);
        lastSnapshot = Date.now();
        pollCursor = Math.max(pollCursor, ...recent.map((item) => item.id), 0);
        update((previous) => ({ ...previous, ...summaryPatch(summary),
          series: buildSeries(series, summary.server_time),
          breakdown: Object.fromEntries(breakdown.map((r) => [r.event_type, Number(r.count)])),
          dims: { wiki: Object.fromEntries(wiki.map((r) => [r.value, Number(r.count)])),
            bot: Object.fromEntries(bot.map((r) => [r.value, Number(r.count)])) },
          feed: mergeFeed(previous.feed, recent), error: null,
          status: live ? "live" : "polling" }));
      } catch {
        update((previous) => ({ ...previous, error: "Could not refresh dashboard data. Retrying automatically.",
          status: live ? "live" : "offline" }));
      } finally {
        refreshing = false;
        if (refreshAgain && !disposed) { refreshAgain = false; void refresh(); }
      }
    }
    async function pollLive() {
      if (disposed || polling || !lastSnapshot || document.visibilityState !== "visible") return;
      polling = true;
      try {
        const result = await get(`/api/live?source=${SOURCE}&after=${pollCursor}`);
        pollCursor = Math.max(pollCursor, result.cursor);
        update((previous) => ({ ...previous, ...summaryPatch(result.summary),
          feed: mergeFeed(previous.feed, result.events), status: "polling", error: null }));
      } catch {
        update((previous) => ({ ...previous, status: "offline",
          error: "Could not refresh dashboard data. Retrying automatically." }));
      } finally { polling = false; }
    }
    async function pullWikipedia() {
      if (disposed) return;
      let delay = 1500;
      if (document.visibilityState === "visible") {
        try {
          const result = await get("/api/ingest/wikipedia?seconds=18", { method: "POST" }, 35000);
          if (result.status === "busy") delay = 5000;
        } catch { delay = 5000; }
        void refresh();
      }
      if (!disposed) pullTimer = setTimeout(pullWikipedia, delay);
    }
    function connect() {
      if (disposed || mode !== "ws") return;
      update((previous) => ({ ...previous, status: lastSnapshot ? "polling" : "connecting" }));
      const url = new URL(`/ws?source=${SOURCE}`, location.href);
      url.protocol = location.protocol === "https:" ? "wss:" : "ws:";
      socket = new WebSocket(url);
      lastFrame = Date.now();
      socket.onmessage = (event) => {
        if (disposed) return;
        try {
          const message = JSON.parse(event.data);
          lastFrame = Date.now();
          if (message.kind === "hello") {
            live = true;
            attempts = 0;
            void refresh();
          } else if (message.kind === "stats") {
            live = true;
            attempts = 0;
            stats = message;
          } else if (message.kind === "events" && Array.isArray(message.events)) {
            const events = message.events.filter((item) => item.source === SOURCE);
            buffer = mergeFeed(buffer, events);
            for (const item of events) {
              const delay = Date.now() - clockOffset - item.ingested_at;
              if (Number.isFinite(delay) && delay >= 0 && delay < 60000) delaySamples.push(delay);
            }
            delaySamples = delaySamples.slice(-500);
          } else if (message.kind === "resync") {
            buffer = [];
            delaySamples = [];
            void refresh();
          }
        } catch { /* A malformed frame is recovered by the next REST snapshot. */ }
      };
      socket.onerror = () => socket.close();
      socket.onclose = () => {
        live = false;
        if (disposed) return;
        update((previous) => ({ ...previous, status: lastSnapshot ? "polling" : "offline" }));
        void refresh();
        reconnectTimer = setTimeout(connect, Math.min(10000, 500 * 2 ** Math.min(attempts++, 5)));
      };
    }
    void refresh();
    void get("/api/config").then((config) => {
      mode = config.realtime === "ws" ? "ws" : "poll";
      if (mode === "ws") connect();
      else {
        pollTimer = setInterval(() => { void pollLive(); }, 2000);
        if (config.wiki_pull) void pullWikipedia();
      }
    }).catch(() => connect());
    const flushTimer = setInterval(() => {
      if (!buffer.length && !stats) return;
      const events = buffer, summary = stats;
      buffer = []; stats = null;
      const sorted = [...delaySamples].sort((a, b) => a - b);
      update((previous) => ({ ...previous, ...(summary ? summaryPatch(summary) : {}),
        feed: mergeFeed(previous.feed, events), status: live ? "live" : previous.status,
        latency: { p50: live && sorted.length ? sorted[Math.floor(sorted.length / 2)] : null } }));
    }, 250);
    const refreshTimer = setInterval(() => { void refresh(); }, 10000);
    const watchdog = setInterval(() => {
      if (socket && socket.readyState < WebSocket.CLOSING && Date.now() - lastFrame > 30000) socket.close();
    }, 5000);
    const visible = () => { if (document.visibilityState === "visible") void refresh(); };
    document.addEventListener("visibilitychange", visible);
    return () => {
      disposed = true;
      clearTimeout(reconnectTimer);
      clearTimeout(pullTimer);
      clearInterval(pollTimer);
      [flushTimer, refreshTimer, watchdog].forEach(clearInterval);
      document.removeEventListener("visibilitychange", visible);
      controllers.forEach((controller) => controller.abort());
      socket?.close();
    };
  }, []);
  return state;
}
