/* First-party website tracker: no cookies, stored IPs, or query-string capture. */
(() => {
  "use strict";
  if (navigator.doNotTrack === "1" || window.doNotTrack === "1" ||
      navigator.globalPrivacyControl || navigator.webdriver) return;

  const script = document.currentScript;
  const endpoint = script?.getAttribute("data-endpoint") || "/api/collect";
  const randomId = () => crypto.randomUUID ? crypto.randomUUID() :
    Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) => byte.toString(16).padStart(2, "0")).join("");
  function stored(kind, key, make) {
    try {
      const storage = window[kind];
      const existing = storage.getItem(key);
      if (existing) return existing;
      const value = make();
      storage.setItem(key, value);
      return value;
    } catch { return make(); }
  }
  const visitor = stored("localStorage", "rta_visitor", () => `v_${randomId()}`);
  function source() {
    const utm = new URLSearchParams(location.search).get("utm_source")?.toLowerCase();
    if (utm) return utm.replace(/[^a-z0-9_-]/g, "").slice(0, 40) || "direct";
    let host = "";
    try { host = new URL(document.referrer).hostname.replace(/^www\./, "").toLowerCase(); }
    catch { /* no referrer */ }
    if (!host || host === location.hostname.replace(/^www\./, "")) return "direct";
    if (/(^|\.)linkedin\.com$|^lnkd\.in$/.test(host)) return "linkedin";
    if (/(^|\.)github\.com$/.test(host)) return "github";
    if (/(^|\.)google\./.test(host)) return "google";
    if (/(^|\.)bing\.com$|(^|\.)duckduckgo\.com$/.test(host)) return "search";
    if (/^(t\.co|x\.com|twitter\.com)$/.test(host)) return "x";
    if (/(^|\.)joinhandshake\.com$/.test(host)) return "handshake";
    return host.slice(0, 40);
  }
  const refSource = stored("sessionStorage", "rta_ref_source", source);
  const queue = [];
  let flushTimer = 0, lastPath = null, visibleSince = 0, visibleMs = 0;
  let engagementTimer = 0, engagedSent = false;
  function flush() {
    clearTimeout(flushTimer);
    flushTimer = 0;
    while (queue.length) {
      const body = JSON.stringify({ events: queue.splice(0, 20) });
      const beacon = navigator.sendBeacon?.(endpoint, new Blob([body], { type: "text/plain" }));
      if (!beacon) fetch(endpoint, { method: "POST", body, keepalive: true,
        mode: "cors", headers: { "Content-Type": "text/plain" } }).catch(() => {});
    }
  }
  function send(type, props = {}) {
    queue.push({ event_id: randomId(), type, user_id: visitor, ts: Date.now(),
      props: { path: location.pathname.slice(0, 256), ref_source: refSource, ...props } });
    if (!flushTimer) flushTimer = setTimeout(flush, 250);
  }
  function armEngagement() {
    clearTimeout(engagementTimer);
    if (engagedSent || !visibleSince) return;
    engagementTimer = setTimeout(() => {
      engagedSent = true;
      send("engaged", { seconds: 30 });
    }, Math.max(0, 30000 - visibleMs));
  }
  function pageView() {
    const path = location.pathname;
    if (path === lastPath) return;
    lastPath = path;
    clearTimeout(engagementTimer);
    visibleMs = 0;
    visibleSince = document.visibilityState === "visible" ? Date.now() : 0;
    engagedSent = false;
    send("page_view", { title: document.title.slice(0, 120) });
    armEngagement();
  }
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
      if (visibleSince) visibleMs += Date.now() - visibleSince;
      visibleSince = 0;
      clearTimeout(engagementTimer);
      flush();
    } else {
      visibleSince = Date.now();
      armEngagement();
    }
  });
  window.addEventListener("pagehide", flush);
  for (const method of ["pushState", "replaceState"]) {
    const original = history[method];
    history[method] = function (...args) {
      const result = original.apply(this, args);
      queueMicrotask(pageView);
      return result;
    };
  }
  window.addEventListener("popstate", pageView);
  document.addEventListener("click", (event) => {
    const link = event.target?.closest?.("a[href]");
    if (!link) return;
    const href = link.getAttribute("href") || "";
    let target;
    if (/^mailto:/i.test(href)) target = "email";
    else {
      let url;
      try { url = new URL(href, location.href); }
      catch { return; }
      if (!["http:", "https:"].includes(url.protocol)) return;
      const host = url.hostname.replace(/^www\./, "");
      if (/resume|cv/i.test(url.pathname) || /\.pdf$/i.test(url.pathname)) target = "resume";
      else if (/(^|\.)github\.com$/.test(host)) target = "github";
      else if (/(^|\.)linkedin\.com$/.test(host)) target = "linkedin";
      else if (host === location.hostname.replace(/^www\./, "")) target = /project/i.test(url.pathname) ? "project" : null;
      else target = "external";
    }
    if (target) { send("link_click", { target }); flush(); }
  }, true);
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", pageView, { once: true });
  else pageView();
})();
