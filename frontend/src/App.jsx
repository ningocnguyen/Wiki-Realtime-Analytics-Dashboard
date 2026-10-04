import { useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useLiveStream } from "./useLiveStream";
import { safePageUrl } from "./dashboardData";
import { colorOf, fmt, hhmm, typeLabels, typeShort, wikiName } from "./presentation";

const REPO = "https://github.com/ningocnguyen/Wiki-Realtime-Analytics-Dashboard";

function Kpi({ label, value, unit, hint }) {
  return <div className="kpi"><div className="kpi-label">{label}</div>
    <div className="kpi-value">{value}{unit && <span className="kpi-unit">{unit}</span>}</div>
    <div className="kpi-hint">{hint}</div></div>;
}

function BarList({ rows, colorFor = () => "var(--bar-neutral)", limit = 8 }) {
  const total = rows.reduce((sum, row) => sum + row.count, 0);
  const max = Math.max(1, ...rows.map((row) => row.count));
  if (!rows.length) return <p className="empty-small">No recorded activity in this window.</p>;
  return <ul className="barlist">{rows.slice(0, limit).map((row) => <li key={row.key} title={`${row.label}: ${fmt(row.count)}`}>
    <div className="bl-row"><span className="bl-label">{row.label}</span>
      <span className="bl-value">{fmt(row.count)}<span className="bl-share">{total ? Math.round(100 * row.count / total) : 0}%</span></span></div>
    <span className="bl-track"><span className="bl-bar" style={{ width: `${100 * row.count / max}%`, background: colorFor(row.key) }} /></span>
  </li>)}</ul>;
}

function SeriesTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  return <div className="tip"><div className="tip-head">{hhmm(row.t)} · {fmt(row.total)} changes</div>
    {Object.entries(row.byType).sort((a, b) => b[1] - a[1]).map(([type, count]) => <div className="tip-row" key={type}>
      <span className="dot" style={{ background: colorOf(type) }} /><span>{typeShort[type] || type}</span><span>{fmt(count)}</span>
    </div>)}</div>;
}

function Feed({ feed, loading }) {
  const [paused, setPaused] = useState(false);
  const [frozen, setFrozen] = useState([]);
  const rows = paused ? frozen : feed;
  function toggle() {
    if (!paused) setFrozen(feed);
    setPaused(!paused);
  }
  return <div className="card feed-card wide"><div className="card-heading">
    <h2>Latest changes <span className="sub">click a title to open the page</span></h2>
    <button className="btn ghost" onClick={toggle} aria-pressed={paused}>{paused ? "Resume feed" : "Pause feed"}</button>
  </div>{!rows.length ? <p className="empty-small">{loading ? "Loading recent changes…" : "Waiting for the first recorded changes."}</p> :
    <ul className="feed" aria-label={paused ? "Paused recent changes" : "Recent changes"}>{rows.map((event) => {
      const href = safePageUrl(event.props?.url);
      const title = event.props?.title || "Untitled change";
      return <li key={event.id}>
        <span className="dot" style={{ background: colorOf(event.type) }} />
        <span className="feed-main" title={title}>{href ? <a href={href} target="_blank" rel="noreferrer">{title}</a> : title}
          {event.props?.replay && <span className="replay"> replay</span>}</span>
        <span className="feed-sec" title={wikiName(event.props?.wiki)}>{wikiName(event.props?.wiki)}</span>
        <span className="feed-sec">{typeShort[event.type] || event.type}{event.props?.bot === "bot" ? " · bot" : ""}</span>
        <time className="feed-time" dateTime={event.occurred_at}>{new Date(event.occurred_at).toLocaleTimeString()}</time>
      </li>;
    })}</ul>}</div>;
}

export default function App() {
  const { status, summary, series, breakdown, dims, feed, latency, error, approximate, backend, updatedAt } = useLiveStream();
  const botTotal = Object.values(dims.bot || {}).reduce((sum, count) => sum + count, 0);
  const botShare = botTotal ? Math.round(100 * (dims.bot?.bot || 0) / botTotal) : null;
  const rows = (values, label) => Object.entries(values || {}).map(([key, count]) => ({ key, label: label(key), count }))
    .sort((a, b) => b.count - a.count || a.key.localeCompare(b.key));
  const loading = !summary;
  const statusLabel = { live: "Live · WebSocket", polling: "Polling · REST snapshots", connecting: "Connecting…", offline: "Offline · retrying" }[status];
  const pipeline = [
    ["Wikimedia live feed", "Public edits, new pages, category updates, and log actions."],
    ["Ingest API", "A Python connector batches changes; Flask validates and stores them."],
    ["PostgreSQL", "Raw changes and minute summaries are written in one transaction."],
    ["Redis → WebSocket", "Live counters and a shared stream deliver updates to open browsers."],
    ["This dashboard", "React shows Wikipedia activity and recovers missed updates through REST."],
  ];
  return <main className="viz-root">
    <header className="hero"><p className="eyebrow">Wikipedia, live · a data pipeline you can watch</p>
      <h1>Real-Time Analytics Dashboard</h1>
      <p className="lede">Changes stream in from Wikimedia, get stored and aggregated, and appear here as they happen.</p>
      <div className="hero-links"><a className="btn" href={REPO} target="_blank" rel="noreferrer">View the code on GitHub</a>
        <a className="btn ghost" href="#how">How it works</a>
        <span className={`status status-${status}`} role="status"><span className="status-dot" />{statusLabel}</span></div>
    </header>
    <section className="explain" aria-labelledby="about-title"><h2 className="headline" id="about-title">What is changing across Wikipedia right now?</h2>
      <p>Wikipedia and its sister projects—including Wiktionary, Wikidata, and Wikimedia Commons—publish a public stream of changes from people and bots, in every language.</p>
      <p className="note">Counts show changes collected by this project, including any marked replay data. Collection continues while the connector runs, even when this page is closed. Today uses UTC.</p>
    </section>
    {error && <div className="empty-banner" role="alert">{error} Existing data remains visible.</div>}
    <section className="kpis" aria-label="Live Wikipedia statistics">
      <Kpi label="Changes today" value={fmt(summary?.events_today)} hint={`${fmt(summary?.events_last_minute)} in the last completed minute`} />
      <Kpi label="People & bots editing now" value={fmt(summary?.active_users_5m)} hint={approximate ? "estimated accounts, five minute buckets" : "distinct accounts, last five minutes"} />
      <Kpi label="Distinct editors today" value={fmt(summary?.unique_users_today)} hint={approximate ? "estimated with HyperLogLog · UTC" : "exact stored accounts · UTC"} />
      <Kpi label="Changes made by bots" value={fmt(botShare)} unit="%" hint="automated accounts, last 60 minutes" />
      <Kpi label="Delivery to this browser" value={status === "live" ? fmt(latency.p50 == null ? null : Math.round(latency.p50)) : "—"} unit="ms" hint="estimated median · stored to received" />
    </section>
    {!loading && summary.events_today === 0 && !feed.length && <div className="empty-banner">No changes have been collected yet. Start the Wikipedia connector to populate this dashboard.</div>}
    <section className="grid" aria-label="Wikipedia activity">
      <div className="card wide"><h2>Changes per minute <span className="sub">last hour · completed minutes</span></h2>
        {loading ? <p className="empty-small">Loading activity history…</p> : <>
          <p className="sr-only">{fmt(series.reduce((sum, row) => sum + row.total, 0))} recorded changes across the last 60 completed minutes.</p>
          <ResponsiveContainer width="100%" height={220}><LineChart data={series} margin={{ top: 8, right: 16, bottom: 0, left: 0 }} accessibilityLayer>
            <CartesianGrid stroke="var(--grid)" vertical={false} />
            <XAxis dataKey="t" tickFormatter={hhmm} stroke="var(--text-muted)" tickLine={false} minTickGap={40} fontSize={12} />
            <YAxis stroke="var(--text-muted)" tickLine={false} axisLine={false} width={52} fontSize={12} allowDecimals={false} />
            <Tooltip content={<SeriesTooltip />} />
            <Line type="monotone" dataKey="total" stroke="var(--series-1)" strokeWidth={2} dot={false} isAnimationActive={false} />
          </LineChart></ResponsiveContainer></>}
      </div>
      <div className="card"><h2>What kind of activity <span className="sub">last 60 min</span></h2><BarList rows={rows(breakdown, (key) => typeLabels[key] || key)} colorFor={colorOf} /></div>
      <div className="card"><h2>Which sites are busiest <span className="sub">last 60 min</span></h2><BarList rows={rows(dims.wiki, wikiName)} /><p className="note">Top 8 of up to 50 sites; percentages use the returned sites.</p></div>
      <div className="card"><h2>Who is making changes <span className="sub">last 60 min</span></h2><BarList rows={rows(dims.bot, (key) => key === "bot" ? "Bots (automated accounts)" : key === "human" ? "Humans" : key)} /></div>
      <Feed feed={feed} loading={loading} />
    </section>
    <section className="how" id="how"><h2 className="headline">How it works</h2><p>Every recorded change travels through this pipeline:</p>
      <ol className="pipeline">{pipeline.map(([title, body], i) => <li key={title}><span className="step-n">{i + 1}</span><div><div className="step-title">{title}</div><div className="step-body">{body}</div></div></li>)}</ol>
      <p className="stack"><strong>Built with</strong> Python · Flask · PostgreSQL · Redis · WebSocket · React · Docker</p>
      <p className="note">The feed updates up to four times a second. History and breakdowns refresh every 10 seconds. If the stream disconnects, polling keeps snapshots current while it reconnects.</p>
    </section>
    <footer><a href={REPO} target="_blank" rel="noreferrer">Source on GitHub</a> · Wikimedia EventStreams
      {backend && <> · Statistics: {backend === "redis" ? "Redis" : "PostgreSQL"}</>}
      {updatedAt && <> · Last statistics update: {hhmm(updatedAt)}</>}</footer>
  </main>;
}
