"""
generate_dashboard.py
Pulls livestream analytics from Aiven PostgreSQL and generates
a self-contained HTML dashboard — one page per livestream.
Run this script, then deploy the /dashboard output folder to any static host.
"""

import os
import re
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from string import Template

import psycopg2
import psycopg2.extras
import shutil
import os

# ── copy static legal pages into dashboard output ─────────────────────────
_output_dir = os.environ.get("DASHBOARD_OUTPUT_DIR", "dashboard")
for legal_file in ["privacy.html", "terms.html"]:
    src = Path(legal_file)
    dst = Path(_output_dir) / legal_file
    if src.exists():
        shutil.copy2(src, dst)
        print(f"Copied {legal_file} to {_output_dir}/")
    else:
        print(f"Warning: {legal_file} not found at repo root — skipping")


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(AIVEN_DATABASE_URL, sslmode="require")


def get_channel_tables(conn) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT channel_id, channel_name, table_name, added_at FROM channels ORDER BY channel_name")
        return cur.fetchall()


def get_streams_for_channel(conn, table: str) -> list[dict]:
    """Return one summary row per unique video_id."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                video_id,
                MAX(video_title)        AS video_title,
                MAX(stream_status)      AS stream_status,
                MIN(collected_at)       AS first_seen,
                MAX(collected_at)       AS last_seen,
                MAX(concurrent_viewers) AS peak_viewers,
                MAX(like_count)         AS peak_likes,
                MAX(comment_count)      AS peak_comments,
                COUNT(*)                AS data_points
            FROM {table}
            GROUP BY video_id
            ORDER BY first_seen DESC
        """)
        return cur.fetchall()


def get_stream_timeseries(conn, table: str, video_id: str) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                collected_at, concurrent_viewers, like_count, comment_count
            FROM {table}
            WHERE video_id = %s
            ORDER BY collected_at
        """, (video_id,))
        return cur.fetchall()


def get_all_rows(conn, table: str, video_id: str) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT *
            FROM {table}
            WHERE video_id = %s
            ORDER BY collected_at DESC
        """, (video_id,))
        return cur.fetchall()


# ── slug helper ───────────────────────────────────────────────────────────────

def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


# ── HTML templates ─────────────────────────────────────────────────────────────

INDEX_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Stream Analytics — Overview</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Fraunces:ital,opsz,wght@0,9..144,300;0,9..144,700;1,9..144,300&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:      #0a0a0f;
    --surface: #13131a;
    --border:  #1e1e2e;
    --accent:  #e8ff47;
    --muted:   #5a5a7a;
    --text:    #e2e2f0;
    --red:     #ff4f6d;
    --blue:    #4fc3f7;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'DM Mono', monospace;
    min-height: 100vh;
    padding: 3rem 2rem;
  }
  .noise {
    position: fixed; inset: 0; pointer-events: none; z-index: 0;
    background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.04'/%3E%3C/svg%3E");
    opacity: 0.4;
  }
  .wrap { max-width: 1100px; margin: 0 auto; position: relative; z-index: 1; }
  header { margin-bottom: 4rem; }
  .eyebrow {
    font-size: 0.7rem; letter-spacing: 0.25em; text-transform: uppercase;
    color: var(--accent); margin-bottom: 0.75rem;
  }
  h1 {
    font-family: 'Fraunces', serif;
    font-size: clamp(2.5rem, 6vw, 5rem);
    font-weight: 700; line-height: 1;
    color: #fff;
  }
  h1 span { color: var(--accent); font-style: italic; }
  .meta { color: var(--muted); font-size: 0.75rem; margin-top: 1rem; }

  .channel-block { margin-bottom: 4rem; }
  .channel-header {
    display: flex; align-items: baseline; gap: 1rem;
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.75rem; margin-bottom: 1.5rem;
  }
  .channel-name {
    font-family: 'Fraunces', serif; font-size: 1.5rem; color: #fff;
  }
  .channel-id { font-size: 0.7rem; color: var(--muted); }

  .streams-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 1rem;
  }
  .stream-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1.25rem;
    text-decoration: none;
    color: inherit;
    display: block;
    transition: border-color 0.2s, transform 0.2s;
    position: relative; overflow: hidden;
  }
  .stream-card::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: var(--accent); transform: scaleX(0); transform-origin: left;
    transition: transform 0.3s;
  }
  .stream-card:hover { border-color: var(--accent); transform: translateY(-2px); }
  .stream-card:hover::before { transform: scaleX(1); }
  .card-status {
    display: inline-block;
    font-size: 0.65rem; letter-spacing: 0.15em; text-transform: uppercase;
    padding: 0.2rem 0.5rem; border-radius: 2px;
    margin-bottom: 0.75rem;
  }
  .status-live { background: rgba(255,79,109,0.15); color: var(--red); border: 1px solid var(--red); }
  .status-upcoming { background: rgba(79,195,247,0.1); color: var(--blue); border: 1px solid var(--blue); }
  .status-vod { background: rgba(90,90,122,0.2); color: var(--muted); border: 1px solid var(--muted); }
  .card-title {
    font-family: 'Fraunces', serif;
    font-size: 1rem; font-weight: 700;
    color: #fff; margin-bottom: 1rem;
    line-height: 1.3;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
  }
  .card-stats { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.5rem; }
  .stat-item { }
  .stat-label { font-size: 0.6rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.1em; }
  .stat-value { font-size: 0.95rem; color: var(--accent); font-weight: 500; }
  .card-date { font-size: 0.65rem; color: var(--muted); margin-top: 1rem; }
  .empty { color: var(--muted); font-size: 0.8rem; font-style: italic; padding: 1rem 0; }
  .generated { text-align: center; color: var(--muted); font-size: 0.7rem; margin-top: 5rem; }
</style>
</head>
<body>
<div class="noise"></div>
<div class="wrap">
  <header>
    <p class="eyebrow">YouTube Analytics Dashboard</p>
    <h1>Stream <span>Overview</span></h1>
    <p class="meta">Generated: $generated_at &nbsp;·&nbsp; $total_streams streams across $total_channels channels</p>
  </header>
  $channel_blocks
  <p class="generated">Auto-generated by yt-livestream-tracker</p>
  <footer style="
    margin-top: 3rem;
    padding-top: 1.5rem;
    border-top: 1px solid #1e1e2e;
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    gap: 1rem;
    font-size: 0.7rem;
    color: #5a5a7a;
    font-family: 'DM Mono', monospace;
  ">
    <span>&#169; 2026 IDVTuber Tracker &#8212; Non-commercial fan project</span>
    <span>
      <a href="privacy.html" style="color:#5a5a7a; text-decoration:none;">Privacy Policy</a>
      &nbsp;&middot;&nbsp;
      <a href="terms.html" style="color:#5a5a7a; text-decoration:none;">Terms of Use</a>
    </span>
  </footer>
</div>
</body>
</html>
"""

CHANNEL_BLOCK = """\
<div class="channel-block">
  <div class="channel-header">
    <span class="channel-name">$channel_name</span>
    <span class="channel-id">$channel_id</span>
  </div>
  <div class="streams-grid">
    $stream_cards
  </div>
</div>
"""

STREAM_CARD = """\
<a class="stream-card" href="$href">
  <span class="card-status $status_class">$status_label</span>
  <p class="card-title">$title</p>
  <div class="card-stats">
    <div class="stat-item">
      <div class="stat-label">Peak Viewers</div>
      <div class="stat-value">$peak_viewers</div>
    </div>
    <div class="stat-item">
      <div class="stat-label">Peak Likes</div>
      <div class="stat-value">$peak_likes</div>
    </div>
    <div class="stat-item">
      <div class="stat-label">Comments</div>
      <div class="stat-value">$peak_comments</div>
    </div>
  </div>
  <p class="card-date">$first_seen</p>
</a>
"""

STREAM_PAGE_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>$video_title — Stream Analytics</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Fraunces:ital,opsz,wght@0,9..144,300;0,9..144,700;1,9..144,300&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg:      #0a0a0f;
    --surface: #13131a;
    --border:  #1e1e2e;
    --accent:  #e8ff47;
    --muted:   #5a5a7a;
    --text:    #e2e2f0;
    --red:     #ff4f6d;
    --blue:    #4fc3f7;
    --green:   #47ffb2;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg); color: var(--text);
    font-family: 'DM Mono', monospace;
    min-height: 100vh; padding: 3rem 2rem;
  }
  .noise {
    position: fixed; inset: 0; pointer-events: none; z-index: 0;
    background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.04'/%3E%3C/svg%3E");
    opacity: 0.4;
  }
  .wrap { max-width: 1100px; margin: 0 auto; position: relative; z-index: 1; }
  .back {
    display: inline-flex; align-items: center; gap: 0.4rem;
    font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.15em;
    color: var(--muted); text-decoration: none; margin-bottom: 2.5rem;
    transition: color 0.2s;
  }
  .back:hover { color: var(--accent); }
  .eyebrow { font-size: 0.7rem; letter-spacing: 0.25em; text-transform: uppercase; color: var(--accent); margin-bottom: 0.75rem; }
  h1 { font-family: 'Fraunces', serif; font-size: clamp(1.8rem, 4vw, 3.5rem); font-weight: 700; line-height: 1.1; color: #fff; margin-bottom: 0.5rem; }
  .channel-tag { font-size: 0.75rem; color: var(--muted); margin-bottom: 2.5rem; }
  .channel-tag span { color: var(--text); }

  .kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 1rem; margin-bottom: 2.5rem; }
  .kpi {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 1.25rem; position: relative; overflow: hidden;
  }
  .kpi::after {
    content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 2px;
    background: var(--accent);
  }
  .kpi-label { font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.15em; color: var(--muted); margin-bottom: 0.5rem; }
  .kpi-value { font-family: 'Fraunces', serif; font-size: 2rem; font-weight: 700; color: var(--accent); }
  .kpi-sub { font-size: 0.65rem; color: var(--muted); margin-top: 0.25rem; }

  .chart-box {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 1.5rem; margin-bottom: 2.5rem;
  }
  .chart-title { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.15em; color: var(--muted); margin-bottom: 1.25rem; }
  .chart-wrap { position: relative; height: 280px; }

  .section-title { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.2em; color: var(--muted); margin-bottom: 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid var(--border); }
  .data-table { width: 100%; border-collapse: collapse; font-size: 0.72rem; margin-bottom: 3rem; }
  .data-table th { text-align: left; padding: 0.5rem 0.75rem; color: var(--muted); font-weight: 500; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; border-bottom: 1px solid var(--border); }
  .data-table td { padding: 0.5rem 0.75rem; border-bottom: 1px solid rgba(30,30,46,0.5); color: var(--text); }
  .data-table tr:hover td { background: var(--surface); }
  .data-table .num { text-align: right; color: var(--accent); font-weight: 500; }
  .data-table .ts { color: var(--muted); }
  .pill {
    display: inline-block; font-size: 0.6rem; padding: 0.15rem 0.4rem;
    border-radius: 2px; text-transform: uppercase; letter-spacing: 0.1em;
  }
  .pill-live { background: rgba(255,79,109,0.15); color: var(--red); border: 1px solid var(--red); }
  .pill-upcoming { background: rgba(79,195,247,0.1); color: var(--blue); border: 1px solid var(--blue); }
  .generated { text-align: center; color: var(--muted); font-size: 0.7rem; margin-top: 3rem; }
</style>
</head>
<body>
<div class="noise"></div>
<div class="wrap">
  <a class="back" href="../index.html">&#8592; All streams</a>

  <p class="eyebrow">$channel_name</p>
  <h1>$video_title</h1>
  <p class="channel-tag">Video ID: <span>$video_id</span> &nbsp;·&nbsp; $data_points data points</p>

  <div class="kpi-row">
    <div class="kpi">
      <div class="kpi-label">Peak Viewers</div>
      <div class="kpi-value">$peak_viewers</div>
      <div class="kpi-sub">concurrent</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Peak Likes</div>
      <div class="kpi-value">$peak_likes</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Peak Comments</div>
      <div class="kpi-value">$peak_comments</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">First Seen</div>
      <div class="kpi-value" style="font-size:1.1rem;">$first_seen</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Last Seen</div>
      <div class="kpi-value" style="font-size:1.1rem;">$last_seen</div>
    </div>
  </div>

  <div class="chart-box">
    <div class="chart-title">Concurrent Viewers over Time</div>
    <div class="chart-wrap">
      <canvas id="viewerChart"></canvas>
    </div>
  </div>

  <div class="chart-box">
    <div class="chart-title">Likes &amp; Comments over Time</div>
    <div class="chart-wrap">
      <canvas id="engagementChart"></canvas>
    </div>
  </div>

  <p class="section-title">All collected data points</p>
  <table class="data-table">
    <thead>
      <tr>
        <th>Collected At</th>
        <th>Status</th>
        <th style="text-align:right">Viewers</th>
        <th style="text-align:right">Likes</th>
        <th style="text-align:right">Comments</th>
        <th>Stream Start</th>
      </tr>
    </thead>
    <tbody>
      $table_rows
    </tbody>
  </table>

  <p class="generated">Generated $generated_at &nbsp;·&nbsp; yt-livestream-tracker</p>
  <footer style="
    margin-top: 3rem;
    padding-top: 1.5rem;
    border-top: 1px solid #1e1e2e;
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    gap: 1rem;
    font-size: 0.7rem;
    color: #5a5a7a;
    font-family: 'DM Mono', monospace;
  ">
    <span>&#169; 2026 IDVTuber Tracker &#8212; Non-commercial fan project</span>
    <span>
      <a href="../privacy.html" style="color:#5a5a7a; text-decoration:none;">Privacy Policy</a>
      &nbsp;&middot;&nbsp;
      <a href="../terms.html" style="color:#5a5a7a; text-decoration:none;">Terms of Use</a>
    </span>
  </footer>
</div>

<script>
const ts     = $chart_labels;
const views  = $chart_viewers;
const likes  = $chart_likes;
const comms  = $chart_comments;

const gridColor  = 'rgba(30,30,46,0.8)';
const tickColor  = '#5a5a7a';
const baseOpts = {
  responsive: true, maintainAspectRatio: false,
  interaction: { mode: 'index', intersect: false },
  plugins: { legend: { labels: { color: tickColor, font: { family: 'DM Mono', size: 11 }, boxWidth: 12 } } },
  scales: {
    x: { ticks: { color: tickColor, font: { family: 'DM Mono', size: 10 }, maxTicksLimit: 10, maxRotation: 0 }, grid: { color: gridColor } },
    y: { ticks: { color: tickColor, font: { family: 'DM Mono', size: 10 } }, grid: { color: gridColor } }
  }
};

new Chart(document.getElementById('viewerChart'), {
  type: 'line',
  data: {
    labels: ts,
    datasets: [{
      label: 'Concurrent Viewers',
      data: views,
      borderColor: '#e8ff47',
      backgroundColor: 'rgba(232,255,71,0.07)',
      borderWidth: 2, pointRadius: 2, fill: true, tension: 0.3
    }]
  },
  options: { ...baseOpts }
});

new Chart(document.getElementById('engagementChart'), {
  type: 'line',
  data: {
    labels: ts,
    datasets: [
      {
        label: 'Likes',
        data: likes,
        borderColor: '#ff4f6d',
        backgroundColor: 'rgba(255,79,109,0.05)',
        borderWidth: 2, pointRadius: 2, fill: true, tension: 0.3
      },
      {
        label: 'Comments',
        data: comms,
        borderColor: '#4fc3f7',
        backgroundColor: 'rgba(79,195,247,0.05)',
        borderWidth: 2, pointRadius: 2, fill: true, tension: 0.3
      }
    ]
  },
  options: { ...baseOpts }
});
</script>
</body>
</html>
"""


# ── number formatter ───────────────────────────────────────────────────────────

def fmt(n) -> str:
    if n is None:
        return "—"
    try:
        return f"{int(n):,}"
    except (ValueError, TypeError):
        return str(n)


def fmt_dt(dt) -> str:
    if dt is None:
        return "—"
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    return str(dt)[:16]


# ── build dashboard ────────────────────────────────────────────────────────────

def build_stream_page(
    stream: dict,
    rows: list[dict],
    timeseries: list[dict],
    channel_name: str,
    out_path: Path,
) -> None:
    # chart data
    labels   = [fmt_dt(r["collected_at"]) for r in timeseries]
    viewers  = [int(r["concurrent_viewers"] or 0) for r in timeseries]
    likes    = [int(r["like_count"] or 0)         for r in timeseries]
    comments = [int(r["comment_count"] or 0)       for r in timeseries]

    # table rows
    table_rows_html = ""
    for r in rows:
        status = r.get("stream_status", "")
        pill_class = "pill-live" if status == "live" else "pill-upcoming"
        table_rows_html += f"""
        <tr>
          <td class="ts">{fmt_dt(r['collected_at'])}</td>
          <td><span class="pill {pill_class}">{status}</span></td>
          <td class="num">{fmt(r['concurrent_viewers'])}</td>
          <td class="num">{fmt(r['like_count'])}</td>
          <td class="num">{fmt(r['comment_count'])}</td>
          <td class="ts">{fmt_dt(r.get('actual_start') or r.get('scheduled_start'))}</td>
        </tr>"""

    html = Template(STREAM_PAGE_HTML).substitute(
        video_title    = stream["video_title"] or stream["video_id"],
        video_id       = stream["video_id"],
        channel_name   = channel_name,
        peak_viewers   = fmt(stream["peak_viewers"]),
        peak_likes     = fmt(stream["peak_likes"]),
        peak_comments  = fmt(stream["peak_comments"]),
        data_points    = fmt(stream["data_points"]),
        first_seen     = fmt_dt(stream["first_seen"]),
        last_seen      = fmt_dt(stream["last_seen"]),
        chart_labels   = json.dumps(labels),
        chart_viewers  = json.dumps(viewers),
        chart_likes    = json.dumps(likes),
        chart_comments = json.dumps(comments),
        table_rows     = table_rows_html,
        generated_at   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    )
    out_path.write_text(html, encoding="utf-8")
    log.info("  Written: %s", out_path)


def build_dashboard() -> None:
    conn = psycopg2.connect(AIVEN_DATABASE_URL, sslmode="require")
    channels = get_channel_tables(conn)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_streams = 0
    channel_blocks_html = ""

    for ch in channels:
        table        = ch["table_name"]
        channel_name = ch["channel_name"]
        channel_id   = ch["channel_id"]
        streams      = get_streams_for_channel(conn, table)

        log.info("Channel: %s — %d streams", channel_name, len(streams))

        # per-channel output sub-folder
        ch_dir = OUTPUT_DIR / slugify(channel_name)
        ch_dir.mkdir(exist_ok=True)

        cards_html = ""
        for stream in streams:
            vid      = stream["video_id"]
            slug     = slugify(vid)
            page_file = ch_dir / f"{slug}.html"

            timeseries = get_stream_timeseries(conn, table, vid)
            all_rows   = get_all_rows(conn, table, vid)

            build_stream_page(stream, all_rows, timeseries, channel_name, page_file)

            status = stream.get("stream_status", "vod")
            if status == "live":
                status_class, status_label = "status-live", "Live"
            elif status == "upcoming":
                status_class, status_label = "status-upcoming", "Upcoming"
            else:
                status_class, status_label = "status-vod", "VOD"

            href = f"{slugify(channel_name)}/{slug}.html"
            cards_html += Template(STREAM_CARD).substitute(
                href          = href,
                status_class  = status_class,
                status_label  = status_label,
                title         = (stream["video_title"] or vid)[:80],
                peak_viewers  = fmt(stream["peak_viewers"]),
                peak_likes    = fmt(stream["peak_likes"]),
                peak_comments = fmt(stream["peak_comments"]),
                first_seen    = fmt_dt(stream["first_seen"]),
            )
            total_streams += 1

        if not cards_html:
            cards_html = '<p class="empty">No streams recorded yet.</p>'

        channel_blocks_html += Template(CHANNEL_BLOCK).substitute(
            channel_name = channel_name,
            channel_id   = channel_id,
            stream_cards = cards_html,
        )

    index_html = Template(INDEX_HTML).substitute(
        generated_at    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        total_streams   = total_streams,
        total_channels  = len(channels),
        channel_blocks  = channel_blocks_html,
    )
    (OUTPUT_DIR / "index.html").write_text(index_html, encoding="utf-8")
    log.info("Index written: %s/index.html", OUTPUT_DIR)
    conn.close()
    log.info("Dashboard generation complete. %d streams across %d channels.", total_streams, len(channels))


if __name__ == "__main__":
    build_dashboard()
