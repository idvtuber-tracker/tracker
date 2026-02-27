"""
generate_dashboard.py
Pulls livestream analytics from PostgreSQL and generates a 4-level
static HTML dashboard:
  index.html                          — org cards
  {org}/index.html                    — channel list for org
  {org}/{channel}/index.html          — stream cards for channel
  {org}/{channel}/{video_slug}.html   — stream detail with charts
"""

import os
import re
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from string import Template

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

AIVEN_DATABASE_URL = os.environ.get("AIVEN_DATABASE_URL", "")

# ── Organisation registry ──────────────────────────────────────────────────────
# Maps each channel_name (as stored in DB) to its org slug and display label.
# Add new channels here when expanding.
CHANNEL_ORG_MAP = {
    # PANDAVVA
    "PANDAVVA":           "pandavva",
    "Yudistira Yogendra": "pandavva",
    "Bima Bayusena":      "pandavva",
    "Arjuna Arkana":      "pandavva",
    "Nakula Narenda":     "pandavva",
    "Sadewa Sagara":      "pandavva",
    # Project:LIVIUM
    "Project:LIVIUM":        "project-livium",
    "Indira Naylarissa":     "project-livium",
    "Silvia Valleria":       "project-livium",
    "Yuura Yozakura":        "project-livium",
    "Ymelia Meiru":          "project-livium",
    "Fareye Closhartt":      "project-livium",
    "Yuela GuiGui":          "project-livium",
    "Lillis Infernallies":   "project-livium",
    # Whicker Butler
    "Whicker Butler":        "whicker-butler",
    "Valthea Nankila":       "whicker-butler",
    "Ignis Grimoire":        "whicker-butler",
    "Darlyne Nightbloom":    "whicker-butler",
    "Thalita Sylvaine":      "whicker-butler",
    "Oriana Solstair":       "whicker-butler",
}

ORG_META = {
    "pandavva": {
        "label": "PANDAVVA",
        "color": "#e8ff47",
        "desc":  "An Indonesian VTuber organization with a rich mythological theme inspired by the Mahabharata.",
    },
    "project-livium": {
        "label": "Project:LIVIUM",
        "color": "#47ffb2",
        "desc":  "A dynamic VTuber project featuring eight unique talents spanning a wide range of creative personalities.",
    },
    "whicker-butler": {
        "label": "Whicker Butler",
        "color": "#b47fff",
        "desc":  "A boutique VTuber agency known for its refined aesthetic and five distinctive talents with global appeal.",
    },
}

# Ordered org list for index page display
ORG_ORDER = ["pandavva", "project-livium", "whicker-butler"]

# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        AIVEN_DATABASE_URL,
        sslmode="require",
        options="-c search_path=public -c statement_timeout=30000",
    )


def get_channel_tables(conn) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT channel_id, channel_name, table_name, added_at "
            "FROM channels ORDER BY channel_name"
        )
        return cur.fetchall()


def get_streams_for_channel(conn, table: str) -> list[dict]:
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
            SELECT collected_at, concurrent_viewers, like_count, comment_count
            FROM {table}
            WHERE video_id = %s
            ORDER BY collected_at
        """, (video_id,))
        return cur.fetchall()


def get_all_rows(conn, table: str, video_id: str) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT * FROM {table}
            WHERE video_id = %s
            ORDER BY collected_at DESC
        """, (video_id,))
        return cur.fetchall()


# ── Helpers ────────────────────────────────────────────────────────────────────

def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


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


def now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ── Shared CSS & snippets ──────────────────────────────────────────────────────

FONTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link href="https://fonts.googleapis.com/css2?family=DM+Mono:ital,wght@0,400;0,500;1,400'
    '&family=Fraunces:ital,opsz,wght@0,9..144,300;0,9..144,700;1,9..144,400'
    '&display=swap" rel="stylesheet">'
)

BASE_CSS = """\
  :root {
    --bg:       #0a0a0f;
    --surface:  #13131a;
    --surface2: #1a1a26;
    --border:   #1e1e2e;
    --accent:   #e8ff47;
    --muted:    #5a5a7a;
    --text:     #e2e2f0;
    --white:    #ffffff;
    --red:      #ff4f6d;
    --blue:     #4fc3f7;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body {
    background: var(--bg); color: var(--text);
    font-family: 'DM Mono', monospace;
    font-size: 14px; line-height: 1.7;
    min-height: 100vh; overflow-x: hidden;
  }
  body::before {
    content: ''; position: fixed; inset: 0;
    pointer-events: none; z-index: 0;
    background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 200 200' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.035'/%3E%3C/svg%3E");
    opacity: 0.5;
  }
  .page { position: relative; z-index: 1; max-width: 1100px; margin: 0 auto; padding: 0 2rem 6rem; }

  /* breadcrumb */
  .breadcrumb {
    display: flex; align-items: center; flex-wrap: wrap; gap: 0.4rem;
    padding: 1.5rem 0; font-size: 0.68rem;
    letter-spacing: 0.12em; text-transform: uppercase; color: var(--muted);
    border-bottom: 1px solid var(--border); margin-bottom: 3rem;
  }
  .breadcrumb a { color: var(--muted); text-decoration: none; transition: color 0.2s; }
  .breadcrumb a:hover { color: var(--org-color, var(--accent)); }
  .breadcrumb .sep { color: var(--border); }
  .breadcrumb .current { color: var(--org-color, var(--accent)); }

  /* typography */
  .eyebrow {
    font-size: 0.65rem; letter-spacing: 0.3em; text-transform: uppercase;
    color: var(--org-color, var(--accent)); margin-bottom: 0.6rem;
  }
  h1 {
    font-family: 'Fraunces', serif;
    font-size: clamp(2rem, 5vw, 4.5rem);
    font-weight: 700; line-height: 1.0; color: var(--white);
    margin-bottom: 0.5rem;
  }
  h1 em { font-style: italic; color: var(--org-color, var(--accent)); }
  h2 { font-family: 'Fraunces', serif; font-size: 1.3rem; font-weight: 700; color: var(--white); }
  .page-meta { font-size: 0.72rem; color: var(--muted); margin-top: 0.75rem; }

  /* org cards — index page */
  .orgs-grid {
    display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
    gap: 1.5rem; margin-top: 3rem;
  }
  .org-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 2rem;
    text-decoration: none; color: inherit; display: block;
    transition: border-color 0.2s, transform 0.2s;
    position: relative; overflow: hidden;
  }
  .org-card::after {
    content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 2px;
    background: var(--org-color, var(--accent)); transform: scaleX(0);
    transform-origin: left; transition: transform 0.35s;
  }
  .org-card:hover { border-color: var(--org-color, var(--accent)); transform: translateY(-3px); }
  .org-card:hover::after { transform: scaleX(1); }
  .org-dot {
    width: 10px; height: 10px; border-radius: 50%;
    background: var(--org-color, var(--accent)); margin-bottom: 1.25rem;
    box-shadow: 0 0 12px var(--org-color, var(--accent));
  }
  .org-title { font-family: 'Fraunces', serif; font-size: 1.6rem; font-weight: 700; color: var(--white); margin-bottom: 0.5rem; }
  .org-desc { font-size: 0.78rem; color: var(--muted); margin-bottom: 1.25rem; line-height: 1.6; }
  .org-stat { font-size: 0.72rem; color: var(--muted); }
  .org-stat strong { color: var(--org-color, var(--accent)); }

  /* channel list — org page */
  .channels-list { display: flex; flex-direction: column; gap: 0.75rem; margin-top: 2.5rem; }
  .channel-item {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 1.25rem 1.5rem;
    text-decoration: none; color: inherit;
    display: flex; align-items: center; justify-content: space-between;
    transition: border-color 0.2s, background 0.2s;
  }
  .channel-item:hover { border-color: var(--org-color, var(--accent)); background: var(--surface2); }
  .channel-item-left { display: flex; align-items: center; gap: 1rem; }
  .channel-badge {
    font-size: 0.6rem; letter-spacing: 0.15em; text-transform: uppercase;
    padding: 0.2rem 0.5rem; border-radius: 2px;
    border: 1px solid var(--org-color, var(--accent));
    color: var(--org-color, var(--accent));
    background: rgba(0,0,0,0.3); flex-shrink: 0;
  }
  .channel-name-text { font-size: 0.95rem; color: var(--white); }
  .channel-meta { font-size: 0.7rem; color: var(--muted); }
  .channel-arrow { color: var(--muted); font-size: 0.8rem; transition: transform 0.2s, color 0.2s; }
  .channel-item:hover .channel-arrow { transform: translateX(4px); color: var(--org-color, var(--accent)); }

  /* stream cards — channel page */
  .streams-grid {
    display: grid; grid-template-columns: repeat(auto-fill, minmax(290px, 1fr));
    gap: 1rem; margin-top: 2.5rem;
  }
  .stream-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 1.25rem;
    text-decoration: none; color: inherit; display: block;
    transition: border-color 0.2s, transform 0.2s;
    position: relative; overflow: hidden;
  }
  .stream-card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: var(--org-color, var(--accent)); transform: scaleX(0);
    transform-origin: left; transition: transform 0.3s;
  }
  .stream-card:hover { border-color: var(--org-color, var(--accent)); transform: translateY(-2px); }
  .stream-card:hover::before { transform: scaleX(1); }
  .stream-status {
    display: inline-block; font-size: 0.6rem; letter-spacing: 0.15em;
    text-transform: uppercase; padding: 0.2rem 0.5rem; border-radius: 2px;
    margin-bottom: 0.75rem;
  }
  .status-live     { background: rgba(255,79,109,0.15); color: var(--red);  border: 1px solid var(--red); }
  .status-upcoming { background: rgba(79,195,247,0.10); color: var(--blue); border: 1px solid var(--blue); }
  .status-vod      { background: rgba(90,90,122,0.20);  color: var(--muted); border: 1px solid var(--muted); }
  .stream-title {
    font-family: 'Fraunces', serif; font-size: 1rem; font-weight: 700;
    color: var(--white); margin-bottom: 1rem; line-height: 1.3;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
  }
  .stream-stats { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.5rem; }
  .stat-label { font-size: 0.58rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.1em; }
  .stat-value { font-size: 0.9rem; color: var(--org-color, var(--accent)); font-weight: 500; }
  .stream-date { font-size: 0.62rem; color: var(--muted); margin-top: 1rem; }
  .empty { color: var(--muted); font-size: 0.8rem; font-style: italic; padding: 1rem 0; }

  /* stream detail — KPIs */
  .kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 1rem; margin: 2.5rem 0; }
  .kpi {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 1.25rem; position: relative; overflow: hidden;
  }
  .kpi::after {
    content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 2px;
    background: var(--org-color, var(--accent));
  }
  .kpi-label { font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.15em; color: var(--muted); margin-bottom: 0.5rem; }
  .kpi-value { font-family: 'Fraunces', serif; font-size: 2rem; font-weight: 700; color: var(--org-color, var(--accent)); }
  .kpi-sub { font-size: 0.65rem; color: var(--muted); margin-top: 0.25rem; }

  /* stream detail — charts */
  .chart-box {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 1.5rem; margin-bottom: 2.5rem;
  }
  .chart-title { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.15em; color: var(--muted); margin-bottom: 1.25rem; }
  .chart-wrap { position: relative; height: 280px; }

  /* stream detail — data table */
  .section-title {
    font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.2em;
    color: var(--muted); margin-bottom: 1rem; padding-bottom: 0.5rem;
    border-bottom: 1px solid var(--border);
  }
  .data-table { width: 100%; border-collapse: collapse; font-size: 0.72rem; margin-bottom: 3rem; }
  .data-table th {
    text-align: left; padding: 0.5rem 0.75rem; color: var(--muted);
    font-weight: 500; font-size: 0.65rem; text-transform: uppercase;
    letter-spacing: 0.1em; border-bottom: 1px solid var(--border);
  }
  .data-table td { padding: 0.5rem 0.75rem; border-bottom: 1px solid rgba(30,30,46,0.5); color: var(--text); }
  .data-table tr:hover td { background: var(--surface); }
  .data-table .num { text-align: right; color: var(--org-color, var(--accent)); font-weight: 500; }
  .data-table .ts { color: var(--muted); }
  .pill { display: inline-block; font-size: 0.6rem; padding: 0.15rem 0.4rem; border-radius: 2px; text-transform: uppercase; letter-spacing: 0.1em; }
  .pill-live     { background: rgba(255,79,109,0.15); color: var(--red);  border: 1px solid var(--red); }
  .pill-upcoming { background: rgba(79,195,247,0.10); color: var(--blue); border: 1px solid var(--blue); }

  /* footer */
  footer {
    margin-top: 5rem; padding-top: 2rem;
    border-top: 1px solid var(--border);
    display: flex; flex-wrap: wrap; justify-content: space-between;
    gap: 1rem; font-size: 0.7rem; color: var(--muted);
  }
  footer a { color: var(--muted); text-decoration: none; transition: color 0.2s; }
  footer a:hover { color: var(--org-color, var(--accent)); }

  /* animations */
  @keyframes fadeUp {
    from { opacity: 0; transform: translateY(14px); }
    to   { opacity: 1; transform: translateY(0); }
  }
  header { animation: fadeUp 0.5s ease both; }
  .orgs-grid, .channels-list, .streams-grid, .kpi-row { animation: fadeUp 0.5s 0.1s ease both; }
"""


def page_head(title: str, org_color: str, extra_scripts: str = "") -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — IDVTuber Tracker</title>
{FONTS}
{extra_scripts}
<style>
{BASE_CSS}
  :root {{ --org-color: {org_color}; }}
</style>
</head>
<body>
<div class="page">
"""


def breadcrumb_html(crumbs: list[tuple]) -> str:
    """crumbs = [(label, href_or_None), ...]  last entry = current page"""
    parts = []
    for i, (label, href) in enumerate(crumbs):
        if i == len(crumbs) - 1:
            parts.append(f'<span class="current">{label}</span>')
        else:
            parts.append(f'<a href="{href}">{label}</a>')
        if i < len(crumbs) - 1:
            parts.append('<span class="sep">›</span>')
    return '<nav class="breadcrumb">' + " ".join(parts) + "</nav>\n"


def footer_html(root: str) -> str:
    return f"""  <footer>
    <span>&#169; 2026 IDVTuber Tracker &#8212; Non-commercial fan project</span>
    <span>
      <a href="{root}index.html">Home</a> &nbsp;&middot;&nbsp;
      <a href="{root}privacy.html">Privacy Policy</a> &nbsp;&middot;&nbsp;
      <a href="{root}terms.html">Terms of Use</a>
    </span>
  </footer>
</div>
</body>
</html>"""


# ── Page builders ──────────────────────────────────────────────────────────────

def build_index(output_dir: Path, org_channels: dict, total_streams: int) -> None:
    """Top-level index — one card per organisation."""
    org_cards_html = ""
    for org_slug in ORG_ORDER:
        meta      = ORG_META[org_slug]
        color     = meta["color"]
        label     = meta["label"]
        desc      = meta["desc"]
        n_ch      = len(org_channels.get(org_slug, []))
        n_streams = sum(s["_stream_count"] for s in org_channels.get(org_slug, []))
        org_cards_html += f"""
  <a class="org-card" href="{org_slug}/index.html" style="--org-color:{color}">
    <div class="org-dot"></div>
    <div class="org-title">{label}</div>
    <div class="org-desc">{desc}</div>
    <div class="org-stat">
      <strong>{n_ch}</strong> channels &nbsp;·&nbsp; <strong>{n_streams}</strong> streams recorded
    </div>
  </a>"""

    html = (
        page_head("Stream Analytics", "#e8ff47")
        + f"""  <header>
    <p class="eyebrow">IDVTuber Tracker — Live Analytics</p>
    <h1>Stream <em>Overview</em></h1>
    <p class="page-meta">Generated: {now_str()} &nbsp;·&nbsp; {total_streams} streams across 3 organisations</p>
  </header>
  <div class="orgs-grid">{org_cards_html}
  </div>
"""
        + footer_html("")
    )
    (output_dir / "index.html").write_text(html, encoding="utf-8")
    log.info("Written: index.html")


def build_org_page(output_dir: Path, org_slug: str, channels: list[dict]) -> None:
    """Organisation page — list of channels belonging to this org."""
    meta    = ORG_META[org_slug]
    color   = meta["color"]
    label   = meta["label"]
    org_dir = output_dir / org_slug
    org_dir.mkdir(exist_ok=True)

    items_html = ""
    for ch in channels:
        ch_name  = ch["channel_name"]
        ch_slug  = slugify(ch_name)
        badge    = "ORG CH" if ch_name == label or ch_name == ch.get("_org_name") else "TALENT"
        n_streams = ch["_stream_count"]
        items_html += f"""
  <a class="channel-item" href="{ch_slug}/index.html" style="--org-color:{color}">
    <div class="channel-item-left">
      <span class="channel-badge">{badge}</span>
      <div>
        <div class="channel-name-text">{ch_name}</div>
        <div class="channel-meta">{n_streams} streams recorded</div>
      </div>
    </div>
    <span class="channel-arrow">→</span>
  </a>"""

    bc  = breadcrumb_html([("Home", "../index.html"), (label, None)])
    html = (
        page_head(label, color)
        + bc
        + f"""  <header>
    <p class="eyebrow">Organisation</p>
    <h1>{label}</h1>
    <p class="page-meta">{len(channels)} channels · select a channel to view stream analytics</p>
  </header>
  <div class="channels-list">{items_html}
  </div>
"""
        + footer_html("../")
    )
    (org_dir / "index.html").write_text(html, encoding="utf-8")
    log.info("Written: %s/index.html", org_slug)


def build_channel_page(
    output_dir: Path,
    org_slug: str,
    ch_name: str,
    streams: list[dict],
) -> None:
    """Channel page — grid of stream cards."""
    meta     = ORG_META[org_slug]
    color    = meta["color"]
    org_lbl  = meta["label"]
    ch_slug  = slugify(ch_name)
    ch_dir   = output_dir / org_slug / ch_slug
    ch_dir.mkdir(parents=True, exist_ok=True)

    cards_html = ""
    for stream in streams:
        vid    = stream["video_id"]
        v_slug = slugify(vid)
        status = stream.get("stream_status", "vod") or "vod"
        if status == "live":
            s_cls, s_lbl = "status-live", "🔴 Live"
        elif status == "upcoming":
            s_cls, s_lbl = "status-upcoming", "Upcoming"
        else:
            s_cls, s_lbl = "status-vod", "VOD"

        title = (stream["video_title"] or vid)[:80]
        cards_html += f"""
  <a class="stream-card" href="{v_slug}.html" style="--org-color:{color}">
    <span class="stream-status {s_cls}">{s_lbl}</span>
    <div class="stream-title">{title}</div>
    <div class="stream-stats">
      <div><div class="stat-label">Peak viewers</div><div class="stat-value">{fmt(stream["peak_viewers"])}</div></div>
      <div><div class="stat-label">Peak likes</div><div class="stat-value">{fmt(stream["peak_likes"])}</div></div>
      <div><div class="stat-label">Comments</div><div class="stat-value">{fmt(stream["peak_comments"])}</div></div>
    </div>
    <div class="stream-date">{fmt_dt(stream["first_seen"])}</div>
  </a>"""

    if not cards_html:
        cards_html = '<p class="empty">No streams recorded yet.</p>'

    bc   = breadcrumb_html([
        ("Home",    "../../index.html"),
        (org_lbl,   "../index.html"),
        (ch_name,   None),
    ])
    html = (
        page_head(ch_name, color)
        + bc
        + f"""  <header>
    <p class="eyebrow">{org_lbl}</p>
    <h1>{ch_name}</h1>
    <p class="page-meta">{len(streams)} streams recorded · sorted by date</p>
  </header>
  <div class="streams-grid">{cards_html}
  </div>
"""
        + footer_html("../../")
    )
    (ch_dir / "index.html").write_text(html, encoding="utf-8")
    log.info("  Written: %s/%s/index.html", org_slug, ch_slug)


def build_stream_page(
    output_dir: Path,
    org_slug: str,
    ch_name: str,
    stream: dict,
    rows: list[dict],
    timeseries: list[dict],
) -> None:
    """Stream detail page — KPIs, charts, data table."""
    meta    = ORG_META[org_slug]
    color   = meta["color"]
    org_lbl = meta["label"]
    ch_slug = slugify(ch_name)
    vid     = stream["video_id"]
    v_slug  = slugify(vid)
    title   = stream["video_title"] or vid

    # chart data
    labels   = [fmt_dt(r["collected_at"]) for r in timeseries]
    viewers  = [int(r["concurrent_viewers"] or 0) for r in timeseries]
    likes    = [int(r["like_count"]         or 0) for r in timeseries]
    comments = [int(r["comment_count"]      or 0) for r in timeseries]

    # data table rows
    table_rows_html = ""
    for r in rows:
        status     = r.get("stream_status", "")
        pill_cls   = "pill-live" if status == "live" else "pill-upcoming"
        table_rows_html += f"""
      <tr>
        <td class="ts">{fmt_dt(r['collected_at'])}</td>
        <td><span class="pill {pill_cls}">{status}</span></td>
        <td class="num">{fmt(r['concurrent_viewers'])}</td>
        <td class="num">{fmt(r['like_count'])}</td>
        <td class="num">{fmt(r['comment_count'])}</td>
        <td class="ts">{fmt_dt(r.get('actual_start') or r.get('scheduled_start'))}</td>
      </tr>"""

    chart_script = '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>'
    bc = breadcrumb_html([
        ("Home",              "../../../index.html"),
        (org_lbl,             "../../index.html"),
        (ch_name,             "../index.html"),
        (title[:40] + ("…" if len(title) > 40 else ""), None),
    ])

    html = (
        page_head(title[:60], color, chart_script)
        + bc
        + f"""  <header>
    <p class="eyebrow">{org_lbl} · {ch_name}</p>
    <h1>{title}</h1>
    <p class="page-meta">Video ID: {vid} &nbsp;·&nbsp; {fmt(stream["data_points"])} data points</p>
  </header>

  <div class="kpi-row">
    <div class="kpi">
      <div class="kpi-label">Peak Viewers</div>
      <div class="kpi-value">{fmt(stream["peak_viewers"])}</div>
      <div class="kpi-sub">concurrent</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Peak Likes</div>
      <div class="kpi-value">{fmt(stream["peak_likes"])}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Peak Comments</div>
      <div class="kpi-value">{fmt(stream["peak_comments"])}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">First Seen</div>
      <div class="kpi-value" style="font-size:1.1rem;">{fmt_dt(stream["first_seen"])}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Last Seen</div>
      <div class="kpi-value" style="font-size:1.1rem;">{fmt_dt(stream["last_seen"])}</div>
    </div>
  </div>

  <div class="chart-box">
    <div class="chart-title">Concurrent Viewers over Time</div>
    <div class="chart-wrap"><canvas id="viewerChart"></canvas></div>
  </div>

  <div class="chart-box">
    <div class="chart-title">Likes &amp; Comments over Time</div>
    <div class="chart-wrap"><canvas id="engagementChart"></canvas></div>
  </div>

  <p class="section-title">All collected data points</p>
  <table class="data-table">
    <thead>
      <tr>
        <th>Collected At</th><th>Status</th>
        <th style="text-align:right">Viewers</th>
        <th style="text-align:right">Likes</th>
        <th style="text-align:right">Comments</th>
        <th>Stream Start</th>
      </tr>
    </thead>
    <tbody>{table_rows_html}
    </tbody>
  </table>

  <p style="text-align:center;color:var(--muted);font-size:0.7rem;margin-top:3rem;">
    Generated {now_str()} &nbsp;·&nbsp; yt-livestream-tracker
  </p>

<script>
const ts    = {json.dumps(labels)};
const views = {json.dumps(viewers)};
const likes = {json.dumps(likes)};
const comms = {json.dumps(comments)};
const gridColor = 'rgba(30,30,46,0.8)';
const tickColor = '#5a5a7a';
const accentColor = '{color}';
const baseOpts = {{
  responsive: true, maintainAspectRatio: false,
  interaction: {{ mode: 'index', intersect: false }},
  plugins: {{ legend: {{ labels: {{ color: tickColor, font: {{ family: 'DM Mono', size: 11 }}, boxWidth: 12 }} }} }},
  scales: {{
    x: {{ ticks: {{ color: tickColor, font: {{ family: 'DM Mono', size: 10 }}, maxTicksLimit: 10, maxRotation: 0 }}, grid: {{ color: gridColor }} }},
    y: {{ ticks: {{ color: tickColor, font: {{ family: 'DM Mono', size: 10 }} }}, grid: {{ color: gridColor }} }}
  }}
}};
new Chart(document.getElementById('viewerChart'), {{
  type: 'line',
  data: {{ labels: ts, datasets: [{{
    label: 'Concurrent Viewers', data: views,
    borderColor: accentColor,
    backgroundColor: accentColor.replace(')', ', 0.07)').replace('rgb', 'rgba'),
    borderWidth: 2, pointRadius: 2, fill: true, tension: 0.3
  }}] }},
  options: {{ ...baseOpts }}
}});
new Chart(document.getElementById('engagementChart'), {{
  type: 'line',
  data: {{ labels: ts, datasets: [
    {{ label: 'Likes', data: likes, borderColor: '#ff4f6d', backgroundColor: 'rgba(255,79,109,0.05)', borderWidth: 2, pointRadius: 2, fill: true, tension: 0.3 }},
    {{ label: 'Comments', data: comms, borderColor: '#4fc3f7', backgroundColor: 'rgba(79,195,247,0.05)', borderWidth: 2, pointRadius: 2, fill: true, tension: 0.3 }}
  ] }},
  options: {{ ...baseOpts }}
}});
</script>
"""
        + footer_html("../../../")
    )

    out_path = output_dir / org_slug / ch_slug / f"{v_slug}.html"
    out_path.write_text(html, encoding="utf-8")
    log.info("    Written: %s/%s/%s.html", org_slug, ch_slug, v_slug)


# ── Main orchestrator ──────────────────────────────────────────────────────────

def build_dashboard() -> None:
    if not AIVEN_DATABASE_URL:
        print("ERROR: AIVEN_DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    conn       = get_conn()
    channels   = get_channel_tables(conn)
    output_dir = Path(os.environ.get("DASHBOARD_OUTPUT_DIR", "dashboard"))
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── copy static legal pages ────────────────────────────────────────────────
    for legal_file in ["privacy.html", "terms.html"]:
        src = Path(legal_file)
        dst = output_dir / legal_file
        if src.exists():
            shutil.copy2(src, dst)
            print(f"Copied {legal_file} to {output_dir}/")
        else:
            print(f"Warning: {legal_file} not found at repo root — skipping")

    # ── group channels by org ──────────────────────────────────────────────────
    org_channels: dict[str, list[dict]] = {slug: [] for slug in ORG_ORDER}
    unmatched: list[str] = []

    for ch in channels:
        ch_name  = ch["channel_name"]
        org_slug = CHANNEL_ORG_MAP.get(ch_name)
        if org_slug is None:
            log.warning("Channel '%s' not in CHANNEL_ORG_MAP — skipping", ch_name)
            unmatched.append(ch_name)
            continue

        streams = get_streams_for_channel(conn, ch["table_name"])
        log.info("Channel: %s (%s) — %d streams", ch_name, org_slug, len(streams))

        ch_entry = dict(ch)
        ch_entry["_stream_count"] = len(streams)
        ch_entry["_streams"]      = streams
        ch_entry["_org_name"]     = ORG_META[org_slug]["label"]
        org_channels[org_slug].append(ch_entry)

    if unmatched:
        log.warning("Unmatched channels (not assigned to any org): %s", unmatched)

    # ── build pages ────────────────────────────────────────────────────────────
    total_streams = 0

    for org_slug in ORG_ORDER:
        ch_list = org_channels[org_slug]
        if not ch_list:
            log.warning("No channels found for org: %s", org_slug)
            continue

        # org index page
        build_org_page(output_dir, org_slug, ch_list)

        for ch_entry in ch_list:
            ch_name = ch_entry["channel_name"]
            streams = ch_entry["_streams"]
            table   = ch_entry["table_name"]

            # channel index page
            build_channel_page(output_dir, org_slug, ch_name, streams)

            # stream detail pages
            for stream in streams:
                vid        = stream["video_id"]
                timeseries = get_stream_timeseries(conn, table, vid)
                all_rows   = get_all_rows(conn, table, vid)
                build_stream_page(output_dir, org_slug, ch_name, stream, all_rows, timeseries)
                total_streams += 1

    # ── top-level index ────────────────────────────────────────────────────────
    build_index(output_dir, org_channels, total_streams)

    conn.close()
    log.info("Dashboard complete — %d streams across %d channels.", total_streams, len(channels))


if __name__ == "__main__":
    build_dashboard()
