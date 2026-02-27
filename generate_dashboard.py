"""
generate_dashboard.py
Pulls livestream analytics from PostgreSQL and generates a 4-level
static HTML dashboard:
  index.html                       ← org cards
  {org}/index.html                 ← channel list per org
  {org}/{channel}/index.html       ← stream cards per channel
  {org}/{channel}/{video}.html     ← stream detail + charts

Org membership is driven by the ORG_MAP dict below — update it when
channels are added or moved between organisations.
"""

import os
import re
import json
import shutil
import logging
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────
AIVEN_DATABASE_URL = os.environ.get("AIVEN_DATABASE_URL", "")
OUTPUT_DIR         = Path(os.environ.get("DASHBOARD_OUTPUT_DIR", "dashboard"))

# ── org definitions ───────────────────────────────────────────────────────────
# Keys must match channel_name values in the `channels` DB table exactly.
# "type" = "org" for the organisation's own channel, "talent" for talent channels.
# Add / remove channels here as the roster grows.

ORG_MAP = {
    "pandavva": {
        "label":   "PANDAVVA",
        "color":   "#e8ff47",
        "desc":    "An Indonesian VTuber organization with a rich mythological theme inspired by the Mahabharata.",
        "channels": [
            ("PANDAVVA Official",             "org"),
            ("Yudistira Yogendra【PANDAVVA】", "talent"),
            ("Bima Bayusena【PANDAVVA】",      "talent"),   # not yet in DB
            ("Arjuna Arkana【PANDAVVA】",      "talent"),   # not yet in DB
            ("Nakula Narenda【PANDAVVA】",     "talent"),   # not yet in DB
            ("Sadewa Sagara【PANDAVVA】",      "talent"),
        ],
    },
    "project-livium": {
        "label":   "Project:LIVIUM",
        "color":   "#47ffb2",
        "desc":    "A dynamic VTuber project featuring seven unique talents spanning a wide range of creative personalities.",
        "channels": [
            ("Project:LIVIUM",                             "org"),    # not yet in DB
            ("Indira Naylarissa Ch.〔LiviPro〕",    "talent"),
            ("Silvia Valleria Ch.〔LiviPro〕",      "talent"), # not yet in DB
            ("Yuura Yozakura Ch.〔LiviPro〕",       "talent"),
            ("Ymelia Meiru Ch.〔LiviPro〕",         "talent"), # not yet in DB
            ("Fareye Closhartt Ch.〔LiviPro〕",     "talent"), # not yet in DB
            ("Yuela GuiGui Ch.〔LiviPro〕",         "talent"),
            ("Lillis Infernallies Ch.〔LiviPro〕",  "talent"),
        ],
    },
    "whicker-butler": {
        "label":   "Whicker Butler",
        "color":   "#b47fff",
        "desc":    "A boutique VTuber agency known for its refined aesthetic and five distinctive talents with global appeal.",
        "channels": [
            ("Whicker Butler",                             "org"),    # not yet in DB
            ("Valthea Nankila 【 Whicker Butler 】",   "talent"),
            ("Ignis Grimoire【Whicker Butler】",        "talent"),
            ("Darlyne Nightbloom【Whicker Butler】",    "talent"),
            ("Thalita Sylvaine【Whicker Butler】",      "talent"),
            ("Oriana Solstair【Whicker Butler】",       "talent"),
        ],
    },
}


# ── DB helpers ─────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        AIVEN_DATABASE_URL,
        sslmode="require",
        options="-c search_path=public -c statement_timeout=30000",
    )


def get_channel_rows(conn) -> list[dict]:
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


# ── helpers ───────────────────────────────────────────────────────────────────
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


def esc(s) -> str:
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))


# ── shared CSS + fonts ────────────────────────────────────────────────────────
_FONTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link href="https://fonts.googleapis.com/css2?family=DM+Mono:ital,wght'
    '@0,400;0,500;1,400&family=Fraunces:ital,opsz,wght@0,9..144,300;0,9..144'
    ',700;1,9..144,400&display=swap" rel="stylesheet">'
)

_BASE_CSS = """
  :root {
    --bg:        #0a0a0f;
    --surface:   #13131a;
    --surface2:  #1a1a26;
    --border:    #1e1e2e;
    --muted:     #5a5a7a;
    --text:      #e2e2f0;
    --white:     #ffffff;
    --red:       #ff4f6d;
    --blue:      #4fc3f7;
    --org-color: #e8ff47;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body {
    background: var(--bg); color: var(--text);
    font-family: 'DM Mono', monospace;
    font-size: 14px; line-height: 1.7; min-height: 100vh;
    overflow-x: hidden;
  }
  body::before {
    content: ''; position: fixed; inset: 0; pointer-events: none; z-index: 0;
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
  .breadcrumb a:hover { color: var(--org-color); }
  .breadcrumb .sep { color: var(--border); }
  .breadcrumb .current { color: var(--org-color); }

  /* headings */
  .eyebrow {
    font-size: 0.65rem; letter-spacing: 0.3em; text-transform: uppercase;
    color: var(--org-color); margin-bottom: 0.6rem;
  }
  h1 {
    font-family: 'Fraunces', serif;
    font-size: clamp(2rem, 5vw, 4.5rem);
    font-weight: 700; line-height: 1.0; color: var(--white); margin-bottom: 0.5rem;
  }
  h1 em { font-style: italic; color: var(--org-color); }
  .page-meta { font-size: 0.72rem; color: var(--muted); margin-top: 0.75rem; }

  /* org cards */
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
    background: var(--org-color); transform: scaleX(0);
    transform-origin: left; transition: transform 0.35s;
  }
  .org-card:hover { border-color: var(--org-color); transform: translateY(-3px); }
  .org-card:hover::after { transform: scaleX(1); }
  .org-dot {
    width: 10px; height: 10px; border-radius: 50%;
    background: var(--org-color); margin-bottom: 1.25rem;
    box-shadow: 0 0 12px var(--org-color);
  }
  .org-title { font-family: 'Fraunces', serif; font-size: 1.6rem; font-weight: 700; color: var(--white); margin-bottom: 0.5rem; }
  .org-desc { font-size: 0.78rem; color: var(--muted); margin-bottom: 1.25rem; line-height: 1.6; }
  .org-stat { font-size: 0.72rem; color: var(--muted); }
  .org-stat strong { color: var(--org-color); }

  /* channel list */
  .channels-list { display: flex; flex-direction: column; gap: 0.75rem; margin-top: 2.5rem; }
  .channel-item {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 1.25rem 1.5rem;
    text-decoration: none; color: inherit;
    display: flex; align-items: center; justify-content: space-between;
    transition: border-color 0.2s, background 0.2s;
  }
  .channel-item:hover { border-color: var(--org-color); background: var(--surface2); }
  .channel-item-left { display: flex; align-items: center; gap: 1rem; }
  .channel-badge {
    font-size: 0.6rem; letter-spacing: 0.15em; text-transform: uppercase;
    padding: 0.2rem 0.5rem; border-radius: 2px;
    border: 1px solid var(--org-color); color: var(--org-color);
    background: rgba(0,0,0,0.3); flex-shrink: 0;
  }
  .channel-name-text { font-size: 0.95rem; color: var(--white); }
  .channel-arrow { color: var(--muted); font-size: 0.8rem; transition: transform 0.2s, color 0.2s; }
  .channel-item:hover .channel-arrow { transform: translateX(4px); color: var(--org-color); }

  /* stream cards */
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
    background: var(--org-color); transform: scaleX(0);
    transform-origin: left; transition: transform 0.3s;
  }
  .stream-card:hover { border-color: var(--org-color); transform: translateY(-2px); }
  .stream-card:hover::before { transform: scaleX(1); }
  .stream-status {
    display: inline-block; font-size: 0.6rem; letter-spacing: 0.15em;
    text-transform: uppercase; padding: 0.2rem 0.5rem; border-radius: 2px;
    margin-bottom: 0.75rem;
  }
  .status-live     { background: rgba(255,79,109,0.15); color: var(--red);   border: 1px solid var(--red); }
  .status-upcoming { background: rgba(79,195,247,0.10); color: var(--blue);  border: 1px solid var(--blue); }
  .status-vod      { background: rgba(90,90,122,0.20);  color: var(--muted); border: 1px solid var(--muted); }
  .stream-title {
    font-family: 'Fraunces', serif; font-size: 1rem; font-weight: 700;
    color: var(--white); margin-bottom: 1rem; line-height: 1.3;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
  }
  .stream-stats { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.5rem; }
  .stat-label { font-size: 0.58rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.1em; }
  .stat-value { font-size: 0.9rem; color: var(--org-color); font-weight: 500; }
  .stream-date { font-size: 0.62rem; color: var(--muted); margin-top: 1rem; }
  .empty { color: var(--muted); font-size: 0.8rem; font-style: italic; padding: 1rem 0; }

  /* stream detail */
  .kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 1rem; margin-top: 2.5rem; margin-bottom: 2.5rem; }
  .kpi {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 1.25rem; position: relative; overflow: hidden;
  }
  .kpi::after {
    content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 2px;
    background: var(--org-color);
  }
  .kpi-label { font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.15em; color: var(--muted); margin-bottom: 0.5rem; }
  .kpi-value { font-family: 'Fraunces', serif; font-size: 2rem; font-weight: 700; color: var(--org-color); }
  .kpi-sub   { font-size: 0.65rem; color: var(--muted); margin-top: 0.25rem; }
  .chart-box {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 1.5rem; margin-bottom: 2.5rem;
  }
  .chart-title { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.15em; color: var(--muted); margin-bottom: 1.25rem; }
  .chart-wrap  { position: relative; height: 280px; }
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
  .data-table .num { text-align: right; color: var(--org-color); font-weight: 500; }
  .data-table .ts  { color: var(--muted); }
  .pill { display: inline-block; font-size: 0.6rem; padding: 0.15rem 0.4rem; border-radius: 2px; text-transform: uppercase; letter-spacing: 0.1em; }
  .pill-live     { background: rgba(255,79,109,0.15); color: var(--red);  border: 1px solid var(--red); }
  .pill-upcoming { background: rgba(79,195,247,0.10); color: var(--blue); border: 1px solid var(--blue); }
  .generated { text-align: center; color: var(--muted); font-size: 0.7rem; margin-top: 3rem; }

  /* footer */
  footer {
    margin-top: 5rem; padding-top: 2rem;
    border-top: 1px solid var(--border);
    display: flex; flex-wrap: wrap; justify-content: space-between;
    gap: 1rem; font-size: 0.7rem; color: var(--muted);
  }
  footer a { color: var(--muted); text-decoration: none; transition: color 0.2s; }
  footer a:hover { color: var(--org-color); }

  /* animations */
  @keyframes fadeUp {
    from { opacity: 0; transform: translateY(14px); }
    to   { opacity: 1; transform: translateY(0); }
  }
  header { animation: fadeUp 0.5s ease both; }
  .orgs-grid, .channels-list, .streams-grid, .kpi-row { animation: fadeUp 0.5s 0.1s ease both; }
"""


# ── HTML helpers ──────────────────────────────────────────────────────────────
def _html_head(title: str, depth: int, org_color: str = "#e8ff47",
               extra_scripts: str = "") -> str:
    return (
        f'<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        f'<meta charset="UTF-8">\n'
        f'<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'<title>{esc(title)} — IDVTuber Tracker</title>\n'
        f'{_FONTS}\n'
        f'{extra_scripts}\n'
        f'<style>\n{_BASE_CSS}\n  :root {{ --org-color: {org_color}; }}\n</style>\n'
        f'</head>\n<body>\n<div class="page">\n'
    )


def _html_foot(depth: int) -> str:
    rel = "../" * depth
    return (
        f'\n  <footer>\n'
        f'    <span>&#169; 2026 IDVTuber Tracker &#8212; Non-commercial fan project</span>\n'
        f'    <span>\n'
        f'      <a href="{rel}index.html">Home</a>\n'
        f'      &nbsp;&middot;&nbsp;\n'
        f'      <a href="{rel}privacy.html">Privacy Policy</a>\n'
        f'      &nbsp;&middot;&nbsp;\n'
        f'      <a href="{rel}terms.html">Terms of Use</a>\n'
        f'    </span>\n'
        f'  </footer>\n'
        f'</div>\n</body>\n</html>'
    )


def _breadcrumb(crumbs: list[tuple[str, str]]) -> str:
    parts = []
    for i, (label, href) in enumerate(crumbs):
        if i == len(crumbs) - 1:
            parts.append(f'<span class="current">{esc(label)}</span>')
        else:
            parts.append(f'<a href="{href}">{esc(label)}</a>')
        if i < len(crumbs) - 1:
            parts.append('<span class="sep">&#8250;</span>')
    return '<nav class="breadcrumb">' + " ".join(parts) + "</nav>\n"


# ══════════════════════════════════════════════════════════════════════════════
# PAGE WRITERS
# ══════════════════════════════════════════════════════════════════════════════

def write_index(total_streams: int, total_channels: int, generated_at: str) -> None:
    org_cards = ""
    for org_slug, org in ORG_MAP.items():
        n_ch = len(org["channels"])
        org_cards += (
            f'\n    <a class="org-card" href="{org_slug}/index.html"'
            f' style="--org-color:{org["color"]}">\n'
            f'      <div class="org-dot"></div>\n'
            f'      <div class="org-title">{esc(org["label"])}</div>\n'
            f'      <div class="org-desc">{esc(org["desc"])}</div>\n'
            f'      <div class="org-stat"><strong>{n_ch}</strong> channels tracked</div>\n'
            f'    </a>'
        )

    body = (
        f'  <header>\n'
        f'    <p class="eyebrow">IDVTuber Tracker &#8212; Live Analytics</p>\n'
        f'    <h1>Stream <em>Overview</em></h1>\n'
        f'    <p class="page-meta">'
        f'Generated: {generated_at} &nbsp;&#183;&nbsp; '
        f'{total_streams} streams &nbsp;&#183;&nbsp; '
        f'{total_channels} channels &nbsp;&#183;&nbsp; '
        f'3 organisations</p>\n'
        f'  </header>\n'
        f'  <div class="orgs-grid">{org_cards}\n  </div>\n'
    )

    html = _html_head("Stream Analytics", 0) + body + _html_foot(0)
    (OUTPUT_DIR / "index.html").write_text(html, encoding="utf-8")
    log.info("Written: index.html")


def write_org_page(org_slug: str, org: dict, stream_counts: dict) -> None:
    org_dir = OUTPUT_DIR / org_slug
    org_dir.mkdir(exist_ok=True)

    items = ""
    for ch_name, ch_type in org["channels"]:
        ch_slug = slugify(ch_name)
        badge   = "ORG CH" if ch_type == "org" else "TALENT"
        n_str   = stream_counts.get(ch_name, 0)
        items += (
            f'\n    <a class="channel-item" href="{ch_slug}/index.html">\n'
            f'      <div class="channel-item-left">\n'
            f'        <span class="channel-badge">{badge}</span>\n'
            f'        <span class="channel-name-text">{esc(ch_name)}</span>\n'
            f'      </div>\n'
            f'      <span style="font-size:0.7rem;color:var(--muted);">{n_str} streams</span>\n'
            f'      <span class="channel-arrow">&#8594;</span>\n'
            f'    </a>'
        )

    bc = _breadcrumb([("Home", "../index.html"), (org["label"], "")])
    body = (
        bc
        + f'  <header>\n'
        f'    <p class="eyebrow">{esc(org["label"])}</p>\n'
        f'    <h1>{esc(org["label"])}</h1>\n'
        f'    <p class="page-meta">{len(org["channels"])} channels &#8212; select a channel to view streams</p>\n'
        f'  </header>\n'
        f'  <div class="channels-list">{items}\n  </div>\n'
    )

    html = _html_head(org["label"], 1, org["color"]) + body + _html_foot(1)
    (org_dir / "index.html").write_text(html, encoding="utf-8")
    log.info("Written: %s/index.html", org_slug)


def write_channel_page(org_slug: str, org: dict,
                       ch_name: str, streams: list[dict]) -> None:
    ch_slug = slugify(ch_name)
    ch_dir  = OUTPUT_DIR / org_slug / ch_slug
    ch_dir.mkdir(parents=True, exist_ok=True)

    cards = ""
    for stream in streams:
        vid    = stream["video_id"]
        v_slug = slugify(vid)
        status = stream.get("stream_status", "vod") or "vod"
        if status == "live":
            s_cls, s_lbl = "status-live",     "&#128308; Live"
        elif status == "upcoming":
            s_cls, s_lbl = "status-upcoming", "Upcoming"
        else:
            s_cls, s_lbl = "status-vod",      "VOD"

        title = esc((stream["video_title"] or vid)[:80])
        cards += (
            f'\n    <a class="stream-card" href="{v_slug}.html">\n'
            f'      <span class="stream-status {s_cls}">{s_lbl}</span>\n'
            f'      <div class="stream-title">{title}</div>\n'
            f'      <div class="stream-stats">\n'
            f'        <div><div class="stat-label">Peak Viewers</div>'
            f'<div class="stat-value">{fmt(stream["peak_viewers"])}</div></div>\n'
            f'        <div><div class="stat-label">Peak Likes</div>'
            f'<div class="stat-value">{fmt(stream["peak_likes"])}</div></div>\n'
            f'        <div><div class="stat-label">Comments</div>'
            f'<div class="stat-value">{fmt(stream["peak_comments"])}</div></div>\n'
            f'      </div>\n'
            f'      <div class="stream-date">{fmt_dt(stream["first_seen"])}</div>\n'
            f'    </a>'
        )

    if not cards:
        cards = '\n    <p class="empty">No streams recorded yet.</p>'

    bc = _breadcrumb([
        ("Home",       "../../index.html"),
        (org["label"], "../index.html"),
        (ch_name,      ""),
    ])
    body = (
        bc
        + f'  <header>\n'
        f'    <p class="eyebrow">{esc(org["label"])}</p>\n'
        f'    <h1>{esc(ch_name)}</h1>\n'
        f'    <p class="page-meta">{len(streams)} streams recorded &#8212; sorted by date</p>\n'
        f'  </header>\n'
        f'  <div class="streams-grid">{cards}\n  </div>\n'
    )

    html = _html_head(ch_name, 2, org["color"]) + body + _html_foot(2)
    (ch_dir / "index.html").write_text(html, encoding="utf-8")
    log.info("  Written: %s/%s/index.html", org_slug, ch_slug)


def write_stream_page(org_slug: str, org: dict, ch_name: str,
                      stream: dict, rows: list[dict],
                      timeseries: list[dict]) -> None:
    vid     = stream["video_id"]
    v_slug  = slugify(vid)
    ch_slug = slugify(ch_name)
    ch_dir  = OUTPUT_DIR / org_slug / ch_slug
    ch_dir.mkdir(parents=True, exist_ok=True)

    status = stream.get("stream_status", "vod") or "vod"
    if status == "live":
        s_cls, s_lbl = "status-live",     "&#128308; Live"
    elif status == "upcoming":
        s_cls, s_lbl = "status-upcoming", "Upcoming"
    else:
        s_cls, s_lbl = "status-vod",      "VOD"

    labels   = [fmt_dt(r["collected_at"])         for r in timeseries]
    viewers  = [int(r["concurrent_viewers"] or 0) for r in timeseries]
    likes    = [int(r["like_count"]         or 0) for r in timeseries]
    comments = [int(r["comment_count"]      or 0) for r in timeseries]

    table_rows_html = ""
    for r in rows:
        r_status   = r.get("stream_status", "")
        pill_class = "pill-live" if r_status == "live" else "pill-upcoming"
        table_rows_html += (
            f'\n        <tr>'
            f'<td class="ts">{fmt_dt(r["collected_at"])}</td>'
            f'<td><span class="pill {pill_class}">{esc(r_status)}</span></td>'
            f'<td class="num">{fmt(r["concurrent_viewers"])}</td>'
            f'<td class="num">{fmt(r["like_count"])}</td>'
            f'<td class="num">{fmt(r["comment_count"])}</td>'
            f'<td class="ts">{fmt_dt(r.get("actual_start") or r.get("scheduled_start"))}</td>'
            f'</tr>'
        )

    title_text  = stream["video_title"] or vid
    short_title = (title_text[:40] + "…") if len(title_text) > 40 else title_text
    org_color   = org["color"]

    bc = _breadcrumb([
        ("Home",       "../../../index.html"),
        (org["label"], "../../index.html"),
        (ch_name,      "../index.html"),
        (short_title,  ""),
    ])

    chart_script = (
        '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js">'
        '</script>'
    )

    body = (
        bc
        + f'  <header>\n'
        f'    <p class="eyebrow">{esc(org["label"])} &nbsp;&#183;&nbsp; {esc(ch_name)}</p>\n'
        f'    <span class="stream-status {s_cls}" style="display:inline-block;margin-bottom:0.75rem;">{s_lbl}</span>\n'
        f'    <h1>{esc(title_text)}</h1>\n'
        f'    <p class="page-meta">Video ID: {esc(vid)} &nbsp;&#183;&nbsp; {fmt(stream["data_points"])} data points</p>\n'
        f'  </header>\n\n'
        f'  <div class="kpi-row">\n'
        f'    <div class="kpi"><div class="kpi-label">Peak Viewers</div><div class="kpi-value">{fmt(stream["peak_viewers"])}</div><div class="kpi-sub">concurrent</div></div>\n'
        f'    <div class="kpi"><div class="kpi-label">Peak Likes</div><div class="kpi-value">{fmt(stream["peak_likes"])}</div></div>\n'
        f'    <div class="kpi"><div class="kpi-label">Peak Comments</div><div class="kpi-value">{fmt(stream["peak_comments"])}</div></div>\n'
        f'    <div class="kpi"><div class="kpi-label">First Seen</div><div class="kpi-value" style="font-size:1.1rem;">{fmt_dt(stream["first_seen"])}</div></div>\n'
        f'    <div class="kpi"><div class="kpi-label">Last Seen</div><div class="kpi-value" style="font-size:1.1rem;">{fmt_dt(stream["last_seen"])}</div></div>\n'
        f'  </div>\n\n'
        f'  <div class="chart-box">\n'
        f'    <div class="chart-title">Concurrent Viewers over Time</div>\n'
        f'    <div class="chart-wrap"><canvas id="viewerChart"></canvas></div>\n'
        f'  </div>\n\n'
        f'  <div class="chart-box">\n'
        f'    <div class="chart-title">Likes &amp; Comments over Time</div>\n'
        f'    <div class="chart-wrap"><canvas id="engagementChart"></canvas></div>\n'
        f'  </div>\n\n'
        f'  <p class="section-title">All collected data points</p>\n'
        f'  <table class="data-table">\n'
        f'    <thead><tr>\n'
        f'      <th>Collected At</th><th>Status</th>\n'
        f'      <th style="text-align:right">Viewers</th>\n'
        f'      <th style="text-align:right">Likes</th>\n'
        f'      <th style="text-align:right">Comments</th>\n'
        f'      <th>Stream Start</th>\n'
        f'    </tr></thead>\n'
        f'    <tbody>{table_rows_html}\n    </tbody>\n'
        f'  </table>\n\n'
        f'  <p class="generated">Generated {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}'
        f' &nbsp;&#183;&nbsp; yt-livestream-tracker</p>\n\n'
        f'<script>\n'
        f'const ts    = {json.dumps(labels)};\n'
        f'const views = {json.dumps(viewers)};\n'
        f'const likes = {json.dumps(likes)};\n'
        f'const comms = {json.dumps(comments)};\n'
        f"const orgColor  = '{org_color}';\n"
        f"const gridColor = 'rgba(30,30,46,0.8)';\n"
        f"const tickColor = '#5a5a7a';\n"
        f'const baseOpts = {{\n'
        f'  responsive: true, maintainAspectRatio: false,\n'
        f'  interaction: {{ mode: "index", intersect: false }},\n'
        f'  plugins: {{ legend: {{ labels: {{ color: tickColor, font: {{ family: "DM Mono", size: 11 }}, boxWidth: 12 }} }} }},\n'
        f'  scales: {{\n'
        f'    x: {{ ticks: {{ color: tickColor, font: {{ family: "DM Mono", size: 10 }}, maxTicksLimit: 10, maxRotation: 0 }}, grid: {{ color: gridColor }} }},\n'
        f'    y: {{ ticks: {{ color: tickColor, font: {{ family: "DM Mono", size: 10 }} }}, grid: {{ color: gridColor }} }}\n'
        f'  }}\n'
        f'}};\n'
        f'new Chart(document.getElementById("viewerChart"), {{\n'
        f'  type: "line",\n'
        f'  data: {{ labels: ts, datasets: [{{ label: "Concurrent Viewers", data: views,\n'
        f'    borderColor: orgColor, backgroundColor: orgColor + "22",\n'
        f'    borderWidth: 2, pointRadius: 2, fill: true, tension: 0.3 }}] }},\n'
        f'  options: {{ ...baseOpts }}\n'
        f'}});\n'
        f'new Chart(document.getElementById("engagementChart"), {{\n'
        f'  type: "line",\n'
        f'  data: {{ labels: ts, datasets: [\n'
        f'    {{ label: "Likes",    data: likes, borderColor: "#ff4f6d", backgroundColor: "rgba(255,79,109,0.05)",  borderWidth: 2, pointRadius: 2, fill: true, tension: 0.3 }},\n'
        f'    {{ label: "Comments", data: comms, borderColor: "#4fc3f7", backgroundColor: "rgba(79,195,247,0.05)", borderWidth: 2, pointRadius: 2, fill: true, tension: 0.3 }}\n'
        f'  ] }},\n'
        f'  options: {{ ...baseOpts }}\n'
        f'}});\n'
        f'</script>\n'
    )

    html = _html_head(title_text, 3, org_color, chart_script) + body + _html_foot(3)
    (ch_dir / f"{v_slug}.html").write_text(html, encoding="utf-8")
    log.info("    Written: %s/%s/%s.html", org_slug, ch_slug, v_slug)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN BUILD
# ══════════════════════════════════════════════════════════════════════════════
def build_dashboard() -> None:
    if not AIVEN_DATABASE_URL:
        print("ERROR: AIVEN_DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    conn = get_conn()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # copy static legal pages from repo root into output
    for legal_file in ["privacy.html", "terms.html"]:
        src = Path(legal_file)
        dst = OUTPUT_DIR / legal_file
        if src.exists():
            shutil.copy2(src, dst)
            log.info("Copied %s to %s/", legal_file, OUTPUT_DIR)
        else:
            log.warning("Legal file not found: %s — skipping", legal_file)

    # load all channel rows from DB
    db_channels  = get_channel_rows(conn)
    db_by_name   = {ch["channel_name"]: ch for ch in db_channels}

    total_streams  = 0
    total_channels = 0
    stream_counts: dict[str, int] = {}
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    for org_slug, org in ORG_MAP.items():
        log.info("── Org: %s", org["label"])
        (OUTPUT_DIR / org_slug).mkdir(exist_ok=True)

        for ch_name, _ch_type in org["channels"]:
            db_row = db_by_name.get(ch_name)
            if not db_row:
                log.warning("  Channel '%s' not found in DB — skipping", ch_name)
                stream_counts[ch_name] = 0
                continue

            table   = db_row["table_name"]
            streams = get_streams_for_channel(conn, table)
            stream_counts[ch_name] = len(streams)
            total_channels += 1
            total_streams  += len(streams)
            log.info("  Channel: %s — %d streams", ch_name, len(streams))

            for stream in streams:
                ts       = get_stream_timeseries(conn, table, stream["video_id"])
                all_rows = get_all_rows(conn, table, stream["video_id"])
                write_stream_page(org_slug, org, ch_name, stream, all_rows, ts)

            write_channel_page(org_slug, org, ch_name, streams)

        write_org_page(org_slug, org, stream_counts)

    write_index(total_streams, total_channels, generated_at)

    conn.close()
    log.info("Dashboard complete — %d streams across %d channels.", total_streams, total_channels)


if __name__ == "__main__":
    build_dashboard()
