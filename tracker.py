"""
YouTube Livestream Analytics Tracker
Monitors specified channels for live streams and collects real-time analytics.
"""
import sys
import io

# Force UTF-8 output on Windows to support emoji and special characters
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import os
import time
import csv
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich import box
from rich.progress import Progress, SpinnerColumn, TextColumn
import plotext as plt

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("tracker.log")],
)
log = logging.getLogger(__name__)
console = Console()

# ── config from env ────────────────────────────────────────────────────────────
YOUTUBE_API_KEY      = os.environ["YOUTUBE_API_KEY"]
CHANNEL_IDS          = [c.strip() for c in os.environ["CHANNEL_IDS"].split(",")]
AIVEN_DATABASE_URL   = os.environ.get("AIVEN_DATABASE_URL")          # postgres DSN
CSV_OUTPUT_PATH      = os.environ.get("CSV_OUTPUT_PATH", "analytics.csv")
POLL_INTERVAL_SEC    = int(os.environ.get("POLL_INTERVAL_SEC", "60"))
STREAM_POLL_SEC      = int(os.environ.get("STREAM_POLL_SEC", "30"))
MAX_HISTORY_POINTS   = int(os.environ.get("MAX_HISTORY_POINTS", "60"))   # chart window

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# ── in-memory history for charts ───────────────────────────────────────────────
history: dict[str, list] = {}   # video_id -> list of (ts, viewers, likes, comments)


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════

def get_db_connection() -> Optional[psycopg2.extensions.connection]:
    if not AIVEN_DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(AIVEN_DATABASE_URL, sslmode="require")
        return conn
    except Exception as e:
        log.error("DB connection failed: %s", e)
        return None


def get_table_name(channel_name: str) -> str:
    """Convert a channel name to a safe PostgreSQL table name."""
    import re
    # lowercase, replace spaces and special chars with underscores
    safe = re.sub(r"[^a-z0-9]", "_", channel_name.lower())
    # collapse multiple underscores, strip leading/trailing
    safe = re.sub(r"_+", "_", safe).strip("_")
    return f"stream_{safe}"


def init_db(conn) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        # Master channel registry table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                channel_id      TEXT PRIMARY KEY,
                channel_name    TEXT NOT NULL,
                table_name      TEXT NOT NULL,
                added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
    conn.autocommit = False
    log.info("Database initialised.")


def init_channel_table(conn, channel_id: str, channel_name: str) -> str:
    """
    Create a dedicated analytics table for a channel if it doesn't exist.
    Returns the table name used.
    """
    table = get_table_name(channel_name)
    conn.autocommit = True
    with conn.cursor() as cur:
        # Register channel in master table
        cur.execute("""
            INSERT INTO channels (channel_id, channel_name, table_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (channel_id) DO UPDATE
                SET channel_name = EXCLUDED.channel_name
        """, (channel_id, channel_name, table))

        # Create the channel's own analytics table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id                 SERIAL PRIMARY KEY,
                collected_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                channel_id         TEXT NOT NULL,
                channel_name       TEXT,
                video_id           TEXT NOT NULL,
                video_title        TEXT,
                concurrent_viewers BIGINT,
                like_count         BIGINT,
                comment_count      BIGINT,
                stream_status      TEXT,
                scheduled_start    TIMESTAMPTZ,
                actual_start       TIMESTAMPTZ
            )
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table}_video_id
                ON {table}(video_id)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table}_collected_at
                ON {table}(collected_at)
        """)
    conn.autocommit = False
    log.info("Table '%s' ready for channel '%s'.", table, channel_name)
    return table
    
def save_to_db(conn, row: dict, table: str) -> None:
    sql = f"""
        INSERT INTO {table}
            (collected_at, channel_id, channel_name, video_id, video_title,
             concurrent_viewers, like_count, comment_count, stream_status,
             scheduled_start, actual_start)
        VALUES
            (%(collected_at)s, %(channel_id)s, %(channel_name)s, %(video_id)s, %(video_title)s,
             %(concurrent_viewers)s, %(like_count)s, %(comment_count)s, %(stream_status)s,
             %(scheduled_start)s, %(actual_start)s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, row)
    conn.commit()
# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD REGENERATION
# ══════════════════════════════════════════════════════════════════════════════

import subprocess

def regenerate_dashboard() -> None:
    """Trigger dashboard regeneration as a subprocess."""
    try:
        result = subprocess.run(
            ["python", "generate_dashboard.py"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            log.info("Dashboard regenerated successfully.")
        else:
            log.error("Dashboard generation failed:\n%s", result.stderr)
    except subprocess.TimeoutExpired:
        log.error("Dashboard generation timed out.")
    except Exception as e:
        log.error("Dashboard generation error: %s", e)
        
def deploy_dashboard() -> None:
    """
    Commit and push the regenerated dashboard folder to the gh-pages branch.
    Requires the runner to have git configured and push access to the repo.
    """
    try:
        # Configure git identity (needed for commits in CI/runner environments)
        subprocess.run(["git", "config", "user.email", "tracker-bot@localhost"], check=True)
        subprocess.run(["git", "config", "user.name",  "Stream Tracker Bot"],   check=True)

        dashboard_dir = os.environ.get("DASHBOARD_OUTPUT_DIR", "dashboard")

        # Stage only the dashboard output folder
        subprocess.run(["git", "add", dashboard_dir], check=True)

        # Only commit if there are actual changes
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            capture_output=True
        )
        if result.returncode == 0:
            log.info("Dashboard unchanged — skipping deploy commit.")
            return

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        subprocess.run(
            ["git", "commit", "-m", f"chore: dashboard update {ts}"],
            check=True
        )
        subprocess.run(["git", "push"], check=True)
        log.info("Dashboard deployed to GitHub Pages.")

    except subprocess.CalledProcessError as e:
        log.error("Deploy failed: %s", e)

# ══════════════════════════════════════════════════════════════════════════════
# CSV
# ══════════════════════════════════════════════════════════════════════════════

CSV_FIELDS = [
    "collected_at", "channel_id", "channel_name", "video_id", "video_title",
    "concurrent_viewers", "like_count", "comment_count", "stream_status",
    "scheduled_start", "actual_start",
]


def save_to_csv(row: dict) -> None:
    write_header = not os.path.exists(CSV_OUTPUT_PATH)
    with open(CSV_OUTPUT_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow({k: row.get(k) for k in CSV_FIELDS})


# ══════════════════════════════════════════════════════════════════════════════
# YOUTUBE API HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def find_live_videos(channel_id: str) -> list[dict]:
    """
    Use activities.list (1 unit) instead of search.list (100 units)
    to detect live streams on a channel.
    """
    results = []
    try:
        resp = youtube.activities().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=10,
        ).execute()

        for item in resp.get("items", []):
            details = item.get("contentDetails", {})

            # activities feed includes uploads; we then check if it's live
            upload = details.get("upload", {})
            video_id = upload.get("videoId")
            if not video_id:
                continue

            # cheap videos.list call (1 unit) to confirm it's live
            video_resp = youtube.videos().list(
                part="snippet,liveStreamingDetails",
                id=video_id,
            ).execute()
            video_items = video_resp.get("items", [])
            if not video_items:
                continue

            snippet     = video_items[0].get("snippet", {})
            live_detail = video_items[0].get("liveStreamingDetails", {})

            broadcast_status = snippet.get("liveBroadcastContent")  # "live" | "upcoming" | "none"
            if broadcast_status not in ("live", "upcoming"):
                continue

            results.append({
                "video_id":      video_id,
                "channel_id":    channel_id,
                "channel_name":  snippet.get("channelTitle", ""),
                "video_title":   snippet.get("title", ""),
                "stream_status": broadcast_status,
            })

    except HttpError as e:
        log.error("activities API error for %s: %s", channel_id, e)

    return results


def get_video_analytics(video_id: str) -> Optional[dict]:
    """Fetch liveStreamingDetails + statistics for a single video."""
    try:
        resp = youtube.videos().list(
            part="liveStreamingDetails,statistics,snippet",
            id=video_id,
        ).execute()
        items = resp.get("items", [])
        if not items:
            return None
        item  = items[0]
        stats = item.get("statistics", {})
        live  = item.get("liveStreamingDetails", {})
        return {
            "concurrent_viewers": int(live.get("concurrentViewers", 0) or 0),
            "like_count":         int(stats.get("likeCount", 0) or 0),
            "comment_count":      int(stats.get("commentCount", 0) or 0),
            "scheduled_start":    live.get("scheduledStartTime"),
            "actual_start":       live.get("actualStartTime"),
        }
    except HttpError as e:
        log.error("videos API error for %s: %s", video_id, e)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# VISUALIZATION
# ══════════════════════════════════════════════════════════════════════════════

def build_summary_table(active_streams: list[dict]) -> Table:
    table = Table(
        title="📡 Live Streams", box=box.ROUNDED,
        show_header=True, header_style="bold cyan",
    )
    table.add_column("Channel",   style="green",  no_wrap=True)
    table.add_column("Title",     style="white",  max_width=40)
    table.add_column("Status",    style="yellow", justify="center")
    table.add_column("👁 Viewers", style="magenta", justify="right")
    table.add_column("👍 Likes",   style="blue",    justify="right")
    table.add_column("💬 Comments",style="cyan",    justify="right")
    table.add_column("Started",   style="dim",     justify="center")

    for s in active_streams:
        status_icon = "🔴 LIVE" if s["stream_status"] == "live" else "⏳ Soon"
        table.add_row(
            s.get("channel_name", "")[:25],
            s.get("video_title", "")[:40],   
            status_icon,
            f"{s.get('concurrent_viewers', 0):,}",
            f"{s.get('like_count', 0):,}",
            f"{s.get('comment_count', 0):,}",
            (s.get("actual_start") or s.get("scheduled_start") or "—")[:16],
        )
    return table


def draw_viewer_chart(video_id: str, channel_name: str) -> None:
    pts = history.get(video_id, [])
    if len(pts) < 2:
        return
    xs = list(range(len(pts)))
    ys = [p[1] for p in pts]          # concurrent_viewers
    plt.clf()
    plt.plot_size(60, 12)
    plt.plot(xs, ys, marker="braille")
    plt.title(f"Viewers — {channel_name} (last {len(pts)} samples)")
    plt.xlabel("samples ago")
    plt.ylabel("viewers")
    plt.show()


def render_dashboard(active_streams: list[dict]) -> str:
    """Return a plain-text snapshot for the console (used outside Rich Live)."""
    lines = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines.append(f"\n{'═'*64}")
    lines.append(f"  YouTube Livestream Tracker  |  {now}")
    lines.append(f"{'═'*64}")
    if not active_streams:
        lines.append("  No live or upcoming streams found.")
    for s in active_streams:
        lines.append(
            f"  [{s['stream_status'].upper():^8}] {s['channel_name']} – {s['video_title'][:45]}"
        )
        lines.append(
            f"           Viewers: {s['concurrent_viewers']:>8,}  |  "
            f"Likes: {s['like_count']:>7,}  |  Comments: {s['comment_count']:>7,}"
        )
    lines.append(f"{'─'*64}\n")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# CORE LOOP
# ══════════════════════════════════════════════════════════════════════════════

_running = True

def _handle_signal(sig, frame):
    global _running
    log.info("Shutdown signal received, exiting gracefully…")
    _running = False

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def collect_and_store(stream: dict, conn, table: str) -> None:
    video_id  = stream["video_id"]
    analytics = get_video_analytics(video_id)
    if analytics is None:
        return

    stream.update(analytics)
    stream["collected_at"] = datetime.now(timezone.utc).isoformat()

    history.setdefault(video_id, []).append((
        stream["collected_at"],
        analytics["concurrent_viewers"],
        analytics["like_count"],
        analytics["comment_count"],
    ))
    if len(history[video_id]) > MAX_HISTORY_POINTS:
        history[video_id].pop(0)

    save_to_csv(stream)
    if conn:
        try:
            save_to_db(conn, stream, table)
        except Exception as e:
            log.error("DB save failed: %s", e)

    # Regenerate dashboard after every successful data collection
    regenerate_dashboard()
    deploy_dashboard()

def run() -> None:
    log.info("Tracker starting. Monitoring channels: %s", CHANNEL_IDS)
    conn = get_db_connection()
    if conn:
        init_db(conn)
    else:
        log.warning("No DB connection – CSV-only mode.")

    known_streams: dict[str, dict] = {}
    channel_tables: dict[str, str] = {}   # channel_id -> table name
    channel_poll_counter = 0

    with console.status("[bold green]Scanning for streams…", spinner="dots"):
        time.sleep(1)

    while _running:
		try:
			if channel_poll_counter == 0:
				discovered: dict[str, dict] = {}
				for ch in CHANNEL_IDS:
					for s in find_live_videos(ch):
						vid = s["video_id"]
						discovered[vid] = s

						# initialise the channel's table on first encounter
						if conn and ch not in channel_tables:
							channel_tables[ch] = init_channel_table(
								conn, ch, s["channel_name"]
							)

						if vid not in known_streams:
							console.print(
								Panel(
									f"[bold yellow]NEW STREAM DETECTED!\n[/]"
									f"Channel : {s['channel_name']}\n"
									f"Title   : {s['video_title']}\n"
									f"Status  : {s['stream_status']}",
									title="Event Trigger",
									border_style="yellow",
								)
							)
							log.info("New stream: %s – %s", s["channel_name"], vid)
				for vid in list(known_streams):
					if vid not in discovered:
						log.info("Stream ended: %s", vid)
				known_streams = discovered

			channel_poll_counter = (channel_poll_counter + 1) % max(1, POLL_INTERVAL_SEC // STREAM_POLL_SEC)

		except Exception as e:
			log.error("Unexpected error in main loop: %s — continuing in %ds",
                  e, STREAM_POLL_SEC)
			time.sleep(STREAM_POLL_SEC)
        continue

        active_streams: list[dict] = list(known_streams.values())
        for stream in active_streams:
            if stream["stream_status"] == "live":
                table = channel_tables.get(stream["channel_id"])
                collect_and_store(stream, conn, table)
            else:
                # Ensure upcoming streams always have safe default keys
                stream.setdefault("concurrent_viewers", 0)
                stream.setdefault("like_count", 0)
                stream.setdefault("comment_count", 0)
        
        console.print(build_summary_table(active_streams))
        for s in active_streams:
            if s["stream_status"] == "live":
                draw_viewer_chart(s["video_id"], s["channel_name"])

        console.print(
            f"[dim]Next analytics poll in {STREAM_POLL_SEC}s  |  "
            f"Next channel scan in "
            f"{(max(1, POLL_INTERVAL_SEC // STREAM_POLL_SEC) - channel_poll_counter) * STREAM_POLL_SEC}s[/]"
        )
        time.sleep(STREAM_POLL_SEC)

    if conn:
        conn.close()
    log.info("Tracker stopped.")

if __name__ == "__main__":
    run()
