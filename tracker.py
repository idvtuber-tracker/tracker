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
# YOUTUBE_API_KEYS accepts a comma-separated list of keys, e.g.:
#   YOUTUBE_API_KEYS=key1,key2,key3
# Falls back to the single YOUTUBE_API_KEY secret for backwards compatibility.
_raw_keys = os.environ.get("YOUTUBE_API_KEYS") or os.environ.get("YOUTUBE_API_KEY", "")
YOUTUBE_API_KEYS     = [k.strip() for k in _raw_keys.split(",") if k.strip()]
if not YOUTUBE_API_KEYS:
    raise RuntimeError("No YouTube API keys found. Set YOUTUBE_API_KEYS or YOUTUBE_API_KEY.")

CHANNEL_IDS          = [c.strip() for c in os.environ["CHANNEL_IDS"].split(",")]
AIVEN_DATABASE_URL   = os.environ.get("AIVEN_DATABASE_URL")          # postgres DSN
CSV_OUTPUT_PATH      = os.environ.get("CSV_OUTPUT_PATH", "analytics.csv")
POLL_INTERVAL_SEC    = int(os.environ.get("POLL_INTERVAL_SEC", "60"))
STREAM_POLL_SEC      = int(os.environ.get("STREAM_POLL_SEC", "30"))
MAX_HISTORY_POINTS   = int(os.environ.get("MAX_HISTORY_POINTS", "60"))   # chart window

# ── API key rotation ───────────────────────────────────────────────────────────
_key_index         = 0          # index of the currently active key
_exhausted: set    = set()      # keys confirmed quota-exceeded for today

# Write to a fixed path outside the workspace so the file survives the
# fresh checkout that happens on every GitHub Actions run.
# Uses C:\actions-runner (the runner install dir) which persists across runs
# on a Windows self-hosted runner running as Local System.
def _resolve_persistent_dir() -> str:
    candidates = [
        # RUNNER_TOOL_CACHE is inside _work and gets wiped between runs — skip it.
        # RUNNER_WORKSPACE is e.g. C:\actions-runner\_work\repo\repo
        # Three levels up reaches C:\actions-runner which persists across runs.
        os.path.join(os.environ.get("RUNNER_WORKSPACE", ""), "..", "..", ".."),
        "C:\\actions-runner",
        "/tmp",
    ]
    for c in candidates:
        if not c:
            continue
        resolved = os.path.normpath(c)
        if os.path.isdir(resolved):
            test = os.path.join(resolved, ".write_test")
            try:
                with open(test, "w") as f:
                    f.write("x")
                os.remove(test)
                return resolved
            except Exception:
                continue
    return os.path.dirname(os.path.abspath(__file__))

_EXHAUSTED_FILE = os.path.join(_resolve_persistent_dir(), "yt_tracker_exhausted_keys.json")
log.info("Exhausted keys file path: %s", _EXHAUSTED_FILE)

def _current_key() -> str:
    return YOUTUBE_API_KEYS[_key_index]

def _build_client(key: str):
    return build("youtube", "v3", developerKey=key)

def _quota_reset_date() -> str:
    """Return the current date in Pacific time (UTC-8).
    YouTube quota resets at midnight Pacific, so this is the correct
    boundary for determining whether the exhausted key list should clear."""
    return _now_pacific().strftime("%Y-%m-%d")

def _load_exhausted() -> None:
    """Load persisted exhausted keys from disk. Clears them if the date has changed."""
    global _exhausted, _key_index
    try:
        if os.path.exists(_EXHAUSTED_FILE):
            with open(_EXHAUSTED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("date") == _quota_reset_date():
                _exhausted = set(data.get("keys", []))
                log.info(
                    "Loaded %d exhausted key(s) from previous run (date: %s).",
                    len(_exhausted), data["date"],
                )
            else:
                _exhausted = set()
                log.info(
                    "New quota day (%s) — exhausted key list cleared.",
                    _quota_reset_date(),
                )
                _save_exhausted()
        # Advance _key_index to first non-exhausted key so we don't start
        # on a key that was already spent in a previous run today.
        for i in range(len(YOUTUBE_API_KEYS)):
            if YOUTUBE_API_KEYS[i] not in _exhausted:
                _key_index = i
                break
        else:
            _key_index = 0   # all exhausted — will be caught at first API call
    except Exception as e:
        log.warning("Could not load exhausted keys file: %s — starting fresh.", e)
        _exhausted = set()

def _save_exhausted() -> None:
    """Persist the current exhausted key set to disk."""
    try:
        with open(_EXHAUSTED_FILE, "w", encoding="utf-8") as f:
            json.dump({"date": _quota_reset_date(), "keys": list(_exhausted)}, f)
    except Exception as e:
        log.warning("Could not save exhausted keys file: %s", e)

def _rotate_key() -> bool:
    """
    Advance to the next non-exhausted key.
    Returns True if a fresh key was found, False if all keys are exhausted.
    """
    global _key_index
    for _ in range(len(YOUTUBE_API_KEYS)):
        _key_index = (_key_index + 1) % len(YOUTUBE_API_KEYS)
        if _current_key() not in _exhausted:
            log.warning(
                "API key rotated → key index %d (%d/%d keys remaining).",
                _key_index,
                len(YOUTUBE_API_KEYS) - len(_exhausted),
                len(YOUTUBE_API_KEYS),
            )
            return True
    log.error("All %d API key(s) are quota-exhausted. Pausing until tomorrow.", len(YOUTUBE_API_KEYS))
    return False

def _mark_exhausted() -> bool:
    """Mark the current key as exhausted, persist to disk, and rotate.
    Returns True if a fresh key is still available."""
    _exhausted.add(_current_key())
    _save_exhausted()
    return _rotate_key()

_load_exhausted()
youtube = _build_client(_current_key())
log.info("YouTube API client initialised with %d key(s) (%d exhausted today).",
         len(YOUTUBE_API_KEYS), len(_exhausted))

# ── timezone helpers ──────────────────────────────────────────────────────────
from datetime import timedelta
from zoneinfo import ZoneInfo

# YouTube API quota resets at midnight Pacific time.
# ZoneInfo handles DST automatically (PST=UTC-8 in winter, PDT=UTC-7 in summer).
_PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
# Display timezone — Indonesia does not observe DST so this is always UTC+7.
_LOCAL_TZ   = ZoneInfo("Asia/Jakarta")

def _now_local() -> datetime:
    """Current time in local display timezone (WIB / UTC+7)."""
    return datetime.now(_LOCAL_TZ)

def _now_pacific() -> datetime:
    """Current time in Pacific timezone, DST-aware."""
    return datetime.now(_PACIFIC_TZ)

# ── in-memory history for charts ───────────────────────────────────────────────
history: dict[str, list] = {}   # video_id -> list of (ts, viewers, likes, comments)


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════

def _new_conn() -> Optional[psycopg2.extensions.connection]:
    """Open and return a fresh DB connection, or None on failure."""
    if not AIVEN_DATABASE_URL:
        return None
    try:
        return psycopg2.connect(
            AIVEN_DATABASE_URL,
            sslmode="require",
            connect_timeout=10,
            options="-c search_path=public -c statement_timeout=30000",
        )
    except Exception as e:
        log.error("DB connection failed: %s", e)
        return None


def ping_db() -> bool:
    """Lightweight connectivity check. Returns True if DB is reachable."""
    conn = _new_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception as e:
        log.warning("DB ping failed: %s", e)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_table_name(channel_name: str) -> str:
    """Convert a channel name to a safe PostgreSQL table name."""
    import re
    # lowercase, replace spaces and special chars with underscores
    safe = re.sub(r"[^a-z0-9]", "_", channel_name.lower())
    # collapse multiple underscores, strip leading/trailing
    safe = re.sub(r"_+", "_", safe).strip("_")
    return f"stream_{safe}"


def init_db() -> None:
    """Create channels registry table if it does not exist."""
    conn = _new_conn()
    if conn is None:
        return
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    channel_id      TEXT PRIMARY KEY,
                    channel_name    TEXT NOT NULL,
                    table_name      TEXT NOT NULL,
                    added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        log.info("Database initialised.")
    except Exception as e:
        log.error("DB init failed: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def init_channel_table(channel_id: str, channel_name: str) -> Optional[str]:
    """
    Register channel and create its analytics table if needed.
    Opens a fresh connection, runs DDL, closes immediately.
    Returns the table name, or None on failure.
    """
    table = get_table_name(channel_name)
    conn = _new_conn()
    if conn is None:
        return None
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO channels (channel_id, channel_name, table_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (channel_id) DO UPDATE
                    SET channel_name = EXCLUDED.channel_name
            """, (channel_id, channel_name, table))
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
        log.info("Table '%s' ready for channel '%s'.", table, channel_name)
        return table
    except Exception as e:
        log.error("DB init_channel_table failed: %s", e)
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass
    
def save_to_db(row: dict, table: str) -> None:
    """Open a fresh connection, insert one analytics row, close immediately."""
    conn = _new_conn()
    if conn is None:
        return
    try:
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
    except Exception as e:
        log.error("DB save failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
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
    dashboard_dir = os.environ.get("DASHBOARD_OUTPUT_DIR", "dashboard")
    pat           = os.environ.get("GH_PAT", "")
    repo_slug     = os.environ.get("GITHUB_REPOSITORY", "")
    repo_dir      = os.getcwd()

    try:
        subprocess.run(["git", "config", "user.email", "tracker-bot@localhost"],
                       cwd=repo_dir, check=True, capture_output=True)
        subprocess.run(["git", "config", "user.name", "Stream Tracker Bot"],
                       cwd=repo_dir, check=True, capture_output=True)

        # Pull latest remote state before staging anything, so our push is
        # always a fast-forward even if another workflow committed in between.
        pull = subprocess.run(
            ["git", "pull", "--rebase", "origin", "HEAD"],
            cwd=repo_dir, capture_output=True, text=True
        )
        if pull.returncode != 0:
            log.warning("git pull --rebase failed (will still attempt push): %s", pull.stderr.strip())

        subprocess.run(["git", "add", dashboard_dir],
                       cwd=repo_dir, check=True, capture_output=True)

        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=repo_dir, capture_output=True
        )
        if result.returncode == 0:
            log.info("Dashboard unchanged — skipping deploy commit.")
            return

        ts = _now_local().strftime("%Y-%m-%d %H:%M:%S WIB")
        subprocess.run(
            ["git", "commit", "-m", f"chore: dashboard update {ts}"],
            cwd=repo_dir, check=True, capture_output=True
        )

        # Push with up to 3 retries, re-pulling on each rejection.
        for attempt in range(1, 4):
            push = subprocess.run(
                ["git", "push", "origin", "HEAD"],
                cwd=repo_dir, capture_output=True, text=True
            )
            if push.returncode == 0:
                log.info("Dashboard pushed to repository (attempt %d).", attempt)
                break
            log.warning("git push failed (attempt %d): %s", attempt, push.stderr.strip())
            if attempt < 3:
                # Re-pull and rebase our commit on top of whatever was pushed
                subprocess.run(
                    ["git", "pull", "--rebase", "origin", "HEAD"],
                    cwd=repo_dir, capture_output=True
                )
        else:
            log.error("git push failed after 3 attempts — skipping deploy dispatch.")
            return

        # Fire the Pages deploy trigger
        if pat and repo_slug:
            headers = {
                "Authorization":        f"Bearer {pat}",
                "Accept":               "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "Content-Type":         "application/json",
            }
            payload = json.dumps({
                "event_type": "deploy-dashboard",
                "client_payload": {"triggered_by": "tracker"}
            })
            resp = requests.post(
                f"https://api.github.com/repos/{repo_slug}/dispatches",
                headers=headers,
                data=payload,
                timeout=10,
            )
            if resp.status_code == 204:
                log.info("Deploy event fired successfully.")
            else:
                log.error("Deploy event failed: %s %s", resp.status_code, resp.text)

    except subprocess.CalledProcessError as e:
        log.error("Deploy failed: %s\nstderr: %s",
                  e, e.stderr.decode() if e.stderr else '')
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
    Rotates to the next API key automatically on a 403 quota error.
    """
    global youtube
    results = []
    for attempt in range(len(YOUTUBE_API_KEYS)):
        try:
            resp = youtube.activities().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=10,
            ).execute()

            for item in resp.get("items", []):
                details = item.get("contentDetails", {})
                upload  = details.get("upload", {})
                video_id = upload.get("videoId")
                if not video_id:
                    continue

                video_resp = youtube.videos().list(
                    part="snippet,liveStreamingDetails",
                    id=video_id,
                ).execute()
                video_items = video_resp.get("items", [])
                if not video_items:
                    continue

                snippet          = video_items[0].get("snippet", {})
                broadcast_status = snippet.get("liveBroadcastContent")
                if broadcast_status not in ("live", "upcoming"):
                    continue

                results.append({
                    "video_id":      video_id,
                    "channel_id":    channel_id,
                    "channel_name":  snippet.get("channelTitle", ""),
                    "video_title":   snippet.get("title", ""),
                    "stream_status": broadcast_status,
                })
            return results

        except HttpError as e:
            if e.resp.status == 403:
                log.warning("403 on find_live_videos (key index %d): %s", _key_index, e)
                if not _mark_exhausted():
                    return results   # all keys exhausted
                youtube = _build_client(_current_key())
                continue            # retry with new key
            log.error("activities API error for %s: %s", channel_id, e)
            return results

    return results


def get_video_analytics(video_id: str) -> Optional[dict]:
    """
    Fetch liveStreamingDetails + statistics for a single video.
    Rotates to the next API key automatically on a 403 quota error.
    """
    global youtube
    for attempt in range(len(YOUTUBE_API_KEYS)):
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
            if e.resp.status == 403:
                log.warning("403 on get_video_analytics (key index %d): %s", _key_index, e)
                if not _mark_exhausted():
                    return None   # all keys exhausted
                youtube = _build_client(_current_key())
                continue          # retry with new key
            log.error("videos API error for %s: %s", video_id, e)
            return None

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
    now = _now_local().strftime("%Y-%m-%d %H:%M:%S WIB")
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


def collect_and_store(stream: dict, table: str) -> None:
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
    if table:
        save_to_db(stream, table)

    regenerate_dashboard()

def run() -> None:
    log.info("Tracker starting. Monitoring channels: %s", CHANNEL_IDS)
    if AIVEN_DATABASE_URL:
        init_db()
    else:
        log.warning("No DB connection – CSV-only mode.")

    known_streams: dict[str, dict] = {}
    channel_tables: dict[str, str] = {}   # channel_id -> table name
    last_deploy_time = datetime.now(timezone.utc)
    DEPLOY_INTERVAL_SEC = int(os.environ.get("DEPLOY_INTERVAL_SEC", "900"))  # 15 min default
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
                        if AIVEN_DATABASE_URL and ch not in channel_tables:
                            channel_tables[ch] = init_channel_table(
                                ch, s["channel_name"]
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
                collect_and_store(stream, table)
                now = datetime.now(timezone.utc)
                if (now - last_deploy_time).total_seconds() >= DEPLOY_INTERVAL_SEC:
                    deploy_dashboard()
                    last_deploy_time = now
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

    log.info("Tracker stopped.")

if __name__ == "__main__":
    run()
