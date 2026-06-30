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
import threading
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("tracker.log")],
)
log = logging.getLogger(__name__)

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
POLL_INTERVAL_SEC          = int(os.environ.get("POLL_INTERVAL_SEC", "60"))
STREAM_POLL_SEC            = int(os.environ.get("STREAM_POLL_SEC", "30"))
SLOW_CYCLE_THRESHOLD_SEC   = int(os.environ.get("SLOW_CYCLE_THRESHOLD_SEC", "60"))
DISCORD_WEBHOOK            = os.environ.get("DISCORD_WEBHOOK", "")
MAX_HISTORY_POINTS         = int(os.environ.get("MAX_HISTORY_POINTS", "60"))   # chart window
# How many consecutive cycles a live stream can return no analytics at all
# (deleted, private, transient API gap) before we give up on it without
# writing a closing row. A stream that ends normally is detected via
# actualEndTime instead and writes its closing "vod" row immediately,
# regardless of this tolerance — this constant only covers the genuinely
# ambiguous "no data returned" case.
ANALYTICS_MISS_TOLERANCE   = int(os.environ.get("ANALYTICS_MISS_TOLERANCE", "3"))
# How close to its scheduled start time an upcoming stream must be before
# it enters the fast-poll cycle (check_upcoming_went_live every 30s).
# Streams outside this window are left to the normal activities.list scan.
# Default: 600s (10 minutes). Lower = tighter detection, more API calls.
UPCOMING_POLL_WINDOW_SEC   = int(os.environ.get("UPCOMING_POLL_WINDOW_SEC", "600"))


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
                    view_count         BIGINT,
                    like_count         BIGINT,
                    comment_count      BIGINT,
                    stream_status      TEXT,
                    scheduled_start    TIMESTAMPTZ,
                    actual_start       TIMESTAMPTZ
                )
            """)
            cur.execute(f"""
                ALTER TABLE {table} ADD COLUMN IF NOT EXISTS view_count BIGINT
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
    """Open a fresh connection, insert one analytics row, close immediately.
    For bulk saves across multiple streams use save_many_to_db() instead."""
    conn = _new_conn()
    if conn is None:
        return
    try:
        sql = f"""
            INSERT INTO {table}
                (collected_at, channel_id, channel_name, video_id, video_title,
                 concurrent_viewers, view_count, like_count, comment_count, stream_status,
                 scheduled_start, actual_start)
            VALUES
                (%(collected_at)s, %(channel_id)s, %(channel_name)s, %(video_id)s, %(video_title)s,
                 %(concurrent_viewers)s, %(view_count)s, %(like_count)s, %(comment_count)s, %(stream_status)s,
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


def save_many_to_db(rows: list[tuple[dict, str]]) -> None:
    """Insert analytics rows for multiple streams in a single DB connection.

    Each element of rows is (row_dict, table_name). All inserts are sent in
    one round-trip, paying the TCP + SSL handshake cost only once per cycle
    instead of once per live stream.
    """
    if not rows:
        return
    conn = _new_conn()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            for row, table in rows:
                sql = f"""
                    INSERT INTO {table}
                        (collected_at, channel_id, channel_name, video_id, video_title,
                         concurrent_viewers, view_count, like_count, comment_count,
                         stream_status, scheduled_start, actual_start)
                    VALUES
                        (%(collected_at)s, %(channel_id)s, %(channel_name)s, %(video_id)s,
                         %(video_title)s, %(concurrent_viewers)s, %(view_count)s,
                         %(like_count)s, %(comment_count)s, %(stream_status)s,
                         %(scheduled_start)s, %(actual_start)s)
                """
                cur.execute(sql, row)
        conn.commit()
        log.info("DB: inserted %d row(s) in one connection.", len(rows))
    except Exception as e:
        log.error("DB batch save failed: %s", e)
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
# DASHBOARD DEPLOY TRIGGER
# ══════════════════════════════════════════════════════════════════════════════
# NOTE: tracker.py no longer runs generate_dashboard.py locally. Previously
# this module ran a full local regeneration (regenerate_dashboard(), removed)
# every REGEN_INTERVAL_SEC, then separately pushed the pre-rendered output to
# the dashboard repo and dispatched a Pages deploy that regenerated everything
# AGAIN on the ubuntu runner. That meant every live cycle paid for two full
# dashboard builds. Generation now happens exactly once, inside the dashboard
# repo's own GitHub Actions workflows (generate_live.py / generate_backfill.py
# via deploy_dashboard_live.yml / deploy_dashboard_backfill.yml). tracker.py's
# only remaining job is to fire the repository_dispatch trigger below.

import subprocess


def _notify_slow_cycle(elapsed: float, cycle_num: int) -> None:
    """Post a Discord warning when a single poll cycle exceeds SLOW_CYCLE_THRESHOLD_SEC.

    Called from run() immediately after the cycle timing log line so it
    fires during the 6-hour run, not at the end of it. The webhook URL is
    read from DISCORD_WEBHOOK env var (same secret used by tracker.yml).
    Silently skips if the webhook URL is not configured.
    """
    if not DISCORD_WEBHOOK:
        return
    try:
        body = {
            "content": "@here",
            "embeds": [{
                "title": "[SLOW] Cycle exceeded threshold",
                "description": (
                    f"A poll cycle took **{elapsed:.1f}s** "
                    f"(threshold: {SLOW_CYCLE_THRESHOLD_SEC}s). "
                    "This may indicate network latency, DB slowness, "
                    "or dashboard generation taking longer than expected."
                ),
                "color": 16776960,   # yellow (#FFFF00)
                "fields": [
                    {"name": "Elapsed",   "value": f"{elapsed:.1f}s",          "inline": True},
                    {"name": "Threshold", "value": f"{SLOW_CYCLE_THRESHOLD_SEC}s", "inline": True},
                    {"name": "Cycle #",   "value": str(cycle_num),             "inline": True},
                ],
                "footer": {"text": "IDVTuber Tracker"},
            }],
        }
        requests.post(
            DISCORD_WEBHOOK,
            json=body,
            timeout=10,
        )
        log.warning(
            "Slow cycle alert sent to Discord: %.1fs elapsed (threshold %ds).",
            elapsed, SLOW_CYCLE_THRESHOLD_SEC,
        )
    except Exception as e:
        log.warning("Could not send slow-cycle Discord notification: %s", e)


def deploy_dashboard() -> None:
    """
    Fire a repository_dispatch event on DASHBOARD_REPO to trigger the live-loop
    GitHub Actions workflow (deploy_dashboard_live.yml), which now does ALL
    dashboard generation and deployment itself by running generate_live.py
    directly against the database on the ubuntu runner.

    Previously this function also cloned the dashboard repo, copied tracker's
    own locally pre-rendered dashboard/ folder into it, committed, and pushed
    before dispatching. That meant the dashboard was fully generated twice per
    deploy cycle — once here on the Windows self-hosted runner (via the now-
    removed regenerate_dashboard()), and again inside the GH Actions workflow
    itself. Removing the local generation entirely and leaving this function
    as a thin trigger eliminates that duplicated work; the workflow's own
    generate_live.py is now the single source of truth for dashboard output.
    """
    pat            = os.environ.get("GH_PAT", "")
    # DASHBOARD_REPO is the slug of the separate repo that hosts GitHub Pages,
    # e.g. "idvtuber-tracker/dashboard".
    dashboard_repo = os.environ.get("DASHBOARD_REPO", "").strip()

    if not pat:
        log.error("GH_PAT is not set — cannot dispatch dashboard deploy.")
        return
    if not dashboard_repo:
        log.error("DASHBOARD_REPO is not set — cannot dispatch dashboard deploy.")
        return

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
    try:
        resp = requests.post(
            f"https://api.github.com/repos/{dashboard_repo}/dispatches",
            headers=headers,
            data=payload,
            timeout=10,
        )
        if resp.status_code == 204:
            log.info("Deploy event fired on %s successfully.", dashboard_repo)
        else:
            log.error(
                "Deploy event on %s failed: %s %s",
                dashboard_repo, resp.status_code, resp.text
            )
    except requests.exceptions.ConnectionError as e:
        log.error("Deploy dispatch failed (network error): %s", e)
    except requests.exceptions.Timeout:
        log.error("Deploy dispatch timed out.")

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
                live_details     = video_items[0].get("liveStreamingDetails", {})
                broadcast_status = snippet.get("liveBroadcastContent")
                if broadcast_status not in ("live", "upcoming"):
                    continue

                results.append({
                    "video_id":        video_id,
                    "channel_id":      channel_id,
                    "channel_name":    snippet.get("channelTitle", ""),
                    "video_title":     snippet.get("title", ""),
                    "stream_status":   broadcast_status,
                    "scheduled_start": live_details.get("scheduledStartTime"),
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


def check_upcoming_went_live(video_ids: list[str]) -> dict[str, str]:
    """
    Fix A — Scheduled stream fast detection.

    For every video_id currently tracked as 'upcoming', call videos.list
    to check whether it has transitioned to 'live'. This costs 1 unit per
    call and runs every cycle, so a scheduled stream is detected within
    the next 30s cycle rather than waiting up to POLL_INTERVAL_SEC.

    Returns a dict of {video_id: new_status} for any that changed.
    """
    if not video_ids:
        return {}
    global youtube
    changed: dict[str, str] = {}
    # videos.list accepts up to 50 IDs per call
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        for attempt in range(len(YOUTUBE_API_KEYS)):
            try:
                resp = youtube.videos().list(
                    part="snippet",
                    id=",".join(batch),
                ).execute()
                for item in resp.get("items", []):
                    vid    = item["id"]
                    status = item.get("snippet", {}).get("liveBroadcastContent")
                    if status in ("live", "none"):
                        changed[vid] = status
                break
            except HttpError as e:
                if e.resp.status == 403:
                    log.warning("403 on check_upcoming_went_live: %s", e)
                    if not _mark_exhausted():
                        return changed
                    youtube = _build_client(_current_key())
                    continue
                log.error("check_upcoming_went_live error: %s", e)
                break
    return changed


def get_video_analytics(video_id: str) -> Optional[dict]:
    """
    Fetch liveStreamingDetails + statistics for a single video.

    NOTE: Prefer get_bulk_video_analytics() when fetching multiple streams
    in the same cycle — it batches up to 50 IDs per API call instead of
    issuing one call per stream.
    """
    result = get_bulk_video_analytics([video_id])
    return result.get(video_id)


def get_bulk_video_analytics(video_ids: list[str]) -> dict[str, dict]:
    """
    Fetch liveStreamingDetails + statistics for up to N videos in batches
    of 50 (the videos.list API maximum).

    Returns a dict of {video_id: analytics_dict}. Missing IDs (deleted,
    private, or returned no items) are simply absent from the result.

    Replaces the old per-stream get_video_analytics() loop in run() —
    100 live streams → 2 API calls instead of 100.
    Rotates to the next API key automatically on a 403 quota error.
    """
    if not video_ids:
        return {}
    global youtube
    results: dict[str, dict] = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        for attempt in range(len(YOUTUBE_API_KEYS)):
            try:
                resp = youtube.videos().list(
                    part="liveStreamingDetails,statistics,snippet",
                    id=",".join(batch),
                ).execute()
                for item in resp.get("items", []):
                    vid   = item["id"]
                    stats = item.get("statistics", {})
                    live  = item.get("liveStreamingDetails", {})
                    results[vid] = {
                        "concurrent_viewers": int(live.get("concurrentViewers", 0) or 0),
                        "view_count":         int(stats.get("viewCount", 0) or 0),
                        "like_count":         int(stats.get("likeCount", 0) or 0),
                        "comment_count":      int(stats.get("commentCount", 0) or 0),
                        "scheduled_start":    live.get("scheduledStartTime"),
                        "actual_start":       live.get("actualStartTime"),
                        # actualEndTime is set by YouTube the moment a livestream
                        # ends — this is the reliable signal for "stream finished
                        # normally", used below to write a closing vod row instead
                        # of silently dropping the stream with no final status.
                        "actual_end":         live.get("actualEndTime"),
                    }
                break   # batch succeeded — move to next batch
            except HttpError as e:
                if e.resp.status == 403:
                    log.warning(
                        "403 on get_bulk_video_analytics (key index %d): %s",
                        _key_index, e,
                    )
                    if not _mark_exhausted():
                        return results   # all keys exhausted — return what we have
                    youtube = _build_client(_current_key())
                    continue             # retry this batch with the new key
                log.error("videos API error in bulk fetch: %s", e)
                break   # non-quota error — skip this batch, move on
    return results


# ══════════════════════════════════════════════════════════════════════════════
# VISUALIZATION
# ══════════════════════════════════════════════════════════════════════════════

def log_active_streams(active_streams: list[dict]) -> None:
    """Log a simple summary of currently active streams."""
    if not active_streams:
        log.info("No live or upcoming streams.")
        return
    log.info("Active streams (%d):", len(active_streams))
    for s in active_streams:
        log.info(
            "  [%s] %s — %s | viewers: %s, likes: %s, comments: %s",
            s["stream_status"].upper(),
            s.get("channel_name", ""),
            s.get("video_title", "")[:50],
            f'{s.get("concurrent_viewers", 0):,}',
            f'{s.get("like_count", 0):,}',
            f'{s.get("comment_count", 0):,}',
        )


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


def collect_and_store(stream: dict, table: str, analytics: dict) -> None:
    """Apply pre-fetched analytics for one live stream and persist to CSV + history.

    analytics must be a dict as returned by get_bulk_video_analytics() for
    this stream's video_id.  The caller (run()) fetches analytics for all
    live streams in one batched API call before invoking this function, so
    no additional HTTP requests are made here.

    Dashboard regeneration and DB writes are intentionally NOT done here —
    they are handled once per cycle in run() after all streams are processed,
    paying those fixed costs only once regardless of how many streams are live.
    """
    video_id = stream["video_id"]

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
    # DB write is handled in run() via save_many_to_db() so all streams
    # are flushed in a single connection per cycle rather than one per stream.


def load_channel_tables_from_db() -> dict[str, str]:
    """
    Pre-populate channel_tables from the DB channels registry.

    Called once at startup. Returns {channel_id: table_name} for every
    channel already registered in the DB. This restores the full mapping
    after a restart so no channel ever has a missing table entry just
    because it had no stream at startup time.
    """
    if not AIVEN_DATABASE_URL:
        return {}
    conn = _new_conn()
    if conn is None:
        return {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT channel_id, table_name FROM channels")
            rows = cur.fetchall()
        result = {row["channel_id"]: row["table_name"] for row in rows}
        log.info("Loaded %d channel table mapping(s) from DB.", len(result))
        return result
    except Exception as e:
        log.error("Could not load channel tables from DB: %s", e)
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ensure_all_channel_tables(existing: dict[str, str]) -> dict[str, str]:
    """
    Ensure every tracked channel has a DB table and a channel_tables entry.

    Called once at startup after load_channel_tables_from_db(). For channels
    already in the DB, their entry is already in `existing` — skip them.
    For genuinely new channels (not yet in DB), fetch their names from the
    YouTube channels.list API (1 unit per 50 channels) and call
    init_channel_table() so the table and registry entry exist before any
    stream is ever detected.

    Returns the updated channel_tables dict (existing + newly created).
    """
    result = dict(existing)
    new_ids = [ch for ch in CHANNEL_IDS if ch not in result]
    if not new_ids:
        log.info("All %d channel(s) already have DB table entries.", len(CHANNEL_IDS))
        return result

    log.info("%d new channel(s) not yet in DB — fetching names from YouTube API.",
             len(new_ids))

    global youtube
    names: dict[str, str] = {}   # channel_id → channel_name
    for i in range(0, len(new_ids), 50):
        batch = new_ids[i:i + 50]
        for attempt in range(len(YOUTUBE_API_KEYS)):
            try:
                resp = youtube.channels().list(
                    part="snippet",
                    id=",".join(batch),
                    maxResults=50,
                ).execute()
                for item in resp.get("items", []):
                    cid   = item["id"]
                    title = item.get("snippet", {}).get("title", cid)
                    names[cid] = title
                break
            except HttpError as e:
                if e.resp.status == 403:
                    log.warning("403 on startup channels.list: %s", e)
                    if not _mark_exhausted():
                        log.error("All API keys exhausted — cannot fetch channel names.")
                        return result
                    youtube = _build_client(_current_key())
                    continue
                log.error("channels.list error at startup: %s", e)
                break

    for ch_id in new_ids:
        ch_name = names.get(ch_id)
        if not ch_name:
            log.warning(
                "Could not fetch name for channel %s — table will be created "
                "on first stream detection instead.", ch_id
            )
            continue
        table = init_channel_table(ch_id, ch_name)
        if table:
            result[ch_id] = table
            log.info("Pre-initialised table for '%s' (%s).", ch_name, ch_id)

    return result


# ── Background dashboard deploy trigger ───────────────────────────────────────
# deploy_dashboard() is now just an HTTP POST (repository_dispatch) — no
# subprocess, no git clone/push. It's still run in a daemon thread purely as
# a safety margin against network hangs, so a slow GitHub API response can
# never stall the main poll loop.
#
# _dashboard_lock prevents a second dispatch from starting while a previous
# one is still in-flight (e.g. if a request is hanging on a slow connection).
_dashboard_lock = threading.Lock()


def _run_dashboard_in_background() -> None:
    """
    Launch deploy_dashboard() in a daemon thread so the main poll loop is
    never blocked by network latency to the GitHub API.

    If the lock is already held (a previous dispatch is still in-flight),
    this cycle's request is skipped and a warning is logged rather than
    queuing up a second concurrent dispatch.
    """
    def _worker():
        if not _dashboard_lock.acquire(blocking=False):
            log.warning(
                "Dashboard deploy dispatch skipped — previous one still in progress."
            )
            return
        try:
            deploy_dashboard()
        finally:
            _dashboard_lock.release()

    t = threading.Thread(target=_worker, name="dashboard-worker", daemon=True)
    t.start()


def run() -> None:
    log.info("Tracker starting. Monitoring channels: %s", CHANNEL_IDS)
    if AIVEN_DATABASE_URL:
        init_db()
    else:
        log.warning("No DB connection – CSV-only mode.")

    known_streams: dict[str, dict] = {}

    # Pre-populate channel_tables from the DB, then ensure every tracked
    # channel has a table — even ones that have never had a stream detected.
    # This means a channel that went live before being picked up by
    # activities.list will have its table ready the moment it's first seen.
    if AIVEN_DATABASE_URL:
        _db_tables    = load_channel_tables_from_db()
        channel_tables = ensure_all_channel_tables(_db_tables)
    else:
        channel_tables: dict[str, str] = {}

    last_deploy_time  = datetime.now(timezone.utc)
    # How often to fire the dashboard deploy dispatch (seconds). Default 900s
    # = 15 min, matching the dashboard's target update window. REGEN_INTERVAL_SEC
    # no longer exists — local pre-rendering was removed; generate_live.py
    # inside the dashboard repo's own GH Actions workflow is now the only
    # place dashboard generation happens, triggered by this dispatch.
    DEPLOY_INTERVAL_SEC = int(os.environ.get("DEPLOY_INTERVAL_SEC", "900"))
    channel_poll_counter = 0
    cycle_num = 0

    log.info("Scanning for streams…")

    while _running:
        try:
            # ── activities.list channel scan (every POLL_INTERVAL_SEC) ────
            # Timed separately from the rest of the cycle. The scan is a long
            # sequential HTTP loop (up to 2 minutes for 209 channels) and is
            # excluded from the slow-cycle elapsed window so the alert only
            # fires for problems in the analytics/DB/dashboard work, not for
            # the expected scan cost.
            if channel_poll_counter == 0:
                scan_start = datetime.now(timezone.utc)
                discovered: dict[str, dict] = {}
                for ch in CHANNEL_IDS:
                    for s in find_live_videos(ch):
                        vid = s["video_id"]
                        discovered[vid] = s
                        if AIVEN_DATABASE_URL and ch not in channel_tables:
                            channel_tables[ch] = init_channel_table(
                                ch, s["channel_name"]
                            )
                        if vid not in known_streams:
                            log.info("New stream detected (activities): %s — %s [%s]",
                                     s["channel_name"], s["video_title"], s["stream_status"])
                for vid in list(known_streams):
                    if vid not in discovered:
                        log.info("Stream ended: %s", vid)
                known_streams = discovered
                scan_elapsed = (datetime.now(timezone.utc) - scan_start).total_seconds()
                log.info(
                    "Channel scan completed: %d channel(s), %d stream(s) found in %.1fs.",
                    len(CHANNEL_IDS), len(known_streams), scan_elapsed,
                )

            channel_poll_counter = (channel_poll_counter + 1) % max(1, POLL_INTERVAL_SEC // STREAM_POLL_SEC)

            # cycle_start is set AFTER the channel scan so that scan latency
            # (which is expected and unavoidable) does not count toward the
            # slow-cycle threshold. The scan timing is logged separately above.
            cycle_start = datetime.now(timezone.utc)

            # ── upcoming→live fast detection (every cycle, 1 unit/vid) ────
            # Only fast-poll streams whose scheduled start is within
            # UPCOMING_POLL_WINDOW_SEC of now. Streams scheduled hours away
            # are left to the normal activities.list scan — polling them
            # every 30s wastes API calls on guaranteed "still upcoming" results.
            # Streams with no known scheduled_start are always included as a
            # safe fallback (we don't know when they'll start).
            _now_utc = datetime.now(timezone.utc)
            _window  = timedelta(seconds=UPCOMING_POLL_WINDOW_SEC)
            upcoming_ids = []
            for vid, s in known_streams.items():
                if s.get("stream_status") != "upcoming":
                    continue
                sched = s.get("scheduled_start")
                if sched is None:
                    upcoming_ids.append(vid)
                    continue
                try:
                    sched_dt = datetime.fromisoformat(
                        sched.replace("Z", "+00:00")
                    )
                    if sched_dt - _now_utc <= _window:
                        upcoming_ids.append(vid)
                except (ValueError, TypeError):
                    upcoming_ids.append(vid)  # unparseable — include as fallback
            if upcoming_ids:
                status_changes = check_upcoming_went_live(upcoming_ids)
                for vid, new_status in status_changes.items():
                    if new_status == "live" and known_streams.get(vid, {}).get("stream_status") != "live":
                        known_streams[vid]["stream_status"] = "live"
                        ch_id   = known_streams[vid].get("channel_id", "")
                        ch_name = known_streams[vid].get("channel_name", "")
                        if AIVEN_DATABASE_URL and ch_id and ch_id not in channel_tables:
                            tbl = init_channel_table(ch_id, ch_name)
                            if tbl:
                                channel_tables[ch_id] = tbl
                        log.info("Stream went live (fast poll): %s — %s",
                                 ch_name or vid,
                                 known_streams[vid].get("video_title", ""))
                    elif new_status == "none":
                        # Stream ended or was cancelled without going live
                        log.info("Upcoming stream cancelled/ended: %s", vid)
                        known_streams.pop(vid, None)

        except Exception as e:
            log.error("Unexpected error in main loop: %s — continuing in %ds",
                      e, STREAM_POLL_SEC)
            time.sleep(STREAM_POLL_SEC)
            continue

        active_streams: list[dict] = list(known_streams.values())

        # ── Step 1: bulk-fetch analytics for all live streams in one go ───
        # get_bulk_video_analytics() batches up to 50 IDs per videos.list
        # call, so 100 live streams costs 2 API calls instead of 100.
        live_streams = [s for s in active_streams if s["stream_status"] == "live"]
        live_ids     = [s["video_id"] for s in live_streams]
        bulk_analytics: dict[str, dict] = {}
        if live_ids:
            bulk_analytics = get_bulk_video_analytics(live_ids)
            log.info(
                "Bulk analytics fetched: %d live stream(s), %d result(s) returned "
                "(%d API call(s)).",
                len(live_ids), len(bulk_analytics),
                (len(live_ids) + 49) // 50,
            )

        # ── Step 2: apply analytics, write CSV + history ──────────────────
        # A live stream can stop appearing as "live" for two different
        # reasons, and they must be handled differently:
        #   1. It genuinely ended — YouTube sets actualEndTime on
        #      liveStreamingDetails the moment broadcast stops. This is the
        #      reliable signal. We write ONE final row with
        #      stream_status="vod" so the stream's last DB row correctly
        #      reflects that it finished, instead of being silently
        #      abandoned mid-"live" forever (which previously caused every
        #      dashboard/archiver script reading this data to treat it as
        #      still live indefinitely).
        #   2. Analytics are simply missing this cycle (deleted, made
        #      private, transient API hiccup) — actualEndTime is also absent
        #      in this case. We tolerate a few consecutive misses (in case
        #      it's transient) before giving up and dropping the stream
        #      without writing a closing row, since we have no reliable way
        #      to know its final stats.
        db_batch: list[tuple[dict, str]] = []
        any_live = False
        ended_video_ids: list[str] = []

        for stream in active_streams:
            if stream["stream_status"] == "live":
                video_id  = stream["video_id"]
                analytics = bulk_analytics.get(video_id)

                if analytics is not None and analytics.get("actual_end"):
                    # Stream ended normally — write one final closing row
                    # with stream_status="vod" using whatever final stats
                    # YouTube returned in this same response, then stop
                    # tracking it. This is the row that was previously
                    # never written.
                    table = channel_tables.get(stream["channel_id"])
                    stream.update(analytics)
                    stream["stream_status"]   = "vod"
                    stream["collected_at"]    = datetime.now(timezone.utc).isoformat()
                    if table:
                        db_batch.append((dict(stream), table))
                    log.info(
                        "Stream ended: %s — %s (closing vod row written)",
                        stream.get("channel_name", ""), video_id,
                    )
                    ended_video_ids.append(video_id)
                    continue

                if analytics is None:
                    # No data at all this cycle — could be deleted/private,
                    # or a transient API gap. Tolerate a few misses before
                    # giving up, in case it's transient.
                    miss_count = stream.get("_analytics_miss_count", 0) + 1
                    stream["_analytics_miss_count"] = miss_count
                    if miss_count < ANALYTICS_MISS_TOLERANCE:
                        log.warning(
                            "No analytics returned for live stream %s (%s) — "
                            "miss %d/%d, will retry.",
                            video_id, stream.get("channel_name", ""),
                            miss_count, ANALYTICS_MISS_TOLERANCE,
                        )
                        continue
                    # Exhausted tolerance — give up without a closing row,
                    # since we have no final stats to write. This stream's
                    # last DB row will remain "live"; the backfill loop's
                    # staleness handling (or a future archiver pass) is the
                    # backstop for this rarer case.
                    log.warning(
                        "No analytics for live stream %s (%s) after %d "
                        "consecutive misses — dropping without closing row.",
                        video_id, stream.get("channel_name", ""), miss_count,
                    )
                    ended_video_ids.append(video_id)
                    continue

                stream.pop("_analytics_miss_count", None)
                table = channel_tables.get(stream["channel_id"])
                collect_and_store(stream, table, analytics)
                if table:
                    db_batch.append((stream, table))
                any_live = True
            else:
                stream.setdefault("concurrent_viewers", 0)
                stream.setdefault("like_count", 0)
                stream.setdefault("comment_count", 0)

        for vid in ended_video_ids:
            known_streams.pop(vid, None)

        # ── Step 3: flush all DB rows in one connection ───────────────────
        if db_batch:
            save_many_to_db(db_batch)

        # ── Step 4: deploy dashboard trigger (non-blocking) ───────────────
        # deploy_dashboard() is now a single fast HTTP POST (repository_dispatch),
        # not a subprocess. Running it in a daemon thread is just a safety
        # margin against a slow network response. last_deploy_time is updated
        # immediately so a subsequent cycle never double-fires even if the
        # dispatch request hasn't completed yet.
        if any_live:
            now = datetime.now(timezone.utc)
            deploy_due = (now - last_deploy_time).total_seconds() >= DEPLOY_INTERVAL_SEC
            if deploy_due:
                last_deploy_time = now
                _run_dashboard_in_background()

        log_active_streams(active_streams)

        # ── Step 5: sleep only the remaining time ─────────────────────────
        # Subtract the time already spent on work so the total cycle length
        # stays close to STREAM_POLL_SEC regardless of how many streams are
        # live or how long the dashboard regeneration takes.
        elapsed   = (datetime.now(timezone.utc) - cycle_start).total_seconds()
        remaining = max(0.0, STREAM_POLL_SEC - elapsed)
        cycle_num += 1
        log.info(
            "Cycle %d: %.1fs work + %.1fs sleep = %.1fs total | "
            "Next channel scan in %ds",
            cycle_num, elapsed, remaining, elapsed + remaining,
            (max(1, POLL_INTERVAL_SEC // STREAM_POLL_SEC) - channel_poll_counter)
            * STREAM_POLL_SEC,
        )
        if elapsed > SLOW_CYCLE_THRESHOLD_SEC and cycle_num > 1:
            _notify_slow_cycle(elapsed, cycle_num)
        time.sleep(remaining)

    log.info("Tracker stopped.")

if __name__ == "__main__":
    run()
