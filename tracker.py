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
MAX_HISTORY_POINTS         = int(os.environ.get("MAX_HISTORY_POINTS", "60"))   # chart window
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

    # Why fetch+reset instead of pull --rebase:
    # Both the tracker (Windows runner) and deploy_dashboard.yml (ubuntu runner)
    # commit to the same branch concurrently. When the deploy workflow commits
    # cache/ or manifest.json between two tracker deploys, the tracker's local
    # history diverges from remote. Rebasing then replays old tracker commits
    # on top of remote, hitting merge conflicts on the same dashboard files.
    # "Rebasing (1/N)" in the error log is the symptom.
    #
    # fetch+reset --hard always aligns local HEAD exactly with remote,
    # discarding any stale local commits. This is safe because dashboard/
    # is regenerated fresh from the DB on every deploy — git history is not
    # the source of truth for dashboard content, the database is.

    try:
        subprocess.run(["git", "config", "user.email", "tracker-bot@localhost"],
                       cwd=repo_dir, check=True, capture_output=True)
        subprocess.run(["git", "config", "user.name", "Stream Tracker Bot"],
                       cwd=repo_dir, check=True, capture_output=True)

        # Step 1: fetch latest remote state — no merge, no rebase.
        fetch = subprocess.run(
            ["git", "fetch", "origin", "main"],
            cwd=repo_dir, capture_output=True, text=True
        )
        if fetch.returncode != 0:
            log.warning("git fetch failed: %s", fetch.stderr.strip())

        # Step 2: hard-reset local HEAD to origin/main, discarding any stale
        # local commits that diverged from remote due to concurrent commits
        # from deploy_dashboard.yml (e.g. cache/ or manifest.json updates).
        reset = subprocess.run(
            ["git", "reset", "--hard", "origin/main"],
            cwd=repo_dir, capture_output=True, text=True
        )
        if reset.returncode != 0:
            log.warning("git reset --hard failed: %s", reset.stderr.strip())

        # Step 3: stage fresh dashboard output on top of the aligned HEAD.
        subprocess.run(["git", "add", dashboard_dir],
                       cwd=repo_dir, check=True, capture_output=True)

        unchanged = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=repo_dir, capture_output=True
        )
        if unchanged.returncode == 0:
            log.info("Dashboard unchanged — skipping deploy commit.")
            return

        ts = _now_local().strftime("%Y-%m-%d %H:%M:%S WIB")
        subprocess.run(
            ["git", "commit", "-m", f"chore: dashboard update {ts}"],
            cwd=repo_dir, check=True, capture_output=True
        )

        # Step 4: push with up to 3 retries.
        # If another commit landed between our fetch and push, re-fetch and
        # re-reset before retrying — always produces a clean fast-forward.
        for attempt in range(1, 4):
            push = subprocess.run(
                ["git", "push", "origin", "main"],
                cwd=repo_dir, capture_output=True, text=True
            )
            if push.returncode == 0:
                log.info("Dashboard pushed to repository (attempt %d).", attempt)
                break
            log.warning("git push failed (attempt %d): %s", attempt, push.stderr.strip())
            if attempt < 3:
                subprocess.run(["git", "fetch", "origin", "main"],
                               cwd=repo_dir, capture_output=True)
                subprocess.run(["git", "reset", "--hard", "origin/main"],
                               cwd=repo_dir, capture_output=True)
                subprocess.run(["git", "add", dashboard_dir],
                               cwd=repo_dir, capture_output=True)
                rechk = subprocess.run(
                    ["git", "diff", "--cached", "--quiet"],
                    cwd=repo_dir, capture_output=True
                )
                if rechk.returncode == 0:
                    log.info("Dashboard unchanged after re-fetch — skipping retry.")
                    break
                subprocess.run(
                    ["git", "commit", "-m", f"chore: dashboard update {ts}"],
                    cwd=repo_dir, capture_output=True
                )
        else:
            log.error("git push failed after 3 attempts — skipping deploy dispatch.")
            return

        # Step 5: fire the Pages deploy trigger.
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
            try:
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
            except requests.exceptions.ConnectionError as e:
                log.error("Deploy dispatch failed (network error) — dashboard push still completed: %s", e)
            except requests.exceptions.Timeout:
                log.error("Deploy dispatch timed out — dashboard push still completed.")

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
                "view_count":         int(stats.get("viewCount", 0) or 0),
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


def collect_and_store(stream: dict, table: str) -> None:
    """Fetch analytics for one live stream and persist them.
    Dashboard regeneration is intentionally NOT called here — it is called
    once per cycle in run() after all streams are processed, so the
    subprocess overhead is paid once regardless of how many streams are live.
    """
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
    DEPLOY_INTERVAL_SEC = int(os.environ.get("DEPLOY_INTERVAL_SEC", "900"))
    channel_poll_counter = 0

    log.info("Scanning for streams…")

    while _running:
        try:
            # ── activities.list channel scan (every POLL_INTERVAL_SEC) ────
            if channel_poll_counter == 0:
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

            channel_poll_counter = (channel_poll_counter + 1) % max(1, POLL_INTERVAL_SEC // STREAM_POLL_SEC)

            # ── Fix A: upcoming→live fast detection (every cycle, 1 unit/vid)
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
                    # No schedule info — include as safe fallback
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
        
        cycle_start = datetime.now(timezone.utc)
        active_streams: list[dict] = list(known_streams.values())

        # ── Step 1: collect analytics for every live stream ───────────────
        # collect_and_store() now saves to CSV and records to history but
        # does NOT write to DB or regenerate the dashboard — those happen
        # below in a single batch, paying their fixed costs only once.
        db_batch: list[tuple[dict, str]] = []
        any_live = False
        for stream in active_streams:
            if stream["stream_status"] == "live":
                table = channel_tables.get(stream["channel_id"])
                collect_and_store(stream, table)
                if table:
                    db_batch.append((stream, table))
                any_live = True
            else:
                stream.setdefault("concurrent_viewers", 0)
                stream.setdefault("like_count", 0)
                stream.setdefault("comment_count", 0)

        # ── Step 2: flush all DB rows in one connection ───────────────────
        if db_batch:
            save_many_to_db(db_batch)

        # ── Step 3: regenerate dashboard once per cycle ───────────────────
        if any_live:
            regenerate_dashboard()

        # ── Step 4: deploy (throttled) ────────────────────────────────────
        if any_live:
            now = datetime.now(timezone.utc)
            if (now - last_deploy_time).total_seconds() >= DEPLOY_INTERVAL_SEC:
                deploy_dashboard()
                last_deploy_time = now

        log_active_streams(active_streams)

        # ── Step 5: sleep only the remaining time ─────────────────────────
        # Subtract the time already spent on work so the total cycle length
        # stays close to STREAM_POLL_SEC regardless of how many streams are
        # live or how long the dashboard regeneration takes.
        elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
        remaining = max(0.0, STREAM_POLL_SEC - elapsed)
        log.info(
            "Cycle: %.1fs work + %.1fs sleep = %.1fs total | "
            "Next channel scan in %ds",
            elapsed, remaining, elapsed + remaining,
            (max(1, POLL_INTERVAL_SEC // STREAM_POLL_SEC) - channel_poll_counter)
            * STREAM_POLL_SEC,
        )
        time.sleep(remaining)

    log.info("Tracker stopped.")

if __name__ == "__main__":
    run()
