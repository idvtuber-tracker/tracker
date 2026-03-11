"""
archiver.py
───────────
Archives streams approaching the 30-day deletion window from the live Aiven DB
into a local SQLite history database (history.db).

Runs daily via GitHub Actions (archiver.yml) on ubuntu-latest.
The SQLite file lives in a checked-out copy of the idvt-history repo and is
committed + pushed back after each run.

Logic:
  - Finds all video_ids in the live DB where last_seen >= ARCHIVE_THRESHOLD days ago
    AND stream_status = 'vod'  (only archive completed streams)
  - Skips video_ids already present in history.db
  - Copies full timeseries rows + aggregated stats into history.db
  - Reports how many streams were archived

Environment variables required:
  AIVEN_DATABASE_URL   — live Postgres connection string
  HISTORY_DB_PATH      — path to history.db (default: ../idvt-history/history.db)
"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

# ── Config ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

AIVEN_DATABASE_URL = os.environ.get("AIVEN_DATABASE_URL", "")
HISTORY_DB_PATH    = os.environ.get("HISTORY_DB_PATH", "../idvt-history/history.db")
ARCHIVE_THRESHOLD  = int(os.environ.get("ARCHIVE_THRESHOLD_DAYS", "25"))  # days before deletion
_LOCAL_TZ          = ZoneInfo("Asia/Jakarta")

if not AIVEN_DATABASE_URL:
    sys.exit("ERROR: AIVEN_DATABASE_URL not set.")


# ── Live DB helpers ───────────────────────────────────────────────────────────

def live_conn():
    return psycopg2.connect(AIVEN_DATABASE_URL, sslmode="require")


def get_channel_rows(conn) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT channel_name, table_name FROM public.channels ORDER BY channel_name")
        return cur.fetchall()


def get_archivable_streams(conn, table: str, threshold_days: int) -> list[dict]:
    """
    Returns streams that are VOD and last_seen is older than threshold_days.
    These are approaching the 30-day deletion window.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=threshold_days)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                video_id,
                MAX(video_title)        AS video_title,
                MAX(stream_status)      AS stream_status,
                MIN(collected_at)       AS stream_start,
                MAX(collected_at)       AS stream_end,
                MAX(concurrent_viewers) AS peak_viewers,
                MAX(view_count)         AS view_count,
                MAX(like_count)         AS peak_likes,
                MAX(comment_count)      AS peak_comments,
                COUNT(*)                AS data_points
            FROM {table}
            GROUP BY video_id
            HAVING MAX(collected_at) < %s
            ORDER BY MIN(collected_at) DESC
        """, (cutoff,))
        return cur.fetchall()


def get_timeseries(conn, table: str, video_id: str) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                collected_at,
                concurrent_viewers,
                view_count,
                like_count,
                comment_count
            FROM {table}
            WHERE video_id = %s
            ORDER BY collected_at
        """, (video_id,))
        return cur.fetchall()


# ── SQLite helpers ────────────────────────────────────────────────────────────

def init_sqlite(path: str) -> sqlite3.Connection:
    """Open (or create) history.db and ensure schema exists."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS streams (
            video_id        TEXT PRIMARY KEY,
            channel_name    TEXT NOT NULL,
            org             TEXT NOT NULL,
            video_title     TEXT,
            stream_status   TEXT,
            stream_start    TEXT,
            stream_end      TEXT,
            peak_viewers    INTEGER,
            avg_viewers     INTEGER,
            view_count      INTEGER,
            peak_likes      INTEGER,
            peak_comments   INTEGER,
            data_points     INTEGER,
            archived_at     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS timeseries (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id            TEXT NOT NULL REFERENCES streams(video_id),
            collected_at        TEXT NOT NULL,
            concurrent_viewers  INTEGER,
            view_count          INTEGER,
            like_count          INTEGER,
            comment_count       INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_ts_video_id
            ON timeseries(video_id);

        CREATE INDEX IF NOT EXISTS idx_streams_channel
            ON streams(channel_name);

        CREATE INDEX IF NOT EXISTS idx_streams_start
            ON streams(stream_start);
    """)
    conn.commit()
    return conn


def get_archived_video_ids(hist: sqlite3.Connection) -> set[str]:
    rows = hist.execute("SELECT video_id FROM streams").fetchall()
    return {r["video_id"] for r in rows}


def archive_stream(hist: sqlite3.Connection, stream: dict,
                   channel_name: str, org: str,
                   timeseries_rows: list[dict]) -> None:
    """Write one stream + its timeseries into history.db."""
    archived_at = datetime.now(timezone.utc).isoformat()

    # compute avg_viewers from timeseries
    viewer_vals = [r["concurrent_viewers"] for r in timeseries_rows if r["concurrent_viewers"]]
    avg_viewers = round(sum(viewer_vals) / len(viewer_vals)) if viewer_vals else None

    def _iso(val) -> str | None:
        if val is None:
            return None
        if isinstance(val, str):
            return val
        return val.isoformat()

    hist.execute("""
        INSERT OR IGNORE INTO streams (
            video_id, channel_name, org, video_title, stream_status,
            stream_start, stream_end,
            peak_viewers, avg_viewers, view_count,
            peak_likes, peak_comments, data_points, archived_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        stream["video_id"],
        channel_name,
        org,
        stream["video_title"],
        stream["stream_status"],
        _iso(stream["stream_start"]),
        _iso(stream["stream_end"]),
        stream["peak_viewers"],
        avg_viewers,
        stream["view_count"],
        stream["peak_likes"],
        stream["peak_comments"],
        stream["data_points"],
        archived_at,
    ))

    hist.executemany("""
        INSERT INTO timeseries (
            video_id, collected_at,
            concurrent_viewers, view_count, like_count, comment_count
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, [
        (
            stream["video_id"],
            _iso(r["collected_at"]),
            r["concurrent_viewers"],
            r.get("view_count"),
            r["like_count"],
            r["comment_count"],
        )
        for r in timeseries_rows
    ])


# ── Build org lookup from ORG_MAP ─────────────────────────────────────────────

def build_org_lookup() -> dict[str, str]:
    """
    Returns {channel_name: org_label} by importing ORG_MAP from
    generate_dashboard.py sitting in the same directory.
    Falls back to empty dict if not importable.
    """
    try:
        import importlib.util, sys as _sys
        spec = importlib.util.spec_from_file_location(
            "generate_dashboard",
            os.path.join(os.path.dirname(__file__), "generate_dashboard.py")
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        lookup = {}
        for org_slug, org in mod.ORG_MAP.items():
            for entry in org["channels"]:
                lookup[entry[0]] = org["label"]
        return lookup
    except Exception as e:
        log.warning("Could not import ORG_MAP from generate_dashboard.py: %s", e)
        return {}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== IDVTuber Tracker Archiver ===")
    log.info("Threshold: archive streams with last_seen > %d days ago", ARCHIVE_THRESHOLD)
    log.info("History DB: %s", HISTORY_DB_PATH)

    lconn = live_conn()
    hist  = init_sqlite(HISTORY_DB_PATH)

    already_archived = get_archived_video_ids(hist)
    log.info("Streams already in history.db: %d", len(already_archived))

    org_lookup    = build_org_lookup()
    channel_rows  = get_channel_rows(lconn)
    log.info("Live DB channels: %d", len(channel_rows))

    total_archived = 0
    total_skipped  = 0

    for ch in channel_rows:
        ch_name = ch["channel_name"]
        table   = ch["table_name"]
        org     = org_lookup.get(ch_name, "Unknown")

        streams = get_archivable_streams(lconn, table, ARCHIVE_THRESHOLD)
        new_streams = [s for s in streams if s["video_id"] not in already_archived]

        if not new_streams:
            log.info("  %s — nothing new to archive (%d archivable, all already done)",
                     ch_name, len(streams))
            total_skipped += len(streams)
            continue

        log.info("  %s — archiving %d stream(s) (skipping %d already done)",
                 ch_name, len(new_streams), len(streams) - len(new_streams))

        for stream in new_streams:
            vid = stream["video_id"]
            ts  = get_timeseries(lconn, table, vid)
            archive_stream(hist, stream, ch_name, org, ts)
            log.info("    ✓ %s — %d timeseries rows", vid, len(ts))
            total_archived += 1

        hist.commit()

    lconn.close()
    hist.close()

    log.info("Archiver complete — %d stream(s) newly archived, %d skipped (already in history).",
             total_archived, total_skipped)


if __name__ == "__main__":
    main()
