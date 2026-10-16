# 📡 IDVTuber Livestream Tracker

An automated Python pipeline that monitors Indonesian VTuber (IDVTuber) YouTube channels for live streams, collects real-time analytics, stores them in **Supabase PostgreSQL**, and generates a static **HTML dashboard** deployed to GitHub Pages — all running fully headless on **GitHub Actions** with a self-hosted Windows runner.

---

## Overview

This project consists of two main scripts working in concert:

| Script | Purpose |
|---|---|
| `tracker.py` | Core polling loop — detects live/upcoming streams, collects analytics, writes to DB + CSV, and fires an update trigger to the dashboard generation scripts in the separate dashboard repo |
| `archiver.py` | Archives streams nearing the 30-day PostgreSQL deletion window into a SQLite history database |

---

## Architecture

```
GitHub Actions (self-hosted Windows runner) — this repo (tracker)
    │
    └── tracker.py  (runs every ~6 hours, automatically restarted)
            │
            ├── YouTube Data API v3  (activities.list, videos.list)
            ├── Supabase PostgreSQL     (per-channel tables)
            ├── analytics.csv        (local fallback)
            └── update trigger → dashboard repo's live generation script

GitHub Actions (Github-hosted Linux runner) — this repo (tracker)
    └── archiver.py  (runs daily)
            │
            ├── Supabase PostgreSQL     (reads streams older than threshold)
            └── history.db           (idvt-history repo, SQLite)


GitHub Actions (Github-hosted Linux runner) — dashboard repo
    └── generate_live.py (triggered by tracker.py runs) and generate_backfill.py (triggered chronically, catches up historical/archived data)
            │
            ├── Supabase PostgreSQL     (reads all channel tables)
            ├── history.db           (idvt-history repo, archived streams)
            └── dashboard/           (built + deployed via GitHub Pages)


```

---

## Features

| Feature | Detail |
|---|---|
| 🔔 Stream detection | Uses `activities.list` (1 unit) instead of `search.list` (100 units) for quota efficiency |
| ⏱️ Upcoming stream fast-poll | `check_upcoming_went_live` checks every 30s for streams near their scheduled start |
| 🔑 Single API key | One key from one consolidated Google Cloud project; when the daily quota is exhausted the tracker pauses API calls until the quota resets (00:05 Pacific) and resumes automatically |
| 📊 Real-time analytics | Concurrent viewers, view count, likes, comments per stream |
| 🗄️ Per-channel DB tables | Each channel gets its own PostgreSQL table (e.g. `stream_bima_bayusena_pandavva`) |
| 📄 CSV fallback | Always writes a local `analytics.csv` |
| 🌐 Static dashboard | 4-level HTML site: org index → channel list → stream cards → stream detail with charts |
| 📦 Stream archiver | Moves completed streams to SQLite (`history.db`) before the 30-day PostgreSQL TTL window |
| 🕰️ Timezone-aware | All display times in WIB (Asia/Jakarta, UTC+7); quota resets tracked in Pacific time |
| ⚙️ GitHub Actions | Fully headless, self-hosted Windows runner |

---

## Tracked Organisations

The dashboard currently tracks 180+ channels across **28 Indonesian VTuber organisations**, spanning a wide range of agencies, indie groups, and idol units active in the IDVTuber scene.

Organisation and channel definitions live in the `ORG_MAP` dict inside the dashboard repo's generation scripts (not in this repo). Add new channels or orgs there and they will be picked up on the next run.

> **Gotcha:** a channel's display name must match exactly between `ORG_MAP` and the `channels` registry table in Postgres. A mismatch here has previously caused streams to be collected but silently dropped instead of written to the right table — worth double-checking after renaming a channel or org entry.

---

## Repository Layout

```
.
├── tracker.py              # Core polling loop and analytics collector
├── archiver.py             # PostgreSQL → SQLite archiver
├── requirements.txt        # Python dependencies
├── analytics.csv           # Auto-created at runtime (local CSV fallback)
├── cache/
│   ├── channel_logos_cache.json     # Daily-cached YouTube channel thumbnails
│   └── channel_logos_fallback.json  # Last-known-good fallback for logos
└── .github/
    └── workflows/          # GitHub Actions workflow definitions
```

> **Note:** Dashboard generation and the `dashboard/` output folder live entirely in a **separate repository** (`DASHBOARD_REPO`). This repo only fires an update trigger; it doesn't build or push any HTML itself.

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `YOUTUBE_API_KEY` | A single YouTube Data API v3 key from the project's one Google Cloud project. |
| `CHANNEL_IDS` | Comma-separated YouTube channel IDs to monitor |
| `SUPABASE_DATABASE_URL` | |

### Optional

| Variable | Default | Description |
|---|---|---|
| `CSV_OUTPUT_PATH` | `analytics.csv` | Path for the local CSV fallback |
| `POLL_INTERVAL_SEC` | `60` | Seconds between channel scans for new streams |
| `STREAM_POLL_SEC` | `30` | Seconds between analytics collection for active streams |
| `MAX_HISTORY_POINTS` | `60` | In-memory data points kept per stream for charting |
| `UPCOMING_POLL_WINDOW_SEC` | `600` | Seconds before scheduled start to begin fast-polling an upcoming stream |
| `GH_PAT` | — | GitHub Personal Access Token used to fire the update trigger on the dashboard repo |
| `DASHBOARD_REPO` | *(tracker repo)* | Slug of the separate repo hosting GitHub Pages (e.g. `idvtuber-tracker/dashboard`) |
| `HISTORY_DB_PATH` | `../idvt-history/history.db` | Path to the SQLite archive database |
| `ARCHIVE_THRESHOLD_DAYS` | `25` | Days since last activity before a stream is archived |

---

## Database Schema

### PostgreSQL (Supabase) — per-channel tables

Each channel gets a dedicated table named `stream_<channel_slug>`, created automatically on first detection. All tables share this schema:

```sql
CREATE TABLE stream_<channel_slug> (
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
    stream_status      TEXT,          -- 'live' | 'upcoming' | 'vod'
    scheduled_start    TIMESTAMPTZ,
    actual_start       TIMESTAMPTZ
);
```

A `channels` registry table maps `channel_id → channel_name → table_name`.

### SQLite (`history.db`) — archive

Completed streams older than `ARCHIVE_THRESHOLD_DAYS` are migrated here by `archiver.py`:

```sql
CREATE TABLE streams (
    video_id     TEXT PRIMARY KEY,
    channel_name TEXT NOT NULL,
    org          TEXT NOT NULL,
    video_title  TEXT,
    stream_status TEXT,
    stream_start  TEXT,
    stream_end    TEXT,
    peak_viewers  INTEGER,
    avg_viewers   INTEGER,
    view_count    INTEGER,
    peak_likes    INTEGER,
    peak_comments INTEGER,
    data_points   INTEGER,
    archived_at   TEXT NOT NULL
);

CREATE TABLE timeseries (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id            TEXT NOT NULL REFERENCES streams(video_id),
    collected_at        TEXT NOT NULL,
    concurrent_viewers  INTEGER,
    view_count          INTEGER,
    like_count          INTEGER,
    comment_count       INTEGER
);
```

---

## Dashboard

The dashboard repo's generation scripts read from both PostgreSQL (recent/live streams) and `history.db` (archived streams) and build a 4-level static site:

```
dashboard/
├── index.html                          ← Organisation cards
├── manifest.json                       ← Partial-build state tracker
├── {org}/
│   └── index.html                      ← Channel list for org
│       └── {channel}/
│           └── index.html              ← Stream cards for channel
│               └── {video_id}.html     ← Stream detail + viewer/likes/comments charts
```

**Partial build:** Only stream pages that are new or currently live are regenerated each run. Finished VOD pages are skipped once written, keeping build time low regardless of history size. `manifest.json` tracks this partial-build state; it's worth being mindful of concurrent writes to it if a run is retried or overlaps with another.

Org and channel index pages support filtering and sorting via chips. Stream detail pages feature hero sections and KPI strips summarizing key stats, with charts supporting pinch-to-zoom and pan (via HammerJS + chartjs-plugin-zoom) for inspecting specific moments in a stream.

The site supports light/dark themes (follows system preference, with a manual toggle) and all times are displayed in WIB (UTC+7).

---

## Quick Start (Local)

### 1. Clone and install dependencies

```bash
git clone https://github.com/idvtuber-tracker/tracker.git
cd tracker
pip install -r requirements.txt
```

### 2. Set environment variables

```bash
export YOUTUBE_API_KEY="AIzaYourKey"
export CHANNEL_IDS="UCxxxxxx,UCyyyyyy"
export SUPABASE_DATABASE_URL="postgres://postgres:<password>@db.<project-ref>.supabase.co:<port>/postgres"
```

### 3. Run the tracker

```bash
python tracker.py
```

### 4. Run the archiver manually

```bash
python archiver.py
```

---

## GitHub Actions Setup

The tracker is designed to run on a **self-hosted Windows runner**. Add repository secrets under **Settings → Secrets and variables → Actions**:

| Secret | Required |
|---|---|
| `YOUTUBE_API_KEY` | ✅ |
| `CHANNEL_IDS` | ✅ |
| `SUPABASE_DATABASE_URL` | ✅ |
| `GH_PAT` | ✅ (to trigger the dashboard repo) |
| `DASHBOARD_REPO` | Recommended |

The tracker workflow runs on a endless loops of 6 hours session with the self-hosted runner that can be triggered manually and only stops in the event of error and the auto-restart unable to connect to the runner. Each run installs dependencies, executes `tracker.py`, collects analytics, and fires a `repository_dispatch` event to the dashboard repo, which runs its own generation script(s) and deploys to GitHub Pages.

The archiver workflow runs daily on an `ubuntu-latest` runner, checking out the `idvt-history` repo, running `archiver.py`, and committing any newly archived streams back to the history repo.

---

## API Quota Notes

- `activities.list` — **1 unit** per call (used for stream detection)
- `videos.list` — **1 unit** per call (used for analytics, upcoming status checks, and to check each upload found by the channel scan)
- `channels.list` — **1 unit** per call (used at startup and for logo/subscriber caching)
- `search.list` — **100 units** per call (intentionally avoided)

Estimated daily usage with the production workflow settings (`POLL_INTERVAL_SEC=900` → 96 channel scans/day, `STREAM_POLL_SEC=60`):

| Source | Cost | Per day |
|---|---|---|
| Channel scan | 1 unit/channel (`activities.list`) + 1 unit per upload found in that channel's 10 most recent activities (`videos.list`), i.e. 1–11 units per channel per scan | 96 (no recent uploads) to ~1,050 (10 recent uploads) **per tracked channel** |
| Live analytics | one batched `videos.list` per 60 s cycle while any stream is live (≤50 videos per call) | ≤ ~1,500 |
| Upcoming fast-poll | one batched `videos.list` per 60 s cycle while any stream is within its start window | ≤ ~1,500 |
| Logo/subscriber refresh | a few `channels.list` calls | < 10 |

The channel scan dominates. With the default free quota of 10,000 units/day, and reserving ~3,000 units/day for everything except the scan, the number of channels that can be tracked is roughly:

`channels ≈ 7,000 ÷ (96 × (1 + average recent uploads per channel))`

| Average recent uploads per channel | Units per channel per day | Approx. channels within 10,000 units/day |
|---|---|---|
| 0 (best case) | ~96 | ~70 |
| 3 (typical) | ~385 | ~18 |
| 10 (worst case) | ~1,050 | ~6 |

These are estimates; check actual consumption on the Google Cloud Console quota page. Tracking more channels than this requires a higher quota (see below). Note that the code default `POLL_INTERVAL_SEC=60` would multiply the scan cost by 15 compared with the workflow's 900 s.

All requests use a single API key from a single Google Cloud project. If the daily quota is exhausted, the tracker does not switch keys: it pauses YouTube API calls until the quota resets (midnight Pacific time, plus a small buffer) and then resumes automatically. Additional capacity should be requested through YouTube's official quota extension process, not by creating extra projects.

---

## Adding a New Organisation or Channel

Edit the `ORG_MAP` dictionary in the dashboard repo's generation scripts. Each entry follows this structure:

```python
"org-slug": {
    "label": "Display Name",
    "color": "#hexcolor",
    "desc": "Short description of the organisation.",
    "channels": [
        ("Channel Display Name", "org" | "talent", "UCxxxxxxxxxxxxxxxxxx"),
    ],
},
```

Then add the corresponding channel IDs to the `CHANNEL_IDS` secret/environment variable. The tracker will create the database table automatically on the first detected stream.

---

## Dependencies

```
google-api-python-client==2.131.0
psycopg2-binary==2.9.9
requests==2.32.3
tzdata==2025.3
```
