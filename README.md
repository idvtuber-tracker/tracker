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
| 🔑 API key rotation | Supports multiple keys via `YOUTUBE_API_KEYS`; rotates automatically on 403, persists exhausted state across runs |
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
| `YOUTUBE_API_KEYS` | Comma-separated list of YouTube Data API v3 keys (e.g. `key1,key2,key3`). Supports rotation on quota exhaustion. Falls back to `YOUTUBE_API_KEY` for single-key setups. |
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
export YOUTUBE_API_KEYS="AIzaKey1,AIzaKey2"
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
| `YOUTUBE_API_KEYS` | ✅ |
| `CHANNEL_IDS` | ✅ |
| `SUPABASE_DATABASE_URL` | ✅ |
| `GH_PAT` | ✅ (to trigger the dashboard repo) |
| `DASHBOARD_REPO` | Recommended |

The tracker workflow runs on a endless loops of 6 hours session with the self-hosted runner that can be triggered manually and only stops in the event of error and the auto-restart unable to connect to the runner. Each run installs dependencies, executes `tracker.py`, collects analytics, and fires a `repository_dispatch` event to the dashboard repo, which runs its own generation script(s) and deploys to GitHub Pages.

The archiver workflow runs daily on an `ubuntu-latest` runner, checking out the `idvt-history` repo, running `archiver.py`, and committing any newly archived streams back to the history repo.

---

## API Quota Notes

- `activities.list` — **1 unit** per call (used for stream detection)
- `videos.list` — **1 unit** per call (used for analytics + upcoming status checks)
- `channels.list` — **1 unit** per call (used at startup and for logo/subscriber caching)
- `search.list` — **100 units** per call (intentionally avoided)

With the `activities.list`-based detection, a typical run across many channels stays well within the 10,000 unit/day free quota. Multiple API keys can be provided via `YOUTUBE_API_KEYS` and are rotated automatically when one is exhausted; exhausted state is persisted across runner restarts and cleared at the YouTube quota reset boundary (midnight Pacific time).

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
