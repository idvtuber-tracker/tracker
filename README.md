# 📡 YouTube Livestream Analytics Tracker

A "set and forget" Python tracker that monitors YouTube channels for live streams, collects real-time analytics, stores them in **Aiven PostgreSQL** (and/or CSV), and renders a live dashboard in the console. It runs fully automated on **GitHub Actions**.

---

## Features

| Feature | Detail |
|---|---|
| 🔔 Event trigger | Detects new / ongoing streams automatically |
| 📊 Real-time analytics | Concurrent viewers, likes, comments |
| 🗄️ Aiven PostgreSQL | Long-term storage via SSL connection |
| 📄 CSV fallback | Always writes a local `analytics.csv` |
| 🖥️ Console dashboard | Rich table + in-terminal viewer trend chart |
| ⚙️ GitHub Actions | Scheduled cron every 5 min, fully headless |

---

## Repository layout

```
.
├── tracker.py                  # main application
├── requirements.txt
├── analytics.csv               # auto-created at runtime
└── .github/
    └── workflows/
        └── tracker.yml         # GitHub Actions workflow
```

---

## Quick-start

### 1. Fork / clone this repo

```bash
git clone https://github.com/YOUR_USERNAME/yt-livestream-tracker.git
cd yt-livestream-tracker
```

### 2. Install dependencies locally (for testing)

```bash
pip install -r requirements.txt
```

### 3. Set environment variables

| Variable | Required | Description |
|---|---|---|
| `YOUTUBE_API_KEY` | ✅ | YouTube Data API v3 key |
| `CHANNEL_IDS` | ✅ | Comma-separated channel IDs, e.g. `UCxxxxxx,UCyyyyyy` |
| `AIVEN_DATABASE_URL` | optional | Aiven PostgreSQL DSN (`postgres://user:pass@host:port/db`) |
| `CSV_OUTPUT_PATH` | optional | Defaults to `analytics.csv` |
| `POLL_INTERVAL_SEC` | optional | How often to scan for new streams (default `60`) |
| `STREAM_POLL_SEC` | optional | How often to collect analytics (default `30`) |

#### Finding a Channel ID

Go to the channel page → view page source → search for `"channelId"`.  
Or use: `https://www.youtube.com/@HANDLE/about` and look at the URL after redirect.

### 4. Run locally

```bash
export YOUTUBE_API_KEY="AIza..."
export CHANNEL_IDS="UCxxxxxx,UCyyyyyy"
export AIVEN_DATABASE_URL="postgres://avnadmin:password@host.aivencloud.com:12345/defaultdb"

python tracker.py
```

---

## GitHub Actions (set and forget)

### Add repository secrets

Go to **Settings → Secrets and variables → Actions → New repository secret** and add:

- `YOUTUBE_API_KEY`
- `CHANNEL_IDS`
- `AIVEN_DATABASE_URL`

### How it works

The workflow (`.github/workflows/tracker.yml`) triggers every **5 minutes** (the GitHub minimum). Each run:

1. Installs dependencies
2. Runs `tracker.py` for up to 4 minutes
3. Uploads `analytics.csv` as an artifact (7-day retention)

Because data is also written to Aiven PostgreSQL, you have full long-term history even as CSV artifacts rotate.

---

## Aiven setup

1. Create a free PostgreSQL service at [aiven.io](https://aiven.io).
2. Copy the **Service URI** (it looks like `postgres://avnadmin:...@....aivencloud.com:PORT/defaultdb`).
3. The tracker automatically creates the `livestream_analytics` table on first run.

### Database schema

```sql
CREATE TABLE livestream_analytics (
    id                 SERIAL PRIMARY KEY,
    collected_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    channel_id         TEXT NOT NULL,
    channel_name       TEXT,
    video_id           TEXT NOT NULL,
    video_title        TEXT,
    concurrent_viewers BIGINT,
    like_count         BIGINT,
    comment_count      BIGINT,
    stream_status      TEXT,          -- 'live' | 'upcoming'
    scheduled_start    TIMESTAMPTZ,
    actual_start       TIMESTAMPTZ
);
```

### Useful queries

```sql
-- Peak viewers per stream
SELECT video_title, MAX(concurrent_viewers) AS peak
FROM livestream_analytics
GROUP BY video_id, video_title
ORDER BY peak DESC;

-- Viewer growth over time for a specific stream
SELECT collected_at, concurrent_viewers
FROM livestream_analytics
WHERE video_id = 'dQw4w9WgXcQ'
ORDER BY collected_at;
```

---

## Console output example

```
╭─────────────────────────────────────────────────────────╮
│ Event Trigger                                           │
│ 🔔 NEW STREAM DETECTED!                                 │
│ Channel : My Favourite Channel                          │
│ Title   : Sunday Livestream #42                         │
│ Status  : live                                          │
╰─────────────────────────────────────────────────────────╯

╭─ 📡 Live Streams ──────────────────────────────────────╮
│ Channel          Title               Status  Viewers    │
│ My Fav Channel   Sunday Livestream   🔴 LIVE  12,453    │
╰────────────────────────────────────────────────────────╯

 Viewers — My Fav Channel (last 12 samples)
 14000 ┤                          ╭──╮
 12000 ┤              ╭──╮    ╭───╯  ╰
 10000 ┤    ╭──╮  ╭───╯  ╰────╯
  8000 ┤────╯  ╰──╯
```

---

## Limitations & tips

- **YouTube API quota**: The free tier has 10,000 units/day. Each `search.list` costs 100 units; `videos.list` costs 1 unit. With 2 channels and polling every 60 s you'll use roughly 3,000–5,000 units/day — well within limits.
- **GitHub Actions minutes**: The free tier has 2,000 min/month. This workflow uses ~4 min per run × 288 runs/day = ~1,150 min/day, which may exceed free limits for heavy use. Consider self-hosting the runner or increasing `POLL_INTERVAL_SEC`.
- **Private/member-only streams**: These won't appear in Search API results without OAuth — API key access is public-data only.
