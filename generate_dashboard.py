"""
generate_dashboard.py
Pulls livestream analytics from PostgreSQL and generates a 4-level
static HTML dashboard:
  index.html                       ← org cards
  {org}/index.html                 ← channel list per org
  {org}/{channel}/index.html       ← stream cards per channel
  {org}/{channel}/{video}.html     ← stream detail + charts

Partial build algorithm:
  - A manifest (dashboard/manifest.json) tracks every generated stream page.
  - On each run, only stream pages that are NEW or currently LIVE are
    (re)generated. Their parent channel and org pages are then also
    regenerated to reflect updated stream counts / card lists.
  - The index page is always regenerated (trivially cheap).
  - Unchanged stream pages (VOD, already in manifest) are never touched.

Org membership is driven by the ORG_MAP dict below — update it when
channels are added or moved between organisations.
"""

import os
import re
import json
import shutil
import sqlite3
import logging
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_LOCAL_TZ = ZoneInfo("Asia/Jakarta")

def _now_local() -> datetime:
    return datetime.now(_LOCAL_TZ)

import psycopg2
import psycopg2.extras

try:
    from googleapiclient.discovery import build as yt_build
    from googleapiclient.errors import HttpError as _HttpError
    _YT_AVAILABLE = True
except ImportError:
    _YT_AVAILABLE = False

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
HISTORY_DB_PATH    = os.environ.get(
    "HISTORY_DB_PATH",
    str(Path(__file__).parent.parent / "idvt-history" / "history.db")
)
MANIFEST_PATH      = OUTPUT_DIR / "manifest.json"


# ── org definitions ───────────────────────────────────────────────────────────
ORG_MAP = {
    "pandavva": {
        "label":   "PANDAVVA",
        "color":   "#e8ff47",
        "desc":    "An Indonesian VTuber organization with a rich mythological theme inspired by the Mahabharata.",
        "channels": [
            ("PANDAVVA Official",             "org",    "UCxhBc3OUK0PdnjD-Pjj5-ZA"),
            ("Yudistira Yogendra 【PANDAVVA】", "talent", "UCdVRAGFhvkSIhMYxPdVBDzA"),
            ("Bima Bayusena【PANDAVVA】",       "talent", "UCJTNnAFxljZnKGMb5B8XKIA"),
            ("Arjuna Arkana【PANDAVVA】",       "talent", "UCmpT2MkZjPYkqLMrEHv6k0w"),
            ("Nakula Nalendra【PANDAVVA】",      "talent", "UCtGgHePeV6ePoTtlEspXJbQ"),
            ("Sadewa Sagara【PANDAVVA】",       "talent", "UCaQwGFUjKGFz0kqxJrP6etA"),
        ],
    },
    "project-livium": {
        "label":   "Project:LIVIUM",
        "color":   "#47ffb2",
        "desc":    "A dynamic VTuber project featuring seven unique talents spanning a wide range of creative personalities.",
        "channels": [
            ("Project:LIVIUM",                            "org",    "UC0ZYul2i5OcyKbdKB2v1O2w"),
            ("Indira Naylarissa Ch.〔LiviPro〕",   "talent", "UC0bqAp0JfFpJvgEp2U5LJHQ"),
            ("Silvia Valleria Ch.〔LiviPro〕",     "talent", "UCXRm3Aqtk5ilju1InZALcgA"),
            ("Yuura Yozakura Ch.〔LiviPro〕",      "talent", "UCnQAkbWmWkfOvRYoAza5cbA"),
            ("Ymelia Meiru Ch.〔LiviPro〕",        "talent", "UClv13dr4Q3eptzrH-Ul4e7Q"),
            ("Fareye Closhartt Ch.〔LiviPro〕",    "talent", "UCrC4jCRi3ZM-GxgLJSvmkfQ"),
            ("Yuela GuiGui Ch.〔LiviPro〕",        "talent", "UCnQAkbWmWkfOvRYoAza5cbB"),
            ("Lillis Infernallies Ch.〔LiviPro〕", "talent", "UCnQAkbWmWkfOvRYoAza5cbC"),
        ],
    },
    "whicker-butler": {
        "label":   "Whicker Butler",
        "color":   "#b47fff",
        "desc":    "A boutique VTuber agency known for its refined aesthetic and five distinctive talents with global appeal.",
        "channels": [
            ("Whicker Butler",                           "org",    "UCc04w_tCWOiTkszx5DGqSag"),
            ("Valthea Nankila 【 Whicker Butler 】",  "talent", "UCY1GUw8wBb_PSOzg7AoghvQ"),
            ("Ignis Grimoire【Whicker Butler】",       "talent", "UCJbrzGrVtSC0KbtkEzD50cw"),
            ("Darlyne Nightbloom【Whicker Butler】",   "talent", "UCtiNMw_89OUjPThykjwIsAA"),
            ("Thalita Sylvaine【Whicker Butler】",     "talent", "UCHNwyrNLObSZaHYAvvrGhCA"),
            ("Oriana Solstair【Whicker Butler】",      "talent", "UCvxEBCJlF0m81ffDtJ7YE2w"),
        ],
    },
    "yorukaze": {
        "label":   "Yorukaze Production",
        "color":   "#7ec8e3",
        "desc":    "An Indonesian VTuber organization serving as a bridge and support platform for virtual content creators.",
        "channels": [
            ("Yorukaze Production",            "org",    "UCXn1p9luEl8oUKL-dbtMd9g"),
            ("Hessa Elainore Ch.【Yorukaze】",  "talent", "UCL61Tr4KMxiv6o9u_WXxDCg"),
            ("Tsukiyo Miho Ch.【Yorukaze】",    "talent", "UC9VZtHQN1GGs5V7vZgvTQ4A"),
            ("Mihiro Kamigawa【Yorukaze】",      "talent", "UCUx_NfLnJma2dHzG90_I5HA"),
            ("Vincent Cerbero【Yorukaze】",      "talent", "UC2maoIIbLUrbPApf11GLR6A"),
            ("Utahime Yukari Ch.【Yorukaze】",   "talent", "UC4HgZAlD5MUFIe2nHYhPhJw"),
            ("Nanaka Poi Ch. 【Yorukaze】",      "talent", "UCFjaorskBcTDdDIQ5BP2ucg"),
            ("Amare Michiya【Yorukaze】",         "talent", "UC04yaXbxeiG_sN47idcdimg"),
            ("Hoshikawa Rui【Yorukaze】",         "talent", "UCnh6AfYwFB9Elsdtkl73cuQ"),
            ("Yuzumi_Ch【Yorukaze】",             "talent", "UCCZ4ZY1kaSkZC6OiA-2916Q"),
            ("Ellise Youka【Yorukaze】",          "talent", "UCE5Mvtoy8GiPtsv5sLUKlgg"),
            ("WanTaps Ch.【Yorukaze】",           "talent", "UC7CpE_gbbvNBkUFMHWeUMpA"),
            ("Wintergea Ch. ゲア 【Yorukaze】",   "talent", "UCv9P--tuUkAxpZaTowy7h9Q"),
        ],
    },
    "prism-nova": {
        "label":   "Prism:NOVA",
        "color":   "#c084fc",
        "desc":    "An Indonesian VTuber agency focused on characterisation, storytelling, and roleplaying.",
        "channels": [
            ("Prism:NOVA",                      "org",    "UCpaiXLRcHzx5XpHysrO9JQA"),
            ("Oxa Lydea 【Prism:NOVA】",         "talent", "UCV2KRUSE92ZyAPed1770Dww"),
            ("Serika Cosmica 【Prism:NOVA】",    "talent", "UCjIlQoGYrKRyykHTJdJ9fdA"),
            ("Thalia Symphonia 【Prism:NOVA】",  "talent", "UCBGkli-RvhJozIcXVVKg2iA"),
        ],
    },
    "vcosmix": {
        "label":   "VCosmix",
        "color":   "#f472b6",
        "desc":    "An Indonesian VTuber group who provides girls fun experience.",
        "channels": [
            ("Vcosmix",          "org",    "UCxdS5pTt5WfbTD6WUY8z2EQ"),
            ("Lea Lestari Ch.",  "talent", "UCmlIjSXna6pZLeM7HK8Vewg"),
            ("Miichan Chu Ch.",  "talent", "UCx-WXnxhiZwUsr_bagtUxwQ"),
            ("Li Mingshu Ch.",   "talent", "UCJL18InOQxSBXswn4sfD_fQ"),
        ],
    },
    "dexter": {
        "label":   "DEXTER",
        "color":   "#f87171",
        "desc":    "An Indonesian VTuber agency with male talents that specializes in various contents and singing.",
        "channels": [
            ("Dexter Official",              "org",    "UCVitBVX8nnvP0gfe1s5VRGA"),
            ("Richard Ravindra【DEXTER】",    "talent", "UCeEXULUk2S16jjLSv7ar51Q"),
            ("Rex Arcadia【DEXTER】",         "talent", "UCCv6ctVKeh2U3LAH1RNejmQ"),
            ("Lucentia【DEXTER】",            "talent", "UC1J_JlLEIzkXwkDdkEhE2dg"),
            ("Noa Florastra【DEXTER】",       "talent", "UC19WbhRSDkExpUE6XAqDANg"),
        ],
    },
    "cozycazt": {
        "label":   "CozyCazt",
        "color":   "#fb923c",
        "desc":    "An Indonesian VTuber agency which projects comfortable and friendly aura.",
        "channels": [
            ("Cozy Cazt",                        "org",    "UCFCfSe5tJrnQt3cuT9g59Lw"),
            ("Rannia Taiga 【CozyCazt】",         "talent", "UCjPBlVNDtHHYwrvpxBCiY5g"),
            ("Lyta Luciana Ch.【CozyCazt】",      "talent", "UC7nVykWmH1ORUOBU3bTlNIg"),
            ("Arphina Stellaria【CozyCazt】",     "talent", "UCDxHKSgQD7tcTr36F2GnvDg"),
            ("Vianna Risendria 【CozyCazt】",     "talent", "UCuGlDdzoTyM55cQrJO_kEZw"),
            ("Fuyo Mafuyu【CozyCazt】",           "talent", "UCMtHNyhNeLZEPw6S2-1556A"),
            ("Silveryshore Ch.【CozyCazt】",      "talent", "UCQpD-UhHdhFL1DTJxhJpuyA"),
        ],
    },
    "afterain": {
        "label":   "AfteRain",
        "color":   "#60a5fa",
        "desc":    "An Indonesian VTuber agency with various talents background and specialties.",
        "channels": [
            ("AFTERAIN PROJECT",             "org",    "UCOJwb4RalSz3_3EHIM5pVfw"),
            ("LynShuu 【AFTERAIN】",          "talent", "UCzsHESRY504seJawYVRSs7Q"),
            ("Nezufu Senshirou【AFTERAIN】",  "talent", "UCGR-Fzxnm0TQs2uAIWn8IlQ"),
            ("Lvna Tylthia【AFTERAIN】",      "talent", "UCgZoh0CWVTg_o_wHy1F5QBQ"),
            ("Poffie Hunni【AFTERAIN】",      "talent", "UCFgLJQqhovnBBf4CIsC0OCA"),
            ("Flein Ryst【AFTERAIN】",        "talent", "UCY-fhXM0BzpBtBk1czoY5fA"),
            ("Avy Inkaiserin 【AFTERAIN】",   "talent", "UCvHWaiG9YSPgmLhNuLmqMUA"),
            ("Kana Chizu 【AFTERAIN】",       "talent", "UCh5wq5bs4VG1ah3THbW156g"),
            ("Ririna Ruu【AFTERAIN】",        "talent", "UC_vnL6pH3Mm3XrnuQ38LWtQ"),
        ],
    },
    "magniv": {
        "label":   "MagniV",
        "color":   "#a855f7",
        "desc":    "An Indonesian Male VTuber idol group. Concept: 'Five as one, we shine.'",
        "channels": [
            ("Gema Gathika【MagniV】", "talent", "UC9Mfuai-qdXnTTFN0Z3hkAA"),  
            ("Istmodius【AKA Virtual】", "talent", "UCpe6USwJgyctDpWQhzVBeVQ"),  
            ("Mosa【MagniV】", "talent", "UCkYmpSIAkPgNg4wBeNn9JAA"), 
            ("Funin Mamori【AKA Virtual】", "talent", "UCO6ngsu6Bx1SnJgL1iLyafA"), 
        ],
    },
    "sandaiva": {
        "label":   "SANDAiVA",
        "color":   "#e879f9",
        "desc":    "A three-member Indonesian VTuber idol unit.",
        "channels": [
            ("SANDAiVA【AKA Virtual】", "org", "UCHiV8178uHAj6a6KxrYiEdQ"),  
            ("Raveanne【AKA Virtual】", "talent", "UCmNeOjitXbWHg_CDNGrwoHw"),  
            ("njess 【AKA Virtual】 ", "talent", "UC_AGUYW4usybdi-WTZQcYxg"),  
            ("Quiver Rannette Ch.【AKA Virtual】", "talent", "UCGx0t5bUkm-rY3jKSRolY1w"),  
        ],
    },
    "versa": {
        "label":   "VERSA",
        "color":   "#38bdf8",
        "desc":    "An Indonesian VTuber boyband group.",
        "channels": [
            ("Agata Seven【AKA Virtual】", "talent", "UC4NdM7WwMvyGzkUr0dsSGJQ"),  
            ("Alarich【AKA Virtual】", "talent", "UCn58MSGrtsDY8N_VhyLcPjg"),  
            ("Eray Ryuki【AKA Virtual】", "talent", "UCXp26d9RQqUnCeRwaulXZgA"),  
            ("Ryoutaa  【AKA Virtual】", "talent", "UCRQCV5LXaJKVQ35qaMuI0dw"),  
            ("SouRizu☪︎【AKA Virtual】", "talent", "UCs2eFSeyAQqjH7PyjZwSH1w"),  
        ],
    },
    "jkt48v": {
        "label":   "JKT48V",
        "color":   "#f59e0b",
        "desc":    "The virtual idol sub-unit of JKT48, Indonesia's iconic idol group. Your Idol, Comes Virtual.",
        "channels": [
            ("JKT48V", "org", "UCX3wkex0h-KP7Z3Q9SDkMIA"),  
            ("Pia Meraleo - JKT48V", "talent", "UCIa2OxCyhjWjJke-9yYNbwA"),  
            ("Tana Nona - JKT48V", "talent", "UCyam-qAWHwBoVnTNXk3gHbQ"),  
            ("Sami Maono - JKT48V", "talent", "UCrLhVcbVYhSGWlR6oM8FqTg"),  
            ("Isha Kirana - JKT48V", "talent", "UCYm4XQ_YzSnaBZ0UdOIAlrQ"),  
            ("Maura Nilambari - JKT48V", "talent", "UCWK3jDHD_LzCTu4CF7amN8A"),  
        ],
    },
    "maha5": {
        "label":   "MAHA5",
        "color":   "#34d399",
        "desc":    "An Indonesian VTuber agency (Mahapanca) under Rentracks Indonesia, connecting Indonesia and Japan through anime and otaku culture.",
        "channels": [
            ("MAHA5 mahapanca - Vtuber Group", "org", "UCzc8GwjUvecxpjhGtuewYOQ"),  
            ("Kevin Vangardo【MAHA5】", "talent", "UCAnKiHbZhEayttn6p-sxfbg"),  
            ("Rena Anggraeni【MAHA5】", "talent", "UCjQyHnE_Q58jYTaP8gRHv4g"),  
            ("Hera Garalea【MAHA5】", "talent", "UCXMdn7Omv5l2yqQxuepQtNA"),  
            ("Daisy Ignacia Y【MAHA5】", "talent", "UCgwZmQZC7O-TP1Xbnz50VtQ"),  
            ("Saku Kurata 【MAHA5】", "talent", "UCxL9H-mOD2Op4yynXPOWGnQ"),  
            ("Maudy Sukaiga【MAHA5】", "talent", "UCmp1vw137-GvWyrBFraXQUw"),  
            ("Fuyumi Celestia【MAHA5】", "talent", "UCge_6FJHyeOCxRtWCmaVTAQ"),  
        ],
    },
    "rememories": {
        "label":   "Re:Memories",
        "color":   "#fb7185",
        "desc":    "An Indonesian indie VTuber agency focused on two-way interactive entertainment and fostering real connections with their community.",
        "channels": [
            ("Re:Memories", "org", "UCJZnhqz3mNpWJJ1FGrAy_qA"),  
            ("Chloe Pawapua Ch.『 Re:Memories 』", "talent", "UCrKS2bOUZDXA_R3qhCux7ow"),  
            ("Lily Ifeta Ch.『 Re:Memories 』", "talent", "UCXSbl3XQYtx1u4Gvvca7NUA"),  
            ("Reynard Blanc Ch.『 Re:Memories 』", "talent", "UCoUFv7APM1XOo4TUaWbRekw"),  
            ("Elaine Celestia Ch.『 Re:Memories 』", "talent", "UCyapmNSsYj2KkoQEhZEhxrw"),  
            ("Cecilia Lieberia Ch.『 Re:Memories 』", "talent", "UC4pEixMozb6UnOtwg5Uew-Q"),  
            ("Marin Goldlock Ch.『 Re:Memories 』", "talent", "UCgexPS9fwEtLTCM8VZnEpjA"),  
            ("Izanami Chiara Ch.『 Re:Memories 』", "talent", "UCYVhzyujupNgRbvVPcA3KrA"),  
            ("Leo Axenos Ch.『 Re:Memories 』", "talent", "UCoWz6jan_0RD-Z1pk3-h9Mg"),  
            ("Pinku Rimu", "talent", "UC1fvUNao61EenXh0KPFGSTg"),  
        ],
    },
    "eon-of-stars": {
        "label":   "Eon of Stars",
        "color":   "#818cf8",
        "desc":    "An Indonesian indie male VTuber group providing high-quality boyfriend experience.",
        "channels": [
            ("EON OF STARS", "org", "UCpvevZ8VPSNe4qlSKAcLMBg"),  
            ("Harris Caine【EOS】", "talent", "UCtC7olOldksX4fcl_8XKUFA"),  
            ("Gingitsune Gehenna【EOS】", "talent", "UC8D3XmwYEr97q-tuNZjUuww"),  
            ("Souta【EOS】", "talent", "UCv7rxNkDhRu-uyyLIQg2tew"),  
            ("Mikazuki Arion【EOS】", "talent", "UCz_9zqgFPUYQBhDiZBFc00w"),  
        ],
    },

}

# Build reverse lookup: channel_name → (org_slug, org)
_CH_TO_ORG: dict[str, tuple[str, dict]] = {}
for _slug, _org in ORG_MAP.items():
    for _entry in _org["channels"]:
        _CH_TO_ORG[_entry[0]] = (_slug, _org)


# ══════════════════════════════════════════════════════════════════════════════
# MANIFEST
# ══════════════════════════════════════════════════════════════════════════════

def load_manifest() -> dict:
    """
    Returns the manifest dict, keyed by video_id.
    Each entry: {org_slug, ch_slug, ch_name, status, generated_at}
    """
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("Manifest unreadable (%s) — treating as empty.", e)
    return {}


def save_manifest(manifest: dict) -> None:
    """Write manifest atomically via a temp file so a mid-write crash can never
    corrupt the file and cause 'Manifest unreadable' warnings on the next run."""
    try:
        tmp = MANIFEST_PATH.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        tmp.replace(MANIFEST_PATH)   # atomic on POSIX; near-atomic on Windows
    except Exception as e:
        log.warning("Could not save manifest: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════════════════════════════

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


_schema_cache: dict[str, dict] = {}  # table_name → {exists, has_view_count}


def _load_schema_cache(conn, tables: list[str]) -> None:
    """
    Bulk-load table existence and view_count column presence for all tables
    in a single query. Results are stored in _schema_cache for the lifetime
    of the process — schema never changes mid-run.
    """
    global _schema_cache
    if not tables:
        return
    placeholders = ",".join(["%s"] * len(tables))
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                t.table_name,
                bool_or(c.column_name = 'view_count') AS has_view_count
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c
                ON  c.table_schema = t.table_schema
                AND c.table_name   = t.table_name
            WHERE t.table_schema = 'public'
              AND t.table_name IN ({placeholders})
            GROUP BY t.table_name
        """, tables)
        for row in cur.fetchall():
            _schema_cache[row["table_name"]] = {
                "exists":         True,
                "has_view_count": bool(row["has_view_count"]),
            }
    # tables not returned by the query simply don't exist
    for t in tables:
        if t not in _schema_cache:
            _schema_cache[t] = {"exists": False, "has_view_count": False}
    log.info("Schema cache loaded for %d tables (%d exist).",
             len(tables), sum(1 for v in _schema_cache.values() if v["exists"]))


def _table_exists(conn, table: str) -> bool:
    if table not in _schema_cache:
        _load_schema_cache(conn, [table])
    return _schema_cache[table]["exists"]


def _has_column(conn, table: str, column: str) -> bool:
    if column != "view_count":
        # only view_count is cached; fall back to direct query for anything else
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name   = %s
                  AND column_name  = %s
            """, (table, column))
            return cur.fetchone() is not None
    if table not in _schema_cache:
        _load_schema_cache(conn, [table])
    return _schema_cache[table]["has_view_count"]


def get_streams_for_channel(conn, table: str) -> list[dict]:
    if not _table_exists(conn, table):
        log.warning("Table '%s' does not exist yet — skipping.", table)
        return []
    view_count_expr = (
        "MAX(view_count) AS view_count"
        if _has_column(conn, table, "view_count")
        else "NULL::BIGINT AS view_count"
    )
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                video_id,
                MAX(video_title)        AS video_title,
                MAX(stream_status)      AS stream_status,
                MIN(collected_at)       AS first_seen,
                MAX(collected_at)       AS last_seen,
                MAX(concurrent_viewers) AS peak_viewers,
                {view_count_expr},
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


# ══════════════════════════════════════════════════════════════════════════════
# HISTORY DB HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_history_conn():
    path = HISTORY_DB_PATH
    if not os.path.exists(path):
        log.info("history.db not found at %s — archived streams will not be shown.", path)
        return None
    try:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn
    except Exception as e:
        log.warning("Could not open history.db: %s", e)
        return None


def get_archived_streams_for_channel(hist, channel_name: str,
                                     exclude_video_ids: set) -> list:
    rows = hist.execute("""
        SELECT
            video_id, video_title, stream_status,
            stream_start  AS first_seen,
            stream_end    AS last_seen,
            peak_viewers, avg_viewers, view_count,
            peak_likes, peak_comments, data_points
        FROM streams
        WHERE channel_name = ?
        ORDER BY stream_start DESC
    """, (channel_name,)).fetchall()

    result = []
    for r in rows:
        if r["video_id"] in exclude_video_ids:
            continue
        d = dict(r)
        for key in ("first_seen", "last_seen"):
            val = d.get(key)
            if isinstance(val, str):
                try:
                    d[key] = datetime.fromisoformat(val)
                except ValueError:
                    pass
        d["_source"] = "history"
        result.append(d)
    return result


def get_archived_timeseries(hist, video_id: str) -> list:
    rows = hist.execute("""
        SELECT collected_at, concurrent_viewers, like_count, comment_count
        FROM timeseries
        WHERE video_id = ?
        ORDER BY collected_at
    """, (video_id,)).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("collected_at"), str):
            try:
                d["collected_at"] = datetime.fromisoformat(d["collected_at"])
            except ValueError:
                pass
        result.append(d)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# LOGO / SUBSCRIBER CACHE
# ══════════════════════════════════════════════════════════════════════════════

_CACHE_DIR           = Path(__file__).parent / "cache"
_LOGO_CACHE_FILE     = str(_CACHE_DIR / "channel_logos_cache.json")
_LOGO_FALLBACK_FILE  = str(_CACHE_DIR / "channel_logos_fallback.json")


def _load_fallback() -> tuple[dict[str, str], dict[str, int]]:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if os.path.exists(_LOGO_FALLBACK_FILE):
        try:
            with open(_LOGO_FALLBACK_FILE, encoding="utf-8") as f:
                data = json.load(f)
            logos       = data.get("logos", {})
            subscribers = data.get("subscribers", {})
            saved_at    = data.get("saved_at", "unknown date")
            log.info("Loaded fallback channel data from %s (%d logos, %d subscriber counts).",
                     saved_at, len(logos), len(subscribers))
            return logos, subscribers
        except Exception as e:
            log.warning("Fallback cache unreadable: %s", e)
    return {}, {}


def _save_fallback(logos: dict[str, str], subscribers: dict[str, int]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_LOGO_FALLBACK_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "saved_at":    _now_local().strftime("%Y-%m-%d %H:%M WIB"),
                "logos":       logos,
                "subscribers": subscribers,
            }, f)
        log.info("Fallback channel data updated (%d logos, %d subscriber counts).",
                 len(logos), len(subscribers))
    except Exception as e:
        log.warning("Could not save fallback channel data: %s", e)


def get_channel_data(channel_ids: list[str]) -> tuple[dict[str, str], dict[str, int]]:
    """
    Fetch channel thumbnail URLs and subscriber counts from YouTube API.
    Rotates through all available API keys on 403.
    Falls back to last successful fetch on complete failure.
    Results are cached to disk for the remainder of the local day.

    Cache validity requires BOTH:
      (a) the cache date matches today, AND
      (b) every requested channel_id is already present in the cache.
    If new channel IDs are requested (e.g. newly-added orgs), the cache is
    considered stale and a fresh fetch is performed for all missing IDs.
    The result is then merged back into the cache and saved.
    """
    today = _now_local().strftime("%Y-%m-%d")

    if os.path.exists(_LOGO_CACHE_FILE):
        try:
            with open(_LOGO_CACHE_FILE, encoding="utf-8") as f:
                cache = json.load(f)
            if cache.get("date") == today:
                cached_logos = cache.get("logos", {})
                cached_subs  = cache.get("subscribers", {})
                missing_ids  = [cid for cid in channel_ids if cid not in cached_logos]
                if not missing_ids:
                    log.info("Using cached channel data (%d entries, all present).",
                             len(cached_logos))
                    return cached_logos, cached_subs
                log.info(
                    "Cache is from today but missing %d channel ID(s) — "
                    "fetching missing entries only.",
                    len(missing_ids),
                )
                # Fall through to fetch only the missing IDs, then merge below
                channel_ids = missing_ids
                # Keep existing cached data so we can merge at the end
                _partial_cache = (cached_logos, cached_subs)
            else:
                _partial_cache = None
        except Exception as e:
            log.warning("Logo cache unreadable (%s) — will re-fetch.", e)
            _partial_cache = None
    else:
        _partial_cache = None

    raw_keys = os.environ.get("YOUTUBE_API_KEYS") or os.environ.get("YOUTUBE_API_KEY", "")
    api_keys = [k.strip() for k in raw_keys.split(",") if k.strip()]

    if not _YT_AVAILABLE:
        log.warning("Channel data fetch skipped: google-api-python-client not installed.")
        return _load_fallback()
    if not api_keys:
        log.warning("Channel data fetch skipped: no API keys in environment.")
        return _load_fallback()
    if not channel_ids:
        log.warning("Channel data fetch skipped: channel_ids list is empty.")
        return _load_fallback()

    logos:       dict[str, str] = {}
    subscribers: dict[str, int] = {}
    api_failed   = False

    for i in range(0, len(channel_ids), 50):
        batch      = channel_ids[i:i + 50]
        batch_done = False

        for api_key in api_keys:
            try:
                log.info("channels.list batch %d–%d using key ...%s",
                         i, i + len(batch), api_key[-6:])
                yt   = yt_build("youtube", "v3", developerKey=api_key)
                resp = yt.channels().list(
                    part="snippet,statistics",
                    id=",".join(batch),
                    maxResults=50,
                ).execute()
                items_returned = resp.get("items", [])
                log.info("  → %d item(s) returned (totalResults=%s).",
                         len(items_returned),
                         resp.get("pageInfo", {}).get("totalResults", "?"))
                for item in items_returned:
                    cid    = item["id"]
                    thumbs = item.get("snippet", {}).get("thumbnails", {})
                    url    = (thumbs.get("medium") or thumbs.get("default") or {}).get("url", "")
                    if url:
                        logos[cid] = url
                    else:
                        log.warning("No thumbnail URL found for channel ID: %s", cid)
                    sub_count = item.get("statistics", {}).get("subscriberCount")
                    if sub_count is not None:
                        subscribers[cid] = int(sub_count)
                batch_done = True
                break
            except _HttpError as e:
                if e.resp.status == 403:
                    log.warning("403 on channels.list (key ...%s) — rotating.", api_key[-6:])
                    continue
                log.error("channels.list HTTP error (key ...%s): %s", api_key[-6:], e)
                api_failed = True
                break
            except Exception as e:
                log.error("channels.list unexpected error (key ...%s): %s", api_key[-6:], e)
                api_failed = True
                break

        if not batch_done:
            log.error("All %d key(s) failed for batch %d–%d.", len(api_keys), i, i + len(batch))
            api_failed = True

    if api_failed and not logos and not subscribers:
        log.warning("API fetch produced no data — falling back to last known good channel data.")
        # Still merge with any partial cache we loaded earlier
        if _partial_cache:
            return _partial_cache
        return _load_fallback()

    if api_failed and (logos or subscribers):
        log.warning("API fetch partially failed — merging fresh results with fallback data.")
        fallback_logos, fallback_subs = _load_fallback()
        logos       = {**fallback_logos, **logos}
        subscribers = {**fallback_subs,  **subscribers}

    # Merge with the partial cache (data already present from today's earlier fetch)
    if _partial_cache:
        prev_logos, prev_subs = _partial_cache
        logos       = {**prev_logos, **logos}       # fresh data wins on conflict
        subscribers = {**prev_subs,  **subscribers}

    try:
        with open(_LOGO_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"date": today, "logos": logos, "subscribers": subscribers}, f)
        log.info("Channel data cached — %d logos, %d subscriber count(s) total.",
                 len(logos), len(subscribers))
    except Exception as e:
        log.warning("Could not save channel data cache: %s", e)

    _save_fallback(logos, subscribers)
    return logos, subscribers


# ══════════════════════════════════════════════════════════════════════════════
# UTILITY HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def fmt(n) -> str:
    if n is None:
        return "—"
    try:
        return f"{int(n):,}"
    except (ValueError, TypeError):
        return str(n)


def fmt_subs(n) -> str:
    if n is None:
        return "—"
    try:
        n = int(n)
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.1f}K"
        return str(n)
    except (ValueError, TypeError):
        return "—"


def fmt_dt(dt) -> str:
    if dt is None:
        return "—"
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_LOCAL_TZ).strftime("%Y-%m-%d %H:%M WIB")
    try:
        parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(_LOCAL_TZ).strftime("%Y-%m-%d %H:%M WIB")
    except Exception:
        return str(dt)[:16]


def esc(s) -> str:
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))


# ══════════════════════════════════════════════════════════════════════════════
# SHARED CSS + HTML HELPERS
# ══════════════════════════════════════════════════════════════════════════════

_FONTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link href="https://fonts.googleapis.com/css2?family=DM+Mono:ital,wght'
    '@0,400;0,500;1,400&family=Fraunces:ital,opsz,wght@0,9..144,300;0,9..144'
    ',700;1,9..144,400&display=swap" rel="stylesheet">'
)

_BASE_CSS = """
  :root {
    --bg:          #0a0a0f;
    --surface:     #13131a;
    --surface2:    #1a1a26;
    --border:      #1e1e2e;
    --muted:       #5a5a7a;
    --text:        #e2e2f0;
    --white:       #ffffff;
    --red:         #ff4f6d;
    --blue:        #4fc3f7;
    --org-color:   #e8ff47;
    --accent-text: var(--org-color);
  }

  /* ── light theme ── */
  [data-theme="light"] {
    --bg:          #f5f5f0;
    --surface:     #ffffff;
    --surface2:    #eaeae5;
    --border:      #d8d8ce;
    --muted:       #767670;
    --text:        #1a1a14;
    --white:       #1a1a14;
    --red:         #c0002a;
    --blue:        #005f8a;
    --accent-text: #2a2a20;
  }

  /* ── follow system preference by default ── */
  @media (prefers-color-scheme: light) {
    :root:not([data-theme="dark"]) {
      --bg:          #f5f5f0;
      --surface:     #ffffff;
      --surface2:    #eaeae5;
      --border:      #d8d8ce;
      --muted:       #767670;
      --text:        #1a1a14;
      --white:       #1a1a14;
      --red:         #c0002a;
      --blue:        #005f8a;
      --accent-text: #2a2a20;
    }
  }

  /* ── light theme badge and pill background overrides ── */
  [data-theme="light"] .status-live     { background: rgba(192,0,42,0.10); color: var(--red);  border-color: var(--red);  }
  [data-theme="light"] .status-upcoming { background: rgba(0,95,138,0.10); color: var(--blue); border-color: var(--blue); }
  [data-theme="light"] .status-vod      { background: rgba(100,100,90,0.12); }
  [data-theme="light"] .pill-live       { background: rgba(192,0,42,0.10); color: var(--red);  border-color: var(--red);  }
  [data-theme="light"] .pill-upcoming   { background: rgba(0,95,138,0.10); color: var(--blue); border-color: var(--blue); }
  @media (prefers-color-scheme: light) {
    :root:not([data-theme="dark"]) .status-live     { background: rgba(192,0,42,0.10); color: var(--red);  border-color: var(--red);  }
    :root:not([data-theme="dark"]) .status-upcoming { background: rgba(0,95,138,0.10); color: var(--blue); border-color: var(--blue); }
    :root:not([data-theme="dark"]) .status-vod      { background: rgba(100,100,90,0.12); }
    :root:not([data-theme="dark"]) .pill-live       { background: rgba(192,0,42,0.10); color: var(--red);  border-color: var(--red);  }
    :root:not([data-theme="dark"]) .pill-upcoming   { background: rgba(0,95,138,0.10); color: var(--blue); border-color: var(--blue); }
  }

  /* ── light theme: suppress neon glow effects ── */
  [data-theme="light"] .org-dot,
  [data-theme="light"] .month-heading::before { box-shadow: none; }
  @media (prefers-color-scheme: light) {
    :root:not([data-theme="dark"]) .org-dot              { box-shadow: none; }
    :root:not([data-theme="dark"]) .month-heading::before { box-shadow: none; }
  }

  /* ── theme toggle ── */
  .theme-toggle { margin-left: auto; flex-shrink: 0; display: flex; align-items: center; }
  .toggle-pill {
    position: relative; display: flex; align-items: center;
    width: 56px; height: 28px;
    background: var(--surface2); border: 1px solid var(--border);
    border-radius: 14px; cursor: pointer;
    transition: background 0.25s, border-color 0.25s;
  }
  .toggle-pill:hover { border-color: var(--org-color); }
  .toggle-thumb {
    position: absolute; left: 4px;
    width: 20px; height: 20px; border-radius: 50%;
    background: var(--org-color);
    transition: transform 0.25s cubic-bezier(0.34, 1.56, 0.64, 1);
    display: flex; align-items: center; justify-content: center;
    font-size: 11px; line-height: 1; pointer-events: none;
  }
  .toggle-thumb.is-light { transform: translateX(28px); }
  .toggle-icon-dark, .toggle-icon-light {
    position: absolute; font-size: 10px; line-height: 1; pointer-events: none;
  }
  .toggle-icon-dark  { right: 7px; }
  .toggle-icon-light { left:  7px; }

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
  .breadcrumb a:hover { color: var(--accent-text); }
  .breadcrumb .sep { color: var(--border); }
  .breadcrumb .current { color: var(--accent-text); }

  /* headings */
  .eyebrow {
    font-size: 0.65rem; letter-spacing: 0.3em; text-transform: uppercase;
    color: var(--accent-text); margin-bottom: 0.6rem;
  }
  h1 {
    font-family: 'Fraunces', serif;
    font-size: clamp(2rem, 5vw, 4.5rem);
    font-weight: 700; line-height: 1.0; color: var(--white); margin-bottom: 0.5rem;
  }
  h1 em { font-style: italic; color: var(--accent-text); }
  .page-meta { font-size: 0.72rem; color: var(--muted); margin-top: 0.75rem; }

  /* org cards */
  .orgs-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1.5rem; margin-top: 3rem; }
  @media (max-width: 1100px) { .orgs-grid { grid-template-columns: repeat(2, 1fr); } }
  @media (max-width: 600px)  { .orgs-grid { grid-template-columns: 1fr; } }
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
  .org-dot { width: 10px; height: 10px; border-radius: 50%; background: var(--org-color); margin-bottom: 1.25rem; box-shadow: 0 0 12px var(--org-color); }
  .org-title { font-family: 'Fraunces', serif; font-size: 1.6rem; font-weight: 700; color: var(--white); margin-bottom: 0.5rem; }
  .org-desc { font-size: 0.78rem; color: var(--muted); margin-bottom: 1.25rem; line-height: 1.6; }
  .org-stat { font-size: 0.72rem; color: var(--muted); }
  .org-stat strong { color: var(--accent-text); }

  /* channel card grid */
  .channels-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 1.25rem; margin-top: 2.5rem; }
  .channel-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 1.5rem 1.25rem;
    text-decoration: none; color: inherit; display: flex;
    flex-direction: column; align-items: center; text-align: center;
    gap: 0.9rem; transition: border-color 0.2s, transform 0.2s;
    position: relative; overflow: hidden;
  }
  .channel-card::after {
    content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 2px;
    background: var(--org-color); transform: scaleX(0);
    transform-origin: left; transition: transform 0.35s;
  }
  .channel-card:hover { border-color: var(--org-color); transform: translateY(-3px); }
  .channel-card:hover::after { transform: scaleX(1); }
  .channel-avatar { width: 72px; height: 72px; border-radius: 50%; object-fit: cover; border: 2px solid var(--border); transition: border-color 0.2s; background: var(--surface2); }
  .channel-card:hover .channel-avatar { border-color: var(--org-color); }
  .channel-avatar-placeholder {
    width: 72px; height: 72px; border-radius: 50%;
    background: var(--surface2); border: 2px solid var(--border);
    display: flex; align-items: center; justify-content: center;
    font-family: 'Fraunces', serif; font-size: 1.4rem; font-weight: 700;
    color: var(--accent-text); flex-shrink: 0; transition: border-color 0.2s;
  }
  .channel-card:hover .channel-avatar-placeholder { border-color: var(--org-color); }
  .channel-badge {
    font-size: 0.58rem; letter-spacing: 0.15em; text-transform: uppercase;
    padding: 0.18rem 0.45rem; border-radius: 2px;
    border: 1px solid var(--org-color); color: var(--accent-text);
    background: rgba(0,0,0,0.3); flex-shrink: 0;
  }
  .channel-card-name { font-family: 'Fraunces', serif; font-size: 0.95rem; font-weight: 700; color: var(--white); line-height: 1.25; }
  .channel-card-meta { font-size: 0.65rem; color: var(--muted); }
  .channel-card-stats { display: flex; flex-direction: column; gap: 0.3rem; width: 100%; border-top: 1px solid var(--border); padding-top: 0.75rem; margin-top: 0.1rem; }
  .stat-row { display: flex; justify-content: space-between; align-items: center; font-size: 0.63rem; }
  .stat-row .stat-label { color: var(--muted); text-transform: uppercase; letter-spacing: 0.1em; }
  .stat-row .stat-value { color: var(--text); }
  .stat-row .stat-value.highlight { color: var(--accent-text); }

  /* stream cards */
  .streams-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(290px, 1fr)); gap: 1rem; margin-top: 2.5rem; }
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
  .stream-status { display: inline-block; font-size: 0.6rem; letter-spacing: 0.15em; text-transform: uppercase; padding: 0.2rem 0.5rem; border-radius: 2px; margin-bottom: 0.75rem; }
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
  .stat-value { font-size: 0.9rem; color: var(--accent-text); font-weight: 500; }
  .stream-date { font-size: 0.62rem; color: var(--muted); margin-top: 1rem; }
  .empty { color: var(--muted); font-size: 0.8rem; font-style: italic; padding: 1rem 0; }

  /* month heading */
  .month-heading {
    font-size: 0.65rem; letter-spacing: 0.25em; text-transform: uppercase;
    color: var(--muted); margin: 2.5rem 0 1rem;
    padding-bottom: 0.5rem; border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 0.75rem;
  }
  .month-heading::before {
    content: ''; display: block; width: 6px; height: 6px;
    border-radius: 50%; background: var(--org-color);
    box-shadow: 0 0 6px var(--org-color); flex-shrink: 0;
  }

  /* stream detail */
  .stream-hero { display: grid; grid-template-columns: 37fr 63fr; gap: 1.5rem; margin: 2rem 0 2.5rem; align-items: start; }
  @media (max-width: 700px) { .stream-hero { grid-template-columns: 1fr; } }
  .embed-side { min-width: 0; border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
  .embed-wrap { position: relative; width: 100%; padding-bottom: 56.25%; background: #000; }
  .embed-wrap iframe { position: absolute; inset: 0; width: 100%; height: 100%; border: none; }
  .stream-thumb-meta {
    display: flex; align-items: center; justify-content: space-between;
    padding: 0.65rem 0.9rem; background: var(--surface); border-top: 1px solid var(--border);
    font-size: 0.65rem; color: var(--muted);
  }
  .stream-thumb-meta a { color: var(--accent-text); text-decoration: none; }
  .stream-thumb-meta a:hover { text-decoration: underline; }
  .kpi-side { min-width: 0; }
  .kpi-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 1px; background: var(--border); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
  .kpi { background: var(--surface); padding: 1.1rem 1.25rem; position: relative; }
  .kpi.kpi-wide { grid-column: span 2; }
  .kpi-label { font-size: 0.58rem; text-transform: uppercase; letter-spacing: 0.18em; color: var(--muted); margin-bottom: 0.45rem; }
  .kpi-value { font-family: 'Fraunces', serif; font-size: 1.6rem; font-weight: 700; color: var(--accent-text); line-height: 1.1; }
  .kpi-value.kpi-sm { font-size: 1rem; }
  .kpi-sub { font-size: 0.6rem; color: var(--muted); margin-top: 0.25rem; }
  .kpi-grid .kpi:nth-child(-n+2)::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px; background: var(--org-color); }
  .chart-box { background: var(--surface); border: 1px solid var(--border); border-radius: 4px; padding: 1.5rem; margin-bottom: 2.5rem; }
  .chart-title { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.15em; color: var(--muted); margin-bottom: 1.25rem; }
  .chart-wrap { position: relative; height: 280px; }
  .section-title { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.2em; color: var(--muted); margin-bottom: 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid var(--border); }
  .data-table { width: 100%; border-collapse: collapse; font-size: 0.72rem; margin-bottom: 3rem; }
  .data-table th { text-align: left; padding: 0.5rem 0.75rem; color: var(--muted); font-weight: 500; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; border-bottom: 1px solid var(--border); }
  .data-table td { padding: 0.5rem 0.75rem; border-bottom: 1px solid rgba(30,30,46,0.5); color: var(--text); }
  .data-table tr:hover td { background: var(--surface); }
  .data-table .num { text-align: right; color: var(--accent-text); font-weight: 500; }
  .data-table .ts  { color: var(--muted); }
  .pill { display: inline-block; font-size: 0.6rem; padding: 0.15rem 0.4rem; border-radius: 2px; text-transform: uppercase; letter-spacing: 0.1em; }
  .pill-live     { background: rgba(255,79,109,0.15); color: var(--red);  border: 1px solid var(--red); }
  .pill-upcoming { background: rgba(79,195,247,0.10); color: var(--blue); border: 1px solid var(--blue); }
  .generated { text-align: center; color: var(--muted); font-size: 0.7rem; margin-top: 3rem; }

  footer { margin-top: 5rem; padding-top: 2rem; border-top: 1px solid var(--border); display: flex; flex-wrap: wrap; justify-content: space-between; gap: 1rem; font-size: 0.7rem; color: var(--muted); }
  footer a { color: var(--muted); text-decoration: none; transition: color 0.2s; }
  footer a:hover { color: var(--accent-text); }

  @keyframes fadeUp { from { opacity: 0; transform: translateY(14px); } to { opacity: 1; transform: translateY(0); } }
  header { animation: fadeUp 0.5s ease both; }
  .orgs-grid, .channels-list, .streams-grid, .kpi-row { animation: fadeUp 0.5s 0.1s ease both; }
"""


_THEME_JS = """
<script>
(function() {
  var STORAGE_KEY = 'idvt-theme';
  var root  = document.documentElement;
  var btn   = null;
  var thumb = null;

  function getSystemTheme() {
    return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  }
  function getEffectiveTheme() {
    return localStorage.getItem(STORAGE_KEY) || getSystemTheme();
  }
  function applyTheme(theme) {
    root.setAttribute('data-theme', theme);
    if (thumb) {
      theme === 'light' ? thumb.classList.add('is-light') : thumb.classList.remove('is-light');
    }
  }
  function toggleTheme() {
    var next = getEffectiveTheme() === 'dark' ? 'light' : 'dark';
    localStorage.setItem(STORAGE_KEY, next);
    applyTheme(next);
  }
  applyTheme(getEffectiveTheme());
  document.addEventListener('DOMContentLoaded', function() {
    btn   = document.getElementById('theme-toggle');
    thumb = document.getElementById('toggle-thumb');
    applyTheme(getEffectiveTheme());
    if (btn) btn.addEventListener('click', toggleTheme);
  });
  window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', function() {
    if (!localStorage.getItem(STORAGE_KEY)) applyTheme(getSystemTheme());
  });
})();
</script>"""

_TOGGLE_HTML = (
    '<div class="theme-toggle">'
    '<button class="toggle-pill" id="theme-toggle" aria-label="Toggle theme" title="Toggle light/dark theme">'
    '<span class="toggle-icon-light">☀</span>'
    '<span class="toggle-thumb" id="toggle-thumb">&#10022;</span>'
    '<span class="toggle-icon-dark">☽</span>'
    '</button>'
    '</div>'
)


def _html_head(title: str, depth: int, org_color: str = "#e8ff47",
               extra_scripts: str = "") -> str:
    return (
        f'<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        f'<meta charset="UTF-8">\n'
        f'<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'<meta name="color-scheme" content="dark light">\n'
        f'<script>!function(){{var t=localStorage.getItem("idvt-theme")||'
        f'(window.matchMedia("(prefers-color-scheme: light)").matches?"light":"dark");'
        f'document.documentElement.setAttribute("data-theme",t)}}();</script>\n'
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
        f'</div>\n'
        f'{_THEME_JS}\n'
        f'</body>\n</html>'
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
    return '<nav class="breadcrumb">' + " ".join(parts) + _TOGGLE_HTML + "</nav>\n"


# ══════════════════════════════════════════════════════════════════════════════
# PAGE WRITERS  (unchanged from original — all logic preserved)
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
        f'    <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:1rem;">\n'
        f'      <div>\n'
        f'        <p class="eyebrow">IDVTuber Tracker &#8212; Live Analytics</p>\n'
        f'        <h1>Stream <em>Overview</em></h1>\n'
        f'        <p class="page-meta">'
        f'Generated: {generated_at} &nbsp;&#183;&nbsp; '
        f'{total_streams} streams &nbsp;&#183;&nbsp; '
        f'{total_channels} channels &nbsp;&#183;&nbsp; '
        f'9 organisations</p>\n'
        f'      </div>\n'
        f'      <div class="theme-toggle" style="padding-top:0.5rem;">{_TOGGLE_HTML}</div>\n'
        f'    </div>\n'
        f'  </header>\n'
        f'  <div class="orgs-grid">{org_cards}\n  </div>\n'
    )

    html = _html_head("Stream Analytics", 0) + body + _html_foot(0)
    (OUTPUT_DIR / "index.html").write_text(html, encoding="utf-8")
    log.info("Written: index.html")


def write_org_page(org_slug: str, org: dict, stream_counts: dict,
                   logos: dict[str, str] | None = None,
                   channel_ids_map: dict[str, str] | None = None,
                   subscribers: dict[str, int] | None = None) -> None:
    org_dir = OUTPUT_DIR / org_slug
    org_dir.mkdir(exist_ok=True)
    logos           = logos or {}
    channel_ids_map = channel_ids_map or {}
    subscribers     = subscribers or {}

    cards = ""
    for entry in org["channels"]:
        ch_name   = entry[0]
        ch_type   = entry[1]
        ch_slug   = slugify(ch_name)
        badge     = "ORG CH" if ch_type == "org" else "TALENT"
        n_str     = stream_counts.get(ch_name, 0)
        ch_id     = channel_ids_map.get(ch_name, "")
        logo_url  = logos.get(ch_id, "")
        sub_count = subscribers.get(ch_id)

        if logo_url:
            avatar_html = f'<img class="channel-avatar" src="{logo_url}" alt="{esc(ch_name)}" referrerpolicy="no-referrer" loading="lazy">'
        else:
            initial = ch_name[0].upper()
            avatar_html = f'<div class="channel-avatar-placeholder">{initial}</div>'

        stats_html = (
            f'<div class="channel-card-stats">'
            f'<div class="stat-row">'
            f'<span class="stat-label">Subscribers</span>'
            f'<span class="stat-value highlight">{fmt_subs(sub_count)}</span>'
            f'</div>'
            f'<div class="stat-row">'
            f'<span class="stat-label">Streams</span>'
            f'<span class="stat-value">{n_str} stream{"s" if n_str != 1 else ""}</span>'
            f'</div>'
            f'</div>'
        )

        cards += (
            f'\n    <a class="channel-card" href="{ch_slug}/index.html">\n'
            f'      {avatar_html}\n'
            f'      <span class="channel-badge">{badge}</span>\n'
            f'      <div class="channel-card-name">{esc(ch_name)}</div>\n'
            f'      {stats_html}\n'
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
        f'  <div class="channels-grid">{cards}\n  </div>\n'
    )

    html = _html_head(org["label"], 1, org["color"]) + body + _html_foot(1)
    (org_dir / "index.html").write_text(html, encoding="utf-8")
    log.info("Written: %s/index.html", org_slug)


def write_channel_page(org_slug: str, org: dict,
                       ch_name: str, streams: list[dict]) -> None:
    ch_slug = slugify(ch_name)
    ch_dir  = OUTPUT_DIR / org_slug / ch_slug
    ch_dir.mkdir(parents=True, exist_ok=True)

    months: OrderedDict = OrderedDict()
    for stream in streams:
        first_seen = stream["first_seen"]
        if first_seen is None:
            month_key = "Unknown"
        else:
            if isinstance(first_seen, str):
                try:
                    first_seen = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
                except ValueError:
                    first_seen = None
            if first_seen:
                local_dt  = first_seen.astimezone(_LOCAL_TZ) if first_seen.tzinfo else first_seen
                month_key = local_dt.strftime("%B %Y")
            else:
                month_key = "Unknown"
        months.setdefault(month_key, []).append(stream)

    cards = ""
    for month_label, month_streams in months.items():
        cards += f'\n  <div class="month-heading">{month_label}</div>\n  <div class="streams-grid">'
        for stream in month_streams:
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
                f'        <div><div class="stat-label">View Count</div>'
                f'<div class="stat-value">{fmt(stream.get("view_count"))}</div></div>\n'
                f'        <div><div class="stat-label">Peak Likes</div>'
                f'<div class="stat-value">{fmt(stream["peak_likes"])}</div></div>\n'
                f'      </div>\n'
                f'      <div class="stream-date">{fmt_dt(stream["first_seen"])}</div>\n'
                f'    </a>'
            )
        cards += '\n  </div>'

    if not months:
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
        + cards + '\n'
    )

    html = _html_head(ch_name, 2, org["color"]) + body + _html_foot(2)
    (ch_dir / "index.html").write_text(html, encoding="utf-8")
    log.info("  Written: %s/%s/index.html", org_slug, ch_slug)


def write_stream_page(org_slug: str, org: dict, ch_name: str,
                      stream: dict, timeseries: list[dict]) -> None:
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

    title_text  = stream["video_title"] or vid
    short_title = (title_text[:40] + "…") if len(title_text) > 40 else title_text
    org_color   = org["color"]

    bc = _breadcrumb([
        ("Home",       "../../index.html"),
        (org["label"], "../index.html"),
        (ch_name,      "index.html"),
        (short_title,  ""),
    ])

    chart_script = (
        '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js">'
        '</script>'
    )

    def _fmt_duration(first, last) -> str:
        if not first or not last:
            return "—"
        try:
            delta = last - first
            total = int(delta.total_seconds())
            h, rem = divmod(total, 3600)
            m, s   = divmod(rem, 60)
            return f"{h}h {m:02d}m {s:02d}s" if h else f"{m}m {s:02d}s"
        except Exception:
            return "—"

    duration_str = _fmt_duration(stream["first_seen"], stream["last_seen"])

    body = (
        bc
        + f'  <header>\n'
        f'    <p class="eyebrow">{esc(org["label"])} &nbsp;&#183;&nbsp; {esc(ch_name)}</p>\n'
        f'    <span class="stream-status {s_cls}" style="display:inline-block;margin-bottom:0.75rem;">{s_lbl}</span>\n'
        f'    <h1>{esc(title_text)}</h1>\n'
        f'    <p class="page-meta">Video ID: {esc(vid)}</p>\n'
        f'  </header>\n\n'
        f'  <div class="stream-hero">\n'
        f'    <div class="embed-side">\n'
        f'      <div class="embed-wrap">\n'
        f'        <iframe src="https://www.youtube.com/embed/{vid}" allowfullscreen loading="lazy"></iframe>\n'
        f'      </div>\n'
        f'      <div class="stream-thumb-meta">\n'
        f'        <span>{fmt_dt(stream["first_seen"])}</span>\n'
        f'        <a href="https://www.youtube.com/watch?v={vid}" target="_blank" rel="noopener">Watch on YouTube ↗</a>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'    <div class="kpi-side">\n'
        f'      <div class="kpi-grid">\n'
        f'        <div class="kpi"><div class="kpi-label">Peak Viewers</div><div class="kpi-value">{fmt(stream["peak_viewers"])}</div><div class="kpi-sub">concurrent</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Avg Viewers</div><div class="kpi-value">{fmt(stream.get("avg_viewers"))}</div><div class="kpi-sub">concurrent</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Peak Likes</div><div class="kpi-value">{fmt(stream["peak_likes"])}</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Peak Comments</div><div class="kpi-value">{fmt(stream["peak_comments"])}</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Stream Start</div><div class="kpi-value kpi-sm">{fmt_dt(stream["first_seen"])}</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Stream End</div><div class="kpi-value kpi-sm">{fmt_dt(stream["last_seen"])}</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Duration</div><div class="kpi-value kpi-sm">{duration_str}</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">View Count</div><div class="kpi-value kpi-sm">{fmt(stream.get("view_count"))}</div><div class="kpi-sub">total plays</div></div>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'  </div>\n\n'
        f'  <div class="chart-box">\n'
        f'    <div class="chart-title">Concurrent Viewers over Time</div>\n'
        f'    <div class="chart-wrap"><canvas id="viewerChart"></canvas></div>\n'
        f'  </div>\n\n'
        f'  <div class="chart-box">\n'
        f'    <div class="chart-title">Likes &amp; Comments over Time</div>\n'
        f'    <div class="chart-wrap"><canvas id="engagementChart"></canvas></div>\n'
        f'  </div>\n\n'
        f'  <p class="generated">Generated {_now_local().strftime("%Y-%m-%d %H:%M WIB")}'
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

    html = _html_head(title_text, 2, org_color, chart_script) + body + _html_foot(2)
    (ch_dir / f"{v_slug}.html").write_text(html, encoding="utf-8")
    log.info("    Written: %s/%s/%s.html", org_slug, ch_slug, v_slug)


# ══════════════════════════════════════════════════════════════════════════════
# PARTIAL BUILD ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _enrich_stream(stream: dict, conn, table: str, hist) -> tuple[dict, list, list]:
    """
    Fetch timeseries and compute avg_viewers for a stream.
    Returns (enriched_stream, timeseries).
    all_rows is no longer fetched — the raw data table was removed from the stream page.
    """
    is_archived = stream.get("_source") == "history"

    if is_archived:
        ts = get_archived_timeseries(hist, stream["video_id"])
        if stream.get("avg_viewers") is None:
            viewer_vals = [int(r["concurrent_viewers"]) for r in ts if r["concurrent_viewers"]]
            stream["avg_viewers"] = round(sum(viewer_vals) / len(viewer_vals)) if viewer_vals else None
    else:
        ts = get_stream_timeseries(conn, table, stream["video_id"])
        viewer_vals = [int(r["concurrent_viewers"]) for r in ts if r["concurrent_viewers"]]
        stream = dict(stream)
        stream["avg_viewers"] = round(sum(viewer_vals) / len(viewer_vals)) if viewer_vals else None

    return stream, ts


def build_dashboard() -> None:
    if not AIVEN_DATABASE_URL:
        print("ERROR: AIVEN_DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    conn = get_conn()
    hist = get_history_conn()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── static legal pages ────────────────────────────────────────────────────
    for legal_file in ["privacy.html", "terms.html"]:
        src = Path(legal_file)
        dst = OUTPUT_DIR / legal_file
        if src.exists():
            shutil.copy2(src, dst)
            log.info("Copied %s to %s/", legal_file, OUTPUT_DIR)
        else:
            log.warning("Legal file not found: %s — skipping", legal_file)

    # ── channel ID / logo maps ────────────────────────────────────────────────
    db_channels = get_channel_rows(conn)
    db_by_name  = {ch["channel_name"]: ch for ch in db_channels}
    db_by_id    = {ch["channel_id"]:   ch for ch in db_channels}

    channel_ids_map: dict[str, str] = {}
    for org in ORG_MAP.values():
        for entry in org["channels"]:
            if len(entry) > 2 and entry[2]:
                channel_ids_map[entry[0]] = entry[2]
    for ch in db_channels:
        channel_ids_map[ch["channel_name"]] = ch["channel_id"]

    all_channel_ids = list(dict.fromkeys(channel_ids_map.values()))

    log.info("Fetching channel data from YouTube API…")
    logos, subscribers = get_channel_data(all_channel_ids)
    log.info("Fetched %d logo(s) and %d subscriber count(s).", len(logos), len(subscribers))

    # ── bulk-load schema cache (single query for all 62 tables) ──────────────
    all_table_names = [ch["table_name"] for ch in db_channels]
    _load_schema_cache(conn, all_table_names)

    # ── load manifest ─────────────────────────────────────────────────────────
    manifest = load_manifest()
    log.info("Manifest loaded — %d stream pages previously generated.", len(manifest))

    # ── collect ALL streams from DB + history per channel ────────────────────
    # Structure: {ch_name: [stream_dict, ...]}
    all_streams_by_channel: dict[str, list[dict]] = {}
    stream_counts: dict[str, int] = {}
    total_streams  = 0
    total_channels = 0

    for org_slug, org in ORG_MAP.items():
        (OUTPUT_DIR / org_slug).mkdir(exist_ok=True)
        for entry in org["channels"]:
            ch_name = entry[0]
            # Primary lookup: by channel_name (exact match).
            # Fallback: by channel_id from ORG_MAP entry[2] — handles the common
            # case where the tracker stored a slightly different channel_name than
            # what is written in ORG_MAP (e.g. missing 【bracket】 suffixes).
            db_row = db_by_name.get(ch_name)
            if not db_row:
                org_map_id = entry[2] if len(entry) > 2 else ""
                if org_map_id:
                    db_row = db_by_id.get(org_map_id)
                if db_row:
                    log.info(
                        "ORG_MAP name '%s' matched DB by channel_id (%s) — "
                        "DB stores it as '%s'. Pages will be generated correctly. "
                        "Consider aligning the ORG_MAP name to avoid this fallback.",
                        ch_name, org_map_id, db_row["channel_name"],
                    )
                else:
                    log.warning(
                        "ORG_MAP channel '%s' (org: %s) not found in DB by name "
                        "or channel_id — no pages will be generated for it. "
                        "The tracker may not have seen this channel stream yet.",
                        ch_name, org_slug,
                    )
                    stream_counts[ch_name] = 0
                    all_streams_by_channel[ch_name] = []
                    continue

            table       = db_row["table_name"]
            raw_streams = get_streams_for_channel(conn, table)
            live_ids    = {s["video_id"] for s in raw_streams}
            archived    = get_archived_streams_for_channel(hist, ch_name, live_ids) if hist else []
            all_raw     = list(raw_streams) + archived

            all_streams_by_channel[ch_name] = all_raw
            stream_counts[ch_name]          = len(all_raw)
            total_channels += 1
            total_streams  += len(all_raw)

    log.info("DB query complete — %d streams across %d channels.", total_streams, total_channels)

    # ── diff: determine which stream pages need (re)generating ────────────────
    # A stream is dirty if:
    #   (a) it has no entry in the manifest yet  →  new stream
    #   (b) it was recorded as 'live' last run   →  may still be updating
    dirty_video_ids: set[str] = set()
    dirty_channels:  set[str] = set()  # channel names whose channel page needs rebuild
    dirty_orgs:      set[str] = set()  # org slugs whose org page needs rebuild

    for ch_name, streams in all_streams_by_channel.items():
        for stream in streams:
            vid    = stream["video_id"]
            status = stream.get("stream_status") or "vod"
            in_manifest = vid in manifest
            was_live    = manifest.get(vid, {}).get("status") == "live"

            if not in_manifest or was_live:
                dirty_video_ids.add(vid)
                dirty_channels.add(ch_name)
                org_result = _CH_TO_ORG.get(ch_name)
                if org_result:
                    dirty_orgs.add(org_result[0])

    log.info(
        "Partial build plan: %d stream page(s) to generate, "
        "%d channel page(s) to regenerate, %d org page(s) to regenerate.",
        len(dirty_video_ids), len(dirty_channels), len(dirty_orgs)
    )

    # ── generate dirty stream pages ───────────────────────────────────────────
    for org_slug, org in ORG_MAP.items():
        for entry in org["channels"]:
            ch_name = entry[0]
            db_row  = db_by_name.get(ch_name)
            if not db_row:
                continue

            table   = db_row["table_name"]
            streams = all_streams_by_channel.get(ch_name, [])

            for stream in streams:
                vid = stream["video_id"]
                if vid not in dirty_video_ids:
                    continue

                stream, ts = _enrich_stream(stream, conn, table, hist)
                write_stream_page(org_slug, org, ch_name, stream, ts)

                # update manifest entry
                manifest[vid] = {
                    "org_slug":     org_slug,
                    "ch_slug":      slugify(ch_name),
                    "ch_name":      ch_name,
                    "status":       stream.get("stream_status") or "vod",
                    "generated_at": _now_local().strftime("%Y-%m-%d %H:%M WIB"),
                }

    # ── regenerate channel pages ─────────────────────────────────────────────
    # Write ALL channel pages that exist in the DB — not just dirty ones.
    # This ensures pages are created on the first run for newly-added orgs,
    # even when all their streams are already VOD (and therefore not dirty).
    # Channel pages are cheap to write (no timeseries, just summary cards).
    channels_written = 0
    for org_slug, org in ORG_MAP.items():
        for entry in org["channels"]:
            ch_name = entry[0]
            if not db_by_name.get(ch_name):
                continue  # not in DB yet — skip (warning already logged above)

            streams = all_streams_by_channel.get(ch_name, [])
            enriched = []
            for stream in streams:
                s = dict(stream)
                if s.get("avg_viewers") is None:
                    s["avg_viewers"] = None
                enriched.append(s)

            write_channel_page(org_slug, org, ch_name, enriched)
            channels_written += 1

    log.info("Channel pages written: %d", channels_written)

    # ── regenerate org pages ─────────────────────────────────────────────────
    # Always write ALL org pages so that newly-added orgs with no streams yet
    # still get their index.html (otherwise the org card on the home page 404s).
    # Org pages are cheap — no timeseries, just channel cards.
    for org_slug, org in ORG_MAP.items():
        write_org_page(org_slug, org, stream_counts,
                       logos=logos,
                       channel_ids_map=channel_ids_map,
                       subscribers=subscribers)
    log.info("Org pages written: %d", len(ORG_MAP))

    # ── always regenerate index ───────────────────────────────────────────────
    generated_at = _now_local().strftime("%Y-%m-%d %H:%M WIB")
    write_index(total_streams, total_channels, generated_at)

    # ── persist manifest ──────────────────────────────────────────────────────
    save_manifest(manifest)

    conn.close()
    if hist:
        hist.close()

    pages_written = len(dirty_video_ids) + channels_written + len(ORG_MAP) + 1
    log.info(
        "Dashboard complete — %d page(s) written "
        "(%d stream, %d channel, %d org, 1 index) out of %d total streams.",
        pages_written,
        len(dirty_video_ids), channels_written, len(ORG_MAP),
        total_streams
    )


if __name__ == "__main__":
    build_dashboard()
