"""
generate_dashboard.py
Pulls livestream analytics from PostgreSQL and generates a 4-level
static HTML dashboard:
  index.html                       <- org cards
  {org}/index.html                 <- channel list per org
  {org}/{channel}/index.html       <- stream cards per channel
  {org}/{channel}/{video}.html     <- stream detail + charts

Partial build algorithm:
  - A manifest (dashboard/manifest.json) tracks every stream page.
  - On each run, only stream pages that are NEW or currently LIVE are
    (re)generated. Their parent channel and org pages are then also
    regenerated to reflect updated stream counts / card lists.
  - The index page is always regenerated (trivially cheap).
  - Unchanged stream pages (VOD, already in manifest) are never touched.

Org membership is driven by the ORG_MAP dict below.
"""

import os
import re
import json
import shutil
import sqlite3
import logging
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from functools import lru_cache
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
            ("Crystallyn Caparina",   "talent", "UCyVT2GRTAWyUjdjTsHhIy6w"),
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
            ("MagniV",             "org",    "UCJifvCPf04WdIqdZY50Tsag"),
            ("Gema Gathika【MagniV】", "talent", "UC9Mfuai-qdXnTTFN0Z3hkAA"),  
            ("Istmodius【MagniV】", "talent", "UCpe6USwJgyctDpWQhzVBeVQ"),  
            ("Funin Mamori【MagniV】", "talent", "UCO6ngsu6Bx1SnJgL1iLyafA"), 
            ("Shiru 【MagniV】", "talent", "UCYE181HONC3O7Iul62aAoYg"), 
            ("CANCNCN -Ezacancan-【MagniV】", "talent", "UC31csAlk6YaJffLT3qPvEZg"), 
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
    "uver-id": {
        "label":   "UVER ID",
        "color":   "#4a00e0",
        "desc":    "An large group with eclectic style that shows the diversity of its talents.",
        "channels": [
            ("UVER ID", "org", "UCjgqLbt6MXPeTcnYbRBdChw"),  
            ("Shana Ophelia [ UVER ID ]", "talent", "UCqEDMsqjTiO239UTJ2z2ftQ"),  
            ("Scardia Agnibrata ♦ [ UVER ID ]", "talent", "UCq3hfX7OAnEfb3nkw22E7mw"),  
            ("Meltdhe Corsieur Ch. [ UVER ID ]", "talent", "UCYvEICuAdq_dHsvgEJQglKA"),  
            ("Cyure Michantrell Ch. [ UVER ID ]", "talent", "UCd9llXPNO3BF1QypPqWanSA"),  
            ("Zelinus Vihnlock", "talent", "UCqlHd6k2zDNbjqVS9it8VJA"),  
            ("Nephthya Anya Ch. [ UVER ID ]", "talent", "UC2EuxVDjpoEd-wdEmz5nfOA"),  
            ("FUJIO  [ UVER ID ]", "talent", "UCK4HWNrcNiCFNdefybmEnVw"),  
            ("Hiro Kyasuta [ UVER ID ]", "talent", "UC3GcxfDhuFQ8XfKCp34qpMg"),  
            ("Arjuna Candra Pawitra Ch. [ UVER ID ]", "talent", "UCnMgBvpptZbgfvt4RVZ9Jcg"),  
            ("Wei Yang [ UVER ID ]", "talent", "UCGC_uTT99Co4G0aiYWXDxOA"),  
            ("Jae Min Ho [ UVER ID ]", "talent", "UCRjD_xEJ3mKIv6399Jt8GIw"),  
            ("Doh Chen ah [ UVER ID ]", "talent", "UCNYi3CluU99__mhU014xpYg"),  
            ("Gi Hwa Young [ UVER ID ]", "talent", "UCzOX836IOW8jpzIM6LYyjfA"),
            ("Aira Lunagrandia [ UVER ID ]", "talent", "UCFJ7A3461hnv_mcgsEojAGw"),  
            ("Norn Haira〔UVER ID〕", "talent", "UCjev0yMRo4XoJRNN9boGlfw"),  
            ("Isabella Naemi. Ch. ★ [ UVER ID ]", "talent", "UCcMzPa8mDNFvfwZ2qmuk7Zw"),  
        ],
    },
    "crims-on": {
        "label":   "CRIMS:on",
        "color":   "#d90429",
        "desc":    "Circle of friends with different background who shares one passion towards content creators scene.",
        "channels": [
            ("Cole Calamello【CRIMS:on】", "talent", "UCqyX914s1cW_NwhbREvKTxA"),  
            ("Rijii【CRIMS:on】", "talent", "UCZoxWxYT0X_4gqUDo4kR6BA"),  
            ("Iana【CRIMS:on】", "talent", "UCbwIX7MLfRsMdbP7yzyRoEg"),  
            ("Makoto Takuma 【CRIMS:on】", "talent", "UChpYVFWNuWbOTtXfxCL62Kw"),  
            ("Selia Aisnith 【CRIMS:on】", "talent", "UCZphYueZZE2NVcriExbnkBw"),  
        ],
    },
    "gravt": {
        "label":   "GRAVT",
        "color":   "#212529",
        "desc":    "Boyband groups formed from four members with music expertise.",
        "channels": [
            ("GRAVT", "org", "UCKnLF98-xHPQMwtIlXnAmkQ"),  
            ("Akemi Ch. 猫町アケミ【GRAVT】【AKA Virtual】", "talent", "UC61iJVuFVS4YsnPkZe5EmXg"),  
            ("Ave Kanehoshii【GRAVT】【AKA Virtual】", "talent", "UCdrbNcRAy424_FFWsY1A6og"),  
            ("daem【GRAVT】", "talent", "UCiJVUvvDMYHof7P5lt9NU3g"),   
        ],
    },
    "eterluna": {
        "label":   "EterLuna",
        "color":   "#d0d1ff",
        "desc":    "Small VTuber agency with the focus of management and talent development.",
        "channels": [
            ("EterLuna", "org", "UCbfT3rJmyTjGk5O6ZqiksjA"),  
            ("Sanna Salma Ch.", "talent", "UC45RCWpsk3g7u1wLkvt0f-A"),  
            ("Watanabe Selena【EterLuna】", "talent", "UCM53Oe2gLMAAmW2bt_a6DtA"),  
        ],
    },
    "magisona": {
        "label":   "MagiSona",
        "color":   "#8338ec",
        "desc":    "An agency which highlights the magical and fun experience of VTubing.",
        "channels": [
            ("MagiSona", "org", "UCytdUc5bQvLLceAK9KbCKKg"),  
            ("Reika Ayasa 【MagiSona】", "talent", "UCuzc9EAaSymkjmc4oZ9idzw"),  
            ("Lyra Azalea【MagiSona】", "talent", "UCWAdYE-5usbzNYhi_vLDSfA"),  
            ("ioFiel Feliz Ch.【MagiSona】", "talent", "UCs4ZMNzilmO3yncwQaRYZiw"),  
        ],
    },
    "lav-idn": {
        "label":   "LAV IDN",
        "color":   "#9f86c0",
        "desc":    "Developing agency with talents and affiliates system.",
        "channels": [
            ("Limitless Actress Virtual", "org", "UCggn9-ggn4aJplrvlgiSVeQ"),  
            ("Tachibana Mirai Ch. 【LAV】", "talent", "UC8YJreYPp3rnoLYOSGvwAAw"),  
            ("Akane Nanase Ch. 【LAV】", "talent", "UCV1iD5TIQZnK33Y7Q4y84IA"),  
            ("Kanata Reina Ch.【LAV】", "talent", "UC7mf0CkU3-8ulUiVVleVnhg"),  
        ],
    },
    "arvi-media": {
        "label":   "ARVI Media",
        "color":   "#e31837",
        "desc":    "Part of Area Media, a digital idol bridging entertainment and community through live streams, creative content, and authentic fan interactions.",
        "channels": [
            ("Aika Sakuraba Ch. 【ARVI】", "talent", "UCW6ZmofKJm_Rnwyq1brysmg"),  
            ("Makotoshi Ch.", "talent", "UCgcsnZ6-ys_Fr9wEX6sz5xg"),  
            ("Navarra Clementia", "talent", "UCAa31OBocBS80q2x4meCSUw"),
            ("Ringo Soda", "talent", "UCHTj4RRPQjba2GekeJKvB_w"),
            ("Suou Ono Ch.【ARVI】", "talent", "UCZoZhv9PkSryZSN4-WVi0nA"),    
        ],
    },
    "hrcome": {
        "label":   "HRCOME",
        "color":   "#ff8c00",
        "desc":    "Upstart VTuber agency with focuses on digital entertainment, virtual talents development and management.",
        "channels": [
            ("HRCOME - VTuber Group", "org", "UCEiB89eUXCpv0q48G1yNhQA"),  
            ("TETRANOVA Official", "org", "UCgptYrQ8oOVt9JWQIbI7CoA"),  
            ("Netazura Rasy【HRCOME】", "talent", "UCnOu9hYTSozQoR3aR3MW_eA"),  
            ("RawnVinyl【HRCOME】", "talent", "UC32cBlC2xcp2_RwPDCsSRPA"),  
            ("Yuno Ni Royco【HRCOME】", "talent", "UCxtomvr4EC_kXYOj2MABrkw"),  
            ("RoroKaili Yumi【HRCOME】", "talent", "UC1zP61yu4aOl5MR6bgjxMyA"),  
            ("Viera Aori【HRCOME】", "talent", "UCey9e7qTJl7ONRG-7po5QgA"),  
            ("Hana Aranya【HRCOME】", "talent", "UCZBsVlJM5tUyRg-JcG-KpMQ"),  
            ("Chikage Nox 【HRCOME】", "talent", "UCBrW_wylHbf_wdkx7VeTOsA"),  
        ],
    },
    "laeta": {
        "label":   "LAETA",
        "color":   "#00b4d8",
        "desc":    "Small Indonesian VTuber agency and creative group.",
        "channels": [
            ("LAETA Official", "org", "UCv8p8YZ5uyFho4TAXLm3WQg"),  
            ("Mila Dragon【LAETA】", "talent", "UCy-330LSZFXB_nPXgviKg8g"),  
            ("Airi Airintika【LAETA】", "talent", "UCnlFvQbfsH2RFUAihZxIRGg"),  
            ("Sevina Reina【LAETA】", "talent", "UCdj5dtnp5WEdwuASQvcQfZA"),  
            ("Shiraishi Oriel【LAETA】", "talent", "UCksATXnF-u3-XMjsEI3-_Gw"),  
            ("Althea Antonia 【LAETA】", "talent", "UCh1hZ_Zvv3anRNH8Nh-N2WA"),  
            ("Mizuky Okami 【LAETA】", "talent", "UCKsbx_QJzQewq8uA4q6OaWQ"),  
        ],
    },
    "kanau-virtual": {
        "label":   "KANAU Virtual",
        "color":   "#8a2be2",
        "desc":    "Part of KANAU media which focuses on  virtual talent dan creative project.",
        "channels": [
            ("KANAU Virtual", "org", "UCA4f6J2G6GsIrWHUmrAqSVg"),  
            ("Ryuzaki Ayato Ch. #WorstGenesis", "talent", "UC6POMkx3rS3K8gdTvxhKy6A"),  
            ("Luii Lafete【KANAU Virtual】", "talent", "UCtT8ko5dvjrQIT5f_kzLTeg"),  
            ("Nela Mellifera【KANAU Virtual】", "talent", "UCyTGiO76s55eu97NczLTXBg"),  
            ("fein 【KANAU Virtual】", "talent", "UCBu6WKvgy5OA3ffxfw4i0dA"),  
            ("Noazumi Ch.【KANAU Virtual】", "talent", "UCtxtfcx9hfmAUwUdcY4O0dQ"),  
            ("Zoelyn Belenncia 【KANAU Virtual】", "talent", "UCWgJUCk3-3OBgKUFTCYc5RA"),  
        ],
    },
    "asobu-production": {
        "label":   "Asobu Production",
        "color":   "#ffd700",
        "desc":    "A virtual agency where imagination and creativity has no limit.",
        "channels": [
            ("Asobu Production", "org", "UC5JyQVwWF6kRvRM3pFTKUcg"),  
            ("Keira Nakayura【ASOBU】", "talent", "UC5ve05mzKKh2s28DBoGMk1Q"),  
            ("Hakuro Nakayoshi 【ASOBU】", "talent", "UC8OpiVNM2MkfTEr_cNFkidA"),  
            ("Mo Xiao Mei【ASOBU】", "talent", "UCu8OmWUWMHD_uW2jkYwBWfQ"),  
            ("Namazu Reire【ASOBU】", "talent", "UCOXAPlV_XxMbfZelSbHc0IQ"),  
            ("Lyra Monphelia 【ASOBU】", "talent", "UC6cGmy7IqsY6z3Ng6tOFWFA"),  
            ("Kalani Rainash 【ASOBU】", "talent", "UCM41EWO7hDMfA3pTAWEXmXw"),  
        ],
    },
    "snowvalley-agency": {
        "label":   "SnowValley Agency",
        "color":   "#a0c4ff",
        "desc":    "Independent creative group who operates with the motto 'Together We Entertain the World with Laughter and Joy'.",
        "channels": [
            ("SnowValley Agency", "org", "UCJrlIe6-YsZEZ-WhWpkQ8xQ"),  
            ("Alicia Ravenna Ch. •SnowValley", "talent", "UC2LODZvn-QQupykmwxey4zA"),  
            ("Tuti Hanna Ch. •SnowValley", "talent", "UCigj-FBk5US8xWxgStdNw_A"),  
            ("Shayla Meyjiera Ch •SnowValley", "talent", "UCNyZthXc-DusXe_A89XOdUQ"),  
            ("Serein Venetia Ch. •SnowValley", "talent", "UCTQjwoiIQQ9giaeJS2WMTFw"),  
            ("Celina Olystia Ch •SnowValley", "talent", "UCPAOc_fgQeTjHMAni8rx-Cw"),  
            ("Shine Naira Ch. •SnowValley", "talent", "UC9ik6MJGi3esadK15ioS6FA"),  
            ("Naona Neon Ch. •SnowValley", "talent", "UCIW8RtcLw4WiG1ul2dTaryA"),  
        ],
    },
    "evren-community": {
        "label":   "EVREN Community",
        "color":   "#d7b576",
        "desc":    "Indonesian Vtuber community that operates based on passion.",
        "channels": [
            ("EVREN", "org", "UCC4a8ZZH9G85ZSGOSTFxmVA"),  
            ("Lyanna Chernovskaya", "talent", "UCQO6uKa81T5-e9HzUHjS9DA"),  
            ("Amami Yoruna", "talent", "UCYWs6Tvqs6k1EjZMK27r7fA"),  
        ],
    },
    "vilitera": {
        "label":   "VILITERA",
        "color":   "#c0392b",
        "desc":    "Indonesia's established, multi-generational VTuber agencies.",
        "channels": [
            ("VILITERA", "org", "UCH1YNlsGKODuxk1vJpMVACA"),  
            ("Mahiru Rui Ch. VILITERA-ID", "talent", "UCr82gewKlAE5faJwsJSnKfg"),  
            ("Erishia Hearn Ch. VILITERA-ID", "talent", "UCDoXlj0lBza5i2cUe5QJIBw"),  
            ("Letto Allenka Ch. VILITERA-ID", "talent", "UChJY-3LOgt37f28WrUaKgkA"),  
            ("Francine Pavlova Ch. VILITERA-ID", "talent", "UCxvm3GRDZylYX_cFOC78pww"),  
            ("Amon Gauri Ch. VILITERA-ID", "talent", "UCCH2-H6NfX58KUW_5eW_85Q"),  
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


def get_all_streams_bulk(conn, table_infos: list[tuple[str, str]]) -> dict[str, list[dict]]:
    """
    Fetch summary rows for every channel in one round-trip using UNION ALL.
    *table_infos* is [(channel_name, table_name), ...] for tables that exist.
    Returns {channel_name: [stream_dict, ...]} with streams ordered newest-first.

    Channel names are NOT embedded in the SQL — they are stored in an index
    list and looked up from an integer tag column to avoid encoding issues with
    Unicode characters (e.g. Japanese brackets 【】) that psycopg2's latin-1
    adapter cannot handle.
    """
    if not table_infos:
        return {}

    # Map integer index → channel_name so we never put Unicode into SQL text.
    idx_to_ch: list[str] = []
    parts = []
    for idx, (ch_name, table) in enumerate(table_infos):
        idx_to_ch.append(ch_name)
        view_count_expr = (
            "MAX(view_count) AS view_count"
            if _has_column(conn, table, "view_count")
            else "NULL::BIGINT AS view_count"
        )
        parts.append(f"""
            SELECT
                {idx} AS ch_idx,
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
        """)

    union_sql = " UNION ALL ".join(parts) + " ORDER BY ch_idx, first_seen DESC"

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(union_sql)
        rows = cur.fetchall()

    result: dict[str, list[dict]] = {ch: [] for ch, _ in table_infos}
    for row in rows:
        d = dict(row)
        ch = idx_to_ch[d.pop("ch_idx")]
        result[ch].append(d)
    return result


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


def get_all_archived_streams(hist, channel_names: list[str]) -> dict[str, list]:
    """
    Bulk-fetch archived streams for all requested channel names in a single
    SQLite query.  Returns {channel_name: [stream_dict, ...]} for every name
    in *channel_names* (missing channels get an empty list).
    """
    if not channel_names:
        return {}
    placeholders = ",".join("?" * len(channel_names))
    rows = hist.execute(f"""
        SELECT
            channel_name,
            video_id, video_title, stream_status,
            stream_start  AS first_seen,
            stream_end    AS last_seen,
            peak_viewers, avg_viewers, view_count,
            peak_likes, peak_comments, data_points
        FROM streams
        WHERE channel_name IN ({placeholders})
        ORDER BY channel_name, stream_start DESC
    """, channel_names).fetchall()

    result: dict[str, list] = {name: [] for name in channel_names}
    for r in rows:
        d = dict(r)
        ch = d.pop("channel_name")
        for key in ("first_seen", "last_seen"):
            val = d.get(key)
            if isinstance(val, str):
                try:
                    d[key] = datetime.fromisoformat(val)
                except ValueError:
                    pass
        d["_source"] = "history"
        result[ch].append(d)
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

@lru_cache(maxsize=None)
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


def fmt_dt(dt, time_only: bool = False) -> str:
    """Format a datetime for display in WIB (UTC+7).

    time_only=True returns only HH:MM — used for chart x-axis labels
    so ticks stay readable without the date repeating on every tick.
    """
    if dt is None:
        return "—"
    try:
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            local = dt.astimezone(_LOCAL_TZ)
        else:
            parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            local = parsed.astimezone(_LOCAL_TZ)
        return local.strftime("%H:%M") if time_only else local.strftime("%Y-%m-%d %H:%M WIB")
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

  /* ── headings — shared scale used on every level ── */
  .eyebrow {
    font-size: 0.65rem; letter-spacing: 0.3em; text-transform: uppercase;
    color: var(--accent-text); margin-bottom: 0.6rem;
    /* level-1 (index) uses the full accent colour;
       lower levels inherit --org-color via --accent-text automatically */
  }
  h1 {
    font-family: 'Fraunces', serif;
    font-size: clamp(2rem, 5vw, 4.5rem);
    font-weight: 700; line-height: 1.0; color: var(--white); margin-bottom: 0.5rem;
  }
  h1 em { font-style: italic; color: var(--accent-text); }
  /* page-meta sits directly under every h1; identical treatment everywhere */
  .page-meta {
    font-size: 0.72rem; color: var(--muted); margin-top: 0.75rem;
    letter-spacing: 0.02em; line-height: 1.6;
  }

  /* ── section labels (used as sub-headings inside page body) ── */
  /* .section-title already defined below; .panel-hdr is its inline-panel twin */
  /* Keep both the same visual weight so scanning a page feels consistent */

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
  .chart-toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.25rem; gap: 0.75rem; }
  .chart-title { font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.2em; color: var(--muted); margin: 0; font-weight: 500; }
  .chart-actions { display: flex; gap: 0.5rem; flex-shrink: 0; }
  .chart-btn {
    font-family: "DM Mono", monospace; font-size: 0.6rem; letter-spacing: 0.08em;
    text-transform: uppercase; padding: 0.25rem 0.6rem; border-radius: 3px;
    border: 1px solid var(--border); background: transparent; color: var(--muted);
    cursor: pointer; transition: border-color 0.2s, color 0.2s; white-space: nowrap;
  }
  .chart-btn:hover { border-color: var(--org-color); color: var(--accent-text); }
  .chart-hint { font-size: 0.58rem; color: var(--muted); opacity: 0.6; margin-top: 0.5rem; text-align: right; }
  .chart-wrap { position: relative; height: 320px; }
  /* section-title and panel-hdr share identical typographic treatment so
     sub-headings read consistently whether inside a card, a panel, or free-floating */
  .section-title,
  .panel-hdr {
    font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.2em;
    color: var(--muted); font-weight: 500;
  }
  .section-title { margin-bottom: 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid var(--border); }
  .data-table { width: 100%; border-collapse: collapse; font-size: 0.72rem; margin-bottom: 3rem; }
  .data-table th { text-align: left; padding: 0.5rem 0.75rem; color: var(--muted); font-weight: 500; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; border-bottom: 1px solid var(--border); }
  .data-table td { padding: 0.5rem 0.75rem; border-bottom: 1px solid var(--border); color: var(--text); }
  .data-table tr:hover td { background: var(--surface); }
  .data-table .num { text-align: right; color: var(--accent-text); font-weight: 500; }
  .data-table .ts  { color: var(--muted); }
  .pill { display: inline-block; font-size: 0.6rem; padding: 0.15rem 0.4rem; border-radius: 2px; text-transform: uppercase; letter-spacing: 0.1em; }
  .pill-live     { background: rgba(255,79,109,0.15); color: var(--red);  border: 1px solid var(--red); }
  .pill-upcoming { background: rgba(79,195,247,0.10); color: var(--blue); border: 1px solid var(--blue); }
  .generated { text-align: center; color: var(--muted); font-size: 0.7rem; margin-top: 3rem; }

  footer {
    margin-top: 5rem;
    padding: 1.75rem 2rem;
    border-top: 1px solid var(--border);
    background: var(--surface);           /* distinct from page bg in both themes */
    border-radius: 6px;
    display: flex; flex-wrap: wrap;
    justify-content: space-between; gap: 1rem;
    font-size: 0.7rem; color: var(--muted);
  }
  footer a { color: var(--muted); text-decoration: none; transition: color 0.2s; }
  footer a:hover { color: var(--accent-text); }
  footer .footer-links { display: flex; gap: 1.25rem; flex-wrap: wrap; align-items: center; }
  footer .footer-sep { opacity: 0.35; }
  /* light-mode footer is already handled by --surface and --border vars */


  /* ── nav bar ── */
  .site-nav { position: sticky; top: 0; z-index: 200; background: var(--surface); border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 0.75rem; padding: 0.65rem 2rem; }
  .site-nav-logo { font-family: "Fraunces", serif; font-size: 0.95rem; font-weight: 700; color: var(--white); text-decoration: none; flex-shrink: 0; }
  .site-nav-logo em { font-style: italic; color: var(--accent-text); }
  .nav-pill { display: inline-flex; align-items: center; gap: 0.35rem; font-size: 0.6rem; padding: 0.2rem 0.55rem; border: 1px solid var(--border); border-radius: 99px; color: var(--muted); white-space: nowrap; }
  .nav-pill strong { color: var(--text); font-weight: 500; }
  .nav-live-pill { border-color: rgba(34,197,94,.4); background: rgba(34,197,94,.06); color: #16a34a; }
  .nav-live-dot { width: 5px; height: 5px; border-radius: 50%; background: #22c55e; flex-shrink: 0; }
  [data-theme="light"] .nav-live-pill { color: #15803d; }
  .nav-spacer { flex: 1; }
  .nav-search-wrap { position: relative; display: flex; align-items: center; }
  .nav-search { font-family: "DM Mono", monospace; font-size: 0.65rem; padding: 0.28rem 0.75rem 0.28rem 1.8rem; border: 1px solid var(--border); border-radius: 99px; background: var(--surface2); color: var(--text); width: 200px; transition: border-color 0.2s, width 0.2s; outline: none; }
  .nav-search:focus { border-color: var(--org-color); width: 260px; }
  .nav-search::placeholder { color: var(--muted); }
  .nav-search-icon { position: absolute; left: 0.55rem; font-size: 0.75rem; color: var(--muted); pointer-events: none; }

  /* ── stats bar (index page) ── */
  .stats-bar { display: flex; gap: 0.6rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
  .stat-pill-bar { display: flex; align-items: center; gap: 0.35rem; padding: 0.3rem 0.75rem; background: var(--surface); border: 1px solid var(--border); border-radius: 99px; font-size: 0.62rem; color: var(--muted); }
  .stat-pill-bar strong { color: var(--text); font-weight: 500; }
  .filter-strip { display: flex; gap: 0.5rem; margin-bottom: 1.25rem; flex-wrap: wrap; align-items: center; }
  .filter-lbl { font-size: 0.6rem; color: var(--muted); margin-right: 0.2rem; }
  .filter-chip { padding: 0.22rem 0.65rem; font-size: 0.6rem; border: 1px solid var(--border); border-radius: 99px; background: transparent; color: var(--muted); cursor: pointer; font-family: inherit; transition: background 0.15s, color 0.15s, border-color 0.15s; }
  .filter-chip:hover, .filter-chip.active { background: var(--surface2); color: var(--accent-text); border-color: var(--org-color); }

  /* ── redesigned org cards ── */
  .org-accent-bar { height: 3px; background: var(--org-color); border-radius: 0; }
  .org-card-body { padding: 1rem 1.25rem 1.1rem; }
  .org-card-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.5rem; gap: 0.5rem; }
  .org-card-title { font-family: "Fraunces", serif; font-size: 1.1rem; font-weight: 700; color: var(--white); line-height: 1.2; }
  .org-card-desc { font-size: 0.72rem; color: var(--muted); margin-bottom: 0.85rem; line-height: 1.5; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
  .org-card-stats { display: flex; gap: 1rem; }
  .ocs { font-size: 0.62rem; color: var(--muted); display: flex; align-items: center; gap: 0.25rem; }
  .ocs strong { color: var(--accent-text); font-weight: 500; }
  .live-badge-sm { display: inline-flex; align-items: center; gap: 0.3rem; font-size: 0.55rem; letter-spacing: 0.08em; padding: 0.15rem 0.45rem; border-radius: 99px; background: rgba(34,197,94,.12); color: #16a34a; border: 1px solid rgba(34,197,94,.3); flex-shrink: 0; white-space: nowrap; }
  .live-dot-sm { width: 5px; height: 5px; border-radius: 50%; background: #22c55e; flex-shrink: 0; }
  [data-theme="light"] .live-badge-sm { color: #15803d; }

  /* ── org hero ── */
  .org-hero { background: var(--surface); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; margin-bottom: 1.5rem; animation: fadeUp 0.5s ease both; }
  .org-hero-accent { height: 4px; background: var(--org-color); }
  .org-hero-body { display: flex; align-items: flex-start; justify-content: space-between; gap: 1.5rem; padding: 1.4rem 1.75rem; flex-wrap: wrap; }
  .org-hero-info { flex: 1; min-width: 0; }
  .org-hero-name { font-family: "Fraunces", serif; font-size: 1.6rem; font-weight: 700; color: var(--white); margin-bottom: 0.3rem; }
  .org-hero-desc { font-size: 0.75rem; color: var(--muted); max-width: 560px; line-height: 1.6; margin-bottom: 0.85rem; }
  .org-hero-stats { display: flex; gap: 1.5rem; flex-wrap: wrap; }
  .ohs { text-align: center; }
  .ohs-val { font-family: "Fraunces", serif; font-size: 1.4rem; font-weight: 700; color: var(--accent-text); line-height: 1.1; }
  .ohs-lbl { font-size: 0.58rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.1em; }

  /* ── sort/filter strip ── */
  .sort-strip { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem; flex-wrap: wrap; }
  .sort-lbl { font-size: 0.6rem; color: var(--muted); }
  .sort-chip { padding: 0.22rem 0.65rem; font-size: 0.6rem; border: 1px solid var(--border); border-radius: 99px; background: transparent; color: var(--muted); cursor: pointer; font-family: inherit; transition: background 0.15s, color 0.15s, border-color 0.15s; }
  .sort-chip:hover { background: var(--surface2); color: var(--text); }
  .sort-chip.active { background: var(--surface2); color: var(--accent-text); border-color: var(--org-color); }

  /* ── channel card redesign ── */
  .ch-card-top { display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.65rem; }
  .ch-card-name-wrap { min-width: 0; }
  .ch-card-name { font-family: "Fraunces", serif; font-size: 0.88rem; font-weight: 700; color: var(--white); line-height: 1.25; }
  .ch-card-role { font-size: 0.58rem; color: var(--muted); margin-top: 0.1rem; }
  .ch-card-stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.35rem; }
  .ch-stat-cell { background: var(--surface2); border-radius: 3px; padding: 0.4rem 0.6rem; }
  .ch-stat-cell-lbl { font-size: 0.55rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; }
  .ch-stat-cell-val { font-size: 0.82rem; font-weight: 500; color: var(--text); }

  @keyframes fadeUp { from { opacity: 0; transform: translateY(14px); } to { opacity: 1; transform: translateY(0); } }
  header { animation: fadeUp 0.5s ease both; }
  .orgs-grid, .channels-list, .streams-grid, .kpi-row { animation: fadeUp 0.5s 0.1s ease both; }

  /* ── channel hero ── */
  .channel-hero { background: var(--surface); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; margin-bottom: 1.5rem; animation: fadeUp 0.5s ease both; }
  .hero-shimmer { height: 4px; background: linear-gradient(90deg, var(--org-color) 0%, transparent 50%, var(--org-color) 100%); background-size: 200% 100%; animation: heroShimmer 3s linear infinite; }
  @keyframes heroShimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }
  .hero-body { display: flex; align-items: flex-start; gap: 1.5rem; padding: 1.75rem 2rem; }
  @media (max-width: 600px) { .hero-body { flex-direction: column; } }
  .hero-avatar-large { width: 88px; height: 88px; border-radius: 50%; object-fit: cover; border: 2px solid var(--org-color); display: block; background: var(--surface2); flex-shrink: 0; }
  .hero-avatar-large-placeholder { width: 88px; height: 88px; border-radius: 50%; background: var(--surface2); border: 2px solid var(--org-color); display: flex; align-items: center; justify-content: center; font-family: "Fraunces", serif; font-size: 1.8rem; font-weight: 700; color: var(--accent-text); flex-shrink: 0; }
  .hero-org-badge { display: inline-flex; align-items: center; gap: 0.4rem; font-size: 0.58rem; letter-spacing: 0.15em; text-transform: uppercase; padding: 0.18rem 0.55rem 0.18rem 0.35rem; border-radius: 2px; border: 1px solid var(--org-color); color: var(--accent-text); background: color-mix(in srgb, var(--org-color) 8%, transparent); margin-bottom: 0.6rem; }
  .hero-org-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--org-color); box-shadow: 0 0 5px var(--org-color); }
  [data-theme="light"] .hero-org-dot { box-shadow: none; }
  .hero-info { flex: 1; min-width: 0; }
  .hero-name { font-family: "Fraunces", serif; font-size: clamp(1.5rem, 3.5vw, 2.4rem); font-weight: 700; line-height: 1.05; color: var(--white); margin-bottom: 0.6rem; }
  .hero-name em { font-style: italic; color: var(--accent-text); }
  .hero-meta-row { display: flex; flex-wrap: wrap; gap: 1rem; font-size: 0.65rem; color: var(--muted); margin-top: 0.35rem; }
  .hero-meta-item { display: flex; align-items: center; gap: 0.3rem; }
  .hero-meta-item strong { color: var(--accent-text); font-weight: 500; }
  .hero-actions { flex-shrink: 0; align-self: flex-start; }
  .yt-link { display: inline-flex; align-items: center; gap: 0.35rem; font-size: 0.6rem; letter-spacing: 0.1em; text-transform: uppercase; color: var(--muted); text-decoration: none; border: 1px solid var(--border); border-radius: 3px; padding: 0.38rem 0.7rem; transition: border-color 0.2s, color 0.2s; }
  .yt-link:hover { border-color: var(--red); color: var(--red); }

  /* ── kpi strip ── */
  .kpi-strip { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1px; background: var(--border); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; margin-bottom: 2rem; animation: fadeUp 0.5s 0.05s ease both; }
  @media (max-width: 700px) { .kpi-strip { grid-template-columns: repeat(2, 1fr); } }
  .kpi-cell { background: var(--surface); padding: 1.1rem 1.25rem; position: relative; }
  .kpi-cell::before { content: ""; position: absolute; top: 0; left: 0; right: 0; height: 2px; background: var(--org-color); transform: scaleX(0); transform-origin: left; transition: transform 0.4s; }
  .kpi-cell:hover::before { transform: scaleX(1); }

  /* ── channel main grid ── */
  .ch-main-grid { display: grid; grid-template-columns: 1fr 310px; gap: 1.25rem; align-items: start; animation: fadeUp 0.5s 0.1s ease both; }
  @media (max-width: 820px) { .ch-main-grid { grid-template-columns: 1fr; } }

  /* ── recent streams card grid (4×2, full-width, above main grid) ── */
  .recent-streams-section { margin-bottom: 2rem; animation: fadeUp 0.5s 0.08s ease both; }
  .recent-streams-hdr { font-size: 0.62rem; letter-spacing: 0.2em; text-transform: uppercase; color: var(--muted); margin-bottom: 0.9rem; display: flex; align-items: center; gap: 0.6rem; font-weight: 500; }
  .recent-streams-hdr::before { content: ""; display: block; width: 6px; height: 6px; border-radius: 50%; background: var(--org-color); box-shadow: 0 0 6px var(--org-color); flex-shrink: 0; }
  [data-theme="light"] .recent-streams-hdr::before { box-shadow: none; }
  .recent-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.85rem; }
  @media (max-width: 900px) { .recent-grid { grid-template-columns: repeat(2, 1fr); } }
  @media (max-width: 500px) { .recent-grid { grid-template-columns: 1fr; } }
  .rc { background: var(--surface); border: 1px solid var(--border); border-radius: 5px; overflow: hidden; text-decoration: none; color: inherit; display: flex; flex-direction: column; transition: border-color 0.2s, transform 0.18s; position: relative; }
  .rc:hover { border-color: var(--org-color); transform: translateY(-2px); }
  .rc::after { content: ""; position: absolute; top: 0; left: 0; right: 0; height: 2px; background: var(--org-color); transform: scaleX(0); transform-origin: left; transition: transform 0.3s; }
  .rc:hover::after { transform: scaleX(1); }
  .rc-thumb { position: relative; width: 100%; padding-bottom: 56.25%; background: var(--surface2); overflow: hidden; flex-shrink: 0; }
  .rc-thumb img { position: absolute; inset: 0; width: 100%; height: 100%; object-fit: cover; display: block; }
  .rc-thumb .rc-placeholder { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: var(--muted); font-size: 1.3rem; }
  .rc-live { position: absolute; top: 5px; left: 5px; background: var(--red); color: #fff; font-size: 0.48rem; font-weight: 500; letter-spacing: 0.1em; padding: 2px 5px; border-radius: 2px; text-transform: uppercase; }
  .rc-body { padding: 0.65rem 0.75rem 0.75rem; flex: 1; display: flex; flex-direction: column; gap: 4px; }
  .rc-title { font-family: "Fraunces", serif; font-size: 0.8rem; font-weight: 700; color: var(--white); line-height: 1.3; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
  .rc-date { font-size: 0.57rem; color: var(--muted); }
  .rc-stats { display: flex; gap: 0.6rem; font-size: 0.58rem; color: var(--muted); margin-top: auto; padding-top: 4px; }
  .rc-peak { color: var(--accent-text); font-weight: 500; }

  /* ── chronological stream list (new row layout) ── */
  .stream-list-panel { background: var(--surface); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
  /* panel-hdr font already merged with .section-title above */
  .panel-hdr { padding: 0.8rem 1.25rem; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; }

  /* collapsible month group */
  .month-group { border-bottom: 1px solid var(--border); }
  .month-group:last-child { border-bottom: none; }
  .month-toggle { display: flex; align-items: center; justify-content: space-between; padding: 0.6rem 1.25rem; font-size: 0.62rem; letter-spacing: 0.2em; text-transform: uppercase; color: var(--muted); font-weight: 500; background: var(--surface2); cursor: pointer; user-select: none; border: none; width: 100%; text-align: left; font-family: inherit; transition: color 0.15s; gap: 0.75rem; }
  .month-toggle:hover { color: var(--accent-text); }
  .month-toggle-left { display: flex; align-items: center; gap: 0.55rem; }
  .month-toggle-left::before { content: ""; display: block; width: 5px; height: 5px; border-radius: 50%; background: var(--org-color); box-shadow: 0 0 5px var(--org-color); flex-shrink: 0; }
  [data-theme="light"] .month-toggle-left::before { box-shadow: none; }
  .month-toggle-right { display: flex; align-items: center; gap: 0.5rem; flex-shrink: 0; }
  .month-cnt-badge { font-size: 0.55rem; padding: 0.08rem 0.38rem; border-radius: 2px; background: rgba(0,0,0,0.25); border: 1px solid rgba(255,255,255,0.1); color: var(--accent-text); }
  [data-theme="light"] .month-cnt-badge { background: rgba(0,0,0,0.06); border-color: rgba(0,0,0,0.15); }
  .month-chevron { font-size: 0.55rem; transition: transform 0.25s; display: inline-block; }
  .month-group.is-open .month-chevron { transform: rotate(180deg); }
  .month-body { display: none; }
  .month-group.is-open .month-body { display: block; }

  .stream-row-item { display: flex; gap: 0.9rem; padding: 0.8rem 1.25rem; border-bottom: 1px solid var(--border); text-decoration: none; color: inherit; position: relative; transition: background 0.15s; }
  .stream-row-item:last-child { border-bottom: none; }
  .stream-row-item::after { content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 2px; background: var(--org-color); transform: scaleY(0); transform-origin: top; transition: transform 0.25s; }
  .stream-row-item:hover { background: var(--surface2); }
  .stream-row-item:hover::after { transform: scaleY(1); }
  .stream-thumb-cell { width: 88px; height: 50px; border-radius: 3px; flex-shrink: 0; background: var(--surface2); overflow: hidden; position: relative; }
  .stream-thumb-cell img { width: 100%; height: 100%; object-fit: cover; display: block; }
  .stream-thumb-cell .th-placeholder { width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; color: var(--muted); font-size: 1.1rem; }
  .stream-row-body { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; }
  .stream-row-title { font-family: "Fraunces", serif; font-size: 0.88rem; font-weight: 700; color: var(--white); line-height: 1.3; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
  .stream-row-meta { font-size: 0.6rem; color: var(--muted); display: flex; align-items: center; gap: 0.35rem; flex-wrap: wrap; }
  .row-sep { opacity: 0.35; }
  .stream-row-stats { display: flex; gap: 0.85rem; font-size: 0.62rem; color: var(--muted); margin-top: 2px; }
  .rs { display: flex; align-items: center; gap: 0.2rem; }
  .rs-peak { color: var(--accent-text); font-weight: 500; }
  .show-more-link { display: flex; align-items: center; gap: 0.5rem; padding: 0.65rem 1.25rem; font-size: 0.6rem; color: var(--muted); cursor: pointer; letter-spacing: 0.12em; text-transform: uppercase; text-decoration: none; border-top: 1px solid var(--border); transition: background 0.15s, color 0.2s; }
  .show-more-link::before { content: ""; display: block; width: 5px; height: 1px; background: currentColor; flex-shrink: 0; }
  .show-more-link:hover { background: var(--surface2); color: var(--accent-text); }

  /* ── sidebar panels ── */
  .sidebar-col { display: flex; flex-direction: column; gap: 1.25rem; }
  .side-panel { background: var(--surface); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
  .monthly-tbl { width: 100%; border-collapse: collapse; font-size: 0.65rem; }
  .monthly-tbl th { padding: 0.5rem 1rem; text-align: left; color: var(--muted); font-size: 0.57rem; text-transform: uppercase; letter-spacing: 0.12em; font-weight: 500; border-bottom: 1px solid var(--border); background: var(--surface2); }
  .monthly-tbl th:not(:first-child) { text-align: right; }
  .monthly-tbl td { padding: 0.52rem 1rem; border-bottom: 1px solid var(--border); color: var(--text); }
  .monthly-tbl td:not(:first-child) { text-align: right; }
  .monthly-tbl tr:last-child td { border-bottom: none; }
  .monthly-tbl tr:hover td { background: var(--surface2); }
  .month-a { color: var(--text); text-decoration: none; transition: color 0.2s; }
  .month-a:hover { color: var(--accent-text); }
  .month-cnt { display: inline-block; font-size: 0.57rem; padding: 0.1rem 0.4rem; border-radius: 2px; background: rgba(0,0,0,0.25); border: 1px solid rgba(255,255,255,0.1); color: var(--accent-text); }
  [data-theme="light"] .month-cnt { background: rgba(0,0,0,0.06); border-color: rgba(0,0,0,0.15); }
  .month-peak { color: var(--accent-text); font-weight: 500; }
  .month-best-row td { background: color-mix(in srgb, var(--org-color) 4%, transparent); }
  .month-best-row .month-a::after { content: " ★"; color: var(--org-color); font-size: 0.55rem; }
  .rec-row { display: flex; justify-content: space-between; align-items: flex-start; padding: 0.8rem 1.25rem; border-bottom: 1px solid var(--border); gap: 0.5rem; }
  .rec-row:last-child { border-bottom: none; }
  .rec-lbl { font-size: 0.56rem; text-transform: uppercase; letter-spacing: 0.12em; color: var(--muted); margin-bottom: 0.25rem; }
  .rec-val { font-family: "Fraunces", serif; font-size: 1.25rem; font-weight: 700; color: var(--accent-text); line-height: 1.1; }
  .rec-ctx { font-size: 0.57rem; color: var(--muted); margin-top: 0.15rem; max-width: 145px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .rec-right { text-align: right; flex-shrink: 0; }
  .rec-date { font-size: 0.6rem; color: var(--muted); }
  .subs-body { padding: 0.9rem 1.25rem 1.1rem; }
  .subs-count { font-family: "Fraunces", serif; font-size: 1.75rem; font-weight: 700; color: var(--accent-text); line-height: 1; margin-bottom: 0.2rem; }
  .subs-label { font-size: 0.58rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.14em; margin-bottom: 0.85rem; }
  svg.sparkline { width: 100%; height: 40px; overflow: visible; display: block; }
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
        f'</head>\n<body>\n'
        f'<nav class="site-nav">\n'
        f'  <a class="site-nav-logo" href="{"../" * depth}index.html">IDVTuber <em>Tracker</em></a>\n'
        f'  <span class="nav-spacer"></span>\n'
        + (f'  <div class="nav-search-wrap">\n'
           f'    <span class="nav-search-icon">&#128269;</span>\n'
           f'    <input class="nav-search" id="navSearch" type="search" '
           f'placeholder="Search organisations…" autocomplete="off">\n'
           f'  </div>\n' if depth == 0 else '')
        + f'  {_TOGGLE_HTML}\n'
        f'</nav>\n'
        f'<div class="page">\n'
    )


# ── Page-specific JS snippets ────────────────────────────────────────────────
# INDEX_JS: search input filters org cards live; filter chips toggle active state.
_INDEX_JS = """
<script>
(function () {
  var grid  = document.querySelector('.orgs-grid');
  var inp   = document.getElementById('navSearch');
  var chips = document.querySelectorAll('.filter-chip');
  var mode  = 'all';    // 'all' | 'az' | 'peak'

  function applyFilter() {
    var q = inp ? inp.value.trim().toLowerCase() : '';
    var cards = grid ? grid.querySelectorAll('.org-card') : [];
    cards.forEach(function (c) {
      var name = (c.getAttribute('data-name') || '').toLowerCase();
      var show = q === '' || name.includes(q);
      c.style.display = show ? '' : 'none';
    });
  }

  function applySort() {
    if (!grid) return;
    var cards = Array.from(grid.querySelectorAll('.org-card'));
    if (mode === 'az') {
      cards.sort(function (a, b) {
        return (a.getAttribute('data-name') || '').localeCompare(b.getAttribute('data-name') || '');
      });
      cards.forEach(function (c) { grid.appendChild(c); });
    } else if (mode === 'peak') {
      cards.sort(function (a, b) {
        var pa = parseFloat(a.getAttribute('data-peak') || '0') || 0;
        var pb = parseFloat(b.getAttribute('data-peak') || '0') || 0;
        return pb - pa;   // descending — highest peak viewers first
      });
      cards.forEach(function (c) { grid.appendChild(c); });
    }
    // 'all' = default document order (no re-sort needed on page load)
  }

  if (inp) {
    inp.addEventListener('input', applyFilter);
  }

  chips.forEach(function (chip) {
    chip.addEventListener('click', function () {
      chips.forEach(function (c) { c.classList.remove('active'); });
      chip.classList.add('active');
      var label = chip.textContent.trim();
      if (label === 'A–Z') {
        mode = 'az';
      } else if (label === 'Peak Viewers') {
        mode = 'peak';
      } else {
        mode = 'all';
      }
      applySort();
      applyFilter();
    });
  });
})();
</script>"""

# ORG_JS: sort chips re-order channel cards by the selected metric.
_ORG_JS = """
<script>
(function () {
  var grid  = document.querySelector('.channels-grid');
  var chips = document.querySelectorAll('.sort-chip');
  if (!grid || !chips.length) return;

  function getVal(card, attr) {
    return parseFloat(card.getAttribute(attr) || '0') || 0;
  }

  function sortCards(key) {
    var cards = Array.from(grid.querySelectorAll('.channel-card'));
    cards.sort(function (a, b) {
      if (key === 'az') {
        return (a.getAttribute('data-name') || '').localeCompare(b.getAttribute('data-name') || '');
      }
      var map = { 'subs': 'data-subs', 'peak': 'data-peak', 'likes': 'data-likes', 'streams': 'data-streams' };
      return getVal(b, map[key]) - getVal(a, map[key]);
    });
    cards.forEach(function (c) { grid.appendChild(c); });
  }

  var keyMap = {
    'Subscribers': 'subs',
    'Peak CCV':    'peak',
    'Peak Likes':  'likes',
    'Streams':     'streams',
    'A–Z':   'az',
  };

  chips.forEach(function (chip) {
    chip.addEventListener('click', function () {
      chips.forEach(function (c) { c.classList.remove('active'); });
      chip.classList.add('active');
      var key = keyMap[chip.textContent.trim()] || 'subs';
      sortCards(key);
    });
  });
})();
</script>"""


def _html_foot(depth: int, page_type: str = '') -> str:
    rel = "../" * depth
    extra_js = ''
    if page_type == 'index':
        extra_js = _INDEX_JS + '\n'
    elif page_type == 'org':
        extra_js = _ORG_JS + '\n'
    return (
        f'\n  <footer>\n'
        f'    <span>&#169; 2026 IDVTuber Tracker &#8212; Non-commercial fan project</span>\n'
        f'    <nav class="footer-links">\n'
        f'      <a href="{rel}index.html">Home</a>\n'
        f'      <span class="footer-sep">·</span>\n'
        f'      <a href="{rel}privacy.html">Privacy Policy</a>\n'
        f'      <span class="footer-sep">·</span>\n'
        f'      <a href="{rel}terms.html">Terms of Use</a>\n'
        f'    </nav>\n'
        f'  </footer>\n'
        f'</div>\n'
        f'{_THEME_JS}\n'
        + extra_js
        + f'</body>\n</html>'
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
# PAGE WRITERS  (unchanged from original — all logic preserved)
# ══════════════════════════════════════════════════════════════════════════════

def write_index(total_streams: int, total_channels: int, generated_at: str,
                stream_counts: dict | None = None,
                all_streams_by_channel: dict | None = None) -> None:
    stream_counts          = stream_counts or {}
    all_streams_by_channel = all_streams_by_channel or {}

    # ── per-org stats: stream count and peak CCV ──────────────────────────────
    def _org_stats(org):
        n_str = sum(stream_counts.get(e[0], 0) for e in org["channels"])
        peak  = 0
        for e in org["channels"]:
            for s in all_streams_by_channel.get(e[0], []):
                v = s.get("peak_viewers") or 0
                if v > peak:
                    peak = v
        return n_str, peak

    # ── build org cards ───────────────────────────────────────────────────────
    org_cards = ""
    for org_slug, org in ORG_MAP.items():
        n_ch = len(org["channels"])
        n_streams, peak = _org_stats(org)
        peak_str = fmt(peak) if peak else "—"
        org_cards += (
            f'\n    <a class="org-card" href="{org_slug}/index.html"'
            f' style="--org-color:{org["color"]}"'
            f' data-name="{esc(org["label"])}"'
            f' data-streams="{n_streams}" data-peak="{peak}">\n'
            f'      <div class="org-accent-bar"></div>\n'
            f'      <div class="org-card-body">\n'
            f'        <div class="org-card-top">\n'
            f'          <div class="org-card-title">{esc(org["label"])}</div>\n'
            f'        </div>\n'
            f'        <div class="org-card-desc">{esc(org["desc"])}</div>\n'
            f'        <div class="org-card-stats">\n'
            f'          <span class="ocs">&#128100; <strong>{n_ch}</strong></span>\n'
            f'          <span class="ocs">&#9654; <strong>{n_streams}</strong></span>\n'
            f'          <span class="ocs">&#128065; <strong>{peak_str}</strong> peak</span>\n'
            f'        </div>\n'
            f'      </div>\n'
            f'    </a>'
        )

    body = (
        f'  <header>\n'
        f'    <p class="eyebrow">IDVTuber Tracker &#8212; Live Analytics</p>\n'
        f'    <h1>Stream <em>Overview</em></h1>\n'
        f'    <p class="page-meta">Generated: {generated_at}</p>\n'
        f'  </header>\n'
        f'  <div class="stats-bar">\n'
        f'    <span class="stat-pill-bar">&#127981; <strong>{len(ORG_MAP)}</strong> organisations</span>\n'
        f'    <span class="stat-pill-bar">&#128100; <strong>{total_channels}</strong> channels</span>\n'
        f'    <span class="stat-pill-bar">&#9654; <strong>{total_streams}</strong> streams recorded</span>\n'
        f'  </div>\n'
        f'  <div class="filter-strip">\n'
        f'    <span class="filter-lbl">Filter:</span>\n'
        f'    <span class="filter-chip" style="background:var(--surface2);color:var(--accent-text);border-color:var(--org-color)">All</span>\n'
        f'    <span class="filter-chip">A&#8211;Z</span>\n'
        f'    <span class="filter-chip">Peak Viewers</span>\n'
        f'  </div>\n'
        f'  <div class="orgs-grid">{org_cards}\n  </div>\n'
    )

    html = _html_head("Stream Analytics", 0) + body + _html_foot(0, 'index')
    (OUTPUT_DIR / "index.html").write_text(html, encoding="utf-8")
    log.info("Written: index.html")

def write_org_page(org_slug: str, org: dict, stream_counts: dict,
                   logos: dict[str, str] | None = None,
                   channel_ids_map: dict[str, str] | None = None,
                   subscribers: dict[str, int] | None = None,
                   all_streams_by_channel: dict | None = None) -> None:
    org_dir = OUTPUT_DIR / org_slug
    org_dir.mkdir(exist_ok=True)
    logos                  = logos or {}
    channel_ids_map        = channel_ids_map or {}
    subscribers            = subscribers or {}
    all_streams_by_channel = all_streams_by_channel or {}

    # ── aggregate org-level stats for hero ────────────────────────────────────
    total_org_streams = sum(stream_counts.get(e[0], 0) for e in org["channels"])
    peak_all = 0
    for e in org["channels"]:
        for s in all_streams_by_channel.get(e[0], []):
            v = s.get("peak_viewers") or 0
            if v > peak_all:
                peak_all = v
    total_subs = 0
    for e in org["channels"]:
        ch_id = channel_ids_map.get(e[0], "")
        total_subs += subscribers.get(ch_id, 0) or 0

    # ── channel cards ─────────────────────────────────────────────────────────
    cards = ""
    for entry in org["channels"]:
        ch_name   = entry[0]
        ch_type   = entry[1]
        ch_slug   = slugify(ch_name)
        n_str     = stream_counts.get(ch_name, 0)
        ch_id     = channel_ids_map.get(ch_name, "")
        logo_url  = logos.get(ch_id, "")
        sub_count = subscribers.get(ch_id, 0) or 0

        # per-channel peak CCV
        ch_peak  = 0
        ch_likes = 0
        ch_streams = all_streams_by_channel.get(ch_name, [])
        peaks = [s.get("peak_viewers") or 0 for s in ch_streams if s.get("peak_viewers")]
        if peaks:
            ch_peak = max(peaks)
        likes = [s.get("peak_likes") or 0 for s in ch_streams if s.get("peak_likes")]
        if likes:
            ch_likes = max(likes)

        # avatar
        words    = ch_name.replace("【", " ").replace("〔", " ").replace("Ch.", "").split()
        initials = "".join(w[0].upper() for w in words if w)[:2] or "?"
        if logo_url:
            _oe = f"this.outerHTML='<div class=&quot;channel-avatar-placeholder&quot;>{initials}</div>'"
            avatar_html = (
                f'<img class="channel-avatar" src="{logo_url}" alt="" '
                f'loading="lazy" referrerpolicy="no-referrer" onerror="{_oe}">'
            )
        else:
            avatar_html = f'<div class="channel-avatar-placeholder">{initials}</div>'

        role_lbl = "Org Channel" if ch_type == "org" else "Talent"

        cards += (
            f'\n    <a class="channel-card" href="{ch_slug}/index.html"'
            f' data-name="{esc(ch_name)}" data-subs="{sub_count}"'
            f' data-streams="{n_str}" data-peak="{ch_peak}" data-likes="{ch_likes}">\n'
            f'      <div class="ch-card-top">\n'
            f'        {avatar_html}\n'
            f'        <div class="ch-card-name-wrap">\n'
            f'          <div class="ch-card-name">{esc(ch_name)}</div>\n'
            f'          <div class="ch-card-role">{role_lbl}</div>\n'
            f'        </div>\n'
            f'      </div>\n'
            f'      <div class="ch-card-stat-grid">\n'
            f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Subscribers</div><div class="ch-stat-cell-val">{fmt_subs(sub_count)}</div></div>\n'
            f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Streams</div><div class="ch-stat-cell-val">{n_str}</div></div>\n'
            f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Peak CCV</div><div class="ch-stat-cell-val">{fmt(ch_peak) if ch_peak else "—"}</div></div>\n'
            f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Peak Likes</div><div class="ch-stat-cell-val">{fmt(ch_likes) if ch_likes else "—"}</div></div>\n'
            f'      </div>\n'
            f'    </a>'
        )

    bc = _breadcrumb([("Home", "../index.html"), (org["label"], "")])
    body = (
        bc
        # org hero
        + f'  <div class="org-hero">\n'
        f'    <div class="org-hero-accent"></div>\n'
        f'    <div class="org-hero-body">\n'
        f'      <div class="org-hero-info">\n'
        f'        <div class="org-hero-name">{esc(org["label"])}</div>\n'
        f'        <div class="org-hero-desc">{esc(org["desc"])}</div>\n'
        f'        <div class="org-hero-stats">\n'
        f'          <div class="ohs"><div class="ohs-val">{len(org["channels"])}</div><div class="ohs-lbl">Channels</div></div>\n'
        f'          <div class="ohs"><div class="ohs-val">{total_org_streams}</div><div class="ohs-lbl">Streams</div></div>\n'
        f'          <div class="ohs"><div class="ohs-val">{fmt(peak_all) if peak_all else "—"}</div><div class="ohs-lbl">Peak CCV</div></div>\n'
        f'          <div class="ohs"><div class="ohs-val">{fmt_subs(total_subs)}</div><div class="ohs-lbl">Combined subs</div></div>\n'
        f'        </div>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'  </div>\n'
        # sort strip
        + f'  <div class="sort-strip">\n'
        f'    <span class="sort-lbl">Sort by:</span>\n'
        f'    <span class="sort-chip active">Subscribers</span>\n'
        f'    <span class="sort-chip">Peak CCV</span>\n'
        f'    <span class="sort-chip">Peak Likes</span>\n'
        f'    <span class="sort-chip">Streams</span>\n'
        f'    <span class="sort-chip">A&#8211;Z</span>\n'
        f'  </div>\n'
        f'  <div class="channels-grid">{cards}\n  </div>\n'
    )

    html = _html_head(org["label"], 1, org["color"]) + body + _html_foot(1, 'org')
    (org_dir / "index.html").write_text(html, encoding="utf-8")
    log.info("Written: %s/index.html", org_slug)


def _dur_str(first, last) -> str:
    """Return compact duration string e.g. '2h 07m'."""
    if not first or not last:
        return ""
    try:
        total = int((last - first).total_seconds())
        h, rem = divmod(total, 3600)
        m = rem // 60
        return f"{h}h {m:02d}m" if h else f"{m}m"
    except Exception:
        return ""


def write_channel_page(org_slug: str, org: dict, ch_name: str,
                       streams: list[dict],
                       logos: dict | None = None,
                       channel_ids_map: dict | None = None,
                       subscribers: dict | None = None) -> None:
    logos          = logos          or {}
    channel_ids_map = channel_ids_map or {}
    subscribers    = subscribers    or {}

    ch_slug  = slugify(ch_name)
    ch_dir   = OUTPUT_DIR / org_slug / ch_slug
    ch_dir.mkdir(parents=True, exist_ok=True)

    # ── resolve avatar + subscriber count ─────────────────────────────────────
    ch_id    = channel_ids_map.get(ch_name, "")
    logo_url = logos.get(ch_id, "")
    sub_raw  = subscribers.get(ch_id, 0) or 0
    sub_fmt  = fmt(sub_raw) if sub_raw else "—"

    # Build initials fallback (up to 2 chars from the display name)
    words    = ch_name.replace("【", " ").replace("〔", " ").replace("Ch.", "").split()
    initials = "".join(w[0].upper() for w in words if w)[:2] or "?"

    # Avatar HTML — real image with onerror fallback to initials placeholder
    if logo_url:
        _oe = f"this.outerHTML='<div class=&quot;hero-avatar-large-placeholder&quot;>{initials}</div>'"
        avatar_html = (
            f'<img class="hero-avatar-large"'
            f' src="{logo_url}" alt="{esc(ch_name)} avatar" loading="lazy"'
            f' onerror="{_oe}">\n'
        )
    else:
        avatar_html = f'<div class="hero-avatar-large-placeholder">{initials}</div>\n'

    # ── tracking window ────────────────────────────────────────────────────────
    def _stream_dt(s):
        v = s.get("first_seen")
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = datetime.fromisoformat(v.replace("Z", "+00:00"))
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.astimezone(_LOCAL_TZ)
        except Exception:
            return None

    dts = [d for d in (_stream_dt(s) for s in streams) if d]
    if dts:
        oldest  = min(dts)
        newest  = max(dts)
        if oldest.year == newest.year and oldest.month == newest.month:
            window_str = oldest.strftime("%b %Y")
        else:
            window_str = f'{oldest.strftime("%b %Y")} – {newest.strftime("%b %Y")}'
    else:
        window_str = "—"

    # ── aggregate stats (KPI strip) ───────────────────────────────────────────
    n_streams   = len(streams)
    peak_ccvs   = [s["peak_viewers"] for s in streams if s.get("peak_viewers")]
    all_time_peak = max(peak_ccvs) if peak_ccvs else 0
    avg_peak    = round(sum(peak_ccvs) / len(peak_ccvs)) if peak_ccvs else 0
    total_views = sum(s.get("view_count") or 0 for s in streams)

    # ── records ────────────────────────────────────────────────────────────────
    def _best(key):
        candidates = [(s.get(key) or 0, s) for s in streams if s.get(key)]
        return max(candidates, key=lambda x: x[0]) if candidates else (0, None)

    peak_ccv_val, peak_ccv_stream = _best("peak_viewers")
    peak_likes_val, peak_likes_stream = _best("peak_likes")
    peak_views_val, peak_views_stream = _best("view_count")

    def _rec_title(stream):
        if not stream:
            return "—"
        t = (stream.get("video_title") or "").strip()
        return t[:45] + "…" if len(t) > 45 else t or "—"

    def _rec_date(stream):
        if not stream:
            return "—"
        dt = _stream_dt(stream)
        return dt.strftime("%d %b %Y") if dt else "—"

    # ── group streams by month ─────────────────────────────────────────────────
    months: OrderedDict = OrderedDict()
    for stream in streams:
        dt = _stream_dt(stream)
        month_key = dt.strftime("%B %Y") if dt else "Unknown"
        months.setdefault(month_key, []).append(stream)

    # ── monthly summary table (for sidebar) ───────────────────────────────────
    monthly_peaks = {}
    for mk, ms in months.items():
        mp = max((s.get("peak_viewers") or 0 for s in ms), default=0)
        monthly_peaks[mk] = mp
    global_best_month = max(monthly_peaks, key=monthly_peaks.get) if monthly_peaks else None

    monthly_rows = ""
    for mk, ms in months.items():
        is_best = mk == global_best_month
        tr_cls  = ' class="month-best-row"' if is_best else ""
        pk      = monthly_peaks.get(mk, 0)
        monthly_rows += (
            f'      <tr{tr_cls}>\n'
            f'        <td><a class="month-a" href="#">{mk}</a></td>\n'
            f'        <td><span class="month-cnt">{len(ms)}</span></td>\n'
            f'        <td class="month-peak">{fmt(pk)}</td>\n'
            f'      </tr>\n'
        )

    # ── recent streams card grid (latest 8, full-width 4×2) ──────────────────
    recent_8 = streams[:8]

    def _rc_card(stream) -> str:
        vid    = stream["video_id"]
        v_slug = slugify(vid)
        status = stream.get("stream_status", "vod") or "vod"
        live   = status == "live"
        dt     = _stream_dt(stream)
        date_s = dt.strftime("%d %b %Y") if dt else "—"
        title  = esc((stream.get("video_title") or vid)[:70])
        thumb  = f"https://i.ytimg.com/vi/{vid}/mqdefault_live.jpg"
        _onerr = "this.parentNode.innerHTML='<div class=&quot;rc-placeholder&quot;&gt;&#9654;</div>'"
        live_b = '<span class="rc-live">Live</span>' if live else ""
        return (
            f'    <a class="rc" href="{v_slug}.html">\n'
            f'      <div class="rc-thumb">\n'
            f'        <img src="{thumb}" alt="" loading="lazy" onerror="{_onerr}">\n'
            f'        {live_b}\n'
            f'      </div>\n'
            f'      <div class="rc-body">\n'
            f'        <div class="rc-title">{title}</div>\n'
            f'        <div class="rc-date">{date_s}</div>\n'
            f'        <div class="rc-stats">\n'
            f'          <span>&#128065; <span class="rc-peak">{fmt(stream.get("peak_viewers"))}</span></span>\n'
            f'          <span>&#9825; {fmt(stream.get("peak_likes"))}</span>\n'
            f'          <span>&#9654; {fmt(stream.get("view_count"))}</span>\n'
            f'        </div>\n'
            f'      </div>\n'
            f'    </a>\n'
        )

    recent_cards_html = ""
    for s in recent_8:
        recent_cards_html += _rc_card(s)

    recent_section_html = (
        f'  <div class="recent-streams-section">\n'
        f'    <div class="recent-streams-hdr">Recent streams</div>\n'
        f'    <div class="recent-grid">\n'
        + recent_cards_html
        + f'    </div>\n'
        f'  </div>\n'
    ) if recent_8 else ""

    # ── chronological stream list — collapsible month groups ─────────────────
    def _row_item(stream) -> str:
        vid      = stream["video_id"]
        v_slug   = slugify(vid)
        status   = stream.get("stream_status", "vod") or "vod"
        live     = status == "live"
        badge    = '<div class="live-badge">Live</div>' if live else ""
        dt       = _stream_dt(stream)
        date_str = dt.strftime("%d %b %Y") if dt else "—"
        time_str = fmt_dt(stream.get("first_seen"), time_only=True)
        dur      = _dur_str(stream.get("first_seen"), stream.get("last_seen"))
        thumb    = f"https://i.ytimg.com/vi/{vid}/mqdefault_live.jpg"
        title    = esc((stream.get("video_title") or vid)[:90])
        _onerr   = "this.parentNode.innerHTML='<div class=&quot;th-placeholder&quot;&gt;&#9654;</div>'"
        dur_part = (
            f'        <span class="row-sep">·</span>\n'
            f'        <span>{dur}</span>\n'
        ) if dur else ""
        return (
            f'  <a class="stream-row-item" href="{v_slug}.html">\n'
            f'    <div class="stream-thumb-cell">\n'
            f'      <img src="{thumb}" alt="" loading="lazy" onerror="{_onerr}">\n'
            f'      {badge}\n'
            f'    </div>\n'
            f'    <div class="stream-row-body">\n'
            f'      <div class="stream-row-title">{title}</div>\n'
            f'      <div class="stream-row-meta">\n'
            f'        <span>{date_str}</span>\n'
            f'        <span class="row-sep">·</span>\n'
            f'        <span>{time_str} WIB</span>\n'
            + dur_part
            + f'      </div>\n'
            f'      <div class="stream-row-stats">\n'
            f'        <span class="rs">&#128065; <span class="rs-peak">{fmt(stream.get("peak_viewers"))}</span> peak</span>\n'
            f'        <span class="rs">&#9825; {fmt(stream.get("peak_likes"))}</span>\n'
            f'        <span class="rs">&#9654; {fmt(stream.get("view_count"))}</span>\n'
            f'      </div>\n'
            f'    </div>\n'
            f'  </a>\n'
        )

    # Build one collapsible group per month; first month open by default
    chron_groups = ""
    for i, (mk, ms) in enumerate(months.items()):
        open_cls = " is-open" if i == 0 else ""
        rows_html = "".join(_row_item(s) for s in ms)
        chron_groups += (
            f'  <div class="month-group{open_cls}">\n'
            f'    <button class="month-toggle" aria-expanded="{"true" if i == 0 else "false"}">\n'
            f'      <span class="month-toggle-left">{mk}</span>\n'
            f'      <span class="month-toggle-right">'
            f'<span class="month-cnt-badge">{len(ms)}</span>'
            f'<span class="month-chevron">▾</span>'
            f'</span>\n'
            f'    </button>\n'
            f'    <div class="month-body">\n'
            + rows_html
            + f'    </div>\n'
            f'  </div>\n'
        )

    if not months:
        chron_groups = '  <p class="empty" style="padding:1.25rem;">No streams recorded yet.</p>\n'

    chron_js = (
        '<script>\n'
        '(function(){\n'
        '  document.querySelectorAll(".month-toggle").forEach(function(btn){\n'
        '    btn.addEventListener("click", function(){\n'
        '      var grp = btn.closest(".month-group");\n'
        '      var open = grp.classList.toggle("is-open");\n'
        '      btn.setAttribute("aria-expanded", open ? "true" : "false");\n'
        '    });\n'
        '  });\n'
        '})();\n'
        '</script>\n'
    )

    stream_list_html = (
        f'    <div class="stream-list-panel">\n'
        f'      <div class="panel-hdr">All streams — by month</div>\n'
        + chron_groups
        + f'    </div>\n'
    )

    # ── assemble page ─────────────────────────────────────────────────────────
    bc = _breadcrumb([
        ("Home",       "../../index.html"),
        (org["label"], "../index.html"),
        (ch_name,      ""),
    ])

    yt_url = f"https://youtube.com/channel/{ch_id}" if ch_id else "#"

    body = (
        bc
        # ── hero ──
        + f'  <div class="channel-hero">\n'
        f'    <div class="hero-shimmer"></div>\n'
        f'    <div class="hero-body">\n'
        f'      {avatar_html}'
        f'      <div class="hero-info">\n'
        f'        <div class="hero-org-badge">\n'
        f'          <div class="hero-org-dot"></div>\n'
        f'          {esc(org["label"])}\n'
        f'        </div>\n'
        f'        <div class="hero-name">{esc(ch_name)}</div>\n'
        f'        <div class="hero-meta-row">\n'
        f'          <div class="hero-meta-item"><span>Subscribers</span><strong>{sub_fmt}</strong></div>\n'
        f'          <div class="hero-meta-item"><span>Total views</span><strong>{fmt(sum(s.get("view_count") or 0 for s in streams))}</strong></div>\n'
        f'          <div class="hero-meta-item"><span>Tracking</span><strong>{window_str}</strong></div>\n'
        f'        </div>\n'
        f'      </div>\n'
        f'      <div class="hero-actions">\n'
        f'        <a class="yt-link" href="{yt_url}" target="_blank" rel="noopener">&#9654; YouTube</a>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'  </div>\n'
        # ── kpi strip ──
        + f'  <div class="kpi-strip">\n'
        f'    <div class="kpi-cell"><div class="kpi-label">Streams tracked</div><div class="kpi-value">{n_streams}</div><div class="kpi-sub">{window_str}</div></div>\n'
        f'    <div class="kpi-cell"><div class="kpi-label">Peak CCV</div><div class="kpi-value">{fmt(all_time_peak)}</div><div class="kpi-sub">All-time record</div></div>\n'
        f'    <div class="kpi-cell"><div class="kpi-label">Avg peak CCV</div><div class="kpi-value">{fmt(avg_peak)}</div><div class="kpi-sub">Per stream</div></div>\n'
        f'    <div class="kpi-cell"><div class="kpi-label">Total views</div><div class="kpi-value">{fmt(total_views)}</div><div class="kpi-sub">Tracked streams</div></div>\n'
        f'  </div>\n'
        # ── recent streams grid (full-width, above main grid) ──
        + recent_section_html
        # ── main grid ──
        + f'  <div class="ch-main-grid">\n'
        # left: collapsible chronological list
        + stream_list_html
        # right: sidebar
        + f'    <div class="sidebar-col">\n'
        # 1. monthly summary
        + f'      <div class="side-panel">\n'
        f'        <div class="panel-hdr">Monthly summary</div>\n'
        f'        <table class="monthly-tbl">\n'
        f'          <thead><tr>\n'
        f'            <th>Month</th><th>Streams</th><th>Peak CCV</th>\n'
        f'          </tr></thead>\n'
        f'          <tbody>\n'
        + monthly_rows
        + f'          </tbody>\n'
        f'        </table>\n'
        f'      </div>\n'
        # 2. current subscribers (count only, no graph)
        + f'      <div class="side-panel">\n'
        f'        <div class="panel-hdr">Subscribers</div>\n'
        f'        <div class="subs-body">\n'
        f'          <div class="subs-count">{sub_fmt}</div>\n'
        f'          <div class="subs-label">YouTube subscribers</div>\n'
        f'        </div>\n'
        f'      </div>\n'
        # 3. channel records
        + f'      <div class="side-panel">\n'
        f'        <div class="panel-hdr">Channel records</div>\n'
        f'        <div class="rec-row">\n'
        f'          <div><div class="rec-lbl">Peak CCV</div><div class="rec-val">{fmt(peak_ccv_val)}</div><div class="rec-ctx">{esc(_rec_title(peak_ccv_stream))}</div></div>\n'
        f'          <div class="rec-right"><div class="rec-lbl">Date</div><div class="rec-date">{_rec_date(peak_ccv_stream)}</div></div>\n'
        f'        </div>\n'
        f'        <div class="rec-row">\n'
        f'          <div><div class="rec-lbl">Most liked</div><div class="rec-val">{fmt(peak_likes_val)}</div><div class="rec-ctx">{esc(_rec_title(peak_likes_stream))}</div></div>\n'
        f'          <div class="rec-right"><div class="rec-lbl">Date</div><div class="rec-date">{_rec_date(peak_likes_stream)}</div></div>\n'
        f'        </div>\n'
        f'        <div class="rec-row">\n'
        f'          <div><div class="rec-lbl">Most viewed</div><div class="rec-val">{fmt(peak_views_val)}</div><div class="rec-ctx">{esc(_rec_title(peak_views_stream))}</div></div>\n'
        f'          <div class="rec-right"><div class="rec-lbl">Date</div><div class="rec-date">{_rec_date(peak_views_stream)}</div></div>\n'
        f'        </div>\n'
        f'      </div>\n'
        f'    </div>\n'   # close sidebar-col
        f'  </div>\n'     # close ch-main-grid
        + chron_js
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

    labels   = [fmt_dt(r["collected_at"], time_only=True) for r in timeseries]
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
        '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>\n'
        '<script src="https://cdnjs.cloudflare.com/ajax/libs/hammer.js/2.0.8/hammer.min.js"></script>\n'
        '<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>'
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
        f'    <div class="chart-toolbar">\n'
        f'      <div class="chart-title">Concurrent Viewers over Time</div>\n'
        f'      <div class="chart-actions">\n'
        f'        <button class="chart-btn" onclick="resetZoom(\'viewerChart\')">Reset Zoom</button>\n'
        f'        <button class="chart-btn" onclick="downloadCSV(\'viewerChart\')">Download CSV</button>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'    <div class="chart-wrap"><canvas id="viewerChart"></canvas></div>\n'
        f'    <p class="chart-hint">Scroll to zoom &nbsp;&#183;&nbsp; Shift+drag to select range &nbsp;&#183;&nbsp; Drag to pan &nbsp;&#183;&nbsp; Double-click to reset</p>\n'
        f'  </div>\n\n'
        f'  <div class="chart-box">\n'
        f'    <div class="chart-toolbar">\n'
        f'      <div class="chart-title">Likes &amp; Comments over Time</div>\n'
        f'      <div class="chart-actions">\n'
        f'        <button class="chart-btn" onclick="resetZoom(\'engagementChart\')">Reset Zoom</button>\n'
        f'        <button class="chart-btn" onclick="downloadCSV(\'engagementChart\')">Download CSV</button>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'    <div class="chart-wrap"><canvas id="engagementChart"></canvas></div>\n'
        f'    <p class="chart-hint">Scroll to zoom &nbsp;&#183;&nbsp; Shift+drag to select range &nbsp;&#183;&nbsp; Drag to pan &nbsp;&#183;&nbsp; Double-click to reset</p>\n'
        f'  </div>\n\n'
        f'  <p class="generated">Generated {_now_local().strftime("%Y-%m-%d %H:%M WIB")}'
        f' &nbsp;&#183;&nbsp; yt-livestream-tracker</p>\n\n'
        f'<script>\n'
        f'// ── Data ────────────────────────────────────────────────────────\n'
        f'const ts    = {json.dumps(labels)};\n'
        f'const views = {json.dumps(viewers)};\n'
        f'const likes = {json.dumps(likes)};\n'
        f'const comms = {json.dumps(comments)};\n'
        f'const VIDEO_ID = {json.dumps(vid)};\n'
        f"const orgColor = '{org_color}';\n"
        f'\n'
        f'// ── Theme-aware colours (read CSS variables at runtime) ─────────\n'
        f'function getCSSVar(name) {{\n'
        f'  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();\n'
        f'}}\n'
        f'function chartColors() {{\n'
        f'  return {{\n'
        f'    grid: getCSSVar("--border") || "rgba(90,90,122,0.25)",\n'
        f'    tick: getCSSVar("--muted")  || "#5a5a7a",\n'
        f'  }};\n'
        f'}}\n'
        f'\n'
        f'// ── Shared dataset defaults ──────────────────────────────────────\n'
        f'const LINE = {{\n'
        f'  borderWidth: 2,\n'
        f'  pointRadius: 0,          // no dots on the line\n'
        f'  pointHoverRadius: 4,     // dot appears only on hover\n'
        f'  pointHoverBorderWidth: 2,\n'
        f'  fill: true,\n'
        f'  tension: 0.4,            // smooth cubic bezier curve\n'
        f'}};\n'
        f'\n'
        f'// ── Base chart options ───────────────────────────────────────────\n'
        f'function makeOpts(extraPlugins) {{\n'
        f'  const c = chartColors();\n'
        f'  return {{\n'
        f'    responsive: true,\n'
        f'    maintainAspectRatio: false,\n'
        f'    interaction: {{ mode: "index", intersect: false }},\n'
        f'    plugins: {{\n'
        f'      legend: {{ labels: {{ color: c.tick, font: {{ family: "DM Mono", size: 11 }}, boxWidth: 12 }} }},\n'
        f'      zoom: {{\n'
        f'        pan: {{\n'
        f'          enabled: true,\n'
        f'          mode: "x",\n'
        f'        }},\n'
        f'        zoom: {{\n'
        f'          wheel:  {{ enabled: true }},\n'
        f'          pinch:  {{ enabled: true }},\n'
        f'          drag:   {{ enabled: true, modifierKey: "shift", backgroundColor: "rgba(255,255,255,0.05)", borderColor: "rgba(255,255,255,0.3)", borderWidth: 1 }},\n'
        f'          mode: "x",\n'
        f'        }},\n'
        f'      }},\n'
        f'      ...extraPlugins,\n'
        f'    }},\n'
        f'    scales: {{\n'
        f'      x: {{\n'
        f'        ticks: {{ color: c.tick, font: {{ family: "DM Mono", size: 10 }}, maxTicksLimit: 10, maxRotation: 0 }},\n'
        f'        grid:  {{ color: c.grid }},\n'
        f'      }},\n'
        f'      y: {{\n'
        f'        ticks: {{ color: c.tick, font: {{ family: "DM Mono", size: 10 }}, beginAtZero: true }},\n'
        f'        grid:  {{ color: c.grid }},\n'
        f'      }},\n'
        f'    }},\n'
        f'  }};\n'
        f'}}\n'
        f'\n'
        f'// ── Chart registry ───────────────────────────────────────────────\n'
        f'const CHARTS = {{}};\n'
        f'\n'
        f'// ── Viewer chart ─────────────────────────────────────────────────\n'
        f'CHARTS.viewerChart = new Chart(document.getElementById("viewerChart"), {{\n'
        f'  type: "line",\n'
        f'  data: {{\n'
        f'    labels: ts,\n'
        f'    datasets: [{{\n'
        f'      label: "Concurrent Viewers",\n'
        f'      data: views,\n'
        f'      borderColor: orgColor,\n'
        f'      backgroundColor: orgColor + "18",\n'
        f'      ...LINE,\n'
        f'    }}],\n'
        f'  }},\n'
        f'  options: makeOpts({{}}),\n'
        f'}});\n'
        f'\n'
        f'// ── Engagement chart ─────────────────────────────────────────────\n'
        f'CHARTS.engagementChart = new Chart(document.getElementById("engagementChart"), {{\n'
        f'  type: "line",\n'
        f'  data: {{\n'
        f'    labels: ts,\n'
        f'    datasets: [\n'
        f'      {{ label: "Likes",    data: likes, borderColor: "#ff4f6d", backgroundColor: "rgba(255,79,109,0.06)",  ...LINE }},\n'
        f'      {{ label: "Comments", data: comms, borderColor: "#4fc3f7", backgroundColor: "rgba(79,195,247,0.06)", ...LINE }},\n'
        f'    ],\n'
        f'  }},\n'
        f'  options: makeOpts({{}}),\n'
        f'}});\n'
        f'\n'
        f'// ── Reset zoom ───────────────────────────────────────────────────\n'
        f'function resetZoom(id) {{\n'
        f'  const c = CHARTS[id];\n'
        f'  if (c) c.resetZoom();\n'
        f'}}\n'
        f'\n'
        f'// Attach double-click reset to both canvases\n'
        f'document.getElementById("viewerChart").addEventListener("dblclick", function() {{ resetZoom("viewerChart"); }});\n'
        f'document.getElementById("engagementChart").addEventListener("dblclick", function() {{ resetZoom("engagementChart"); }});\n'
        f'\n'
        f'// ── CSV download ─────────────────────────────────────────────────\n'
        f'function downloadCSV(id) {{\n'
        f'  const chart = CHARTS[id];\n'
        f'  if (!chart) return;\n'
        f'  const datasets = chart.data.datasets;\n'
        f'  const labels   = chart.data.labels;\n'
        f'  // Header row: Timestamp + one column per dataset\n'
        f'  const header = ["Timestamp", ...datasets.map(function(d) {{ return d.label; }})];\n'
        f'  // Data rows\n'
        f'  const rows = labels.map(function(lbl, i) {{\n'
        f'    return [lbl, ...datasets.map(function(d) {{ return d.data[i] ?? ""; }})]\n'
        f'      .map(function(v) {{ return String(v).includes(",") ? \'"\' + v + \'"\' : v; }})\n'
        f'      .join(",");\n'
        f'  }});\n'
        f'  const csv  = [header.join(","), ...rows].join("\\n");\n'
        f'  const blob = new Blob([csv], {{ type: "text/csv" }});\n'
        f'  const url  = URL.createObjectURL(blob);\n'
        f'  const a    = document.createElement("a");\n'
        f'  a.href     = url;\n'
        f'  a.download = VIDEO_ID + "_" + id + ".csv";\n'
        f'  document.body.appendChild(a);\n'
        f'  a.click();\n'
        f'  document.body.removeChild(a);\n'
        f'  URL.revokeObjectURL(url);\n'
        f'}}\n'
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

    # ── bulk-load schema cache (single query for all tables) ─────────────────
    all_table_names = [ch["table_name"] for ch in db_channels]
    _load_schema_cache(conn, all_table_names)

    # ── load manifest ─────────────────────────────────────────────────────────
    manifest = load_manifest()
    log.info("Manifest loaded — %d stream pages previously generated.", len(manifest))

    # ── resolve ORG_MAP channels → DB rows (with fallback by channel_id) ─────
    # resolved_channels: {ch_name: db_row}  (only channels found in DB)
    resolved_channels: dict[str, dict] = {}
    for org_slug, org in ORG_MAP.items():
        (OUTPUT_DIR / org_slug).mkdir(exist_ok=True)
        for entry in org["channels"]:
            ch_name = entry[0]
            if ch_name in resolved_channels:
                continue
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
            if db_row:
                resolved_channels[ch_name] = db_row

    # ── BULK fetch all stream summaries in ONE Postgres round-trip ────────────
    table_infos = [
        (ch_name, row["table_name"])
        for ch_name, row in resolved_channels.items()
        if _table_exists(conn, row["table_name"])
    ]
    log.info("Fetching stream summaries for %d channel tables in bulk…", len(table_infos))
    bulk_live: dict[str, list[dict]] = get_all_streams_bulk(conn, table_infos) if table_infos else {}

    # ── BULK fetch all archived streams in ONE SQLite round-trip ─────────────
    all_ch_names = list(resolved_channels.keys())
    bulk_archived: dict[str, list[dict]] = (
        get_all_archived_streams(hist, all_ch_names) if hist else {}
    )

    # ── merge live + archived per channel ─────────────────────────────────────
    all_streams_by_channel: dict[str, list[dict]] = {}
    stream_counts: dict[str, int] = {}
    total_streams  = 0
    total_channels = 0

    for ch_name in resolved_channels:
        live_streams = bulk_live.get(ch_name, [])
        live_ids     = {s["video_id"] for s in live_streams}
        archived     = [s for s in bulk_archived.get(ch_name, [])
                        if s["video_id"] not in live_ids]
        merged = list(live_streams) + archived
        all_streams_by_channel[ch_name] = merged
        stream_counts[ch_name]          = len(merged)
        total_channels += 1
        total_streams  += len(merged)

    # channels not found in DB still need an entry for org page stream counts
    for org in ORG_MAP.values():
        for entry in org["channels"]:
            ch_name = entry[0]
            if ch_name not in stream_counts:
                stream_counts[ch_name] = 0
            if ch_name not in all_streams_by_channel:
                all_streams_by_channel[ch_name] = []

    log.info("DB query complete — %d streams across %d channels.", total_streams, total_channels)

    # ── diff: determine which stream pages need (re)generating ────────────────
    dirty_video_ids: set[str] = set()
    dirty_channels:  set[str] = set()
    dirty_orgs:      set[str] = set()

    for ch_name, streams in all_streams_by_channel.items():
        for stream in streams:
            vid         = stream["video_id"]
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

    # ── build work list for dirty stream pages ────────────────────────────────
    # Each item: (org_slug, org, ch_name, db_row_table, stream)
    dirty_work: list[tuple] = []
    for org_slug, org in ORG_MAP.items():
        for entry in org["channels"]:
            ch_name = entry[0]
            db_row  = resolved_channels.get(ch_name)
            if not db_row:
                continue
            table   = db_row["table_name"]
            for stream in all_streams_by_channel.get(ch_name, []):
                if stream["video_id"] in dirty_video_ids:
                    dirty_work.append((org_slug, org, ch_name, table, stream))

    # Capture a single timestamp for all manifest entries written this run
    run_ts = _now_local().strftime("%Y-%m-%d %H:%M WIB")

    # ── generate dirty stream pages (parallel) ────────────────────────────────
    # Both psycopg2 and sqlite3 connections are NOT thread-safe — they must not
    # be shared across threads.  Each worker opens its own short-lived
    # connections and closes them before returning.
    def _write_one_stream(args):
        org_slug, org, ch_name, table, stream = args
        t_conn = get_conn()
        t_hist = get_history_conn()
        try:
            enriched, ts = _enrich_stream(stream, t_conn, table, t_hist)
            write_stream_page(org_slug, org, ch_name, enriched, ts)
            return enriched["video_id"], {
                "org_slug":     org_slug,
                "ch_slug":      slugify(ch_name),
                "ch_name":      ch_name,
                "status":       enriched.get("stream_status") or "vod",
                "generated_at": run_ts,
            }
        finally:
            t_conn.close()
            if t_hist:
                t_hist.close()

    # _enrich_stream issues DB queries; use threads so the GIL releases during
    # network I/O and multiple timeseries fetches overlap.
    max_workers = min(8, max(1, len(dirty_work)))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_write_one_stream, item) for item in dirty_work]
        for fut in as_completed(futures):
            try:
                vid, entry = fut.result()
                manifest[vid] = entry
            except Exception as exc:
                log.error("Stream page generation failed: %s", exc)

    # ── regenerate channel pages (parallel) ──────────────────────────────────
    channel_write_args = []
    for org_slug, org in ORG_MAP.items():
        for entry in org["channels"]:
            ch_name = entry[0]
            if not resolved_channels.get(ch_name):
                continue
            streams  = all_streams_by_channel.get(ch_name, [])
            channel_write_args.append(
                (org_slug, org, ch_name, streams, logos, channel_ids_map, subscribers)
            )

    def _write_channel(args):
        write_channel_page(*args)

    with ThreadPoolExecutor(max_workers=min(8, max(1, len(channel_write_args)))) as pool:
        futs = [pool.submit(_write_channel, a) for a in channel_write_args]
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception as exc:
                log.error("Channel page generation failed: %s", exc)

    channels_written = len(channel_write_args)
    log.info("Channel pages written: %d", channels_written)

    # ── regenerate org pages (parallel) ──────────────────────────────────────
    def _write_org(args):
        write_org_page(*args)

    org_write_args = [
        (org_slug, org, stream_counts, logos, channel_ids_map, subscribers,
         all_streams_by_channel)
        for org_slug, org in ORG_MAP.items()
    ]
    with ThreadPoolExecutor(max_workers=min(8, len(org_write_args))) as pool:
        futs = [pool.submit(_write_org, a) for a in org_write_args]
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception as exc:
                log.error("Org page generation failed: %s", exc)

    log.info("Org pages written: %d", len(ORG_MAP))

    # ── always regenerate index ───────────────────────────────────────────────
    generated_at = _now_local().strftime("%Y-%m-%d %H:%M WIB")
    write_index(total_streams, total_channels, generated_at,
                stream_counts, all_streams_by_channel)

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
