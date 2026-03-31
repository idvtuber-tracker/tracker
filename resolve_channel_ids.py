"""
resolve_channel_ids.py
──────────────────────
Resolves YouTube Channel IDs for all new org channels using the YouTube Data API.
Uses the `search.list` endpoint to look up each channel by name.

Usage:
    python resolve_channel_ids.py

Requires:
    YOUTUBE_API_KEY or YOUTUBE_API_KEYS environment variable (same as tracker.py)

Output:
    Prints ORG_MAP-ready entries with channel IDs for copy-pasting into generate_dashboard.py
    Also saves results to resolved_channels.json for reference.
"""

import os
import json
import time
import googleapiclient.discovery

# ── API setup ─────────────────────────────────────────────────────────────────

def get_api_key():
    keys = os.environ.get("YOUTUBE_API_KEYS", "") or os.environ.get("YOUTUBE_API_KEY", "")
    if not keys:
        raise SystemExit("ERROR: Set YOUTUBE_API_KEY or YOUTUBE_API_KEYS env var.")
    return keys.split(",")[0].strip()


def build_youtube(api_key):
    return googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)


# ── Channels to resolve ───────────────────────────────────────────────────────

ORGS = {
    "yorukaze": {
        "label": "Yorukaze Production",
        "color": "#7ec8e3",
        "desc": "An Indonesian VTuber organization serving as a bridge and support platform for virtual content creators.",
        "channels": [
            ("Yorukaze Production",             "org"),
            ("Hessa Elainore Ch.【Yorukaze】",   "talent"),
            ("Tsukiyo Miho Ch.【Yorukaze】",     "talent"),
            ("Mihiro Kamigawa【Yorukaze】",       "talent"),
            ("Vincent Cerbero【Yorukaze】",       "talent"),
            ("Utahime Yukari Ch.【Yorukaze】",    "talent"),
            ("Nanaka Poi Ch. 【Yorukaze】",       "talent"),
            ("Amare Michiya【Yorukaze】",         "talent"),
            ("Hoshikawa Rui【Yorukaze】",         "talent"),
            ("Yuzumi_Ch【Yorukaze】",             "talent"),
            ("Ellise Youka【Yorukaze】",          "talent"),
            ("WanTaps Ch.【Yorukaze】",           "talent"),
            ("Wintergea Ch. ゲア 【Yorukaze】",   "talent"),
        ],
    },
    "prism-nova": {
        "label": "Prism:NOVA",
        "color": "#c084fc",
        "desc": "An Indonesian VTuber agency focused on characterisation, storytelling, and roleplaying.",
        "channels": [
            ("Prism:NOVA",                  "org"),
            ("Oxa Lydea 【Prism:NOVA】",     "talent"),
            ("Serika Cosmica 【Prism:NOVA】","talent"),
            ("Thalia Symphonia 【Prism:NOVA】","talent"),
        ],
    },
    "vcosmix": {
        "label": "VCosmix",
        "color": "#f472b6",
        "desc": "An Indonesian VTuber group.",
        "channels": [
            ("Vcosmix",         "org"),
            ("Lea Lestari Ch.", "talent"),
            ("Miichan Chu Ch.", "talent"),
            ("Li Mingshu Ch.",  "talent"),
        ],
    },
    "dexter": {
        "label": "DEXTER",
        "color": "#f87171",
        "desc": "An Indonesian VTuber agency.",
        "channels": [
            ("Dexter Official",              "org"),
            ("Richard Ravindra【DEXTER】",    "talent"),
            ("Rex Arcadia【DEXTER】",         "talent"),
            ("Lucentia【DEXTER】",            "talent"),
            ("Noa Florastra【DEXTER】",       "talent"),
        ],
    },
    "cozycazt": {
        "label": "CozyCazt",
        "color": "#fb923c",
        "desc": "An Indonesian VTuber agency.",
        "channels": [
            ("Cozy Cazt",                        "org"),
            ("Rannia Taiga 【CozyCazt】",         "talent"),
            ("Lyta Luciana Ch.【CozyCazt】",      "talent"),
            ("Arphina Stellaria【CozyCazt】",     "talent"),
            ("Vianna Risendria 【CozyCazt】",     "talent"),
            ("Fuyo Mafuyu【CozyCazt】",           "talent"),
            ("Silveryshore Ch.【CozyCazt】",      "talent"),
        ],
    },
    "afterain": {
        "label": "AfteRain",
        "color": "#60a5fa",
        "desc": "An Indonesian VTuber agency.",
        "channels": [
            ("AFTERAIN PROJECT",             "org"),
            ("LynShuu 【AFTERAIN】",          "talent"),
            ("Nezufu Senshirou【AFTERAIN】",  "talent"),
            ("Lvna Tylthia【AFTERAIN】",      "talent"),
            ("Poffie Hunni【AFTERAIN】",      "talent"),
            ("Flein Ryst【AFTERAIN】",        "talent"),
            ("Avy Inkaiserin 【AFTERAIN】",   "talent"),
            ("Kana Chizu 【AFTERAIN】",       "talent"),
            ("Ririna Ruu【AFTERAIN】",        "talent"),
        ],
    },
    # ── NEW ORGS ──────────────────────────────────────────────────────────────
    "eon-of-stars": {
        "label": "Eon of Stars",
        "color": "#818cf8",
        "desc": "An Indonesian indie VTuber group formed by former AKA Virtual SOL4CE members Harris Caine, Gingitsune Gehenna, Souta Izumi, and Mikazuki Arion. Debuted January 2026.",
        "channels": [
            ("Eon of Stars",             "org"),
            ("Harris Caine【EOS】",        "talent"),
            ("Gingitsune Gehenna【EOS】",  "talent"),
            ("Souta【EOS】",               "talent"),
            ("Mikazuki Arion【EOS】",      "talent"),
        ],
    },
    "magniv": {
        "label": "MagniV",
        "color": "#a855f7",
        "desc": "An Indonesian VTuber idol group under AKA Virtual, debuted April 2025. Concept: 'Five as one, we shine.'",
        "channels": [
            ("MagniV【AKA Virtual】",         "org"),
            ("Gema Gathika【AKA Virtual】",   "talent"),
            ("Istmodius【AKA Virtual】",      "talent"),
            ("Mosa【AKA Virtual】",           "talent"),
            ("Funin Mamori【AKA Virtual】",   "talent"),
        ],
    },
    "sandaiva": {
        "label": "SANDAiVA",
        "color": "#e879f9",
        "desc": "A three-member Indonesian VTuber idol unit under AKA Virtual, debuted February 2025.",
        "channels": [
            ("SANDAiVA【AKA Virtual】",            "org"),
            ("Kirigaya Raveanne【AKA Virtual】",   "talent"),
            ("Njess【AKA Virtual】",               "talent"),
            ("Quiver Rannette【AKA Virtual】",     "talent"),
        ],
    },
    "versa": {
        "label": "VERSA",
        "color": "#38bdf8",
        "desc": "An Indonesian VTuber boyband group under AKA Virtual, debuted October 2025.",
        "channels": [
            ("VERSA【AKA Virtual】",          "org"),
            ("Agata Seven【AKA Virtual】",    "talent"),
            ("Alarich【AKA Virtual】",        "talent"),
            ("Eray Ryuki【AKA Virtual】",     "talent"),
            ("Ryoutaa【AKA Virtual】",        "talent"),
            ("SouRizu【AKA Virtual】",        "talent"),
        ],
    },
    "jkt48v": {
        "label": "JKT48V",
        "color": "#f59e0b",
        "desc": "The virtual idol sub-unit of JKT48, Indonesia's iconic idol group, produced by AKA Virtual. Concept: 'Your Idol Comes Virtual.'",
        "channels": [
            ("JKT48V",              "org"),
            ("Pia Meraleo【JKT48V】","talent"),
            ("Tana Nona【JKT48V】",  "talent"),
            ("Sami Maono【JKT48V】", "talent"),
            ("Isha Kirana【JKT48V】","talent"),
            ("Maura Nilambari【JKT48V】", "talent"),
        ],
    },
    "maha5": {
        "label": "MAHA5",
        "color": "#34d399",
        "desc": "An Indonesian VTuber agency (Mahapanca) under Rentracks Indonesia, connecting Indonesia and Japan through anime and otaku culture.",
        "channels": [
            ("MAHA5",                    "org"),
            ("Kevin Vangardo【MAHA5】",   "talent"),
            ("Rena Anggraeni【MAHA5】",   "talent"),
            ("Hera Garalea【MAHA5】",     "talent"),
            ("Daisy Ignacia Y【MAHA5】",  "talent"),
            ("Saku Kurata【MAHA5】",      "talent"),
            ("Maudy Sukaiga【MAHA5】",    "talent"),
            ("Fuyumi Celestia【MAHA5】",  "talent"),
        ],
    },
    "rememories": {
        "label": "Re:Memories",
        "color": "#fb7185",
        "desc": "An Indonesian indie VTuber agency focused on two-way interactive entertainment and fostering real connections with their community.",
        "channels": [
            ("Re:Memories",                  "org"),
            ("Chloe Pawapua『Re:Memories』",  "talent"),
            ("Lily Ifeta『Re:Memories』",     "talent"),
            ("Reynard Blanc『Re:Memories』",  "talent"),
            ("Elaine Celestia『Re:Memories』","talent"),
            ("Cecilia Lieberia『Re:Memories』","talent"),
            ("Marin Goldlock『Re:Memories』", "talent"),
            ("Izanami Chiara『Re:Memories』", "talent"),
            ("Leo Axenos『Re:Memories』",     "talent"),
            ("Pinku Rimu『Re:Memories』",     "talent"),
        ],
    },
}

# Known handles to try first — avoids ambiguous search results for channels
# with common names. Format: channel_name → @handle
KNOWN_HANDLES = {
    # ── Yorukaze ──────────────────────────────────────────────────────────────
    "Yorukaze Production":              "@YorukazeProduction",
    "Hessa Elainore Ch.【Yorukaze】":   "@HessaElainore",
    "Tsukiyo Miho Ch.【Yorukaze】":     "@TsukiyoMiho",
    "Mihiro Kamigawa【Yorukaze】":      "@MihiroKamigawa",
    "Vincent Cerbero【Yorukaze】":      "@VincentCerbero",
    "Utahime Yukari Ch.【Yorukaze】":   "@utahimeyukari",
    "Nanaka Poi Ch. 【Yorukaze】":      "@NanakaPoi",
    "Amare Michiya【Yorukaze】":        "@AmareMichiya",
    "Hoshikawa Rui【Yorukaze】":        "@HoshikawaRui",
    "Yuzumi_Ch【Yorukaze】":            "@Yuzumi_Ch",
    "Ellise Youka【Yorukaze】":         "@Elliseeyou",
    "WanTaps Ch.【Yorukaze】":          "@WanTapsCh",
    "Wintergea Ch. ゲア 【Yorukaze】":  "@WintergeaCh",
    # ── Prism:NOVA ────────────────────────────────────────────────────────────
    "Prism:NOVA":                       "@PrismNOVA",
    "Oxa Lydea 【Prism:NOVA】":         "@OxaLydea",
    "Serika Cosmica 【Prism:NOVA】":    "@SerikaCosmica",
    "Thalia Symphonia 【Prism:NOVA】":  "@ThaliaSymphonia",
    # ── VCosmix ───────────────────────────────────────────────────────────────
    "Vcosmix":                          "@VCosmixOfficial",
    "Lea Lestari Ch.":                  "@LeaLestariCh",
    "Miichan Chu Ch.":                  "@MiichanChuCh",
    "Li Mingshu Ch.":                   "@LiMingshuCh",
    # ── DEXTER ────────────────────────────────────────────────────────────────
    "Dexter Official":                  "@DexterOfficial",
    "Richard Ravindra【DEXTER】":       "@RichardRavindra",
    "Rex Arcadia【DEXTER】":            "@RexArcadia",
    "Lucentia【DEXTER】":               "@Lucentia",
    "Noa Florastra【DEXTER】":          "@NoaFlorastra",
    # ── CozyCazt ──────────────────────────────────────────────────────────────
    "Cozy Cazt":                        "@CozyCazt",
    "Rannia Taiga 【CozyCazt】":        "@RanniaTaiga",
    "Lyta Luciana Ch.【CozyCazt】":     "@LytaLuciana",
    "Arphina Stellaria【CozyCazt】":    "@ArphinaStellaria",
    "Vianna Risendria 【CozyCazt】":    "@ViannaRisendria",
    "Fuyo Mafuyu【CozyCazt】":          "@FuyoMafuyu",
    "Silveryshore Ch.【CozyCazt】":     "@SilveryshoreVT",
    # ── AfteRain ──────────────────────────────────────────────────────────────
    "AFTERAIN PROJECT":                 "@AFTERAINPROJECT",
    "LynShuu 【AFTERAIN】":             "@LynShuu",
    "Nezufu Senshirou【AFTERAIN】":     "@NezufuSenshirou",
    "Lvna Tylthia【AFTERAIN】":         "@LvnaTylthia",
    "Poffie Hunni【AFTERAIN】":         "@PoffieHunni",
    "Flein Ryst【AFTERAIN】":           "@FleinRyst",
    "Avy Inkaiserin 【AFTERAIN】":      "@AvyInkaiserin",
    "Kana Chizu 【AFTERAIN】":          "@KanaChizu",
    "Ririna Ruu【AFTERAIN】":           "@RirinaRuu",
    # ── Eon of Stars ─────────────────────────────────────────────────────────
    "Eon of Stars":                     "@EonOfStars",
    "Harris Caine【EOS】":               "@HarrisCaine",
    "Gingitsune Gehenna【EOS】":         "@gingehenna",
    "Souta【EOS】":                      "@xsoutaa",
    "Mikazuki Arion【EOS】":             "@MikazukiArion",
    # ── MagniV ────────────────────────────────────────────────────────────────
    "MagniV【AKA Virtual】":            "@wearemagniv",
    "Gema Gathika【AKA Virtual】":      "@GemaGathika",
    "Istmodius【AKA Virtual】":         "@IstmodiusCh",      # youtube.com/c/IstmodiusCh
    "Mosa【AKA Virtual】":              "@garrammosaurus",   # Twitter: @garrammosaurus
    "Funin Mamori【AKA Virtual】":      "@FuninVT_id",       # Twitter: @FuninVT_id
    # ── SANDAiVA ──────────────────────────────────────────────────────────────
    "SANDAiVA【AKA Virtual】":          "@3DAiVAOfficial",
    "Kirigaya Raveanne【AKA Virtual】": "@raveanne_",
    "Njess【AKA Virtual】":             "@njejessie",
    "Quiver Rannette【AKA Virtual】":   "@Quiver_R011",
    # ── VERSA ─────────────────────────────────────────────────────────────────
    "VERSA【AKA Virtual】":             "@versa_aka",
    "Agata Seven【AKA Virtual】":       "@Agata_Seven",
    "Alarich【AKA Virtual】":           "@alarichalberich",
    "Eray Ryuki【AKA Virtual】":        "@ErayRyuki",
    "Ryoutaa【AKA Virtual】":           "@UtaaRyo",
    "SouRizu【AKA Virtual】":           "@SouRizu",
    # ── JKT48V ────────────────────────────────────────────────────────────────
    "JKT48V":                           "@jkt48v_official",
    "Pia Meraleo【JKT48V】":            "@PiaMeraleo",
    "Tana Nona【JKT48V】":              "@TanaNona",
    "Sami Maono【JKT48V】":             "@SamiJkt48v",
    "Isha Kirana【JKT48V】":            "@IshaJkt48v",
    "Maura Nilambari【JKT48V】":        "@Maura_jkt48v",
    # ── MAHA5 ─────────────────────────────────────────────────────────────────
    "MAHA5":                            "@maha5official",
    "Kevin Vangardo【MAHA5】":          "@KevinVangardo",
    "Rena Anggraeni【MAHA5】":          "@RenaAnggraeni",
    "Hera Garalea【MAHA5】":            "@HeraGaraleaCh",
    "Daisy Ignacia Y【MAHA5】":         "@DaisyIgnaciaY",
    "Saku Kurata【MAHA5】":             "@SakuKurata",
    "Maudy Sukaiga【MAHA5】":           "@MaudySukaiga",
    "Fuyumi Celestia【MAHA5】":         "@FuyumiCelestia",
    # ── Re:Memories ───────────────────────────────────────────────────────────
    "Re:Memories":                          "@RememoriesOfficial",
    "Chloe Pawapua『Re:Memories』":         "@ChloePawapua",
    "Lily Ifeta『Re:Memories』":            "@LilyIfeta",
    "Reynard Blanc『Re:Memories』":         "@ReynardBlanc",
    "Elaine Celestia『Re:Memories』":       "@ElaineCelestia",
    "Cecilia Lieberia『Re:Memories』":      "@CeciliaLieberia",
    "Marin Goldlock『Re:Memories』":        "@MarinGoldlock",
    "Izanami Chiara『Re:Memories』":        "@IzanamiChiara",
    "Leo Axenos『Re:Memories』":            "@LeoAxenos",
    "Pinku Rimu『Re:Memories』":            "@PinkuRimu",
}


# ── Resolver ──────────────────────────────────────────────────────────────────

def resolve_by_handle(yt, handle: str) -> str | None:
    """Resolve a @handle to a channel ID using forHandle parameter."""
    try:
        resp = yt.channels().list(
            part="id,snippet",
            forHandle=handle.lstrip("@"),
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0]["id"]
    except Exception as e:
        print(f"    [handle lookup failed: {e}]")
    return None


def resolve_by_search(yt, name: str) -> str | None:
    """Fall back to search.list if handle lookup fails."""
    try:
        resp = yt.search().list(
            part="snippet",
            q=name,
            type="channel",
            maxResults=1,
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0]["snippet"]["channelId"]
    except Exception as e:
        print(f"    [search lookup failed: {e}]")
    return None


def resolve_all(yt) -> dict:
    results = {}
    total = sum(len(org["channels"]) for org in ORGS.values())
    done = 0

    for org_slug, org in ORGS.items():
        for ch_name, ch_type in org["channels"]:
            done += 1
            print(f"[{done}/{total}] Resolving: {ch_name}")

            channel_id = None

            # Try handle first (more precise, cheaper — uses channels.list not search.list)
            handle = KNOWN_HANDLES.get(ch_name)
            if handle:
                channel_id = resolve_by_handle(yt, handle)
                if channel_id:
                    print(f"    ✓ {channel_id} (via handle {handle})")

            # Fall back to search
            if not channel_id:
                channel_id = resolve_by_search(yt, ch_name)
                if channel_id:
                    print(f"    ✓ {channel_id} (via search)")
                else:
                    print(f"    ✗ NOT FOUND — fill in manually")
                    channel_id = "UNKNOWN"

            results[ch_name] = {
                "org_slug": org_slug,
                "type": ch_type,
                "channel_id": channel_id,
            }

            # Avoid hitting quota too fast
            time.sleep(0.2)

    return results


# ── Output ────────────────────────────────────────────────────────────────────

def print_org_map(results: dict):
    print("\n\n" + "=" * 70)
    print("ORG_MAP entries — copy into generate_dashboard.py")
    print("=" * 70)

    for org_slug, org in ORGS.items():
        print(f'\n    "{org_slug}": {{')
        print(f'        "label":   "{org["label"]}",')
        print(f'        "color":   "{org["color"]}",')
        print(f'        "desc":    "{org["desc"]}",')
        print(f'        "channels": [')
        for ch_name, ch_type in org["channels"]:
            cid = results.get(ch_name, {}).get("channel_id", "UNKNOWN")
            flag = "  # ← FILL IN MANUALLY" if cid == "UNKNOWN" else ""
            print(f'            ("{ch_name}", "{ch_type}", "{cid}"),{flag}')
        print(f'        ],')
        print(f'    }},')


def save_json(results: dict):
    path = "resolved_channels.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nResults saved to {path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    api_key = get_api_key()
    yt = build_youtube(api_key)
    print(f"Resolving {sum(len(o['channels']) for o in ORGS.values())} channels across {len(ORGS)} orgs...\n")
    results = resolve_all(yt)
    print_org_map(results)
    save_json(results)


if __name__ == "__main__":
    main()
