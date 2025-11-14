# fpl_bot.py
# Single-file Telegram bot — secure single-file implementation (senior-level)
# Requirements: python 3.10+, python-telegram-bot v20+, httpx, beautifulsoup4
# Persisted files: /mnt/data/...
# All sensitive IDs/tokens come from environment

import os
import asyncio
import json
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple, Set, Any
import logging

import httpx
from bs4 import BeautifulSoup

from telegram import __version__ as PTB_VERSION
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# -------------------------
# CONFIG FROM ENV
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
# Owner who always may use private chat (int)
OWNER_ID = int(os.getenv("OWNER_ID", "469807230"))
# Allowed group where bot is active and where notifications are sent (int)
ALLOWED_GROUP_ID = int(os.getenv("ALLOWED_GROUP_ID", "4973694653"))
# League ID used for league-based ordering
LEAGUE_ID = int(os.getenv("LEAGUE_ID", "980121"))

DATA_DIR = "/mnt/data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# persisted file paths
USER_TZ_FILE = os.path.join(DATA_DIR, "user_timezones.json")
LAST_SENT_FILE = os.path.join(DATA_DIR, "user_last_sent.json")
PRICES_BASELINE_FILE = os.path.join(DATA_DIR, "prices_baseline.json")
LEAGUE_CACHE_FILE = os.path.join(DATA_DIR, "league_player_set.json")
NOTIF_FLAG_FILE = os.path.join(DATA_DIR, "notifications_enabled.json")
ALLOWED_USERS_FILE = os.path.join(DATA_DIR, "allowed_users.json")

# FPL endpoints
FPL_BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
LIVEFPL_PRICES = "https://www.livefpl.net/prices"

# Scheduling constants
PRICE_CHANGE_UTC_PLUS = 5  # prices change at 06:00 UTC+5
PRICE_CHANGE_WINDOW_START = time(5, 45)  # UTC+5 05:45
PRICE_CHANGE_WINDOW_END = time(8, 0)     # UTC+5 08:00
PRICE_CHANGE_POLL_SECONDS = 5 * 60       # 5 minutes

EVENING_HOUR_UTC5 = 23
EVENING_MINUTE_UTC5 = 0

# GW disable target
GW_DISABLE_TARGET = 38

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fpl_bot")

# -------------------------
# Persistence helpers
# -------------------------
def _load_json(path: str, default):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        logger.exception("Failed to load %s", path)
    return default

def _save_json(path: str, data):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        logger.exception("Failed to save %s", path)

# persisted runtime structures
user_timezones: Dict[int, str] = {int(k): v for k, v in _load_json(USER_TZ_FILE, {}).items()}
user_last_sent: Dict[str, Dict[str, str]] = _load_json(LAST_SENT_FILE, {})
prices_baseline: Dict[str, int] = _load_json(PRICES_BASELINE_FILE, {})
league_cache: Dict[str, Any] = _load_json(LEAGUE_CACHE_FILE, {})
notif_flag_obj = _load_json(NOTIF_FLAG_FILE, {"enabled": True})
_allowed_users: Set[int] = set(int(x) for x in _load_json(ALLOWED_USERS_FILE, []))

def _save_allowed_users():
    try:
        _save_json(ALLOWED_USERS_FILE, sorted(list(_allowed_users)))
    except Exception:
        logger.exception("Failed to save allowed users")

def notifications_enabled() -> bool:
    return bool(notif_flag_obj.get("enabled", True))

def set_notifications_enabled(enabled: bool):
    notif_flag_obj["enabled"] = bool(enabled)
    _save_json(NOTIF_FLAG_FILE, notif_flag_obj)

# -------------------------
# HTTP client
# -------------------------
_http_client = httpx.AsyncClient(timeout=20.0, headers={"User-Agent": "FPL-Bot/1.0"})

async def fetch_json(url: str) -> Optional[dict]:
    try:
        r = await _http_client.get(url)
        if r.status_code == 200:
            return r.json()
        logger.warning("fetch_json %s -> status %s", url, r.status_code)
    except Exception:
        logger.exception("fetch_json failed %s", url)
    return None

async def fetch_text(url: str) -> Optional[str]:
    try:
        r = await _http_client.get(url)
        if r.status_code == 200:
            return r.text
        logger.warning("fetch_text %s -> %s", url, r.status_code)
    except Exception:
        logger.exception("fetch_text failed %s", url)
    return None

# -------------------------
# FPL team mapping (team_code -> abbreviation)
# -------------------------
FPL_TEAM_ABBR = {
    1:  "ARS", 2:  "AVL", 3:  "BOU", 4:  "BRE", 5:  "BHA",
    6:  "BUR", 7:  "CHE", 8:  "CRY", 9:  "EVE", 10: "FUL",
    11: "LIV", 12: "LUT", 13: "MCI", 14: "MUN", 15: "NEW",
    16: "NFO", 17: "SHU", 18: "TOT", 19: "WHU", 20: "WOL",
}

# -------------------------
# TZ helpers
# -------------------------
DEFAULT_TZ_STR = f"UTC+{PRICE_CHANGE_UTC_PLUS}"

def parse_tz_input(tz_input: str) -> Optional[str]:
    if not tz_input:
        return None
    s = tz_input.strip()
    if (s.startswith("+") or s.startswith("-")) and s[1:].replace(".", "", 1).isdigit():
        return f"UTC{s}"
    if s.upper().startswith("UTC") and (len(s) == 3 or (len(s) > 3 and s[3] in "+-")):
        return s.upper()
    try:
        ZoneInfo(s)
        return s
    except Exception:
        return None

def tz_to_zone(tz_str: str):
    s = tz_str
    if s.upper().startswith("UTC") and (len(s) == 3 or (len(s) > 3 and s[3] in "+-")):
        if len(s) == 3:
            return timezone.utc
        offset = int(s[3:])
        return timezone(timedelta(hours=offset))
    return ZoneInfo(s)

def local_now(tz_str: str) -> datetime:
    zone = tz_to_zone(tz_str)
    return datetime.now(timezone.utc).astimezone(zone)

# -------------------------
# Parsing LiveFPL helpers
# -------------------------
def parse_percent(s: str) -> float:
    try:
        if s is None:
            return 0.0
        s2 = str(s).replace("%", "").strip()
        return float(s2) if s2 != "" else 0.0
    except Exception:
        return 0.0

def parse_row_cells(cells: List[str]) -> Dict[str, str]:
    row = {}
    if len(cells) >= 1:
        row["Name"] = cells[0]
    if len(cells) >= 2:
        row["Pos"] = cells[1]
    if len(cells) >= 3:
        row["Team"] = cells[2]
    for c in cells[3:]:
        if "£" in c or c.replace(".", "", 1).isdigit():
            row["Price"] = c.replace("£", "").strip()
            break
    for c in reversed(cells):
        if "%" in c and "Target" not in row:
            row["Target"] = c.strip()
        elif "%" in c and "Owned by" not in row:
            row["Owned by"] = c.strip()
    row.setdefault("Price", "")
    row.setdefault("Target", "")
    row.setdefault("Owned by", "")
    return row

def extract_table(html_soup: BeautifulSoup, key_hint: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    possible = html_soup.find_all(["h3", "h4", "h2", "strong"])
    found = None
    for p in possible:
        txt = p.get_text(" ", strip=True).lower()
        if key_hint.lower() in txt:
            found = p
            break
    if not found:
        for p in possible:
            txt = p.get_text(" ", strip=True).lower()
            if any(w in txt for w in key_hint.lower().split()):
                found = p
                break
    if not found:
        return rows
    tbl = found.find_next("table")
    if not tbl:
        for li in found.find_next_siblings("li"):
            txt = li.get_text(" ", strip=True)
            cells = [txt]
            rows.append(parse_row_cells(cells))
        return rows
    for tr in tbl.find_all("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if not tds or len(tds) < 1:
            continue
        row = parse_row_cells(tds)
        rows.append(row)
    return rows

# -------------------------
# League cache and sorting/filtering
# -------------------------
async def get_current_gw() -> int:
    data = await fetch_json(FPL_BOOTSTRAP)
    if not data:
        return 0
    for ev in data.get("events", []) or []:
        if ev.get("is_current"):
            return int(ev.get("id", 0))
    return 0

async def fetch_league_player_set_once(league_id: int, cap: int = 80) -> Set[str]:
    out: Set[str] = set()
    url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"
    data = await fetch_json(url)
    if not data:
        return out
    results = data.get("standings", {}).get("results") or data.get("results") or data.get("entries") or []
    entry_ids = []
    for r in results:
        eid = r.get("entry") or r.get("id") or r.get("entry_id")
        if eid:
            entry_ids.append(int(eid))
    entry_ids = entry_ids[:cap]
    sem = asyncio.Semaphore(8)
    async def fetch_entry(eid: int):
        async with sem:
            try:
                url_e = f"https://fantasy.premierleague.com/api/entry/{eid}/event/0/picks/"
                j = await fetch_json(url_e)
                if not j:
                    return
                picks = j.get("picks", []) or []
                for p in picks:
                    el = p.get("element")
                    if el:
                        out.add(f"id:{int(el)}")
            except Exception:
                pass
    tasks = [asyncio.create_task(fetch_entry(e)) for e in entry_ids]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    return out

async def get_league_player_set_cached(league_id: int) -> Set[str]:
    gw = await get_current_gw()
    gw_saved = int(league_cache.get("gw", -1))
    if gw_saved == gw and league_cache.get("set"):
        return set(league_cache.get("set", []))
    s = await fetch_league_player_set_once(league_id)
    league_cache["gw"] = gw
    league_cache["set"] = list(s)
    _save_json(LEAGUE_CACHE_FILE, league_cache)
    return s

def is_in_league_row(r: Dict[str, Any], league_set: Set[str]) -> int:
    name = (r.get("Name") or "").strip().lower()
    if not name:
        return 0
    if name in (n.lower() for n in league_set):
        return 1
    eid = r.get("id") or r.get("Element") or r.get("element")
    try:
        if eid and f"id:{int(eid)}" in league_set:
            return 1
    except Exception:
        pass
    return 0

def sort_and_filter_sections(sections: Dict[str, List[Dict[str, Any]]], league_set: Set[str]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for title, rows in sections.items():
        processed = []
        for r in rows:
            owned_val = parse_percent(r.get("Owned by", "0"))
            if owned_val < 1.0:
                continue
            target_val = parse_percent(r.get("Target", "0"))
            in_league = is_in_league_row(r, league_set)
            r["_target_val"] = target_val
            r["_in_league"] = in_league
            r["_owned_val"] = owned_val
            if "_direction" not in r:
                r["_direction"] = "rise" if target_val > 0 else "fall" if target_val < 0 else "neutral"
            dir_pr = 1 if r["_direction"] != "fall" else 0
            r["_dir_pr"] = dir_pr
            processed.append(r)
        processed.sort(key=lambda x: (x["_dir_pr"], x["_target_val"], x["_in_league"], x["_owned_val"]), reverse=True)
        for rr in processed:
            for k in ("_target_val", "_in_league", "_owned_val", "_dir_pr"):
                rr.pop(k, None)
        out[title] = processed
    return out

# -------------------------
# Formatting compact table (ABBR + name)
# -------------------------
def center_text(s: str, width: int = 40) -> str:
    return s.center(width)

def format_prices_compact(sections: Dict[str, List[Dict[str, Any]]], el_map: Dict[int, dict]) -> str:
    order = ["Already reached target", "Projected to reach target", "Others who will be close"]
    blocks: List[str] = []
    for title in order:
        rows = sections.get(title, []) or []
        names = [r.get("Name", "") for r in rows]
        prices = [f"£{r.get('Price')}".strip() for r in rows]
        targets = []
        for r in rows:
            t = r.get("Target", "")
            tval = int(parse_percent(t))
            if r.get("_direction", "neutral") == "fall":
                targets.append(f"-{abs(int(tval))}%")
            else:
                targets.append(f"{int(tval)}%")
        max_name = max((len(x) for x in names), default=0)
        max_price = max((len(x) for x in prices), default=0)
        max_target = max((len(x) for x in targets), default=0)
        header = center_text(title, 40)
        block_lines = [header]
        for i, r in enumerate(rows):
            team_abbr = "UNK"
            el_id = None
            for k in ("element", "id", "Element"):
                try:
                    if k in r:
                        el_id = int(r[k])
                        break
                except Exception:
                    pass
            if el_id and el_id in el_map:
                tc = el_map[el_id].get("team_code") or el_map[el_id].get("team")
                try:
                    team_abbr = FPL_TEAM_ABBR.get(int(tc), "UNK")
                except Exception:
                    team_abbr = FPL_TEAM_ABBR.get(tc, "UNK")
            else:
                tcol = (r.get("Team") or "").strip()
                if tcol:
                    team_abbr = tcol.upper()
            name = (r.get("Name") or "").ljust(max_name)
            p = prices[i].ljust(max_price)
            tgt = targets[i].rjust(max_target)
            block_lines.append(f"{team_abbr}  {name}  {p}  ({tgt})")
        if not rows:
            block_lines.append("(none)")
        blocks.append("<code>" + "\n".join(block_lines) + "</code>")
    return "\n\n".join(blocks)

# -------------------------
# Bootstrap loading + smart match
# -------------------------
async def load_bootstrap_elements() -> Tuple[List[dict], Dict[int, dict]]:
    data = await fetch_json(FPL_BOOTSTRAP)
    if not data:
        return [], {}
    elements = data.get("elements", []) or []
    el_map = {}
    for el in elements:
        try:
            el_map[int(el.get("id"))] = el
        except Exception:
            continue
    return elements, el_map

def _pos_code_to_str(code: int) -> str:
    return {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}.get(int(code), "")

def find_element_by_name_smart(name: str, team_hint: str, pos_hint: str, price_hint: float, elements: List[dict]) -> Optional[dict]:
    name_low = (name or "").lower().strip()
    candidates = [el for el in elements if str(el.get("web_name", "")).lower().strip() == name_low]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    # step 2: filter by team_hint (abbr)
    filtered = []
    for el in candidates:
        tc = el.get("team_code") or el.get("team")
        try:
            tc_int = int(tc)
        except Exception:
            tc_int = None
        abbr = FPL_TEAM_ABBR.get(tc_int) if tc_int is not None else None
        if abbr and abbr.upper() == (team_hint or "").upper():
            filtered.append(el)
    if filtered:
        candidates = filtered
        if len(candidates) == 1:
            return candidates[0]
    # step 3: filter by position
    if pos_hint:
        filtered = [el for el in candidates if _pos_code_to_str(el.get("element_type")) == pos_hint.upper()]
        if filtered:
            candidates = filtered
            if len(candidates) == 1:
                return candidates[0]
    # step 4: filter by price
    try:
        filtered = []
        for el in candidates:
            now_cost = float(el.get("now_cost", 0)) / 10.0
            if abs(now_cost - float(price_hint)) < 0.01:
                filtered.append(el)
        if filtered:
            candidates = filtered
            if len(candidates) == 1:
                return candidates[0]
    except Exception:
        pass
    logger.warning("Ambiguous player match for '%s' -> using first candidate id=%s", name, candidates[0].get("id"))
    return candidates[0]

# -------------------------
# Price change detector helpers
# -------------------------
async def fetch_bootstrap_prices_map() -> Tuple[Dict[int, int], Dict[int, dict], List[dict]]:
    data = await fetch_json(FPL_BOOTSTRAP)
    out = {}
    el_map = {}
    elements = []
    if not data:
        return out, el_map, elements
    elements = data.get("elements", []) or []
    for el in elements:
        try:
            eid = int(el.get("id"))
            now_cost = int(el.get("now_cost") or 0)
            out[eid] = now_cost
            el_map[eid] = el
        except Exception:
            continue
    return out, el_map, elements

def detect_price_changes(old: Dict[str, int], new: Dict[int, int]) -> List[Tuple[int, int, int]]:
    changes = []
    for eid, new_cost in new.items():
        old_cost = old.get(str(eid))
        if old_cost is None:
            continue
        if new_cost != old_cost:
            changes.append((eid, old_cost, new_cost))
    return changes

def format_price_changes_message(changes: List[Tuple[int, int, int]], el_map: Dict[int, dict]) -> str:
    if not changes:
        return ""
    inc = []
    dec = []
    for eid, old_c, new_c in changes:
        old_p = f"£{old_c/10:.1f}"
        new_p = f"£{new_c/10:.1f}"
        name = el_map.get(eid, {}).get("web_name", f"id:{eid}")
        tc = el_map.get(eid, {}).get("team_code")
        try:
            tc_int = int(tc)
        except Exception:
            tc_int = None
        team_abbr = FPL_TEAM_ABBR.get(tc_int, "UNK")
        line = f"{team_abbr}  {name.ljust(15)} {old_p} → {new_p}"
        if new_c > old_c:
            inc.append(line)
        else:
            dec.append(line)
    lines = ["<code>", center_text("Price changes detected", 40), "(updated at 06:00 UTC+5)", ""]
    if inc:
        lines.append("Increased price")
        lines.extend(inc)
        lines.append("")
    if dec:
        lines.append("Decreased price")
        lines.extend(dec)
        lines.append("")
    lines.append("</code>")
    return "\n".join(lines)

# -------------------------
# /prices builder
# -------------------------
async def build_prices_sections_and_format() -> str:
    txt = await fetch_text(LIVEFPL_PRICES)
    if not txt:
        return "Could not fetch prices page."
    soup = BeautifulSoup(txt, "html.parser")
    raw_sections = {
        "Already reached target": extract_table(soup, "Already reached target"),
        "Projected to reach target": extract_table(soup, "Projected to reach target"),
        "Others who will be close": extract_table(soup, "Others who will be close"),
    }
    name_dir_map = {}
    for sec in ("Predicted Rises", "Predicted Falls"):
        rows = extract_table(soup, sec)
        d = "rise" if "Rise" in sec else "fall"
        for r in rows:
            n = (r.get("Name") or "").strip()
            if n:
                name_dir_map[n.lower()] = d
    for title, rows in raw_sections.items():
        for r in rows:
            n = (r.get("Name") or "").strip().lower()
            if name_dir_map.get(n):
                r["_direction"] = name_dir_map[n]
            else:
                t = parse_percent(r.get("Target", "0"))
                r["_direction"] = "rise" if t > 0 else ("fall" if t < 0 else "neutral")
    # attach element ids using bootstrap smart matching
    elements, el_map = await load_bootstrap_elements()
    for title, rows in raw_sections.items():
        for r in rows:
            name = (r.get("Name") or "").strip()
            team_hint = (r.get("Team") or "").strip()
            pos_hint = (r.get("Pos") or "").strip()
            try:
                price_hint = float(str(r.get("Price") or "0").replace("£", "").strip())
            except Exception:
                price_hint = 0.0
            found = find_element_by_name_smart(name, team_hint, pos_hint, price_hint, elements)
            if found:
                try:
                    r["element"] = int(found.get("id"))
                except Exception:
                    pass
    league_set = await get_league_player_set_cached(LEAGUE_ID)
    processed = sort_and_filter_sections(raw_sections, league_set)
    msg = format_prices_compact(processed, el_map)
    return msg

# -------------------------
# Security + sending
# -------------------------
def is_authorized_update(update: Update) -> bool:
    try:
        chat = update.effective_chat
        user = update.effective_user
        if chat is None or user is None:
            return False
        uid = int(user.id)
        # Owner always allowed in private
        if uid == OWNER_ID and chat.type == "private":
            return True
        # If message is in allowed group -> allow and add to whitelist
        if chat.id == ALLOWED_GROUP_ID:
            if uid not in _allowed_users:
                _allowed_users.add(uid)
                _save_allowed_users()
            return True
        # Private chat: allow only if whitelisted or owner
        if chat.type == "private":
            return uid in _allowed_users or uid == OWNER_ID
        # All other chats denied
        return False
    except Exception:
        logger.exception("Authorization check failed")
        return False

async def send_message_secure(app: Application, text: str, *, silent: bool = True, parse_mode: str = "HTML"):
    """
    Secure send wrapper - only sends to ALLOWED_GROUP_ID.
    Use this for all automated notifications and group-sent messages.
    """
    try:
        if not notifications_enabled():
            logger.debug("Notifications disabled - blocking secure send")
            return
        await app.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=text, parse_mode=parse_mode,
                                   disable_web_page_preview=True, disable_notification=silent)
    except Exception:
        logger.exception("send_message_secure failed")

# -------------------------
# Background tasks:
#  - utc5_daily_sender() -> 23:00 UTC+5 daily /prices (sound)
#  - price_change_detector() -> 05:45-08:00 UTC+5 every 5 minutes until update or end (silent). If none -> send summary "No price changes today."
# -------------------------
_utc5_daily_task: Optional[asyncio.Task] = None
_change_detector_task: Optional[asyncio.Task] = None

def _next_utc5_23(now: Optional[datetime] = None) -> datetime:
    if now is None:
        now = datetime.now(timezone.utc)
    tz_base = timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))
    local_now_base = now.astimezone(tz_base)
    today_target = datetime.combine(local_now_base.date(), time(EVENING_HOUR_UTC5, EVENING_MINUTE_UTC5), tzinfo=tz_base)
    if local_now_base < today_target:
        target_local = today_target
    else:
        target_local = today_target + timedelta(days=1)
    return target_local.astimezone(timezone.utc)

async def utc5_daily_sender(app: Application):
    logger.info("UTC+5 daily sender started (23:00 UTC+5)")
    try:
        while True:
            next_send_utc = _next_utc5_23()
            now_utc = datetime.now(timezone.utc)
            wait_secs = (next_send_utc - now_utc).total_seconds()
            if wait_secs > 0:
                # sleep responsive chunks
                while wait_secs > 0:
                    chunk = min(wait_secs, 3600)
                    await asyncio.sleep(chunk)
                    now_utc = datetime.now(timezone.utc)
                    wait_secs = (next_send_utc - now_utc).total_seconds()
            # build & send
            try:
                if not notifications_enabled():
                    logger.info("Notifications disabled - skipping utc5 daily send")
                else:
                    msg = await build_prices_sections_and_format()
                    await send_message_secure(app, msg, silent=False)
                    logger.info("Sent utc5 daily /prices to group %s", ALLOWED_GROUP_ID)
            except Exception:
                logger.exception("Failed to build/send utc5 daily message")
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        logger.info("utc5_daily_sender cancelled")
    except Exception:
        logger.exception("utc5_daily_sender crashed")

def _utc5_window_today(now_utc: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    tz_base = timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))  # UTC+5 zone
    local = now_utc.astimezone(tz_base)
    start_local = datetime.combine(local.date(), PRICE_CHANGE_WINDOW_START, tzinfo=tz_base)
    end_local = datetime.combine(local.date(), PRICE_CHANGE_WINDOW_END, tzinfo=tz_base)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

async def price_change_detector(app: Application):
    logger.info("Price change detector started (05:45-08:00 UTC+5 every 5 min)")
    try:
        while True:
            # compute today's window in UTC
            start_utc, end_utc = _utc5_window_today()
            now_utc = datetime.now(timezone.utc)
            # if now before start -> sleep until start
            if now_utc < start_utc:
                sleep_secs = (start_utc - now_utc).total_seconds()
                # sleep in chunks
                while sleep_secs > 0:
                    chunk = min(sleep_secs, 3600)
                    await asyncio.sleep(chunk)
                    now_utc = datetime.now(timezone.utc)
                    sleep_secs = (start_utc - now_utc).total_seconds()
            # now in or after start_utc
            updates_found = False
            # run polling loop until end_utc
            while datetime.now(timezone.utc) <= end_utc:
                if not notifications_enabled():
                    logger.info("Notifications disabled - skipping today's change checks")
                    break
                # do check
                try:
                    new_map, el_map, elements = await fetch_bootstrap_prices_map()
                    changes = detect_price_changes(prices_baseline, new_map)
                    if changes:
                        # format & send (silent)
                        msg = format_price_changes_message(changes, el_map)
                        if msg:
                            await send_message_secure(app, msg, silent=True)
                            logger.info("Sent price-change notification to group %s", ALLOWED_GROUP_ID)
                        # update baseline on first detection
                        pb = {str(k): v for k, v in new_map.items()}
                        _save_json(PRICES_BASELINE_FILE, pb)
                        global prices_baseline
                        prices_baseline = pb
                        updates_found = True
                        break  # stop polling for today
                    else:
                        # if baseline empty, initialize
                        if not prices_baseline:
                            pb = {str(k): v for k, v in new_map.items()}
                            _save_json(PRICES_BASELINE_FILE, pb)
                            prices_baseline.update(pb)
                except Exception:
                    logger.exception("Error during price-change check")
                # sleep 5 minutes before next attempt (but break early if time passed)
                await asyncio.sleep(PRICE_CHANGE_POLL_SECONDS)
            # after polling window
            if not updates_found and notifications_enabled():
                # send "no changes" summary
                no_msg = "<code>\n" + center_text("Price changes summary", 40) + "\n" + "-------------------------------\n" + "No price changes today.\n" + "</code>"
                try:
                    await send_message_secure(app, no_msg, silent=True)
                    logger.info("Sent no-update summary to group %s", ALLOWED_GROUP_ID)
                except Exception:
                    logger.exception("Failed to send no-update summary")
            # sleep until next day's window (i.e., compute next day's start)
            # compute next day's start_utc by adding 1 day to start_utc
            start_utc_next = start_utc + timedelta(days=1)
            now_utc = datetime.now(timezone.utc)
            sleep_secs = (start_utc_next - now_utc).total_seconds()
            if sleep_secs > 0:
                # sleep in chunks
                while sleep_secs > 0:
                    chunk = min(sleep_secs, 3600)
                    await asyncio.sleep(chunk)
                    now_utc = datetime.now(timezone.utc)
                    sleep_secs = (start_utc_next - now_utc).total_seconds()
    except asyncio.CancelledError:
        logger.info("price_change_detector cancelled")
    except Exception:
        logger.exception("price_change_detector crashed")

# -------------------------
# GW38 auto-disable
# -------------------------
async def check_and_disable_after_gw38():
    try:
        data = await fetch_json(FPL_BOOTSTRAP)
        if not data:
            return
        for ev in data.get("events", []) or []:
            try:
                if int(ev.get("id", -1)) == GW_DISABLE_TARGET:
                    finished = bool(ev.get("finished") or ev.get("is_finished") or False)
                    if finished:
                        set_notifications_enabled(False)
                        logger.info("GW38 finished -> notifications disabled")
                    return
            except Exception:
                continue
    except Exception:
        logger.exception("check_and_disable_after_gw38 failed")

# -------------------------
# Bot command handlers
# -------------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_update(update):
        return
    text = (
        "FPL Prices Bot\n"
        "Commands:\n"
        "/prices - show current compact prices (3 sections)\n"
        "/settz <Zone> - set your timezone (IANA e.g. Asia/Almaty or +5)\n"
        "/mytz - show your timezone\n"
        "/notify_status - show notification enabled/disabled\n"
    )
    if update.effective_chat.type == "private":
        await update.message.reply_text(text)
    else:
        await send_message_secure(context.application, text, silent=False)

async def prices_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_update(update):
        return
    msg = await build_prices_sections_and_format()
    if update.effective_chat.type == "private":
        await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
    else:
        await send_message_secure(context.application, msg, silent=False)

async def settz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_update(update):
        return
    args = context.args or []
    if not args:
        cur = user_timezones.get(update.effective_user.id, DEFAULT_TZ_STR)
        if update.effective_chat.type == "private":
            await update.message.reply_text(f"Usage: /settz <IANA zone or +N>\nCurrent: {cur}")
        else:
            await send_message_secure(context.application, f"Usage: /settz <IANA zone or +N>\nCurrent: {cur}", silent=False)
        return
    parsed = parse_tz_input(args[0])
    if not parsed:
        await update.message.reply_text("Unknown timezone. Use IANA like Asia/Almaty or offset +5")
        return
    try:
        _ = tz_to_zone(parsed)
    except Exception:
        await update.message.reply_text("Cannot resolve timezone; try IANA.")
        return
    user_timezones[update.effective_user.id] = parsed
    _save_json(USER_TZ_FILE, {str(k): v for k, v in user_timezones.items()})
    if update.effective_chat.type == "private":
        await update.message.reply_text(f"Timezone set to {parsed}")
    else:
        await send_message_secure(context.application, f"Timezone set to {parsed}", silent=False)

async def mytz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_update(update):
        return
    tz = user_timezones.get(update.effective_user.id, DEFAULT_TZ_STR)
    if update.effective_chat.type == "private":
        await update.message.reply_text(f"Your timezone: {tz}")
    else:
        await send_message_secure(context.application, f"Your timezone: {tz}", silent=False)

async def notify_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_update(update):
        return
    enabled = notifications_enabled()
    text = f"Notifications enabled: {enabled}\nNotification flag file: {NOTIF_FLAG_FILE}"
    if update.effective_chat.type == "private":
        await update.message.reply_text(text)
    else:
        await send_message_secure(context.application, text, silent=False)

# group logger to populate whitelist when users post in group
async def _group_message_logger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_chat and update.effective_chat.id == ALLOWED_GROUP_ID:
            is_authorized_update(update)  # adds author to whitelist
    except Exception:
        pass

# -------------------------
# Application lifecycle
# -------------------------
async def start_background_tasks(app: Application):
    await check_and_disable_after_gw38()
    global _utc5_daily_task, _change_detector_task
    if _utc5_daily_task is None or _utc5_daily_task.done():
        _utc5_daily_task = asyncio.create_task(utc5_daily_sender(app))
    if _change_detector_task is None or _change_detector_task.done():
        _change_detector_task = asyncio.create_task(price_change_detector(app))
    logger.info("Background tasks started")

async def stop_background_tasks():
    global _utc5_daily_task, _change_detector_task
    tasks = [_utc5_daily_task, _change_detector_task]
    for t in tasks:
        if t:
            t.cancel()
    await asyncio.sleep(0.1)

# -------------------------
# Main
# -------------------------
def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is required in environment")
        return
    app = Application.builder().token(BOT_TOKEN).build()
    # handlers
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("prices", prices_cmd))
    app.add_handler(CommandHandler("settz", settz_cmd))
    app.add_handler(CommandHandler("mytz", mytz_cmd))
    app.add_handler(CommandHandler("notify_status", notify_status_cmd))
    app.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), _group_message_logger))
    # lifecycle hooks
    async def _on_start(_app: Application):
        logger.info("Bot started (PTB %s)", PTB_VERSION)
        # ensure allowed users loaded (already loaded at import)
        await start_background_tasks(_app)
    async def _on_stop(_app: Application):
        logger.info("Bot stopping")
        await stop_background_tasks()
        await _http_client.aclose()
    app.post_init = _on_start
    app.post_shutdown = _on_stop
    logger.info("Starting bot...")
    app.run_polling()

if __name__ == "__main__":
    main()
