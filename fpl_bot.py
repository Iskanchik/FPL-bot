# fpl_bot.py
# Single-file Telegram bot — full implementation (senior-level)
# Requirements: python 3.10+, python-telegram-bot v20+, httpx, beautifulsoup4
# Persisted files: /mnt/data/...
# ENV: BOT_TOKEN (required), ALERT_CHAT_ID (optional)

import os
import asyncio
import json
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Dict, List, Optional, Tuple, Set, Any
import logging

import httpx
from bs4 import BeautifulSoup

from telegram import __version__ as PTB_VERSION  # for dev info
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# -------------------------
# CONFIG
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ALERT_CHAT_ID = os.getenv("ALERT_CHAT_ID")  # optional default target
LEAGUE_ID = int(os.getenv("LEAGUE_ID", "980121"))
DATA_DIR = "/mnt/data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# persistent files
USER_TZ_FILE = os.path.join(DATA_DIR, "user_timezones.json")
LAST_SENT_FILE = os.path.join(DATA_DIR, "user_last_sent.json")
PRICES_BASELINE_FILE = os.path.join(DATA_DIR, "prices_baseline.json")
LEAGUE_CACHE_FILE = os.path.join(DATA_DIR, "league_player_set.json")
NOTIF_FLAG_FILE = os.path.join(DATA_DIR, "notifications_enabled.json")

# FPL API / LiveFPL URLs
FPL_BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
FPL_ENTRY_URL = "https://fantasy.premierleague.com/api/entry/{entry_id}/event/0/picks/"
LIVEFPL_PRICES = "https://www.livefpl.net/prices"

# Scheduling constants
PRICE_CHANGE_UTC_PLUS = 5  # prices change at 06:00 UTC+5
PRICE_CHANGE_TIME_UTC5 = time(6, 0)
PRECHANGE_HOUR_UTC5 = 4  # 2 hours before
PRECHANGE_TIME_UTC5 = time(PRECHANGE_HOUR_UTC5, 0)

# morning update (detect actual changes) runs once after 06:00 UTC+5
# daily summary at 23:00 local user time (or fallback)
EVENING_HOUR = 23
EVENING_MINUTE = 0

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fpl_bot")

# -------------------------
# Utilities: persistence
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

# user tzs: chat_id -> tz_string
user_timezones: Dict[int, str] = {int(k): v for k, v in _load_json(USER_TZ_FILE, {}).items()}
# last_sent: chat_id -> {"daily": "YYYY-MM-DD", "change": "YYYY-MM-DD"}
user_last_sent: Dict[str, Dict[str, str]] = _load_json(LAST_SENT_FILE, {})

# baseline prices snapshot: element_id -> now_cost (int)
prices_baseline: Dict[str, int] = _load_json(PRICES_BASELINE_FILE, {})

# league player set + gw saved
league_cache: Dict[str, Any] = _load_json(LEAGUE_CACHE_FILE, {})
# notifications enabled flag
notif_flag_obj = _load_json(NOTIF_FLAG_FILE, {"enabled": True})

def notifications_enabled() -> bool:
    return bool(notif_flag_obj.get("enabled", True))

def set_notifications_enabled(enabled: bool):
    notif_flag_obj["enabled"] = bool(enabled)
    _save_json(NOTIF_FLAG_FILE, notif_flag_obj)

# -------------------------
# HTTP client (single global)
# -------------------------
_http_client = httpx.AsyncClient(timeout=15.0, headers={"User-Agent": "FPL-Bot/1.0"})

async def fetch_json(url: str) -> Optional[dict]:
    try:
        r = await _http_client.get(url)
        if r.status_code == 200:
            return r.json()
        else:
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
# TZ helpers
# -------------------------
DEFAULT_TZ_STR = f"UTC+{PRICE_CHANGE_UTC_PLUS}"

def parse_tz_input(tz_input: str) -> Optional[str]:
    if not tz_input:
        return None
    s = tz_input.strip()
    if (s.startswith("+") or s.startswith("-")) and s[1:].replace(".", "").isdigit():
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
# TEAM EMOJI
# -------------------------
TEAM_EMOJI = {
    "ARS": "🔴", "AVL": "🟣", "BRE": "🟥⬛", "BHA": "🟦", "BUR": "🟥",
    "CHE": "🔵", "CRY": "🔷", "EVE": "🔵", "FUL": "⚫⚪", "LIV": "🔴",
    "MCI": "🔵", "MUN": "🔴", "NEW": "⚫⚪", "NFO": "🔴", "SHU": "🔴⚪",
    "TOT": "⚪", "WHU": "🟣🔵", "WOL": "🟧", "LUT": "🟧", "BOU": "🔴⚫"
}

# -------------------------
# Parsing LiveFPL prices page -> extract 3 sections + keep original direction info
# Note: LiveFPL HTML structure may change; this uses heuristics.
# -------------------------
def label_direction_from_section_name(section_name: str) -> str:
    s = (section_name or "").lower()
    if "fall" in s:
        return "fall"
    if "rise" in s:
        return "rise"
    return "neutral"

def parse_row_cells(cells: List[str]) -> Dict[str, str]:
    # heuristic mapping - depends on table columns on livefpl
    # expected columns: Player | Pos | Team | Price | Target | Owned by | ...
    row = {}
    # fill best-effort
    if len(cells) >= 1:
        row["Name"] = cells[0]
    if len(cells) >= 2:
        row["Pos"] = cells[1]
    if len(cells) >= 3:
        row["Team"] = cells[2]
    # find price-like cell
    for c in cells[3:]:
        if "£" in c or c.replace(".", "", 1).isdigit():
            row["Price"] = c.replace("£", "").strip()
            break
    # target and owned
    for c in reversed(cells):
        if "%" in c and "Target" not in row:
            row["Target"] = c.strip()
        elif "%" in c and "Owned by" not in row:
            row["Owned by"] = c.strip()
    # normalize keys
    row.setdefault("Price", "")
    row.setdefault("Target", "")
    row.setdefault("Owned by", "")
    return row

def extract_table(html_soup: BeautifulSoup, key_hint: str) -> List[Dict[str, Any]]:
    # Find header that mentions key_hint; return list of row dicts
    rows: List[Dict[str, Any]] = []
    # find possible headings
    possible = html_soup.find_all(["h3", "h4", "h2", "strong"])
    found = None
    for p in possible:
        txt = p.get_text(" ", strip=True).lower()
        if key_hint.lower() in txt:
            found = p
            break
    if not found:
        # try fuzzy
        for p in possible:
            txt = p.get_text(" ", strip=True).lower()
            if any(w in txt for w in key_hint.lower().split()):
                found = p
                break
    if not found:
        return rows
    # next table
    tbl = found.find_next("table")
    if not tbl:
        # maybe lists
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
# process categories per rules:
# - Only three categories used (Already reached target, Projected to reach target, Others who will be close)
# - Apply Owned >= 1% filter
# - Keep original LiveFPL section membership unless target >=100 or <=-100 (goes to Already)
# - Sort inside each category:
#    1) direction partition: rising (target>0) first, then falling (target<=0)
#    2) then target descending
#    3) then league membership (1/0) descending
#    4) then global owned descending
# -------------------------
def parse_percent(s: str) -> float:
    try:
        if s is None:
            return 0.0
        s2 = str(s).replace("%", "").strip()
        return float(s2) if s2 != "" else 0.0
    except Exception:
        return 0.0

def is_in_league_row(r: Dict[str, Any], league_set: Set[str]) -> int:
    name = (r.get("Name") or "").strip().lower()
    # league_set may contain 'id:123' entries or names
    if not name:
        return 0
    if name in (n.lower() for n in league_set):
        return 1
    # try element id matching
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
                continue  # filter < 1%
            target_val = parse_percent(r.get("Target", "0"))
            # direction: if original section indicates Predicted Falls/Rises we can set _direction
            direction = r.get("_direction") or "neutral"
            # override direction if table name indicated rise/fall - already attached in caller maybe
            # league membership
            in_league = is_in_league_row(r, league_set)
            r["_target_val"] = target_val
            r["_in_league"] = in_league
            r["_owned_val"] = owned_val
            # direction derived from r["_direction"] or sign of target for partitioning
            if "_direction" not in r:
                r["_direction"] = "rise" if target_val > 0 else "fall" if target_val < 0 else "neutral"
            # determine dir priority: rise first (1), else 0
            dir_pr = 1 if r["_direction"] != "fall" else 0
            r["_dir_pr"] = dir_pr
            processed.append(r)
        # sort by (dir_pr desc, target desc, in_league desc, owned desc)
        processed.sort(key=lambda x: (x["_dir_pr"], x["_target_val"], x["_in_league"], x["_owned_val"]), reverse=True)
        # cleanup temps
        for rr in processed:
            for k in ("_target_val", "_in_league", "_owned_val", "_dir_pr"):
                rr.pop(k, None)
        # Only keep three categories in final output; name adjustments done in caller
        out[title] = processed
    return out

# -------------------------
# League players cache: fetch once per GW and persist
# -------------------------
async def get_current_gw() -> int:
    data = await fetch_json(FPL_BOOTSTRAP)
    if not data:
        return 0
    for ev in data.get("events", []):
        if ev.get("is_current"):
            return int(ev.get("id", 0))
    return 0

async def fetch_league_player_set_once(league_id: int, cap: int = 80) -> Set[str]:
    # read standings -> entry ids -> fetch picks for top N entries -> collect 'id:element' strings
    out: Set[str] = set()
    url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"
    data = await fetch_json(url)
    if not data:
        return out
    # standings shape can vary; try common shapes
    results = data.get("standings", {}).get("results") or data.get("results") or data.get("entries") or []
    entry_ids = []
    for r in results:
        eid = r.get("entry") or r.get("id") or r.get("entry_id")
        if eid:
            entry_ids.append(int(eid))
    # cap
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
    # load from league_cache: {"gw": N, "set": [..]}
    gw = await get_current_gw()
    gw_saved = int(league_cache.get("gw", -1))
    if gw_saved == gw and league_cache.get("set"):
        return set(league_cache.get("set", []))
    s = await fetch_league_player_set_once(league_id)
    league_cache["gw"] = gw
    league_cache["set"] = list(s)
    _save_json(LEAGUE_CACHE_FILE, league_cache)
    return s

# -------------------------
# Formatting compact table (mono block) with center headers
# -------------------------
def center_text(s: str, width: int = 40) -> str:
    return s.center(width)

def format_prices_compact(sections: Dict[str, List[Dict[str, Any]]]) -> str:
    order = [
        "Already reached target",
        "Projected to reach target",
        "Others who will be close",
    ]
    blocks: List[str] = []
    # build per section
    for title in order:
        rows = sections.get(title, []) or []
        # compute columns widths
        names = [r.get("Name", "") for r in rows]
        prices = [f"£{r.get('Price')}".strip() for r in rows]
        targets = []
        for r in rows:
            t = r.get("Target", "")
            tval = int(parse_percent(t))
            # negative sign only for fall direction
            if r.get("_direction", "neutral") == "fall":
                targets.append(f"-{abs(int(tval))}%")
            else:
                targets.append(f"{int(tval)}%")
        max_name = max((len(x) for x in names), default=0)
        max_price = max((len(x) for x in prices), default=0)
        max_target = max((len(x) for x in targets), default=0)
        # header centered
        header = center_text(title, 40)
        block_lines = [header]
        for i, r in enumerate(rows):
            emoji = TEAM_EMOJI.get((r.get("Team") or "").upper(), "⚪")
            name = (r.get("Name") or "").ljust(max_name)
            p = prices[i].ljust(max_price)
            tgt = targets[i].rjust(max_target)
            block_lines.append(f"{emoji}  {name}  {p}  ({tgt})")
        if not rows:
            block_lines.append("(none)")
        # wrap in code block for monospaced alignment
        blocks.append("<code>" + "\n".join(block_lines) + "</code>")
    return "\n\n".join(blocks)

# -------------------------
# Price change detector: compare bootstrap now_cost -> baseline
# sends silent notification with increased/decreased lists if changes detected
# -------------------------
async def fetch_bootstrap_prices_map() -> Tuple[Dict[int, int], Dict[int, dict]]:
    data = await fetch_json(FPL_BOOTSTRAP)
    out = {}
    el_map = {}
    if not data:
        return out, el_map
    for el in data.get("elements", []) or []:
        try:
            eid = int(el.get("id"))
            now_cost = int(el.get("now_cost") or 0)
            out[eid] = now_cost
            el_map[eid] = el
        except Exception:
            continue
    return out, el_map

def detect_price_changes(old: Dict[str, int], new: Dict[int, int]) -> List[Tuple[int, int, int]]:
    changes = []
    for eid, new_cost in new.items():
        old_cost = old.get(str(eid))
        if old_cost is None:
            # first run: treat as baseline only
            continue
        if new_cost != old_cost:
            changes.append((eid, old_cost, new_cost))
    return changes

def format_price_changes_message(changes: List[Tuple[int, int, int]], el_map: Dict[int, dict]) -> str:
    if not changes:
        return ""
    # prepare lines
    inc = []
    dec = []
    for eid, old_c, new_c in changes:
        old_p = f"£{old_c/10:.1f}"
        new_p = f"£{new_c/10:.1f}"
        name = el_map.get(eid, {}).get("web_name", f"id:{eid}")
        team_code = el_map.get(eid, {}).get("team_code") or el_map.get(eid, {}).get("team")
        emoji = TEAM_EMOJI.get(team_code, "⚪")
        line = f"{emoji}  {name.ljust(15)} {old_p} → {new_p}"
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
# /prices handler
# -------------------------
async def build_prices_sections_and_format() -> str:
    # fetch livefpl html
    txt = await fetch_text(LIVEFPL_PRICES)
    if not txt:
        return "Could not fetch prices page."
    soup = BeautifulSoup(txt, "html.parser")
    # extract three named tables (the extract_table func is heuristic)
    raw_sections = {
        "Already reached target": extract_table(soup, "Already reached target"),
        "Projected to reach target": extract_table(soup, "Projected to reach target"),
        "Others who will be close": extract_table(soup, "Others who will be close"),
    }
    # attach direction tags where possible: try to detect source section in html by searching "Predicted Falls/Rises" tables and mapping players
    # simpler: try to find predicted falls and rises lists and mark rows by name
    # build name->direction map by reading "Predicted Rises" and "Predicted Falls" tables
    name_dir_map = {}
    for sec in ("Predicted Rises", "Predicted Falls"):
        rows = extract_table(soup, sec)
        d = "rise" if "Rise" in sec else "fall"
        for r in rows:
            n = (r.get("Name") or "").strip()
            if n:
                name_dir_map[n.lower()] = d
    # apply directions to raw sections
    for title, rows in raw_sections.items():
        for r in rows:
            n = (r.get("Name") or "").strip().lower()
            if name_dir_map.get(n):
                r["_direction"] = name_dir_map[n]
            else:
                # fallback: infer from target sign
                t = parse_percent(r.get("Target", "0"))
                r["_direction"] = "rise" if t > 0 else ("fall" if t < 0 else "neutral")
    # league set
    league_set = await get_league_player_set_cached(LEAGUE_ID)
    # sort & filter
    processed = sort_and_filter_sections(raw_sections, league_set)
    # final formatting
    msg = format_prices_compact(processed)
    return msg

# -------------------------
# Background tasks: user scheduler with single notification per day (priority 23:00 local -> fallback prechange)
# and morning price change detector after 06:00 UTC+5 (silent)
# -------------------------
_daily_task: Optional[asyncio.Task] = None
_prechange_task: Optional[asyncio.Task] = None
_change_detector_task: Optional[asyncio.Task] = None

def _ensure_user_entry(chat_id: int):
    k = str(chat_id)
    if k not in user_last_sent:
        user_last_sent[k] = {"daily": "", "change": ""}

async def send_message(app: Application, chat_id: int, text: str, silent: bool):
    try:
        if chat_id is None:
            return
        await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML",
                                   disable_web_page_preview=True, disable_notification=silent==True and True or False)
    except Exception:
        logger.exception("Failed to send message to %s", chat_id)

async def per_user_daily_scheduler(app: Application):
    logger.info("User daily scheduler started")
    while True:
        try:
            if not notifications_enabled():
                await asyncio.sleep(300)
                continue
            now_utc = datetime.now(timezone.utc)
            # loop users
            for chat_id, tz_str in list(user_timezones.items()):
                try:
                    zone = tz_to_zone(tz_str)
                except Exception:
                    zone = tz_to_zone(DEFAULT_TZ_STR)
                now_local = now_utc.astimezone(zone)
                _ensure_user_entry(chat_id)
                k = str(chat_id)
                today = now_local.date().isoformat()
                # priority 1: 23:00 local
                if now_local.hour == EVENING_HOUR and now_local.minute == EVENING_MINUTE:
                    if user_last_sent[k].get("daily") != today:
                        msg = await build_prices_sections_and_format()
                        # send with sound
                        await send_message(app, chat_id, msg, silent=False)
                        user_last_sent[k]["daily"] = today
                        _save_json(LAST_SENT_FILE, user_last_sent)
                        continue
                # fallback: time corresponding to 04:00 UTC+5
                # compute 04:00 in UTC then convert to user's local
                base_zone = timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))
                base_dt_local = datetime.combine(now_utc.astimezone(base_zone).date(), PRECHANGE_TIME_UTC5, tzinfo=base_zone)
                base_dt_utc = base_dt_local.astimezone(timezone.utc)
                user_pre_dt_local = base_dt_utc.astimezone(zone)
                # if now_local matches that minute
                if user_pre_dt_local.date() == now_local.date() and now_local.hour == user_pre_dt_local.hour and now_local.minute == user_pre_dt_local.minute:
                    if user_last_sent[k].get("daily") != today:
                        msg = await build_prices_sections_and_format()
                        await send_message(app, chat_id, msg, silent=False)
                        user_last_sent[k]["daily"] = today
                        _save_json(LAST_SENT_FILE, user_last_sent)
                # small delay to avoid hot loop
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            logger.info("per_user_daily_scheduler cancelled")
            break
        except Exception:
            logger.exception("Error in per_user_daily_scheduler")
            await asyncio.sleep(5)

async def price_change_detector(app: Application):
    # runs frequently around 06:00 UTC+5 but safe to run periodic checks
    logger.info("Price change detector started")
    while True:
        try:
            if not notifications_enabled():
                await asyncio.sleep(300)
                continue
            # determine current UTC time and check if it's past 06:00 UTC+5 for today
            now_utc = datetime.now(timezone.utc)
            tz_base = timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))
            now_in_base = now_utc.astimezone(tz_base)
            # only attempt detection within some window after 06:00 base (e.g. 06:00-09:00)
            if time(6,0) <= now_in_base.time() <= time(9,0):
                # fetch current prices map
                new_map, el_map = await fetch_bootstrap_prices_map()
                # detect changes vs baseline
                changes = detect_price_changes(prices_baseline, new_map)
                if changes:
                    # format and send to all users quietly (silent)
                    msg = format_price_changes_message(changes, el_map)
                    if msg:
                        # send to configured alert chat (ALERT_CHAT_ID) and to individual users?
                        # We send to ALERT_CHAT_ID if set; and to all users as well (silent)
                        target = ALERT_CHAT_ID
                        if target:
                            try:
                                await send_message(app, int(target), msg, silent=True)
                            except Exception:
                                logger.exception("Failed to send to ALERT_CHAT_ID")
                        # send to all known users
                        for chat_id in list(user_timezones.keys()):
                            try:
                                k = str(chat_id)
                                today = now_in_base.date().isoformat()
                                if user_last_sent.get(k, {}).get("change") == today:
                                    continue
                                await send_message(app, chat_id, msg, silent=True)
                                _ensure_user_entry(chat_id)
                                user_last_sent[k]["change"] = today
                                _save_json(LAST_SENT_FILE, user_last_sent)
                            except Exception:
                                logger.exception("Failed to send price change to user %s", chat_id)
                    # update baseline to new map
                    # save new baseline
                    pb = {str(k): v for k, v in new_map.items()}
                    _save_json(PRICES_BASELINE_FILE, pb)
                    global prices_baseline
                    prices_baseline = pb
                else:
                    # if baseline empty, set it
                    if not prices_baseline:
                        pb = {str(k): v for k, v in new_map.items()}
                        _save_json(PRICES_BASELINE_FILE, pb)
                        prices_baseline.update(pb)
            # sleep until next check; check often but this loop is cheap
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            logger.info("price_change_detector cancelled")
            break
        except Exception:
            logger.exception("Error in price_change_detector")
            await asyncio.sleep(30)

# -------------------------
# GW38 auto-disable check
# -------------------------
GW_DISABLE_TARGET = 38

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
    chat_id = update.effective_chat.id
    text = (
        "FPL Prices Bot\n"
        "Commands:\n"
        "/prices - show current compact prices (3 sections)\n"
        "/settz <Zone> - set your timezone (IANA e.g. Asia/Almaty or +5)\n"
        "/mytz - show your timezone\n"
        "/notify_status - show notification enabled/disabled and last sends\n"
    )
    await update.message.reply_text(text)

async def prices_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await build_prices_sections_and_format()
    await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)

async def settz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args or []
    if not args:
        cur = user_timezones.get(chat_id, DEFAULT_TZ_STR)
        await update.message.reply_text(f"Usage: /settz <IANA zone or +N>\nCurrent: {cur}")
        return
    parsed = parse_tz_input(args[0])
    if not parsed:
        await update.message.reply_text("Unknown timezone. Use IANA like Asia/Almaty or offset +5")
        return
    # validate tz
    try:
        _ = tz_to_zone(parsed)
    except Exception:
        await update.message.reply_text("Cannot resolve timezone; try IANA till you succeed.")
        return
    user_timezones[chat_id] = parsed
    _save_json(USER_TZ_FILE, {str(k): v for k, v in user_timezones.items()})
    await update.message.reply_text(f"Timezone set to {parsed}")

async def mytz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    tz = user_timezones.get(chat_id, DEFAULT_TZ_STR)
    await update.message.reply_text(f"Your timezone: {tz}")

async def notify_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    enabled = notifications_enabled()
    await update.message.reply_text(f"Notifications enabled: {enabled}\nNotification flag file: {NOTIF_FLAG_FILE}")

# -------------------------
# Application lifecycle: start/stop tasks
# -------------------------
async def start_background_tasks(app: Application):
    # check GW38 once at start
    await check_and_disable_after_gw38()
    global _daily_task, _change_detector_task
    if _daily_task is None or _daily_task.done():
        _daily_task = asyncio.create_task(per_user_daily_scheduler(app))
    if _change_detector_task is None or _change_detector_task.done():
        _change_detector_task = asyncio.create_task(price_change_detector(app))
    logger.info("Background tasks started")

async def stop_background_tasks():
    global _daily_task, _change_detector_task
    tasks = [_daily_task, _change_detector_task]
    for t in tasks:
        if t:
            t.cancel()
    # gather to finish
    await asyncio.sleep(0.1)

# -------------------------
# Main
# -------------------------
def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is required in env")
        return
    app = Application.builder().token(BOT_TOKEN).build()
    # handlers
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("prices", prices_cmd))
    app.add_handler(CommandHandler("settz", settz_cmd))
    app.add_handler(CommandHandler("mytz", mytz_cmd))
    app.add_handler(CommandHandler("notify_status", notify_status_cmd))
    # start/stop hooks
    async def _on_start(_app: Application):
        logger.info("Bot started (PTB %s)", PTB_VERSION)
        await start_background_tasks(_app)
    async def _on_stop(_app: Application):
        logger.info("Bot stopping")
        await stop_background_tasks()
        await _http_client.aclose()
    app.post_init = _on_start
    app.post_shutdown = _on_stop
    # run
    logger.info("Starting bot...")
    app.run_polling()

if __name__ == "__main__":
    main()
