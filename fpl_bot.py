# fpl_bot.py
# Enterprise single-file FPL Prices Bot (final)
# - Single-file (no splitting) as requested
# - Features: atomic file I/O, HMAC whitelist, bootstrap cache + name index,
#   circuit breaker for LiveFPL, strict validation, health-state, DI-style init,
#   retries/backoff, precise scheduler, safe sending only to ALLOWED_GROUP_ID.
#
# Requirements (install into image):
# python-telegram-bot[ext]==20.6
# httpx==0.25.2
# beautifulsoup4==4.12.3
# lxml==4.9.3
# fastjsonschema==2.19.1
# tzdata
# loguru==0.7.2
# tenacity==8.2.3
#
# ENV required:
# BOT_TOKEN, OWNER_ID, ALLOWED_GROUP_ID, WHITELIST_HMAC_KEY (recommended), LEAGUE_ID (opt)
# Optional tuning: BOOTSTRAP_TTL_SECS, PRICE_CHANGE_POLL_SECONDS, PRICE_CHANGE_UTC_PLUS
#
# Minimal comments: only for non-obvious rationale.

import os
import sys
import asyncio
import json
import hmac
import hashlib
import time as _time
from collections import defaultdict
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple, Set, Any
import logging

import httpx
from bs4 import BeautifulSoup
import fastjsonschema
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from telegram import __version__ as PTB_VERSION
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# -------------------------
# CONFIG FROM ENV
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
ALLOWED_GROUP_ID = int(os.getenv("ALLOWED_GROUP_ID", "0"))
LEAGUE_ID = int(os.getenv("LEAGUE_ID", "980121"))

DATA_DIR = os.getenv("DATA_DIR", "/mnt/data")
os.makedirs(DATA_DIR, exist_ok=True)

WHITELIST_HMAC_KEY = os.getenv("WHITELIST_HMAC_KEY", "")  # recommended
BOOTSTRAP_TTL_SECS = int(os.getenv("BOOTSTRAP_TTL_SECS", "30"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "2"))
PRICE_CHANGE_POLL_SECONDS = int(os.getenv("PRICE_CHANGE_POLL_SECONDS", str(5 * 60)))  # default 5 min
PRICE_CHANGE_UTC_PLUS = int(os.getenv("PRICE_CHANGE_UTC_PLUS", "5"))
GW_DISABLE_TARGET = int(os.getenv("GW_DISABLE_TARGET", "38"))

# Files
USER_TZ_FILE = os.path.join(DATA_DIR, "user_timezones.json")
LAST_SENT_FILE = os.path.join(DATA_DIR, "user_last_sent.json")
PRICES_BASELINE_FILE = os.path.join(DATA_DIR, "prices_baseline.json")
LEAGUE_CACHE_FILE = os.path.join(DATA_DIR, "league_player_set.json")
NOTIF_FLAG_FILE = os.path.join(DATA_DIR, "notifications_enabled.json")
ALLOWED_USERS_FILE = os.path.join(DATA_DIR, "allowed_users.json")
PRICE_CHECK_STATE_FILE = os.path.join(DATA_DIR, "price_check_state.json")
LAST_LIVEFPL_HTML = os.path.join(DATA_DIR, "last_livefpl_html.html")

FPL_BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
LIVEFPL_PRICES = "https://www.livefpl.net/prices"

# -------------------------
# Basic validation
# -------------------------
if not BOT_TOKEN:
    print("BOT_TOKEN is required in environment", file=sys.stderr)
    sys.exit(1)
if OWNER_ID <= 0:
    print("OWNER_ID must be set in environment", file=sys.stderr)
    sys.exit(1)
if ALLOWED_GROUP_ID == 0:
    print("ALLOWED_GROUP_ID must be set in environment", file=sys.stderr)
    sys.exit(1)

# -------------------------
# Logger (text human-readable)
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("fpl_bot")
logger.info("Logger initialized; PTB %s", PTB_VERSION)

# Minimal in-memory metrics for observability
metrics = defaultdict(int)

# -------------------------
# Atomic async file I/O with locks
# -------------------------
_file_locks: Dict[str, asyncio.Lock] = {}

def _get_lock(path: str) -> asyncio.Lock:
    if path not in _file_locks:
        _file_locks[path] = asyncio.Lock()
    return _file_locks[path]

def _load_json_sync(path: str, default):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        logger.exception("Failed to load %s", path)
    return default

async def _save_json_atomic(path: str, data):
    lock = _get_lock(path)
    async with lock:
        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, path)
        except Exception:
            logger.exception("Atomic save failed %s", path)
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass

# -------------------------
# Load persisted state at startup (sync)
# -------------------------
user_timezones: Dict[int, str] = {int(k): v for k, v in _load_json_sync(USER_TZ_FILE, {}).items()}
user_last_sent: Dict[str, Dict[str, str]] = _load_json_sync(LAST_SENT_FILE, {})
prices_baseline: Dict[str, int] = _load_json_sync(PRICES_BASELINE_FILE, {})
league_cache: Dict[str, Any] = _load_json_sync(LEAGUE_CACHE_FILE, {})
notif_flag_obj = _load_json_sync(NOTIF_FLAG_FILE, {"enabled": True})
_price_check_state = _load_json_sync(PRICE_CHECK_STATE_FILE, {})

# -------------------------
# Whitelist pack/unpack (HMAC)
# -------------------------
def _whitelist_pack(users: List[int]) -> dict:
    payload = {"users": sorted(list(users))}
    sig = ""
    if WHITELIST_HMAC_KEY:
        hm = hmac.new(WHITELIST_HMAC_KEY.encode(), json.dumps(payload["users"]).encode(), hashlib.sha256).hexdigest()
        sig = hm
    return {"payload": payload, "sig": sig, "schema": 1}

def _whitelist_unpack(blob: dict) -> List[int]:
    payload = blob.get("payload", {}).get("users", [])
    sig = blob.get("sig", "")
    if WHITELIST_HMAC_KEY:
        expected = hmac.new(WHITELIST_HMAC_KEY.encode(), json.dumps(payload).encode(), hashlib.sha256).hexdigest()
        if not sig or not hmac.compare_digest(expected, sig):
            logger.warning("Whitelist signature mismatch - ignoring whitelist on load")
            return []
    return payload

# load allowed users (secure)
_allowed_users: Set[int] = set()
if os.path.exists(ALLOWED_USERS_FILE):
    try:
        blob = _load_json_sync(ALLOWED_USERS_FILE, {})
        users = _whitelist_unpack(blob)
        _allowed_users = set(int(x) for x in users)
    except Exception:
        logger.exception("Failed to load whitelist securely")

async def _save_allowed_users():
    blob = _whitelist_pack(sorted(list(_allowed_users)))
    await _save_json_atomic(ALLOWED_USERS_FILE, blob)

# -------------------------
# HTTP client with retry/backoff
# -------------------------
class HttpClient:
    def __init__(self, timeout:int=HTTP_TIMEOUT, retries:int=HTTP_RETRIES):
        self._client = httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "FPL-Enterprise-Bot/1.0"})
        self._retries = retries

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10),
           retry=retry_if_exception_type((httpx.TransportError, httpx.ReadTimeout)))
    async def get(self, url: str, **kwargs) -> Optional[httpx.Response]:
        resp = await self._client.get(url, **kwargs)
        if resp.status_code >= 500:
            # raise to trigger retry
            raise httpx.HTTPStatusError("server error", request=resp.request, response=resp)
        return resp

    async def close(self):
        await self._client.aclose()

_http = HttpClient(timeout=HTTP_TIMEOUT, retries=HTTP_RETRIES)

async def fetch_json(url: str) -> Optional[dict]:
    try:
        resp = await _http.get(url)
        if resp is None:
            return None
        if resp.status_code != 200:
            logger.warning("fetch_json %s -> status %s", url, resp.status_code)
            return None
        return resp.json()
    except Exception:
        logger.exception("fetch_json failed %s", url)
        return None

async def fetch_text(url: str) -> Optional[str]:
    try:
        resp = await _http.get(url)
        if resp is None:
            return None
        if resp.status_code != 200:
            logger.warning("fetch_text %s -> status %s", url, resp.status_code)
            return None
        return resp.text
    except Exception:
        logger.exception("fetch_text failed %s", url)
    return None

# -------------------------
# Bootstrap cache + name index
# -------------------------
_BOOTSTRAP_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None, "elements": [], "el_map": {}, "name_index": {}}

async def fetch_bootstrap_cached(force: bool = False) -> Optional[dict]:
    loop = asyncio.get_event_loop()
    now = loop.time()
    if _BOOTSTRAP_CACHE["data"] and not force and (now - _BOOTSTRAP_CACHE["ts"] < BOOTSTRAP_TTL_SECS):
        return _BOOTSTRAP_CACHE["data"]
    data = await fetch_json(FPL_BOOTSTRAP)
    if not data:
        return None
    elements = data.get("elements", []) or []
    el_map = {}
    name_index = defaultdict(list)
    for el in elements:
        try:
            eid = int(el.get("id"))
            el_map[eid] = el
            name = str(el.get("web_name", "")).lower().strip()
            if name:
                name_index[name].append(el)
        except Exception:
            continue
    _BOOTSTRAP_CACHE.update({"ts": now, "data": data, "elements": elements, "el_map": el_map, "name_index": name_index})
    metrics["bootstrap_refresh"] += 1
    return data

# -------------------------
# Validation schema for LiveFPL row (STRICT) using fastjsonschema
# -------------------------
LIVEFPL_ROW_SCHEMA = {
    "type": "object",
    "properties": {
        "Name": {"type": "string"},
        "Pos": {"type": "string"},
        "Team": {"type": "string"},
        "Price": {"type": "string"},
        "Target": {"type": "string"},
        "Owned by": {"type": "string"},
    },
    "required": ["Name", "Price", "Target", "Owned by"],
    "additionalProperties": True
}
validate_live_row = fastjsonschema.compile(LIVEFPL_ROW_SCHEMA)

def validate_row_strict(row: Dict[str, Any]) -> bool:
    try:
        # will raise error if invalid
        validate_live_row(row)
        return True
    except Exception as e:
        logger.warning("LiveFPL row failed validation (strict): %s | row=%s", e, row.get("Name"))
        metrics["validation_failures"] += 1
        return False

# -------------------------
# Parse helpers
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

def safe_extract_tables(soup: BeautifulSoup, hints: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    res = {}
    for h in hints:
        rows = extract_table(soup, h)
        if rows:
            # strict validate each row
            valid_rows = []
            for r in rows:
                if validate_row_strict(r):
                    valid_rows.append(r)
            res[h] = valid_rows
        else:
            logger.warning("Failed to parse '%s' table; falling back to empty", h)
            res[h] = []
    return res

# -------------------------
# FPL team mapping (team_code -> ABBR)
# -------------------------
FPL_TEAM_ABBR = {
    1:  "ARS", 2:  "AVL", 3:  "BOU", 4:  "BRE", 5:  "BHA",
    6:  "BUR", 7:  "CHE", 8:  "CRY", 9:  "EVE", 10: "FUL",
    11: "LIV", 12: "LUT", 13: "MCI", 14: "MUN", 15: "NEW",
    16: "NFO", 17: "SHU", 18: "TOT", 19: "WHU", 20: "WOL",
}

# -------------------------
# Smart matching using name_index -> team -> pos -> price -> fallback
# -------------------------
def _pos_code_to_str(code: int) -> str:
    return {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}.get(int(code), "")

def find_element_by_name_smart(name: str, team_hint: str, pos_hint: str, price_hint: float, name_index: dict, elements: List[dict]) -> Optional[dict]:
    name_low = (name or "").lower().strip()
    candidates = name_index.get(name_low, [])
    if not candidates:
        candidates = [el for el in elements if str(el.get("web_name", "")).lower().strip() == name_low]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    # filter by team
    if team_hint:
        filtered = []
        for el in candidates:
            tc = el.get("team_code") or el.get("team")
            try:
                tc_int = int(tc)
            except Exception:
                tc_int = None
            abbr = FPL_TEAM_ABBR.get(tc_int) if tc_int is not None else None
            if abbr and abbr.upper() == team_hint.upper():
                filtered.append(el)
        if filtered:
            candidates = filtered
            if len(candidates) == 1:
                return candidates[0]
    # filter by pos
    if pos_hint:
        filtered = [el for el in candidates if _pos_code_to_str(el.get("element_type")) == pos_hint.upper()]
        if filtered:
            candidates = filtered
            if len(candidates) == 1:
                return candidates[0]
    # filter by price
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
    metrics["ambiguous_matches"] += 1
    return candidates[0]

# -------------------------
# League cache
# -------------------------
async def get_current_gw() -> int:
    data = await fetch_bootstrap_cached()
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
    await _save_json_atomic(LEAGUE_CACHE_FILE, league_cache)
    return s

# -------------------------
# Sorting/filtering sections (apply owned >=1% filter)
# -------------------------
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
# Formatting compact table
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
                tc = el_map[el_id].get("team_code")
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
# Price change detector & bootstraps
# -------------------------
async def fetch_bootstrap_prices_map() -> Tuple[Dict[int, int], Dict[int, dict], List[dict]]:
    data = await fetch_bootstrap_cached()
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
# Circuit Breaker for LiveFPL
# -------------------------
class CircuitBreaker:
    def __init__(self, fail_threshold:int=3, cooldown_seconds:int=600):
        self.fail_threshold = fail_threshold
        self.cooldown_seconds = cooldown_seconds
        self.fail_count = 0
        self.open_until = 0  # epoch

    def record_success(self):
        self.fail_count = 0
        self.open_until = 0

    def record_failure(self):
        self.fail_count += 1
        if self.fail_count >= self.fail_threshold:
            self.open_until = int(_time.time()) + self.cooldown_seconds
            logger.warning("Circuit breaker opened for %ds after %d failures", self.cooldown_seconds, self.fail_count)
            metrics["circuit_opened"] += 1

    def is_open(self) -> bool:
        if self.open_until == 0:
            return False
        if _time.time() >= self.open_until:
            # auto-half-open: allow trial
            logger.info("Circuit breaker cooldown expired; allowing trial")
            self.fail_count = 0
            self.open_until = 0
            return False
        return True

livefpl_cb = CircuitBreaker(fail_threshold=3, cooldown_seconds=600)

# -------------------------
# /prices builder with CB
# -------------------------
async def build_prices_sections_and_format() -> str:
    # check circuit breaker
    if livefpl_cb.is_open():
        logger.warning("LiveFPL circuit open — returning fallback message")
        return "<code>\n       Price changes summary\n-------------------------------\nLiveFPL temporarily unavailable — try later.\n</code>"

    txt = await fetch_text(LIVEFPL_PRICES)
    if not txt:
        livefpl_cb.record_failure()
        return "<code>\n       Price changes summary\n-------------------------------\nCould not fetch LiveFPL prices.\n</code>"
    # success
    livefpl_cb.record_success()
    try:
        with open(LAST_LIVEFPL_HTML, "w", encoding="utf-8") as f:
            f.write(txt[:200000])
    except Exception:
        logger.exception("Failed to save last LiveFPL HTML")
    soup = BeautifulSoup(txt, "html.parser")
    raw_sections = safe_extract_tables(soup, ["Already reached target", "Projected to reach target", "Others who will be close"])
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
    # attach element ids via smart matching using bootstrap cache
    data = await fetch_bootstrap_cached()
    elements = data.get("elements", []) if data else []
    el_map = _BOOTSTRAP_CACHE.get("el_map", {})
    name_index = _BOOTSTRAP_CACHE.get("name_index", {})
    for title, rows in raw_sections.items():
        for r in rows:
            name = (r.get("Name") or "").strip()
            team_hint = (r.get("Team") or "").strip()
            pos_hint = (r.get("Pos") or "").strip()
            try:
                price_hint = float(str(r.get("Price") or "0").replace("£", "").strip())
            except Exception:
                price_hint = 0.0
            found = find_element_by_name_smart(name, team_hint, pos_hint, price_hint, name_index, elements)
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
# Authorization + secure send
# -------------------------
def is_authorized_update(update: Update) -> bool:
    try:
        chat = update.effective_chat
        user = update.effective_user
        if chat is None or user is None:
            return False
        uid = int(user.id)
        # owner in private always allowed
        if uid == OWNER_ID and chat.type == "private":
            return True
        # allowed group: whitelist side effect
        if chat.id == ALLOWED_GROUP_ID:
            if uid not in _allowed_users:
                _allowed_users.add(uid)
                asyncio.create_task(_save_allowed_users())
            return True
        # private: owner or whitelisted only
        if chat.type == "private":
            return uid in _allowed_users or uid == OWNER_ID
        # other chats denied silently
        return False
    except Exception:
        logger.exception("Authorization check failed")
        return False

def notifications_enabled() -> bool:
    return bool(notif_flag_obj.get("enabled", True))

async def send_message_secure(app: Application, text: str, *, silent: bool = True, parse_mode: str = "HTML"):
    if not notifications_enabled():
        logger.debug("Notifications disabled - blocking secure send")
        return
    try:
        await app.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=text, parse_mode=parse_mode,
                                   disable_web_page_preview=True, disable_notification=silent)
        metrics["messages_sent"] += 1
    except Exception:
        logger.exception("send_message_secure failed")
        metrics["send_errors"] += 1

# -------------------------
# Scheduler helpers (precise sleep)
# -------------------------
async def sleep_until(target_dt_utc: datetime):
    while True:
        now = datetime.now(timezone.utc)
        secs = (target_dt_utc - now).total_seconds()
        if secs <= 0:
            return
        await asyncio.sleep(min(secs, 60))

def next_daily_utc5(hour:int, minute:int, now: Optional[datetime]=None) -> datetime:
    if now is None:
        now = datetime.now(timezone.utc)
    tz_base = timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))
    local_now = now.astimezone(tz_base)
    target_local = datetime.combine(local_now.date(), time(hour, minute), tzinfo=tz_base)
    if local_now >= target_local:
        target_local = target_local + timedelta(days=1)
    return target_local.astimezone(timezone.utc)

def _utc5_window_utc_for_today(now_utc: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    tz_base = timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))
    local = now_utc.astimezone(tz_base)
    start_local = datetime.combine(local.date(), time(5,45), tzinfo=tz_base)
    end_local = datetime.combine(local.date(), time(8,0), tzinfo=tz_base)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

# -------------------------
# Price change detector + daily sender
# -------------------------
_utc5_daily_task: Optional[asyncio.Task] = None
_change_detector_task: Optional[asyncio.Task] = None

async def utc5_daily_sender(app: Application):
    logger.info("UTC+5 daily sender started (23:00 UTC+5)")
    try:
        while True:
            next_send_utc = next_daily_utc5(23, 0)
            await sleep_until(next_send_utc)
            if not notifications_enabled():
                logger.info("Notifications disabled - skipping utc5 daily send")
            else:
                msg = await build_prices_sections_and_format()
                await send_message_secure(app, msg, silent=False)
                logger.info("Sent utc5 daily /prices to group %s", ALLOWED_GROUP_ID)
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("utc5_daily_sender cancelled")
    except Exception:
        logger.exception("utc5_daily_sender crashed")

async def price_change_detector(app: Application):
    logger.info("Price change detector started (05:45-08:00 UTC+5 every 5 min)")
    try:
        while True:
            start_utc, end_utc = _utc5_window_utc_for_today()
            await sleep_until(start_utc)
            today_iso = start_utc.astimezone(timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))).date().isoformat()
            if _price_check_state.get("last_checked_date") == today_iso:
                logger.info("Price check already done today - skipping window")
                next_start = start_utc + timedelta(days=1)
                await sleep_until(next_start)
                continue
            updates_found = False
            while datetime.now(timezone.utc) <= end_utc:
                if not notifications_enabled():
                    logger.info("Notifications disabled - breaking price check loop")
                    break
                try:
                    new_map, el_map, elements = await fetch_bootstrap_prices_map()
                    changes = detect_price_changes(prices_baseline, new_map)
                    if changes:
                        msg = format_price_changes_message(changes, el_map)
                        if msg:
                            await send_message_secure(app, msg, silent=True)
                            logger.info("Sent price-change notification to group %s", ALLOWED_GROUP_ID)
                        pb = {str(k): v for k, v in new_map.items()}
                        await _save_json_atomic(PRICES_BASELINE_FILE, pb)
                        global prices_baseline
                        prices_baseline = pb
                        updates_found = True
                        break
                    else:
                        if not prices_baseline:
                            pb = {str(k): v for k, v in new_map.items()}
                            await _save_json_atomic(PRICES_BASELINE_FILE, pb)
                            prices_baseline.update(pb)
                except Exception:
                    logger.exception("Error during price-change check")
                await asyncio.sleep(PRICE_CHANGE_POLL_SECONDS)
            if not updates_found and notifications_enabled():
                no_msg = "<code>\n" + center_text("Price changes summary", 40) + "\n" + "-------------------------------\n" + "No price changes today.\n" + "</code>"
                await send_message_secure(app, no_msg, silent=True)
                logger.info("Sent no-update summary to group %s", ALLOWED_GROUP_ID)
            _price_check_state["last_checked_date"] = today_iso
            await _save_json_atomic(PRICE_CHECK_STATE_FILE, _price_check_state)
            next_start = start_utc + timedelta(days=1)
            await sleep_until(next_start)
    except asyncio.CancelledError:
        logger.info("price_change_detector cancelled")
    except Exception:
        logger.exception("price_change_detector crashed")

# -------------------------
# GW38 disable
# -------------------------
async def check_and_disable_after_gw38():
    try:
        data = await fetch_bootstrap_cached()
        if not data:
            return
        for ev in data.get("events", []) or []:
            try:
                if int(ev.get("id", -1)) == GW_DISABLE_TARGET:
                    finished = bool(ev.get("finished") or ev.get("is_finished") or False)
                    if finished:
                        notif_flag_obj["enabled"] = False
                        await _save_json_atomic(NOTIF_FLAG_FILE, notif_flag_obj)
                        logger.info("GW38 finished -> notifications disabled")
                    return
            except Exception:
                continue
    except Exception:
        logger.exception("check_and_disable_after_gw38 failed")

# -------------------------
# Handlers
# -------------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_update(update):
        return
    text = (
        "FPL Prices Bot (enterprise)\n"
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
    parsed = args[0].strip()
    parsed_tz = None
    # accept +N or IANA
    if (parsed.startswith("+") or parsed.startswith("-")) and parsed[1:].replace(".", "", 1).isdigit():
        parsed_tz = f"UTC{parsed}"
    else:
        try:
            ZoneInfo(parsed)
            parsed_tz = parsed
        except Exception:
            parsed_tz = None
    if not parsed_tz:
        await update.message.reply_text("Unknown timezone. Use IANA like Asia/Almaty or offset +5")
        return
    user_timezones[update.effective_user.id] = parsed_tz
    await _save_json_atomic(USER_TZ_FILE, {str(k): v for k, v in user_timezones.items()})
    if update.effective_chat.type == "private":
        await update.message.reply_text(f"Timezone set to {parsed_tz}")
    else:
        await send_message_secure(context.application, f"Timezone set to {parsed_tz}", silent=False)

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
    enabled = bool(notif_flag_obj.get("enabled", True))
    text = f"Notifications enabled: {enabled}\nMetrics: sent={metrics.get('messages_sent',0)}, errors={metrics.get('send_errors',0)}"
    if update.effective_chat.type == "private":
        await update.message.reply_text(text)
    else:
        await send_message_secure(context.application, text, silent=False)

async def _group_message_logger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_chat and update.effective_chat.id == ALLOWED_GROUP_ID:
            is_authorized_update(update)
    except Exception:
        pass

# -------------------------
# Health state
# -------------------------
HEALTH = {
    "start_time": datetime.now(timezone.utc).isoformat(),
    "last_livefpl_success": None,
    "last_bootstrap_success": None,
    "last_scheduler_tick": None,
    "last_price_check": None,
    "uptime_seconds": 0
}

def update_health(key: str, value: Any):
    HEALTH[key] = value

# -------------------------
# Lifecycle: start background tasks
# -------------------------
async def start_background_tasks(app: Application):
    try:
        await check_and_disable_after_gw38()
        global _utc5_daily_task, _change_detector_task
        if _utc5_daily_task is None or _utc5_daily_task.done():
            _utc5_daily_task = asyncio.create_task(utc5_daily_sender(app))
        if _change_detector_task is None or _change_detector_task.done():
            _change_detector_task = asyncio.create_task(price_change_detector(app))
        logger.info("Background tasks started")
    except Exception:
        logger.exception("Failed to start background tasks")

async def stop_background_tasks():
    global _utc5_daily_task, _change_detector_task
    tasks = [_utc5_daily_task, _change_detector_task]
    for t in tasks:
        if t:
            t.cancel()
    await asyncio.sleep(0.2)
    try:
        await _http.close()
    except Exception:
        pass

# -------------------------
# Application entry
# -------------------------
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("prices", prices_cmd))
    app.add_handler(CommandHandler("settz", settz_cmd))
    app.add_handler(CommandHandler("mytz", mytz_cmd))
    app.add_handler(CommandHandler("notify_status", notify_status_cmd))
    app.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), _group_message_logger))

    async def _on_start(_app: Application):
        logger.info("Bot started (PTB %s)", PTB_VERSION)
        update_health("last_scheduler_tick", datetime.now(timezone.utc).isoformat())
        await start_background_tasks(_app)
    async def _on_stop(_app: Application):
        logger.info("Bot stopping")
        await stop_background_tasks()
    app.post_init = _on_start
    app.post_shutdown = _on_stop

    logger.info("Starting bot...")
    app.run_polling()

if __name__ == "__main__":
    main()
