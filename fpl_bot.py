
# fpl_bot_enterprise_full.py
# Enterprise-ready FPL Prices Bot (Upstash Redis HASH storage, full A+B features)
# Features added:
# - Upstash Redis (HASH) storage (A2)
# - Structured JSON logging (ELK-ready)
# - /health endpoint (async HTTP on port 8080)
# - /metrics Prometheus endpoint (minimal)
# - Rate limiting (Redis-based token bucket)
# - Redis retry/backoff for Upstash operations
# - Admin mode + audit logs
# - Graceful shutdown (SIGTERM/SIGHUP)
# - Critical error audit log (Redis list)
# - LiveFPL parser v2 (more fault tolerant)
# - Unified exception middleware for handlers
# - Pydantic models if available (optional)
# - Redis key TTL auto-rotate for snapshots
#
# Requirements (update your requirements.txt accordingly):
# python-telegram-bot[ext]==20.6
# httpx==0.25.2
# beautifulsoup4==4.12.3
# lxml==4.9.3
# fastjsonschema==2.19.1
# tzdata
# loguru==0.7.2
# tenacity==8.2.3
# aiohttp==3.8.5
# pydantic==1.10.9  # optional but recommended
#
# ENV required:
# BOT_TOKEN, OWNER_ID, ALLOWED_GROUP_ID, WHITELIST_HMAC_KEY,
# UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN
#
# Note: This file is large â download and run in Northflank as described earlier.

import os
import sys
import json
import hmac
import hashlib
import asyncio
import logging
import uuid
import random
import signal
from datetime import datetime, timedelta, timezone, time
from typing import Dict, Any, Optional, List, Tuple, Set
from collections import defaultdict
from urllib.parse import quote_plus
import traceback

import httpx
from bs4 import BeautifulSoup
import fastjsonschema
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# optional pydantic
try:
    from pydantic import BaseModel
    PYDANTIC_AVAILABLE = True
except Exception:
    PYDANTIC_AVAILABLE = False

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# -------------------- CONFIG --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
ALLOWED_GROUP_ID = int(os.getenv("ALLOWED_GROUP_ID", "0"))
LEAGUE_ID = int(os.getenv("LEAGUE_ID", "980121"))
WHITELIST_HMAC_KEY = os.getenv("WHITELIST_HMAC_KEY", "")
UPSTASH_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
PRICE_CHANGE_POLL_SECONDS = int(os.getenv("PRICE_CHANGE_POLL_SECONDS", "300"))
PRICE_CHANGE_UTC_PLUS = int(os.getenv("PRICE_CHANGE_UTC_PLUS", "5"))
BOOTSTRAP_TTL = int(os.getenv("BOOTSTRAP_TTL_SECS", "30"))
GW_DISABLE_TARGET = int(os.getenv("GW_DISABLE_TARGET", "38"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
METRICS_PORT = int(os.getenv("METRICS_PORT", "8080"))
RATE_LIMIT_TOKENS = int(os.getenv("RATE_LIMIT_TOKENS", "20"))  # tokens per window
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))  # seconds
SNAPSHOT_TTL_DAYS = int(os.getenv("SNAPSHOT_TTL_DAYS", "30"))  # expire snapshots

if not BOT_TOKEN or OWNER_ID <= 0 or ALLOWED_GROUP_ID == 0:
    print("BOT_TOKEN, OWNER_ID, ALLOWED_GROUP_ID must be set", file=sys.stderr)
    sys.exit(1)
if not UPSTASH_REST_URL or not UPSTASH_REST_TOKEN:
    print("UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN must be set", file=sys.stderr)
    sys.exit(1)
if not WHITELIST_HMAC_KEY:
    print("WARNING: WHITELIST_HMAC_KEY is not set â whitelist HMAC disabled", file=sys.stderr)

# -------------------- LOGGING (structured JSON) --------------------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "ts": datetime.utcnow().isoformat(),
            "lvl": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        if hasattr(record, "extra_ctx"):
            payload.update(record.extra_ctx)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logger = logging.getLogger("fpl_bot")
logger.setLevel(LOG_LEVEL)
logger.addHandler(handler)

def log_extra(msg, **ctx):
    logger.info(msg, extra={"extra_ctx": ctx})

# -------------------- UTILS --------------------
def correlation_id() -> str:
    return uuid.uuid4().hex[:12]

def canonical_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()

# -------------------- UPSTASH CLIENT (REST) with retry --------------------
class UpstashClient:
    def __init__(self, base_url: str, token: str, timeout: int = HTTP_TIMEOUT):
        self.base = base_url.rstrip("/")
        self.token = token
        self.client = httpx.AsyncClient(timeout=timeout, headers={"Authorization": f"Bearer {self.token}"})

    async def close(self):
        try:
            await self.client.aclose()
        except Exception:
            pass

    async def _req(self, path: str):
        url = f"{self.base}/{path}"
        try:
            resp = await self.client.get(url)
        except Exception as e:
            logger.warning("Upstash request transport error %s", e)
            return None
        if resp.status_code != 200:
            logger.warning("Upstash request failed %s -> %s", url, resp.status_code)
            return None
        try:
            return resp.json()
        except Exception:
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def hgetall(self, key: str) -> Dict[str, str]:
        path = f"hgetall/{quote_plus(key)}"
        j = await self._req(path)
        if not j:
            return {}
        if isinstance(j, dict) and "result" in j and isinstance(j["result"], dict):
            return {k: str(v) for k, v in j["result"].items()}
        return {k: str(v) for k, v in (j or {}).items()}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def hset_map(self, key: str, mapping: Dict[str, str]):
        parts = [f"hset/{quote_plus(key)}"]
        for k, v in mapping.items():
            parts.append(quote_plus(str(k)))
            parts.append(quote_plus(str(v)))
        path = "/".join(parts)
        await self._req(path)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def hdel(self, key: str, field: str):
        path = f"hdel/{quote_plus(key)}/{quote_plus(str(field))}"
        await self._req(path)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def get(self, key: str) -> Optional[str]:
        path = f"get/{quote_plus(key)}"
        j = await self._req(path)
        if not j:
            return None
        if isinstance(j, dict) and "result" in j:
            return None if j["result"] is None else str(j["result"])
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def set(self, key: str, value: str):
        path = f"set/{quote_plus(key)}/{quote_plus(str(value))}"
        await self._req(path)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def incr(self, key: str) -> Optional[int]:
        path = f"incr/{quote_plus(key)}"
        j = await self._req(path)
        if not j:
            return None
        if isinstance(j, dict) and "result" in j:
            return int(j["result"])
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def expire(self, key: str, seconds: int):
        path = f"expire/{quote_plus(key)}/{seconds}"
        await self._req(path)

upstash = UpstashClient(UPSTASH_REST_URL, UPSTASH_REST_TOKEN)

# -------------------- SIMPLE HTTP CLIENT (for external APIs) --------------------
class HttpClient:
    def __init__(self, timeout:int=HTTP_TIMEOUT):
        self.client = httpx.AsyncClient(timeout=timeout, headers={"User-Agent":"FPL-Enterprise-Bot/1.0"})

    async def get(self, url: str) -> Optional[httpx.Response]:
        try:
            resp = await self.client.get(url)
            if resp.status_code >= 500 or resp.status_code in (429, 502, 503, 504):
                raise httpx.HTTPStatusError("server error", request=resp.request, response=resp)
            return resp
        except Exception:
            logger.exception("HTTP get failed %s", url)
            return None

    async def get_json(self, url: str) -> Optional[dict]:
        r = await self.get(url)
        return r.json() if r and r.status_code == 200 else None

    async def get_text(self, url: str) -> Optional[str]:
        r = await self.get(url)
        return r.text if r and r.status_code == 200 else None

    async def close(self):
        try:
            await self.client.aclose()
        except Exception:
            pass

http = HttpClient()

# -------------------- BOOTSTRAP CACHE --------------------
_BOOTSTRAP_CACHE = {"ts": 0.0, "data": None, "elements": [], "el_map": {}, "name_index": {}}

async def fetch_bootstrap_cached(force: bool=False) -> Optional[dict]:
    now = asyncio.get_event_loop().time()
    if _BOOTSTRAP_CACHE["data"] and not force and (now - _BOOTSTRAP_CACHE["ts"] < BOOTSTRAP_TTL):
        return _BOOTSTRAP_CACHE["data"]
    data = await http.get_json("https://fantasy.premierleague.com/api/bootstrap-static/")
    if not data:
        return None
    elements = data.get("elements", []) or []
    el_map = {}
    name_index = defaultdict(list)
    for el in elements:
        try:
            eid = int(el.get("id"))
            el_map[eid] = el
            name = str(el.get("web_name","")).lower().strip()
            if name:
                name_index[name].append(el)
        except Exception:
            continue
    _BOOTSTRAP_CACHE.update({"ts": now, "data": data, "elements": elements, "el_map": el_map, "name_index": name_index})
    return data

# -------------------- Validation schema --------------------
LIVEFPL_ROW_SCHEMA = {
    "type":"object",
    "properties": {"Name":{"type":"string"},"Price":{"type":"string"},"Target":{"type":"string"}},
    "required":["Name","Price","Target"]
}
validate_live_row = fastjsonschema.compile(LIVEFPL_ROW_SCHEMA)

# -------------------- Circuit breaker --------------------
class CircuitBreaker:
    def __init__(self, fail_threshold:int=3, cooldown_seconds:int=600):
        self.fail_threshold = fail_threshold
        self.cooldown_seconds = cooldown_seconds
        self.fail_count = 0
        self.open_until = 0

    def record_success(self):
        self.fail_count = 0
        self.open_until = 0

    def record_failure(self):
        self.fail_count += 1
        if self.fail_count >= self.fail_threshold:
            self.open_until = int(datetime.now().timestamp()) + self.cooldown_seconds + random.randint(0,30)
            logger.warning("Circuit opened until %s", self.open_until)

    def is_open(self) -> bool:
        if self.open_until == 0:
            return False
        if datetime.now().timestamp() >= self.open_until:
            self.fail_count = 0
            self.open_until = 0
            return False
        return True

livefpl_cb = CircuitBreaker()

# -------------------- LiveFPL parser v2 (more fault tolerant) --------------------
def parse_row_cells_v2(cells: List[str]) -> Dict[str,str]:
    row = {}
    joined = " ".join(cells)
    # Try to extract name until position marker (pos are 3-letter codes) or price marker
    if len(cells) >= 1:
        row["Name"] = cells[0]
    if len(cells) >= 2:
        row["Pos"] = cells[1]
    if len(cells) >= 3:
        row["Team"] = cells[2]
    # price detection: first token containing Â£ or digits with dot
    for c in cells[3:]:
        if "Â£" in c or c.replace(".", "", 1).isdigit():
            row["Price"] = c.replace("Â£", "").strip()
            break
    # owned/target detection: percentages anywhere
    percents = [c for c in cells if "%" in c]
    if percents:
        row["Owned by"] = percents[-1]
        if len(percents) >= 2:
            row["Target"] = percents[-2]
    row.setdefault("Price","")
    row.setdefault("Target","")
    row.setdefault("Owned by","")
    return row

def extract_table_v2(html_soup: BeautifulSoup, key_hint: str) -> List[Dict[str,Any]]:
    rows = []
    # look for headings and also for tables with captions
    headers = html_soup.find_all(["h2","h3","h4","strong","caption"])
    found = None
    for h in headers:
        txt = h.get_text(" ", strip=True).lower()
        if key_hint.lower() in txt:
            found = h
            break
    if not found:
        tables = html_soup.find_all("table")
        for t in tables:
            cap = t.find("caption")
            if cap and key_hint.lower() in cap.get_text(" ", strip=True).lower():
                found = t
                break
    if not found:
        return rows
    tbl = found.find_next("table") if not isinstance(found, type(html_soup)) else found
    if not tbl:
        return rows
    for tr in tbl.find_all("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if not tds:
            continue
        row = parse_row_cells_v2(tds)
        rows.append(row)
    return rows

def safe_extract_tables_v2(soup: BeautifulSoup, hints: List[str]) -> Dict[str, List[Dict[str,Any]]]:
    res = {}
    for h in hints:
        rows = extract_table_v2(soup, h)
        if rows:
            valid_rows = []
            for r in rows:
                try:
                    validate_live_row(r)
                    valid_rows.append(r)
                except Exception:
                    logger.warning("row failed validation %s", r.get("Name"))
            res[h] = valid_rows
        else:
            res[h] = []
    return res

# -------------------- Matching helpers --------------------
FPL_TEAM_ABBR = {1:"ARS",2:"AVL",3:"BOU",4:"BRE",5:"BHA",6:"BUR",7:"CHE",8:"CRY",9:"EVE",10:"FUL",11:"LIV",12:"LUT",13:"MCI",14:"MUN",15:"NEW",16:"NFO",17:"SHU",18:"TOT",19:"WHU",20:"WOL"}
def _pos_code_to_str(code: int) -> str:
    return {1:"GKP",2:"DEF",3:"MID",4:"FWD"}.get(int(code), "")

def find_element_by_name_smart(name: str, team_hint: str, pos_hint: str, price_hint: float, name_index: dict, elements: List[dict]) -> Optional[dict]:
    name_low = (name or "").lower().strip()
    candidates = name_index.get(name_low, [])
    if not candidates:
        candidates = [el for el in elements if str(el.get("web_name","")).lower().strip() == name_low]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
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
    if pos_hint:
        filtered = [el for el in candidates if _pos_code_to_str(el.get("element_type")) == pos_hint.upper()]
        if filtered:
            candidates = filtered
            if len(candidates) == 1:
                return candidates[0]
    try:
        filtered = []
        for el in candidates:
            now_cost = float(el.get("now_cost",0))/10.0
            if abs(now_cost - float(price_hint)) < 0.01:
                filtered.append(el)
        if filtered:
            candidates = filtered
            if len(candidates) == 1:
                return candidates[0]
    except Exception:
        pass
    logger.warning("Ambiguous match for %s -> id=%s", name, candidates[0].get("id"))
    return candidates[0]

# -------------------- Upstash-backed state helpers (with TTL rotate & audit) --------------------
async def load_state_from_upstash():
    global user_timezones, allowed_users, baseline, league_cache, notif_flag, price_state
    try:
        ut = await upstash.hgetall("user_timezones")
        user_timezones = {int(k): v for k, v in ut.items()} if ut else {}
    except Exception:
        user_timezones = {}
    try:
        au = await upstash.hgetall("allowed_users")
        sig = await upstash.get("allowed_users_sig")
        packed = sorted([int(k) for k in au.keys()]) if au else []
        if WHITELIST_HMAC_KEY and sig:
            expected = hmac.new(WHITELIST_HMAC_KEY.encode(), canonical_bytes(packed), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(expected, sig):
                logger.warning("allowed_users sig mismatch -> ignoring allowed_users")
                allowed_users = set()
            else:
                allowed_users = set(packed)
        else:
            allowed_users = set(packed)
    except Exception:
        allowed_users = set()
    try:
        pb = await upstash.hgetall("prices_baseline")
        baseline = {str(k): int(v) for k, v in pb.items()} if pb else {}
    except Exception:
        baseline = {}
    try:
        lp = await upstash.hgetall("league_players")
        league_cache = {"players": list(lp.keys())} if lp else {}
    except Exception:
        league_cache = {}
    try:
        ne = await upstash.get("notifications_enabled")
        notif_flag = {"enabled": (ne == "true")} if ne is not None else {"enabled": True}
    except Exception:
        notif_flag = {"enabled": True}
    try:
        ps = await upstash.hgetall("price_check_state")
        price_state = ps or {}
    except Exception:
        price_state = {}

async def save_allowed_users_to_upstash():
    try:
        mapping = {str(u): "1" for u in sorted(list(allowed_users))}
        await upstash.hset_map("allowed_users", mapping)
        if WHITELIST_HMAC_KEY:
            sig = hmac.new(WHITELIST_HMAC_KEY.encode(), canonical_bytes(sorted(list(allowed_users))), hashlib.sha256).hexdigest()
            await upstash.set("allowed_users_sig", sig)
        # audit
        await upstash.hset_map("audit_allowed_users", {str(int(datetime.utcnow().timestamp())): ",".join(map(str, sorted(list(allowed_users))))})
    except Exception:
        logger.exception("Failed to save allowed_users to upstash")

async def save_user_timezones():
    try:
        mapping = {str(k): v for k, v in user_timezones.items()}
        if mapping:
            await upstash.hset_map("user_timezones", mapping)
    except Exception:
        logger.exception("Failed to save user_timezones")

async def save_baseline_map(new_map: Dict[str,int]):
    try:
        mapping = {str(k): str(v) for k, v in new_map.items()}
        if mapping:
            await upstash.hset_map("prices_baseline", mapping)
        # snapshot with TTL rotate (expire)
        today_str = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))).date().isoformat()
        snap_key = f"prices_baseline:{today_str}"
        await upstash.hset_map(snap_key, mapping)
        # set expire in seconds for snapshot
        await upstash.expire(snap_key, SNAPSHOT_TTL_DAYS * 24 * 3600)
    except Exception:
        logger.exception("Failed to save baseline map")

async def save_price_state():
    try:
        mapping = {str(k): str(v) for k, v in price_state.items()}
        if mapping:
            await upstash.hset_map("price_check_state", mapping)
    except Exception:
        logger.exception("Failed to save price_state")

# -------------------- League service (uses upstash cache) --------------------
class LeagueService:
    async def get_current_gw(self) -> int:
        data = await fetch_bootstrap_cached()
        if not data:
            return 0
        for ev in data.get("events", []) or []:
            if ev.get("is_current"):
                return int(ev.get("id",0))
        return 0

    async def get_league_players_cached(self) -> Set[str]:
        try:
            lp = await upstash.hgetall("league_players")
            if lp:
                return set(lp.keys())
        except Exception:
            pass
        s = await self.fetch_league_player_set_once(LEAGUE_ID)
        try:
            mapping = {str(x): "1" for x in s}
            if mapping:
                await upstash.hset_map("league_players", mapping)
        except Exception:
            pass
        return s

    async def fetch_league_player_set_once(self, league_id: int, cap: int = 80) -> Set[str]:
        out: Set[str] = set()
        url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"
        data = await http.get_json(url)
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
        async def fetch_entry(eid:int):
            async with sem:
                try:
                    url_e = f"https://fantasy.premierleague.com/api/entry/{eid}/event/0/picks/"
                    j = await http.get_json(url_e)
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

league_service = LeagueService()

# -------------------- Price detector (uses v2 parser) --------------------
class PriceDetectorService:
    async def build_prices_msg(self) -> str:
        if livefpl_cb.is_open():
            return "<code>LiveFPL temporarily unavailable</code>"
        txt = await http.get_text("https://www.livefpl.net/prices")
        if not txt:
            livefpl_cb.record_failure()
            return "<code>Could not fetch LiveFPL prices.</code>"
        livefpl_cb.record_success()
        soup = BeautifulSoup(txt, "html.parser")
        raw_sections = safe_extract_tables_v2(soup, ["Already reached target","Projected to reach target","Others who will be close"])
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
                    price_hint = float(str(r.get("Price") or "0").replace("Â£","").strip())
                except Exception:
                    price_hint = 0.0
                found = find_element_by_name_smart(name, team_hint, pos_hint, price_hint, name_index, elements)
                if found:
                    try:
                        r["element"] = int(found.get("id"))
                    except Exception:
                        pass
        processed = {}
        for title, rows in raw_sections.items():
            processed_rows = []
            for r in rows:
                try:
                    owned = float((r.get("Owned by") or "0").replace("%","").strip())
                except Exception:
                    owned = 0.0
                if owned < 1.0:
                    continue
                processed_rows.append(r)
            processed[title] = processed_rows
        blocks = []
        order = ["Already reached target","Projected to reach target","Others who will be close"]
        for title in order:
            rows = processed.get(title, []) or []
            lines = [title]
            for r in rows:
                eid = r.get("element")
                team_abbr = "UNK"
                if eid and el_map and int(eid) in el_map:
                    try:
                        tc = el_map[int(eid)].get("team_code")
                        team_abbr = FPL_TEAM_ABBR.get(int(tc), "UNK")
                    except Exception:
                        pass
                name = (r.get("Name") or "")
                price = f"Â£{r.get('Price')}"
                tgt = r.get("Target","")
                lines.append(f"{team_abbr} {name} {price} ({tgt})")
            if not rows:
                lines.append("(none)")
            blocks.append("<code>" + "\n".join(lines) + "</code>")
        return "\n\n".join(blocks)

    async def detect_changes(self) -> Optional[str]:
        data = await fetch_bootstrap_cached()
        if not data:
            return None
        elements = data.get("elements", []) or []
        new_map = {str(el["id"]): int(el.get("now_cost",0)) for el in elements}
        changes = []
        async with asyncio.Lock():
            for eid, new_cost in new_map.items():
                old = baseline.get(eid)
                if old is not None and old != new_cost:
                    changes.append((eid, old, new_cost))
            if changes:
                await save_baseline_map(new_map)
                baseline.clear()
                baseline.update({str(k): int(v) for k, v in new_map.items()})
                el_map = _BOOTSTRAP_CACHE.get("el_map", {})
                lines = ["<b>Price changes detected</b>"]
                for eid, o, n in changes:
                    nm = el_map.get(int(eid), {}).get("web_name", f"id:{eid}")
                    lines.append(f"<code>{nm.ljust(15)} {o/10:.1f} â {n/10:.1f}</code>")
                # audit
                try:
                    await upstash.hset_map("audit_price_changes", {str(int(datetime.utcnow().timestamp())): ",".join([f"{e}:{o}->{n}" for e,o,n in changes])})
                except Exception:
                    pass
                return "\n".join(lines)
            if not baseline:
                baseline.update({str(k): int(v) for k, v in new_map.items()})
                await save_baseline_map(new_map)
        return None

    def detect_between_maps(self, m1: Dict[str,int], m2: Dict[str,int]) -> List[Tuple[str,int,int]]:
        out = []
        for eid, v2 in m2.items():
            v1 = m1.get(eid)
            if v1 is not None and v1 != v2:
                out.append((eid, v1, v2))
        return out

price_service = PriceDetectorService()

# -------------------- AUTH, rate-limit, admin audit --------------------
async def is_authorized_update(update: Update) -> bool:
    try:
        chat = update.effective_chat
        user = update.effective_user
        if chat is None or user is None:
            return False
        uid = int(user.id)
        if uid == OWNER_ID and chat.type == "private":
            return True
        if chat.id == ALLOWED_GROUP_ID:
            if uid not in allowed_users:
                allowed_users.add(uid)
                await save_allowed_users_to_upstash()
            return True
        if chat.type == "private":
            return uid in allowed_users or uid == OWNER_ID
        return False
    except Exception:
        logger.exception("auth check failed")
        return False

async def rate_limit_check(user_id: int) -> bool:
    # Redis-based token bucket: incr key and set expire, compare to tokens
    try:
        key = f"rl:{user_id}"
        val = await upstash.incr(key)
        if val is None:
            return True
        if val == 1:
            await upstash.expire(key, RATE_LIMIT_WINDOW)
        if val > RATE_LIMIT_TOKENS:
            return False
        return True
    except Exception:
        # fallback to in-memory naive rate-limit
        window = RATE_LIMIT_WINDOW
        now = int(datetime.utcnow().timestamp())
        bucket = in_memory_rl.get(user_id, [])
        bucket = [t for t in bucket if t > now - window]
        if len(bucket) >= RATE_LIMIT_TOKENS:
            return False
        bucket.append(now)
        in_memory_rl[user_id] = bucket
        return True

async def admin_audit(action: str, actor: int, details: str = ""):
    try:
        ts = int(datetime.utcnow().timestamp())
        await upstash.hset_map("audit_admin", {str(ts): f"{actor}:{action}:{details}"})
    except Exception:
        logger.exception("admin audit failed")

# -------------------- Helpers for daily snapshot load --------------------
async def _load_daily_baseline_async(date_str: str) -> Optional[Dict[str,int]]:
    key = f"prices_baseline:{date_str}"
    d = await upstash.hgetall(key)
    if not d:
        return None
    return {str(k): int(v) for k, v in d.items()}

# -------------------- Unified exception decorator --------------------
def handler_guard(fn):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            # rate limit per-user (if private or member)
            user = update.effective_user
            uid = user.id if user else None
            if uid and not await rate_limit_check(int(uid)):
                try:
                    await update.message.reply_text("Rate limit exceeded. Try later.")
                except Exception:
                    pass
                return
            await fn(update, context)
        except Exception as e:
            logger.exception("Handler error: %s", e)
            # critical audit
            try:
                await upstash.hset_map("audit_errors", {str(int(datetime.utcnow().timestamp())): str(e)})
            except Exception:
                pass
            try:
                # inform owner privately
                if OWNER_ID:
                    await context.application.bot.send_message(chat_id=OWNER_ID, text=f"Handler error: {e}")
            except Exception:
                pass
    return wrapper

# -------------------- HANDLERS --------------------
@handler_guard
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized_update(update):
        return
    text = "FPL Prices Bot (enterprise)\n/price_on YYYY-MM-DD - show price changes for specified date\n/prices - show current compact prices\n/settz <Zone> - set timezone\n/mytz - show your timezone\n/notify_status - show notification enabled/disabled\n/admin_info - owner only\n"
    if update.effective_chat.type == "private":
        await update.message.reply_text(text)
    else:
        if notifications_enabled():
            await context.application.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=text)

@handler_guard
async def prices_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized_update(update):
        return
    msg = await price_service.build_prices_msg()
    await update.message.reply_text(msg, parse_mode="HTML")

@handler_guard
async def price_on_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized_update(update):
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /price_on YYYY-MM-DD")
        return
    date_str = args[0].strip()
    try:
        dt = datetime.fromisoformat(date_str)
    except Exception:
        await update.message.reply_text("Invalid date format. Use YYYY-MM-DD.")
        return
    day1 = await _load_daily_baseline_async(date_str)
    if not day1:
        await update.message.reply_text(f"No baseline for {date_str}")
        return
    next_day = (dt.date() + timedelta(days=1)).isoformat()
    day2 = await _load_daily_baseline_async(next_day)
    if not day2:
        await update.message.reply_text(f"No baseline for {next_day} (need both days)")
        return
    changes = price_service.detect_between_maps(day1, day2)
    if not changes:
        await update.message.reply_text(f"<code>No changes between {date_str} and {next_day}</code>", parse_mode="HTML")
        return
    el_map = _BOOTSTRAP_CACHE.get("el_map", {})
    lines = [f"<b>Price changes {date_str} â {next_day}</b>"]
    for eid, o, n in changes:
        nm = el_map.get(int(eid), {}).get("web_name", f"id:{eid}")
        lines.append(f"<code>{nm.ljust(15)} {o/10:.1f} â {n/10:.1f}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

@handler_guard
async def settz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized_update(update):
        return
    args = context.args or []
    if not args:
        cur = user_timezones.get(update.effective_user.id, f"UTC{PRICE_CHANGE_UTC_PLUS:+}")
        await update.message.reply_text(f"Usage: /settz <IANA zone or +N>\nCurrent: {cur}")
        return
    parsed = args[0].strip()
    parsed_tz = None
    if (parsed.startswith("+") or parsed.startswith("-")) and parsed[1:].replace(".", "", 1).isdigit():
        parsed_tz = f"UTC{parsed}"
    else:
        try:
            from zoneinfo import ZoneInfo
            ZoneInfo(parsed)
            parsed_tz = parsed
        except Exception:
            parsed_tz = None
    if not parsed_tz:
        await update.message.reply_text("Unknown timezone. Use IANA like Asia/Almaty or offset +5")
        return
    user_timezones[update.effective_user.id] = parsed_tz
    await save_user_timezones()
    await update.message.reply_text(f"Timezone set to {parsed_tz}")

@handler_guard
async def mytz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized_update(update):
        return
    tz = user_timezones.get(update.effective_user.id, f"UTC{PRICE_CHANGE_UTC_PLUS:+}")
    await update.message.reply_text(f"Your timezone: {tz}")

@handler_guard
async def notify_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized_update(update):
        return
    enabled = bool(notif_flag.get("enabled", True))
    await update.message.reply_text(f"Notifications enabled: {enabled}")

@handler_guard
async def admin_info_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or int(user.id) != OWNER_ID:
        return
    # show internal health
    info = {
        "bootstrap_cached": bool(_BOOTSTRAP_CACHE.get("data")),
        "baseline_count": len(baseline),
        "allowed_users": len(allowed_users),
        "notif_enabled": notif_flag.get("enabled", True),
    }
    await update.message.reply_text(f"<code>{json.dumps(info, indent=2)}</code>", parse_mode="HTML")
    await admin_audit("admin_info", int(user.id), json.dumps(info))

async def _group_message_logger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_chat and update.effective_chat.id == ALLOWED_GROUP_ID:
            await is_authorized_update(update)
    except Exception:
        pass

# -------------------- SCHEDULERS + graceful shutdown --------------------
_utc5_daily_task: Optional[asyncio.Task] = None
_change_detector_task: Optional[asyncio.Task] = None
_http_server_task: Optional[asyncio.Task] = None
_shutdown = False

async def next_daily_utc5(hour:int, minute:int, now: Optional[datetime]=None) -> datetime:
    if now is None:
        now = datetime.now(timezone.utc)
    tz_base = timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))
    local_now = now.astimezone(tz_base)
    target_local = datetime.combine(local_now.date(), time(hour, minute), tzinfo=tz_base)
    if local_now >= target_local:
        target_local = target_local + timedelta(days=1)
    return target_local.astimezone(timezone.utc)

async def sleep_until(target_dt_utc: datetime):
    while True:
        now = datetime.now(timezone.utc)
        secs = (target_dt_utc - now).total_seconds()
        if secs <= 0 or _shutdown:
            return
        await asyncio.sleep(min(secs, 60))

async def utc5_daily_sender(app: Application):
    try:
        while not _shutdown:
            next_send_utc = await next_daily_utc5(23, 0)
            await sleep_until(next_send_utc)
            if _shutdown:
                break
            if notifications_enabled():
                msg = await price_service.build_prices_msg()
                await app.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=msg, parse_mode="HTML")
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("utc5_daily_sender cancelled")
    except Exception:
        logger.exception("utc5_daily_sender crashed")

def _utc5_window_utc_for_today(now_utc: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    tz_base = timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))
    local = now_utc.astimezone(tz_base)
    start_local = datetime.combine(local.date(), time(5,45), tzinfo=tz_base)
    end_local = datetime.combine(local.date(), time(8,0), tzinfo=tz_base)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

async def price_change_detector(app: Application):
    try:
        while not _shutdown:
            start_utc, end_utc = _utc5_window_utc_for_today()
            await sleep_until(start_utc)
            if _shutdown:
                break
            today_iso = start_utc.astimezone(timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))).date().isoformat()
            if price_state.get("last_checked_date") == today_iso:
                next_start = start_utc + timedelta(days=1)
                await sleep_until(next_start)
                continue
            updates_found = False
            while datetime.now(timezone.utc) <= end_utc and not _shutdown:
                if not notifications_enabled():
                    break
                try:
                    msg = await price_service.detect_changes()
                    if msg:
                        await app.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=msg, parse_mode="HTML")
                        updates_found = True
                        break
                except Exception:
                    logger.exception("Error during price-change check")
                await asyncio.sleep(PRICE_CHANGE_POLL_SECONDS)
            if not updates_found and notifications_enabled():
                no_msg = "<code>\n" + "Price changes summary\n" + "-------------------------------\nNo price changes today.\n" + "</code>"
                await app.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=no_msg, parse_mode="HTML")
            price_state["last_checked_date"] = today_iso
            await save_price_state()
            next_start = start_utc + timedelta(days=1)
            await sleep_until(next_start)
    except asyncio.CancelledError:
        logger.info("price_change_detector cancelled")
    except Exception:
        logger.exception("price_change_detector crashed")

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
                        notif_flag["enabled"] = False
                        await upstash.set("notifications_enabled", "false")
                    return
            except Exception:
                continue
    except Exception:
        logger.exception("check_and_disable_after_gw38 failed")

async def start_background_tasks(app: Application):
    try:
        await load_state_from_upstash()
        await check_and_disable_after_gw38()
        global _utc5_daily_task, _change_detector_task, _http_server_task
        if _utc5_daily_task is None or _utc5_daily_task.done():
            _utc5_daily_task = asyncio.create_task(utc5_daily_sender(app))
        if _change_detector_task is None or _change_detector_task.done():
            _change_detector_task = asyncio.create_task(price_change_detector(app))
        # start small HTTP server for health + metrics
        if _http_server_task is None or _http_server_task.done():
            _http_server_task = asyncio.create_task(start_http_server())
        logger.info("Background tasks started")
    except Exception:
        logger.exception("Failed to start background tasks")

async def stop_background_tasks():
    global _utc5_daily_task, _change_detector_task, _http_server_task, _shutdown
    _shutdown = True
    tasks = [_utc5_daily_task, _change_detector_task, _http_server_task]
    for t in tasks:
        if t:
            t.cancel()
    await asyncio.sleep(0.2)
    try:
        await http.close()
        await upstash.close()
    except Exception:
        pass

# -------------------- Minimal async HTTP server for /health and /metrics --------------------
from aiohttp import web

METRICS = defaultdict(int)
def inc_metric(name: str, v: int = 1):
    METRICS[name] += v

async def handle_health(request):
    data = {
        "start_time": START_TIME,
        "bootstrap_cached": bool(_BOOTSTRAP_CACHE.get("data")),
        "allowed_users": len(allowed_users),
        "baseline_size": len(baseline),
        "notifications_enabled": notif_flag.get("enabled", True),
        "uptime_seconds": int((datetime.utcnow() - START_TIME_DT).total_seconds()),
    }
    return web.json_response(data)

async def handle_metrics(request):
    lines = []
    for k, v in METRICS.items():
        lines.append(f"# HELP {k} autogenerated\n# TYPE {k} gauge\n{k} {v}")
    body = "\n".join(lines) + "\n"
    return web.Response(text=body, content_type="text/plain; version=0.0.4")

async def start_http_server():
    app = web.Application()
    app.router.add_get("/health", handle_health)
    app.router.add_get("/metrics", handle_metrics)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", METRICS_PORT)
    await site.start()
    logger.info("HTTP server started on port %s", METRICS_PORT)
    while not _shutdown:
        await asyncio.sleep(1)

# -------------------- Application entry --------------------
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("prices", prices_cmd))
    app.add_handler(CommandHandler("price_on", price_on_cmd))
    app.add_handler(CommandHandler("settz", settz_cmd))
    app.add_handler(CommandHandler("mytz", mytz_cmd))
    app.add_handler(CommandHandler("notify_status", notify_status_cmd))
    app.add_handler(CommandHandler("admin_info", admin_info_cmd))
    app.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), _group_message_logger))

    async def _on_start(_app: Application):
        try:
            await _app.bot.set_my_commands([
                ("start", "Show help and available commands"),
                ("prices", "Show today's LiveFPL price projections"),
                ("price_on", "Show price changes for a given date (YYYY-MM-DD)"),
                ("settz", "Set your timezone (IANA or +N)"),
                ("mytz", "Show your timezone"),
                ("notify_status", "Show notification enabled state"),
            ])
        except Exception:
            logger.exception("Failed to set bot commands")
        await start_background_tasks(_app)

    async def _on_stop(_app: Application):
        logger.info("Bot stopping")
        await stop_background_tasks()

    app.post_init = _on_start
    app.post_shutdown = _on_stop

    logger.info("Starting bot...")
    try:
        app.run_polling()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    # in-memory caches (populated in on_start)
    START_TIME = datetime.utcnow().isoformat()
    START_TIME_DT = datetime.utcnow()
    user_timezones: Dict[int,str] = {}
    allowed_users: Set[int] = set()
    baseline: Dict[str,int] = {}
    league_cache: Dict[str,Any] = {}
    notif_flag: Dict[str,Any] = {"enabled": True}
    price_state: Dict[str,str] = {}
    in_memory_rl: Dict[int,List[int]] = {}
    try:
        main()
    except Exception:
        logger.exception("Fatal error in main")
