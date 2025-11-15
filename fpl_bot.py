# fpl_bot_enterprise.py
# Enterprise Full single-file FPL Prices Bot
# - Upstash-backed state
# - Bootstrap cache & smart matching
# - Hybrid tolerant LiveFPL parser (robust)
# - Circuit breaker + 3-level retry (tenacity)
# - Prometheus metrics + health HTTP endpoint
# - Background tasks: daily sender, price detector, supervisor (watchdog)
# - Commands: /prices, /price_on, /price_history, /settz, /mytz, /notify_status, /health, /metrics
# Minimal comments, pragmatic error handling, defensive persistence.

import os
import sys
import json
import hmac
import hashlib
import asyncio
import logging
import random
import re
import time as _time
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
import fastjsonschema
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from prometheus_client import start_http_server, Counter, Gauge
from telegram import Update, __version__ as PTB_VERSION
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# -------------------------
# CONFIG (ENV)
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
ALLOWED_GROUP_ID = int(os.getenv("ALLOWED_GROUP_ID", "0"))
LEAGUE_ID = int(os.getenv("LEAGUE_ID", "980121"))

UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")

WHITELIST_HMAC_KEY = os.getenv("WHITELIST_HMAC_KEY", "")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
BOOTSTRAP_TTL_SECS = int(os.getenv("BOOTSTRAP_TTL_SECS", "30"))
PRICE_CHANGE_POLL_SECONDS = int(os.getenv("PRICE_CHANGE_POLL_SECONDS", str(5 * 60)))
PRICE_CHANGE_UTC_PLUS = int(os.getenv("PRICE_CHANGE_UTC_PLUS", "5"))
GW_DISABLE_TARGET = int(os.getenv("GW_DISABLE_TARGET", "38"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8080"))
SNAPSHOT_TTL_DAYS = int(os.getenv("SNAPSHOT_TTL_DAYS", "30"))
RATE_LIMIT_TOKENS = int(os.getenv("RATE_LIMIT_TOKENS", "20"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))

FPL_BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
LIVEFPL_PRICES = "https://www.livefpl.net/prices"

if not BOT_TOKEN or OWNER_ID <= 0 or ALLOWED_GROUP_ID == 0:
    print("BOT_TOKEN, OWNER_ID, ALLOWED_GROUP_ID must be set", file=sys.stderr)
    sys.exit(1)
if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
    print("UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN must be set", file=sys.stderr)
    sys.exit(1)

# -------------------------
# Logging (JSON-like minimal)
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", stream=sys.stdout)
logger = logging.getLogger("fpl_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# -------------------------
# Metrics
# -------------------------
MET = defaultdict(int)
MSG_SENT = Counter("fpl_messages_sent_total", "Messages sent")
PRICE_CHECKS = Counter("fpl_price_checks_total", "Price checks performed")
UP_ERRORS = Counter("fpl_upstash_errors_total", "Upstash errors")
CB_OPEN = Gauge("fpl_circuit_open", "LiveFPL circuit open (1=open,0=closed)")

def inc_metric(name: str, v: int = 1):
    MET[name] += v

# -------------------------
# Upstash REST client
# -------------------------
class UpstashClient:
    def __init__(self, base: str, token: str, timeout: int = HTTP_TIMEOUT):
        self.base = base.rstrip("/")
        self.client = httpx.AsyncClient(timeout=timeout, headers={"Authorization": f"Bearer {token}"})

    async def close(self):
        try:
            await self.client.aclose()
        except Exception:
            pass

    async def _get(self, path: str) -> Optional[dict]:
        url = f"{self.base}/{path}"
        try:
            r = await self.client.get(url)
        except Exception as e:
            UP_ERRORS.inc()
            logger.warning("Upstash transport error: %s", e)
            return None
        if r.status_code != 200:
            UP_ERRORS.inc()
            logger.warning("Upstash status %s for %s", r.status_code, path)
            return None
        try:
            return r.json()
        except Exception:
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def hgetall(self, key: str) -> Dict[str, str]:
        j = await self._get(f"hgetall/{key}")
        if not j:
            return {}
        if isinstance(j, dict) and "result" in j and isinstance(j["result"], dict):
            return {k: str(v) for k, v in j["result"].items()}
        return {k: str(v) for k, v in (j or {}).items()}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def hset_map(self, key: str, mapping: Dict[str, str]):
        # Upstash expects /hset/<key>/<field>/<value>/...
        parts = ["hset", key]
        for k, v in mapping.items():
            parts.append(k); parts.append(v)
        path = "/".join(parts)
        await self._get(path)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def get(self, key: str) -> Optional[str]:
        j = await self._get(f"get/{key}")
        if not j:
            return None
        if isinstance(j, dict) and "result" in j:
            return None if j["result"] is None else str(j["result"])
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def set(self, key: str, value: str):
        await self._get(f"set/{key}/{value}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def incr(self, key: str) -> Optional[int]:
        j = await self._get(f"incr/{key}")
        if not j:
            return None
        if isinstance(j, dict) and "result" in j:
            try:
                return int(j["result"])
            except Exception:
                return None
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4),
           retry=retry_if_exception_type(httpx.TransportError))
    async def expire(self, key: str, seconds: int):
        await self._get(f"expire/{key}/{seconds}")

_upstash = UpstashClient(UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN)

# -------------------------
# HTTP client with retry
# -------------------------
class HttpClient:
    def __init__(self, timeout=HTTP_TIMEOUT):
        self._client = httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "FPL-Enterprise-Bot/1.0"})

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=6),
           retry=retry_if_exception_type((httpx.TransportError, httpx.ReadTimeout)))
    async def get_text(self, url: str) -> Optional[str]:
        r = await self._client.get(url)
        if r.status_code != 200:
            raise httpx.HTTPStatusError("status", request=r.request, response=r)
        return r.text

    async def close(self):
        try:
            await self._client.aclose()
        except Exception:
            pass

_http = HttpClient()

# -------------------------
# Bootstrap cache + smart match
# -------------------------
_BOOTSTRAP_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None, "elements": [], "el_map": {}, "name_index": {}}
BOOTSTRAP_TTL = BOOTSTRAP_TTL_SECS

async def fetch_bootstrap_cached(force: bool = False) -> Optional[dict]:
    loop = asyncio.get_event_loop()
    now = loop.time()
    if _BOOTSTRAP_CACHE["data"] and not force and (now - _BOOTSTRAP_CACHE["ts"] < BOOTSTRAP_TTL):
        return _BOOTSTRAP_CACHE["data"]
    try:
        text = await _http.get_text(FPL_BOOTSTRAP)
        data = json.loads(text)
    except Exception:
        inc_metric("bootstrap_fetch_fail")
        logger.exception("fetch_bootstrap_cached failed")
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
    inc_metric("bootstrap_refresh")
    return data

FPL_TEAM_ABBR = {
    1:  "ARS", 2:  "AVL", 3:  "BOU", 4:  "BRE", 5:  "BHA",
    6:  "BUR", 7:  "CHE", 8:  "CRY", 9:  "EVE", 10: "FUL",
    11: "LIV", 12: "LUT", 13: "MCI", 14: "MUN", 15: "NEW",
    16: "NFO", 17: "SHU", 18: "TOT", 19: "WHU", 20: "WOL",
}

def _pos_code_to_str(code: int) -> str:
    return {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}.get(int(code), "")

def find_element_by_name_smart(name: str, team_hint: str, pos_hint: str, price_hint: float, name_index: dict, elements: List[dict]) -> Optional[dict]:
    name_low = (name or "").lower().strip()
    candidates = name_index.get(name_low, []) if name_index else [el for el in elements if str(el.get("web_name","")).lower().strip() == name_low]
    if not candidates:
        candidates = [el for el in elements if str(el.get("web_name","")).lower().strip() == name_low]
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
# -------------------------
# continued in part 2/3
# continuation of fpl_bot_enterprise.py (part 2/3)

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
    inc_metric("ambiguous_matches")
    logger.warning("Ambiguous player match for '%s' -> using first candidate id=%s", name, candidates[0].get("id"))
    return candidates[0]

# -------------------------
# Robust parser: tolerant row + hybrid html parse
# -------------------------
def sanitize_name(s: Optional[str]) -> str:
    if not s:
        return ""
    s2 = re.sub(r"[\x00-\x1f\x7f]+", " ", str(s))
    s2 = re.sub(r"\s+", " ", s2).strip()
    s2 = s2.strip(" -–—_,.;:")
    return s2

def sanitize_price_val(s: Optional[str]) -> Optional[float]:
    try:
        if not s:
            return None
        t = str(s).replace("£", "").replace(",", ".").strip()
        return float(t)
    except Exception:
        return None

def _parse_row_tolerant(cells: List[str]) -> Dict[str, str]:
    out = {"Name": "", "Pos": "", "Team": "", "Price": "", "Target": "", "Owned by": ""}
    tokens = [c.strip() for c in cells if c and c.strip() != ""]
    if not tokens:
        return out
    # percents
    perc = [t for t in tokens if "%" in t]
    if perc:
        out["Owned by"] = perc[-1]
        if len(perc) >= 2:
            out["Target"] = perc[-2]
    # price
    price_tok = ""
    for t in tokens:
        if "£" in t or re.match(r"^\d+(\.\d+)?$", t):
            price_tok = t
            break
    if price_tok:
        out["Price"] = price_tok.replace("£", "").strip()
    # name tokens
    cand = []
    role_tokens = {"GKP", "DEF", "MID", "FWD", "GK", "DF", "MF", "FW"}
    for t in tokens:
        if t == price_tok or "%" in t:
            continue
        if any(rt in t for rt in role_tokens):
            parts = t.split()
            for p in parts:
                pu = p.upper()
                if pu in role_tokens and not out["Pos"]:
                    out["Pos"] = pu
                elif re.match(r"^\d+(\.\d+)?$", p) and not out["Price"]:
                    out["Price"] = p
                else:
                    cand.append(p)
            continue
        cand.append(t)
    if cand:
        out["Name"] = sanitize_name(" ".join(cand))
    else:
        for t in tokens:
            if t == price_tok or "%" in t:
                break
            out["Name"] = sanitize_name(t)
            break
    # heuristics for team/pos from columns
    if len(cells) >= 2 and not out["Team"]:
        c1 = cells[1].strip()
        if len(c1) <= 4 and c1.isalpha():
            out["Team"] = c1.upper()
        elif c1.upper() in role_tokens:
            out["Pos"] = c1.upper()
    if len(cells) >= 3 and not out["Team"]:
        c2 = cells[2].strip()
        if len(c2) <= 4 and c2.isalpha():
            out["Team"] = c2.upper()
    return out

def _find_section_nodes(soup: BeautifulSoup, hints: List[str]):
    nodes = {}
    candidates = soup.find_all(["h2", "h3", "h4", "strong", "caption", "p", "div"])
    for hint in hints:
        hint_low = hint.lower()
        node = None
        for c in candidates:
            try:
                txt = c.get_text(" ", strip=True).lower()
            except Exception:
                txt = ""
            if hint_low in txt:
                node = c
                break
        nodes[hint] = node
    return nodes

async def hybrid_parse_livefpl(html_text: str, hints: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    try:
        soup = BeautifulSoup(html_text, "html.parser")
    except Exception:
        return {}
    if hints is None:
        hints = ["Already reached target", "Projected to reach target", "Others who will be close", "Predicted Rises", "Predicted Falls"]
    nodes = _find_section_nodes(soup, hints)
    sections: Dict[str, List[Dict[str, Any]]] = {}
    for hint in hints:
        node = nodes.get(hint)
        rows = []
        tbl = None
        if node:
            tbl = node.find_next("table")
            if not tbl and node.name == "table":
                tbl = node
        if tbl:
            for tr in tbl.find_all("tr"):
                tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                if not tds:
                    continue
                parsed = _parse_row_tolerant(tds)
                rows.append(parsed)
        else:
            if node:
                for li in node.find_next_siblings("li"):
                    txt = li.get_text(" ", strip=True)
                    parsed = _parse_row_tolerant([txt])
                    rows.append(parsed)
        final = [r for r in rows if r.get("Name") or r.get("Owned by") or r.get("Target")]
        sections[hint] = final
    # predicted rises/falls mapping
    name_dir_map = {}
    for r in sections.get("Predicted Rises", []) or []:
        n = (r.get("Name") or "").strip()
        if n:
            name_dir_map[n.lower()] = "rise"
    for r in sections.get("Predicted Falls", []) or []:
        n = (r.get("Name") or "").strip()
        if n:
            name_dir_map[n.lower()] = "fall"
    # enrich with bootstrap
    try:
        data = await fetch_bootstrap_cached()
        elements = data.get("elements", []) if data else []
        name_index = _BOOTSTRAP_CACHE.get("name_index", {})
    except Exception:
        elements = []
        name_index = {}
    for title, rows in list(sections.items()):
        enriched = []
        for r in rows:
            name = (r.get("Name") or "").strip()
            team_hint = (r.get("Team") or "").strip()
            pos_hint = (r.get("Pos") or "").strip()
            price_hint = sanitize_price_val(r.get("Price"))
            found = None
            try:
                found = find_element_by_name_smart(name, team_hint, pos_hint, price_hint or 0.0, name_index, elements)
            except Exception:
                found = None
            if found:
                try:
                    r["element"] = int(found.get("id"))
                except Exception:
                    r["element"] = found.get("id")
                try:
                    tc = found.get("team_code") or found.get("team")
                    if tc is not None:
                        try:
                            tc_int = int(tc)
                            r["Team"] = FPL_TEAM_ABBR.get(tc_int, r.get("Team") or "UNK")
                        except Exception:
                            if isinstance(tc, str) and tc.strip():
                                r["Team"] = tc.strip()
                except Exception:
                    pass
            nlow = (name or "").lower()
            if name_dir_map.get(nlow):
                r["_direction"] = name_dir_map[nlow]
            else:
                tperc = r.get("Target", "")
                try:
                    tv = float(str(tperc).replace("%", "").strip()) if tperc else 0.0
                    r["_direction"] = "rise" if tv > 0 else ("fall" if tv < 0 else "neutral")
                except Exception:
                    r["_direction"] = "neutral"
            enriched.append(r)
        sections[title] = enriched
    return sections

# -------------------------
# Utilities: percent parsing, compact formatting
# -------------------------
def parse_percent(s: str) -> float:
    try:
        if s is None:
            return 0.0
        s2 = str(s).replace("%", "").strip()
        return float(s2) if s2 != "" else 0.0
    except Exception:
        return 0.0

def center_text(s: str, width: int = 40) -> str:
    return s.center(width)

def format_prices_compact(sections: Dict[str, List[Dict[str, Any]]], el_map: Dict[int, dict]) -> str:
    order = ["Already reached target", "Projected to reach target", "Others who will be close"]
    blocks: List[str] = []
    for title in order:
        rows = sections.get(title, []) or []
        header = center_text(title, 40)
        block_lines = [header]
        if not rows:
            block_lines.append("(none)")
            blocks.append("<code>" + "\n".join(block_lines) + "</code>")
            continue
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
            if el_id and el_map:
                try:
                    tc = el_map[el_id].get("team_code")
                    team_abbr = FPL_TEAM_ABBR.get(int(tc), "UNK")
                except Exception:
                    pass
            else:
                tcol = (r.get("Team") or "").strip()
                if tcol:
                    team_abbr = tcol.upper()
            name = (r.get("Name") or "")
            price = f"£{r.get('Price')}".strip()
            tgt = r.get("Target") or ""
            block_lines.append(f"{team_abbr}  {name}  {price}  ({tgt})")
        blocks.append("<code>" + "\n".join(block_lines) + "</code>")
    return "\n\n".join(blocks)

# -------------------------
# Persistence and state (Upstash-backed)
# -------------------------
async def load_all_state():
    global user_timezones, allowed_users_set, prices_baseline, league_cache_state, notif_flag_obj, price_check_state
    try:
        ut = await _upstash.hgetall("user_timezones")
        user_timezones = {int(k): v for k, v in ut.items()} if ut else {}
    except Exception:
        user_timezones = {}
    try:
        au = await _upstash.hgetall("allowed_users")
        users = []
        if au:
            users = [int(k) for k in au.keys() if str(k).isdigit()]
        sig = await _upstash.get("allowed_users_sig")
        if WHITELIST_HMAC_KEY and sig:
            expected = hmac.new(WHITELIST_HMAC_KEY.encode(), json.dumps(sorted(users)).encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(expected, sig):
                logger.warning("allowed_users signature mismatch; ignoring")
                allowed_users_set = set()
            else:
                allowed_users_set = set(users)
        else:
            allowed_users_set = set(users)
    except Exception:
        allowed_users_set = set()
    try:
        pb = await _upstash.hgetall("prices_baseline_current")
        clean = {}
        for k, v in (pb or {}).items():
            try:
                clean[str(k)] = int(v)
            except Exception:
                continue
        prices_baseline = clean
    except Exception:
        prices_baseline = {}
    try:
        lp = await _upstash.hgetall("league_players")
        league_cache_state = {"players": list(lp.keys())} if lp else {}
    except Exception:
        league_cache_state = {}
    try:
        ne = await _upstash.get("notifications_enabled")
        notif_flag_obj = {"enabled": (ne == "true")} if ne is not None else {"enabled": True}
    except Exception:
        notif_flag_obj = {"enabled": True}
    try:
        ps = await _upstash.hgetall("price_check_state")
        price_check_state = {k: v for k, v in (ps or {}).items()}
    except Exception:
        price_check_state = {}
    logger.info("State loaded")
# -------------------------
# continued in part 3/3
# continuation of fpl_bot_enterprise.py (part 3/3)

async def save_allowed_users():
    try:
        mapping = {str(u): "1" for u in sorted(list(allowed_users_set))}
        if mapping:
            await _upstash.hset_map("allowed_users", mapping)
        if WHITELIST_HMAC_KEY:
            sig = hmac.new(WHITELIST_HMAC_KEY.encode(), json.dumps(sorted(list(allowed_users_set))).encode(), hashlib.sha256).hexdigest()
            await _upstash.set("allowed_users_sig", sig)
        await _upstash.hset_map("audit_allowed_users", {str(int(datetime.utcnow().timestamp())): ",".join(map(str, sorted(list(allowed_users_set))))})
    except Exception:
        logger.exception("save_allowed_users failed")

async def save_user_timezones():
    try:
        mapping = {str(k): v for k, v in user_timezones.items()}
        if mapping:
            await _upstash.hset_map("user_timezones", mapping)
    except Exception:
        logger.exception("save_user_timezones failed")

async def save_baseline_map(new_map: Dict[str,int]):
    try:
        mapping = {str(k): str(v) for k, v in new_map.items()}
        if mapping:
            await _upstash.hset_map("prices_baseline_current", mapping)
        today_str = datetime.utcnow().astimezone(timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))).date().isoformat()
        snap_key = f"prices_baseline:{today_str}"
        await _upstash.hset_map(snap_key, mapping)
        await _upstash.expire(snap_key, SNAPSHOT_TTL_DAYS * 24 * 3600)
    except Exception:
        logger.exception("save_baseline_map failed")

async def save_price_state():
    try:
        mapping = {str(k): str(v) for k, v in price_check_state.items()}
        if mapping:
            await _upstash.hset_map("price_check_state", mapping)
    except Exception:
        logger.exception("save_price_state failed")

# -------------------------
# League helpers
# -------------------------
class LeagueService:
    async def get_current_gw(self) -> int:
        data = await fetch_bootstrap_cached()
        if not data:
            return 0
        for ev in data.get("events", []) or []:
            if ev.get("is_current"):
                return int(ev.get("id", 0))
        return 0

    async def fetch_league_player_set_once(self, league_id: int, cap: int = 80) -> Set[str]:
        out: Set[str] = set()
        url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"
        data = None
        try:
            data = json.loads(await _http.get_text(url))
        except Exception:
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
                    j = json.loads(await _http.get_text(url_e))
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

    async def get_league_player_set_cached(self) -> Set[str]:
        try:
            lp = await _upstash.hgetall("league_players")
            if lp:
                return set(lp.keys())
        except Exception:
            pass
        s = await self.fetch_league_player_set_once(LEAGUE_ID)
        try:
            mapping = {str(x): "1" for x in s}
            if mapping:
                await _upstash.hset_map("league_players", mapping)
        except Exception:
            pass
        return s

league_service = LeagueService()

# -------------------------
# Price detector & message builder
# -------------------------
class PriceDetectorService:
    async def build_prices_msg_from_html(self, html_text: str) -> str:
        if livefpl_cb.is_open():
            return "<code>LiveFPL temporarily unavailable</code>"
        if not html_text:
            livefpl_cb.record_failure()
            return "<code>Could not fetch LiveFPL prices.</code>"
        try:
            sections = await hybrid_parse_livefpl(html_text)
            if not sections:
                livefpl_cb.record_failure()
                return "<code>No price projections parsed.</code>"
            inc_metric("livefpl_parse_success")
            league_set = await league_service.get_league_player_set_cached()
            processed = sort_and_filter_sections(sections, league_set)
            el_map = _BOOTSTRAP_CACHE.get("el_map", {})
            return format_prices_compact(processed, el_map)
        except Exception:
            livefpl_cb.record_failure()
            logger.exception("build_prices_msg_from_html failed")
            return "<code>Could not build prices message.</code>"

    async def build_prices_msg(self) -> str:
        try:
            txt = await _http.get_text(LIVEFPL_PRICES)
            if not txt:
                return "<code>Could not fetch LiveFPL page.</code>"
            livefpl_cb.record_success()
            return await self.build_prices_msg_from_html(txt)
        except Exception:
            livefpl_cb.record_failure()
            logger.exception("build_prices_msg main failed")
            return "<code>Could not fetch LiveFPL page.</code>"

    async def detect_changes(self) -> Optional[str]:
        data = await fetch_bootstrap_cached()
        if not data:
            return None
        elements = data.get("elements", []) or []
        new_map = {str(el["id"]): int(el.get("now_cost", 0)) for el in elements}
        changes = []
        for eid, new_cost in new_map.items():
            old = prices_baseline.get(eid)
            if old is not None and old != new_cost:
                changes.append((eid, old, new_cost))
        if changes:
            await save_baseline_map(new_map)
            prices_baseline.clear()
            prices_baseline.update({str(k): int(v) for k, v in new_map.items()})
            try:
                await _upstash.hset_map("audit_price_changes", {str(int(datetime.utcnow().timestamp())): ",".join([f"{e}:{o}->{n}" for e, o, n in changes])})
            except Exception:
                pass
            el_map = _BOOTSTRAP_CACHE.get("el_map", {})
            lines = ["<b>Price changes detected</b>"]
            for eid, o, n in changes:
                nm = el_map.get(int(eid), {}).get("web_name", f"id:{eid}")
                lines.append(f"<code>{nm.ljust(15)} {o/10:.1f} → {n/10:.1f}</code>")
            return "\n".join(lines)
        if not prices_baseline:
            prices_baseline.update({str(k): int(v) for k, v in new_map.items()})
            await save_baseline_map(new_map)
        return None

price_service = PriceDetectorService()

# -------------------------
# Sorting/filtering (same as earlier)
# -------------------------
def is_in_league_row(r: Dict[str, Any], league_set: Set[str]) -> int:
    name = (r.get("Name") or "").strip().lower()
    if not name:
        return 0
    if name in (n.lower() for n in league_set):
        return 1
    eid = r.get("element") or r.get("Element") or r.get("id")
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
# Circuit breaker class
# -------------------------
class CircuitBreaker:
    def __init__(self, fail_threshold: int = 3, cooldown_seconds: int = 600):
        self.fail_threshold = fail_threshold
        self.cooldown_seconds = cooldown_seconds
        self.fail_count = 0
        self.open_until = 0

    def record_success(self):
        self.fail_count = 0
        self.open_until = 0
        CB_OPEN.set(0)

    def record_failure(self):
        self.fail_count += 1
        if self.fail_count >= self.fail_threshold:
            self.open_until = int(_time.time()) + self.cooldown_seconds + random.randint(0, 30)
            logger.warning("Circuit breaker opened for %ds after %d failures", self.cooldown_seconds, self.fail_count)
            inc_metric("circuit_opened")
            CB_OPEN.set(1)

    def is_open(self) -> bool:
        if self.open_until == 0:
            return False
        if _time.time() >= self.open_until:
            logger.info("Circuit breaker cooldown expired; trial allowed")
            self.fail_count = 0
            self.open_until = 0
            CB_OPEN.set(0)
            return False
        return True

livefpl_cb = CircuitBreaker()

# -------------------------
# Rate limiter
# -------------------------
in_memory_rl: Dict[int, List[int]] = {}
async def rate_limit_check(user_id: int) -> bool:
    try:
        val = await _upstash.incr(f"rl:{user_id}")
        if val is None:
            raise RuntimeError("upstash incr failed")
        if val == 1:
            await _upstash.expire(f"rl:{user_id}", RATE_LIMIT_WINDOW)
        return val <= RATE_LIMIT_TOKENS
    except Exception:
        now = int(datetime.utcnow().timestamp())
        bucket = in_memory_rl.get(user_id, [])
        bucket = [t for t in bucket if t > now - RATE_LIMIT_WINDOW]
        if len(bucket) >= RATE_LIMIT_TOKENS:
            return False
        bucket.append(now)
        in_memory_rl[user_id] = bucket
        return True

# -------------------------
# Authorization + secure send
# -------------------------
_allowed_users: Set[int] = set()

async def is_authorized_update(update: Update) -> bool:
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
                asyncio.create_task(save_allowed_users())
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
        MSG_SENT.inc()
    except Exception:
        logger.exception("send_message_secure failed")
        inc_metric("send_errors")

# -------------------------
# Daily baseline loader (defensive)
# -------------------------
async def _load_daily_baseline_async(date_str: str) -> Optional[Dict[str,int]]:
    key = f"prices_baseline:{date_str}"
    try:
        d = await _upstash.hgetall(key)
        if not d:
            return None
        clean = {}
        for k, v in d.items():
            try:
                if isinstance(v, (int, float)):
                    clean[str(k)] = int(v)
                    continue
                sv = str(v).strip()
                if sv.startswith("[") or sv.startswith("{") or sv == "":
                    continue
                clean[str(k)] = int(float(sv))
            except Exception:
                continue
        return clean
    except Exception:
        logger.exception("load daily baseline failed")
        return None

# -------------------------
# Handler guard wrapper
# -------------------------
def handler_guard(fn):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            user = update.effective_user
            uid = int(user.id) if user else None
            if uid and not await rate_limit_check(uid):
                try:
                    await update.message.reply_text("Rate limit exceeded. Try later.")
                except Exception:
                    pass
                return
            await fn(update, context)
        except Exception as e:
            logger.exception("Handler error: %s", e)
            try:
                await _upstash.hset_map("audit_errors", {str(int(datetime.utcnow().timestamp())): str(e)})
            except Exception:
                pass
            try:
                if OWNER_ID:
                    await context.application.bot.send_message(chat_id=OWNER_ID, text=f"Handler error: {e}")
            except Exception:
                pass
    return wrapper

# -------------------------
# Handlers and commands
# -------------------------
@handler_guard
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized_update(update):
        return
    text = (
        "FPL Prices Bot (Enterprise)\n"
        "/prices - show current compact prices (3 sections)\n"
        "/price_on YYYY-MM-DD - show price changes between days\n"
        "/price_history <player> - show historical snapshots for player\n"
        "/settz <Zone> - set your timezone\n"
        "/mytz - show your timezone\n"
        "/notify_status - show notification enabled/disabled\n"
        "/health - service health\n"
    )
    if update.effective_chat.type == "private":
        await update.message.reply_text(text)
    else:
        await send_message_secure(context.application, text, silent=False)

@handler_guard
async def prices_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized_update(update):
        return
    msg = await price_service.build_prices_msg()
    if update.effective_chat.type == "private":
        await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
    else:
        await send_message_secure(context.application, msg, silent=False)

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
        _ = datetime.fromisoformat(date_str)
    except Exception:
        await update.message.reply_text("Invalid date format. Use YYYY-MM-DD.")
        return
    day1 = await _load_daily_baseline_async(date_str)
    if not day1:
        await update.message.reply_text(f"No baseline for {date_str}")
        return
    next_day = (datetime.fromisoformat(date_str).date() + timedelta(days=1)).isoformat()
    day2 = await _load_daily_baseline_async(next_day)
    if not day2:
        await update.message.reply_text(f"No baseline for {next_day} (need both days)")
        return
    changes = price_service.detect_between_maps(day1, day2)
    if not changes:
        await update.message.reply_text(f"<code>No changes between {date_str} and {next_day}</code>", parse_mode="HTML")
        return
    el_map = _BOOTSTRAP_CACHE.get("el_map", {})
    lines = [f"<b>Price changes {date_str} → {next_day}</b>"]
    for eid, o, n in changes:
        nm = el_map.get(int(eid), {}).get("web_name", f"id:{eid}")
        lines.append(f"<code>{nm.ljust(15)} {o/10:.1f} → {n/10:.1f}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

@handler_guard
async def price_history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized_update(update):
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /price_history <player name>")
        return
    player = " ".join(args).lower()
    lines = []
    for i in range(0, 30):
        d = (datetime.utcnow().date() - timedelta(days=i)).isoformat()
        snap = await _load_daily_baseline_async(d)
        if not snap:
            continue
        for eid, val in snap.items():
            try:
                el = _BOOTSTRAP_CACHE.get("el_map", {}).get(int(eid), {})
                nm = (el.get("web_name") or "").lower()
                if player in nm:
                    lines.append(f"{d} {el.get('web_name')} £{int(val)/10:.1f}")
            except Exception:
                continue
    if not lines:
        await update.message.reply_text("No history found")
    else:
        await update.message.reply_text("<pre>" + "\n".join(lines[:100]) + "</pre>", parse_mode="HTML")

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
    enabled = bool(notif_flag_obj.get("enabled", True))
    await update.message.reply_text(f"Notifications enabled: {enabled}\nMetrics: sent={MET.get('messages_sent',0)}, errors={MET.get('send_errors',0)}")

async def _group_message_logger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_chat and update.effective_chat.id == ALLOWED_GROUP_ID:
            await is_authorized_update(update)
    except Exception:
        pass

# -------------------------
# Background tasks + supervisor/watchdog
# -------------------------
_utc5_daily_task: Optional[asyncio.Task] = None
_change_detector_task: Optional[asyncio.Task] = None
_supervisor_task: Optional[asyncio.Task] = None
_shutdown = False
APP_INSTANCE = None

def next_daily_utc5(hour:int, minute:int, now: Optional[datetime]=None) -> datetime:
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
    logger.info("UTC+5 daily sender started")
    try:
        while not _shutdown:
            next_send_utc = next_daily_utc5(23, 0)
            await sleep_until(next_send_utc)
            if _shutdown:
                break
            if notifications_enabled():
                msg = await price_service.build_prices_msg()
                await send_message_secure(app, msg, silent=False)
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
    logger.info("Price change detector started")
    try:
        while not _shutdown:
            start_utc, end_utc = _utc5_window_utc_for_today()
            await sleep_until(start_utc)
            if _shutdown:
                break
            today_iso = start_utc.astimezone(timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))).date().isoformat()
            if price_check_state.get("last_checked_date") == today_iso:
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
                        await send_message_secure(app, msg, silent=True)
                        updates_found = True
                        break
                except Exception:
                    logger.exception("Error during price-change check")
                await asyncio.sleep(PRICE_CHANGE_POLL_SECONDS)
            if not updates_found and notifications_enabled():
                no_msg = "<code>\n" + center_text("Price changes summary", 40) + "\n" + "-------------------------------\n" + "No price changes today.\n" + "</code>"
                await send_message_secure(app, no_msg, silent=True)
            price_check_state["last_checked_date"] = today_iso
            await save_price_state()
            next_start = start_utc + timedelta(days=1)
            await sleep_until(next_start)
    except asyncio.CancelledError:
        logger.info("price_change_detector cancelled")
    except Exception:
        logger.exception("price_change_detector crashed")

def start_supervisor(loop, tasks_map_getter, restart_fn, interval=30):
    async def _loop():
        while not _shutdown:
            try:
                tasks = tasks_map_getter()
                for name, t in tasks.items():
                    if t is None or t.done():
                        logger.warning("Supervisor: task %s not running -> restarting", name)
                        try:
                            await restart_fn(name)
                            inc_metric("supervisor_restarts")
                        except Exception:
                            logger.exception("Supervisor failed to restart %s", name)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Supervisor crashed loop")
                await asyncio.sleep(interval)
    return loop.create_task(_loop())

async def restart_background_task(name: str):
    global _utc5_daily_task, _change_detector_task
    if name == "utc5_daily":
        if _utc5_daily_task and not _utc5_daily_task.done():
            _utc5_daily_task.cancel()
        _utc5_daily_task = asyncio.create_task(utc5_daily_sender(APP_INSTANCE))
    elif name == "price_detector":
        if _change_detector_task and not _change_detector_task.done():
            _change_detector_task.cancel()
        _change_detector_task = asyncio.create_task(price_change_detector(APP_INSTANCE))

# -------------------------
# Mini HTTP server for health & metrics
# -------------------------
from aiohttp import web

START_TIME_DT = datetime.utcnow()

async def handle_health(request):
    data = {
        "uptime_seconds": int((datetime.utcnow() - START_TIME_DT).total_seconds()),
        "bootstrap_cached": bool(_BOOTSTRAP_CACHE.get("data")),
        "baseline_count": len(prices_baseline),
        "notifications_enabled": notif_flag_obj.get("enabled", True),
    }
    return web.json_response(data)

async def handle_metrics(request):
    lines = []
    for k, v in MET.items():
        lines.append(f"# HELP {k} autogenerated")
        lines.append(f"# TYPE {k} gauge")
        lines.append(f"{k} {v}")
    return web.Response(text="\n".join(lines), content_type="text/plain; version=0.0.4")

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

# -------------------------
# Lifecycle: start/stop background tasks
# -------------------------
async def start_background_tasks(app: Application):
    try:
        await load_all_state()
        await check_and_disable_after_gw38()
        global _utc5_daily_task, _change_detector_task, _supervisor_task, APP_INSTANCE
        loop = asyncio.get_event_loop()
        APP_INSTANCE = app
        if _utc5_daily_task is None or _utc5_daily_task.done():
            _utc5_daily_task = asyncio.create_task(utc5_daily_sender(app))
        if _change_detector_task is None or _change_detector_task.done():
            _change_detector_task = asyncio.create_task(price_change_detector(app))
        def tasks_getter():
            return {"utc5_daily": _utc5_daily_task, "price_detector": _change_detector_task}
        _supervisor_task = start_supervisor(loop, tasks_getter, restart_background_task, interval=30)
        loop.create_task(start_http_server())
        logger.info("Background tasks started")
    except Exception:
        logger.exception("Failed to start background tasks")

async def stop_background_tasks():
    global _utc5_daily_task, _change_detector_task, _supervisor_task, _shutdown
    _shutdown = True
    tasks = [_utc5_daily_task, _change_detector_task, _supervisor_task]
    for t in tasks:
        if t:
            t.cancel()
    await asyncio.sleep(0.2)
    try:
        await _http.close()
        await _upstash.close()
    except Exception:
        pass

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
                        await _upstash.set("notifications_enabled", "false")
                        logger.info("GW finished -> notifications disabled")
                    return
            except Exception:
                continue
    except Exception:
        logger.exception("check_and_disable_after_gw38 failed")

# -------------------------
# Health/state defaults
# -------------------------
user_timezones: Dict[int, str] = {}
_allowed_users = set()
allowed_users_set = set()
prices_baseline: Dict[str, int] = {}
league_cache_state: Dict[str, Any] = {}
notif_flag_obj: Dict[str, Any] = {"enabled": True}
price_check_state: Dict[str, str] = {}

# -------------------------
# App builder + run
# -------------------------
def build_app():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("prices", prices_cmd))
    app.add_handler(CommandHandler("price_on", price_on_cmd))
    app.add_handler(CommandHandler("price_history", price_history_cmd))
    app.add_handler(CommandHandler("settz", settz_cmd))
    app.add_handler(CommandHandler("mytz", mytz_cmd))
    app.add_handler(CommandHandler("notify_status", notify_status_cmd))
    app.add_handler(CommandHandler("health", lambda u,c: c))  # placeholder - health via HTTP
    app.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), _group_message_logger))

    async def _on_start(_app: Application):
        logger.info("Bot started (PTB %s)", PTB_VERSION)
        try:
            await _app.bot.set_my_commands([
                ("start","Show help and commands"),
                ("prices","Show LiveFPL price projections"),
                ("price_on","Show changes between days"),
                ("price_history","Show player price history"),
                ("settz","Set timezone"),
                ("mytz","Show timezone"),
                ("notify_status","Notifications status"),
            ])
        except Exception:
            logger.exception("Failed to set commands")
        await start_background_tasks(_app)

    async def _on_stop(_app: Application):
        logger.info("Bot stopping")
        await stop_background_tasks()

    app.post_init = _on_start
    app.post_shutdown = _on_stop
    return app

def main():
    app = build_app()
    logger.info("Starting bot...")
    try:
        app.run_polling()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
