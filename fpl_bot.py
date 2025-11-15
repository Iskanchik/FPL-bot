# fpl_bot.py
# Single-file enterprise-grade FPL Prices Bot (subset: /prices, /price_on, /help)
# Variant B formatting chosen: TEAM POS Name £price  EMOJI  target% (compact mobile-friendly)
# Changes applied per request:
# - Monospaced output, max width 40 (no artificial stretching; only truncate)
# - Percent highlighted (bold)
# - Names shortened when too long
# - price_on uses bootstrap-static as baseline fallback (no LiveFPL fetch)
# - Baseline snapshots still saved to Upstash; detect_changes uses bootstrap-static
# - Price queries restricted to current season (Aug 1 .. Jul 31 next year)
# Minimal comments kept.

import os, sys, json, asyncio, logging, random, re, time as _time
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
from datetime import datetime, timedelta, timezone, time as dt_time, date
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from prometheus_client import Counter, Gauge
from aiohttp import web

from telegram import Update, __version__ as PTB_VERSION
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# -------------------------
# CONFIG (ENV) - required
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
ALLOWED_GROUP_ID = int(os.getenv("ALLOWED_GROUP_ID", "0"))

UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
BOOTSTRAP_TTL_SECS = int(os.getenv("BOOTSTRAP_TTL_SECS", "30"))
PRICE_CHANGE_UTC_PLUS = int(os.getenv("PRICE_CHANGE_UTC_PLUS", "5"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8080"))
RATE_LIMIT_TOKENS = int(os.getenv("RATE_LIMIT_TOKENS", "20"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))
SNAPSHOT_TTL_DAYS = int(os.getenv("SNAPSHOT_TTL_DAYS", "30"))
GW_DISABLE_TARGET = int(os.getenv("GW_DISABLE_TARGET", "38"))

FPL_BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
LIVEFPL_PRICES = "https://www.livefpl.net/prices"

if not BOT_TOKEN or OWNER_ID <= 0 or ALLOWED_GROUP_ID == 0:
    print("BOT_TOKEN, OWNER_ID, ALLOWED_GROUP_ID must be set", file=sys.stderr)
    sys.exit(1)
if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
    print("UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN must be set", file=sys.stderr)
    sys.exit(1)

# -------------------------
# Formatting limits (user requested)
# -------------------------
TABLE_MAX_WIDTH = 40        # maximum characters per line (do not artificially stretch shorter lines)
MAX_NAME_LEN = 18          # max chars for displayed player name (truncate/smart-shorten if longer)

# -------------------------
# Logging & metrics
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", stream=sys.stdout)
logger = logging.getLogger("fpl_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

MET = defaultdict(int)
MSG_SENT = Counter("fpl_messages_sent_total", "Messages sent")
UP_ERRORS = Counter("fpl_upstash_errors_total", "Upstash errors")
CB_OPEN = Gauge("fpl_circuit_open", "LiveFPL circuit open (1=open,0=closed)")
PARSE_SUCCESS = Counter("fpl_parse_success_total", "Successful LiveFPL parses")
PARSE_FAIL = Counter("fpl_parse_fail_total", "Failed LiveFPL parses")

def inc_metric(name: str, v: int = 1):
    MET[name] += v

# -------------------------
# Upstash REST client (minimal)
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
            logger.debug("Upstash transport error: %s", e)
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
        if not mapping:
            return
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
# Bootstrap cache + helpers
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
# Parser (robust tolerant HTML variant)
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
    perc = [t for t in tokens if "%" in t]
    if perc:
        out["Owned by"] = perc[-1]
        if len(perc) >= 2:
            out["Target"] = perc[-2]
    price_tok = ""
    for t in tokens:
        if "£" in t or re.match(r"^\d+(\.\d+)?$", t):
            price_tok = t
            break
    if price_tok:
        out["Price"] = price_tok.replace("£", "").strip()
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
        PARSE_FAIL.inc()
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
                    try:
                        txt = li.get_text(" ", strip=True)
                    except Exception:
                        txt = ""
                    parsed = _parse_row_tolerant([txt])
                    rows.append(parsed)
        final = [r for r in rows if (r.get("Name") or "").strip()]
        sections[hint] = final
    # rises/falls mapping
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
    PARSE_SUCCESS.inc()
    return sections

# -------------------------
# Utilities & Variant B formatting (TEAM POS Name £price  EMOJI  target%)
# -------------------------
def parse_percent(s: str) -> float:
    try:
        if s is None:
            return 0.0
        s2 = str(s).replace("%", "").strip()
        return float(s2) if s2 != "" else 0.0
    except Exception:
        return 0.0

def emoji_for_direction(direction: str) -> str:
    if not direction:
        return "▫"
    d = direction.lower()
    if d in ("rise", "up"):
        return "🔼"
    if d in ("fall", "down"):
        return "🔽"
    return "▫"

def enforce_width(s: str) -> str:
    # do not artificially pad; only truncate to TABLE_MAX_WIDTH
    if s is None:
        return ""
    s2 = str(s)
    if len(s2) <= TABLE_MAX_WIDTH:
        return s2
    # truncate gracefully with ellipsis fitting into TABLE_MAX_WIDTH
    if TABLE_MAX_WIDTH <= 3:
        return s2[:TABLE_MAX_WIDTH]
    return s2[:TABLE_MAX_WIDTH-1] + "…"

def shorten_name(name: str, max_len: int = MAX_NAME_LEN) -> str:
    if not name:
        return ""
    n = name.strip()
    if len(n) <= max_len:
        return n
    parts = n.split()
    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
        short = f"{first[0]}. {last}"
        if len(short) <= max_len:
            return short
        # try last name only truncated
        if len(last) > max_len:
            return last[:max_len-1] + "…"
        return short[:max_len-1] + "…"
    # single long token: truncate
    return n[:max_len-1] + "…"

def format_line_variant_b(r: Dict[str, Any], el_map: Dict[int, dict]) -> str:
    # Compose: TEAM POS Name £price  EMOJI  <b>target%</b>
    team = (r.get("Team") or "").upper() or "UNK"
    pos = (r.get("Pos") or "").upper() or ""
    name_raw = (r.get("Name") or "").strip()
    name = shorten_name(name_raw, MAX_NAME_LEN)
    price_val = r.get("Price") or ""
    try:
        pv = float(str(price_val))
        price = f"£{pv:.1f}"
    except Exception:
        price = f"£{price_val}".strip()
    tgt_raw = r.get("Target") or ""
    if not tgt_raw:
        tgt_raw = r.get("Owned by") or ""
    tgt_pct = ""
    try:
        tnum = parse_percent(tgt_raw)
        tgt_pct = f"{int(round(tnum))}%"
    except Exception:
        tgt_pct = (tgt_raw or "").strip()
    direction = r.get("_direction", "neutral")
    em = emoji_for_direction(direction)
    pieces = []
    if pos:
        pieces.append(f"{team} {pos}")
    else:
        pieces.append(team)
    pieces.append(price)
    pieces.append(name)
    pieces.append(em)
    if tgt_pct:
        # percent highlighted in bold (HTML)
        pieces.append(f"<b>{tgt_pct}</b>")
    # join with two spaces for readability, then enforce width
    line = "  ".join([p for p in pieces if p is not None and str(p) != ""])
    return enforce_width(line)

def format_prices_variant_b(sections: Dict[str, List[Dict[str, Any]]], el_map: Dict[int, dict]) -> str:
    order = ["Already reached target", "Projected to reach target", "Others who will be close"]
    out_blocks = []
    for title in order:
        rows = sections.get(title, []) or []
        header = title  # do not pad header artificially
        lines = [header]
        if not rows:
            lines.append("(none)")
            out_blocks.append("\n".join(lines))
            continue
        for r in rows:
            try:
                line = format_line_variant_b(r, el_map)
                lines.append(line)
            except Exception:
                continue
        out_blocks.append("\n".join(lines))
    # join blocks and wrap in <pre> to force monospace in Telegram HTML mode
    body = "\n\n".join(out_blocks)
    return "<pre>" + body + "</pre>"

# -------------------------
# Persistence keys & helpers (new schema)
# -------------------------
async def save_baseline_map(new_map: Dict[str,int]):
    try:
        mapping = {str(k): str(v) for k, v in new_map.items()}
        if mapping:
            await _upstash.hset_map("fpl:prices:current", mapping)
        today_str = datetime.utcnow().astimezone(timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))).date().isoformat()
        snap_key = f"fpl:prices:{today_str}"
        await _upstash.hset_map(snap_key, mapping)
        await _upstash.expire(snap_key, SNAPSHOT_TTL_DAYS * 24 * 3600)
    except Exception:
        logger.exception("save_baseline_map failed")

async def _load_daily_baseline_async(date_str: str) -> Optional[Dict[str,int]]:
    key = f"fpl:prices:{date_str}"
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
                if sv == "" or sv.startswith("[") or sv.startswith("{"):
                    continue
                clean[str(k)] = int(float(sv))
            except Exception:
                continue
        return clean
    except Exception:
        logger.exception("load daily baseline failed")
        return None

# -------------------------
# Season helpers (restrict queries to current season)
# -------------------------
def current_season_range(now: Optional[date] = None) -> Tuple[date, date]:
    # FPL season convention: season runs from Aug 1 to next year Jul 31
    if now is None:
        now = datetime.utcnow().date()
    if now.month >= 8:
        start = date(now.year, 8, 1)
        end = date(now.year+1, 7, 31)
    else:
        start = date(now.year-1, 8, 1)
        end = date(now.year, 7, 31)
    return (start, end)

def assert_within_current_season(d: date) -> bool:
    s, e = current_season_range()
    return s <= d <= e

# -------------------------
# Price detector service (uses hybrid parser + formatting Variant B)
# -------------------------
class PriceDetectorService:
    async def build_prices_msg(self) -> str:
        if livefpl_cb.is_open():
            return "<code>LiveFPL temporarily unavailable</code>"
        try:
            txt = await _http.get_text(LIVEFPL_PRICES)
        except Exception:
            livefpl_cb.record_failure()
            logger.exception("Failed to fetch LiveFPL page")
            return "<code>Could not fetch LiveFPL page.</code>"
        livefpl_cb.record_success()
        try:
            sections = await hybrid_parse_livefpl(txt)
            if not sections:
                return "<code>No price projections parsed.</code>"
            await fetch_bootstrap_cached()  # ensure bootstrap data available
            el_map = _BOOTSTRAP_CACHE.get("el_map", {})
            return format_prices_variant_b(sections, el_map)
        except Exception:
            logger.exception("Error building prices message")
            return "<code>Could not build prices message.</code>"

    def detect_between_maps(self, m1: Dict[str,int], m2: Dict[str,int]) -> List[Tuple[str,int,int]]:
        out = []
        try:
            for k, v2 in m2.items():
                try:
                    v1 = m1.get(k)
                    if v1 is None:
                        continue
                    if int(v1) != int(v2):
                        out.append((k, int(v1), int(v2)))
                except Exception:
                    continue
        except Exception:
            logger.exception("detect_between_maps failed")
        return out

    async def detect_changes_and_update_baseline(self) -> Optional[List[Tuple[str,int,int]]]:
        # baseline generation ONLY via bootstrap-static (requested)
        try:
            data = await fetch_bootstrap_cached()
            if not data:
                return None
            elements = data.get("elements", []) or []
            new_map = {str(el["id"]): int(el.get("now_cost", 0)) for el in elements}
        except Exception:
            logger.exception("failed to build new_map")
            return None
        try:
            pb = await _upstash.hgetall("fpl:prices:current")
            old_map = {}
            for k, v in (pb or {}).items():
                try:
                    old_map[str(k)] = int(v)
                except Exception:
                    continue
        except Exception:
            old_map = {}
        changes = self.detect_between_maps(old_map, new_map)
        if changes:
            await save_baseline_map(new_map)
            try:
                await _upstash.hset_map("fpl:audit:price_changes", {str(int(datetime.utcnow().timestamp())): ",".join([f"{e}:{o}->{n}" for e,o,n in changes])})
            except Exception:
                pass
        else:
            if not old_map:
                await save_baseline_map(new_map)
        return changes

price_service = PriceDetectorService()

# -------------------------
# Circuit breaker & rate limiting (kept minimal)
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
            self.fail_count = 0
            self.open_until = 0
            CB_OPEN.set(0)
            return False
        return True

livefpl_cb = CircuitBreaker()

in_memory_rl: Dict[int, List[int]] = {}

async def rate_limit_check(user_id: int) -> bool:
    try:
        val = await _upstash.incr(f"fpl:rl:{user_id}")
        if val is None:
            raise RuntimeError("upstash incr failed")
        if val == 1:
            await _upstash.expire(f"fpl:rl:{user_id}", RATE_LIMIT_WINDOW)
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
# Authorization (owner + allowed group + persisted whitelist)
# -------------------------
_allowed_users: Set[int] = set()

async def load_allowed_users():
    global _allowed_users
    try:
        au = await _upstash.hgetall("fpl:users:allowed")
        if au:
            _allowed_users = set(int(k) for k in au.keys() if str(k).isdigit())
        else:
            _allowed_users = set()
    except Exception:
        _allowed_users = set()

async def save_allowed_users():
    try:
        mapping = {str(u): "1" for u in sorted(list(_allowed_users))}
        if mapping:
            await _upstash.hset_map("fpl:users:allowed", mapping)
    except Exception:
        logger.exception("save_allowed_users failed")

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
            if uid not in _allowed_users:
                _allowed_users.add(uid)
                asyncio.create_task(save_allowed_users())
            return True
        if chat.type == "private":
            return uid in _allowed_users or uid == OWNER_ID
        return False
    except Exception:
        logger.exception("Authorization check failed")
        return False

async def send_message_secure(app: Application, text: str, *, silent: bool = True, parse_mode: str = "HTML"):
    try:
        await app.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=text, parse_mode=parse_mode,
                                   disable_web_page_preview=True, disable_notification=silent)
        MSG_SENT.inc()
    except Exception:
        logger.exception("send_message_secure failed")
        inc_metric("send_errors")

# -------------------------
# Handler guard (rate limiting + error audit)
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
                await _upstash.hset_map("fpl:audit:errors", {str(int(datetime.utcnow().timestamp())): str(e)})
            except Exception:
                pass
            try:
                if OWNER_ID:
                    await context.application.bot.send_message(chat_id=OWNER_ID, text=f"Handler error: {e}")
            except Exception:
                pass
    return wrapper

# -------------------------
# Handlers: /help, /prices, /price_on
# -------------------------
@handler_guard
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized_update(update):
        return
    text = (
        "FPL Prices Bot (Enterprise — subset)\n"
        "/prices — show LiveFPL price projections (compact, mobile)\n"
        "/price_on YYYY-MM-DD — show changes between that day and next day\n"
        "/help — show this help\n"
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
        d0 = datetime.fromisoformat(date_str).date()
    except Exception:
        await update.message.reply_text("Invalid date format. Use YYYY-MM-DD.")
        return
    # Restrict to current season
    if not assert_within_current_season(d0):
        s,e = current_season_range()
        await update.message.reply_text(f"Date {date_str} is outside current season range {s.isoformat()}..{e.isoformat()}")
        return
    # load baseline for requested date; if absent, fallback to bootstrap-static snapshot for that date
    day1 = await _load_daily_baseline_async(date_str)
    if not day1:
        # fallback: build baseline map from bootstrap (snapshot for that date)
        b = await fetch_bootstrap_cached()
        if not b:
            await update.message.reply_text(f"No baseline for {date_str} and bootstrap fetch failed")
            return
        elements = b.get("elements", []) or []
        day1 = {str(el["id"]): int(el.get("now_cost", 0)) for el in elements}
    next_day = (d0 + timedelta(days=1)).isoformat()
    day2 = await _load_daily_baseline_async(next_day)
    if not day2:
        b = await fetch_bootstrap_cached()
        if not b:
            await update.message.reply_text(f"No baseline for {next_day} and bootstrap fetch failed")
            return
        elements = b.get("elements", []) or []
        day2 = {str(el["id"]): int(el.get("now_cost", 0)) for el in elements}
    changes = price_service.detect_between_maps(day1, day2)
    if not changes:
        await update.message.reply_text(f"<code>No changes between {date_str} and {next_day}</code>", parse_mode="HTML")
        return
    el_map = _BOOTSTRAP_CACHE.get("el_map", {})
    lines = [f"<b>Price changes {date_str} → {next_day}</b>"]
    for eid, o, n in changes:
        el = el_map.get(int(eid), {})
        nm = el.get("web_name", f"id:{eid}")
        lines.append(f"<code>{nm.ljust(20)} {o/10:.1f} → {n/10:.1f}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

# -------------------------
# Background: daily snapshot & supervisor (minimal)
# -------------------------
_bg_task: Optional[asyncio.Task] = None
_shutdown = False
APP_INSTANCE = None

def next_daily_utc5(hour:int, minute:int, now: Optional[datetime]=None) -> datetime:
    if now is None:
        now = datetime.now(timezone.utc)
    tz_base = timezone(timedelta(hours=PRICE_CHANGE_UTC_PLUS))
    local_now = now.astimezone(tz_base)
    target_local = datetime.combine(local_now.date(), dt_time(hour, minute), tzinfo=tz_base)
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

async def daily_snapshot_task(app: Application):
    logger.info("Daily snapshot task started")
    try:
        try:
            changes = await price_service.detect_changes_and_update_baseline()
            if changes:
                logger.info("Initial baseline updated with %d changes", len(changes))
        except Exception:
            logger.exception("Initial baseline update failed")
        while not _shutdown:
            next_run = next_daily_utc5(6, 0)
            await sleep_until(next_run)
            if _shutdown:
                break
            try:
                await price_service.detect_changes_and_update_baseline()
            except Exception:
                logger.exception("daily snapshot detect failed")
    except asyncio.CancelledError:
        logger.info("daily_snapshot_task cancelled")
    except Exception:
        logger.exception("daily_snapshot_task crashed")

_supervisor_task: Optional[asyncio.Task] = None

def start_supervisor(loop, tasks_getter, restart_fn, interval=30):
    async def _loop():
        while not _shutdown:
            try:
                tasks = tasks_getter()
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
    global _bg_task
    if name == "daily_snapshot":
        if _bg_task and not _bg_task.done():
            _bg_task.cancel()
        _bg_task = asyncio.create_task(daily_snapshot_task(APP_INSTANCE))

# -------------------------
# HTTP server for health/metrics
# -------------------------
START_TIME_DT = datetime.utcnow()

async def handle_health(request):
    data = {
        "uptime_seconds": int((datetime.utcnow() - START_TIME_DT).total_seconds()),
        "bootstrap_cached": bool(_BOOTSTRAP_CACHE.get("data")),
        "baseline_count": len(await _upstash.hgetall("fpl:prices:current") or {}),
        "metrics": dict(MET),
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
# Start/stop background tasks lifecycle
# -------------------------
async def start_background_tasks(app: Application):
    try:
        await load_allowed_users()
        await fetch_bootstrap_cached()
        global _bg_task, _supervisor_task, APP_INSTANCE
        APP_INSTANCE = app
        loop = asyncio.get_event_loop()
        if _bg_task is None or _bg_task.done():
            _bg_task = asyncio.create_task(daily_snapshot_task(app))
        def tasks_getter():
            return {"daily_snapshot": _bg_task}
        _supervisor_task = start_supervisor(loop, tasks_getter, restart_background_task, interval=30)
        loop.create_task(start_http_server())
        logger.info("Background tasks started")
    except Exception:
        logger.exception("Failed to start background tasks")

async def stop_background_tasks():
    global _bg_task, _supervisor_task, _shutdown
    _shutdown = True
    tasks = [_bg_task, _supervisor_task]
    for t in tasks:
        if t:
            t.cancel()
    await asyncio.sleep(0.2)
    try:
        await _http.close()
        await _upstash.close()
    except Exception:
        pass

# -------------------------
# App builder and run
# -------------------------
def build_app():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("prices", prices_cmd))
    app.add_handler(CommandHandler("price_on", price_on_cmd))
    app.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), lambda u,c: None))
    async def _on_start(_app: Application):
        logger.info("Bot started (PTB %s)", PTB_VERSION)
        try:
            await _app.bot.set_my_commands([
                ("help","Show help and commands"),
                ("prices","Show LiveFPL price projections"),
                ("price_on","Show changes between days"),
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
