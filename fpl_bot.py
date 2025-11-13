# FPL Telegram Bot — live events, per-user timezone, Squid Game, price predictions
# FULL CODE with timestamp enhancement.
# New change (per request):
# In every command that outputs a table (/rank, /players_pts, /gwpoints, /month, /prices) the header now includes
# a line "Обновлено: YYYY-MM-DD HH:MM (Таймзона UTC±X)" based on the requesting user's timezone.
# DC (Defensive Contribution) is SHOWN again in players_pts Stats column when threshold met.
# Nothing from previous logic has been deliberately removed; live event logic preserved.

import os
import json
import asyncio
import threading
import logging
import time
import signal
import random
import hashlib
import re
from typing import Any, Dict, Optional, List, Tuple, Set, Iterable, Awaitable
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify
import httpx
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler
)
from telegram import (
    Update,
    BotCommand,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

try:
    from upstash_redis import Redis
except ImportError:
    Redis = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

# ===== Config =====
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN required")

TARGET_CHAT_ID = os.environ.get("TARGET_CHAT_ID")
LEAGUE_ID = os.environ.get("LEAGUE_ID", "980121")  # Zakynule Fantasy Vibe

PORT = int(os.environ.get("PORT", "10000"))
TELEGRAM_CONCURRENCY = int(os.environ.get("TELEGRAM_CONCURRENCY", "4"))
USE_WEBHOOK = os.environ.get("USE_WEBHOOK", "0") == "1"

FPL_PROXY_BASE = os.environ.get("FPL_PROXY_BASE", "").rstrip("/")
ENABLE_HTTP2 = os.environ.get("ENABLE_HTTP2", "1") == "1"

LIVE_POLL_INTERVAL = int(os.environ.get("LIVE_POLL_INTERVAL", "30"))

FPL_CACHE_TTL_MIN = int(os.environ.get("FPL_CACHE_TTL", "8"))
FPL_STANDINGS_TTL = int(os.environ.get("FPL_STANDINGS_TTL", "300"))
FPL_PICKS_TTL = int(os.environ.get("FPL_PICKS_TTL", "300"))
FPL_PICKS_ALLOW_STALE = os.environ.get("FPL_PICKS_ALLOW_STALE", "1") == "1"
FPL_CONCURRENCY = int(os.environ.get("FPL_CONCURRENCY", "3"))
PICKS_CONCURRENCY = int(os.environ.get("PICKS_CONCURRENCY", "10"))

FIXTURES_TTL = int(os.environ.get("FIXTURES_TTL", "900"))
LEAGUE_PLAYERS_TTL = 300
IDEMPOTENCY_TTL_SEC = 10 * 24 * 3600

TIMEZONE_ENV = os.environ.get("TIMEZONE_OFFSET_HOURS")
INITIAL_TZ_OFFSET = int(TIMEZONE_ENV) if TIMEZONE_ENV and TIMEZONE_ENV.lstrip("+-").isdigit() else None
TZ_REDIS_KEY_PREFIX = "fpl:tz:user:"
DEFAULT_TZ_NAME = "Астана"

TZ_MAP = {
    "Астана": ("Asia/Almaty", 5),
    "Баку": ("Asia/Baku", 4),
    "Москва": ("Europe/Moscow", 3),
    "Киев": ("Europe/Kyiv", 2),
    "Центральная Европа": ("Europe/Berlin", 1),
    "Лондон": ("Europe/London", 0),
    "Бишкек": ("Asia/Bishkek", 6),
    "Новосибирск": ("Asia/Novosibirsk", 7),
    "Владивосток": ("Asia/Vladivostok", 10),
}

logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)
logger = logging.getLogger("fpl_bot")

flask_app = Flask(__name__)

# Health metrics
last_live_tick_time: Optional[float] = None
last_active_fixtures_count: int = 0
last_redis_latency_ms: Optional[float] = None

@flask_app.route("/")
def index():
    return "FPL BOT live running"

@flask_app.route("/healthz")
def health():
    return jsonify({
        "ok": True,
        "bootstrap_cached": bootstrap_cache_ts is not None,
        "standings_cached": standings_cache_ts is not None,
        "fixtures_cache_entries": len(_fixtures_cache),
        "redis_connected": redis_client is not None,
        "season_tag": SEASON_TAG,
        "league_name": LEAGUE_NAME,
        "squid_active": squid_active_state is not None,
        "last_live_tick_time": last_live_tick_time,
        "active_fixtures_count": last_active_fixtures_count,
        "redis_latency_ms": last_redis_latency_ms
    })

def start_flask():
    logger.info(f"Starting Flask on {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT)

# ===== HTTP =====
def fpl_url(path: str) -> str:
    base = FPL_PROXY_BASE or "https://fantasy.premierleague.com"
    return f"{base}{path}"

FPL_BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://fantasy.premierleague.com/",
    "Origin": "https://fantasy.premierleague.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

http_client: Optional[httpx.AsyncClient] = None
application: Optional[Application] = None
fpl_semaphore = asyncio.Semaphore(FPL_CONCURRENCY)
picks_semaphore = asyncio.Semaphore(PICKS_CONCURRENCY)

async def fetch_json(url: str, timeout: float = 15.0, attempts: int = 3, extra_headers: Optional[Dict[str,str]]=None, return_response: bool=False):
    headers = dict(FPL_BASE_HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    for attempt in range(1, attempts + 1):
        await asyncio.sleep(random.uniform(0.02, 0.08))
        try:
            async with fpl_semaphore:
                resp = await http_client.get(url, headers=headers, timeout=timeout)
            if return_response:
                return resp
            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception as ex:
                    logger.warning(f"JSON decode error {url}: {ex}")
                    return None
            if resp.status_code in (403, 429) or 500 <= resp.status_code < 600:
                if attempt < attempts:
                    await asyncio.sleep(min(2 ** attempt, 8))
                    continue
            return None
        except httpx.TimeoutException:
            if attempt < attempts:
                await asyncio.sleep(min(2 ** attempt, 8))
        except Exception as ex:
            logger.debug(f"fetch_json error {url}: {ex}")
            if attempt < attempts:
                await asyncio.sleep(min(2 ** attempt, 8))
    return None

# ===== Paths =====
bootstrap_path = "/api/bootstrap-static/"
league_path_tpl = "/api/leagues-classic/{league_id}/standings/?page_standings={page}"
entry_picks_path_tpl = "/api/entry/{entry_id}/event/{gw}/picks/"
event_live_path_tpl = "/api/event/{gw}/live/"
fixtures_event_path_tpl = "/api/fixtures/?event={gw}"

# ===== Caches / Globals =====
bootstrap_cache: Dict[str, Any] = {}
bootstrap_cache_ts: Optional[float] = None
_bootstrap_etag: Optional[str] = None
bootstrap_lock = asyncio.Lock()

standings_cache: Dict[str, Any] = {}
standings_cache_ts: Optional[float] = None
standings_lock = asyncio.Lock()

_fixtures_cache: Dict[int, Tuple[float, List[Dict]]] = {}
picks_cache: Dict[Tuple[int, int], Dict[str, Any]] = {}
_picks_locks: Dict[Tuple[int, int], asyncio.Lock] = {}
_picks_locks_guard = asyncio.Lock()

SEASON_TAG: Optional[str] = None
LEAGUE_NAME: Optional[str] = None

stop_event = asyncio.Event()
redis_client: Optional["Redis"] = None

PLAYER_NAME_MAP: Dict[int, str] = {}
PLAYER_POS_MAP: Dict[int, int] = {}
PLAYER_TEAM_MAP: Dict[int, int] = {}
TEAM_SHORT_MAP: Dict[int, str] = {}

_league_players_cache: Dict[int, Tuple[float, Set[int]]] = {}
_league_player_pick_counts: Dict[int, Tuple[float, Dict[int,int]]] = {}

_second_yellow_processed: Set[str] = set()
_red_card_processed: Set[str] = set()
_dc_awarded: Set[str] = set()
_dc_removed_sent: Set[str] = set()
_cs_subbed_sent: Set[str] = set()
_fixture_summary_sent: Set[str] = set()

_last_counts: Dict[Tuple[int,int,int,str], int] = {}
_current_gw: Optional[int] = None
_sent_msg_hashes: Set[str] = set()

_tz_offset_memory: Dict[int, Tuple[str,int]] = {}

ENTRY_TO_MANAGER_NAME: Dict[int, str] = {}
ENTRY_TO_RANK: Dict[int, int] = {}
ENTRY_TO_OVERALL: Dict[int, int] = {}

# Squid
squid_active_state: Optional[Dict[str, Any]] = None
squid_winner_history: List[Dict[str, Any]] = []
squid_lock = asyncio.Lock()

# Prices
prices_cache_data: Optional[Dict[str, Any]] = None
prices_cache_ts: Optional[float] = None
PRICES_CACHE_TTL = 600
PRICES_URL = "https://www.livefpl.net/prices"

# ===== Helpers =====
async def gather_limited(coros: Iterable[Awaitable], limit: int) -> List[Any]:
    sem = asyncio.Semaphore(limit)
    async def run(coro):
        async with sem:
            return await coro
    return await asyncio.gather(*(run(c) for c in coros), return_exceptions=True)

def manager_name(entry_id: int) -> str:
    return ENTRY_TO_MANAGER_NAME.get(entry_id, f"Entry {entry_id}")

def manager_tag_overall(entry_id: int) -> str:
    r = ENTRY_TO_RANK.get(entry_id)
    nm = manager_name(entry_id)
    if r is None:
        return nm
    return f"#{r} {nm}"

def manager_tag_local_plain(index1: int, entry_id: int) -> str:
    return f"{index1} {manager_name(entry_id)}"

def init_redis():
    global redis_client
    if Redis is None:
        logger.info("Redis absent, memory fallback.")
        return
    try:
        redis_client = Redis.from_env()
        logger.info("Redis connected.")
    except Exception as e:
        logger.warning(f"Redis init failed: {e}")
        redis_client = None

async def measure_redis_latency():
    global last_redis_latency_ms
    if not redis_client:
        last_redis_latency_ms = None
        return
    try:
        t0 = time.time()
        redis_client.set("fpl:health:pulse", "1", ex=30)
        _ = redis_client.get("fpl:health:pulse")
        last_redis_latency_ms = round((time.time() - t0)*1000,2)
    except Exception:
        last_redis_latency_ms = None

def get_user_tz(user_id: int) -> Tuple[str,int]:
    if redis_client:
        try:
            raw = redis_client.get(TZ_REDIS_KEY_PREFIX + str(user_id))
            if raw:
                try:
                    obj = json.loads(raw)
                    return obj.get("rus","Астана"), int(obj.get("offset",5))
                except Exception:
                    parts = str(raw).split("|")
                    if len(parts)>=2 and parts[1].lstrip("+-").isdigit():
                        return parts[0], int(parts[1])
        except Exception:
            pass
    mem = _tz_offset_memory.get(user_id)
    if mem: return mem
    return DEFAULT_TZ_NAME, TZ_MAP[DEFAULT_TZ_NAME][1]

def set_user_tz(user_id: int, rus_name: str, offset: int, zone: str):
    _tz_offset_memory[user_id]=(rus_name,offset)
    if redis_client:
        try:
            obj={"rus":rus_name,"offset":offset,"zone":zone,"ts":int(time.time())}
            redis_client.set(TZ_REDIS_KEY_PREFIX+str(user_id), json.dumps(obj))
        except Exception:
            logger.warning("Redis set tz failed")

def resolve_tz_arg(arg: str) -> Optional[Tuple[str,int,str]]:
    if arg in TZ_MAP:
        zone,off=TZ_MAP[arg]; return (arg,off,zone)
    if arg.startswith(("+","-")) or arg.isdigit():
        raw=arg[1:] if arg.startswith("+") else arg
        if raw.lstrip("-").isdigit():
            off=int(raw)
            if -12<=off<=14:
                rus_name=f"UTC{off:+d}"
                zone=f"Etc/GMT{-off}"
                return (rus_name,off,zone)
    if "/" in arg:
        for rus,(zone,off) in TZ_MAP.items():
            if zone.lower()==arg.lower(): return (rus,off,zone)
        return (arg,0,arg)
    return None

def bootstrap_valid() -> bool:
    return bootstrap_cache_ts is not None and (time.time()-bootstrap_cache_ts)/60 < FPL_CACHE_TTL_MIN

async def get_bootstrap_cached() -> Optional[Dict]:
    global bootstrap_cache_ts,_bootstrap_etag
    async with bootstrap_lock:
        headers={}
        if _bootstrap_etag: headers["If-None-Match"]=_bootstrap_etag
        resp=await fetch_json(fpl_url(bootstrap_path), extra_headers=headers, return_response=True)
        if isinstance(resp,httpx.Response):
            if resp.status_code==304 and bootstrap_cache:
                return bootstrap_cache
            if resp.status_code==200:
                try:
                    data=resp.json()
                    bootstrap_cache.clear(); bootstrap_cache.update(data)
                    bootstrap_cache_ts=time.time()
                    _bootstrap_etag=resp.headers.get("ETag") or _bootstrap_etag
                    refresh_bootstrap_maps()
                    return data
                except Exception as ex:
                    logger.warning(f"bootstrap decode error: {ex}")
                    return None
            return bootstrap_cache if bootstrap_cache else None
        return bootstrap_cache if bootstrap_cache else None

def refresh_bootstrap_maps():
    global PLAYER_NAME_MAP,PLAYER_POS_MAP,PLAYER_TEAM_MAP,TEAM_SHORT_MAP
    elements=bootstrap_cache.get("elements",[])
    teams=bootstrap_cache.get("teams",[])
    PLAYER_NAME_MAP={el.get("id"):(el.get("web_name") or el.get("second_name") or f"P{el.get('id')}") for el in elements if isinstance(el.get("id"),int)}
    PLAYER_POS_MAP={el.get("id"):el.get("element_type") for el in elements if isinstance(el.get("id"),int) and isinstance(el.get("element_type"),int)}
    PLAYER_TEAM_MAP={el.get("id"):el.get("team") for el in elements if isinstance(el.get("id"),int) and isinstance(el.get("team"),int)}
    TEAM_SHORT_MAP={t.get("id"):(t.get("short_name") or t.get("name") or f"T{t.get('id')}") for t in teams if isinstance(t.get("id"),int)}

def fixtures_cache_valid(ts: float) -> bool:
    return (time.time()-ts) < FIXTURES_TTL

async def get_fixtures_cached(gw: int) -> List[Dict]:
    st=_fixtures_cache.get(gw)
    if st and fixtures_cache_valid(st[0]): return st[1]
    data=await fetch_json(fpl_url(fixtures_event_path_tpl.format(gw=gw))) or []
    _fixtures_cache[gw]=(time.time(),data); return data

def standings_valid() -> bool:
    return standings_cache_ts is not None and (time.time()-standings_cache_ts)<=FPL_STANDINGS_TTL

async def get_league_results_cached(league_id: str) -> Optional[List[Dict]]:
    global LEAGUE_NAME, standings_cache_ts, ENTRY_TO_MANAGER_NAME, ENTRY_TO_RANK, ENTRY_TO_OVERALL
    if standings_valid() and "results" in standings_cache:
        ln=standings_cache.get("league_name")
        if ln: LEAGUE_NAME=ln
        results=standings_cache.get("results") or []
        ENTRY_TO_MANAGER_NAME={r["entry"]:r.get("player_name","") for r in results if isinstance(r.get("entry"),int)}
        ENTRY_TO_RANK={r["entry"]:r.get("rank") for r in results if isinstance(r.get("entry"),int)}
        ENTRY_TO_OVERALL={r["entry"]:r.get("total") for r in results if isinstance(r.get("entry"),int)}
        return results
    async with standings_lock:
        if standings_valid() and "results" in standings_cache:
            ln=standings_cache.get("league_name")
            if ln: LEAGUE_NAME=ln
            results=standings_cache.get("results") or []
            ENTRY_TO_MANAGER_NAME={r["entry"]:r.get("player_name","") for r in results if isinstance(r.get("entry"),int)}
            ENTRY_TO_RANK={r["entry"]:r.get("rank") for r in results if isinstance(r.get("entry"),int)}
            ENTRY_TO_OVERALL={r["entry"]:r.get("total") for r in results if isinstance(r.get("entry"),int)}
            return results
        all_results=[]
        page=1
        league_name_local=None
        while True:
            url=fpl_url(league_path_tpl.format(league_id=league_id,page=page))
            data=await fetch_json(url)
            if not data: break
            try:
                standings=data["standings"]
                if league_name_local is None:
                    league_name_local=standings.get("league",{}).get("name")
                results=standings["results"]
                all_results.extend(results)
                if not standings.get("has_next"): break
                page+=1
                if page>30: break
            except Exception as ex:
                logger.warning(f"Standings decode error: {ex}")
                break
        if all_results:
            standings_cache.clear()
            standings_cache.update({"results":all_results,"pages":page,"league_name":league_name_local})
            standings_cache_ts=time.time()
            LEAGUE_NAME=league_name_local or LEAGUE_NAME or "Zakynule Fantasy Vibe"
            ENTRY_TO_MANAGER_NAME={r["entry"]:r.get("player_name","") for r in all_results if isinstance(r.get("entry"),int)}
            ENTRY_TO_RANK={r["entry"]:r.get("rank") for r in all_results if isinstance(r.get("entry"),int)}
            ENTRY_TO_OVERALL={r["entry"]:r.get("total") for r in all_results if isinstance(r.get("entry"),int)}
            return all_results
        if not LEAGUE_NAME:
            LEAGUE_NAME="Zakynule Fantasy Vibe"
        return None

def picks_valid(ts: Optional[float]) -> bool:
    return ts is not None and (time.time()-ts)<=FPL_PICKS_TTL

async def _picks_lock(key: Tuple[int,int]) -> asyncio.Lock:
    async with _picks_locks_guard:
        lk=_picks_locks.get(key)
        if not lk:
            lk=asyncio.Lock(); _picks_locks[key]=lk
        return lk

async def get_entry_picks_cached(entry: int, gw: int) -> Optional[Dict]:
    key=(entry,gw)
    cached=picks_cache.get(key)
    if cached and picks_valid(cached.get("ts")):
        return cached.get("data")
    lock=await _picks_lock(key)
    async with lock:
        c2=picks_cache.get(key)
        if c2 and picks_valid(c2.get("ts")):
            return c2.get("data")
        async with picks_semaphore:
            data=await fetch_json(fpl_url(entry_picks_path_tpl.format(entry_id=entry,gw=gw)))
        if data:
            picks_cache[key]={"data":data,"ts":time.time()}
            return data
        if FPL_PICKS_ALLOW_STALE and c2 and c2.get("data"):
            logger.warning(f"Serve stale picks entry={entry} gw={gw}")
            return c2.get("data")
        return None

def discover_season_tag(bootstrap: Dict[str,Any]) -> str:
    global SEASON_TAG
    if SEASON_TAG: return SEASON_TAG
    season=bootstrap.get("game_settings",{}).get("season")
    if not season:
        y=datetime.now().year
        season=f"{y}/{str((y+1)%100).zfill(2)}"
    SEASON_TAG=season
    return SEASON_TAG

async def get_league_player_ids_cached(gw: int) -> Set[int]:
    now=time.time()
    cached=_league_players_cache.get(gw)
    if cached and now-cached[0] <= LEAGUE_PLAYERS_TTL:
        return cached[1]
    results=await get_league_results_cached(LEAGUE_ID)
    if not results:
        _league_players_cache[gw]=(now,set()); return set()
    coros=[]
    for r in results:
        eid=r.get("entry")
        if isinstance(eid,int):
            coros.append(get_entry_picks_cached(eid,gw))
    data_all=await gather_limited(coros,PICKS_CONCURRENCY)
    players=set()
    for pkg in data_all:
        if isinstance(pkg,dict):
            for p in pkg.get("picks",[]):
                elid=p.get("element")
                if isinstance(elid,int):
                    players.add(elid)
    _league_players_cache[gw]=(now,players)
    return players

async def get_league_player_pick_counts(gw: int) -> Dict[int,int]:
    now=time.time()
    cached=_league_player_pick_counts.get(gw)
    if cached and now-cached[0] <= LEAGUE_PLAYERS_TTL:
        return cached[1]
    results=await get_league_results_cached(LEAGUE_ID)
    counts: Dict[int,int]={}
    if not results:
        _league_player_pick_counts[gw]=(now,counts); return counts
    coros=[]
    for r in results:
        eid=r.get("entry")
        if isinstance(eid,int):
            coros.append(get_entry_picks_cached(eid,gw))
    data_all=await gather_limited(coros,PICKS_CONCURRENCY)
    for pkg in data_all:
        if isinstance(pkg,dict):
            for p in pkg.get("picks",[]):
                elid=p.get("element")
                if isinstance(elid,int):
                    counts[elid]=counts.get(elid,0)+1
    _league_player_pick_counts[gw]=(now,counts)
    return counts

def get_current_or_next_gw(bootstrap: Dict[str,Any]) -> Optional[int]:
    current_ev=next((e for e in bootstrap.get("events",[]) if e.get("is_current")),None)
    if current_ev and isinstance(current_ev.get("id"),int):
        return current_ev.get("id")
    next_ev=next((e for e in bootstrap.get("events",[]) if e.get("is_next")),None)
    if next_ev and isinstance(next_ev.get("id"),int):
        return next_ev.get("id")
    return None

# ===== Live helpers =====
def get_active_fixture_ids(fixtures: List[Dict], now: Optional[datetime]=None) -> Set[int]:
    now=now or datetime.now(timezone.utc)
    active=set()
    for fx in fixtures:
        fid=fx.get("id")
        if not isinstance(fid,int): continue
        if fx.get("finished"): continue
        kt=fx.get("kickoff_time")
        dt=parse_deadline(kt) if isinstance(kt,str) else None
        if dt and dt<=now: active.add(fid)
    return active

def format_points(delta: int) -> str:
    return f"(+{delta})" if delta>0 else f"({delta})"

def total_no_bonus(stats: Dict[str,Any]) -> int:
    return int(stats.get("total_points",0) or 0) - int(stats.get("bonus",0) or 0)

def event_points_for(pos: int, etype: str) -> int:
    if etype=="Goal":
        if pos in (1,2): return 6
        if pos==3: return 5
        if pos==4: return 4
    if etype=="Assist": return 3
    if etype=="Own goal": return -2
    if etype=="Penalty missed": return -2
    if etype=="Penalty saved": return 5
    if etype=="Second yellow → red": return -3
    if etype=="Red card": return -3
    if etype=="Clean sheet":
        if pos in (1,2): return 4
        if pos==3: return 1
        return 0
    if etype=="DC": return 2
    return 0

def dc_threshold_met(stats: Dict[str,Any], pos: int) -> bool:
    if pos==1: return False
    dc=0
    for k in ("defensive_contributions","cbit","cbits","def_contributions"):
        v=stats.get(k)
        if isinstance(v,(int,float)):
            dc=int(v); break
    if pos==2: return dc>=10
    if pos in (3,4): return dc>=12
    return False

def ns_key(gw: int, fid: int, pid: int) -> str:
    return f"{gw}:{fid}:{pid}"

def message_hash(text: str) -> str:
    return hashlib.blake2s(text.encode("utf-8")).hexdigest()

def idempotency_set_key(season: str, gw: int) -> str:
    return f"live:msgs:{season}:{gw}"

def split_message_chunks(text: str, limit: int=3900) -> List[str]:
    if len(text)<=limit: return [text]
    out=[]; cur=""
    for ln in text.splitlines(keepends=True):
        if len(cur)+len(ln)>limit:
            out.append(cur.rstrip("\n")); cur=""
        cur+=ln
    if cur: out.append(cur.rstrip("\n"))
    return out

async def send_text_raw(chat_id: int, text: str):
    for chunk in split_message_chunks(text):
        try:
            await application.bot.send_message(chat_id=chat_id, text=chunk)
            await asyncio.sleep(0.03)
        except Exception:
            logger.exception("send_message failed")

async def send_once(text: str, season: str, gw: int):
    key=idempotency_set_key(season,gw)
    h=message_hash(text)
    should=True
    if redis_client:
        try:
            added=redis_client.sadd(key,h)
            should=(added==1 or added=="1")
            redis_client.expire(key,IDEMPOTENCY_TTL_SEC)
        except Exception:
            should=h not in _sent_msg_hashes
    else:
        should=h not in _sent_msg_hashes
    if should:
        if not redis_client: _sent_msg_hashes.add(h)
        if TARGET_CHAT_ID and TARGET_CHAT_ID.isdigit() and application:
            await send_text_raw(int(TARGET_CHAT_ID), text)
        else:
            logger.info("LIVE:\n"+text)

def parse_deadline(dt_str: str) -> Optional[datetime]:
    if not dt_str: return None
    try:
        if dt_str.endswith("Z"):
            dt_str=dt_str.replace("Z","+00:00")
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None

# Helper for timestamp lines
def user_timestamp_line(user_id: Optional[int], width: int) -> str:
    if user_id is None:
        tz_name="UTC"
        offset=0
        now_local=datetime.utcnow()
    else:
        tz_name, offset = get_user_tz(user_id)
        now_local = datetime.utcnow() + timedelta(hours=offset)
    ts_line = f"Обновлено: {now_local.strftime('%Y-%m-%d %H:%M')} ({tz_name} UTC{offset:+d})"
    if width > 0:
        return ts_line.center(width)
    return ts_line

# ===== Squid Game =====
SQUID_ACTIVE_KEY="fpl:squid:active"
SQUID_WINNERS_KEY="fpl:squid:winners"

def load_squid_from_redis():
    global squid_active_state,squid_winner_history
    if not redis_client: return
    try:
        active=redis_client.get(SQUID_ACTIVE_KEY)
        if active:
            try: squid_active_state=json.loads(active)
            except Exception: squid_active_state=None
        winners=redis_client.get(SQUID_WINNERS_KEY)
        if winners:
            try: squid_winner_history=json.loads(winners)
            except Exception: squid_winner_history=[]
    except Exception: pass

def persist_squid():
    if not redis_client: return
    try:
        if squid_active_state is not None:
            redis_client.set(SQUID_ACTIVE_KEY,json.dumps(squid_active_state))
        redis_client.set(SQUID_WINNERS_KEY,json.dumps(squid_winner_history))
    except Exception:
        logger.warning("Persist squid failed")

async def squid_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global squid_active_state
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Использование: /squid_start <gw>"); return
    start_gw=int(context.args[0])
    results=await get_league_results_cached(LEAGUE_ID)
    if not results:
        await update.message.reply_text("Лига недоступна."); return
    players=[r.get("entry") for r in results if isinstance(r.get("entry"),int)]
    squid_active_state={
        "start_gw":start_gw,
        "current_gw":start_gw,
        "players_alive":players,
        "players_eliminated":[],
        "season":SEASON_TAG,
        "cycle":1
    }
    persist_squid()
    await update.message.reply_text(f"Запущен Squid Game с GW {start_gw}. Участников: {len(players)}.")

async def squid_rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text=(
        "Squid Game — правила:\n"
        "• Старт с указанного GW.\n"
        "• После тура считаем GW очки оставшихся.\n"
        "• Проходят те, у кого очки >= среднего.\n"
        "• Игра до одного победителя или GW38.\n"
        "• Победитель раньше 38 → новый цикл со следующего GW.\n"
        "• В GW38 выигрывает самый результативный по GW очкам среди живых."
    )
    await update.message.reply_text(text)

async def squid_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not squid_active_state:
        await update.message.reply_text("Турнир не активен. /squid_start <gw>")
        return
    current_gw=squid_active_state["current_gw"]
    alive=squid_active_state["players_alive"]
    pts_map={}
    coros=[get_entry_picks_cached(eid,current_gw) for eid in alive]
    data_all=await gather_limited(coros,PICKS_CONCURRENCY)
    for eid,data in zip(alive,data_all):
        if isinstance(data,dict):
            pts=data.get("entry_history",{}).get("points")
            pts_map[eid]=pts if isinstance(pts,int) else 0
        else:
            pts_map[eid]=0
    ordered=sorted(alive,key=lambda e: pts_map.get(e,0), reverse=True)
    lines=[
        f"Squid Game статус (цикл {squid_active_state.get('cycle',1)})",
        f"Диапазон: GW {squid_active_state['start_gw']} - GW {current_gw}",
        f"Живых: {len(alive)}",
        user_timestamp_line(update.effective_user.id if update.effective_user else None,0)
    ]
    for i,eid in enumerate(ordered, start=1):
        lines.append(f"{i}. {manager_name(eid)} ({pts_map.get(eid,0)} pts)")
    await update.message.reply_text("\n".join(lines))

async def squid_winners_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not squid_winner_history:
        await update.message.reply_text("Победителей пока нет.")
        return
    lines=["Прошлые победители:", user_timestamp_line(update.effective_user.id if update.effective_user else None,0)]
    for w in squid_winner_history:
        lines.append(f"Цикл {w.get('cycle')} — {manager_name(w.get('winner_entry'))} (GW {w.get('start_gw')} - GW {w.get('end_gw')})")
    await update.message.reply_text("\n".join(lines))

async def process_squid_after_gw_finish(finished_gw: int):
    global squid_active_state
    if not squid_active_state: return
    if finished_gw != squid_active_state["current_gw"]: return
    alive=squid_active_state["players_alive"]
    if not alive: return
    points_map={}
    async def fetch_pts(eid:int):
        data=await get_entry_picks_cached(eid, finished_gw)
        pts=data.get("entry_history",{}).get("points") if data else 0
        return eid, pts if isinstance(pts,int) else 0
    res=await gather_limited([fetch_pts(e) for e in alive], PICKS_CONCURRENCY)
    values=[]
    for eid,pts in res:
        points_map[eid]=pts
        values.append(pts)
    if not values: return
    avg=sum(values)/len(values)
    passed=[e for e in alive if points_map.get(e,0)>=avg]
    eliminated=[e for e in alive if e not in passed]

    gw38=(finished_gw==38)
    winner_entry=None
    if gw38:
        winner_entry=max(alive,key=lambda e: points_map.get(e,0))
    elif len(passed)<=1:
        if len(passed)==1: winner_entry=passed[0]
        elif len(passed)==0: winner_entry=max(alive,key=lambda e: points_map.get(e,0))

    lines=[f"Squid Game — GW {finished_gw} завершён.", f"Среднее очков: {avg:.2f}", "Очки тура:"]
    for eid in sorted(alive,key=lambda e: points_map.get(e,0), reverse=True):
        lines.append(f"- {manager_name(eid)}: {points_map.get(eid,0)} pts")

    if winner_entry is not None:
        lines.append(f"Победитель цикла: {manager_name(winner_entry)}")
        squid_winner_history.append({
            "cycle": squid_active_state.get("cycle",1),
            "winner_entry": winner_entry,
            "start_gw": squid_active_state["start_gw"],
            "end_gw": finished_gw
        })
        if not gw38 and finished_gw<38:
            next_gw=finished_gw+1
            results=await get_league_results_cached(LEAGUE_ID)
            if results:
                all_players=[r.get("entry") for r in results if isinstance(r.get("entry"),int)]
                squid_active_state={
                    "start_gw":next_gw,
                    "current_gw":next_gw,
                    "players_alive":all_players,
                    "players_eliminated":[],
                    "season":SEASON_TAG,
                    "cycle":squid_winner_history[-1]["cycle"]+1
                }
                lines.append(f"Новый цикл с GW {next_gw}. Участников: {len(all_players)}.")
            else:
                squid_active_state=None
                lines.append("Не удалось получить лигу для нового цикла.")
        else:
            lines.append("Турнир завершён.")
            squid_active_state=None
    else:
        lines.append(f"Прошли дальше ({len(passed)}):")
        for i,eid in enumerate(sorted(passed,key=lambda e: points_map.get(e,0), reverse=True), start=1):
            lines.append(f"- {i}. {manager_name(eid)}: {points_map.get(eid,0)} pts")
        if len(passed)!=1:
            lines.append(f"Выбыли ({len(eliminated)}):")
            for i,eid in enumerate(sorted(eliminated,key=lambda e: points_map.get(e,0), reverse=True), start=1):
                lines.append(f"- {i}. {manager_name(eid)}: {points_map.get(eid,0)} pts")
        squid_active_state["players_alive"]=passed
        squid_active_state["players_eliminated"].extend(eliminated)
        squid_active_state["current_gw"]=finished_gw+1

    persist_squid()
    chat_id=int(TARGET_CHAT_ID) if TARGET_CHAT_ID and TARGET_CHAT_ID.isdigit() else None
    text="\n".join(lines)
    if chat_id and application:
        await send_text_raw(chat_id,text)
    else:
        logger.info("SquidGame report:\n"+text)

# ===== Prices parsing =====
def prices_cache_valid() -> bool:
    return prices_cache_ts is not None and (time.time()-prices_cache_ts)<=PRICES_CACHE_TTL

async def fetch_prices_page() -> Optional[str]:
    try:
        resp=await http_client.get(PRICES_URL, timeout=20.0)
        if resp.status_code==200: return resp.text
    except Exception as ex:
        logger.warning(f"Prices fetch error: {ex}")
    return None

async def fetch_prices_json_candidates() -> Optional[Any]:
    candidates=[
        "https://www.livefpl.net/api/prices",
        "https://www.livefpl.net/api/prices?summary=1",
        "https://prices.livefpl.net/latest_prices.json",
        "https://prices.livefpl.net/summary.json"
    ]
    for url in candidates:
        try:
            resp=await http_client.get(url, timeout=15.0)
            if resp.status_code==200:
                try: return resp.json()
                except Exception: continue
        except Exception: continue
    return None

def build_ownership_map() -> Dict[str,float]:
    mp={}
    for el in bootstrap_cache.get("elements",[]):
        name=(el.get("web_name") or el.get("second_name") or "").strip()
        sel=el.get("selected_by_percent")
        try: own=float(sel) if sel is not None else 0.0
        except Exception: own=0.0
        if name: mp[name.lower()]=own
    return mp

def _deep_iter(obj):
    if isinstance(obj,dict):
        for k,v in obj.items():
            yield (k,v); yield from _deep_iter(v)
    elif isinstance(obj,list):
        for it in obj: yield from _deep_iter(it)

def _extract_name_and_progress(item: Any) -> Optional[Tuple[str,str]]:
    if isinstance(item,dict):
        name=item.get("name") or item.get("web_name") or item.get("player") or item.get("player_name")
        prog=item.get("progress") or item.get("percent") or item.get("value") or item.get("priceTargetPercent") or item.get("completion")
        if name is not None and prog is not None:
            try:
                p=float(prog); return (str(name).strip(), f"{int(round(p))}%")
            except Exception:
                ps=str(prog).strip()
                if ps.endswith("%") or ps.isdigit():
                    if not ps.endswith("%"): ps=f"{ps}%"
                    return (str(name).strip(), ps)
    elif isinstance(item,str):
        m=re.match(r"(.+?)\s*\((\d{1,3})%?\)", item.strip())
        if m:
            return (m.group(1).strip(), f"{m.group(2)}%")
    return None

def dedup_keep_order(items: List[Tuple[str,str]]) -> List[Tuple[str,str]]:
    seen=set(); out=[]
    for nm,pr in items:
        key=nm.lower()
        if key in seen: continue
        seen.add(key); out.append((nm,pr))
    return out

def parse_prices_from_json(data: Any, ownership_map: Dict[str,float]) -> Dict[str,Dict[str,List[Tuple[str,str]]]]:
    blocks={"Rises":{}, "Falls":{}}
    rises=[]; falls=[]
    for k,v in _deep_iter(data):
        if isinstance(v,list) and v and all(isinstance(x,(dict,str)) for x in v):
            parent=str(k).lower()
            buf=[]
            for it in v:
                pair=_extract_name_and_progress(it)
                if pair:
                    nm,pr=pair
                    if ownership_map.get(nm.lower(),0.0)>=1.0:
                        buf.append((nm,pr))
            if buf:
                if any(w in parent for w in ["rise","riser","rises","up"]):
                    rises.extend(buf)
                elif any(w in parent for w in ["fall","faller","falls","down"]):
                    falls.extend(buf)
    if rises: blocks["Rises"]["Projected"] = dedup_keep_order(rises)
    if falls: blocks["Falls"]["Projected"] = dedup_keep_order(falls)
    return blocks

def parse_prices_from_html(html: str, ownership_map: Dict[str,float]) -> Dict[str,Dict[str,List[Tuple[str,str]]]]:
    blocks={"Rises":{}, "Falls":{}}
    if BeautifulSoup:
        try:
            soup=BeautifulSoup(html,"html.parser")
            for script in soup.find_all("script"):
                try:
                    if script.get("type")=="application/json":
                        data=json.loads(script.string or "")
                        blk=parse_prices_from_json(data,ownership_map)
                        if any(blk[s] for s in ("Rises","Falls")): return blk
                except Exception: continue
        except Exception as ex:
            logger.warning(f"NEXT_DATA scan error: {ex}")
    text=re.sub(r"\s+"," ",html)
    pattern=r"([A-Z][A-Za-z\.' -]{2,})\s*\((\d{1,3})%?\)"
    items=re.findall(pattern,text)
    rises=[]
    for nm,pr in items:
        if ownership_map.get(nm.lower(),0.0)>=1.0:
            pct=f"{pr}%"
            rises.append((nm,pct))
    if rises: blocks["Rises"]["Detected"]=dedup_keep_order(rises)
    return blocks

async def get_prices_data() -> Dict[str,Any]:
    global prices_cache_data, prices_cache_ts
    if prices_cache_valid() and prices_cache_data: return prices_cache_data
    ownership_map=build_ownership_map()
    data_json=await fetch_prices_json_candidates()
    parsed={"Rises":{}, "Falls":{}}
    if data_json is not None:
        parsed=parse_prices_from_json(data_json, ownership_map)
    if not any(parsed[s] for s in ("Rises","Falls")):
        html=await fetch_prices_page()
        if not html:
            return {"ok":False,"error":"fetch_failed"}
        parsed=parse_prices_from_html(html, ownership_map)
    data={"ok":True,"parsed":parsed,"ts":time.time()}
    prices_cache_data=data; prices_cache_ts=time.time()
    if redis_client:
        try: redis_client.set("fpl:prices:cache", json.dumps(data), ex=PRICES_CACHE_TTL)
        except Exception: pass
    return data

async def prices_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs=await get_bootstrap_cached()
    if not bs:
        await update.message.reply_text("Нет bootstrap."); return
    if redis_client and not prices_cache_valid():
        try:
            raw=redis_client.get("fpl:prices:cache")
            if raw:
                obj=json.loads(raw)
                prices_cache_data=obj
                prices_cache_ts=obj.get("ts")
        except Exception: pass
    data=await get_prices_data()
    if not data.get("ok"):
        await update.message.reply_text("Price data unavailable.")
        return
    parsed=data["parsed"]
    user_id = update.effective_user.id if update.effective_user else None
    ts_line = user_timestamp_line(user_id,0)
    lines=[f"Price Predictions (>=1% ownership)", ts_line, "```"]
    for side in ("Rises","Falls"):
        block=parsed.get(side,{})
        lines.append(f"{side}:")
        if not block:
            lines.append("  (none)")
        else:
            for cat,items in block.items():
                lines.append(f"  {cat}:")
                for nm,pr in items:
                    lines.append(f"    - {nm}: {pr}")
    lines.append("```")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ===== Inline TZ keyboards =====
def build_tz_main_keyboard():
    buttons=[
        [InlineKeyboardButton("Астана (UTC+5)", callback_data="tz|Астана"),
         InlineKeyboardButton("Баку (UTC+4)", callback_data="tz|Баку")],
        [InlineKeyboardButton("Москва (UTC+3)", callback_data="tz|Москва"),
         InlineKeyboardButton("Киев (UTC+2)", callback_data="tz|Киев")],
        [InlineKeyboardButton("Центральная Европа (UTC+1)", callback_data="tz|Центральная Европа"),
         InlineKeyboardButton("Лондон (UTC+0)", callback_data="tz|Лондон")],
        [InlineKeyboardButton("Другое…", callback_data="tz_more")]
    ]
    return InlineKeyboardMarkup(buttons)

def build_tz_more_keyboard():
    buttons=[
        [InlineKeyboardButton("Бишкек (UTC+6)", callback_data="tz|Бишкек"),
         InlineKeyboardButton("Новосибирск (UTC+7)", callback_data="tz|Новосибирск")],
        [InlineKeyboardButton("Владивосток (UTC+10)", callback_data="tz|Владивосток")],
        [InlineKeyboardButton("Назад", callback_data="tz_back")]
    ]
    return InlineKeyboardMarkup(buttons)

async def tz_inline_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query
    if not q: return
    data=q.data
    if data=="tz_more":
        await q.edit_message_reply_markup(reply_markup=build_tz_more_keyboard()); await q.answer(); return
    if data=="tz_back":
        await q.edit_message_reply_markup(reply_markup=build_tz_main_keyboard()); await q.answer(); return
    if data.startswith("tz|"):
        rus=data.split("|",1)[1]
        info=resolve_tz_arg(rus)
        if not info:
            await q.answer("Не удалось"); return
        rn,off,zone=info
        set_user_tz(q.from_user.id,rn,off,zone)
        await q.answer("Установлено")
        await q.edit_message_text(f"Таймзона: {rn} (UTC{off:+d})")

# ===== Commands =====
async def settz_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /tz <название | зона | смещение>")
        return
    arg=" ".join(context.args).strip()
    info=resolve_tz_arg(arg)
    if not info:
        await update.message.reply_text("Не удалось распознать таймзону.")
        return
    rn,off,zone=info
    set_user_tz(update.effective_user.id,rn,off,zone)
    await update.message.reply_text(f"Таймзона: {rn} (UTC{off:+d})")

async def deadline_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs=await get_bootstrap_cached()
    if not bs:
        await update.message.reply_text("Нет bootstrap"); return
    events=bs.get("events",[])
    now_utc=datetime.now(timezone.utc)
    future=[]
    for e in events:
        dt=parse_deadline(e.get("deadline_time"))
        if dt and dt>now_utc: future.append((dt,e))
    if not future:
        await update.message.reply_text("Нет будущих дедлайнов"); return
    dt,e=min(future,key=lambda x:x[0])
    user_id=update.effective_user.id
    tz_name, offset = get_user_tz(user_id)
    local_dt=dt+timedelta(hours=offset)
    now_local=datetime.utcnow()+timedelta(hours=offset)
    left=max(0,int((local_dt-now_local).total_seconds()))
    d=left//86400; h=(left%86400)//3600; m=(left%3600)//60
    gw=e.get("id")
    ts_line=user_timestamp_line(user_id,0)
    await update.message.reply_text(
        f"Дедлайн GW {gw}: {local_dt.strftime('%Y-%m-%d %H:%M:%S')} ({tz_name} UTC{offset:+d})\n"
        f"Осталось: {d}д {h}ч {m}м\n{ts_line}"
    )

async def rank_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    results=await get_league_results_cached(LEAGUE_ID)
    if not results:
        await update.message.reply_text("Standings недоступны"); return
    league_name=LEAGUE_NAME or "Zakynule Fantasy Vibe"
    rows=sorted(results,key=lambda r:r.get("rank",10**9))
    rank_w=max(len("R"), max(len(str(r.get("rank"))) for r in rows))
    player_w=max(len("Manager"), max(len(r.get("player_name","")) for r in rows))
    pts_w=max(len("Pts"), max(len(str(r.get("total"))) for r in rows))
    header=f"{'R'.ljust(rank_w)} {'Manager'.ljust(player_w)} {'Pts'.rjust(pts_w)}"
    user_id = update.effective_user.id if update.effective_user else None
    ts_line = user_timestamp_line(user_id, len(header))
    lines=[f"{league_name} Standings".center(len(header)), ts_line, "```", header]
    for r in rows:
        lines.append(f"{str(r.get('rank')).ljust(rank_w)} {r.get('player_name','').ljust(player_w)} {str(r.get('total')).rjust(pts_w)}")
    lines.append("```")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def players_pts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs=await get_bootstrap_cached()
    if not bs:
        await update.message.reply_text("Нет bootstrap"); return
    args=context.args
    if args and args[0].isdigit():
        gw=int(args[0])
    else:
        gw=get_current_or_next_gw(bs)
    if not isinstance(gw,int):
        await update.message.reply_text("Не удалось определить GW"); return
    await get_league_results_cached(LEAGUE_ID)
    live=await fetch_json(fpl_url(event_live_path_tpl.format(gw=gw)))
    if not live:
        await update.message.reply_text("live недоступен"); return
    league_players=await get_league_player_ids_cached(gw)
    pick_counts=await get_league_player_pick_counts(gw)
    live_elements=live.get("elements",[])
    rows=[]
    for el in live_elements:
        pid=el.get("id")
        if pid not in league_players: continue
        stats=el.get("stats",{}) or {}
        pos=PLAYER_POS_MAP.get(pid,0)
        total_points=int(stats.get("total_points",0) or 0)
        parts=[]
        g=int(stats.get("goals_scored",0) or 0)
        if g>0: parts.append(f"G{g}")
        a=int(stats.get("assists",0) or 0)
        if a>0: parts.append(f"A{a}")
        cs=int(stats.get("clean_sheets",0) or 0)
        if cs>0 and pos!=4: parts.append("CS")
        if dc_threshold_met(stats,pos): parts.append("DC")
        yc=int(stats.get("yellow_cards",0) or 0)
        if yc>0: parts.append(f"YC{yc}")
        rc=int(stats.get("red_cards",0) or 0)
        if rc>0: parts.append("RC")
        if pos==1:
            saves=int(stats.get("saves",0) or 0)
            gks=saves//3
            if gks>0: parts.append(f"GKS{gks}")
        og=int(stats.get("own_goals",0) or 0)
        if og>0: parts.append("OG")
        pm=int(stats.get("penalties_missed",0) or 0)
        if pm>0: parts.append("PM")
        ps=int(stats.get("penalties_saved",0) or 0)
        if ps>0: parts.append("PS")
        bonus=int(stats.get("bonus",0) or 0)
        if bonus>0: parts.append(f"B{bonus}")
        if not parts and total_points<=2:
            continue
        name=PLAYER_NAME_MAP.get(pid,f"P{pid}")
        rows.append({
            "name":name,
            "stats":" ".join(parts) if parts else "-",
            "pts":total_points,
            "own":pick_counts.get(pid,0)
        })
    if not rows:
        await update.message.reply_text("Нет игроков для отображения.")
        return
    rows.sort(key=lambda r:(-r["pts"], -r["own"], r["name"].lower()))
    name_w=max(len("Player"), max(len(r["name"]) for r in rows))
    stats_w=max(len("Stats"), max(len(r["stats"]) for r in rows))
    pts_w=max(len("Pts"), max(len(str(r["pts"])) for r in rows))
    own_w=max(len("Own"), max(len(str(r["own"])) for r in rows))
    header=f"{'Player'.ljust(name_w)} {'Stats'.ljust(stats_w)} {'Pts'.rjust(pts_w)} {'Own'.rjust(own_w)}"
    league_name=LEAGUE_NAME or "Zakynule Fantasy Vibe"
    legend=("G=Goals A=Assists CS=Clean sheet DC=Def contrib YC=Yellow RC=Red "
            "GKS=GK save pts OG=Own goal PM=Pen miss PS=Pen saved B=Bonus")
    user_id = update.effective_user.id if update.effective_user else None
    ts_line = user_timestamp_line(user_id, len(header))
    lines=[f"{league_name} — GW {gw}".center(len(header)), ts_line, "```", header]
    for r in rows:
        lines.append(f"{r['name'].ljust(name_w)} {r['stats'].ljust(stats_w)} {str(r['pts']).rjust(pts_w)} {str(r['own']).rjust(own_w)}")
    lines.append("```")
    lines.append(legend)
    for chunk in split_message_chunks("\n".join(lines)):
        try:
            await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception:
            await update.message.reply_text(chunk.replace("```",""))

async def gwpoints_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs=await get_bootstrap_cached()
    if not bs:
        await update.message.reply_text("Нет bootstrap"); return
    args=context.args
    if args and args[0].isdigit():
        gw=int(args[0])
    else:
        gw=get_current_or_next_gw(bs)
    if not isinstance(gw,int):
        await update.message.reply_text("Не удалось определить GW"); return
    results=await get_league_results_cached(LEAGUE_ID)
    if not results:
        await update.message.reply_text("Standings недоступны"); return
    league_name=LEAGUE_NAME or "Zakynule Fantasy Vibe"
    async def one(r):
        eid=r.get("entry")
        data=await get_entry_picks_cached(eid,gw)
        pts=data.get("entry_history",{}).get("points") if data else None
        return (eid, r.get("player_name"), pts if isinstance(pts,int) else -1, ENTRY_TO_OVERALL.get(eid,0))
    got=await gather_limited([one(r) for r in results if isinstance(r.get("entry"),int)], PICKS_CONCURRENCY)
    entries=[e for e in got if isinstance(e,tuple)]
    entries.sort(key=lambda x:(-x[2], -x[3], x[1].lower()))
    name_col=[manager_tag_local_plain(i+1,e[0]) for i,e in enumerate(entries)]
    name_w=max(len("Player"), max(len(n) for n in name_col))
    pts_w=max(len("Pts"), max(len(str(e[2])) for e in entries))
    header=f"{'Player'.ljust(name_w)} {'Pts'.rjust(pts_w)}"
    user_id = update.effective_user.id if update.effective_user else None
    ts_line = user_timestamp_line(user_id, len(header))
    lines=[f"{league_name} — GW {gw}".center(len(header)), ts_line, "```", header]
    for i,e in enumerate(entries):
        val=e[2] if e[2]>=0 else "n/a"
        lines.append(f"{name_col[i].ljust(name_w)} {str(val).rjust(pts_w)}")
    lines.append("```")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

RUS_MONTH={
    1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",
    7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"
}
SEASON_MONTH_ORDER=[8,9,10,11,12,1,2,3,4,5]

def season_month_index(month_num:int)->Optional[int]:
    try: return SEASON_MONTH_ORDER.index(month_num)+1
    except ValueError: return None

def season_month_index_to_calendar(index:int, season_tag:Optional[str])->Tuple[int,int]:
    if index<1 or index>10: raise ValueError("month index")
    month=SEASON_MONTH_ORDER[index-1]
    if season_tag and "/" in season_tag:
        try: start_year=int(season_tag.split("/")[0])
        except Exception: start_year=datetime.now().year
    else:
        start_year=datetime.now().year
    year=start_year if month>=8 else start_year+1
    return month,year

async def month_points_for(entries: List[Dict], events: List[Dict], target_month:int, target_year:int) -> Tuple[List[Tuple[int,str,int]], int,int]:
    month_events=[]
    for ev in events:
        dt=parse_deadline(ev.get("deadline_time"))
        if dt and dt.month==target_month and dt.year==target_year:
            month_events.append(ev.get("id"))
    if not month_events: return [],0,0
    finished=[ev.get("id") for ev in events if ev.get("finished") and ev.get("id") in month_events]
    played=len(finished)
    remaining=len(month_events)-played
    async def fetch(eid:int,mgr:str):
        total=0
        for gw_id in finished:
            data=await get_entry_picks_cached(eid,gw_id)
            if data:
                pts=data.get("entry_history",{}).get("points")
                if isinstance(pts,int): total+=pts
        return (eid,mgr,total)
    got=await gather_limited([fetch(r.get("entry"), r.get("player_name")) for r in entries if isinstance(r.get("entry"),int)], PICKS_CONCURRENCY)
    rows=[x for x in got if isinstance(x,tuple)]
    rows.sort(key=lambda x:(-x[2], ENTRY_TO_RANK.get(x[0],10**9), x[1].lower()))
    return rows, played, remaining

async def month_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs=await get_bootstrap_cached()
    if not bs:
        await update.message.reply_text("Нет bootstrap"); return
    events=bs.get("events",[])
    results=await get_league_results_cached(LEAGUE_ID)
    if not results:
        await update.message.reply_text("Standings недоступны"); return
    if context.args and context.args[0].isdigit():
        idx=int(context.args[0])
        if idx<1 or idx>10:
            await update.message.reply_text("Месяц: 1..10 (Август..Май)")
            return
        selected_index=idx
    else:
        now=datetime.now(timezone.utc)
        selected_index=season_month_index(now.month) or 1
    season=discover_season_tag(bs)
    cal_month,cal_year=season_month_index_to_calendar(selected_index,season)
    rows,played,remaining=await month_points_for(results, events, cal_month, cal_year)
    month_name=RUS_MONTH.get(cal_month,f"Месяц {cal_month}")
    league_name=LEAGUE_NAME or "Zakynule Fantasy Vibe"
    top=rows[:15]
    display=[f"{i+1} {manager_name(eid)}" for i,(eid,mgr,pts) in enumerate(top)]
    name_w=max(len("Player"), max((len(s) for s in display), default=6))
    pts_w=max(len("Pts"), max((len(str(pts)) for (_eid,_mgr,pts) in top), default=3))
    header=f"{'Player'.ljust(name_w)} {'Pts'.rjust(pts_w)}"
    info=f"Сыграно туров: {played}, осталось: {remaining}"
    user_id = update.effective_user.id if update.effective_user else None
    ts_line = user_timestamp_line(user_id, len(header))
    lines=[f"{league_name} — {month_name}".center(len(header)), ts_line, info.center(len(header)), "```", header]
    for i,(eid,mgr,pts) in enumerate(top):
        lines.append(f"{display[i].ljust(name_w)} {str(pts).rjust(pts_w)}")
    lines.append("```")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ===== LIVE MONITOR LOOP (detailed) =====
async def live_monitor_loop():
    global _current_gw,last_live_tick_time,last_active_fixtures_count
    logger.info("Live loop started")
    while not stop_event.is_set():
        try:
            bs=await get_bootstrap_cached()
            if not bs:
                await asyncio.sleep(LIVE_POLL_INTERVAL); continue
            season=discover_season_tag(bs)
            last_live_tick_time=time.time()
            await measure_redis_latency()
            for e in bs.get("events",[]):
                if e.get("finished") and isinstance(e.get("id"),int):
                    await process_squid_after_gw_finish(e.get("id"))
            current_ev=next((e for e in bs.get("events",[]) if e.get("is_current")),None)
            if not current_ev or current_ev.get("finished"):
                await asyncio.sleep(LIVE_POLL_INTERVAL); continue
            gw=current_ev.get("id")
            if not isinstance(gw,int):
                await asyncio.sleep(LIVE_POLL_INTERVAL); continue
            if _current_gw is None or _current_gw!=gw:
                _second_yellow_processed.clear()
                _red_card_processed.clear()
                _dc_awarded.clear()
                _dc_removed_sent.clear()
                _cs_subbed_sent.clear()
                _fixture_summary_sent.clear()
                _last_counts.clear()
                _current_gw=gw
                logger.info(f"New current GW {gw}")
            fixtures=await get_fixtures_cached(gw)
            active_fids=get_active_fixture_ids(fixtures)
            last_active_fixtures_count=len(active_fids)
            if not active_fids:
                await asyncio.sleep(min(300,LIVE_POLL_INTERVAL*2))
                continue
            live=await fetch_json(fpl_url(event_live_path_tpl.format(gw=gw)))
            if not live:
                await asyncio.sleep(LIVE_POLL_INTERVAL); continue
            live_elements=live.get("elements",[])
            live_by_id={el.get("id"):el for el in live_elements if isinstance(el.get("id"),int)}
            league_players=await get_league_player_ids_cached(gw)
            fixture_index={fx.get("id"):fx for fx in fixtures if isinstance(fx.get("id"),int)}

            pos_pool={}
            neg_pool={}
            player_state={}
            fixture_max_minute={}
            any_delta=False

            for pid,el in live_by_id.items():
                if pid not in league_players: continue
                pos=PLAYER_POS_MAP.get(pid,0)
                stats_overall=el.get("stats",{}) or {}
                total_nb=total_no_bonus(stats_overall)
                explain=el.get("explain",[]) or []
                minutes=int(stats_overall.get("minutes",0) or 0)
                for blk in explain:
                    fid=blk.get("fixture")
                    if not isinstance(fid,int) or fid not in active_fids: continue
                    player_state[(fid,pid)]=(pos,minutes,total_nb)
                    if minutes>fixture_max_minute.get(fid,0):
                        fixture_max_minute[fid]=minutes
                    for st in blk.get("stats",[]):
                        ident=st.get("identifier"); value=st.get("value")
                        if not isinstance(value,int): continue
                        key=(gw,fid,pid,ident)
                        prev=_last_counts.get(key,0)
                        delta=value-prev
                        if delta!=0:
                            any_delta=True
                            tgt=pos_pool if delta>0 else neg_pool
                            tgt.setdefault(fid,{}).setdefault(ident,[])
                            tgt[fid][ident].extend([pid]*abs(delta))
                        _last_counts[key]=value

            if not any_delta:
                await asyncio.sleep(LIVE_POLL_INTERVAL); continue

            for fid in sorted(set(list(pos_pool.keys())+list(neg_pool.keys()))):
                fx=fixture_index.get(fid,{})
                if not fx: continue
                base_h=fx.get("team_h_score") or 0
                base_a=fx.get("team_a_score") or 0
                team_h=fx.get("team_h")
                team_a=fx.get("team_a")
                local_h=base_h
                local_a=base_a

                def header_line() -> str:
                    return f"{TEAM_SHORT_MAP.get(team_h,'T?')} {local_h}-{local_a} {TEAM_SHORT_MAP.get(team_a,'T?')}"

                def get_state(pid:int) -> Tuple[int,int,int]:
                    return player_state.get((fid,pid),(PLAYER_POS_MAP.get(pid,0),0,0))

                update_msgs=[]

                # Assist reassignment
                if "assists" in neg_pool.get(fid,{}) and "assists" in pos_pool.get(fid,{}):
                    while neg_pool[fid]["assists"] and pos_pool[fid]["assists"]:
                        from_pid=neg_pool[fid]["assists"].pop(0)
                        to_pid=pos_pool[fid]["assists"].pop(0)
                        pos_from, minutes_from, total_from = get_state(from_pid)
                        pos_to, _m_to, total_to = get_state(to_pid)
                        pts=event_points_for(pos_from,"Assist")
                        pts_to=event_points_for(pos_to,"Assist")
                        mstr=f"{minutes_from}'"
                        msg=(f"{header_line()}\n"
                             f"{mstr} Update: Assist reassigned - {PLAYER_NAME_MAP.get(from_pid,f'P{from_pid}')} {format_points(-pts)}, total ({total_from}).\n"
                             f"Assist - {PLAYER_NAME_MAP.get(to_pid,f'P{to_pid}')} {format_points(pts_to)}, total ({total_to}).")
                        update_msgs.append(msg)

                # Penalty retake undo → goal
                while neg_pool.get(fid,{}).get("penalties_missed") and neg_pool.get(fid,{}).get("penalties_saved") and pos_pool.get(fid,{}).get("goals_scored"):
                    m_pid=neg_pool[fid]["penalties_missed"].pop(0)
                    s_pid=neg_pool[fid]["penalties_saved"].pop(0)
                    g_pid=pos_pool[fid]["goals_scored"].pop(0)
                    pos_m, minutes_m, total_m = get_state(m_pid)
                    pos_s, _m_s, total_s = get_state(s_pid)
                    pos_g, _m_g, total_g = get_state(g_pid)
                    pts_m=event_points_for(pos_m,"Penalty missed")
                    pts_s=event_points_for(pos_s,"Penalty saved")
                    pts_g=event_points_for(pos_g,"Goal")
                    scorer_team=PLAYER_TEAM_MAP.get(g_pid)
                    if scorer_team==team_h: local_h+=1
                    elif scorer_team==team_a: local_a+=1
                    mstr=f"{minutes_m}'"
                    msg=(f"{header_line()}\n"
                         f"{mstr} Update: Penalty saved removed - {PLAYER_NAME_MAP.get(s_pid,f'P{s_pid}')} {format_points(-pts_s)}, total ({total_s}).\n"
                         f"Penalty missed removed - {PLAYER_NAME_MAP.get(m_pid,f'P{m_pid}')} {format_points(-pts_m)}, total ({total_m}).\n"
                         f"Goal - {PLAYER_NAME_MAP.get(g_pid,f'P{g_pid}')} {format_points(pts_g)}, total ({total_g}).")
                    update_msgs.append(msg)

                # Generic cancellations
                for ident,label,base_event in [
                    ("goals_scored","Update: Goal cancelled (VAR)","Goal"),
                    ("assists","Update: Assist cancelled","Assist"),
                    ("own_goals","Update: Own goal removed","Own goal"),
                    ("penalties_missed","Update: Penalty missed removed","Penalty missed"),
                    ("penalties_saved","Update: Penalty saved removed","Penalty saved"),
                    ("red_cards","Update: Red card cancelled","Red card"),
                    ("clean_sheets","Update: Clean sheet removed","Clean sheet"),
                ]:
                    for pid in list(neg_pool.get(fid,{}).get(ident,[])):
                        pos_p, minutes_p, total_p = get_state(pid)
                        pts=event_points_for(pos_p,base_event)
                        mstr=f"{minutes_p}'"
                        msg=(f"{header_line()}\n"
                             f"{mstr} {label} - {PLAYER_NAME_MAP.get(pid,f'P{pid}')} {format_points(-pts)}, total ({total_p}).")
                        update_msgs.append(msg)
                        neg_pool[fid][ident].remove(pid)

                # DC removed
                for pid in list(PLAYER_TEAM_MAP.keys()):
                    if (fid,pid) in player_state and ns_key(gw,fid,pid) in _dc_awarded:
                        pos_d, minutes_d, total_d = get_state(pid)
                        el=live_by_id.get(pid,{})
                        stats_overall=el.get("stats",{}) or {}
                        if not dc_threshold_met(stats_overall,pos_d) and ns_key(gw,fid,pid) not in _dc_removed_sent:
                            _dc_removed_sent.add(ns_key(gw,fid,pid))
                            pts=event_points_for(pos_d,"DC")
                            mstr=f"{minutes_d}'"
                            msg=(f"{header_line()}\n"
                                 f"{mstr} Update: DC removed - {PLAYER_NAME_MAP.get(pid,f'P{pid}')} {format_points(-pts)}, total ({total_d}).")
                            update_msgs.append(msg)

                for um in update_msgs:
                    await send_once(um, season, gw)

                # Positives
                pos_events=pos_pool.get(fid,{})

                # Goals (+ optional assist)
                goals=list(pos_events.get("goals_scored",[]))
                assists=list(pos_events.get("assists",[]))
                while goals:
                    gid=goals.pop(0)
                    pos_g, minutes_g, total_g = get_state(gid)
                    pts_g=event_points_for(pos_g,"Goal")
                    scorer_team=PLAYER_TEAM_MAP.get(gid)
                    if scorer_team==team_h: local_h+=1
                    elif scorer_team==team_a: local_a+=1
                    a_pid=None
                    for i,aid in enumerate(assists):
                        if aid!=gid and PLAYER_TEAM_MAP.get(aid)==scorer_team:
                            a_pid=aid; assists.pop(i); break
                    header=header_line()
                    mstr=f"{minutes_g}'"
                    line1=f"{mstr} Goal - {PLAYER_NAME_MAP.get(gid,f'P{gid}')} {format_points(pts_g)}, total ({total_g})."
                    if a_pid:
                        pos_a,_m_a,total_a=get_state(a_pid)
                        pts_a=event_points_for(pos_a,"Assist")
                        line2=f"Assist - {PLAYER_NAME_MAP.get(a_pid,f'P{a_pid}')} {format_points(pts_a)}, total ({total_a})."
                        msg=f"{header}\n{line1}\n{line2}"
                    else:
                        msg=f"{header}\n{line1}"
                    await send_once(msg, season, gw)

                # Own goals
                own_goals=list(pos_events.get("own_goals",[]))
                while own_goals:
                    ogid=own_goals.pop(0)
                    pos_og, minutes_og, total_og = get_state(ogid)
                    pts_og=event_points_for(pos_og,"Own goal")
                    og_team=PLAYER_TEAM_MAP.get(ogid)
                    if og_team==team_h: local_a+=1
                    elif og_team==team_a: local_h+=1
                    a_pid=None
                    for i,aid in enumerate(assists):
                        if PLAYER_TEAM_MAP.get(aid)!=og_team:
                            a_pid=aid; assists.pop(i); break
                    header=header_line()
                    mstr=f"{minutes_og}'"
                    line1=f"{mstr} Own goal - {PLAYER_NAME_MAP.get(ogid,f'P{ogid}')} {format_points(pts_og)}, total ({total_og})."
                    if a_pid:
                        pos_a,_m_a,total_a=get_state(a_pid)
                        pts_a=event_points_for(pos_a,"Assist")
                        line2=f"Assist - {PLAYER_NAME_MAP.get(a_pid,f'P{a_pid}')} {format_points(pts_a)}, total ({total_a})."
                        msg=f"{header}\n{line1}\n{line2}"
                    else:
                        msg=f"{header}\n{line1}"
                    await send_once(msg, season, gw)

                # Pens missed/saved
                pens_missed=list(pos_events.get("penalties_missed",[]))
                pens_saved=list(pos_events.get("penalties_saved",[]))
                while pens_missed and pens_saved:
                    pm=pens_missed.pop(0); ps=pens_saved.pop(0)
                    pos_m, minutes_m, total_m = get_state(pm)
                    pos_s,_m_s,total_s=get_state(ps)
                    pts_m=event_points_for(pos_m,"Penalty missed")
                    pts_s=event_points_for(pos_s,"Penalty saved")
                    header=header_line(); mstr=f"{minutes_m}'"
                    line1=f"{mstr} Penalty missed - {PLAYER_NAME_MAP.get(pm,f'P{pm}')} {format_points(pts_m)}, total ({total_m})."
                    line2=f"{mstr} Penalty saved - {PLAYER_NAME_MAP.get(ps,f'P{ps}')} {format_points(pts_s)}, total ({total_s})."
                    await send_once(f"{header}\n{line1}\n{line2}", season, gw)
                for pm in pens_missed:
                    pos_m, minutes_m, total_m = get_state(pm)
                    pts_m=event_points_for(pos_m,"Penalty missed")
                    header=header_line(); mstr=f"{minutes_m}'"
                    await send_once(f"{header}\n{mstr} Penalty missed - {PLAYER_NAME_MAP.get(pm,f'P{pm}')} {format_points(pts_m)}, total ({total_m}).", season, gw)
                for ps in pens_saved:
                    pos_s, minutes_s, total_s = get_state(ps)
                    pts_s=event_points_for(pos_s,"Penalty saved")
                    header=header_line(); mstr=f"{minutes_s}'"
                    await send_once(f"{header}\n{mstr} Penalty saved - {PLAYER_NAME_MAP.get(ps,f'P{ps}')} {format_points(pts_s)}, total ({total_s}).", season, gw)

                # Cards
                yellow_totals={}
                for (kgw,kfid,kpid,ident), val in _last_counts.items():
                    if kgw==gw and kfid==fid and ident=="yellow_cards":
                        yellow_totals[kpid]=val
                reds=list(pos_events.get("red_cards",[]))
                for pid_r in reds:
                    pos_r, minutes_r, total_r = get_state(pid_r)
                    key=ns_key(gw,fid,pid_r)
                    yc_total=yellow_totals.get(pid_r,0)
                    header=header_line(); mstr=f"{minutes_r}'"
                    if yc_total>=2:
                        if key not in _second_yellow_processed:
                            _second_yellow_processed.add(key)
                            pts=event_points_for(pos_r,"Second yellow → red")
                            line=f"{mstr} Second yellow → red - {PLAYER_NAME_MAP.get(pid_r,f'P{pid_r}')} {format_points(pts)}, total ({total_r})."
                            await send_once(f"{header}\n{line}", season, gw)
                    else:
                        if key not in _red_card_processed:
                            _red_card_processed.add(key)
                            pts=event_points_for(pos_r,"Red card")
                            line=f"{mstr} Red card - {PLAYER_NAME_MAP.get(pid_r,f'P{pid_r}')} {format_points(pts)}, total ({total_r})."
                            await send_once(f"{header}\n{line}", season, gw)

                # Clean sheet (subbed off)
                for (pfid,pid),(pos_c, minutes_c, total_c) in list(player_state.items()):
                    if pfid!=fid: continue
                    if pos_c==4: continue
                    if fx.get("finished"): continue
                    if minutes_c<60: continue
                    team_id=PLAYER_TEAM_MAP.get(pid)
                    if not team_id: continue
                    conceded=0
                    if team_id==team_h: conceded=fx.get("team_a_score") or 0
                    elif team_id==team_a: conceded=fx.get("team_h_score") or 0
                    if conceded!=0: continue
                    cs_total=_last_counts.get((gw,fid,pid,"clean_sheets"),0)
                    if cs_total<=0:
                        stats_overall=live_by_id.get(pid,{}).get("stats",{}) or {}
                        cs_total=int(stats_overall.get("clean_sheets",0) or 0)
                        _last_counts[(gw,fid,pid,"clean_sheets")]=cs_total
                    if cs_total<=0: continue
                    maxm=fixture_max_minute.get(fid, minutes_c)
                    if minutes_c>=maxm: continue
                    key=ns_key(gw,fid,pid)
                    if key in _cs_subbed_sent: continue
                    pts=event_points_for(pos_c,"Clean sheet")
                    if pts<=0: continue
                    _cs_subbed_sent.add(key)
                    header=header_line(); mstr=f"{minutes_c}'"
                    line=f"{mstr} Clean sheet (subbed off) - {PLAYER_NAME_MAP.get(pid,f'P{pid}')} {format_points(pts)}, total ({total_c})."
                    await send_once(f"{header}\n{line}", season, gw)

                # DC award
                for (pfid,pid),(pos_d, minutes_d, total_d) in list(player_state.items()):
                    if pfid!=fid: continue
                    key=ns_key(gw,fid,pid)
                    if key in _dc_awarded: continue
                    el=live_by_id.get(pid,{})
                    stats_overall=el.get("stats",{}) or {}
                    if dc_threshold_met(stats_overall,pos_d):
                        _dc_awarded.add(key)
                        pts=event_points_for(pos_d,"DC")
                        header=header_line(); mstr=f"{minutes_d}'"
                        line=f"{mstr} DC - {PLAYER_NAME_MAP.get(pid,f'P{pid}')} {format_points(pts)}, total ({total_d})."
                        await send_once(f"{header}\n{line}", season, gw)

            # Post-match summaries
            finished=[fx for fx in fixtures if fx.get("finished")]
            for fx in finished:
                fid=fx.get("id")
                if not isinstance(fid,int): continue
                sum_key=f"{gw}:{fid}"
                if sum_key in _fixture_summary_sent: continue
                team_h=fx.get("team_h"); team_a=fx.get("team_a")
                hs=fx.get("team_h_score") or 0
                as_=fx.get("team_a_score") or 0
                header=f"{TEAM_SHORT_MAP.get(team_h,'T?')} {hs}-{as_} {TEAM_SHORT_MAP.get(team_a,'T?')} — Final player points with bonus"
                rows=[]
                for pid,el in live_by_id.items():
                    if pid not in league_players: continue
                    if any(b.get("fixture")==fid for b in (el.get("explain") or [])):
                        stats=el.get("stats",{}) or {}
                        total_pts=int(stats.get("total_points",0) or 0)
                        rows.append((PLAYER_NAME_MAP.get(pid,f'P{pid}'), total_pts))
                if rows:
                    rows.sort(key=lambda x:(-x[1], x[0].lower()))
                    width=max(len(header), max((len(f"{i}. {nm}: {pts} pts") for i,(nm,pts) in enumerate(rows,1)), default=0))
                    lines=[header.center(width)]
                    for i,(name,pts) in enumerate(rows,1):
                        lines.append(f"{i}. {name}: {pts} pts".center(width))
                    await send_once("\n".join(lines), season, gw)
                _fixture_summary_sent.add(sum_key)

            await asyncio.sleep(LIVE_POLL_INTERVAL)
        except Exception as ex:
            logger.exception(f"live_monitor_loop error: {ex}")
            await asyncio.sleep(LIVE_POLL_INTERVAL)
    logger.info("Live loop stopped")

# ===== Setup bot =====
async def setup_bot_commands(bot):
    cmds=[
        BotCommand("tz","Set timezone"),
        BotCommand("deadline","Next deadline"),
        BotCommand("rank","League standings"),
        BotCommand("players_pts","Live players points"),
        BotCommand("gwpoints","Points per GW"),
        BotCommand("month","Points per month"),
        BotCommand("squid_start","Start Squid Game"),
        BotCommand("squid_rules","Squid rules"),
        BotCommand("squid_status","Squid status"),
        BotCommand("squid_winners","Squid winners"),
        BotCommand("prices","Price predictions"),
    ]
    try:
        await bot.set_my_commands(cmds)
    except Exception:
        logger.exception("set_my_commands failed")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled", exc_info=context.error)

# ===== Run =====
async def run_bot():
    global http_client, application
    init_redis()
    limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
    use_http2=ENABLE_HTTP2
    try:
        import h2  # noqa
    except Exception:
        use_http2=False
    http_client=httpx.AsyncClient(http2=use_http2, limits=limits, timeout=20.0)

    application=Application.builder().token(BOT_TOKEN).concurrent_updates(TELEGRAM_CONCURRENCY).build()
    application.add_handler(CommandHandler("tz", settz_command))
    application.add_handler(CommandHandler("deadline", deadline_command))
    application.add_handler(CommandHandler("rank", rank_command))
    application.add_handler(CommandHandler("players_pts", players_pts_command))
    application.add_handler(CommandHandler("gwpoints", gwpoints_command))
    application.add_handler(CommandHandler("month", month_command))
    application.add_handler(CommandHandler("squid_start", squid_start_command))
    application.add_handler(CommandHandler("squid_rules", squid_rules_command))
    application.add_handler(CommandHandler("squid_status", squid_status_command))
    application.add_handler(CommandHandler("squid_winners", squid_winners_command))
    application.add_handler(CommandHandler("prices", prices_command))
    application.add_handler(CallbackQueryHandler(tz_inline_callback, pattern="^tz"))
    application.add_error_handler(error_handler)

    await application.initialize()
    await application.start()
    await setup_bot_commands(application.bot)

    bs=await get_bootstrap_cached()
    if not bs:
        logger.warning("Bootstrap not loaded initially")

    load_squid_from_redis()

    if USE_WEBHOOK:
        logger.info("Webhook mode not implemented; using polling.")
    else:
        try:
            await application.updater.start_polling()
        except Exception:
            logger.exception("start_polling failed")
            return

    asyncio.create_task(live_monitor_loop())

    async def daily_prices_poster():
        while not stop_event.is_set():
            try:
                tz_offset=5
                now_utc=datetime.now(timezone.utc)
                now_local=now_utc+timedelta(hours=tz_offset)
                target_local=now_local.replace(hour=23,minute=0,second=0,microsecond=0)
                if now_local>=target_local:
                    target_local+=timedelta(days=1)
                sleep_sec=(target_local-now_local).total_seconds()
                await asyncio.sleep(sleep_sec)
                bs_local=await get_bootstrap_cached()
                if bs_local:
                    data=await get_prices_data()
                    if data.get("ok"):
                        parsed=data["parsed"]
                        lines=["Price Predictions (>=1%)", user_timestamp_line(None,0)]
                        for side in ("Rises","Falls"):
                            block=parsed.get(side,{})
                            lines.append(f"{side}:")
                            if not block:
                                lines.append("  (none)")
                            else:
                                for cat,items in block.items():
                                    lines.append(f"  {cat}:")
                                    for nm,pr in items:
                                        lines.append(f"    - {nm}: {pr}")
                        text="\n".join(lines)
                    else:
                        text="Price data unavailable"
                    if TARGET_CHAT_ID and TARGET_CHAT_ID.isdigit() and application:
                        await send_text_raw(int(TARGET_CHAT_ID), text)
            except Exception:
                logger.exception("daily prices poster failed")
                await asyncio.sleep(120)
    asyncio.create_task(daily_prices_poster())

    try:
        me=await application.bot.get_me()
        logger.info(f"Bot @{getattr(me,'username','?')} started id={getattr(me,'id','?')}")
    except Exception:
        logger.exception("get_me failed")

    await stop_event.wait()

    try:
        if not USE_WEBHOOK:
            await application.updater.stop()
        await application.stop()
        await application.shutdown()
    except Exception:
        logger.exception("Telegram shutdown error")
    try:
        if http_client:
            await http_client.aclose()
    except Exception:
        logger.exception("HTTP client close error")
    logger.info("Bot stopped")

def handle_sigterm(signum, frame):
    try:
        loop=asyncio.get_event_loop()
        loop.call_soon_threadsafe(stop_event.set)
    except RuntimeError:
        pass

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

if __name__=="__main__":
    flask_thread=threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()
    try:
        asyncio.run(run_bot())
    except Exception:
        logger.exception("App crashed")
