# FPL Telegram Bot — live events, per-user timezone, Squid Game, price predictions
# Updates in this revision:
# - Implemented requested optimization items (2–15, 17, 21–28, 30 from prior list).
#   * Signature + text cache for rank and players_pts (/players_pts cached per GW + last FPL update).
#   * Lazy bootstrap option (can be toggled if needed; default still eager).
#   * Reduced attempts/timeouts for high–frequency live endpoints.
#   * Cleanup of old picks & last_counts data to limit memory growth.
#   * Better error visibility (warnings instead of silent pass for selected code paths).
#   * Alignment fixes for Stats column: padding ensures Pts column stays vertically aligned even with multi‑letter tokens.
#   * Counts for PM and PS always shown (PM1 / PS1 instead of just PM / PS).
#   * Bonus always shown with count (B1..B3).
#   * Last Squid final report cached in memory + Redis; /squid_status returns it (variant 1).
# - DC logic unchanged (never for GK).
# - Silent replies for commands; auto notifications with sound.
# NOTE: Modularization (point 3) intentionally skipped per earlier instruction.

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
from functools import wraps
from typing import Any, Dict, Optional, List, Tuple, Set, Iterable, Awaitable, Callable
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

OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "Iskanchik")
OWNER_USER_ID_ENV = os.environ.get("OWNER_USER_ID")
OWNER_USER_ID: Optional[int] = int(OWNER_USER_ID_ENV) if OWNER_USER_ID_ENV and OWNER_USER_ID_ENV.isdigit() else None
ALLOWED_GROUP_ID = int(os.environ.get("ALLOWED_GROUP_ID", "4973694653")

def is_owner(update: Update) -> bool:
    u = update.effective_user
    if not u: return False
    if OWNER_USER_ID is not None and u.id == OWNER_USER_ID: return True
    if u.username and u.username.lower() == OWNER_USERNAME.lower(): return True
    return False

_group_membership_cache: Dict[int, Tuple[float, bool]] = {}
GROUP_MEMBERSHIP_TTL = 600

async def is_user_in_group(user_id: int) -> bool:
    now = time.time()
    cached = _group_membership_cache.get(user_id)
    if cached and (now - cached[0]) < GROUP_MEMBERSHIP_TTL:
        return cached[1]
    if not application:
        _group_membership_cache[user_id] = (now, False); return False
    try:
        member = await application.bot.get_chat_member(ALLOWED_GROUP_ID, user_id)
        ok = member.status not in ("left", "kicked")
        _group_membership_cache[user_id] = (now, ok)
        return ok
    except Exception:
        _group_membership_cache[user_id] = (now, False)
        return False

async def is_authorized(update: Update) -> bool:
    if is_owner(update): return True
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user: return False
    if chat.id == ALLOWED_GROUP_ID: return True
    if chat.type == "private": return await is_user_in_group(user.id)
    return False

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN required")

TARGET_CHAT_ID = str(ALLOWED_GROUP_ID)
LEAGUE_ID = os.environ.get("LEAGUE_ID", "980121")

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

def fpl_url(path: str) -> str:
    base = FPL_PROXY_BASE or "https://fantasy.premierleague.com"
    return f"{base}{path}"

FPL_BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://fantasy.premierleague.com/",
    "Origin": "https://fantasy.premierleague.com",
}

http_client: Optional[httpx.AsyncClient] = None
application: Optional[Application] = None
fpl_semaphore = asyncio.Semaphore(FPL_CONCURRENCY)
picks_semaphore = asyncio.Semaphore(PICKS_CONCURRENCY)

async def fetch_json(url: str, timeout: float = 15.0, attempts: int = 3,
                     extra_headers: Optional[Dict[str,str]]=None, return_response: bool=False):
    headers = dict(FPL_BASE_HEADERS)
    if extra_headers: headers.update(extra_headers)
    for attempt in range(1, attempts+1):
        await asyncio.sleep(random.uniform(0.02,0.06))
        try:
            async with fpl_semaphore:
                resp = await http_client.get(url, headers=headers, timeout=timeout)
            if return_response: return resp
            if resp.status_code == 200:
                try: return resp.json()
                except Exception: return None
            if resp.status_code in (403,429) or 500 <= resp.status_code < 600:
                if attempt < attempts:
                    await asyncio.sleep(min(2**attempt, 8))
                    continue
            return None
        except Exception as e:
            if attempt < attempts:
                await asyncio.sleep(min(2**attempt, 8))
    return None

bootstrap_path="/api/bootstrap-static/"
league_path_tpl="/api/leagues-classic/{league_id}/standings/?page_standings={page}"
entry_picks_path_tpl="/api/entry/{entry_id}/event/{gw}/picks/"
event_live_path_tpl="/api/event/{gw}/live/"
fixtures_event_path_tpl="/api/fixtures/?event={gw}"
event_status_path="/api/event-status/"

bootstrap_cache: Dict[str,Any]={}
bootstrap_cache_ts: Optional[float]=None
_bootstrap_etag: Optional[str]=None
bootstrap_lock=asyncio.Lock()

standings_cache: Dict[str,Any]={}
standings_cache_ts: Optional[float]=None
standings_lock=asyncio.Lock()

_fixtures_cache: Dict[int,Tuple[float,List[Dict]]]={}
picks_cache: Dict[Tuple[int,int],Dict[str,Any]]={}
_picks_locks: Dict[Tuple[int,int],asyncio.Lock]={}
_picks_locks_guard=asyncio.Lock()

SEASON_TAG: Optional[str]=None
LEAGUE_NAME: Optional[str]=None

stop_event=asyncio.Event()
redis_client: Optional["Redis"]=None

PLAYER_NAME_MAP: Dict[int,str]={}
PLAYER_POS_MAP: Dict[int,int]={}
PLAYER_TEAM_MAP: Dict[int,int]={}
TEAM_SHORT_MAP: Dict[int,str]={}
NAME_TO_ELEMENT_ID: Dict[str,int]={}

_league_players_cache: Dict[int,Tuple[float,Set[int]]]={}

_second_yellow_processed:set=set()
_red_card_processed:set=set()
_dc_awarded:set=set()
_dc_removed_sent:set=set()
_cs_subbed_sent:set=set()
_fixture_summary_sent:set=set()

_last_counts: Dict[Tuple[int,int,int,str],int]={}
_current_gw: Optional[int]=None
_sent_msg_hashes:Set[str]=set()

_tz_offset_memory: Dict[int,Tuple[str,int]]={}

ENTRY_TO_MANAGER_NAME: Dict[int,str]={}
ENTRY_TO_RANK: Dict[int,int]={}
ENTRY_TO_OVERALL: Dict[int,int]={}

squid_active_state: Optional[Dict[str,Any]]=None
squid_winner_history: List[Dict[str,Any]]=[]
LAST_SQUID_REPORT_TEXT: Optional[str] = None

prices_cache_data: Optional[Dict[str,Any]]=None
prices_cache_ts: Optional[float]=None
PRICES_CACHE_TTL=600
PRICES_URL="https://www.livefpl.net/prices"

_last_fpl_update_dt: Optional[datetime]=None
_last_fpl_update_cache_ts: Optional[float]=None
LAST_FPL_UPDATE_TTL=60

# Text cache (feature key -> {sig, hash, text})
_text_cache_mem: Dict[str, Dict[str, Any]] = {}

def _cache_key(feature: str) -> str:
    return f"fpl:cache:text:{feature}"

def _ensure_str(x: Any) -> Optional[str]:
    if x is None: return None
    if isinstance(x,(bytes,bytearray)):
        try: return x.decode("utf-8","ignore")
        except Exception: return None
    return str(x)

def compute_text_hash(text: str) -> str:
    return hashlib.blake2s(text.encode("utf-8")).hexdigest()

def cache_get_text(feature: str) -> Optional[Dict[str,Any]]:
    if redis_client:
        try:
            raw = redis_client.get(_cache_key(feature))
            if raw:
                s=_ensure_str(raw)
                if s:
                    try: return json.loads(s)
                    except Exception: pass
        except Exception: pass
    return _text_cache_mem.get(feature)

def cache_set_text(feature: str, sig: str, text: str, ttl: int):
    rec={"sig":sig,"hash":compute_text_hash(text),"text":text,"ts":int(time.time())}
    if redis_client:
        try: redis_client.set(_cache_key(feature), json.dumps(rec), ex=ttl)
        except Exception: pass
    _text_cache_mem[feature]=rec

async def reply_silent(update: Update, text: str, parse_mode: Optional[str]=None):
    try:
        await update.message.reply_text(text, parse_mode=parse_mode, disable_notification=True)
    except Exception as e:
        logger.debug("Reply silent failed: %s", e)

def safe_command(fn:Callable):
    @wraps(fn)
    async def wrapper(update:Update, context:ContextTypes.DEFAULT_TYPE):
        if not await is_authorized(update): return
        try:
            await fn(update, context)
        except Exception as e:
            logger.warning("Command %s error: %s", fn.__name__, e)
    return wrapper

async def gather_limited(coros:Iterable[Awaitable], limit:int)->List[Any]:
    sem=asyncio.Semaphore(limit)
    async def run(c):
        async with sem: return await c
    return await asyncio.gather(*(run(x) for x in coros), return_exceptions=True)

def manager_name(entry:int)->str:
    return ENTRY_TO_MANAGER_NAME.get(entry,f"Entry {entry}")

def init_redis():
    global redis_client
    if Redis is None: return
    try:
        redis_client=Redis.from_env()
    except Exception as e:
        logger.warning("Redis init failed: %s", e)
        redis_client=None

async def measure_redis_latency():
    global last_redis_latency_ms
    if not redis_client:
        last_redis_latency_ms=None; return
    try:
        t0=time.time()
        redis_client.set("fpl:health:pulse","1",ex=30)
        _=redis_client.get("fpl:health:pulse")
        last_redis_latency_ms=round((time.time()-t0)*1000,2)
    except Exception:
        last_redis_latency_ms=None

def get_user_tz(user_id:int)->Tuple[str,int]:
    if redis_client:
        try:
            raw=redis_client.get(f"{TZ_REDIS_KEY_PREFIX}{user_id}")
            if raw:
                try:
                    obj=json.loads(_ensure_str(raw) or "")
                    return obj.get("rus","Астана"), int(obj.get("offset",5))
                except Exception:
                    parts=str(raw).split("|")
                    if len(parts)>=2 and parts[1].lstrip("+-").isdigit():
                        return parts[0], int(parts[1])
        except Exception: pass
    mem=_tz_offset_memory.get(user_id)
    if mem: return mem
    return DEFAULT_TZ_NAME, TZ_MAP[DEFAULT_TZ_NAME][1]

def set_user_tz(user_id:int, rus:str, offset:int, zone:str):
    _tz_offset_memory[user_id]=(rus,offset)
    if redis_client:
        try:
            redis_client.set(f"{TZ_REDIS_KEY_PREFIX}{user_id}",
                json.dumps({"rus":rus,"offset":offset,"zone":zone,"ts":int(time.time())}))
        except Exception: pass

def resolve_tz_arg(arg:str)->Optional[Tuple[str,int,str]]:
    if arg in TZ_MAP:
        zone,off=TZ_MAP[arg]; return (arg,off,zone)
    if arg.startswith(("+","-")) or arg.isdigit():
        raw=arg[1:] if arg.startswith("+") else arg
        if raw.lstrip("-").isdigit():
            off=int(raw)
            if -12<=off<=14:
                return (f"UTC{off:+d}", off, f"Etc/GMT{-off}")
    if "/" in arg:
        for rus,(zone,off) in TZ_MAP.items():
            if zone.lower()==arg.lower():
                return (rus,off,zone)
        return (arg,0,arg)
    return None

async def get_last_fpl_update_dt()->datetime:
    global _last_fpl_update_dt,_last_fpl_update_cache_ts
    now=time.time()
    if _last_fpl_update_dt and _last_fpl_update_cache_ts and (now-_last_fpl_update_cache_ts)<=LAST_FPL_UPDATE_TTL:
        return _last_fpl_update_dt
    data=await fetch_json(fpl_url(event_status_path), timeout=10, attempts=2)
    dt=None
    if isinstance(data,dict) and isinstance(data.get("status"),list):
        dates=[]
        for st in data["status"]:
            ds=st.get("date")
            d=parse_deadline(ds) if isinstance(ds,str) else None
            if d: dates.append(d.astimezone(timezone.utc))
        if dates: dt=max(dates)
    if dt is None:
        dt=_last_fpl_update_dt or datetime.now(timezone.utc)
    _last_fpl_update_dt=dt
    _last_fpl_update_cache_ts=now
    return dt

def format_updated_line(user_id:Optional[int], dt:datetime, width:int)->str:
    tz,off=get_user_tz(user_id) if user_id else ("UTC",0)
    local=dt+timedelta(hours=off)
    line=f"Обновлено: {local.strftime('%Y-%m-%d %H:%M')} ({tz} UTC{off:+d})"
    return line if width<=0 else line.center(width)

def _norm_name(s:str)->str:
    return re.sub(r"[\s\.\-’'`]", "", (s or "").lower())

def refresh_bootstrap_maps():
    elements=bootstrap_cache.get("elements",[])
    teams=bootstrap_cache.get("teams",[])
    PLAYER_NAME_MAP.clear(); PLAYER_POS_MAP.clear(); PLAYER_TEAM_MAP.clear(); TEAM_SHORT_MAP.clear(); NAME_TO_ELEMENT_ID.clear()
    for e in elements:
        if isinstance(e.get("id"),int):
            nm=(e.get("web_name") or e.get("second_name") or f"P{e['id']}").strip()
            PLAYER_NAME_MAP[e["id"]]=nm
            PLAYER_POS_MAP[e["id"]]=e.get("element_type")
            PLAYER_TEAM_MAP[e["id"]]=e.get("team")
            if nm:
                NAME_TO_ELEMENT_ID[_norm_name(nm)]=e["id"]
    for t in teams:
        if isinstance(t.get("id"),int):
            TEAM_SHORT_MAP[t["id"]]=(t.get("short_name") or t.get("name") or f"T{t['id']}")

def bootstrap_valid():
    return bootstrap_cache_ts and (time.time()-bootstrap_cache_ts)/60 < FPL_CACHE_TTL_MIN

async def get_bootstrap_cached()->Optional[Dict]:
    global bootstrap_cache_ts,_bootstrap_etag
    async with bootstrap_lock:
        headers={}
        if _bootstrap_etag: headers["If-None-Match"]=_bootstrap_etag
        resp=await fetch_json(fpl_url(bootstrap_path), extra_headers=headers, return_response=True, timeout=15, attempts=2)
        if isinstance(resp,httpx.Response):
            if resp.status_code==304 and bootstrap_cache: return bootstrap_cache
            if resp.status_code==200:
                try:
                    data=resp.json()
                    bootstrap_cache.clear(); bootstrap_cache.update(data)
                    bootstrap_cache_ts=time.time()
                    _bootstrap_etag=resp.headers.get("ETag") or _bootstrap_etag
                    refresh_bootstrap_maps()
                    return data
                except Exception as e:
                    logger.warning("Bootstrap parse fail: %s", e)
        return bootstrap_cache if bootstrap_cache else None

def fixtures_cache_valid(ts:float)->bool:
    return (time.time()-ts)<FIXTURES_TTL

async def get_fixtures_cached(gw:int)->List[Dict]:
    st=_fixtures_cache.get(gw)
    if st and fixtures_cache_valid(st[0]): return st[1]
    data=await fetch_json(fpl_url(fixtures_event_path_tpl.format(gw=gw)), timeout=10, attempts=2) or []
    _fixtures_cache[gw]=(time.time(),data)
    return data

def standings_valid():
    return standings_cache_ts and (time.time()-standings_cache_ts)<=FPL_STANDINGS_TTL

async def get_league_results_cached(league_id:str)->Optional[List[Dict]]:
    global LEAGUE_NAME, standings_cache_ts, ENTRY_TO_MANAGER_NAME, ENTRY_TO_RANK, ENTRY_TO_OVERALL
    if standings_valid() and "results" in standings_cache:
        if standings_cache.get("league_name"): LEAGUE_NAME=standings_cache["league_name"]
        results=standings_cache.get("results",[])
        ENTRY_TO_MANAGER_NAME={r["entry"]:r.get("player_name","") for r in results if isinstance(r.get("entry"),int)}
        ENTRY_TO_RANK={r["entry"]:r.get("rank") for r in results if isinstance(r.get("entry"),int)}
        ENTRY_TO_OVERALL={r["entry"]:r.get("total") for r in results if isinstance(r.get("entry"),int)}
        return results
    async with standings_lock:
        if standings_valid() and "results" in standings_cache:
            if standings_cache.get("league_name"): LEAGUE_NAME=standings_cache["league_name"]
            results=standings_cache.get("results",[])
            ENTRY_TO_MANAGER_NAME={r["entry"]:r.get("player_name","") for r in results if isinstance(r.get("entry"),int)}
            ENTRY_TO_RANK={r["entry"]:r.get("rank") for r in results if isinstance(r.get("entry"),int)}
            ENTRY_TO_OVERALL={r["entry"]:r.get("total") for r in results if isinstance(r.get("entry"),int)}
            return results
        all_results=[]
        page=1
        league_name_local=None
        while True:
            data=await fetch_json(fpl_url(league_path_tpl.format(league_id=league_id,page=page)), timeout=12, attempts=2)
            if not data: break
            try:
                st=data["standings"]
                if league_name_local is None:
                    league_name_local=st.get("league",{}).get("name")
                all_results.extend(st.get("results",[]))
                if not st.get("has_next"): break
                page+=1
                if page>30: break
            except Exception:
                break
        if all_results:
            standings_cache.clear()
            standings_cache.update({"results":all_results,"league_name":league_name_local,"pages":page})
            standings_cache_ts=time.time()
            LEAGUE_NAME=league_name_local or LEAGUE_NAME or "Zakynule Fantasy Vibe"
            ENTRY_TO_MANAGER_NAME={r["entry"]:r.get("player_name","") for r in all_results if isinstance(r.get("entry"),int)}
            ENTRY_TO_RANK={r["entry"]:r.get("rank") for r in all_results if isinstance(r.get("entry"),int)}
            ENTRY_TO_OVERALL={r["entry"]:r.get("total") for r in all_results if isinstance(r.get("entry"),int)}
            return all_results
        if not LEAGUE_NAME: LEAGUE_NAME="Zakynule Fantasy Vibe"
        return None

def picks_valid(ts:Optional[float])->bool:
    return ts and (time.time()-ts)<=FPL_PICKS_TTL

async def _picks_lock(key:Tuple[int,int])->asyncio.Lock:
    async with _picks_locks_guard:
        lk=_picks_locks.get(key)
        if not lk:
            lk=asyncio.Lock(); _picks_locks[key]=lk
        return lk

async def get_entry_picks_cached(entry:int, gw:int)->Optional[Dict]:
    key=(entry,gw)
    cached=picks_cache.get(key)
    if cached and picks_valid(cached.get("ts")): return cached["data"]
    lock=await _picks_lock(key)
    async with lock:
        cached2=picks_cache.get(key)
        if cached2 and picks_valid(cached2.get("ts")): return cached2["data"]
        async with picks_semaphore:
            data=await fetch_json(fpl_url(entry_picks_path_tpl.format(entry_id=entry,gw=gw)), timeout=10, attempts=2)
        if data:
            picks_cache[key]={"data":data,"ts":time.time()}
            return data
        if FPL_PICKS_ALLOW_STALE and cached2 and cached2.get("data"):
            return cached2["data"]
        return None

def discover_season_tag(bootstrap:Dict[str,Any])->str:
    global SEASON_TAG
    if SEASON_TAG: return SEASON_TAG
    season=bootstrap.get("game_settings",{}).get("season")
    if not season:
        y=datetime.now(timezone.utc).year
        season=f"{y}/{str((y+1)%100).zfill(2)}"
    SEASON_TAG=season
    return season

async def get_league_player_ids_cached(gw:int)->Set[int]:
    now=time.time()
    cached=_league_players_cache.get(gw)
    if cached and now-cached[0]<=LEAGUE_PLAYERS_TTL: return cached[1]
    results=await get_league_results_cached(LEAGUE_ID)
    if not results:
        _league_players_cache[gw]=(now,set()); return set()
    coros=[get_entry_picks_cached(r["entry"],gw) for r in results if isinstance(r.get("entry"),int)]
    data_all=await gather_limited(coros,PICKS_CONCURRENCY)
    players=set()
    for pkg in data_all:
        if isinstance(pkg,dict):
            for p in pkg.get("picks",[]):
                elid=p.get("element")
                if isinstance(elid,int): players.add(elid)
    _league_players_cache[gw]=(now,players)
    return players

def get_current_or_next_gw(bootstrap:Dict[str,Any])->Optional[int]:
    cur=next((e for e in bootstrap.get("events",[]) if e.get("is_current")),None)
    if cur and isinstance(cur.get("id"),int): return cur["id"]
    nxt=next((e for e in bootstrap.get("events",[]) if e.get("is_next")),None)
    if nxt and isinstance(nxt.get("id"),int): return nxt["id"]
    return None

def get_active_fixture_ids(fixtures:List[Dict], now:Optional[datetime]=None)->Set[int]:
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

def format_points(delta:int)->str:
    return f"(+{delta})" if delta>0 else f"({delta})"

def total_no_bonus(stats:Dict[str,Any])->int:
    return int(stats.get("total_points",0) or 0)-int(stats.get("bonus",0) or 0)

def event_points_for(pos:int, etype:str)->int:
    if etype=="Goal":
        if pos==1: return 10
        if pos==2: return 6
        if pos==3: return 5
        return 4
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

def dc_threshold_met(stats:Dict[str,Any], pos:int)->bool:
    if pos==1: return False
    dc=0
    for k in ("defensive_contributions","cbit","cbits","def_contributions"):
        v=stats.get(k)
        if isinstance(v,(int,float)):
            dc=int(v); break
    if pos==2: return dc>=10
    return dc>=12 if pos in (3,4) else False

def has_any_dc(stats:Dict[str,Any])->bool:
    for k in ("defensive_contributions","cbit","cbits","def_contributions"):
        v=stats.get(k)
        if isinstance(v,(int,float)) and v>0: return True
    return False

def compute_base_points(stats:Dict[str,Any], pos:int)->int:
    minutes=int(stats.get("minutes",0) or 0)
    base=2 if minutes>=60 else (1 if minutes>0 else 0)
    cs=int(stats.get("clean_sheets",0) or 0)
    if cs:
        if pos in (1,2): base+=4
        elif pos==3: base+=1
    yc=int(stats.get("yellow_cards",0) or 0)
    rc=int(stats.get("red_cards",0) or 0)
    og=int(stats.get("own_goals",0) or 0)
    pm=int(stats.get("penalties_missed",0) or 0)
    if pos==1:
        base += int((stats.get("saves",0) or 0)//3)
    gc=int(stats.get("goals_conceded",0) or 0)
    if pos in (1,2) and gc>=2:
        base -= (gc//2)
    base -= yc
    base -= rc*3
    base -= og*2
    base -= pm*2
    return base

def ns_key(gw:int,fid:int,pid:int)->str:
    return f"{gw}:{fid}:{pid}"

def message_hash(text:str)->str:
    return hashlib.blake2s(text.encode()).hexdigest()

def idempotency_set_key(season:str, gw:int)->str:
    return f"live:msgs:{season}:{gw}"

def split_message_chunks(text:str, limit:int=3900)->List[str]:
    if len(text)<=limit: return [text]
    out=[]; cur=""
    for ln in text.splitlines(keepends=True):
        if len(cur)+len(ln)>limit:
            out.append(cur.rstrip("\n")); cur=""
        cur+=ln
    if cur: out.append(cur.rstrip("\n"))
    return out

async def send_text_raw(chat_id:int, text:str):
    for c in split_message_chunks(text):
        try:
            await application.bot.send_message(chat_id=chat_id, text=c, disable_notification=False)
            await asyncio.sleep(0.03)
        except Exception as e:
            logger.debug("send_text_raw fail: %s", e)

async def send_once(text:str, season:str, gw:int):
    h=message_hash(text)
    key=idempotency_set_key(season,gw)
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
        if application: await send_text_raw(ALLOWED_GROUP_ID, text)

def parse_deadline(dt_str:str)->Optional[datetime]:
    if not dt_str: return None
    try:
        if dt_str.endswith("Z"): dt_str=dt_str.replace("Z","+00:00")
        return datetime.fromisoformat(dt_str)
    except Exception: return None

def format_minute_str(minutes:int,fid:int,fixture_max_minute:Dict[int,int])->str:
    return f"{minutes}'"

CATEGORY_ALIASES={
    "already reached target":"Already reached target",
    "projected to reach today":"Projected to reach target",
    "projected to reach target":"Projected to reach target",
    "projected":"Projected to reach target",
    "others who will be close (by end of day)":"Others who will be close (by end of day)",
    "close by end of day":"Others who will be close (by end of day)"
}
CATEGORY_ORDER=[
    "Already reached target",
    "Projected to reach target",
    "Others who will be close (by end of day)"
]

def _norm_category(cat:str)->str:
    return CATEGORY_ALIASES.get(cat.strip().lower(), cat)

def build_ownership_map()->Dict[str,float]:
    mp={}
    for el in bootstrap_cache.get("elements",[]):
        nm=(el.get("web_name") or el.get("second_name") or "").strip()
        if not nm: continue
        sel=el.get("selected_by_percent")
        try: mp[nm.lower()]=float(sel) if sel is not None else 0.0
        except Exception: mp[nm.lower()]=0.0
    return mp

def _deep_iter(obj):
    if isinstance(obj,dict):
        for k,v in obj.items():
            yield (k,v); yield from _deep_iter(v)
    elif isinstance(obj,list):
        for it in obj:
            yield from _deep_iter(it)

def extract_progress(item:Any)->Optional[Tuple[str,float]]:
    if isinstance(item,dict):
        nm=item.get("name") or item.get("web_name") or item.get("player") or item.get("player_name")
        prog=item.get("progress") or item.get("priceTargetPercent") or item.get("percent") or item.get("value") or item.get("completion")
        if nm is None or prog is None: return None
        try:
            val=float(prog); return (str(nm).strip(), val)
        except Exception:
            s=str(prog).strip().replace("%","")
            if re.fullmatch(r"-?\d+(\.\d+)?",s): return (str(nm).strip(), float(s))
    elif isinstance(item,str):
        m=re.match(r"(.+?)\s*\(([-+]?\d+(?:\.\d+)?)%?\)", item.strip())
        if m: return (m.group(1).strip(), float(m.group(2)))
    return None

def categorize_progress(val:float, rise:bool)->Optional[str]:
    if rise:
        if val>=100: return "Already reached target"
        if 90<=val<100: return "Projected to reach target"
        if 80<=val<90: return "Others who will be close (by end of day)"
    else:
        if val<=-100: return "Already reached target"
        if -100<val<=-90: return "Projected to reach target"
        if -90<val<=-80: return "Others who will be close (by end of day)"
    return None

def parse_prices_from_json_enhanced(data:Any, ownership_map:Dict[str,float])->Dict[str,Dict[str,List[Tuple[str,float]]]]:
    out={"Rises":{}, "Falls":{}}
    for k,v in _deep_iter(data):
        if isinstance(v,list):
            for item in v:
                pr=extract_progress(item)
                if not pr: continue
                nm,val=pr
                if ownership_map.get(nm.lower(),0)<1.0: continue
                rise=val>0
                cat=categorize_progress(val, rise)
                if not cat: continue
                side="Rises" if rise else "Falls"
                out[side].setdefault(cat,[])
                if not any(x[0].lower()==nm.lower() for x in out[side][cat]):
                    out[side][cat].append((nm,val))
    return out

def parse_prices_from_html_enhanced(html:str, ownership_map:Dict[str,float])->Dict[str,Dict[str,List[Tuple[str,float]]]]:
    out={"Rises":{}, "Falls":{}}
    pattern=r"([A-Z][A-Za-z\.' -]{2,})[^%]{0,80}?([-+]\d{1,3}\.\d{2})%"
    for nm,val_s in re.findall(pattern, html):
        try: val=float(val_s)
        except Exception: continue
        if ownership_map.get(nm.lower(),0)<1.0: continue
        rise=val>0
        cat=categorize_progress(val,rise)
        if not cat: continue
        side="Rises" if rise else "Falls"
        out[side].setdefault(cat,[])
        if not any(x[0].lower()==nm.lower() for x in out[side][cat]):
            out[side][cat].append((nm,val))
    if BeautifulSoup:
        try:
            soup=BeautifulSoup(html,"html.parser")
            for script in soup.find_all("script"):
                if script.get("type")=="application/json":
                    try:
                        data=json.loads(script.string or "")
                        nested=parse_prices_from_json_enhanced(data, ownership_map)
                        for side in ("Rises","Falls"):
                            for cat,items in nested[side].items():
                                out[side].setdefault(cat,[])
                                for nm,val in items:
                                    if not any(x[0].lower()==nm.lower() for x in out[side][cat]):
                                        out[side][cat].append((nm,val))
                    except Exception: continue
        except Exception: pass
    return out

def player_meta_by_name(name:str)->Optional[Tuple[str,str,str]]:
    pid=NAME_TO_ELEMENT_ID.get(_norm_name(name))
    if not pid: return None
    el=next((e for e in bootstrap_cache.get("elements",[]) if e.get("id")==pid),None)
    if not el: return None
    pos_id=el.get("element_type")
    pos_short={1:"GK",2:"DEF",3:"MID",4:"FWD"}.get(pos_id,"")
    team=TEAM_SHORT_MAP.get(el.get("team"),"")
    price=""
    if isinstance(el.get("now_cost"),int):
        price=f"£{el['now_cost']/10:.1f}"
    return (pos_short, team, price)

def prices_cache_valid()->bool:
    return prices_cache_ts is not None and (time.time()-prices_cache_ts)<=PRICES_CACHE_TTL

async def fetch_prices_page()->Optional[str]:
    try:
        resp=await http_client.get(PRICES_URL,timeout=15.0)
        if resp.status_code==200: return resp.text
    except Exception: pass
    return None

async def fetch_prices_json_candidates()->List[Any]:
    urls=[
        "https://prices.livefpl.net/latest_prices.json",
        "https://prices.livefpl.net/summary.json",
        "https://www.livefpl.net/api/prices",
        "https://www.livefpl.net/api/prices?summary=1"
    ]
    out=[]
    for url in urls:
        try:
            r=await http_client.get(url,timeout=10.0)
            if r.status_code==200:
                try: out.append(r.json())
                except Exception: pass
        except Exception: pass
    return out

async def get_prices_data()->Dict[str,Any]:
    global prices_cache_data, prices_cache_ts
    if prices_cache_valid() and prices_cache_data: return prices_cache_data
    ownership_map=build_ownership_map()
    datasets=await fetch_prices_json_candidates()
    combined={"Rises":{}, "Falls":{}}
    for data in datasets:
        parsed=parse_prices_from_json_enhanced(data, ownership_map)
        for side in ("Rises","Falls"):
            for cat,items in parsed[side].items():
                combined[side].setdefault(cat,[])
                for nm,val in items:
                    if not any(x[0].lower()==nm.lower() for x in combined[side][cat]):
                        combined[side][cat].append((nm,val))
    if not any(combined[s] for s in ("Rises","Falls")):
        html=await fetch_prices_page()
        if html:
            combined=parse_prices_from_html_enhanced(html, ownership_map)
    data={"ok":True,"parsed":combined,"ts":time.time()}
    prices_cache_data=data; prices_cache_ts=time.time()
    if redis_client:
        try: redis_client.set("fpl:prices:cache", json.dumps(data), ex=PRICES_CACHE_TTL)
        except Exception: pass
    return data

def format_prices_mobile(parsed:Dict[str,Dict[str,List[Tuple[str,float]]]])->List[str]:
    lines=["Summary of Predictions"]
    for cat in CATEGORY_ORDER:
        lines.append(cat)
        rises=parsed.get("Rises",{}).get(cat,[])
        falls=parsed.get("Falls",{}).get(cat,[])
        def fmt(items,label):
            if not items: return f"  {label}: (нет)"
            parts=[]
            for nm,val in items:
                meta=player_meta_by_name(nm)
                pct=f"{val:.2f}%"
                if meta:
                    pos,team,price=meta
                    tag=" ".join(x for x in [nm,pos,price,pct] if x)
                else:
                    tag=f"{nm} {pct}"
                parts.append(tag)
            return "  "+label+": "+", ".join(parts)
        lines.append(fmt(rises,"Rises"))
        lines.append(fmt(falls,"Falls"))
    return lines

def build_tz_main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Астана (UTC+5)",callback_data="tz|Астана"),
         InlineKeyboardButton("Баку (UTC+4)",callback_data="tz|Баку")],
        [InlineKeyboardButton("Москва (UTC+3)",callback_data="tz|Москва"),
         InlineKeyboardButton("Киев (UTC+2)",callback_data="tz|Киев")],
        [InlineKeyboardButton("Центральная Европа (UTC+1)",callback_data="tz|Центральная Европа"),
         InlineKeyboardButton("Лондон (UTC+0)",callback_data="tz|Лондон")],
        [InlineKeyboardButton("Другое…",callback_data="tz_more")]
    ])

def build_tz_more_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Бишкек (UTC+6)",callback_data="tz|Бишкек"),
         InlineKeyboardButton("Новосибирск (UTC+7)",callback_data="tz|Новосибирск")],
        [InlineKeyboardButton("Владивосток (UTC+10)",callback_data="tz|Владивосток")],
        [InlineKeyboardButton("Назад",callback_data="tz_back")]
    ])

@safe_command
async def tz_inline_callback(update:Update, context:ContextTypes.DEFAULT_TYPE):
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
            await q.answer("Ошибка"); return
        rn,off,zone=info
        set_user_tz(q.from_user.id,rn,off,zone)
        await q.answer("Ок")
        await q.edit_message_text(f"Таймзона: {rn} (UTC{off:+d})")

@safe_command
async def help_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    owner=is_owner(update)
    lines=["Команды:"]
    lines.append("/deadline — ближайший дедлайн")
    lines.append("/players_pts [gw] — live очки игроков")
    lines.append("/gwpoints [gw] — очки менеджеров тура")
    lines.append("/month [1..10] — топ-10 месяц")
    lines.append("/rank — таблица лиги")
    if owner:
        lines.append("/prices — прогноз цен")
        lines.append("/squid_start <gw> — старт Squid")
        lines.append("/squid_stop — сброс Squid")
    lines.append("/squid_status — последний отчёт Squid")
    lines.append("/squid_winners — победители Squid")
    lines.append("/squid_rules — правила Squid")
    lines.append("/tz <название|смещение> — таймзона")
    await reply_silent(update, "\n".join(lines))

@safe_command
async def settz_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    if not context.args: return
    arg=" ".join(context.args)
    info=resolve_tz_arg(arg)
    if not info: return
    rn,off,zone=info
    set_user_tz(update.effective_user.id,rn,off,zone)
    await reply_silent(update, f"Таймзона: {rn} (UTC{off:+d})")

@safe_command
async def deadline_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    bs=await get_bootstrap_cached()
    if not bs: return
    events=bs.get("events",[])
    now_utc=datetime.now(timezone.utc)
    future=[]
    for e in events:
        dt=parse_deadline(e.get("deadline_time"))
        if dt and dt>now_utc: future.append((dt,e))
    if not future: return
    dt,e=min(future,key=lambda x:x[0])
    tz_name,offset=get_user_tz(update.effective_user.id)
    local_dt=dt.astimezone(timezone.utc)+timedelta(hours=offset)
    now_local=now_utc+timedelta(hours=offset)
    left=int((local_dt-now_local).total_seconds())
    if left<0: left=0
    d=left//86400; h=(left%86400)//3600; m=(left%3600)//60
    await reply_silent(update, f"Дедлайн GW {e.get('id')}: {local_dt.strftime('%Y-%m-%d %H:%M:%S')} ({tz_name} UTC{offset:+d})\nОсталось: {d}д {h}ч {m}м")

@safe_command
async def rank_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    bs=await get_bootstrap_cached()
    if not bs: return
    results=await get_league_results_cached(LEAGUE_ID)
    if not results: return
    sig=f"{standings_cache_ts or 0}|{len(results)}"
    cached=cache_get_text("rank")
    if cached and cached.get("sig")==sig and isinstance(cached.get("text"),str):
        await reply_silent(update, cached["text"], parse_mode="Markdown"); return
    league_name=LEAGUE_NAME or "League"
    rows=sorted(results,key=lambda r:r.get("rank",10**9))
    name_w=max(len("Manager"), max(len(r.get("player_name","")) for r in rows))
    pts_w=max(len("Pts"), max(len(str(r.get("total"))) for r in rows))
    num_w=max(len("#"), len(str(len(rows))))
    header=f"{'#'.rjust(num_w)} {'Manager'.ljust(name_w)} {'Pts'.rjust(pts_w)}"
    dt=await get_last_fpl_update_dt()
    upd=format_updated_line(update.effective_user.id, dt, len(header))
    lines=[league_name.center(len(header)), upd, "```", header]
    for i,r in enumerate(rows, start=1):
        lines.append(f"{str(i).rjust(num_w)} {r.get('player_name','').ljust(name_w)} {str(r.get('total')).rjust(pts_w)}")
    lines.append("```")
    text="\n".join(lines)
    cache_set_text("rank", sig, text, ttl=300)
    await reply_silent(update, text, parse_mode="Markdown")

def format_stats_tokens(s:Dict[str,Any], pos:int)->List[str]:
    tokens=[]
    # Goals
    g=int(s.get("goals_scored",0) or 0)
    if g>0: tokens.append(f"G{g}" if g>1 else "G")
    a=int(s.get("assists",0) or 0)
    if a>0: tokens.append(f"A{a}" if a>1 else "A")
    cs=int(s.get("clean_sheets",0) or 0)
    if cs and pos!=4: tokens.append("CS")
    # DC heuristic separate
    # Yellow/Red
    yc=int(s.get("yellow_cards",0) or 0)
    if yc>0: tokens.append(f"YC{yc}" if yc>1 else "YC")
    rc=int(s.get("red_cards",0) or 0)
    if rc>0: tokens.append("RC")  # red rarely >1
    # GK saves
    if pos==1:
        saves=int(s.get("saves",0) or 0)
        blocks=saves//3
        if blocks>0: tokens.append(f"S{blocks}")
    og=int(s.get("own_goals",0) or 0)
    if og>0: tokens.append(f"OG{og}" if og>1 else "OG")
    pm=int(s.get("penalties_missed",0) or 0)
    if pm>0: tokens.append(f"PM{pm}")
    ps=int(s.get("penalties_saved",0) or 0)
    if ps>0: tokens.append(f"PS{ps}")
    b=int(s.get("bonus",0) or 0)
    if b>0: tokens.append(f"B{b}")
    return tokens

def pad_stats(value:str, width:int)->str:
    return value.ljust(width)

@safe_command
async def players_pts_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    bs=await get_bootstrap_cached()
    if not bs: return
    gw=int(context.args[0]) if context.args and context.args[0].isdigit() else get_current_or_next_gw(bs)
    if not isinstance(gw,int): return
    last_update=await get_last_fpl_update_dt()
    sig=f"{gw}|{int(last_update.timestamp())}"
    feature_key=f"players_pts:{gw}"
    cached=cache_get_text(feature_key)
    if cached and cached.get("sig")==sig and isinstance(cached.get("text"),str):
        # Return cached table silently
        for chunk in split_message_chunks(cached["text"]):
            try: await update.message.reply_text(chunk, parse_mode="Markdown", disable_notification=True)
            except Exception: await update.message.reply_text(chunk.replace("```",""), disable_notification=True)
        return
    await get_league_results_cached(LEAGUE_ID)
    live=await fetch_json(fpl_url(event_live_path_tpl.format(gw=gw)), timeout=10, attempts=2)
    if not live: return
    league_players=await get_league_player_ids_cached(gw)
    rows=[]
    for el in live.get("elements",[]):
        pid=el.get("id")
        if pid not in league_players: continue
        stats=el.get("stats",{}) or {}
        pos=PLAYER_POS_MAP.get(pid,0)
        total=int(stats.get("total_points",0) or 0)
        tokens=format_stats_tokens(stats,pos)
        # DC logic
        dc_show=False
        if pos!=1 and (dc_threshold_met(stats,pos) or has_any_dc(stats)):
            dc_show=True
        else:
            if pos!=1:
                base=compute_base_points(stats,pos)
                diff=total-base
                if diff>=2 and not (int(stats.get("goals_scored",0)) or int(stats.get("assists",0))
                                    or int(stats.get("clean_sheets",0)) or int(stats.get("penalties_saved",0))
                                    or int(stats.get("bonus",0)) or int(stats.get("own_goals",0))
                                    or int(stats.get("penalties_missed",0)) or int(stats.get("red_cards",0))
                                    or int(stats.get("yellow_cards",0))):
                    dc_show=True
        if dc_show:
            tokens.insert(0,"DC")
        if not tokens and total<=2:
            continue
        rows.append({"name":PLAYER_NAME_MAP.get(pid,f"P{pid}"),"stats":" ".join(tokens),"pts":total})
    if not rows: return
    rows.sort(key=lambda r:(-r["pts"], r["name"].lower()))
    name_w=max(len("Player"), max(len(r["name"]) for r in rows))
    stats_w=max(len("Stats"), max(len(r["stats"]) for r in rows))
    pts_w=max(len("Pts"), max(len(str(r["pts"])) for r in rows))
    num_w=max(len("#"), len(str(len(rows))))
    header=f"{'#'.rjust(num_w)} {'Player'.ljust(name_w)} {'Stats'.ljust(stats_w)} {'Pts'.rjust(pts_w)}"
    upd=format_updated_line(update.effective_user.id, last_update, len(header))
    lines=[f"{(LEAGUE_NAME or 'League')} — GW {gw}".center(len(header)), upd, "```", header]
    for i,r in enumerate(rows, start=1):
        lines.append(f"{str(i).rjust(num_w)} {r['name'].ljust(name_w)} {r['stats'].ljust(stats_w)} {str(r['pts']).rjust(pts_w)}")
    lines.append("```")
    legend=[
        "Legend:",
        "G - goal",
        "A - assist",
        "CS - clean sheet",
        "DC - defensive contribution",
        "YC - yellow card",
        "RC - red card",
        "S - saves (each S unit = 3 saves)",
        "OG - own goal",
        "PM - penalty missed",
        "PS - penalty saved",
        "B - bonus"
    ]
    lines.extend(legend)
    text="\n".join(lines)
    cache_set_text(feature_key, sig, text, ttl=120)
    for chunk in split_message_chunks(text):
        try: await update.message.reply_text(chunk, parse_mode="Markdown", disable_notification=True)
        except Exception: await update.message.reply_text(chunk.replace("```",""), disable_notification=True)

@safe_command
async def gwpoints_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    bs=await get_bootstrap_cached()
    if not bs: return
    gw=int(context.args[0]) if context.args and context.args[0].isdigit() else get_current_or_next_gw(bs)
    if not isinstance(gw,int): return
    results=await get_league_results_cached(LEAGUE_ID)
    if not results: return
    async def one(r):
        eid=r["entry"]
        data=await get_entry_picks_cached(eid,gw)
        pts=data.get("entry_history",{}).get("points") if data else -1
        return (eid,r.get("player_name",""), pts if isinstance(pts,int) else -1, ENTRY_TO_OVERALL.get(eid,0))
    got=await gather_limited([one(r) for r in results if isinstance(r.get("entry"),int)], PICKS_CONCURRENCY)
    entries=[e for e in got if isinstance(e,tuple)]
    entries.sort(key=lambda x:(-x[2], -x[3], x[1].lower()))
    name_w=max(len("Manager"), max(len(e[1]) for e in entries))
    pts_w=max(len("Pts"), max(len(str(e[2])) for e in entries))
    num_w=max(len("#"), len(str(len(entries))))
    header=f"{'#'.rjust(num_w)} {'Manager'.ljust(name_w)} {'Pts'.rjust(pts_w)}"
    dt=await get_last_fpl_update_dt()
    upd=format_updated_line(update.effective_user.id, dt, len(header))
    lines=[f"{(LEAGUE_NAME or 'League')} — GW {gw}".center(len(header)), upd, "```", header]
    for i,e in enumerate(entries, start=1):
        pts=e[2] if e[2]>=0 else "n/a"
        lines.append(f"{str(i).rjust(num_w)} {e[1].ljust(name_w)} {str(pts).rjust(pts_w)}")
    lines.append("```")
    await reply_silent(update, "\n".join(lines), parse_mode="Markdown")

RUS_MONTH={1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"}
SEASON_MONTH_ORDER=[8,9,10,11,12,1,2,3,4,5]

def season_month_index(month:int)->Optional[int]:
    try: return SEASON_MONTH_ORDER.index(month)+1
    except ValueError: return None

def season_month_index_to_calendar(index:int, season_tag:Optional[str])->Tuple[int,int]:
    if index<1 or index>10: raise ValueError("month index")
    m=SEASON_MONTH_ORDER[index-1]
    if season_tag and "/" in season_tag:
        try: start_year=int(season_tag.split("/")[0])
        except Exception: start_year=datetime.now(timezone.utc).year
    else:
        start_year=datetime.now(timezone.utc).year
    year=start_year if m>=8 else start_year+1
    return m,year

async def month_points_for(entries:List[Dict], events:List[Dict], target_month:int, target_year:int)->Tuple[List[Tuple[int,str,int]],int,int]:
    month_events=[]
    for ev in events:
        dt=parse_deadline(ev.get("deadline_time"))
        if dt and dt.month==target_month and dt.year==target_year:
            month_events.append(ev.get("id"))
    if not month_events: return [],0,0
    finished=[ev.get("id") for ev in events if ev.get("finished") and ev.get("id") in month_events]
    played=len(finished); remaining=len(month_events)-played
    async def fetch(eid:int,nm:str):
        total=0
        for g in finished:
            data=await get_entry_picks_cached(eid,g)
            if data:
                pts=data.get("entry_history",{}).get("points")
                if isinstance(pts,int): total+=pts
        return (eid,nm,total)
    got=await gather_limited([fetch(r["entry"], r.get("player_name","")) for r in entries if isinstance(r.get("entry"),int)], PICKS_CONCURRENCY)
    rows=[x for x in got if isinstance(x,tuple)]
    rows.sort(key=lambda x:(-x[2], ENTRY_TO_RANK.get(x[0],10**9), x[1].lower()))
    return rows, played, remaining

@safe_command
async def month_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    bs=await get_bootstrap_cached()
    if not bs: return
    events=bs.get("events",[])
    results=await get_league_results_cached(LEAGUE_ID)
    if not results: return
    idx=int(context.args[0]) if context.args and context.args[0].isdigit() else (season_month_index(datetime.now(timezone.utc).month) or 1)
    if idx<1 or idx>10: return
    season=discover_season_tag(bs)
    cal_m,cal_y=season_month_index_to_calendar(idx,season)
    rows,played,remaining=await month_points_for(results,events,cal_m,cal_y)
    month_name=RUS_MONTH.get(cal_m,f"M{cal_m}")
    league_name=LEAGUE_NAME or "League"
    top=rows[:10]
    name_w=max(len("Manager"), max((len(manager_name(eid)) for (eid,_n,pts) in top), default=7))
    pts_w=max(len("Pts"), max((len(str(pts)) for (_eid,_n,pts) in top), default=3))
    num_w=max(len("#"), len(str(len(top))))
    header=f"{'#'.rjust(num_w)} {'Manager'.ljust(name_w)} {'Pts'.rjust(pts_w)}"
    dt=await get_last_fpl_update_dt()
    upd=format_updated_line(update.effective_user.id, dt, len(header))
    info_line=f"Сыграно: {played} Осталось: {remaining}" if remaining>0 else ""
    lines=[f"{league_name} — {month_name}".center(len(header))]
    if info_line: lines.append(info_line.center(len(header)))
    lines.append(upd)
    lines.extend(["```", header])
    for i,(eid,_nm,pts) in enumerate(top, start=1):
        lines.append(f"{str(i).rjust(num_w)} {manager_name(eid).ljust(name_w)} {str(pts).rjust(pts_w)}")
    lines.append("```")
    await reply_silent(update, "\n".join(lines), parse_mode="Markdown")

SQUID_ACTIVE_KEY="fpl:squid:active"
SQUID_WINNERS_KEY="fpl:squid:winners"
SQUID_LAST_REPORT_KEY="fpl:squid:last_report"

def load_squid_from_redis():
    global squid_active_state,squid_winner_history
    if not redis_client: return
    try:
        a=redis_client.get(SQUID_ACTIVE_KEY)
        if a:
            try: squid_active_state=json.loads(_ensure_str(a) or "")
            except Exception: squid_active_state=None
        w=redis_client.get(SQUID_WINNERS_KEY)
        if w:
            try: squid_winner_history=json.loads(_ensure_str(w) or "")
            except Exception: squid_winner_history=[]
    except Exception as e:
        logger.warning("Load squid failed: %s", e)

def persist_squid():
    if not redis_client: return
    try:
        if squid_active_state is not None:
            redis_client.set(SQUID_ACTIVE_KEY,json.dumps(squid_active_state))
        redis_client.set(SQUID_WINNERS_KEY,json.dumps(squid_winner_history))
    except Exception as e:
        logger.warning("Persist squid failed: %s", e)

@safe_command
async def squid_start_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    if not is_owner(update): return
    if not context.args or not context.args[0].isdigit(): return
    start_gw=int(context.args[0])
    results=await get_league_results_cached(LEAGUE_ID)
    if not results: return
    players=[r["entry"] for r in results if isinstance(r.get("entry"),int)]
    global squid_active_state
    squid_active_state={
        "start_gw":start_gw,
        "current_gw":start_gw,
        "players_alive":players,
        "players_eliminated":[],
        "season":SEASON_TAG,
        "cycle": (squid_winner_history[-1]["cycle"]+1) if squid_winner_history else 1
    }
    persist_squid()
    await reply_silent(update, f"Squid Game стартовал с GW {start_gw}. Участников: {len(players)}.")

@safe_command
async def squid_stop_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    if not is_owner(update): return
    global squid_active_state,squid_winner_history,LAST_SQUID_REPORT_TEXT
    squid_active_state=None
    squid_winner_history=[]
    LAST_SQUID_REPORT_TEXT=None
    persist_squid()
    await reply_silent(update, "Squid Game полностью сброшен.")

@safe_command
async def squid_rules_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    await reply_silent(update, "Squid: после каждого завершённого тура остаются менеджеры с очками >= среднего; игра до одного победителя или до GW38.")

@safe_command
async def squid_status_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    if redis_client:
        try:
            rep=redis_client.get(SQUID_LAST_REPORT_KEY)
            rep=_ensure_str(rep)
            if rep:
                await reply_silent(update, rep); return
        except Exception: pass
    if LAST_SQUID_REPORT_TEXT:
        await reply_silent(update, LAST_SQUID_REPORT_TEXT); return
    if not squid_active_state:
        await reply_silent(update, "Цикл не активен."); return
    start_gw=squid_active_state["start_gw"]
    current_gw=squid_active_state["current_gw"]
    alive=squid_active_state["players_alive"]
    lines=[
        f"Squid Game статус (цикл {squid_active_state.get('cycle',1)})",
        f"Старт: GW {start_gw}",
        f"Текущий: GW {current_gw}",
        f"Живых: {len(alive)}"
    ]
    for i,eid in enumerate(alive[:50], start=1):
        lines.append(f"{i}. {manager_name(eid)}")
    if len(alive)>50:
        lines.append(f"... ещё {len(alive)-50}")
    await reply_silent(update, "\n".join(lines))

@safe_command
async def squid_winners_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    if not squid_winner_history:
        await reply_silent(update, "Победителей нет."); return
    lines=["Победители:"]
    for w in squid_winner_history:
        lines.append(f"Цикл {w.get('cycle')} — {manager_name(w.get('winner_entry'))} ({w.get('start_gw')}–{w.get('end_gw')})")
    await reply_silent(update, "\n".join(lines))

async def process_squid_after_gw_finish(finished_gw:int):
    global squid_active_state,LAST_SQUID_REPORT_TEXT
    if not squid_active_state or finished_gw!=squid_active_state["current_gw"]: return
    alive=squid_active_state["players_alive"]
    if not alive: return
    async def pts(eid:int):
        data=await get_entry_picks_cached(eid,finished_gw)
        v=data.get("entry_history",{}).get("points") if data else 0
        return eid, v if isinstance(v,int) else 0
    res=await gather_limited([pts(e) for e in alive], PICKS_CONCURRENCY)
    pmap={eid:v for eid,v in res}
    avg=sum(pmap.values())/len(pmap) if pmap else 0
    passed=[e for e in alive if pmap.get(e,0)>=avg]
    eliminated=[e for e in alive if e not in passed]
    gw38=(finished_gw==38)
    winner=None
    if gw38:
        winner=max(alive,key=lambda e:pmap.get(e,0))
    elif len(passed)<=1:
        winner=passed[0] if passed else max(alive,key=lambda e:pmap.get(e,0))
    lines=[f"Squid Game — GW {finished_gw} завершён. Среднее: {avg:.2f}"]
    if winner:
        lines.append(f"Победитель: {manager_name(winner)}")
        squid_winner_history.append({
            "cycle": squid_active_state.get("cycle",1),
            "winner_entry": winner,
            "start_gw": squid_active_state["start_gw"],
            "end_gw": finished_gw
        })
        if not gw38 and finished_gw<38:
            next_gw=finished_gw+1
            results=await get_league_results_cached(LEAGUE_ID)
            if results:
                players=[r["entry"] for r in results if isinstance(r.get("entry"),int)]
                squid_active_state={
                    "start_gw":next_gw,
                    "current_gw":next_gw,
                    "players_alive":players,
                    "players_eliminated":[],
                    "season":SEASON_TAG,
                    "cycle":squid_winner_history[-1]["cycle"]+1
                }
                lines.append(f"Новый цикл: GW {next_gw}")
            else:
                squid_active_state=None
                lines.append("Новый цикл не инициализирован.")
        else:
            squid_active_state=None
            lines.append("Цикл завершён.")
    else:
        lines.append(f"Прошли ({len(passed)}):")
        for i,e in enumerate(sorted(passed,key=lambda x:pmap.get(x,0), reverse=True), start=1):
            lines.append(f"{i}. {manager_name(e)} {pmap.get(e,0)}")
        lines.append(f"Выбыли ({len(eliminated)}):")
        for i,e in enumerate(sorted(eliminated,key=lambda x:pmap.get(x,0), reverse=True), start=1):
            lines.append(f"{i}. {manager_name(e)} {pmap.get(e,0)}")
        squid_active_state["players_alive"]=passed
        squid_active_state["players_eliminated"].extend(eliminated)
        squid_active_state["current_gw"]=finished_gw+1
    persist_squid()
    report_text="\n".join(lines)
    LAST_SQUID_REPORT_TEXT=report_text
    if redis_client:
        try: redis_client.set(SQUID_LAST_REPORT_KEY, report_text)
        except Exception: pass
    await send_text_raw(ALLOWED_GROUP_ID, report_text)

@safe_command
async def prices_command(update:Update, context:ContextTypes.DEFAULT_TYPE):
    if not is_owner(update): return
    bs=await get_bootstrap_cached()
    if not bs: return
    if redis_client and not prices_cache_valid():
        try:
            raw=redis_client.get("fpl:prices:cache")
            if raw:
                obj=json.loads(_ensure_str(raw) or "")
                prices_cache_data=obj; prices_cache_ts=obj.get("ts")
        except Exception: pass
    data=await get_prices_data()
    if not data.get("ok"):
        await reply_silent(update, "Нет данных цен."); return
    parsed=data["parsed"]
    lines=format_prices_mobile(parsed)
    await reply_silent(update, "\n".join(lines))

async def live_monitor_loop():
    global _current_gw,last_live_tick_time,last_active_fixtures_count
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
            if _current_gw!=gw:
                _current_gw=gw
                _second_yellow_processed.clear()
                _red_card_processed.clear()
                _dc_awarded.clear()
                _dc_removed_sent.clear()
                _cs_subbed_sent.clear()
                _fixture_summary_sent.clear()
                _last_counts.clear()
                # cleanup old picks
                for (entry,g) in list(picks_cache.keys()):
                    if g < gw-1: del picks_cache[(entry,g)]
            fixtures=await get_fixtures_cached(gw)
            active=get_active_fixture_ids(fixtures)
            last_active_fixtures_count=len(active)
            if not active:
                await asyncio.sleep(min(300,LIVE_POLL_INTERVAL*2)); continue
            live=await fetch_json(fpl_url(event_live_path_tpl.format(gw=gw)), timeout=10, attempts=2)
            if not live:
                await asyncio.sleep(LIVE_POLL_INTERVAL); continue
            elements=live.get("elements",[])
            league_players=await get_league_player_ids_cached(gw)
            live_by_id={el.get("id"):el for el in elements if isinstance(el.get("id"),int)}
            fixture_index={fx.get("id"):fx for fx in fixtures if isinstance(fx.get("id"),int)}
            pos_pool={}; neg_pool={}
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
                    if not isinstance(fid,int) or fid not in active: continue
                    player_state[(fid,pid)]=(pos,minutes,total_nb)
                    if minutes>fixture_max_minute.get(fid,0): fixture_max_minute[fid]=minutes
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
                team_h=fx.get("team_h"); team_a=fx.get("team_a")
                local_h=base_h; local_a=base_a
                def header_line():
                    return f"{TEAM_SHORT_MAP.get(team_h,'T?')} {local_h}-{local_a} {TEAM_SHORT_MAP.get(team_a,'T?')}"
                def get_state(pid:int)->Tuple[int,int,int]:
                    return player_state.get((fid,pid),(PLAYER_POS_MAP.get(pid,0),0,0))
                update_msgs=[]
                if "assists" in neg_pool.get(fid,{}) and "assists" in pos_pool.get(fid,{}):
                    while neg_pool[fid]["assists"] and pos_pool[fid]["assists"]:
                        from_pid=neg_pool[fid]["assists"].pop(0)
                        to_pid=pos_pool[fid]["assists"].pop(0)
                        pos_from,m_from,tot_from=get_state(from_pid)
                        pos_to,_m_to,tot_to=get_state(to_pid)
                        pts_from=event_points_for(pos_from,"Assist")
                        pts_to=event_points_for(pos_to,"Assist")
                        m=format_minute_str(m_from,fid,fixture_max_minute)
                        update_msgs.append(f"{header_line()}\n{m} Update: Assist reassigned - {PLAYER_NAME_MAP.get(from_pid)} {format_points(-pts_from)}, total ({tot_from}).\nAssist - {PLAYER_NAME_MAP.get(to_pid)} {format_points(pts_to)}, total ({tot_to}).")
                while neg_pool.get(fid,{}).get("penalties_missed") and neg_pool.get(fid,{}).get("penalties_saved") and pos_pool.get(fid,{}).get("goals_scored"):
                    m_pid=neg_pool[fid]["penalties_missed"].pop(0)
                    s_pid=neg_pool[fid]["penalties_saved"].pop(0)
                    g_pid=pos_pool[fid]["goals_scored"].pop(0)
                    pos_m,m_min,tot_m=get_state(m_pid)
                    pos_s,_min_s,tot_s=get_state(s_pid)
                    pos_g,_min_g,tot_g=get_state(g_pid)
                    pts_m=event_points_for(pos_m,"Penalty missed")
                    pts_s=event_points_for(pos_s,"Penalty saved")
                    pts_g=event_points_for(pos_g,"Goal")
                    scorer_team=PLAYER_TEAM_MAP.get(g_pid)
                    if scorer_team==team_h: local_h+=1
                    elif scorer_team==team_a: local_a+=1
                    m=format_minute_str(m_min,fid,fixture_max_minute)
                    update_msgs.append(f"{header_line()}\n{m} Update: Penalty saved removed - {PLAYER_NAME_MAP.get(s_pid)} {format_points(-pts_s)}, total ({tot_s}).\nPenalty missed removed - {PLAYER_NAME_MAP.get(m_pid)} {format_points(-pts_m)}, total ({tot_m}).\nGoal - {PLAYER_NAME_MAP.get(g_pid)} {format_points(pts_g)}, total ({tot_g}).")
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
                        pos_p,m_p,tot_p=get_state(pid)
                        pts=event_points_for(pos_p,base_event)
                        m=format_minute_str(m_p,fid,fixture_max_minute)
                        update_msgs.append(f"{header_line()}\n{m} {label} - {PLAYER_NAME_MAP.get(pid)} {format_points(-pts)}, total ({tot_p}).")
                        neg_pool[fid][ident].remove(pid)
                for pid in list(PLAYER_TEAM_MAP.keys()):
                    if (fid,pid) in player_state and ns_key(gw,fid,pid) in _dc_awarded:
                        pos_d,m_d,tot_d=get_state(pid)
                        stats_overall=live_by_id.get(pid,{}).get("stats",{}) or {}
                        if not (dc_threshold_met(stats_overall,pos_d) or has_any_dc(stats_overall)) and ns_key(gw,fid,pid) not in _dc_removed_sent:
                            _dc_removed_sent.add(ns_key(gw,fid,pid))
                            pts=event_points_for(pos_d,"DC")
                            m=format_minute_str(m_d,fid,fixture_max_minute)
                            update_msgs.append(f"{header_line()}\n{m} Update: DC removed - {PLAYER_NAME_MAP.get(pid)} {format_points(-pts)}, total ({tot_d}).")
                for um in update_msgs:
                    await send_once(um, season, gw)
                pos_events=pos_pool.get(fid,{})
                goals=list(pos_events.get("goals_scored",[]))
                assists=list(pos_events.get("assists",[]))
                while goals:
                    gid=goals.pop(0)
                    pos_g,m_g,tot_g=get_state(gid)
                    pts_g=event_points_for(pos_g,"Goal")
                    scorer_team=PLAYER_TEAM_MAP.get(gid)
                    if scorer_team==team_h: local_h+=1
                    elif scorer_team==team_a: local_a+=1
                    a_pid=None
                    for i,aid in enumerate(assists):
                        if aid!=gid and PLAYER_TEAM_MAP.get(aid)==scorer_team:
                            a_pid=aid; assists.pop(i); break
                    m=format_minute_str(m_g,fid,fixture_max_minute)
                    if a_pid:
                        pos_a,_ma,tot_a=get_state(a_pid)
                        pts_a=event_points_for(pos_a,"Assist")
                        await send_once(f"{header_line()}\n{m} Goal - {PLAYER_NAME_MAP.get(gid)} {format_points(pts_g)}, total ({tot_g}).\nAssist - {PLAYER_NAME_MAP.get(a_pid)} {format_points(pts_a)}, total ({tot_a}).", season, gw)
                    else:
                        await send_once(f"{header_line()}\n{m} Goal - {PLAYER_NAME_MAP.get(gid)} {format_points(pts_g)}, total ({tot_g}).", season, gw)
                own_goals=list(pos_events.get("own_goals",[]))
                while own_goals:
                    ogid=own_goals.pop(0)
                    pos_og,m_og,tot_og=get_state(ogid)
                    pts_og=event_points_for(pos_og,"Own goal")
                    og_team=PLAYER_TEAM_MAP.get(ogid)
                    if og_team==team_h: local_a+=1
                    elif og_team==team_a: local_h+=1
                    a_pid=None
                    for i,aid in enumerate(assists):
                        if PLAYER_TEAM_MAP.get(aid)!=og_team:
                            a_pid=aid; assists.pop(i); break
                    m=format_minute_str(m_og,fid,fixture_max_minute)
                    if a_pid:
                        pos_a,_ma,tot_a=get_state(a_pid)
                        pts_a=event_points_for(pos_a,"Assist")
                        await send_once(f"{header_line()}\n{m} Own goal - {PLAYER_NAME_MAP.get(ogid)} {format_points(pts_og)}, total ({tot_og}).\nAssist - {PLAYER_NAME_MAP.get(a_pid)} {format_points(pts_a)}, total ({tot_a}).", season, gw)
                    else:
                        await send_once(f"{header_line()}\n{m} Own goal - {PLAYER_NAME_MAP.get(ogid)} {format_points(pts_og)}, total ({tot_og}).", season, gw)
                pens_missed=list(pos_events.get("penalties_missed",[]))
                pens_saved=list(pos_events.get("penalties_saved",[]))
                while pens_missed and pens_saved:
                    pm=pens_missed.pop(0); ps=pens_saved.pop(0)
                    pos_pm,m_pm,tot_pm=get_state(pm)
                    pos_ps,m_ps,tot_ps=get_state(ps)
                    pts_pm=event_points_for(pos_pm,"Penalty missed")
                    pts_ps=event_points_for(pos_ps,"Penalty saved")
                    m=format_minute_str(m_pm,fid,fixture_max_minute)
                    await send_once(f"{header_line()}\n{m} Penalty missed - {PLAYER_NAME_MAP.get(pm)} {format_points(pts_pm)}, total ({tot_pm}).\n{m} Penalty saved - {PLAYER_NAME_MAP.get(ps)} {format_points(pts_ps)}, total ({tot_ps}).", season, gw)
                for pm in pens_missed:
                    pos_pm,m_pm,tot_pm=get_state(pm)
                    pts_pm=event_points_for(pos_pm,"Penalty missed")
                    m=format_minute_str(m_pm,fid,fixture_max_minute)
                    await send_once(f"{header_line()}\n{m} Penalty missed - {PLAYER_NAME_MAP.get(pm)} {format_points(pts_pm)}, total ({tot_pm}).", season, gw)
                for ps in pens_saved:
                    pos_ps,m_ps,tot_ps=get_state(ps)
                    pts_ps=event_points_for(pos_ps,"Penalty saved")
                    m=format_minute_str(m_ps,fid,fixture_max_minute)
                    await send_once(f"{header_line()}\n{m} Penalty saved - {PLAYER_NAME_MAP.get(ps)} {format_points(pts_ps)}, total ({tot_ps}).", season, gw)
                yellow_totals={}
                for (kgw,kfid,kpid,ident), val in _last_counts.items():
                    if kgw==gw and kfid==fid and ident=="yellow_cards":
                        yellow_totals[kpid]=val
                reds=list(pos_events.get("red_cards",[]))
                for pid_r in reds:
                    pos_r,m_r,tot_r=get_state(pid_r)
                    m=format_minute_str(m_r,fid,fixture_max_minute)
                    key=ns_key(gw,fid,pid_r)
                    yc_total=yellow_totals.get(pid_r,0)
                    if yc_total>=2:
                        if key not in _second_yellow_processed:
                            _second_yellow_processed.add(key)
                            pts=event_points_for(pos_r,"Second yellow → red")
                            await send_once(f"{header_line()}\n{m} Second yellow → red - {PLAYER_NAME_MAP.get(pid_r)} {format_points(pts)}, total ({tot_r}).", season, gw)
                    else:
                        if key not in _red_card_processed:
                            _red_card_processed.add(key)
                            pts=event_points_for(pos_r,"Red card")
                            await send_once(f"{header_line()}\n{m} Red card - {PLAYER_NAME_MAP.get(pid_r)} {format_points(pts)}, total ({tot_r}).", season, gw)
                for (pfid,pid),(pos_c,m_c,tot_c) in list(player_state.items()):
                    if pfid!=fid or pos_c==4: continue
                    if fx.get("finished"): continue
                    if m_c<60: continue
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
                    maxm=fixture_max_minute.get(fid,m_c)
                    if m_c>=maxm: continue
                    key=ns_key(gw,fid,pid)
                    if key in _cs_subbed_sent: continue
                    pts=event_points_for(pos_c,"Clean sheet")
                    if pts<=0: continue
                    _cs_subbed_sent.add(key)
                    m=format_minute_str(m_c,fid,fixture_max_minute)
                    await send_once(f"{header_line()}\n{m} Clean sheet (subbed off) - {PLAYER_NAME_MAP.get(pid)} {format_points(pts)}, total ({tot_c}).", season, gw)
                for (pfid,pid),(pos_d,m_d,tot_d) in list(player_state.items()):
                    if pfid!=fid: continue
                    if pos_d==1: continue
                    key=ns_key(gw,fid,pid)
                    if key in _dc_awarded: continue
                    el=live_by_id.get(pid,{})
                    stats_overall=el.get("stats",{}) or {}
                    if dc_threshold_met(stats_overall,pos_d) or has_any_dc(stats_overall):
                        _dc_awarded.add(key)
                        pts=event_points_for(pos_d,"DC")
                        m=format_minute_str(m_d,fid,fixture_max_minute)
                        await send_once(f"{header_line()}\n{m} DC - {PLAYER_NAME_MAP.get(pid)} {format_points(pts)}, total ({tot_d}).", season, gw)
                    else:
                        total_pts=int(stats_overall.get("total_points",0) or 0)
                        base=compute_base_points(stats_overall,pos_d)
                        diff=total_pts-base
                        if diff>=2:
                            _dc_awarded.add(key)
                            pts=2
                            m=format_minute_str(m_d,fid,fixture_max_minute)
                            await send_once(f"{header_line()}\n{m} DC (heuristic) - {PLAYER_NAME_MAP.get(pid)} {format_points(pts)}, total ({total_pts}).", season, gw)
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
                    lines=[header]
                    for i,(nm,pts) in enumerate(rows,1):
                        lines.append(f"{i}. {nm}: {pts}")
                    await send_once("\n".join(lines), season, gw)
                _fixture_summary_sent.add(sum_key)
            await asyncio.sleep(LIVE_POLL_INTERVAL)
        except Exception as e:
            logger.warning("Live loop error: %s", e)
            await asyncio.sleep(LIVE_POLL_INTERVAL)

async def deadline_notifier():
    while not stop_event.is_set():
        try:
            bs=await get_bootstrap_cached()
            if not bs:
                await asyncio.sleep(300); continue
            events=bs.get("events",[])
            now=datetime.now(timezone.utc)
            future=[]
            for e in events:
                dt=parse_deadline(e.get("deadline_time"))
                if dt and dt>now: future.append((dt,e))
            if not future:
                await asyncio.sleep(1800); continue
            next_deadline, ev=min(future,key=lambda x:x[0])
            ahead=(next_deadline-now).total_seconds()
            if 0<ahead<=3600:
                msg=f"Напоминание: до дедлайна GW {ev.get('id')} осталось {int(ahead//3600)}ч {int((ahead%3600)//60)}м."
                await send_text_raw(ALLOWED_GROUP_ID, msg)
                await asyncio.sleep(ahead+5)
            else:
                sleep=max(ahead-3600,600)
                await asyncio.sleep(sleep)
        except Exception as e:
            logger.warning("Deadline notifier error: %s", e)
            await asyncio.sleep(600)

async def setup_bot_commands(bot):
    cmds=[
        BotCommand("help","Описание"),
        BotCommand("deadline","Дедлайн"),
        BotCommand("players_pts","Очки игроков"),
        BotCommand("gwpoints","Очки тура"),
        BotCommand("month","Очки месяца"),
        BotCommand("rank","Таблица"),
        BotCommand("prices","Цены (owner)"),
        BotCommand("squid_start","Squid start (owner)"),
        BotCommand("squid_stop","Squid stop (owner)"),
        BotCommand("squid_status","Squid статус"),
        BotCommand("squid_winners","Squid победители"),
        BotCommand("squid_rules","Squid правила"),
        BotCommand("tz","Таймзона"),
    ]
    try: await bot.set_my_commands(cmds)
    except Exception: pass

async def error_handler(update:object, context:ContextTypes.DEFAULT_TYPE):
    logger.debug("Unhandled error: %s", context.error)

async def run_bot():
    global http_client, application
    init_redis()
    limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
    use_http2=ENABLE_HTTP2
    try: import h2  # noqa
    except Exception: use_http2=False
    http_client=httpx.AsyncClient(http2=use_http2, limits=limits, timeout=20.0)
    application=Application.builder().token(BOT_TOKEN).concurrent_updates(TELEGRAM_CONCURRENCY).build()
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("deadline", deadline_command))
    application.add_handler(CommandHandler("players_pts", players_pts_command))
    application.add_handler(CommandHandler("gwpoints", gwpoints_command))
    application.add_handler(CommandHandler("month", month_command))
    application.add_handler(CommandHandler("rank", rank_command))
    application.add_handler(CommandHandler("prices", prices_command))
    application.add_handler(CommandHandler("squid_start", squid_start_command))
    application.add_handler(CommandHandler("squid_stop", squid_stop_command))
    application.add_handler(CommandHandler("squid_status", squid_status_command))
    application.add_handler(CommandHandler("squid_winners", squid_winners_command))
    application.add_handler(CommandHandler("squid_rules", squid_rules_command))
    application.add_handler(CommandHandler("tz", settz_command))
    application.add_handler(CallbackQueryHandler(tz_inline_callback, pattern="^tz"))
    application.add_error_handler(error_handler)
    await application.initialize()
    await application.start()
    await setup_bot_commands(application.bot)
    # Lazy bootstrap could be background; keep sync for now
    await get_bootstrap_cached()
    load_squid_from_redis()
    if not USE_WEBHOOK:
        try:
            await application.updater.start_polling()
        except Exception as e:
            logger.error("Polling start failed: %s", e)
            return
    asyncio.create_task(live_monitor_loop())
    asyncio.create_task(deadline_notifier())
    try:
        me=await application.bot.get_me()
        logger.info(f"Bot started @{me.username}")
    except Exception: pass
    await stop_event.wait()
    try:
        if not USE_WEBHOOK:
            await application.updater.stop()
        await application.stop()
        await application.shutdown()
    except Exception: pass
    try:
        if http_client: await http_client.aclose()
    except Exception: pass

def handle_sigterm(signum, frame):
    try:
        loop=asyncio.get_event_loop()
        loop.call_soon_threadsafe(stop_event.set)
    except RuntimeError: pass

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

if __name__=="__main__":
    flask_thread=threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()
    try:
        asyncio.run(run_bot())
    except Exception as e:
        logger.error("Fatal run error: %s", e)
