# FPL Telegram Bot — live events, optimizations, idempotency, updates
# Изменения:
# - Заголовки: только название лиги (без "Положение лиги", без ID).
# - Центрирование всех таблиц (/rank, /gwinfo, /gwpoints, /month).
# - Таймзона: интерактивная настройка. Если не установлена — первый /deadline подскажет команду /settz <offset>.
# - /gwinfo и /gwpoints: в заголовке название лиги + номер GW.
# - /month: заголовок как в /gwinfo, русское имя месяца, номер месяца сезона (Авг=1..Май=10),
#           количество оставшихся туров до конца месяца, топ-10 игроков по очкам месяца.
# - /rank: отображение изменения ранга (↑, ↓, →0) относительно last_rank.
# - Аббревиатуры: PM (penalties missed), PS (penalties saved).
# - DC в /gwinfo: токен "DC" при DEF>=10 или MID/FWD>=12 (по ключам defensive_contributions/cbit/cbits/def_contributions).
# - Live: 45+N/90+N минуты, Clean sheet (subbed off) и идемпотентность сохранены.

import os
import asyncio
import threading
import logging
import time
import signal
import random
import hashlib
from typing import Any, Dict, Optional, List, Tuple, Set
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify
import httpx
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update, BotCommand

try:
    from upstash_redis import Redis
except ImportError:
    Redis = None

# ===== Config =====
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN required")

TARGET_CHAT_ID = os.environ.get("TARGET_CHAT_ID")
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

FIXTURES_TTL = int(os.environ.get("FIXTURES_TTL", "900"))
LEAGUE_PLAYERS_TTL = 300
IDEMPOTENCY_TTL_SEC = 10 * 24 * 3600

TIMEZONE_ENV = os.environ.get("TIMEZONE_OFFSET_HOURS")  # начальное значение, если нет интерактива
INITIAL_TZ_OFFSET = int(TIMEZONE_ENV) if TIMEZONE_ENV and TIMEZONE_ENV.lstrip("+-").isdigit() else None
TZ_REDIS_KEY = "fpl:tz_offset"

# ===== Logging =====
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)
logger = logging.getLogger("fpl_bot")

# ===== Flask =====
flask_app = Flask(__name__)

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
        "tz_offset": get_timezone_offset()
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
fpl_semaphore = asyncio.Semaphore(FPL_CONCURRENCY)

async def fetch_json(url: str, timeout: float = 15.0, attempts: int = 3) -> Optional[Dict]:
    for attempt in range(1, attempts + 1):
        await asyncio.sleep(random.uniform(0.03, 0.12))
        try:
            async with fpl_semaphore:
                resp = await http_client.get(url, headers=FPL_BASE_HEADERS, timeout=timeout)
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

_second_yellow_processed: Set[str] = set()
_red_card_processed: Set[str] = set()
_dc_awarded: Set[str] = set()
_dc_removed_sent: Set[str] = set()
_cs_subbed_sent: Set[str] = set()
_fixture_summary_sent: Set[str] = set()

_last_counts: Dict[Tuple[int,int,int,str], int] = {}
_current_gw: Optional[int] = None
_sent_msg_hashes: Set[str] = set()

_tz_offset_memory: Optional[int] = None  # in-memory fallback for tz

# ===== Redis =====
def init_redis():
    global redis_client
    if Redis is None:
        logger.info("Redis absent, using memory.")
        return
    try:
        redis_client = Redis.from_env()
        logger.info("Redis connected.")
    except Exception as e:
        logger.warning(f"Redis init failed: {e}")
        redis_client = None

def get_timezone_offset() -> int:
    # Priority: Redis -> memory -> INITIAL_TZ_OFFSET -> 0
    if redis_client:
        try:
            val = redis_client.get(TZ_REDIS_KEY)
            if val is not None and str(val).lstrip("+-").isdigit():
                return int(val)
        except Exception:
            pass
    if _tz_offset_memory is not None:
        return _tz_offset_memory
    if INITIAL_TZ_OFFSET is not None:
        return INITIAL_TZ_OFFSET
    return 0

def set_timezone_offset(offset: int):
    global _tz_offset_memory
    _tz_offset_memory = offset
    if redis_client:
        try:
            redis_client.set(TZ_REDIS_KEY, str(offset))
        except Exception:
            logger.warning("Redis set tz_offset failed")

# ===== Utilities =====
def parse_deadline(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt_str = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None

def bootstrap_valid() -> bool:
    return bootstrap_cache_ts is not None and (time.time() - bootstrap_cache_ts) / 60.0 < FPL_CACHE_TTL_MIN

async def get_bootstrap_cached() -> Optional[Dict]:
    async with bootstrap_lock:
        if bootstrap_valid():
            return bootstrap_cache
    data = await fetch_json(fpl_url(bootstrap_path))
    if data:
        async with bootstrap_lock:
            bootstrap_cache.clear()
            bootstrap_cache.update(data)
            global bootstrap_cache_ts
            bootstrap_cache_ts = time.time()
        refresh_bootstrap_maps()
        return data
    return None

def fixtures_cache_valid(ts: float) -> bool:
    return (time.time() - ts) < FIXTURES_TTL

async def get_fixtures_cached(gw: int) -> List[Dict]:
    st = _fixtures_cache.get(gw)
    if st and fixtures_cache_valid(st[0]):
        return st[1]
    data = await fetch_json(fpl_url(fixtures_event_path_tpl.format(gw=gw))) or []
    _fixtures_cache[gw] = (time.time(), data)
    return data

def standings_valid() -> bool:
    return standings_cache_ts is not None and (time.time() - standings_cache_ts) <= FPL_STANDINGS_TTL

async def get_league_results_cached(league_id: str) -> Optional[List[Dict]]:
    if standings_valid() and "results" in standings_cache:
        # обновим имя лиги, если есть
        global LEAGUE_NAME
        if "league_name" in standings_cache and standings_cache["league_name"]:
            LEAGUE_NAME = standings_cache["league_name"]
        return standings_cache.get("results")
    async with standings_lock:
        if standings_valid() and "results" in standings_cache:
            if "league_name" in standings_cache and standings_cache["league_name"]:
                global LEAGUE_NAME
                LEAGUE_NAME = standings_cache["league_name"]
            return standings_cache.get("results")
        all_results: List[Dict] = []
        page = 1
        league_name_local: Optional[str] = None
        while True:
            url = fpl_url(league_path_tpl.format(league_id=league_id, page=page))
            data = await fetch_json(url)
            if not data:
                break
            try:
                standings = data["standings"]
                if league_name_local is None:
                    league_name_local = standings.get("league", {}).get("name")
                results = standings["results"]
                all_results.extend(results)
                if not standings.get("has_next"):
                    break
                page += 1
                if page > 30:
                    break
            except Exception as ex:
                logger.warning(f"Standings decode error: {ex}")
                break
        if all_results:
            standings_cache.clear()
            standings_cache.update({"results": all_results, "pages": page, "league_name": league_name_local})
            global standings_cache_ts, LEAGUE_NAME
            standings_cache_ts = time.time()
            if league_name_local:
                LEAGUE_NAME = league_name_local
        return all_results if all_results else None

def picks_valid(ts: Optional[float]) -> bool:
    return ts is not None and (time.time() - ts) <= FPL_PICKS_TTL

async def _picks_lock(key: Tuple[int, int]) -> asyncio.Lock:
    async with _picks_locks_guard:
        lk = _picks_locks.get(key)
        if not lk:
            lk = asyncio.Lock()
            _picks_locks[key] = lk
        return lk

async def get_entry_picks_cached(entry: int, gw: int) -> Optional[Dict]:
    key = (entry, gw)
    cached = picks_cache.get(key)
    if cached and picks_valid(cached.get("ts")):
        return cached.get("data")
    lock = await _picks_lock(key)
    async with lock:
        c2 = picks_cache.get(key)
        if c2 and picks_valid(c2.get("ts")):
            return c2.get("data")
        data = await fetch_json(fpl_url(entry_picks_path_tpl.format(entry_id=entry, gw=gw)))
        if data:
            picks_cache[key] = {"data": data, "ts": time.time()}
            return data
        if FPL_PICKS_ALLOW_STALE and c2 and c2.get("data"):
            logger.warning(f"Serve STALE picks entry={entry} gw={gw}")
            return c2.get("data")
        return None

def discover_season_tag(bootstrap: Dict[str, Any]) -> str:
    global SEASON_TAG
    if SEASON_TAG:
        return SEASON_TAG
    season = bootstrap.get("game_settings", {}).get("season")
    if not season:
        y = datetime.now().year
        season = f"{y}/{str((y+1) % 100).zfill(2)}"
    SEASON_TAG = season
    return SEASON_TAG

def refresh_bootstrap_maps():
    global PLAYER_NAME_MAP, PLAYER_POS_MAP, PLAYER_TEAM_MAP, TEAM_SHORT_MAP
    elements = bootstrap_cache.get("elements", [])
    teams = bootstrap_cache.get("teams", [])
    PLAYER_NAME_MAP = {el.get("id"): (el.get("web_name") or el.get("second_name") or f"P{el.get('id')}") for el in elements if isinstance(el.get("id"), int)}
    PLAYER_POS_MAP = {el.get("id"): el.get("element_type") for el in elements if isinstance(el.get("id"), int) and isinstance(el.get("element_type"), int)}
    PLAYER_TEAM_MAP = {el.get("id"): el.get("team") for el in elements if isinstance(el.get("id"), int) and isinstance(el.get("team"), int)}
    TEAM_SHORT_MAP = {t.get("id"): (t.get("short_name") or t.get("name") or f"T{t.get('id')}") for t in teams if isinstance(t.get("id"), int)}

async def get_league_player_ids_cached(gw: int) -> Set[int]:
    now = time.time()
    cached = _league_players_cache.get(gw)
    if cached and now - cached[0] <= LEAGUE_PLAYERS_TTL:
        return cached[1]
    results = await get_league_results_cached(LEAGUE_ID)
    if not results:
        _league_players_cache[gw] = (now, set())
        return set()
    tasks = []
    for r in results:
        eid = r.get("entry")
        if isinstance(eid, int):
            tasks.append(asyncio.create_task(get_entry_picks_cached(eid, gw)))
    data_all = await asyncio.gather(*tasks, return_exceptions=True)
    players: Set[int] = set()
    for pkg in data_all:
        if isinstance(pkg, dict):
            for p in pkg.get("picks", []):
                elid = p.get("element")
                if isinstance(elid, int):
                    players.add(elid)
    _league_players_cache[gw] = (now, players)
    return players

def get_active_fixture_ids(fixtures: List[Dict], now: Optional[datetime] = None) -> Set[int]:
    now = now or datetime.now(timezone.utc)
    active: Set[int] = set()
    for fx in fixtures:
        fid = fx.get("id")
        if not isinstance(fid, int):
            continue
        if fx.get("finished"):
            continue
        kt = fx.get("kickoff_time")
        dt = parse_deadline(kt) if isinstance(kt, str) else None
        if dt and dt <= now:
            active.add(fid)
    return active

def format_points(delta: int) -> str:
    return f"(+{delta})" if delta > 0 else f"({delta})"

def total_no_bonus(stats: Dict[str, Any]) -> int:
    return int(stats.get("total_points", 0) or 0) - int(stats.get("bonus", 0) or 0)

def event_points_for(pos: int, etype: str) -> int:
    if etype == "Goal":
        if pos in (1,2): return 6
        if pos == 3: return 5
        if pos == 4: return 4
    if etype == "Assist": return 3
    if etype == "Own goal": return -2
    if etype == "Penalty missed": return -2
    if etype == "Penalty saved": return 5
    if etype == "Second yellow → red": return -3
    if etype == "Red card": return -3
    if etype == "Clean sheet":
        if pos in (1,2): return 4
        if pos == 3: return 1
        return 0
    if etype == "DC": return 2
    return 0

def dc_threshold_met(stats: Dict[str, Any], pos: int) -> bool:
    if pos == 1:
        return False
    dc = 0
    for k in ("defensive_contributions","cbit","cbits","def_contributions"):
        v = stats.get(k)
        if isinstance(v,(int,float)):
            dc = int(v); break
    if pos == 2: return dc >= 10
    if pos in (3,4): return dc >= 12
    return False

def ns_key(gw: int, fid: int, pid: int) -> str:
    return f"{gw}:{fid}:{pid}"

def message_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def idempotency_set_key(season: str, gw: int) -> str:
    return f"live:msgs:{season}:{gw}"

def split_message_chunks(text: str, limit: int = 4000) -> List[str]:
    if len(text) <= limit: return [text]
    lines = text.splitlines(keepends=True)
    out=[]; buf=""
    for ln in lines:
        if len(buf)+len(ln) > limit:
            out.append(buf); buf=""
        buf += ln
    if buf: out.append(buf)
    return out

async def send_text_raw(chat_id: int, text: str):
    for chunk in split_message_chunks(text):
        try:
            await application.bot.send_message(chat_id=chat_id, text=chunk)
            await asyncio.sleep(0.05)
        except Exception:
            logger.exception("send_message failed")

async def send_once(text: str, season: str, gw: int):
    key = idempotency_set_key(season, gw)
    h = message_hash(text)
    should = True
    if redis_client:
        try:
            added = redis_client.sadd(key, h)
            should = (added == 1 or added == "1")
            redis_client.expire(key, IDEMPOTENCY_TTL_SEC)
        except Exception:
            should = h not in _sent_msg_hashes
    else:
        should = h not in _sent_msg_hashes
    if should:
        if not redis_client:
            _sent_msg_hashes.add(h)
        chat_id = None
        if TARGET_CHAT_ID and TARGET_CHAT_ID.isdigit():
            chat_id = int(TARGET_CHAT_ID)
        if chat_id:
            await send_text_raw(chat_id, text)
        else:
            logger.info("LIVE:\n"+text)

def extract_minutes_for_fixture(el: Dict, fid: int) -> int:
    stats_overall = el.get("stats", {}) or {}
    m = stats_overall.get("minutes")
    return int(m) if isinstance(m, int) else 0

def format_minute_str(minute: int, fid: int, fixture_max_minute: Dict[int, int]) -> str:
    if minute <= 0:
        return "0'"
    if minute > 90:
        return f"90+{minute - 90}'"
    if minute > 45:
        maxm = fixture_max_minute.get(fid, minute)
        if maxm <= 60:
            return f"45+{minute - 45}'"
    return f"{minute}'"

# ===== Live Loop =====
async def live_monitor_loop():
    global _current_gw
    logger.info("Live loop started")
    while not stop_event.is_set():
        try:
            bs = await get_bootstrap_cached()
            if not bs:
                await asyncio.sleep(LIVE_POLL_INTERVAL); continue
            season = discover_season_tag(bs)
            current_ev = next((e for e in bs.get("events", []) if e.get("is_current")), None)
            if not current_ev or current_ev.get("finished"):
                await asyncio.sleep(LIVE_POLL_INTERVAL); continue
            gw = current_ev.get("id")
            if not isinstance(gw, int):
                await asyncio.sleep(LIVE_POLL_INTERVAL); continue

            if _current_gw is None:
                _current_gw = gw
            elif _current_gw != gw:
                _second_yellow_processed.clear()
                _red_card_processed.clear()
                _dc_awarded.clear()
                _dc_removed_sent.clear()
                _cs_subbed_sent.clear()
                _fixture_summary_sent.clear()
                _last_counts.clear()
                _current_gw = gw
                logger.info(f"GW changed -> reset state to GW {gw}")

            fixtures = await get_fixtures_cached(gw)
            active_fids = get_active_fixture_ids(fixtures)
            if not active_fids:
                await asyncio.sleep(min(300, LIVE_POLL_INTERVAL*2))
                continue

            live = await fetch_json(fpl_url(event_live_path_tpl.format(gw=gw)))
            if not live:
                await asyncio.sleep(LIVE_POLL_INTERVAL); continue
            live_elements = live.get("elements", [])
            live_by_id = {el.get("id"): el for el in live_elements if isinstance(el.get("id"), int)}

            league_players = await get_league_player_ids_cached(gw)
            fixture_index = {fx.get("id"): fx for fx in fixtures if isinstance(fx.get("id"), int)}

            pos_pool: Dict[int, Dict[str, List[int]]] = {}
            neg_pool: Dict[int, Dict[str, List[int]]] = {}
            player_state: Dict[Tuple[int,int], Tuple[int,int,int]] = {}
            fixture_max_minute: Dict[int, int] = {}

            for pid, el in live_by_id.items():
                if pid not in league_players:
                    continue
                stats_overall = el.get("stats", {}) or {}
                explain = el.get("explain", []) or []
                pos = PLAYER_POS_MAP.get(pid, 0)
                total_nb = total_no_bonus(stats_overall)
                for blk in explain:
                    fid = blk.get("fixture")
                    if not isinstance(fid, int) or fid not in active_fids:
                        continue
                    minute = extract_minutes_for_fixture(el, fid)
                    player_state[(fid,pid)] = (pos, minute, total_nb)
                    if minute > fixture_max_minute.get(fid, 0):
                        fixture_max_minute[fid] = minute
                    for st in blk.get("stats", []):
                        ident = st.get("identifier"); value = st.get("value")
                        if not isinstance(value,int): continue
                        key = (gw,fid,pid,ident)
                        prev = _last_counts.get(key, 0)
                        delta = value - prev
                        if delta != 0:
                            target = pos_pool if delta>0 else neg_pool
                            target.setdefault(fid, {}).setdefault(ident, [])
                            target[fid][ident].extend([pid]*abs(delta))
                        _last_counts[key] = value

            for fid in sorted(set(list(pos_pool.keys())+list(neg_pool.keys()))):
                fx = fixture_index.get(fid, {})
                if not fx: continue
                base_h = fx.get("team_h_score") or 0
                base_a = fx.get("team_a_score") or 0
                team_h = fx.get("team_h")
                team_a = fx.get("team_a")
                local_h = base_h
                local_a = base_a

                def header_line() -> str:
                    return f"{TEAM_SHORT_MAP.get(team_h,'T?')} {local_h}-{local_a} {TEAM_SHORT_MAP.get(team_a,'T?')}"

                def get_state(pid: int) -> Tuple[int,int,int]:
                    return player_state.get((fid,pid), (PLAYER_POS_MAP.get(pid,0), 0, 0))

                # Updates
                update_msgs: List[str] = []
                if "assists" in neg_pool.get(fid, {}) and "assists" in pos_pool.get(fid, {}):
                    while neg_pool[fid]["assists"] and pos_pool[fid]["assists"]:
                        from_pid = neg_pool[fid]["assists"].pop(0)
                        to_pid = pos_pool[fid]["assists"].pop(0)
                        pos_from, minutes_from, total_from = get_state(from_pid)
                        pos_to, _m_to, total_to = get_state(to_pid)
                        pts = event_points_for(pos_from,"Assist")
                        pts_to = event_points_for(pos_to,"Assist")
                        mstr = format_minute_str(minutes_from, fid, fixture_max_minute)
                        msg = (f"{header_line()}\n"
                               f"{mstr} Update: Assist reassigned - {PLAYER_NAME_MAP.get(from_pid,f'P{from_pid}')} {format_points(-pts)}, total ({total_from}).\n"
                               f"Assist - {PLAYER_NAME_MAP.get(to_pid,f'P{to_pid}')} {format_points(pts_to)}, total ({total_to}).")
                        update_msgs.append(msg)

                while neg_pool.get(fid,{}).get("penalties_missed") and neg_pool.get(fid,{}).get("penalties_saved") and pos_pool.get(fid,{}).get("goals_scored"):
                    m_pid = neg_pool[fid]["penalties_missed"].pop(0)
                    s_pid = neg_pool[fid]["penalties_saved"].pop(0)
                    g_pid = pos_pool[fid]["goals_scored"].pop(0)
                    pos_m, minutes_m, total_m = get_state(m_pid)
                    pos_s, _m_s, total_s = get_state(s_pid)
                    pos_g, _m_g, total_g = get_state(g_pid)
                    pts_m = event_points_for(pos_m,"Penalty missed")
                    pts_s = event_points_for(pos_s,"Penalty saved")
                    pts_g = event_points_for(pos_g,"Goal")
                    scorer_team = PLAYER_TEAM_MAP.get(g_pid)
                    if scorer_team == team_h: local_h += 1
                    elif scorer_team == team_a: local_a += 1
                    mstr = format_minute_str(minutes_m, fid, fixture_max_minute)
                    msg = (f"{header_line()}\n"
                           f"{mstr} Update: Penalty saved removed - {PLAYER_NAME_MAP.get(s_pid,f'P{s_pid}')} {format_points(-pts_s)}, total ({total_s}).\n"
                           f"Penalty missed removed - {PLAYER_NAME_MAP.get(m_pid,f'P{m_pid}')} {format_points(-pts_m)}, total ({total_m}).\n"
                           f"Goal - {PLAYER_NAME_MAP.get(g_pid,f'P{g_pid}')} {format_points(pts_g)}, total ({total_g}).")
                    update_msgs.append(msg)

                for ident, label, base_event in [
                    ("goals_scored","Update: Goal cancelled (VAR)","Goal"),
                    ("assists","Update: Assist cancelled","Assist"),
                    ("own_goals","Update: Own goal removed","Own goal"),
                    ("penalties_missed","Update: Penalty missed removed","Penalty missed"),
                    ("penalties_saved","Update: Penalty saved removed","Penalty saved"),
                    ("red_cards","Update: Red card cancelled","Red card"),
                    ("clean_sheets","Update: Clean sheet removed","Clean sheet"),
                ]:
                    for pid in list(neg_pool.get(fid, {}).get(ident, [])):
                        pos_p, minutes_p, total_p = get_state(pid)
                        pts = event_points_for(pos_p, base_event)
                        mstr = format_minute_str(minutes_p, fid, fixture_max_minute)
                        msg = (f"{header_line()}\n"
                               f"{mstr} {label} - {PLAYER_NAME_MAP.get(pid,f'P{pid}')} {format_points(-pts)}, total ({total_p}).")
                        update_msgs.append(msg)
                        neg_pool[fid][ident].remove(pid)

                for um in update_msgs:
                    await send_once(um, season, gw)

                pos_events = pos_pool.get(fid, {})
                goals = list(pos_events.get("goals_scored", []))
                assists = list(pos_events.get("assists", []))
                while goals:
                    gid = goals.pop(0)
                    pos_g, minutes_g, total_g = get_state(gid)
                    pts_g = event_points_for(pos_g,"Goal")
                    scorer_team = PLAYER_TEAM_MAP.get(gid)
                    if scorer_team == team_h:
                        local_h += 1
                    elif scorer_team == team_a:
                        local_a += 1
                    a_pid = None
                    for i, aid in enumerate(assists):
                        if aid != gid and PLAYER_TEAM_MAP.get(aid) == scorer_team:
                            a_pid = aid
                            assists.pop(i)
                            break
                    header = header_line()
                    mstr = format_minute_str(minutes_g, fid, fixture_max_minute)
                    line1 = f"{mstr} Goal - {PLAYER_NAME_MAP.get(gid,f'P{gid}')} {format_points(pts_g)}, total ({total_g})."
                    if a_pid:
                        pos_a, _m_a, total_a = get_state(a_pid)
                        pts_a = event_points_for(pos_a,"Assist")
                        line2 = f"Assist - {PLAYER_NAME_MAP.get(a_pid,f'P{a_pid}')} {format_points(pts_a)}, total ({total_a})."
                        msg = f"{header}\n{line1}\n{line2}"
                    else:
                        msg = f"{header}\n{line1}"
                    await send_once(msg, season, gw)

                own_goals = list(pos_events.get("own_goals", []))
                while own_goals:
                    ogid = own_goals.pop(0)
                    pos_og, minutes_og, total_og = get_state(ogid)
                    pts_og = event_points_for(pos_og,"Own goal")
                    og_team = PLAYER_TEAM_MAP.get(ogid)
                    if og_team == team_h:
                        local_a += 1
                    elif og_team == team_a:
                        local_h += 1
                    a_pid = None
                    for i, aid in enumerate(assists):
                        if PLAYER_TEAM_MAP.get(aid) != og_team:
                            a_pid = aid
                            assists.pop(i)
                            break
                    header = header_line()
                    mstr = format_minute_str(minutes_og, fid, fixture_max_minute)
                    line1 = f"{mstr} Own goal - {PLAYER_NAME_MAP.get(ogid,f'P{ogid}')} {format_points(pts_og)}, total ({total_og})."
                    if a_pid:
                        pos_a, _m_a, total_a = get_state(a_pid)
                        pts_a = event_points_for(pos_a,"Assist")
                        line2 = f"Assist - {PLAYER_NAME_MAP.get(a_pid,f'P{a_pid}')} {format_points(pts_a)}, total ({total_a})."
                        msg = f"{header}\n{line1}\n{line2}"
                    else:
                        msg = f"{header}\n{line1}"
                    await send_once(msg, season, gw)

                pens_missed = list(pos_events.get("penalties_missed", []))
                pens_saved = list(pos_events.get("penalties_saved", []))
                while pens_missed and pens_saved:
                    pm = pens_missed.pop(0)
                    ps = pens_saved.pop(0)
                    pos_m, minutes_m, total_m = get_state(pm)
                    pos_s, _m_s, total_s = get_state(ps)
                    pts_m = event_points_for(pos_m,"Penalty missed")
                    pts_s = event_points_for(pos_s,"Penalty saved")
                    header = header_line()
                    mstr = format_minute_str(minutes_m, fid, fixture_max_minute)
                    line1 = f"{mstr} Penalty missed - {PLAYER_NAME_MAP.get(pm,f'P{pm}')} {format_points(pts_m)}, total ({total_m})."
                    line2 = f"Penalty saved - {PLAYER_NAME_MAP.get(ps,f'P{ps}')} {format_points(pts_s)}, total ({total_s})."
                    await send_once(f"{header}\n{line1}\n{line2}", season, gw)

                for pm in pens_missed:
                    pos_m, minutes_m, total_m = get_state(pm)
                    pts_m = event_points_for(pos_m,"Penalty missed")
                    header = header_line()
                    mstr = format_minute_str(minutes_m, fid, fixture_max_minute)
                    await send_once(f"{header}\n{mstr} Penalty missed - {PLAYER_NAME_MAP.get(pm,f'P{pm}')} {format_points(pts_m)}, total ({total_m}).", season, gw)

                for ps in pens_saved:
                    pos_s, minutes_s, total_s = get_state(ps)
                    pts_s = event_points_for(pos_s,"Penalty saved")
                    header = header_line()
                    mstr = format_minute_str(minutes_s, fid, fixture_max_minute)
                    await send_once(f"{header}\n{mstr} Penalty saved - {PLAYER_NAME_MAP.get(ps,f'P{ps}')} {format_points(pts_s)}, total ({total_s}).", season, gw)

                yellow_totals: Dict[int,int] = {}
                for (kgw,kfid,kpid,ident), val in _last_counts.items():
                    if kgw==gw and kfid==fid and ident=="yellow_cards":
                        yellow_totals[kpid]=val
                reds = list(pos_events.get("red_cards", []))
                for pid_r in reds:
                    pos_r, minutes_r, total_r = get_state(pid_r)
                    key = ns_key(gw,fid,pid_r)
                    yc_total = yellow_totals.get(pid_r, 0)
                    header = header_line()
                    mstr = format_minute_str(minutes_r, fid, fixture_max_minute)
                    if yc_total >= 2:
                        if key not in _second_yellow_processed:
                            _second_yellow_processed.add(key)
                            pts = event_points_for(pos_r,"Second yellow → red")
                            await send_once(f"{header}\n{mstr} Second yellow → red - {PLAYER_NAME_MAP.get(pid_r,f'P{pid_r}')} {format_points(pts)}, total ({total_r}).", season, gw)
                    else:
                        if key not in _red_card_processed:
                            _red_card_processed.add(key)
                            pts = event_points_for(pos_r,"Red card")
                            await send_once(f"{header}\n{mstr} Red card - {PLAYER_NAME_MAP.get(pid_r,f'P{pid_r}')} {format_points(pts)}, total ({total_r}).", season, gw)

                for (pfid,pid), (pos_c, minutes_c, total_c) in list(player_state.items()):
                    if pfid != fid: continue
                    if pos_c == 4: continue
                    if fx.get("finished"):
                        continue
                    if minutes_c < 60:
                        continue
                    team_id = PLAYER_TEAM_MAP.get(pid)
                    if not team_id: continue
                    conceded = 0
                    if team_id == team_h: conceded = fx.get("team_a_score") or 0
                    elif team_id == team_a: conceded = fx.get("team_h_score") or 0
                    if conceded != 0: continue
                    cs_total = _last_counts.get((gw,fid,pid,"clean_sheets"),0)
                    if cs_total <= 0:
                        stats_overall = live_by_id.get(pid,{}).get("stats",{}) or {}
                        cs_total = int(stats_overall.get("clean_sheets",0) or 0)
                        _last_counts[(gw,fid,pid,"clean_sheets")] = cs_total
                    if cs_total <= 0: continue
                    maxm = fixture_max_minute.get(fid, minutes_c)
                    if minutes_c >= maxm:
                        continue
                    key = ns_key(gw,fid,pid)
                    if key in _cs_subbed_sent: continue
                    pts = event_points_for(pos_c,"Clean sheet")
                    if pts <= 0: continue
                    _cs_subbed_sent.add(key)
                    header = header_line()
                    mstr = format_minute_str(minutes_c, fid, fixture_max_minute)
                    line = f"{mstr} Clean sheet (subbed off) - {PLAYER_NAME_MAP.get(pid,f'P{pid}')} {format_points(pts)}, total ({total_c})."
                    await send_once(f"{header}\n{line}", season, gw)

                for (pfid,pid), (pos_d, minutes_d, total_d) in list(player_state.items()):
                    if pfid != fid: continue
                    key = ns_key(gw,fid,pid)
                    if key in _dc_awarded: continue
                    el = live_by_id.get(pid,{})
                    stats_overall = el.get("stats",{}) or {}
                    if dc_threshold_met(stats_overall,pos_d):
                        _dc_awarded.add(key)
                        pts = event_points_for(pos_d,"DC")
                        header = header_line()
                        mstr = format_minute_str(minutes_d, fid, fixture_max_minute)
                        line = f"{mstr} DC - {PLAYER_NAME_MAP.get(pid,f'P{pid}')} {format_points(pts)}, total ({total_d})."
                        await send_once(f"{header}\n{line}", season, gw)

            finished = [fx for fx in fixtures if fx.get("finished")]
            for fx in finished:
                fid = fx.get("id")
                if not isinstance(fid,int): continue
                sum_key = f"{gw}:{fid}"
                if sum_key in _fixture_summary_sent: continue
                team_h = fx.get("team_h"); team_a = fx.get("team_a")
                hs = fx.get("team_h_score") or 0
                as_ = fx.get("team_a_score") or 0
                header = f"{TEAM_SHORT_MAP.get(team_h,'T?')} {hs}-{as_} {TEAM_SHORT_MAP.get(team_a,'T?')} — Final player points with bonus"
                rows: List[Tuple[str,int]] = []
                for pid, el in live_by_id.items():
                    if pid not in league_players: continue
                    if any(b.get("fixture")==fid for b in (el.get("explain") or [])):
                        stats = el.get("stats",{}) or {}
                        total_pts = int(stats.get("total_points",0) or 0)
                        rows.append((PLAYER_NAME_MAP.get(pid,f'P{pid}'), total_pts))
                if rows:
                    rows.sort(key=lambda x:(-x[1], x[0].lower()))
                    width = max(len(header), max((len(f"{i}. {nm}: {pts} pts") for i,(nm,pts) in enumerate(rows,1)), default=0))
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

# ===== Season month helper =====
RUS_MONTH = {
    1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",
    7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"
}
SEASON_MONTH_ORDER = [8,9,10,11,12,1,2,3,4,5]  # Авг..Май

def season_month_index(month_num: int) -> Optional[int]:
    try:
        return SEASON_MONTH_ORDER.index(month_num) + 1
    except ValueError:
        return None

async def month_points(entries: List[Dict], events: List[Dict]) -> Tuple[List[Tuple[str,str,int]], int]:
    tz_offset = get_timezone_offset()
    now_local = datetime.now(timezone.utc) + timedelta(hours=tz_offset)
    month = now_local.month
    year = now_local.year
    month_event_ids: List[int] = []
    remaining = 0
    for ev in events:
        dt = parse_deadline(ev.get("deadline_time"))
        if not dt: continue
        local_dt = dt + timedelta(hours=tz_offset)
        if local_dt.month == month and local_dt.year == year:
            month_event_ids.append(ev.get("id"))
            if not ev.get("finished"):
                remaining += 1
    if not month_event_ids:
        return [], 0
    finished_ids = [ev.get("id") for ev in events if ev.get("finished") and ev.get("id") in month_event_ids]
    async def fetch_entry(eid: int, e_name: str, p_name: str):
        total = 0
        for gw_id in finished_ids:
            data = await get_entry_picks_cached(eid, gw_id)
            if data:
                pts = data.get("entry_history", {}).get("points")
                if isinstance(pts,int):
                    total += pts
        return (p_name, e_name, total)
    tasks=[]
    for r in entries:
        eid = r.get("entry")
        if isinstance(eid,int):
            tasks.append(asyncio.create_task(fetch_entry(eid, r.get("entry_name"), r.get("player_name"))))
    fetched = await asyncio.gather(*tasks, return_exceptions=True)
    rows=[]
    for f in fetched:
        if isinstance(f, tuple):
            rows.append(f)
    rows.sort(key=lambda x: (-x[2], x[0].lower()))
    return rows, remaining

# ===== Commands =====
async def settz_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /settz <offset>, пример: /settz +5 или /settz -2 или /settz 3")
        return
    raw = context.args[0].strip()
    if raw.startswith("+"): raw = raw[1:]
    if not raw.lstrip("-").isdigit():
        await update.message.reply_text("Offset должен быть целым числом, пример: 5 или -3.")
        return
    offset = int(raw)
    if offset < -12 or offset > 14:
        await update.message.reply_text("Диапазон смещения допустим: -12 .. +14.")
        return
    set_timezone_offset(offset)
    await update.message.reply_text(f"Таймзона установлена: UTC{offset:+d}")

async def deadline_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs = await get_bootstrap_cached()
    if not bs:
        await update.message.reply_text("Нет bootstrap")
        return
    events = bs.get("events", [])
    now_utc = datetime.now(timezone.utc)
    future=[]
    for e in events:
        dt = parse_deadline(e.get("deadline_time"))
        if dt and dt > now_utc:
            future.append((dt,e))
    if not future:
        await update.message.reply_text("Нет будущих дедлайнов")
        return
    dt,e = min(future, key=lambda x: x[0])
    tz_offset = get_timezone_offset()
    # Первый запуск без tz — подсказка
    if INITIAL_TZ_OFFSET is None and _tz_offset_memory is None:
        need = True
        if redis_client:
            try:
                if redis_client.get(TZ_REDIS_KEY) is not None:
                    need = False
            except Exception:
                pass
        if need:
            await update.message.reply_text("Таймзона не настроена. Установи командой /settz <offset> (например /settz +5). Пока показываю время в текущем смещении.")
    local_dt = dt + timedelta(hours=tz_offset)
    now_local = datetime.now(timezone.utc) + timedelta(hours=tz_offset)
    left_seconds = int((local_dt - now_local).total_seconds())
    if left_seconds < 0: left_seconds = 0
    days = left_seconds // 86400
    hours = (left_seconds % 86400) // 3600
    minutes = (left_seconds % 3600) // 60
    gw_num = e.get("id")
    text = (
        f"Ближайший дедлайн: Gameweek {gw_num}\n"
        f"Когда: {local_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC{tz_offset:+d}\n"
        f"Осталось: {days} д. {hours} ч. {minutes} мин."
    )
    await update.message.reply_text(text)

async def rank_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    results = await get_league_results_cached(LEAGUE_ID)
    if not results:
        await update.message.reply_text("Standings недоступны")
        return
    league_name = LEAGUE_NAME or "League"
    rows=[]
    for r in sorted(results, key=lambda x: x.get("rank", 10**9)):
        rank = r.get("rank")
        last = r.get("last_rank")
        delta = None
        if isinstance(rank,int) and isinstance(last,int) and rank>0 and last>0:
            d = last - rank
            if d > 0:
                delta = f"↑{d}"
            elif d < 0:
                delta = f"↓{abs(d)}"
            else:
                delta = "→0"
        player_name = r.get("player_name")
        entry_name = r.get("entry_name")
        total = r.get("total")
        rows.append((rank, player_name, entry_name, total, delta))
    # Center formatting
    rank_w = max(len("Rank"), max((len(str(r[0])) for r in rows), default=4))
    player_w = max(len("Player"), max((len(r[1]) for r in rows), default=6))
    entry_w = max(len("Team"), max((len(r[2]) for r in rows), default=4))
    total_w = max(len("Pts"), max((len(str(r[3])) for r in rows), default=3))
    delta_w = max(len("Δ"), max((len(r[4]) if r[4] else 2 for r in rows), default=2))
    header = f"{'Rank'.center(rank_w)}  {'Player'.center(player_w)}  {'Team'.center(entry_w)}  {'Pts'.center(total_w)}  {'Δ'.center(delta_w)}"
    sep = "-"*len(header)
    lines=[f"{league_name}".center(len(header)), "```", header, sep]
    for r in rows:
        delta = r[4] or "→0"
        lines.append(
            f"{str(r[0]).center(rank_w)}  {r[1].center(player_w)}  {r[2].center(entry_w)}  {str(r[3]).center(total_w)}  {delta.center(delta_w)}"
        )
    lines.append("```")
    text = "\n".join(lines)
    for chunk in split_message_chunks(text):
        try:
            await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception:
            safe = text.replace("```","")
            for c2 in split_message_chunks(safe):
                await update.message.reply_text(c2)
            break

async def gwinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Использование: /gwinfo <номер>")
        return
    gw = int(args[0])
    # Обновим имя лиги
    await get_league_results_cached(LEAGUE_ID)
    live = await fetch_json(fpl_url(event_live_path_tpl.format(gw=gw)))
    if not live:
        await update.message.reply_text("live недоступен")
        return
    league_players = await get_league_player_ids_cached(gw)
    live_elements = live.get("elements", [])
    rows=[]
    for el in live_elements:
        pid = el.get("id")
        if pid not in league_players: continue
        stats = el.get("stats", {}) or {}
        pos = PLAYER_POS_MAP.get(pid, 0)
        total_points = int(stats.get("total_points",0) or 0)
        stat_parts=[]
        g=int(stats.get("goals_scored",0) or 0); 
        if g>0: stat_parts.append(f"G{g}")
        a=int(stats.get("assists",0) or 0)
        if a>0: stat_parts.append(f"A{a}")
        cs=int(stats.get("clean_sheets",0) or 0)
        if cs>0 and pos!=4: stat_parts.append("CS")
        if dc_threshold_met(stats,pos): stat_parts.append("DC")
        yc=int(stats.get("yellow_cards",0) or 0)
        if yc>0: stat_parts.append(f"YC{yc}")
        rc=int(stats.get("red_cards",0) or 0)
        if rc>0: stat_parts.append("RC")
        if pos==1:
            saves=int(stats.get("saves",0) or 0)
            gks=saves//3
            if gks>0: stat_parts.append(f"GKS{gks}")
        og=int(stats.get("own_goals",0) or 0)
        if og>0: stat_parts.append(f"OG{og}")
        pen_m=int(stats.get("penalties_missed",0) or 0)
        if pen_m>0: stat_parts.append(f"PM{pen_m}")
        pen_s=int(stats.get("penalties_saved",0) or 0)
        if pen_s>0: stat_parts.append(f"PS{pen_s}")
        bonus=int(stats.get("bonus",0) or 0)
        if bonus>0: stat_parts.append(f"B{bonus}")
        if not stat_parts and total_points <= 2:
            continue
        name = PLAYER_NAME_MAP.get(pid,f"P{pid}")
        rows.append({"name":name,"stats":" ".join(stat_parts) if stat_parts else "-", "pts":total_points})
    if not rows:
        await update.message.reply_text("Нет игроков для отображения.")
        return
    rows.sort(key=lambda r: (-r["pts"], r["name"].lower()))
    name_w = max(len("Player"), max(len(r["name"]) for r in rows))
    stats_w = max(len("Stats"), max(len(r["stats"]) for r in rows))
    pts_w = max(len("Pts"), max(len(str(r["pts"])) for r in rows))
    header = f"{'Player'.center(name_w)}  {'Stats'.center(stats_w)}  {'Pts'.center(pts_w)}"
    sep = "-"*len(header)
    league_name = LEAGUE_NAME or "League"
    desc = ("G=Goals, A=Assists, CS=Clean sheet, DC=Defensive contribution, "
            "YC=Yellow cards, RC=Red card, GKS=GK save points, OG=Own goals, PM=Penalties missed, "
            "PS=Penalties saved, B=Bonus")
    lines=[f"GW {gw} — {league_name}".center(len(header)), desc.center(len(header)), "```", header, sep]
    for r in rows:
        lines.append(
            f"{r['name'].center(name_w)}  {r['stats'].center(stats_w)}  {str(r['pts']).center(pts_w)}"
        )
    lines.append("```")
    full_text="\n".join(lines)
    for chunk in split_message_chunks(full_text):
        try:
            await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception:
            safe = full_text.replace("```","")
            for c2 in split_message_chunks(safe):
                await update.message.reply_text(c2)
            break

async def gwpoints_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args=context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Использование: /gwpoints <номер>")
        return
    gw=int(args[0])
    results = await get_league_results_cached(LEAGUE_ID)
    if not results:
        await update.message.reply_text("Standings недоступны"); return
    league_name = LEAGUE_NAME or "League"
    async def one(r):
        data=await get_entry_picks_cached(r.get("entry"), gw)
        pts=data.get("entry_history",{}).get("points") if data else None
        return (r.get("player_name"), r.get("entry_name"), pts if isinstance(pts,int) else -1)
    tasks=[asyncio.create_task(one(r)) for r in results]
    got=await asyncio.gather(*tasks, return_exceptions=True)
    entries=[]
    for item in got:
        if isinstance(item, tuple):
            entries.append(item)
    entries.sort(key=lambda x: (-x[2], x[0].lower()))
    player_w = max(len("Player"), max(len(e[0]) for e in entries))
    team_w = max(len("Team"), max(len(e[1]) for e in entries))
    pts_w = max(len("Pts"), max(len(str(e[2])) for e in entries))
    header = f"{'Player'.center(player_w)}  {'Team'.center(team_w)}  {'Pts'.center(pts_w)}"
    sep = "-"*len(header)
    lines=[f"GW {gw} — {league_name}".center(len(header)), "```", header, sep]
    for e in entries:
        val = e[2] if e[2] >= 0 else "n/a"
        lines.append(f"{e[0].center(player_w)}  {e[1].center(team_w)}  {str(val).center(pts_w)}")
    lines.append("```")
    text = "\n".join(lines)
    for chunk in split_message_chunks(text):
        try:
            await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception:
            safe = text.replace("```","")
            for c2 in split_message_chunks(safe):
                await update.message.reply_text(c2)
            break

async def month_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs = await get_bootstrap_cached()
    if not bs:
        await update.message.reply_text("Нет bootstrap")
        return
    events = bs.get("events", [])
    results = await get_league_results_cached(LEAGUE_ID)
    if not results:
        await update.message.reply_text("Standings недоступны")
        return
    rows, remaining = await month_points(results, events)
    tz_offset = get_timezone_offset()
    now_local = datetime.now(timezone.utc) + timedelta(hours=tz_offset)
    rus_name = RUS_MONTH.get(now_local.month, f"Месяц {now_local.month}")
    season_idx = season_month_index(now_local.month)
    league_name = LEAGUE_NAME or "League"
    # Топ‑10
    top = rows[:10]
    player_w = max(len("Player"), max((len(r[0]) for r in top), default=6))
    team_w = max(len("Team"), max((len(r[1]) for r in top), default=4))
    pts_w = max(len("MonthPts"), max((len(str(r[2])) for r in top), default=8))
    header = f"{'Player'.center(player_w)}  {'Team'.center(team_w)}  {'MonthPts'.center(pts_w)}"
    sep = "-"*len(header)
    title = f"Месяц {season_idx if season_idx else '?'} — {rus_name} — {league_name}"
    rem_line = f"Осталось туров в месяце: {remaining}"
    lines=[title.center(len(header)), rem_line.center(len(header)), "```", header, sep]
    for r in top:
        lines.append(f"{r[0].center(player_w)}  {r[1].center(team_w)}  {str(r[2]).center(pts_w)}")
    lines.append("```")
    text="\n".join(lines)
    for chunk in split_message_chunks(text):
        try:
            await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception:
            safe = text.replace("```","")
            for c2 in split_message_chunks(safe):
                await update.message.reply_text(c2)
            break

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled", exc_info=context.error)

async def setup_bot_commands(bot):
    cmds=[
        BotCommand("rank","League standings"),
        BotCommand("deadline","Next deadline"),
        BotCommand("gwinfo","Live GW players"),
        BotCommand("gwpoints","Points per GW"),
        BotCommand("month","Points per month"),
        BotCommand("settz","Set timezone"),
    ]
    try:
        await bot.set_my_commands(cmds)
    except Exception:
        logger.exception("set_my_commands failed")

# ===== Lifecycle =====
async def run_bot():
    global http_client, application
    init_redis()
    limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
    use_http2=ENABLE_HTTP2
    try:
        import h2  # noqa
    except Exception:
        use_http2=False
    http_client = httpx.AsyncClient(http2=use_http2, limits=limits, timeout=15.0)

    application = Application.builder().token(BOT_TOKEN).concurrent_updates(TELEGRAM_CONCURRENCY).build()
    application.add_handler(CommandHandler("rank", rank_command))
    application.add_handler(CommandHandler("deadline", deadline_command))
    application.add_handler(CommandHandler("gwinfo", gwinfo_command))
    application.add_handler(CommandHandler("gwpoints", gwpoints_command))
    application.add_handler(CommandHandler("month", month_command))
    application.add_handler(CommandHandler("settz", settz_command))
    application.add_error_handler(error_handler)

    await application.initialize()
    await application.start()
    await setup_bot_commands(application.bot)

    bs = await get_bootstrap_cached()
    if not bs:
        logger.warning("Bootstrap not loaded initially")

    if USE_WEBHOOK:
        logger.info("Webhook mode not implemented; using polling.")
    else:
        try:
            await application.updater.start_polling()
        except Exception:
            logger.exception("start_polling failed")
            return

    asyncio.create_task(live_monitor_loop())

    try:
        me = await application.bot.get_me()
        logger.info(f"Bot @{getattr(me,'username','?')} started (id={getattr(me,'id','?')})")
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
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(stop_event.set)
    except RuntimeError:
        pass

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()
    try:
        asyncio.run(run_bot())
    except Exception:
        logger.exception("App crashed")
