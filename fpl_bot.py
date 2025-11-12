"""
FPL Telegram Bot (PTB v21) + Upstash Redis persistence for realtime events.

Summary:
- Proxy support (FPL_PROXY_BASE) to avoid 403
- Async HTTP (httpx, optional HTTP/2 if h2 installed & ENABLE_HTTP2=1)
- Caching: bootstrap, standings, picks
- Live monitoring (league players only): goals, assists, yellow cards, red cards, own goals,
  penalties missed, penalties saved, clean sheets (early lock + final)
- Defensive Contribution (DC) points (official CBIT threshold):
    DEF (element_type=2): >=10 CBIT → +2
    MID/FWD (3/4):      >=12 CBIT → +2
    GK (1):             not eligible
  Field names tried: defensive_contributions, cbit, cbits, def_contributions
- /gwinfo table (owners REMOVED from display):
    Columns: Player | Stats | Pts
    Stat order (display priority): G, A, CS, DC, YC, RC, GKS, OG, PenM, PenS, B
      - CS shown as 'CS' (no numeric suffix); CS is shown for GK/DEF/MID, suppressed for FWD.
      - DC is shown as 'DC' (no numeric suffix) when threshold met.
      - YC shows a number, e.g. 'YC1', 'YC2'.
      - RC shown as 'RC' (no number).
      - GKS shows goalkeeper save points (saves // 3), e.g. 'GKS1', 'GKS2'.
      - PenM instead of PM, PenS instead of PS.
      - Bonus (B) always LAST.
    Hidden rows:
      - Pure appearance (≤2 pts AND no stats)
      - MID-only CS is now SHOWN (no longer hidden)
    Alignment: fixed-width columns computed once per output block (monospaced code fence),
               so the Stats column width keeps other columns from shifting even when tokens vary (e.g., YC1).
- Live messages: grouped per fixture (goals paired with assists, others listed)
- Commands: /start /help /points /gw /rank /deadline /gwinfo /liveon /liveoff /con

Tie-break sorting in /gwinfo:
- Primary: Pts descending
- Tie-breaker: higher ownership among league entries (not displayed, only used for sorting)
- Secondary: Player name ascending
"""

import os
import json
import asyncio
import threading
import logging
import time
import signal
import random
import hashlib
import gzip
from typing import Any, Dict, Optional, List, Tuple, Set
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify
import httpx
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update, BotCommand

try:
    from upstash_redis import Redis
except ImportError:
    Redis = None  # fallback to memory

# ---------- ENV ----------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Environment variable BOT_TOKEN is required")

TARGET_CHAT_ID = os.environ.get("TARGET_CHAT_ID")
ENABLE_KILL = os.environ.get("ENABLE_KILL", "0") == "1"

FPL_CACHE_TTL = int(os.environ.get("FPL_CACHE_TTL", "8"))              # minutes
FPL_CONCURRENCY = int(os.environ.get("FPL_CONCURRENCY", "3"))
FPL_STANDINGS_TTL = int(os.environ.get("FPL_STANDINGS_TTL", "60"))     # seconds
FPL_PICKS_TTL = int(os.environ.get("FPL_PICKS_TTL", "300"))            # seconds
FPL_PICKS_ALLOW_STALE = os.environ.get("FPL_PICKS_ALLOW_STALE", "1") == "1"
REDIS_GW_TTL = int(os.environ.get("REDIS_GW_TTL", str(7 * 24 * 3600))) # seconds (7 days)

PORT = int(os.environ.get("PORT", 10000))
TELEGRAM_CONCURRENCY = int(os.environ.get("TELEGRAM_CONCURRENCY", "4"))
USE_WEBHOOK = os.environ.get("USE_WEBHOOK", "0") == "1"
LEAGUE_ID = os.environ.get("LEAGUE_ID", "980121")

FPL_PROXY_BASE = os.environ.get("FPL_PROXY_BASE", "").rstrip("/")
ENABLE_HTTP2 = os.environ.get("ENABLE_HTTP2", "1") == "1"

ENABLE_LIVE_MONITOR = os.environ.get("ENABLE_LIVE_MONITOR", "0") == "1"
LIVE_POLL_INTERVAL = int(os.environ.get("LIVE_POLL_INTERVAL", "30"))

# Snapshot system flags
ENABLE_SNAPSHOT_COMPRESSION = os.environ.get("ENABLE_SNAPSHOT_COMPRESSION", "0") == "1"
SNAPSHOT_REBUILD_RATE_LIMIT = int(os.environ.get("SNAPSHOT_REBUILD_RATE_LIMIT", "2"))  # rebuilds per minute

stop_event = asyncio.Event()

# ---------- Logging ----------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("fpl_bot")

# ---------- Flask ----------
flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    return "FPL BOT is running!"

@flask_app.route("/healthz")
def health():
    age_min = None
    if bootstrap_cache_ts is not None:
        age_min = (time.time() - bootstrap_cache_ts) / 60.0
    standings_age = None
    if standings_cache_ts is not None:
        standings_age = time.time() - standings_cache_ts
    return jsonify({
        "ok": True,
        "bootstrap_cached": bootstrap_cache_ts is not None,
        "bootstrap_cache_age_min": age_min,
        "standings_cached": standings_cache_ts is not None,
        "standings_cache_age_sec": standings_age,
        "standings_cache_size": len(standings_cache.get("results", [])) if standings_cache else 0,
        "picks_cache_size": len(picks_cache),
        "live_monitor_enabled": live_monitor_enabled,
        "proxy_base": FPL_PROXY_BASE or None,
        "redis_connected": redis_client is not None,
        "season_tag": SEASON_TAG
    })

def start_flask():
    logger.info(f"Starting Flask app on port {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT)

def kill_existing_instances():
    logger.info("ENABLE_KILL is set (placeholder)")

# ---------- HTTP / Headers ----------
def fpl_url(path: str) -> str:
    base = FPL_PROXY_BASE or "https://fantasy.premierleague.com"
    return f"{base}{path}"

FPL_BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://fantasy.premierleague.com/",
    "Origin": "https://fantasy.premierleague.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

http_client: Optional[httpx.AsyncClient] = None

bootstrap_cache: Dict[str, Any] = {}
bootstrap_cache_ts: Optional[float] = None
bootstrap_lock = asyncio.Lock()

standings_cache: Dict[str, Any] = {}
standings_cache_ts: Optional[float] = None
standings_lock = asyncio.Lock()

picks_cache: Dict[Tuple[int, int], Dict[str, Any]] = {}
_picks_locks: Dict[Tuple[int, int], asyncio.Lock] = {}
_picks_locks_guard = asyncio.Lock()

# In-memory caches with TTL for fixtures
fixtures_cache: Dict[int, Dict[str, Any]] = {}  # gw -> {data, ts}
fixtures_lock = asyncio.Lock()

bootstrap_path = "/api/bootstrap-static/"
league_path_tpl = "/api/leagues-classic/{league_id}/standings/?page_standings={page}"
entry_picks_path_tpl = "/api/entry/{entry_id}/event/{gw}/picks/"
event_live_path_tpl = "/api/event/{gw}/live/"
fixtures_event_path_tpl = "/api/fixtures/?event={gw}"

fpl_semaphore = asyncio.Semaphore(FPL_CONCURRENCY)

async def fetch_json(
    url: str,
    timeout: float = 15.0,
    max_attempts: int = 3,
    backoff_base: float = 1.5
) -> Optional[Dict]:
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        await asyncio.sleep(random.uniform(0.05, 0.2))
        try:
            async with fpl_semaphore:
                resp = await http_client.get(url, headers=FPL_BASE_HEADERS, timeout=timeout)
            status = resp.status_code
            if status == 200:
                try:
                    return resp.json()
                except Exception as jex:
                    logger.warning(f"JSON decode error {url}: {jex}")
                    return None
            body_preview = (resp.text or "")[:200].replace("\n", " ")
            logger.warning(f"HTTP {status} attempt {attempt}/{max_attempts} {url[:80]}... body='{body_preview}'")
            if status in (403, 429) or 500 <= status < 600:
                if attempt < max_attempts:
                    sleep_time = min(backoff_base ** attempt + random.uniform(0, 0.5), 8.0)
                    await asyncio.sleep(sleep_time)
                    continue
            return None
        except httpx.TimeoutException:
            logger.error(f"Timeout {url[:80]}... attempt {attempt}/{max_attempts}")
            if attempt < max_attempts:
                await asyncio.sleep(min(backoff_base ** attempt, 8.0))
        except Exception as ex:
            logger.error(f"Error fetching {url[:80]}...: {ex}")
            if attempt < max_attempts:
                await asyncio.sleep(min(backoff_base ** attempt, 8.0))
    return None

# ---------- Redis Persistence ----------
redis_client: Optional["Redis"] = None
SEASON_TAG: Optional[str] = None

_mem_events: Dict[str, int] = {}
_mem_sets: Dict[str, Set[str]] = {}
_mem_baseline: Set[str] = set()

# In-memory KV with TTL for preventing indefinite growth
_mem_kv: Dict[str, Any] = {}  # key -> {value, ts}
_mem_ttl_check_interval = 3600  # prune every hour
_mem_ttl_last_check = time.time()

def init_redis():
    global redis_client
    if Redis is None:
        logger.warning("upstash-redis not installed; using in-memory fallback.")
        return
    try:
        redis_client = Redis.from_env()
        logger.info("Upstash Redis connected.")
    except Exception as e:
        logger.warning(f"Redis init failed: {e}; fallback to memory.")
        redis_client = None

def prune_expired_mem_kv():
    """Prune expired entries from in-memory KV to prevent indefinite growth."""
    global _mem_ttl_last_check
    now = time.time()
    if now - _mem_ttl_last_check < _mem_ttl_check_interval:
        return
    _mem_ttl_last_check = now
    
    # Prune _mem_events (use REDIS_GW_TTL as default TTL)
    expired_keys = []
    for key in _mem_events:
        # For simplicity, we'll keep events for REDIS_GW_TTL
        # This is a basic implementation - could be improved with per-key timestamps
        pass
    
    # Prune _mem_kv
    expired_kv = [k for k, v in _mem_kv.items() 
                  if isinstance(v, dict) and 
                  (now - v.get('ts', 0)) > REDIS_GW_TTL]
    for k in expired_kv:
        del _mem_kv[k]
    
    if expired_kv:
        logger.debug(f"Pruned {len(expired_kv)} expired mem_kv entries")

async def r_get(key: str) -> Optional[int]:
    prune_expired_mem_kv()
    if redis_client:
        raw = await asyncio.to_thread(redis_client.get, key)
        if raw is None:
            return None
        try:
            return int(raw)
        except Exception:
            return None
    return _mem_events.get(key)

async def r_get_many(keys: List[str]) -> Dict[str, Optional[int]]:
    """Batch GET operation to reduce round-trips."""
    prune_expired_mem_kv()
    result = {}
    if redis_client:
        try:
            # Use pipeline for Redis if available
            for key in keys:
                raw = await asyncio.to_thread(redis_client.get, key)
                if raw is not None:
                    try:
                        result[key] = int(raw)
                    except Exception:
                        result[key] = None
                else:
                    result[key] = None
        except Exception as e:
            logger.warning(f"Redis batch get failed: {e}")
            for key in keys:
                result[key] = _mem_events.get(key)
    else:
        for key in keys:
            result[key] = _mem_events.get(key)
    return result

async def r_set(key: str, value: int):
    if redis_client:
        await asyncio.to_thread(redis_client.set, key, value, ex=REDIS_GW_TTL)
    else:
        _mem_events[key] = value

async def r_set_many(items: Dict[str, int]):
    """Batch SET operation to reduce round-trips."""
    if redis_client:
        try:
            for key, value in items.items():
                await asyncio.to_thread(redis_client.set, key, value, ex=REDIS_GW_TTL)
        except Exception as e:
            logger.warning(f"Redis batch set failed: {e}")
            _mem_events.update(items)
    else:
        _mem_events.update(items)

async def r_sadd(key: str, member: str):
    if redis_client:
        await asyncio.to_thread(redis_client.sadd, key, member)
        await asyncio.to_thread(redis_client.expire, key, REDIS_GW_TTL)
    else:
        _mem_sets.setdefault(key, set()).add(member)

async def r_sismember(key: str, member: str) -> bool:
    if redis_client:
        res = await asyncio.to_thread(redis_client.sismember, key, member)
        return bool(res)
    return member in _mem_sets.get(key, set())

async def r_set_flag(key: str):
    await r_set(key, 1)

async def r_flag_exists(key: str) -> bool:
    return (await r_get(key)) == 1 or key in _mem_baseline

def key_event(season: str, gw: int, fixture_id: int, identifier: str, player_id: int) -> str:
    return f"fpl:{season}:{gw}:stat:{fixture_id}:{identifier}:{player_id}"

def key_cs_locked(season: str, gw: int) -> str:
    return f"fpl:{season}:{gw}:cs_locked"

def key_cs_final(season: str, gw: int) -> str:
    return f"fpl:{season}:{gw}:cs_final"

def key_baseline(season: str, gw: int) -> str:
    return f"fpl:{season}:{gw}:baseline_done"

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

# ---------- Snapshot System ----------
# Rate limiting for rebuilds
_rebuild_timestamps: List[float] = []
_rebuild_lock = asyncio.Lock()

def compute_roster_hash(entry_ids: List[int]) -> str:
    """Compute SHA256 hash of sorted league entry IDs."""
    sorted_ids = sorted(entry_ids)
    roster_str = ",".join(map(str, sorted_ids))
    return hashlib.sha256(roster_str.encode()).hexdigest()[:16]

async def r_get_json(key: str) -> Optional[Dict]:
    """Get JSON data from Redis."""
    if redis_client:
        raw = await asyncio.to_thread(redis_client.get, key)
        if raw is None:
            return None
        try:
            # Handle both compressed and uncompressed
            if isinstance(raw, bytes) and raw[:2] == b'\x1f\x8b':  # gzip magic number
                raw = gzip.decompress(raw)
            if isinstance(raw, bytes):
                raw = raw.decode('utf-8')
            return json.loads(raw)
        except Exception as e:
            logger.warning(f"Failed to decode JSON from Redis key {key}: {e}")
            return None
    # In-memory fallback
    val = _mem_kv.get(key)
    if val and isinstance(val, dict):
        return val.get('value')
    return None

async def r_set_json(key: str, data: Dict, ttl: Optional[int] = None):
    """Set JSON data to Redis with optional compression."""
    if ttl is None:
        ttl = REDIS_GW_TTL
    
    try:
        json_str = json.dumps(data)
        
        # Optional compression
        if ENABLE_SNAPSHOT_COMPRESSION:
            compressed = gzip.compress(json_str.encode('utf-8'))
            if redis_client:
                await asyncio.to_thread(redis_client.set, key, compressed, ex=ttl)
            else:
                _mem_kv[key] = {'value': data, 'ts': time.time()}
            logger.debug(f"Stored compressed snapshot {key} ({len(compressed)} bytes)")
        else:
            if redis_client:
                await asyncio.to_thread(redis_client.set, key, json_str, ex=ttl)
            else:
                _mem_kv[key] = {'value': data, 'ts': time.time()}
    except Exception as e:
        logger.error(f"Failed to set JSON to Redis key {key}: {e}")

async def r_setnx(key: str, value: str, ex: int) -> bool:
    """Set if not exists (Redis lock primitive). Returns True if set, False if already exists."""
    if redis_client:
        result = await asyncio.to_thread(redis_client.set, key, value, ex=ex, nx=True)
        return result is not None
    else:
        # In-memory fallback
        if key not in _mem_kv:
            _mem_kv[key] = {'value': value, 'ts': time.time()}
            return True
        return False

async def r_delete(key: str):
    """Delete a key from Redis."""
    if redis_client:
        await asyncio.to_thread(redis_client.delete, key)
    else:
        _mem_kv.pop(key, None)

def key_gw_snapshot(season: str, league_id: str, gw: int) -> str:
    """Key for GW snapshot in Redis."""
    return f"fpl:{season}:league:{league_id}:gw:{gw}:snapshot"

def key_snapshot_lock(season: str, league_id: str, gw: int) -> str:
    """Key for snapshot rebuild lock."""
    return f"fpl:{season}:league:{league_id}:gw:{gw}:rebuild_lock"

async def can_rebuild_now() -> bool:
    """Check if we can rebuild a snapshot now based on rate limiting."""
    async with _rebuild_lock:
        now = time.time()
        # Remove timestamps older than 1 minute
        global _rebuild_timestamps
        _rebuild_timestamps = [ts for ts in _rebuild_timestamps if now - ts < 60]
        
        if len(_rebuild_timestamps) >= SNAPSHOT_REBUILD_RATE_LIMIT:
            logger.warning(f"Rebuild rate limit reached: {len(_rebuild_timestamps)} rebuilds in last minute")
            return False
        
        _rebuild_timestamps.append(now)
        return True

def next_poll_interval(now_utc: datetime, next_kickoff_utc: Optional[datetime]) -> int:
    """
    Adaptive poll interval based on time until next kickoff:
    - >6h before: 180s
    - 30m-6h before: 60s
    - During active match window (or within 30m): 30s (or LIVE_POLL_INTERVAL)
    Returns interval in seconds.
    """
    if not next_kickoff_utc:
        return LIVE_POLL_INTERVAL
    
    delta = (next_kickoff_utc - now_utc).total_seconds()
    
    if delta > 6 * 3600:  # >6 hours
        return 180
    elif delta > 30 * 60:  # 30 min to 6 hours
        return 60
    else:  # within 30 min or already started
        return max(LIVE_POLL_INTERVAL, 30)

def find_next_kickoff(events: List[Dict], fixtures: Optional[List[Dict]] = None) -> Optional[datetime]:
    """
    Find the next kickoff time from events and fixtures.
    Returns datetime in UTC or None.
    """
    now = datetime.now(timezone.utc)
    
    # Check current or next event deadline
    current_or_next = [e for e in events if e.get("is_current") or e.get("is_next")]
    if current_or_next:
        deadlines = []
        for e in current_or_next:
            dl = parse_deadline(e.get("deadline_time"))
            if dl and dl > now:
                deadlines.append(dl)
        if deadlines:
            return min(deadlines)
    
    # Check fixtures if provided
    if fixtures:
        for f in fixtures:
            kickoff_time = f.get("kickoff_time")
            if kickoff_time:
                ko = parse_deadline(kickoff_time)
                if ko and ko > now:
                    return ko
    
    return None

async def build_gw_snapshot(season: str, league_id: str, gw: int, 
                            events: List[Dict], current_roster: List[int]) -> Dict[str, Any]:
    """
    Build a GW snapshot storing only numerical data + IDs.
    Names are resolved at render time from standings cache.
    
    Snapshot structure:
    {
        "version": 1,
        "gw": gw,
        "roster_hash": hash,
        "built_at": timestamp,
        "entries": [
            {"id": entry_id, "gw_points": pts, "transfers": tc, "transfer_cost": cost, ...},
            ...
        ]
    }
    """
    roster_hash = compute_roster_hash(current_roster)
    entries_data = []
    
    # Fetch all entry data for this GW
    tasks = []
    for entry_id in current_roster:
        tasks.append(get_entry_picks_cached(entry_id, gw))
    
    picks_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for entry_id, picks_data in zip(current_roster, picks_results):
        if isinstance(picks_data, Exception) or not picks_data:
            continue
        
        entry_history = picks_data.get("entry_history", {})
        entries_data.append({
            "id": entry_id,
            "gw_points": entry_history.get("points"),
            "total_points": entry_history.get("total_points"),
            "transfers": entry_history.get("event_transfers"),
            "transfer_cost": entry_history.get("event_transfers_cost"),
            "bank": entry_history.get("bank"),
            "value": entry_history.get("value"),
            "rank": entry_history.get("rank"),
            "overall_rank": entry_history.get("overall_rank"),
        })
    
    snapshot = {
        "version": 1,
        "gw": gw,
        "roster_hash": roster_hash,
        "built_at": time.time(),
        "entries": entries_data
    }
    
    return snapshot

async def get_or_build_gw_snapshot(season: str, league_id: str, gw: int, 
                                   events: List[Dict], current_roster: List[int],
                                   force_rebuild: bool = False) -> Optional[Dict[str, Any]]:
    """
    Get GW snapshot with lazy rebuild on roster change.
    Uses Redis lock to prevent concurrent rebuilds.
    """
    snapshot_key = key_gw_snapshot(season, league_id, gw)
    lock_key = key_snapshot_lock(season, league_id, gw)
    
    # Check if GW is frozen (past freeze time: last kickoff + 24h)
    gw_event = next((e for e in events if e.get("id") == gw), None)
    if not gw_event:
        return None
    
    is_frozen = gw_event.get("finished", False)
    
    # Load existing snapshot if not forcing rebuild
    if not force_rebuild:
        existing = await r_get_json(snapshot_key)
        if existing:
            current_hash = compute_roster_hash(current_roster)
            existing_hash = existing.get("roster_hash", "")
            
            # If roster hasn't changed or GW is not frozen, return existing
            if existing_hash == current_hash or not is_frozen:
                logger.debug(f"Using existing snapshot for GW {gw}")
                return existing
            
            # Roster changed and GW is frozen - need rebuild
            logger.info(f"Roster hash mismatch for GW {gw}: existing={existing_hash}, current={current_hash}")
    
    # Check rate limiting
    if not await can_rebuild_now():
        logger.warning(f"Rate limit hit, serving existing snapshot for GW {gw}")
        existing = await r_get_json(snapshot_key)
        return existing if existing else None
    
    # Try to acquire lock for rebuild
    lock_acquired = await r_setnx(lock_key, "1", ex=300)
    if not lock_acquired:
        logger.info(f"Another process is rebuilding GW {gw} snapshot, waiting...")
        # Wait a bit and try to fetch
        await asyncio.sleep(2)
        return await r_get_json(snapshot_key)
    
    try:
        logger.info(f"Building snapshot for GW {gw} with {len(current_roster)} entries")
        snapshot = await build_gw_snapshot(season, league_id, gw, events, current_roster)
        await r_set_json(snapshot_key, snapshot, ttl=REDIS_GW_TTL)
        logger.info(f"Snapshot built for GW {gw} with roster_hash={snapshot['roster_hash']}")
        return snapshot
    finally:
        await r_delete(lock_key)

# ---------- Bootstrap ----------
def bootstrap_cache_valid() -> bool:
    if bootstrap_cache_ts is None:
        return False
    age = (time.time() - bootstrap_cache_ts) / 60.0
    return age < FPL_CACHE_TTL

def get_events_from_env() -> Optional[List[Dict]]:
    raw = os.environ.get("FPL_EVENTS_JSON")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception as ex:
        logger.error(f"Failed parse FPL_EVENTS_JSON: {ex}")
        return None

async def get_bootstrap() -> Optional[Dict]:
    async with bootstrap_lock:
        if bootstrap_cache_valid():
            return bootstrap_cache
    data = await fetch_json(fpl_url(bootstrap_path))
    if data:
        async with bootstrap_lock:
            bootstrap_cache.clear()
            bootstrap_cache.update(data)
            global bootstrap_cache_ts
            bootstrap_cache_ts = time.time()
        return data
    env_events = get_events_from_env()
    if env_events:
        logger.warning("Using fallback events from env.")
        fake = {"events": env_events}
        async with bootstrap_lock:
            bootstrap_cache.clear()
            bootstrap_cache.update(fake)
            bootstrap_cache_ts = time.time()
        return fake
    return None

async def get_bootstrap_cached(ttl: int = None) -> Optional[Dict]:
    """
    Read-through cache for bootstrap with custom TTL.
    ttl in seconds; defaults to FPL_CACHE_TTL (minutes).
    """
    if ttl is None:
        ttl = FPL_CACHE_TTL * 60  # convert minutes to seconds
    
    async with bootstrap_lock:
        if bootstrap_cache_ts and (time.time() - bootstrap_cache_ts) < ttl:
            return bootstrap_cache
    
    return await get_bootstrap()

async def get_fixtures_cached(gw: int, ttl: int = 900) -> Optional[List[Dict]]:
    """
    Read-through cache for fixtures with TTL (default 900s = 15 min).
    Returns fixture list for the given gameweek.
    """
    now = time.time()
    async with fixtures_lock:
        cached = fixtures_cache.get(gw)
        if cached and (now - cached.get("ts", 0)) < ttl:
            logger.debug(f"Fixtures cache hit for GW {gw}")
            return cached.get("data")
    
    # Fetch from API
    url = fpl_url(fixtures_event_path_tpl.format(gw=gw))
    data = await fetch_json(url)
    
    if data:
        async with fixtures_lock:
            fixtures_cache[gw] = {"data": data, "ts": now}
        logger.debug(f"Fixtures fetched and cached for GW {gw}")
        return data
    
    # Return stale if available
    if cached:
        logger.warning(f"Serving stale fixtures for GW {gw}")
        return cached.get("data")
    
    return None

# ---------- Standings ----------
def standings_cache_valid() -> bool:
    if standings_cache_ts is None:
        return False
    return (time.time() - standings_cache_ts) <= FPL_STANDINGS_TTL

async def get_league_results_cached(league_id: str) -> Optional[List[Dict]]:
    if standings_cache_valid() and "results" in standings_cache:
        return standings_cache.get("results")  # type: ignore
    async with standings_lock:
        if standings_cache_valid() and "results" in standings_cache:
            return standings_cache.get("results")  # type: ignore
        all_results: List[Dict] = []
        page = 1
        while True:
            url = fpl_url(league_path_tpl.format(league_id=league_id, page=page))
            data = await fetch_json(url)
            if not data:
                if "results" in standings_cache:
                    logger.warning("Standings fetch failed; serve cached.")
                    return standings_cache.get("results")  # type: ignore
                return None
            try:
                standings = data["standings"]
                results = standings["results"]
                all_results.extend(results)
                if not standings.get("has_next"):
                    break
                page += 1
                if page > 30:
                    logger.warning("Pagination cutoff at 30 pages.")
                    break
            except Exception as ex:
                logger.error(f"Decode standings error: {ex}")
                if "results" in standings_cache:
                    logger.warning("Serve cached standings after decode error.")
                    return standings_cache.get("results")  # type: ignore
                return None
        standings_cache.clear()
        standings_cache.update({"results": all_results, "pages": page})
        global standings_cache_ts
        standings_cache_ts = time.time()
        return all_results

# ---------- Picks ----------
def picks_cache_valid(ts: Optional[float]) -> bool:
    if ts is None:
        return False
    return (time.time() - ts) <= FPL_PICKS_TTL

async def _picks_get_lock_guarded(key: Tuple[int, int]) -> asyncio.Lock:
    async with _picks_locks_guard:
        lock = _picks_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _picks_locks[key] = lock
        return lock

async def get_entry_picks_cached(entry_id: int, gw: int) -> Optional[Dict]:
    key = (entry_id, gw)
    cached = picks_cache.get(key)
    if cached and picks_cache_valid(cached.get("ts")):
        return cached.get("data")  # type: ignore
    lock = await _picks_get_lock_guarded(key)
    async with lock:
        cached2 = picks_cache.get(key)
        if cached2 and picks_cache_valid(cached2.get("ts")):
            return cached2.get("data")  # type: ignore
        url = fpl_url(entry_picks_path_tpl.format(entry_id=entry_id, gw=gw))
        data = await fetch_json(url)
        if data:
            picks_cache[key] = {"data": data, "ts": time.time()}
            return data
        if FPL_PICKS_ALLOW_STALE and cached2 and cached2.get("data"):
            logger.warning(f"Serve STALE picks entry={entry_id} gw={gw}")
            return cached2.get("data")  # type: ignore
        return None

# ---------- Helpers ----------
def choose_last_finished_gw(events: List[Dict]) -> Optional[int]:
    finished = [e for e in events if e.get("finished")]
    if finished:
        try:
            return max(e["id"] for e in finished)
        except Exception:
            pass
    current_ids = [e["id"] for e in events if e.get("is_current")]
    if current_ids:
        cid = max(current_ids)
        return max(cid - 1, 1)
    try:
        any_id = max(e["id"] for e in events)
        return max(any_id - 1, 1)
    except Exception:
        return None

def split_message_chunks(text: str, limit: int = 4000) -> List[str]:
    if len(text) <= limit:
        return [text]
    lines = text.splitlines(keepends=True)
    chunks: List[str] = []
    buf = ""
    for ln in lines:
        if len(buf) + len(ln) > limit:
            chunks.append(buf)
            buf = ""
        buf += ln
    if buf:
        chunks.append(buf)
    return chunks

async def send_text(bot_or_update, text: str, parse_mode: str = "Markdown", chat_id: Optional[int] = None):
    """
    Unified message sending utility:
    - Splits long messages into chunks
    - Tries Markdown, falls back to plain text on formatting errors
    - Accepts both Update objects and chat ids
    """
    # Determine target chat and bot
    if hasattr(bot_or_update, 'message'):  # Update object
        target_chat = bot_or_update.effective_chat.id
        bot = bot_or_update.get_bot()
    elif hasattr(bot_or_update, 'send_message'):  # Bot object
        bot = bot_or_update
        target_chat = chat_id
        if not target_chat:
            logger.error("send_text: chat_id required when bot object is passed")
            return
    else:
        logger.error("send_text: invalid bot_or_update parameter")
        return
    
    chunks = split_message_chunks(text)
    
    for chunk in chunks:
        try:
            await bot.send_message(chat_id=target_chat, text=chunk, parse_mode=parse_mode)
        except Exception as e:
            # Fallback to plain text if Markdown fails
            if parse_mode == "Markdown":
                try:
                    safe_chunk = chunk.replace("```", "").replace("*", "").replace("_", "")
                    await bot.send_message(chat_id=target_chat, text=safe_chunk, parse_mode=None)
                except Exception as e2:
                    logger.error(f"Failed to send message even in plain text: {e2}")
            else:
                logger.error(f"Failed to send message: {e}")

def format_timedelta(delta_seconds: int) -> str:
    if delta_seconds < 0:
        return "дедлайн уже прошёл"
    days = delta_seconds // 86400
    hours = (delta_seconds % 86400) // 3600
    minutes = (delta_seconds % 3600) // 60
    parts = []
    if days > 0:
        parts.append(f"{days} д.")
    if hours > 0 or days > 0:
        parts.append(f"{hours} ч.")
    parts.append(f"{minutes} мин.")
    return " ".join(parts)

def parse_deadline(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt_str = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None

def find_next_deadline_event(events: List[Dict]) -> Optional[Dict]:
    now = datetime.now(timezone.utc)
    future_candidates = [e for e in events if parse_deadline(e.get("deadline_time")) and parse_deadline(e.get("deadline_time")) > now]
    if not future_candidates:
        return None
    prioritized = [e for e in future_candidates if e.get("is_current") or e.get("is_next")]
    if prioritized:
        return min(prioritized, key=lambda x: parse_deadline(x["deadline_time"]))
    return min(future_candidates, key=lambda x: parse_deadline(x["deadline_time"]))

# ---------- League Players / Owners ----------
async def get_league_player_ids(gw: int) -> Set[int]:
    results = await get_league_results_cached(LEAGUE_ID)
    if results is None:
        return set()
    players: Set[int] = set()
    tasks = []
    for r in results:
        entry_id = r.get("entry")
        if isinstance(entry_id, int):
            tasks.append(asyncio.create_task(get_entry_picks_cached(entry_id, gw)))
    picks_all = await asyncio.gather(*tasks, return_exceptions=True)
    for pr in picks_all:
        if isinstance(pr, dict):
            picks = pr.get("picks", [])
            for p in picks:
                pid = p.get("element")
                if isinstance(pid, int):
                    players.add(pid)
    return players

async def get_league_player_owners(gw: int) -> Dict[int, int]:
    """
    Returns player_id -> owners_count (number of league entries that own the player in GW).
    Not displayed in /gwinfo; used only for sorting tie-breaker.
    """
    owners_count: Dict[int, int] = {}
    results = await get_league_results_cached(LEAGUE_ID)
    if results is None:
        return owners_count
    tasks = []
    entries: List[int] = []
    for r in results:
        entry_id = r.get("entry")
        if isinstance(entry_id, int):
            entries.append(entry_id)
            tasks.append(asyncio.create_task(get_entry_picks_cached(entry_id, gw)))
    picks_all = await asyncio.gather(*tasks, return_exceptions=True)
    for pr in picks_all:
        if isinstance(pr, dict):
            for p in pr.get("picks", []):
                pid = p.get("element")
                if isinstance(pid, int):
                    owners_count[pid] = owners_count.get(pid, 0) + 1
    return owners_count

# ---------- Player / Team Maps ----------
def build_player_name_map() -> Dict[int, str]:
    elements = bootstrap_cache.get("elements", [])
    mapping = {}
    for el in elements:
        pid = el.get("id")
        name = el.get("web_name") or el.get("second_name") or f"Player{pid}"
        if isinstance(pid, int):
            mapping[pid] = str(name)
    return mapping

def build_player_team_map() -> Dict[int, int]:
    elements = bootstrap_cache.get("elements", [])
    mp = {}
    for el in elements:
        pid = el.get("id")
        team = el.get("team")
        if isinstance(pid, int) and isinstance(team, int):
            mp[pid] = team
    return mp

def build_player_position_map() -> Dict[int, int]:
    """
    player_id -> element_type (1=GK, 2=DEF, 3=MID, 4=FWD)
    """
    elements = bootstrap_cache.get("elements", [])
    mapping: Dict[int, int] = {}
    for el in elements:
        pid = el.get("id")
        et = el.get("element_type")
        if isinstance(pid, int) and isinstance(et, int):
            mapping[pid] = et
    return mapping

def build_team_short_map() -> Dict[int, str]:
    teams = bootstrap_cache.get("teams", [])
    mapping = {}
    for t in teams:
        tid = t.get("id")
        short = t.get("short_name") or t.get("name") or f"T{tid}"
        if isinstance(tid, int):
            mapping[tid] = str(short)
    return mapping

# ---------- Live Stat Keys ----------
STAT_KEYS = [
    "goals_scored",
    "assists",
    "yellow_cards",
    "red_cards",
    "penalties_missed",
    "penalties_saved",
    "own_goals",
]

# ---------- Defensive Contributions ----------
def get_stat_int(stats: Dict[str, Any], *keys: str) -> int:
    for k in keys:
        v = stats.get(k)
        if isinstance(v, (int, float)):
            try:
                return int(v)
            except Exception:
                pass
    return 0

def is_fixture_finished(fixture: Dict[str, Any]) -> bool:
    """
    Check if a fixture is finished (completed).
    Bonus should only be shown after fixture completion.
    """
    return fixture.get("finished") or fixture.get("finished_provisional")

def should_show_bonus(fixtures: List[Dict], player_id: int, player_team_map: Dict[int, int]) -> bool:
    """
    Determine if bonus should be shown for a player based on fixture status.
    Returns True only if ALL fixtures involving the player's team are finished.
    """
    team_id = player_team_map.get(player_id)
    if not team_id:
        return False
    
    # Find all fixtures for this team
    team_fixtures = [f for f in fixtures if f.get("team_h") == team_id or f.get("team_a") == team_id]
    
    if not team_fixtures:
        return False
    
    # All fixtures must be finished to show bonus
    return all(is_fixture_finished(f) for f in team_fixtures)

def calc_dc_points(stats: Dict[str, Any], pos: int) -> int:
    """
    DC points (official):
    - DEF (2): >=10 CBIT -> +2
    - MID/FWD (3/4): >=12 CBIT -> +2
    - GK (1): 0
    """
    if pos == 1:
        return 0
    dc_count = get_stat_int(stats, "defensive_contributions", "cbit", "cbits", "def_contributions")
    if pos == 2:
        return 2 if dc_count >= 10 else 0
    if pos in (3, 4):
        return 2 if dc_count >= 12 else 0
    return 0

# ---------- Baseline Extraction ----------
def extract_current_counts(live_elements: List[Dict]) -> Dict[Tuple[int, str, int, int], int]:
    counts: Dict[Tuple[int, str, int, int], int] = {}
    for el in live_elements:
        pid = el.get("id")
        explain = el.get("explain", [])
        stats_overall = el.get("stats", {})
        minutes_total = stats_overall.get("minutes", 0)
        for fixture_block in explain:
            fixture_id = fixture_block.get("fixture")
            stats_list = fixture_block.get("stats", [])
            if not isinstance(fixture_id, int):
                continue
            for st in stats_list:
                identifier = st.get("identifier")
                value = st.get("value")
                if identifier in STAT_KEYS and isinstance(value, int):
                    key = (fixture_id, identifier, pid, minutes_total)
                    counts[key] = value
    return counts

# ---------- Diff & New Events ----------
async def diff_new_events(season: str, gw: int, counts: Dict[Tuple[int,str,int,int], int]) -> Dict[int, List[Dict]]:
    new_events_by_fixture: Dict[int, List[Dict]] = {}
    
    # Batch GET all keys
    keys_to_fetch = [key_event(season, gw, fixture_id, identifier, player_id) 
                     for (fixture_id, identifier, player_id, minutes) in counts.keys()]
    stored_values = await r_get_many(keys_to_fetch)
    
    # Batch SET updates
    updates = {}
    
    for (fixture_id, identifier, player_id, minutes), current_val in counts.items():
        key = key_event(season, gw, fixture_id, identifier, player_id)
        stored = stored_values.get(key)
        if stored is None:
            stored = 0
        if current_val > stored:
            delta = current_val - stored
            for _ in range(delta):
                new_events_by_fixture.setdefault(fixture_id, []).append(
                    {"type": identifier, "player": player_id, "minutes": minutes}
                )
            updates[key] = current_val
        elif current_val < stored:
            updates[key] = current_val
    
    # Batch write all updates
    if updates:
        await r_set_many(updates)
    
    return new_events_by_fixture

# ---------- Clean Sheets ----------
def fixture_team_conceded(fix: Dict, team_id: int) -> int:
    th = fix.get("team_h")
    ta = fix.get("team_a")
    sh = fix.get("team_h_score")
    sa = fix.get("team_a_score")
    if sh is None: sh = 0
    if sa is None: sa = 0
    if team_id == th: return sa
    if team_id == ta: return sh
    return 0

async def process_clean_sheets(season: str, gw: int, fixtures: List[Dict],
                               live_elements: List[Dict],
                               league_player_ids: Set[int],
                               player_team_map: Dict[int, int]) -> Dict[int, List[str]]:
    messages: Dict[int, List[str]] = {}
    cs_locked_key = key_cs_locked(season, gw)
    cs_final_key = key_cs_final(season, gw)
    for el in live_elements:
        pid = el.get("id")
        if pid not in league_player_ids:
            continue
        stats = el.get("stats", {})
        minutes = stats.get("minutes", 0)
        team_id = player_team_map.get(pid)
        if not team_id:
            continue
        player_fixes = [f for f in fixtures if f.get("team_h") == team_id or f.get("team_a") == team_id]
        for fx in player_fixes:
            fixture_id = fx.get("id")
            if not isinstance(fixture_id, int):
                continue
            conceded = fixture_team_conceded(fx, team_id)
            finished = fx.get("finished")
            member = f"{fixture_id}:{pid}"
            if not finished and conceded == 0 and 60 <= minutes < 90:
                exists = await r_sismember(cs_locked_key, member)
                if not exists:
                    await r_sadd(cs_locked_key, member)
                    messages.setdefault(fixture_id, []).append(f"Ранний кленшит: {player_name_map.get(pid, f'Player{pid}')}")
            if finished and conceded == 0 and minutes >= 60:
                final_exists = await r_sismember(cs_final_key, member)
                if not final_exists:
                    await r_sadd(cs_final_key, member)
                    early_exists = await r_sismember(cs_locked_key, member)
                    if early_exists:
                        messages.setdefault(fixture_id, []).append(f"Кленшит подтверждён: {player_name_map.get(pid, f'Player{pid}')}")
                    else:
                        messages.setdefault(fixture_id, []).append(f"Кленшит: {player_name_map.get(pid, f'Player{pid}')}")
    return messages

# ---------- Pair & Format Events ----------
def pair_goals_assists(events_by_fixture: Dict[int, List[Dict]]) -> Dict[int, List[str]]:
    output: Dict[int, List[str]] = {}
    for fixture_id, evs in events_by_fixture.items():
        goals = [e for e in evs if e["type"] == "goals_scored"]
        assists = [e for e in evs if e["type"] == "assists"]
        others = [e for e in evs if e["type"] not in ("goals_scored", "assists")]
        used_assist_indices: Set[int] = set()
        lines: List[str] = []
        # Goals + assists pairing
        for g in goals:
            scorer_name = player_name_map.get(g["player"], f"Player{g['player']}")
            assist_name = None
            for idx, a in enumerate(assists):
                if idx not in used_assist_indices:
                    assist_name = player_name_map.get(a["player"], f"Player{a['player']}")
                    used_assist_indices.add(idx)
                    break
            if assist_name:
                lines.append(f"Гол: {scorer_name} (ассист: {assist_name})")
            else:
                lines.append(f"Гол: {scorer_name}")
        # Leftover assists
        for idx, a in enumerate(assists):
            if idx not in used_assist_indices:
                assist_name = player_name_map.get(a["player"], f"Player{a['player']}")
                lines.append(f"Ассист: {assist_name}")
        # Other events
        for o in others:
            pname = player_name_map.get(o["player"], f"Player{o['player']}")
            t = o["type"]
            if t == "yellow_cards":
                lines.append(f"Желтая карточка: {pname}")
            elif t == "red_cards":
                lines.append(f"Красная карточка: {pname}")
            elif t == "penalties_missed":
                lines.append(f"Пенальти не забит: {pname}")
            elif t == "penalties_saved":
                lines.append(f"Отбит пенальти: {pname}")
            elif t == "own_goals":
                lines.append(f"Автогол: {pname}")
            else:
                lines.append(f"{t}: {pname}")
        if lines:
            output[fixture_id] = lines
    return output

# ---------- Commands ----------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Я FPL-бот 🚀")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Команды:\n"
        "/start — приветствие\n"
        f"/points — очки за последний завершённый тур лиги {LEAGUE_ID}\n"
        "/gw <номер> — очки за тур\n"
        "/transfers <номер> — трансферы за тур\n"
        "/month <номер> — очки за месяц (1-12)\n"
        f"/rank — положение в лиге {LEAGUE_ID}\n"
        "/deadline — время до ближайшего дедлайна (UTC+5)\n"
        "/gwinfo <номер> — таблица игроков (live)\n"
        "/liveon — включить мониторинг\n"
        "/liveoff — выключить мониторинг\n"
        "/con — конфигурация"
    )
    await update.message.reply_text(text)

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs = await get_bootstrap()
    if not bs:
        await update.message.reply_text("bootstrap недоступен")
        return
    events = bs.get("events", [])
    last_finished = choose_last_finished_gw(events)
    if not last_finished:
        await update.message.reply_text("Нет завершённого тура.")
        return
    await send_league_points(update, LEAGUE_ID, last_finished, events, header_override=None)

async def gw_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Использование: /gw <номер>")
        return
    gw_num = int(args[0])
    bs = await get_bootstrap()
    if not bs:
        await update.message.reply_text("bootstrap недоступен")
        return
    events = bs.get("events", [])
    if not events:
        await update.message.reply_text("Нет данных о турах.")
        return
    max_gw = max(e.get("id", 0) for e in events)
    if gw_num < 1 or gw_num > max_gw:
        await update.message.reply_text(f"Тур вне диапазона 1..{max_gw}")
        return
    sel = next((e for e in events if e.get("id") == gw_num), None)
    if not sel:
        await update.message.reply_text("Тур не найден.")
        return
    finished = sel.get("finished")
    is_current = sel.get("is_current")
    if not finished and not is_current and not sel.get("data_checked", False):
        await update.message.reply_text("Тур не стартовал или нет данных.")
        return
    header = f"*Очки за тур {gw_num}*"
    if not finished:
        header += " (ещё не завершён)"
    header += "\n\n"
    await send_league_points(update, LEAGUE_ID, gw_num, events, header_override=header)

async def rank_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    results = await get_league_results_cached(LEAGUE_ID)
    if results is None:
        await update.message.reply_text("Standings недоступен")
        return
    sorted_res = sorted(results, key=lambda r: r.get("rank", 10**9))
    lines = ["*Текущее положение:*\n"]
    for r in sorted_res:
        rank = r.get("rank")
        last_rank = r.get("last_rank")
        total = r.get("total")
        entry_name = r.get("entry_name")
        player_name = r.get("player_name")
        change = ""
        if isinstance(rank, int) and isinstance(last_rank, int) and rank > 0 and last_rank > 0:
            delta = last_rank - rank
            if delta > 0:
                change = f" ↑{delta}"
            elif delta < 0:
                change = f" ↓{abs(delta)}"
            else:
                change = " →0"
        lines.append(f"{rank}. {player_name} — {entry_name}: {total} pts{change}")
    text = "\n".join(lines)
    for chunk in split_message_chunks(text):
        try:
            await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception:
            await update.message.reply_text(chunk)

async def deadline_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bs = await get_bootstrap()
    if not bs:
        await update.message.reply_text("bootstrap недоступен")
        return
    events = bs.get("events", [])
    target = find_next_deadline_event(events)
    if not target:
        await update.message.reply_text("Нет будущих дедлайнов.")
        return
    dt_utc = parse_deadline(target.get("deadline_time"))
    if not dt_utc:
        await update.message.reply_text("Ошибка парсинга дедлайна.")
        return
    local = dt_utc + timedelta(hours=5)
    now_local = datetime.now(timezone.utc) + timedelta(hours=5)
    delta_seconds = int((local - now_local).total_seconds())
    human = format_timedelta(delta_seconds)
    gw_name = target.get("name", "Gameweek")
    local_str = local.strftime("%Y-%m-%d %H:%M:%S UTC+5")
    text = f"*Ближайший дедлайн:* {gw_name}\nКогда: {local_str}\nОсталось: {human}"
    try:
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception:
        await update.message.reply_text(text)

# ---------- /gwinfo ----------
async def gwinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Использование: /gwinfo <номер>")
        return
    gw = int(args[0])

    bs = await get_bootstrap()
    if not bs:
        await update.message.reply_text("bootstrap недоступен")
        return
    events = bs.get("events", [])
    if not events:
        await update.message.reply_text("Нет events")
        return
    max_gw = max(e.get("id", 0) for e in events)
    if gw < 1 or gw > max_gw:
        await update.message.reply_text(f"Тур вне диапазона 1..{max_gw}")
        return

    live = await fetch_json(fpl_url(event_live_path_tpl.format(gw=gw)))
    fixtures = await get_fixtures_cached(gw, ttl=900)
    if not live or not fixtures:
        await update.message.reply_text("live/fixtures недоступны.")
        return

    league_player_ids = await get_league_player_ids(gw)
    owners_count_map = await get_league_player_owners(gw)  # player_id -> owners_count

    live_elements = live.get("elements", [])
    if not player_name_map:
        await get_bootstrap()

    pos_map = build_player_position_map()
    player_team_map_local = build_player_team_map()

    rows = []
    for el in live_elements:
        pid = el.get("id")
        if pid not in league_player_ids:
            continue
        stats = el.get("stats", {}) or {}
        total_points = int(stats.get("total_points", 0) or 0)
        pos = pos_map.get(pid, 0)

        # Build Stats tokens in strict order:
        # G, A, CS, DC, YC, RC, GKS, OG, PenM, PenS, B
        stat_parts: List[str] = []

        # Goals
        g = int(stats.get("goals_scored", 0) or 0)
        if g > 0:
            stat_parts.append(f"G{g}")

        # Assists
        a = int(stats.get("assists", 0) or 0)
        if a > 0:
            stat_parts.append(f"A{a}")

        # Clean sheet (show for GK/DEF/MID; suppress for FWD)
        cs = int(stats.get("clean_sheets", 0) or 0)
        if cs > 0 and pos != 4:
            stat_parts.append("CS")

        # DC (presence only)
        dc_pts = calc_dc_points(stats, pos)
        if dc_pts > 0:
            stat_parts.append("DC")

        # Yellow cards (with number)
        yc = int(stats.get("yellow_cards", 0) or 0)
        if yc > 0:
            stat_parts.append(f"YC{yc}")

        # Red cards (presence only)
        rc = int(stats.get("red_cards", 0) or 0)
        if rc > 0:
            stat_parts.append("RC")

        # Goalkeeper saves points (saves // 3) as GKS
        if pos == 1:
            saves = int(stats.get("saves", 0) or 0)
            gks = saves // 3
            if gks > 0:
                stat_parts.append(f"GKS{gks}")

        # Own goals (with number)
        og = int(stats.get("own_goals", 0) or 0)
        if og > 0:
            stat_parts.append(f"OG{og}")

        # Penalties missed (PenM)
        pen_m = int(stats.get("penalties_missed", 0) or 0)
        if pen_m > 0:
            stat_parts.append(f"PenM{pen_m}")

        # Penalties saved (PenS)
        pen_s = int(stats.get("penalties_saved", 0) or 0)
        if pen_s > 0:
            stat_parts.append(f"PenS{pen_s}")

        # Bonus (always last) - ONLY if player's fixtures are finished
        bonus_val = int(stats.get("bonus", 0) or 0)
        if bonus_val > 0 and should_show_bonus(fixtures, pid, player_team_map_local):
            stat_parts.append(f"B{bonus_val}")

        # Filter out pure appearance-only (<=2 pts and no stats)
        if (not stat_parts) and (total_points <= 2):
            continue

        name = player_name_map.get(pid, f"Player{pid}")
        owners_count = int(owners_count_map.get(pid, 0))

        rows.append({
            "name": name,
            "stats": " ".join(stat_parts) if stat_parts else "-",
            "pts": total_points,
            "own": owners_count
        })

    if not rows:
        await update.message.reply_text("Нет игроков с событиями или значимыми очками.")
        return

    # Sort by Pts desc, then ownership desc, then name asc
    rows.sort(key=lambda r: (-r["pts"], -r["own"], r["name"].lower()))

    # Alignment: compute widths once (prevents columns from shifting even with tokens like YC1)
    name_w = max(len(r["name"]) for r in rows + [{"name": "Player"}])
    stats_w = max(len(r["stats"]) for r in rows + [{"stats": "Stats"}])
    pts_w = max(len(str(r["pts"])) for r in rows + [{"pts": "Pts"}])

    header = f"{'Player'.ljust(name_w)}  {'Stats'.ljust(stats_w)}  {'Pts'.rjust(pts_w)}"
    sep = "-" * len(header)

    lines = [f"*GW {gw} — Игроки лиги (live)*", "```", header, sep]
    for r in rows:
        line = (
            f"{r['name'].ljust(name_w)}  "
            f"{r['stats'].ljust(stats_w)}  "
            f"{str(r['pts']).rjust(pts_w)}"
        )
        lines.append(line)
    lines.append("```")

    full_text = "\n".join(lines)
    for chunk in split_message_chunks(full_text):
        try:
            await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception:
            safe_text = full_text.replace("```", "")
            for c2 in split_message_chunks(safe_text):
                await update.message.reply_text(c2)
            break

# ---------- Live On/Off ----------
current_target_chat: Optional[int] = None
live_monitor_enabled = ENABLE_LIVE_MONITOR
player_name_map: Dict[int, str] = {}

async def liveon_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global live_monitor_enabled, current_target_chat
    live_monitor_enabled = True
    if TARGET_CHAT_ID:
        await update.message.reply_text("Мониторинг включён. Используется TARGET_CHAT_ID.")
    else:
        current_target_chat = update.effective_chat.id
        await update.message.reply_text(f"Мониторинг включён. События будут приходить сюда (chat_id={current_target_chat}).")

async def liveoff_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global live_monitor_enabled, current_target_chat
    live_monitor_enabled = False
    if not TARGET_CHAT_ID:
        current_target_chat = None
    await update.message.reply_text("Мониторинг выключен.")

# ---------- /month Command ----------
async def month_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Aggregate month results from GW snapshots.
    Usage: /month <month_number> (1=August, 2=September, etc.)
    """
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Использование: /month <номер месяца> (1-12)")
        return
    
    month_num = int(args[0])
    if month_num < 1 or month_num > 12:
        await update.message.reply_text("Месяц должен быть от 1 до 12")
        return
    
    bs = await get_bootstrap_cached()
    if not bs:
        await update.message.reply_text("bootstrap недоступен")
        return
    
    season = discover_season_tag(bs)
    events = bs.get("events", [])
    
    # Find GWs that belong to this month (simplified - use month from deadline)
    month_gws = []
    for e in events:
        deadline = parse_deadline(e.get("deadline_time"))
        if deadline and deadline.month == month_num:
            month_gws.append(e.get("id"))
    
    if not month_gws:
        await update.message.reply_text(f"Нет туров для месяца {month_num}")
        return
    
    # Get current roster
    results = await get_league_results_cached(LEAGUE_ID)
    if not results:
        await update.message.reply_text("Standings недоступен")
        return
    
    current_roster = [r.get("entry") for r in results if r.get("entry")]
    
    # Aggregate from snapshots
    entry_totals: Dict[int, int] = {}
    
    for gw in sorted(month_gws):
        snapshot = await get_or_build_gw_snapshot(season, LEAGUE_ID, gw, events, current_roster)
        if snapshot:
            for entry_data in snapshot.get("entries", []):
                entry_id = entry_data.get("id")
                pts = entry_data.get("gw_points")
                if entry_id and pts is not None:
                    entry_totals[entry_id] = entry_totals.get(entry_id, 0) + pts
    
    if not entry_totals:
        await update.message.reply_text(f"Нет данных для месяца {month_num}")
        return
    
    # Resolve names from standings and format output
    entry_names = {r.get("entry"): (r.get("player_name"), r.get("entry_name")) 
                   for r in results if r.get("entry")}
    
    sorted_entries = sorted(entry_totals.items(), key=lambda x: -x[1])
    
    lines = [f"*Очки за месяц {month_num} (GWs {min(month_gws)}-{max(month_gws)}):*\n"]
    for rank, (entry_id, total) in enumerate(sorted_entries, 1):
        player_name, entry_name = entry_names.get(entry_id, ("Unknown", "Unknown"))
        lines.append(f"{rank}. {player_name} — {entry_name}: {total}")
    
    full_text = "\n".join(lines)
    await send_text(update, full_text, parse_mode="Markdown")

# ---------- /transfers Command ----------
async def transfers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Show transfer activity for a gameweek.
    Usage: /transfers <gw_number>
    """
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Использование: /transfers <номер тура>")
        return
    
    gw_num = int(args[0])
    
    bs = await get_bootstrap_cached()
    if not bs:
        await update.message.reply_text("bootstrap недоступен")
        return
    
    season = discover_season_tag(bs)
    events = bs.get("events", [])
    
    # Get current roster
    results = await get_league_results_cached(LEAGUE_ID)
    if not results:
        await update.message.reply_text("Standings недоступен")
        return
    
    current_roster = [r.get("entry") for r in results if r.get("entry")]
    
    # Get or build snapshot
    snapshot = await get_or_build_gw_snapshot(season, LEAGUE_ID, gw_num, events, current_roster)
    if not snapshot:
        await update.message.reply_text(f"Нет данных для тура {gw_num}")
        return
    
    # Resolve names and format output
    entry_names = {r.get("entry"): (r.get("player_name"), r.get("entry_name")) 
                   for r in results if r.get("entry")}
    
    lines = [f"*Трансферы GW {gw_num}:*\n"]
    
    # Sort by transfers descending
    entries_with_transfers = [e for e in snapshot.get("entries", []) 
                             if e.get("transfers") and e.get("transfers") > 0]
    sorted_entries = sorted(entries_with_transfers, 
                           key=lambda x: -x.get("transfers", 0))
    
    if not sorted_entries:
        lines.append("Нет трансферов в этом туре")
    else:
        for entry_data in sorted_entries:
            entry_id = entry_data.get("id")
            transfers = entry_data.get("transfers", 0)
            cost = entry_data.get("transfer_cost", 0)
            player_name, entry_name = entry_names.get(entry_id, ("Unknown", "Unknown"))
            cost_str = f" (-{cost})" if cost > 0 else ""
            lines.append(f"{player_name} — {entry_name}: {transfers}{cost_str}")
    
    full_text = "\n".join(lines)
    await send_text(update, full_text, parse_mode="Markdown")

# ---------- League Points ----------
async def send_league_points(
    update: Update,
    league_id: str,
    gw_num: int,
    events: List[Dict],
    header_override: Optional[str] = None
):
    results = await get_league_results_cached(league_id)
    if results is None:
        await update.message.reply_text("Standings недоступен")
        return
    header = header_override or f"*Очки за тур {gw_num}:*\n\n"
    lines: List[str] = []
    
    # Check if this is the current GW to use optimization
    current_ev = next((e for e in events if e.get("is_current") and e.get("id") == gw_num), None)
    use_event_total = current_ev is not None

    async def one(entry_id: int, entry_name: str, player_name: str, result: Dict) -> str:
        pts = None
        # Optimization: For current GW, prefer standings event_total if available
        if use_event_total and result.get("event_total") is not None:
            pts = result.get("event_total")
            logger.debug(f"Using event_total for entry {entry_id} GW {gw_num}: {pts}")
        else:
            # Fall back to picks API
            data = await get_entry_picks_cached(entry_id, gw_num)
            if data:
                pts = data.get("entry_history", {}).get("points")
        return f"{player_name} — {entry_name}: {pts if pts is not None else 'нет данных'}"

    tasks = [asyncio.create_task(one(r["entry"], r["entry_name"], r["player_name"], r)) for r in results]
    res = await asyncio.gather(*tasks, return_exceptions=True)
    for item in res:
        if isinstance(item, Exception):
            lines.append("Ошибка участника: нет данных")
        else:
            lines.append(item)
    full_text = header + "\n".join(lines)
    for chunk in split_message_chunks(full_text):
        try:
            await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception:
            await update.message.reply_text(chunk)

# ---------- Live Monitor Loop ----------
async def live_monitor_loop():
    logger.info("Live monitor loop started")
    while not stop_event.is_set():
        try:
            if not live_monitor_enabled:
                await asyncio.sleep(5)
                continue

            bs = await get_bootstrap_cached()
            if not bs:
                await asyncio.sleep(LIVE_POLL_INTERVAL)
                continue
            season = discover_season_tag(bs)
            events = bs.get("events", [])
            
            # Track only active gameweeks: current, previous (if still processing), next (for triggers)
            current_ev = next((e for e in events if e.get("is_current")), None)
            
            if not current_ev:
                # No current gameweek, use adaptive polling
                next_kickoff = find_next_kickoff(events)
                poll_interval = next_poll_interval(datetime.now(timezone.utc), next_kickoff)
                logger.debug(f"No current GW, sleeping {poll_interval}s")
                await asyncio.sleep(poll_interval)
                continue
            
            gw = current_ev.get("id")
            if not isinstance(gw, int):
                await asyncio.sleep(LIVE_POLL_INTERVAL)
                continue
            
            # Check if current GW is finished
            if current_ev.get("finished"):
                # Current GW finished, wait longer
                next_kickoff = find_next_kickoff(events)
                poll_interval = next_poll_interval(datetime.now(timezone.utc), next_kickoff)
                logger.debug(f"GW {gw} finished, sleeping {poll_interval}s")
                await asyncio.sleep(poll_interval)
                continue

            # Use cached fixtures
            fixtures = await get_fixtures_cached(gw, ttl=900)
            if not fixtures:
                logger.warning(f"Fixtures unavailable for GW {gw}")
                await asyncio.sleep(LIVE_POLL_INTERVAL)
                continue
            
            # Determine adaptive poll interval based on fixtures
            next_kickoff = find_next_kickoff(events, fixtures)
            poll_interval = next_poll_interval(datetime.now(timezone.utc), next_kickoff)
            
            live = await fetch_json(fpl_url(event_live_path_tpl.format(gw=gw)))
            if not live:
                logger.debug(f"Live data unavailable for GW {gw}")
                await asyncio.sleep(poll_interval)
                continue

            league_players = await get_league_player_ids(gw)
            live_elements = live.get("elements", [])

            baseline_key = key_baseline(season, gw)
            baseline_done = await r_flag_exists(baseline_key)
            counts = extract_current_counts(live_elements)
            counts = {k: v for k, v in counts.items() if k[2] in league_players}

            if not baseline_done:
                for (fixture_id, stat, player_id, _minutes), val in counts.items():
                    await r_set(key_event(season, gw, fixture_id, stat, player_id), val)
                await r_set_flag(baseline_key)
                logger.info(f"Baseline set for GW {gw}.")
                await asyncio.sleep(poll_interval)
                continue

            new_events_by_fixture = await diff_new_events(season, gw, counts)
            paired_lines = pair_goals_assists(new_events_by_fixture)

            player_team_map_local = build_player_team_map()
            cs_messages = await process_clean_sheets(season, gw, fixtures, live_elements,
                                                     league_players, player_team_map_local)

            team_short = build_team_short_map()
            fixture_index = {f.get("id"): f for f in fixtures if isinstance(f.get("id"), int)}

            messages: List[str] = []
            for fixture_id in set(list(paired_lines.keys()) + list(cs_messages.keys())):
                fx = fixture_index.get(fixture_id, {})
                th = fx.get("team_h"); ta = fx.get("team_a")
                sh = fx.get("team_h_score"); sa = fx.get("team_a_score")
                if sh is None: sh = 0
                if sa is None: sa = 0
                head = f"*Матч:* {team_short.get(th,'T?')} {sh}–{sa} {team_short.get(ta,'T?')}"
                lines = [head]
                if fixture_id in paired_lines:
                    lines.extend(paired_lines[fixture_id])
                if fixture_id in cs_messages:
                    lines.extend(cs_messages[fixture_id])
                if len(lines) > 1:
                    messages.append("\n".join(lines))

            if messages:
                final_text = "\n\n".join(messages)
                target_chat_id = None
                if TARGET_CHAT_ID:
                    try:
                        target_chat_id = int(TARGET_CHAT_ID)
                    except ValueError:
                        target_chat_id = None
                elif current_target_chat is not None:
                    target_chat_id = current_target_chat

                if target_chat_id:
                    for chunk in split_message_chunks(final_text):
                        try:
                            await application.bot.send_message(
                                chat_id=target_chat_id, text=chunk, parse_mode="Markdown"
                            )
                        except Exception:
                            logger.exception("Failed sending live notification.")
                else:
                    logger.debug("Live events (no chat bound): " + final_text)

            # Use adaptive poll interval
            logger.debug(f"Live monitor sleeping {poll_interval}s")
            await asyncio.sleep(poll_interval)
        except Exception as ex:
            logger.exception(f"Error in live_monitor_loop: {ex}")
            await asyncio.sleep(LIVE_POLL_INTERVAL)
    logger.info("Live monitor loop stopped")

# ---------- Error Handler ----------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception", exc_info=context.error)

# ---------- Commands Menu ----------
async def setup_bot_commands(bot):
    commands = [
        BotCommand("start", "Приветствие"),
        BotCommand("help", "Справка"),
        BotCommand("points", "Очки (последний завершённый)"),
        BotCommand("gw", "Очки за тур"),
        BotCommand("transfers", "Трансферы за тур"),
        BotCommand("month", "Очки за месяц"),
        BotCommand("rank", "Положение в лиге"),
        BotCommand("deadline", "До дедлайна (UTC+5)"),
        BotCommand("gwinfo", "События тура"),
        BotCommand("liveon", "Включить мониторинг"),
        BotCommand("liveoff", "Выключить мониторинг"),
        BotCommand("con", "Конфигурация"),
    ]
    await bot.set_my_commands(commands)
    try:
        await bot.set_my_commands(commands, language_code="ru")
    except Exception:
        pass
    logger.info("Bot commands set.")

# ---------- Configuration Snapshot ----------
def con() -> Dict[str, Any]:
    """
    Возвращает текущую конфигурацию и статус (для дебага / инспекции).
    """
    return {
        "BOT_TOKEN_set": bool(BOT_TOKEN),
        "TARGET_CHAT_ID": TARGET_CHAT_ID,
        "LEAGUE_ID": LEAGUE_ID,
        "FPL_PROXY_BASE": FPL_PROXY_BASE,
        "ENABLE_LIVE_MONITOR": ENABLE_LIVE_MONITOR,
        "LIVE_POLL_INTERVAL": LIVE_POLL_INTERVAL,
        "FPL_CACHE_TTL": FPL_CACHE_TTL,
        "FPL_STANDINGS_TTL": FPL_STANDINGS_TTL,
        "FPL_PICKS_TTL": FPL_PICKS_TTL,
        "FPL_PICKS_ALLOW_STALE": FPL_PICKS_ALLOW_STALE,
        "FPL_CONCURRENCY": FPL_CONCURRENCY,
        "REDIS_GW_TTL": REDIS_GW_TTL,
        "ENABLE_HTTP2": ENABLE_HTTP2,
        "USE_WEBHOOK": USE_WEBHOOK,
        "Redis_connected": redis_client is not None,
        "Season_tag": SEASON_TAG,
    }

async def con_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    snapshot = con()
    lines = ["*Конфигурация:*"]
    for k, v in snapshot.items():
        lines.append(f"{k}: {v}")
    text = "\n".join(lines)
    try:
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception:
        await update.message.reply_text(text)

# ---------- run_bot ----------
async def run_bot():
    global http_client, application, player_name_map
    logger.info("Starting bot...")

    init_redis()

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=50)
    use_http2 = ENABLE_HTTP2
    if use_http2:
        try:
            import h2  # noqa: F401
        except ImportError:
            logger.warning("h2 not found, disabling HTTP/2.")
            use_http2 = False

    http_client = httpx.AsyncClient(http2=use_http2, limits=limits, timeout=15.0)
    logger.info(f"httpx client created (http2={use_http2})")

    application = Application.builder().token(BOT_TOKEN).concurrent_updates(TELEGRAM_CONCURRENCY).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("points", points_command))
    application.add_handler(CommandHandler("gw", gw_command))
    application.add_handler(CommandHandler("transfers", transfers_command))
    application.add_handler(CommandHandler("month", month_command))
    application.add_handler(CommandHandler("rank", rank_command))
    application.add_handler(CommandHandler("deadline", deadline_command))
    application.add_handler(CommandHandler("gwinfo", gwinfo_command))
    application.add_handler(CommandHandler("liveon", liveon_command))
    application.add_handler(CommandHandler("liveoff", liveoff_command))
    application.add_handler(CommandHandler("con", con_command))
    application.add_error_handler(error_handler)

    await application.initialize()
    await application.start()

    try:
        await setup_bot_commands(application.bot)
    except Exception:
        logger.exception("Failed set commands")

    bs = await get_bootstrap()
    if bs:
        global player_name_map
        player_name_map = build_player_name_map()
    else:
        player_name_map = {}

    if USE_WEBHOOK:
        logger.info("Webhook mode placeholder (not implemented).")
    else:
        try:
            await application.updater.start_polling()
            logger.info("Polling started.")
        except Exception:
            logger.exception("Failed start polling")
            try:
                await application.stop()
                await application.shutdown()
            except Exception:
                logger.exception("Shutdown after polling failure")
            try:
                if http_client:
                    await http_client.aclose()
            except Exception:
                logger.exception("Close http client failed")
            return

    asyncio.create_task(live_monitor_loop())

    try:
        me = await application.bot.get_me()
        logger.info("Bot started as @%s (id=%s)", getattr(me, 'username', 'unknown'), getattr(me, 'id', 'unknown'))
    except Exception:
        logger.exception("get_me failed")

    logger.info("Waiting for stop_event...")
    try:
        await stop_event.wait()
    finally:
        logger.info("Shutdown initiated.")
        if not USE_WEBHOOK:
            try:
                await application.updater.stop()
            except Exception:
                logger.exception("Updater stop error")
        try:
            await application.stop()
            await application.shutdown()
        except Exception:
            logger.exception("Application shutdown error")
        try:
            if http_client:
                await http_client.aclose()
        except Exception:
            logger.exception("HTTP client close error")
        logger.info("Shutdown complete.")

# ---------- Signals ----------
def handle_sigterm(signum, frame):
    logger.info("SIGTERM/SIGINT received -> stopping.")
    try:
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(stop_event.set)
    except RuntimeError:
        pass

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

# ---------- Entrypoint ----------
if __name__ == "__main__":
    print("Main entrypoint start")
    if ENABLE_KILL:
        kill_existing_instances()
    logger.info("Lock acquired (placeholder)")
    flask_thread = threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask thread started")
    try:
        asyncio.run(run_bot())
    except Exception:
        logger.exception("Application exited with exception")
    finally:
        print("Main entrypoint finished")
