"""
FPL Telegram Bot (Variant 1, PTB v21 manual polling) + caching for standings and picks:
- Manual polling (initialize + start + updater.start_polling)
- Anti-403 headers + optional HTTP/2
- Optional Cloudflare Worker proxy (FPL_PROXY_BASE)
- Bootstrap cache with TTL (minutes, FPL_CACHE_TTL)
- NEW: Standings cache with TTL (seconds, FPL_STANDINGS_TTL)
- NEW: Entry picks cache with TTL (seconds, FPL_PICKS_TTL) + per-key locks, optional stale-on-error
- Fallback: events from FPL_EVENTS_JSON
- Concurrency limiting via semaphore
- Commands: /points, /gw, /rank, /deadline, /help
- Message splitting (Telegram 4096 limit)
- Graceful shutdown
- Flask health endpoint
"""

import os
import json
import asyncio
import threading
import logging
import time
import signal
import random
from typing import Any, Dict, Optional, List, Tuple
from datetime import datetime, timezone

from flask import Flask, jsonify
import httpx
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update, BotCommand

# ---------- 1. ENV ----------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Environment variable BOT_TOKEN is required")

ENABLE_KILL = os.environ.get("ENABLE_KILL", "0") == "1"

# bootstrap-static cache TTL in minutes
FPL_CACHE_TTL = int(os.environ.get("FPL_CACHE_TTL", "8"))

# concurrency for outgoing HTTP requests
FPL_CONCURRENCY = int(os.environ.get("FPL_CONCURRENCY", "3"))

# standings cache TTL in seconds (league standings aggregation)
FPL_STANDINGS_TTL = int(os.environ.get("FPL_STANDINGS_TTL", "60"))

# picks cache TTL in seconds ((entry_id, gw) picks endpoint)
FPL_PICKS_TTL = int(os.environ.get("FPL_PICKS_TTL", "300"))

# serve stale picks if upstream failed (within picks TTL)
FPL_PICKS_ALLOW_STALE = os.environ.get("FPL_PICKS_ALLOW_STALE", "1") == "1"

PORT = int(os.environ.get("PORT", 10000))
TELEGRAM_CONCURRENCY = int(os.environ.get("TELEGRAM_CONCURRENCY", "4"))
USE_WEBHOOK = os.environ.get("USE_WEBHOOK", "0") == "1"
LEAGUE_ID = os.environ.get("LEAGUE_ID", "980121")
FPL_PROXY_BASE = os.environ.get("FPL_PROXY_BASE", "").rstrip("/")
ENABLE_HTTP2 = os.environ.get("ENABLE_HTTP2", "1") == "1"

stop_event = asyncio.Event()

# ---------- 2. Logging ----------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("fpl_bot")

# ---------- 3. Flask ----------
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
        "concurrency_limit": FPL_CONCURRENCY,
        "proxy_base": FPL_PROXY_BASE or None
    })

def start_flask():
    logger.info(f"Starting Flask app on port {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT)

def kill_existing_instances():
    logger.info("ENABLE_KILL is set (placeholder for external coordination)")

# ---------- 4. HTTP / Cache / Headers ----------

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

# bootstrap cache (raw dict from bootstrap-static)
bootstrap_cache: Dict[str, Any] = {}
bootstrap_cache_ts: Optional[float] = None
bootstrap_lock = asyncio.Lock()

# standings cache (aggregated list over all pages)
standings_cache: Dict[str, Any] = {}  # {"results": List[Dict], "pages": int}
standings_cache_ts: Optional[float] = None
standings_lock = asyncio.Lock()

# picks cache: key=(entry_id, gw) -> {"data": Dict, "ts": float}
picks_cache: Dict[Tuple[int, int], Dict[str, Any]] = {}
# per-key locks to avoid stampede
_picks_locks: Dict[Tuple[int, int], asyncio.Lock] = {}
_picks_locks_guard = asyncio.Lock()

def _picks_get_lock(key: Tuple[int, int]) -> asyncio.Lock:
    # fast path without await: may race but okay; ensure creation guarded below
    lock = _picks_locks.get(key)
    if lock is not None:
        return lock
    # guarded creation
    # Note: This is a sync helper called within async funcs; we can't await here.
    # We'll create a new lock here and set under guard in async helper below.
    return asyncio.Lock()

async def _picks_get_lock_guarded(key: Tuple[int, int]) -> asyncio.Lock:
    async with _picks_locks_guard:
        lock = _picks_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _picks_locks[key] = lock
        return lock

bootstrap_path = "/api/bootstrap-static/"
league_path_tpl = "/api/leagues-classic/{league_id}/standings/?page_standings={page}"
entry_picks_path_tpl = "/api/entry/{entry_id}/event/{gw}/picks/"

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
                    logger.warning(f"JSON decode error for {url}: {jex}")
                    return None

            body_preview = (resp.text or "")[:300].replace("\n", " ")
            logger.warning(f"Attempt {attempt}: status={status} url={url} body='{body_preview}'")

            if status in (403, 429) or 500 <= status < 600:
                if attempt < max_attempts:
                    sleep_time = min(backoff_base ** attempt + random.uniform(0, 0.5), 8.0)
                    await asyncio.sleep(sleep_time)
                    continue
            return None

        except httpx.TimeoutException:
            logger.error(f"Timeout fetching {url} (attempt {attempt})")
            if attempt < max_attempts:
                await asyncio.sleep(min(backoff_base ** attempt, 8.0))
        except Exception as ex:
            logger.exception(f"Unexpected error fetching {url} (attempt {attempt}): {ex}")
            if attempt < max_attempts:
                await asyncio.sleep(min(backoff_base ** attempt, 8.0))
    return None

# ---------- bootstrap cache ----------

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
        logger.error(f"Failed to parse FPL_EVENTS_JSON: {ex}")
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
        logger.warning("Using FPL_EVENTS_JSON fallback for bootstrap.events")
        fake = {"events": env_events}
        async with bootstrap_lock:
            bootstrap_cache.clear()
            bootstrap_cache.update(fake)
            bootstrap_cache_ts = time.time()
        return fake
    return None

# ---------- standings cache ----------

def standings_cache_valid() -> bool:
    if standings_cache_ts is None:
        return False
    return (time.time() - standings_cache_ts) <= FPL_STANDINGS_TTL

async def get_league_results_cached(league_id: str) -> Optional[List[Dict]]:
    # read-fast path without lock
    if standings_cache_valid() and "results" in standings_cache:
        return standings_cache.get("results")  # type: ignore

    # guarded refresh
    async with standings_lock:
        if standings_cache_valid() and "results" in standings_cache:
            return standings_cache.get("results")  # type: ignore

        all_results: List[Dict] = []
        page = 1
        while True:
            url = fpl_url(league_path_tpl.format(league_id=league_id, page=page))
            data = await fetch_json(url)
            if not data:
                # if we have some cached data and allow serving it while error, we could return it
                if "results" in standings_cache:
                    logger.warning("Standings fetch failed; serving cached standings.")
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
                    logger.warning("Stopped pagination after 30 pages (safety cutoff).")
                    break
            except Exception as ex:
                logger.error(f"League standings decode error: {ex}")
                if "results" in standings_cache:
                    logger.warning("Decode error; serving cached standings.")
                    return standings_cache.get("results")  # type: ignore
                return None

        # store
        standings_cache.clear()
        standings_cache.update({"results": all_results, "pages": page})
        global standings_cache_ts
        standings_cache_ts = time.time()
        return all_results

# ---------- picks cache ----------

def picks_cache_valid(ts: Optional[float]) -> bool:
    if ts is None:
        return False
    return (time.time() - ts) <= FPL_PICKS_TTL

async def get_entry_picks_cached(entry_id: int, gw: int) -> Optional[Dict]:
    key = (entry_id, gw)
    # fast path
    cached = picks_cache.get(key)
    if cached and picks_cache_valid(cached.get("ts")):
        return cached.get("data")  # type: ignore

    # guarded, per-key lock
    lock = await _picks_get_lock_guarded(key)
    async with lock:
        # re-check
        cached2 = picks_cache.get(key)
        if cached2 and picks_cache_valid(cached2.get("ts")):
            return cached2.get("data")  # type: ignore

        url = fpl_url(entry_picks_path_tpl.format(entry_id=entry_id, gw=gw))
        data = await fetch_json(url)
        if data:
            picks_cache[key] = {"data": data, "ts": time.time()}
            return data

        # upstream failed; serve stale if allowed and present
        if FPL_PICKS_ALLOW_STALE and cached2 and cached2.get("data"):
            logger.warning(f"Serving STALE picks for entry={entry_id} gw={gw}")
            return cached2.get("data")  # type: ignore

        return None

# ---------- helpers ----------

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
    candidates: List[Dict] = []
    for e in events:
        deadline = parse_deadline(e.get("deadline_time"))
        if not deadline:
            continue
        if deadline > now and (e.get("is_current") or e.get("is_next")):
            candidates.append(e)
    if candidates:
        return min(candidates, key=lambda x: parse_deadline(x["deadline_time"]))
    future = [e for e in events if not e.get("finished") and parse_deadline(e.get("deadline_time")) and parse_deadline(e.get("deadline_time")) > now]
    if future:
        return min(future, key=lambda x: parse_deadline(x["deadline_time"]))
    return None

# ---------- 5. Handlers ----------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Я FPL-бот 🚀")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Доступные команды:\n"
        "/start — приветствие\n"
        f"/points — очки за последний завершённый тур лиги {LEAGUE_ID}\n"
        "/gw <номер> — очки за указанный тур (например: /gw 14)\n"
        f"/rank — текущее положение в лиге {LEAGUE_ID}\n"
        "/deadline — сколько осталось до ближайшего дедлайна\n"
        "/help — эта справка"
    )
    await update.message.reply_text(text)

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    league_id = LEAGUE_ID
    bootstrap = await get_bootstrap()
    if not bootstrap:
        err = "FPL API bootstrap-static недоступен (ошибка/таймаут)"
        logger.error(err)
        await update.message.reply_text(err)
        return
    events = bootstrap.get("events", [])
    if not events:
        await update.message.reply_text("Не удалось получить список туров.")
        return
    last_finished_gw = choose_last_finished_gw(events)
    if not last_finished_gw:
        await update.message.reply_text("Не удалось определить завершённый тур.")
        return
    await send_league_points(update, league_id, last_finished_gw, events, header_override=None)

async def gw_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    league_id = LEAGUE_ID
    args = context.args
    if not args:
        await update.message.reply_text("Использование: /gw <номер_тура>. Например: /gw 14")
        return
    gw_str = args[0].strip()
    if not gw_str.isdigit():
        await update.message.reply_text("Номер тура должен быть целым числом. Пример: /gw 12")
        return
    gw_num = int(gw_str)
    bootstrap = await get_bootstrap()
    if not bootstrap:
        await update.message.reply_text("Не удалось обновить bootstrap-static.")
        return
    events = bootstrap.get("events", [])
    if not events:
        await update.message.reply_text("Нет данных о турах.")
        return
    max_gw = max(e.get("id", 0) for e in events)
    if gw_num < 1 or gw_num > max_gw:
        await update.message.reply_text(f"Тур {gw_num} вне диапазона (1..{max_gw}).")
        return
    event_map = {e["id"]: e for e in events}
    selected_event = event_map.get(gw_num)
    finished = selected_event.get("finished") if selected_event else False
    is_current = selected_event.get("is_current") if selected_event else False
    if not finished and not is_current and not selected_event.get("data_checked", False):
        await update.message.reply_text("Этот тур ещё не стартовал или нет данных.")
        return
    header = f"*Очки за тур {gw_num}*"
    if not finished:
        header += " (тур ещё не завершён)"
    header += "\n\n"
    await send_league_points(update, league_id, gw_num, events, header_override=header)

async def rank_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    league_id = LEAGUE_ID
    results = await get_league_results_cached(league_id)
    if results is None:
        err = "Не удалось получить участников лиги (standings)."
        logger.error(err)
        await update.message.reply_text(err)
        return
    results_sorted = sorted(results, key=lambda r: r.get("rank", 10**9))
    header = "*Текущее положение в лиге:*\n\n"
    lines: List[str] = []
    for r in results_sorted:
        rank = r.get("rank")
        last_rank = r.get("last_rank")
        total = r.get("total")
        entry_name = r.get("entry_name")
        player_name = r.get("player_name")
        change = ""
        if isinstance(last_rank, int) and isinstance(rank, int) and last_rank > 0 and rank > 0:
            delta = last_rank - rank
            if delta > 0:
                change = f" ↑{delta}"
            elif delta < 0:
                change = f" ↓{abs(delta)}"
            else:
                change = " →0"
        lines.append(f"{rank}. {player_name} — {entry_name}: {total} pts{change}")
    full_text = header + "\n".join(lines)
    for chunk in split_message_chunks(full_text):
        try:
            await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception:
            await update.message.reply_text(chunk)

async def deadline_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bootstrap = await get_bootstrap()
    if not bootstrap:
        await update.message.reply_text("Не удалось получить данные (bootstrap).")
        return
    events = bootstrap.get("events", [])
    if not events:
        await update.message.reply_text("Нет данных о турах.")
        return
    target = find_next_deadline_event(events)
    if not target:
        await update.message.reply_text("Нет будущих дедлайнов (сезон завершён или данные недоступны).")
        return
    deadline_dt = parse_deadline(target.get("deadline_time"))
    if not deadline_dt:
        await update.message.reply_text("Не удалось распарсить время дедлайна.")
        return
    now = datetime.now(timezone.utc)
    delta_seconds = int((deadline_dt - now).total_seconds())
    human = format_timedelta(delta_seconds)
    gw_id = target.get("id")
    gw_name = target.get("name", f"GW {gw_id}")
    deadline_str = deadline_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    text = (
        f"*Ближайший дедлайн:* {gw_name} (ID {gw_id})\n"
        f"Когда: {deadline_str}\n"
        f"Осталось: {human}"
    )
    try:
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception:
        await update.message.reply_text(text)

# ---------- shared send ----------
async def send_league_points(
    update: Update,
    league_id: str,
    gw_num: int,
    events: List[Dict],
    header_override: Optional[str] = None
):
    results = await get_league_results_cached(league_id)
    if results is None:
        err = "Не удалось получить участников лиги (standings)."
        logger.error(err)
        await update.message.reply_text(err)
        return
    header = header_override or f"*Очки за тур {gw_num}:*\n\n"
    lines: List[str] = []

    async def fetch_points(entry_id: int, entry_name: str, player_name: str) -> str:
        data = await get_entry_picks_cached(entry_id, gw_num)
        points = None
        if data:
            points = data.get("entry_history", {}).get("points")
        return f"{player_name} — {entry_name}: {points if points is not None else 'нет данных'}"

    tasks = [
        asyncio.create_task(fetch_points(r["entry"], r["entry_name"], r["player_name"]))
        for r in results
    ]
    fetched = await asyncio.gather(*tasks, return_exceptions=True)
    for item in fetched:
        if isinstance(item, Exception):
            lines.append("Ошибка участника: нет данных")
        else:
            lines.append(item)

    full_text = header + "\n".join(lines)
    chunks = split_message_chunks(full_text)
    for chunk in chunks:
        try:
            await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception:
            await update.message.reply_text(chunk)

# ---------- misc ----------
async def _register_webhook_if_needed():
    logger.info("Webhook registration placeholder (not implemented)")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception in handler", exc_info=context.error)
    if isinstance(update, Update) and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Произошла внутренняя ошибка. Сообщите администратору."
            )
        except Exception:
            pass

# ---------- 7. Setup Telegram command menu ----------
async def setup_bot_commands(bot):
    commands = [
        BotCommand("start", "Приветствие"),
        BotCommand("help", "Справка по командам"),
        BotCommand("points", "Очки за последний завершённый тур"),
        BotCommand("gw", "Очки за указанный тур: /gw <номер>"),
        BotCommand("rank", "Текущее положение в лиге"),
        BotCommand("deadline", "Сколько осталось до дедлайна"),
    ]
    await bot.set_my_commands(commands)
    try:
        await bot.set_my_commands(commands, language_code="ru")
    except Exception:
        pass
    logger.info("Telegram command menu set.")

# ---------- 8. run_bot (manual polling) ----------
async def run_bot():
    global http_client
    logger.info("Starting bot...")

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=50)
    use_http2 = ENABLE_HTTP2
    if use_http2:
        try:
            import h2  # noqa: F401
        except ImportError:
            logger.warning("h2 package not found, disabling HTTP/2. Install httpx[http2] to enable.")
            use_http2 = False

    http_client = httpx.AsyncClient(http2=use_http2, limits=limits, timeout=15.0)
    logger.info(f"httpx client created (http2={use_http2})")

    application = Application.builder().token(BOT_TOKEN).concurrent_updates(TELEGRAM_CONCURRENCY).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("points", points_command))
    application.add_handler(CommandHandler("gw", gw_command))
    application.add_handler(CommandHandler("rank", rank_command))
    application.add_handler(CommandHandler("deadline", deadline_command))
    application.add_error_handler(error_handler)

    await application.initialize()
    await application.start()

    try:
        await setup_bot_commands(application.bot)
    except Exception:
        logger.exception("Failed to set bot commands")

    if USE_WEBHOOK:
        await _register_webhook_if_needed()
    else:
        try:
            await application.updater.start_polling()
            logger.info("Polling started")
        except Exception:
            logger.exception("Failed to start polling")
            try:
                await application.stop()
                await application.shutdown()
            except Exception:
                logger.exception("Error shutting down application after polling failure")
            try:
                await http_client.aclose()
            except Exception:
                logger.exception("Error closing HTTP client early")
            return

    try:
        me = await application.bot.get_me()
        logger.info("Bot started as @%s (id=%s)", getattr(me, 'username', 'unknown'), getattr(me, 'id', 'unknown'))
    except Exception:
        logger.exception("Failed to get_me")

    logger.info("Bot started, waiting for stop_event...")
    try:
        await stop_event.wait()
    finally:
        logger.info("Shutdown initiated")
        if not USE_WEBHOOK:
            try:
                await application.updater.stop()
                logger.info("Updater stopped")
            except Exception:
                logger.exception("Error stopping updater")
        try:
            await application.stop()
            await application.shutdown()
        except Exception:
            logger.exception("Error shutting down application")
        try:
            if http_client:
                await http_client.aclose()
        except Exception:
            logger.exception("Error closing HTTP client")
        logger.info("SHUTDOWN COMPLETE")

# ---------- 9. Signals ----------
def handle_sigterm(signum, frame):
    logger.info("SIGTERM/SIGINT received, initiating shutdown...")
    try:
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(stop_event.set)
    except RuntimeError:
        pass

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

# ---------- 10. Entrypoint ----------
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
