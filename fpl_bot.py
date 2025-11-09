"""
FPL Telegram Bot with:
- Proper request headers to avoid 403 from FPL API
- Caching of bootstrap-static
- Concurrency limiting for FPL requests
- Selection of last finished gameweek
- Graceful shutdown
- Optional webhook placeholder
- Simple health endpoint
"""

import os
import asyncio
import threading
import logging
import time
from typing import Any, Dict, Optional, List

from flask import Flask, jsonify
import httpx
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update

# ---------- 1. ENV ----------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Environment variable BOT_TOKEN is required")

ENABLE_KILL = os.environ.get("ENABLE_KILL", "0") == "1"
FPL_CACHE_TTL = int(os.environ.get("FPL_CACHE_TTL", "8"))            # minutes
FPL_CONCURRENCY = int(os.environ.get("FPL_CONCURRENCY", "6"))
PORT = int(os.environ.get("PORT", 10000))
TELEGRAM_CONCURRENCY = int(os.environ.get("TELEGRAM_CONCURRENCY", "4"))
USE_WEBHOOK = os.environ.get("USE_WEBHOOK", "0") == "1"
LEAGUE_ID = os.environ.get("LEAGUE_ID", "980121")                    # default league id

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
    return jsonify({
        "ok": True,
        "bootstrap_cached": bootstrap_cache_ts is not None,
        "bootstrap_cache_age_min": age_min,
        "concurrency_limit": FPL_CONCURRENCY
    })

def start_flask():
    logger.info(f"Starting Flask app on port {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT)

def kill_existing_instances():
    # Placeholder for any external coordination (pid file, etc.)
    logger.info("ENABLE_KILL is set, killing existing instances (placeholder)")

# ---------- 4. HTTP Client / Headers / Cache ----------

FPL_BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://fantasy.premierleague.com/",
    "Origin": "https://fantasy.premierleague.com",
}

http_client: Optional[httpx.AsyncClient] = None
bootstrap_cache: Dict[str, Any] = {}
bootstrap_cache_ts: Optional[float] = None

bootstrap_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
league_url_template = "https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"
entry_picks_template = "https://fantasy.premierleague.com/api/entry/{entry_id}/event/{gw}/picks/"

fpl_semaphore = asyncio.Semaphore(FPL_CONCURRENCY)

async def fetch_json(
    url: str,
    timeout: float = 15.0,
    max_attempts: int = 3,
    backoff_base: float = 2.0
) -> Optional[Dict]:
    """
    Fetch JSON with headers, concurrency semaphore, and simple exponential backoff on 403/429/5xx.
    Returns dict or None.
    """
    global http_client
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            async with fpl_semaphore:
                resp = await http_client.get(url, headers=FPL_BASE_HEADERS, timeout=timeout)
            status = resp.status_code
            if status == 200:
                # Attempt JSON decode guarded
                try:
                    return resp.json()
                except Exception as jex:
                    logger.warning(f"JSON decode error for {url}: {jex}")
                    return None

            body_preview = resp.text[:400].replace("\n", " ")
            logger.warning(f"Attempt {attempt}: status={status} url={url} body='{body_preview}'")

            # Determine if we should backoff
            if status in (403, 429) or 500 <= status < 600:
                if attempt < max_attempts:
                    sleep_time = backoff_base ** attempt
                    await asyncio.sleep(sleep_time)
                    continue
            # For other non-200 statuses no retry
            return None

        except httpx.TimeoutException:
            logger.error(f"Timeout fetching {url} (attempt {attempt})")
            if attempt < max_attempts:
                await asyncio.sleep(backoff_base ** attempt)
        except Exception as ex:
            logger.exception(f"Unexpected error fetching {url} (attempt {attempt}): {ex}")
            if attempt < max_attempts:
                await asyncio.sleep(backoff_base ** attempt)
    return None

def bootstrap_cache_valid() -> bool:
    if bootstrap_cache_ts is None:
        return False
    age = (time.time() - bootstrap_cache_ts) / 60.0
    return age < FPL_CACHE_TTL

async def get_bootstrap() -> Optional[Dict]:
    global bootstrap_cache, bootstrap_cache_ts
    if bootstrap_cache_valid():
        return bootstrap_cache
    data = await fetch_json(bootstrap_url)
    if data:
        bootstrap_cache = data
        bootstrap_cache_ts = time.time()
    return data

def choose_last_finished_gw(events: List[Dict]) -> Optional[int]:
    """
    Choose last fully finished gameweek (finished=True). If none, fallback logic.
    """
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

# ---------- 5. Telegram Handlers ----------

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Я FPL-бот 🚀")

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    league_id = LEAGUE_ID

    # 1. bootstrap-static
    bootstrap = await get_bootstrap()
    if not bootstrap:
        err = "FPL API bootstrap-static недоступен (403/timeout/другая ошибка)"
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

    # 2. standings
    league_url = league_url_template.format(league_id=league_id)
    league_json = await fetch_json(league_url)
    if not league_json:
        err = "FPL API standings недоступен (403/timeout/другая ошибка)"
        logger.error(err)
        await update.message.reply_text(err)
        return

    try:
        results = league_json["standings"]["results"]
    except Exception as ex:
        logger.error(f"Ошибка декодирования standings: {ex}")
        await update.message.reply_text("Ошибка при обработке участников лиги.")
        return

    reply_lines = [f"*Очки за тур {last_finished_gw}:*\n"]

    async def fetch_points(entry_id: int, entry_name: str, player_name: str) -> str:
        url = entry_picks_template.format(entry_id=entry_id, gw=last_finished_gw)
        data = await fetch_json(url)
        points = data.get("points") if data else None
        return f"{player_name} — {entry_name}: {points if points is not None else 'нет данных'}"

    tasks = [
        asyncio.create_task(
            fetch_points(r["entry"], r["entry_name"], r["player_name"])
        )
        for r in results
    ]

    fetched_lines = await asyncio.gather(*tasks, return_exceptions=True)
    for line in fetched_lines:
        if isinstance(line, Exception):
            reply_lines.append("Не удалось получить данные участника.\n")
        else:
            reply_lines.append(line + "\n")

    final_reply = "".join(reply_lines)
    try:
        await update.message.reply_text(final_reply, parse_mode="Markdown")
    except Exception:
        # Fallback if Markdown fails
        await update.message.reply_text(final_reply)

async def _register_webhook_if_needed():
    logger.info("Webhook registration is not implemented (placeholder)")

# ---------- 6. Run Bot ----------
async def run_bot():
    global http_client
    logger.info("Starting bot...")

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=50)
    http_client = httpx.AsyncClient(limits=limits, timeout=10.0)

    application = Application.builder().token(BOT_TOKEN).concurrent_updates(TELEGRAM_CONCURRENCY).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("points", points_command))

    await application.initialize()
    await application.start()

    # Identity check
    try:
        me = await application.bot.get_me()
        logger.info("Bot started as @%s (id=%s)", getattr(me, "username", "unknown"), getattr(me, "id", "unknown"))
    except Exception:
        logger.exception("Failed to get_me")

    if USE_WEBHOOK:
        await _register_webhook_if_needed()
    else:
        # For python-telegram-bot v20+, polling via updater attribute still works; else use application.run_polling()
        try:
            await application.updater.start_polling()
            logger.info("Polling started")
        except Exception:
            logger.exception("Failed to start polling")

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
            logger.exception("Error shutting down bot")

        try:
            if http_client:
                await http_client.aclose()
        except Exception:
            logger.exception("Error closing HTTP client")

        logger.info("SHUTDOWN COMPLETE")

# ---------- 7. Entrypoint ----------
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
