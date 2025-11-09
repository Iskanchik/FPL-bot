#!/usr/bin/env python3
import asyncio
import os
import signal
import sys
import logging
import json
import random
import time
from datetime import datetime
from threading import Thread

import psutil
import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from flask import Flask, request, jsonify
try:
    import fcntl
except Exception:
    fcntl = None

from logging.handlers import RotatingFileHandler

# ===== Configuration =====
# Read env with defaults
BOT_TOKEN = os.environ.get("FPL_BOT_TOKEN")
if not BOT_TOKEN:
    print("FPL_BOT_TOKEN is not set in environment. Exiting.")
    sys.exit(1)

try:
    LEAGUE_ID = int(os.environ.get("FPL_LEAGUE_ID", "980121"))
except ValueError:
    LEAGUE_ID = 980121

PORT = int(os.environ.get("PORT", "5000"))
# Enable or disable aggressive kill behavior. Disabled by default for containers.
ENABLE_KILL = os.environ.get('ENABLE_KILL', '0') == '1'
# If you want to run webhook mode, set TELEGRAM_WEBHOOK_URL (e.g. https://yourhost.com)
TELEGRAM_WEBHOOK_URL = os.environ.get('TELEGRAM_WEBHOOK_URL')
TELEGRAM_WEBHOOK_PATH = os.environ.get('TELEGRAM_WEBHOOK_PATH', f"/webhook/{BOT_TOKEN.split(':')[0]}")
USE_WEBHOOK = bool(TELEGRAM_WEBHOOK_URL)

# ===== Logging: structured to stdout =====
logger = logging.getLogger('fpl_bot')
logger.setLevel(logging.INFO)
logger.handlers.clear()

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
logger.addHandler(console_handler)

# Keep file handler optional but don't rely on it in ephemeral containers
file_handler = RotatingFileHandler('fpl_bot.log', maxBytes=5 * 1024 * 1024, backupCount=3)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
logger.addHandler(file_handler)

logging.getLogger('werkzeug').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)

# ===== Globals =====
bot_application: Application | None = None
bot_loop: asyncio.AbstractEventLoop | None = None
bot_running = True
lock_fd = None
http_client: httpx.AsyncClient | None = None
# domain semaphores
_semaphores = {
    'fpl': asyncio.Semaphore(int(os.environ.get('FPL_CONCURRENCY', '6'))),
    'telegram': asyncio.Semaphore(int(os.environ.get('TELEGRAM_CONCURRENCY', '4'))),
}
# simple in-memory cache: url -> (expires_ts, data)
_cache = {}
CACHE_DEFAULT_TTL = int(os.environ.get('FPL_CACHE_TTL', '8'))  # seconds

# Flask app for health, webhook endpoint and control
app = Flask(__name__)


@app.route('/')
def home():
    return f"FPL Bot is running! 🤖⚽ Started at {datetime.now()}"


@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "bot": "running" if bot_running else "stopped",
        "timestamp": datetime.now().isoformat()
    })


@app.route('/restart', methods=['POST'])
def restart_bot():
    global bot_running
    bot_running = False
    return jsonify({"status": "restarting"})


@app.route(TELEGRAM_WEBHOOK_PATH, methods=['POST'])
def telegram_webhook():
    """Receive Telegram updates (webhook mode). This runs in Flask thread; schedule processing on bot loop."""
    if not USE_WEBHOOK:
        return jsonify({'error': 'webhook not enabled'}), 404
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({'error': 'invalid json'}), 400

    if not bot_application or not bot_loop:
        return jsonify({'error': 'bot not ready'}), 503

    try:
        update = Update.de_json(data, bot_application.bot)
        future = asyncio.run_coroutine_threadsafe(bot_application.process_update(update), bot_loop)
        # do not block long; return 200 immediately
        return '', 200
    except Exception as e:
        logger.exception('Error scheduling webhook update: %s', e)
        return jsonify({'error': 'internal error'}), 500


# ===== Utilities: cache, semaphores, backoff =====

def cache_get(url):
    now = time.time()
    entry = _cache.get(url)
    if entry and entry[0] > now:
        return entry[1]
    return None


def cache_set(url, data, ttl=CACHE_DEFAULT_TTL):
    _cache[url] = (time.time() + ttl, data)


async def make_fpl_request_async(url: str, timeout: float = 20.0, max_retries: int = 3, cache_ttl: int = None):
    """Асинхронный запрос к FPL API с retry, exponential backoff + jitter, caching and semaphore protection."""
    global http_client
    if http_client is None:
        raise RuntimeError('HTTP client is not initialized')

    # try cache
    if cache_ttl is not None and cache_ttl > 0:
        cached = cache_get(url)
        if cached is not None:
            logger.info('cache hit for %s', url)
            return cached

    headers = {
        'User-Agent': 'FPL-Bot/1.0 (+https://github.com/)','Accept': 'application/json',
    }

    semaphore = _semaphores.get('fpl')
    base_backoff = 0.5
    max_backoff = 30

    async with semaphore:
        for attempt in range(1, max_retries + 1):
            try:
                resp = await http_client.get(url, headers=headers, timeout=timeout)
                status = resp.status_code
                if status == 200:
                    data = resp.json()
                    if cache_ttl is not None and cache_ttl > 0:
                        cache_set(url, data, ttl=cache_ttl)
                    return data
                if status == 429:
                    # rate limited
                    backoff = min(max_backoff, base_backoff * (2 ** attempt))
                    jitter = random.uniform(0, backoff * 0.3)
                    wait = backoff + jitter
                    logger.warning('Rate limited on %s, waiting %.1fs (attempt %d)', url, wait, attempt)
                    await asyncio.sleep(wait)
                else:
                    logger.warning('HTTP %s for %s (attempt %d)', status, url, attempt)
            except (httpx.ReadTimeout, httpx.ReadError, httpx.ConnectError) as e:
                backoff = min(max_backoff, base_backoff * (2 ** attempt))
                jitter = random.uniform(0, backoff * 0.3)
                wait = backoff + jitter
                logger.warning('HTTP error for %s: %s — retrying in %.1fs', url, e, wait)
                await asyncio.sleep(wait)
            except Exception as e:
                logger.exception('Unexpected error requesting %s: %s', url, e)
                await asyncio.sleep(1)
    logger.error('All attempts failed for: %s', url)
    return None


# Convenience wrappers
async def get_current_gameweek():
    data = await make_fpl_request_async('https://fantasy.premierleague.com/api/bootstrap-static/', cache_ttl=CACHE_DEFAULT_TTL)
    if not data or 'events' not in data:
        logger.warning('bootstrap-static returned no events')
        return None
    events = data['events']
    for ev in events:
        if ev.get('is_current', False):
            return ev['id']
    for ev in events:
        if ev.get('is_next', False):
            return ev['id']
    for ev in events:
        if not ev.get('finished', True):
            return ev['id']
    return events[-1]['id'] if events else None


async def get_league_standings():
    data = await make_fpl_request_async(f'https://fantasy.premierleague.com/api/leagues-classic/{LEAGUE_ID}/standings/', cache_ttl=10)
    if not data or 'standings' not in data or 'results' not in data['standings']:
        return []
    return data['standings']['results'][:15]


async def get_manager_picks_batch(manager_ids, gameweek, concurrency=None):
    if concurrency is None:
        concurrency = int(os.environ.get('FPL_CONCURRENCY', '6'))
    sem = asyncio.Semaphore(concurrency)

    async def fetch_picks(mid):
        async with sem:
            url = f'https://fantasy.premierleague.com/api/entry/{mid}/event/{gameweek}/picks/'
            data = await make_fpl_request_async(url, timeout=15, max_retries=3, cache_ttl=0)
            return mid, data.get('picks', []) if data else []

    tasks = [fetch_picks(mid) for mid in manager_ids]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out = {}
    for res in results:
        if isinstance(res, Exception):
            logger.exception('Error fetching picks batch: %s', res)
            continue
        mid, picks = res
        out[mid] = picks
    return out


# ===== Telegram handlers =====

async def send_long_message(chat, text, parse_mode='Markdown'):
    max_len = 3900
    cur = ''
    for line in text.splitlines(True):
        if len(cur) + len(line) > max_len:
            await chat.send_message(cur, parse_mode=parse_mode)
            cur = line
        else:
            cur += line
    if cur:
        await chat.send_message(cur, parse_mode=parse_mode)


async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        message_obj = await update.message.reply_text('🔄 Загружаю данные...')

        current_gw = await get_current_gameweek()
        if not current_gw:
            await message_obj.edit_text('❌ Не удалось получить текущий gameweek')
            return

        await message_obj.edit_text(f'📅 Текущий Gameweek: {current_gw}\n🔄 Загружаю данные игроков...')

        bootstrap_data = await make_fpl_request_async('https://fantasy.premierleague.com/api/bootstrap-static/', cache_ttl=CACHE_DEFAULT_TTL)
        if not bootstrap_data or 'elements' not in bootstrap_data:
            await message_obj.edit_text('❌ Не удалось получить данные игроков')
            return

        players = {p['id']: p for p in bootstrap_data['elements']}
        teams = {t['id']: t['name'] for t in bootstrap_data['teams']}

        await message_obj.edit_text(f'📅 GW{current_gw}\n🔄 Загружаю live данные...')

        live_data = await make_fpl_request_async(f'https://fantasy.premierleague.com/api/event/{current_gw}/live/', cache_ttl=3)
        if not live_data or 'elements' not in live_data:
            await message_obj.edit_text('❌ Не удалось получить live данные')
            return

        live_points = {}
        for item in live_data['elements']:
            stats = item.get('stats', {})
            if 'total_points' in stats:
                live_points[item['id']] = stats['total_points']

        await message_obj.edit_text(f'📅 GW{current_gw}\n🔄 Загружаю данные лиги...')

        managers = await get_league_standings()
        if not managers:
            await message_obj.edit_text('❌ Не удалось получить данные лиги')
            return

        await message_obj.edit_text(f'📅 GW{current_gw}\n🔄 Загружаю составы менеджеров...')

        manager_ids = [m['entry'] for m in managers]
        all_picks = await get_manager_picks_batch(manager_ids, current_gw)

        successful_picks = len([p for p in all_picks.values() if p])
        if successful_picks == 0:
            await message_obj.edit_text('❌ Не удалось получить составы менеджеров')
            return

        team_players = {}
        manager_names = {m['entry']: m.get('entry_name', f"Manager {m.get('entry')}") for m in managers}

        for manager_id, picks in all_picks.items():
            if not picks:
                continue
            manager_name = manager_names.get(manager_id, f"Manager {manager_id}")
            for pick in picks[:11]:
                player_id = pick.get('element')
                if not player_id or player_id not in players:
                    continue
                player = players[player_id]
                team_name = teams.get(player.get('team'), 'Unknown')
                points = live_points.get(player_id, 0)
                team_players.setdefault(team_name, []).append({
                    'name': player.get('web_name', 'Unknown'),
                    'manager': manager_name,
                    'points': points,
                    'multiplier': pick.get('multiplier', 1)
                })

        if not team_players:
            await message_obj.edit_text('❌ Не найдено данных об игроках')
            return

        header = f"🏆 Лига {LEAGUE_ID} - GW{current_gw}\n"
        header += f"👥 Данные от {successful_picks} менеджеров\n"
        header += f"⏰ Обновлено: {datetime.now().strftime('%H:%M')}\n\n"

        message = header
        sorted_teams = sorted(team_players.items(), key=lambda x: len(x[1]), reverse=True)[:8]
        for team_name, players_list in sorted_teams:
            message += f"⚽ {team_name.upper()}\n"
            sorted_players = sorted(players_list, key=lambda x: x['points'] * x['multiplier'], reverse=True)[:6]
            for player in sorted_players:
                total_points = player['points'] * player['multiplier']
                multiplier_text = ''
                if player['multiplier'] == 2:
                    multiplier_text = ' (C)'
                elif player['multiplier'] == 1.5:
                    multiplier_text = ' (VC)'
                message += f"• {player['name']}{multiplier_text} - {total_points} pts ({player['manager']})\n"
            message += '\n'

        await message_obj.delete()
        await send_long_message(update.message.chat, message, parse_mode='Markdown')

    except Exception as e:
        logger.exception('Error in points_command: %s', e)
        try:
            await update.message.reply_text(f"❌ Произошла ошибка: {str(e)[:200]}...")
        except Exception:
            pass


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        f"🤖 FPL League Bot v2.0\n\n"
        f"Команды:\n"
        f"/points - Получить очки текущего gameweek для лиги {LEAGUE_ID}\n\n"
        "Новые возможности:\n"
        "• Улучшенная стабильность\n"
        "• Быстрая загрузка данных\n"
        "• Защита от конфликтов\n"
        "• Подробная статистика\n\n"
        "Обрабатывается топ-15 менеджеров лиги."
    )
    await update.message.reply_text(welcome_text)


# ===== Run & Graceful shutdown =====
stop_event = asyncio.Event()


def _signal_handler(sig, frame):
    logger.info('Received signal %s, scheduling shutdown', sig)
    stop_event_loop = stop_event
    try:
        # in case called from another thread
        loop = asyncio.get_event_loop()
        if not loop.is_closed():
            loop.call_soon_threadsafe(stop_event.set)
    except RuntimeError:
        # no running event loop in this thread; set event directly
        try:
            stop_event.set()
        except Exception:
            pass


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


async def _register_webhook_if_needed():
    global bot_application
    if USE_WEBHOOK and bot_application:
        webhook_url = TELEGRAM_WEBHOOK_URL.rstrip('/') + TELEGRAM_WEBHOOK_PATH
        try:
            # set webhook via Bot method
            await bot_application.bot.set_webhook(url=webhook_url)
            logger.info('Webhook registered: %s', webhook_url)
        except Exception:
            logger.exception('Failed to set webhook')


async def run_bot():
    global bot_application, bot_loop, http_client, bot_running
    logger.info('Starting bot...')

    # create shared http client
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=50)
    http_client = httpx.AsyncClient(limits=limits, timeout=10.0)

    bot_application = Application.builder().token(BOT_TOKEN).build()
    bot_application.add_handler(CommandHandler('start', start_command))
    bot_application.add_handler(CommandHandler('points', points_command))

    await bot_application.initialize()
    await bot_application.start()

    # Save loop reference so webhook Flask thread can schedule coroutines
    bot_loop = asyncio.get_running_loop()

    # register webhook if requested
    await _register_webhook_if_needed()

    logger.info('Bot started')

    # main loop: wait for stop_event to be set
    try:
        await stop_event.wait()
    finally:
        logger.info('Shutdown initiated')
        bot_running = False
        try:
            if bot_application:
                await bot_application.stop()
                await bot_application.shutdown()
        except Exception:
            logger.exception('Error shutting down bot')
        try:
            if http_client:
                await http_client.aclose()
        except Exception:
            logger.exception('Error closing http client')


def acquire_lock():
    global lock_fd
    if fcntl is None:
        logger.info('fcntl not available; skipping lock')
        return None
    try:
        lock_file = '/tmp/fpl_bot.lock'
        lock_fd = open(lock_file, 'w')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(f"{os.getpid()}:{datetime.now()}")
        lock_fd.flush()
        logger.info('Lock acquired')
        return lock_fd
    except IOError:
        logger.warning('Lock already held; continuing')
        return None


def kill_existing_instances():
    if not ENABLE_KILL:
        logger.info('ENABLE_KILL not set; skipping kill_existing_instances')
        return
    current_pid = os.getpid()
    try:
        current_script = os.path.basename(__file__)
    except Exception:
        current_script = ''
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            pid = proc.info.get('pid')
            if pid == current_pid:
                continue
            cmdline = proc.info.get('cmdline') or []
            name = (proc.info.get('name') or '').lower()
            cmd_str = ' '.join(cmdline)
            if current_script and current_script in cmd_str and 'python' in name:
                logger.info('Killing existing instance %s', pid)
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue


def main():
    global lock_fd
    # attempt to kill others only if enabled
    try:
        kill_existing_instances()
    except Exception:
        logger.exception('kill_existing_instances failed')

    # try to acquire lock (best effort)
    try:
        lock_fd = acquire_lock()
    except Exception:
        logger.exception('acquire_lock failed')
        lock_fd = None

    # start flask in thread
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info('Flask thread started')

    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info('Interrupted by user')
    finally:
        if lock_fd:
            try:
                lock_fd.close()
                os.unlink('/tmp/fpl_bot.lock')
            except Exception:
                pass
        logger.info('Exited')


if __name__ == '__main__':
    main()
