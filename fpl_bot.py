import os
import asyncio
import threading
import logging
from flask import Flask
import httpx
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update

# ---------- 1. Load ENV Variables ----------
BOT_TOKEN = os.environ.get("BOT_TOKEN")  # Имя переменной как в Render Environment!
ENABLE_KILL = os.environ.get("ENABLE_KILL", "0") == "1"
FPL_CACHE_TTL = int(os.environ.get("FPL_CACHE_TTL", "8"))
FPL_CONCURRENCY = int(os.environ.get("FPL_CONCURRENCY", "6"))
PORT = int(os.environ.get('PORT', 10000))
TELEGRAM_CONCURRENCY = int(os.environ.get("TELEGRAM_CONCURRENCY", "4"))
USE_WEBHOOK = os.environ.get("USE_WEBHOOK", "0") == "1"
stop_event = asyncio.Event()

# ---------- 2. Logging ----------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("fpl_bot")

# ---------- 3. Flask App ----------
flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    return "FPL BOT is running!"

def start_flask():
    logger.info(f"Starting Flask app on port {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT)

def kill_existing_instances():
    logger.info("ENABLE_KILL is set, killing existing instances (placeholder)")

# ---------- 4. Telegram Bot Handlers ----------

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Я FPL-бот 🚀")

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    league_id = "980121"
    try:
        async with httpx.AsyncClient() as client:
            # Получаем текущий тур
            gw_resp = await client.get("https://fantasy.premierleague.com/api/bootstrap-static/")
            if gw_resp.status_code != 200:
                err_txt = f"FPL API bootstrap-static недоступен ({gw_resp.status_code})"
                logger.error(err_txt)
                await update.message.reply_text(err_txt)
                return
            try:
                events = gw_resp.json()["events"]
            except Exception as ex:
                logger.error(f"Ошибка декодирования событий FPL GW: {ex}")
                await update.message.reply_text("Ошибка при обработке ответа от FPL API.")
                return
            current_gw = max(event["id"] for event in events if event.get("is_current", False))
            last_gw = current_gw - 1

            # Получаем участников лиги
            url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"
            resp = await client.get(url)
            if resp.status_code != 200:
                err_txt = f"FPL API standings недоступен ({resp.status_code})"
                logger.error(err_txt)
                await update.message.reply_text(err_txt)
                return
            try:
                league = resp.json()
                results = league["standings"]["results"]
            except Exception as ex:
                logger.error(f"Ошибка декодирования standings: {ex}")
                await update.message.reply_text("Ошибка при обработке участников лиги.")
                return

            reply = "*Очки за прошлый тур:*\n\n"

            # Для каждого участника — получение очков
            for result in results:
                entry_id = result["entry"]
                entry_name = result["entry_name"]
                player_name = result["player_name"]

                picks_url = f"https://fantasy.premierleague.com/api/entry/{entry_id}/event/{last_gw}/picks/"
                points = None
                try:
                    picks_resp = await client.get(picks_url)
                    if picks_resp.status_code == 200:
                        picks_json = picks_resp.json()
                        points = picks_json.get("points")
                except Exception as ex:
                    logger.warning(f"Не удалось получить очки для {entry_name}: {ex}")
                reply += f"{player_name} — {entry_name}: {points if points is not None else 'нет данных'}\n"

            await update.message.reply_text(reply, parse_mode="Markdown")
    except Exception as exc:
        logger.exception("Ошибка в обработчике /points")
        await update.message.reply_text("Внутренняя ошибка сервера при получении очков!")

async def _register_webhook_if_needed():
    logger.info("Webhook registration is not implemented in this example (placeholder)")

# ---------- 5. Run Bot ----------
async def run_bot():
    global bot_application, bot_loop, http_client, bot_running
    logger.info('Starting bot...')
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=50)
    http_client = httpx.AsyncClient(limits=limits, timeout=10.0)
    bot_application = Application.builder().token(BOT_TOKEN).concurrent_updates(TELEGRAM_CONCURRENCY).build()
    bot_application.add_handler(CommandHandler('start', start_command))
    bot_application.add_handler(CommandHandler('points', points_command))

    await bot_application.initialize()
    await bot_application.start()
    bot_loop = asyncio.get_running_loop()

    # Проверяем залогиненность бота
    try:
        me = await bot_application.bot.get_me()
        logger.info('Bot started as @%s (id=%s)', getattr(me, 'username', 'unknown'), getattr(me, 'id', 'unknown'))
    except Exception:
        logger.exception('Failed to get_me')

    # Запускаем polling или webhook
    if USE_WEBHOOK:
        await _register_webhook_if_needed()
    else:
        try:
            await bot_application.updater.start_polling()
            logger.info('Polling started')
        except Exception:
            logger.exception('Failed to start polling')

    logger.info('Bot started, waiting for stop_event...')
    bot_running = True
    try:
        await stop_event.wait()
    finally:
        logger.info('Shutdown initiated')
        bot_running = False

        # Корректное завершение polling
        if not USE_WEBHOOK:
            try:
                await bot_application.updater.stop()
                logger.info('Updater stopped')
            except Exception:
                logger.exception('Error stopping updater')

        # Завершение Telegram Application и httpx-клиента
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
            logger.exception('Error closing HTTP client')

        logger.info('SHUTDOWN COMPLETE')

# ---------- 6. Entrypoint ----------
if __name__ == "__main__":
    print('Main entrypoint start')

    if ENABLE_KILL:
        kill_existing_instances()

    logger.info('Lock acquired (placeholder)')
    flask_thread = threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()
    logger.info('Flask thread started')

    try:
        asyncio.run(run_bot())
    except Exception:
        logger.exception('Application exited with exception')
    finally:
        print('Main entrypoint finished')
