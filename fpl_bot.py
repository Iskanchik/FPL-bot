import os
import asyncio
import threading
import logging

from flask import Flask
import httpx
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update

# ---------- 1. Load ENV Variables ----------
BOT_TOKEN = os.environ.get("FPL_BOT_TOKEN")  # Render: your Telegram Bot Token
ENABLE_KILL = os.environ.get("ENABLE_KILL", "0") == "1"
FPL_CACHE_TTL = int(os.environ.get("FPL_CACHE_TTL", "8"))
FPL_CONCURRENCY = int(os.environ.get("FPL_CONCURRENCY", "6"))
PORT = int(os.environ.get("PORT", 10000))  # Render sets PORT automatically
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
    # Реальная логика завершения других процессов по вашему усмотрению (например, через psutil/lsof)

# ---------- 4. Telegram Bot Handlers ----------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Я FPL-бот 🚀")

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Ваши очки: 42\nFPL_CACHE_TTL={FPL_CACHE_TTL}")

async def _register_webhook_if_needed():
    logger.info("Webhook registration is not implemented in this example (placeholder)")
    # Можно добавить логику установки вебхука, если нужно:
    # await bot_application.bot.set_webhook(url=..., ...)

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
