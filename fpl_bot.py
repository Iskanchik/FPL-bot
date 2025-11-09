import requests
import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from flask import Flask, request
from threading import Thread
import os
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import signal
import sys

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('werkzeug').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.WARNING)

# Настройки
BOT_TOKEN = "8554755843:AAHZrdxLhNTDkr4P_G-zreyH2Poa_gsL6XY"
LEAGUE_ID = 980121

# Глобальные переменные
bot_application = None
bot_running = True

# Flask приложение
app = Flask(__name__)

@app.route('/')
def home():
    return "FPL Bot is running! 🤖⚽"

@app.route('/health')
def health():
    return {"status": "healthy", "bot": "running"}

def run_flask():
    port = int(os.environ.get('PORT', 5000))
    try:
        from waitress import serve
        logger.info(f"🚀 Starting production server on port {port}")
        serve(app, host='0.0.0.0', port=port, threads=4)
    except ImportError:
        logger.warning("⚠️ Using development server")
        import warnings
        warnings.filterwarnings("ignore", message=".*development server.*")
        app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)

async def clear_bot_connections():
    try:
        logger.info("🧹 Очистка подключений бота...")
        webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        requests.post(webhook_url, json={'drop_pending_updates': True}, timeout=10)
        
        updates_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
        response = requests.post(updates_url, json={'offset': -1, 'limit': 1}, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('result'):
                last_update_id = data['result'][0]['update_id']
                requests.post(updates_url, json={'offset': last_update_id + 1, 'limit': 1}, timeout=10)
        
        await asyncio.sleep(3)
        logger.info("✅ Bot connections cleared")
    except Exception as e:
        logger.error(f"Error clearing connections: {e}")
        await asyncio.sleep(2)

def make_fpl_request(url, timeout=15, max_retries=3):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 2)
    
    logger.error(f"All attempts failed for: {url}")
    return None

def get_current_gameweek():
    try:
        data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        if not data or 'events' not in data:
            return None
        
        events = data['events']
        for event in events:
            if event.get('is_current', False):
                return event['id']
        
        for event in events:
            if event.get('is_next', False):
                return event['id']
        
        for event in events:
            if not event.get('finished', True):
                return event['id']
        
        return events[-1]['id'] if events else None
    except Exception as e:
        logger.error(f"Error getting gameweek: {e}")
        return None

def get_league_standings():
    try:
        data = make_fpl_request(f"https://fantasy.premierleague.com/api/leagues-classic/{LEAGUE_ID}/standings/")
        if not data or 'standings' not in data or 'results' not in data['standings']:
            return []
        return data['standings']['results'][:10]
    except Exception as e:
        logger.error(f"Error getting standings: {e}")
        return []

def get_manager_picks_batch(manager_ids, gameweek):
    def fetch_picks(manager_id):
        try:
            url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{gameweek}/picks/"
            data = make_fpl_request(url, timeout=10)
            return manager_id, data.get('picks', []) if data else []
        except Exception as e:
            logger.error(f"Error fetching picks for {manager_id}: {e}")
            return manager_id, []
    
    results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_picks, mid): mid for mid in manager_ids}
        for future in futures:
            try:
                manager_id, picks = future.result(timeout=15)
                results[manager_id] = picks
            except Exception:
                results[futures[future]] = []
    return results

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("🔄 Загружаю данные...")
        
        current_gw = get_current_gameweek()
        if not current_gw:
            await update.message.reply_text("❌ Не удалось получить текущий gameweek")
            return
        
        await update.message.reply_text(f"📅 Текущий Gameweek: {current_gw}")
        
        bootstrap_data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        if not bootstrap_data or 'elements' not in bootstrap_data:
            await update.message.reply_text("❌ Не удалось получить данные игроков")
            return
        
        players = {p['id']: p for p in bootstrap_data['elements']}
        teams = {t['id']: t['name'] for t in bootstrap_data['teams']}
        
        live_data = make_fpl_request(f"https://fantasy.premierleague.com/api/event/{current_gw}/live/")
        if not live_data or 'elements' not in live_data:
            await update.message.reply_text("❌ Не удалось получить live данные")
            return
        
        live_points = {}
        for item in live_data['elements']:
            if 'stats' in item and 'total_points' in item['stats']:
                live_points[item['id']] = item['stats']['total_points']
        
        managers = get_league_standings()
        if not managers:
            await update.message.reply_text("❌ Не удалось получить данные лиги")
            return
        
        manager_ids = [m['entry'] for m in managers]
        all_picks = get_manager_picks_batch(manager_ids, current_gw)
        
        successful_picks = len([p for p in all_picks.values() if p])
        if successful_picks == 0:
            await update.message.reply_text("❌ Не удалось получить составы менеджеров")
            return
        
        team_players = {}
        manager_names = {m['entry']: m['entry_name'] for m in managers}
        
        for manager_id, picks in all_picks.items():
            if not picks:
                continue
            
            manager_name = manager_names.get(manager_id, f"Manager {manager_id}")
            
            for pick in picks[:11]:
                player_id = pick['element']
                if player_id not in players:
                    continue
                
                player = players[player_id]
                team_name = teams.get(player['team'], 'Unknown')
                points = live_points.get(player_id, 0)
                
                if team_name not in team_players:
                    team_players[team_name] = []
                
                team_players[team_name].append({
                    'name': player['web_name'],
                    'manager': manager_name,
                    'points': points,
                    'multiplier': pick.get('multiplier', 1)
                })
        
        if not team_players:
            await update.message.reply_text("❌ Не найдено данных об игроках")
            return
        
        message = f"🏆 Лига {LEAGUE_ID} - GW{current_gw}\n👥 Данные от {successful_picks} менеджеров\n\n"
        
        sorted_teams = sorted(team_players.items(), key=lambda x: len(x[1]), reverse=True)[:5]
        
        for team_name, players_list in sorted_teams:
            message += f"⚽ *{team_name.upper()}*\n"
            sorted_players = sorted(players_list, key=lambda x: x['points'] * x['multiplier'], reverse=True)[:5]
            
            for player in sorted_players:
                total_points = player['points'] * player['multiplier']
                multiplier_text = f" (C)" if player['multiplier'] == 2 else f" (VC)" if player['multiplier'] == 1.5 else ""
                message += f"• {player['name']}{multiplier_text} - {total_points} pts ({player['manager']})\n"
            message += "\n"
        
        if len(message) > 4000:
            parts = message.split('\n\n')
            current_message = f"🏆 Лига {LEAGUE_ID} - GW{current_gw}\n👥 Данные от {successful_picks} менеджеров\n\n"
            
            for part in parts[1:]:
                if len(current_message + part) > 3500:
                    await update.message.reply_text(current_message, parse_mode='Markdown')
                    current_message = part + "\n\n"
                else:
                    current_message += part + "\n\n"
            
            if current_message.strip():
                await update.message.reply_text(current_message, parse_mode='Markdown')
        else:
            await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error in points_command: {e}")
        await update.message.reply_text(f"❌ Произошла ошибка: {str(e)}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = f"""🤖 *FPL League Bot*

*Команды:*
/points - Получить очки текущего gameweek для лиги {LEAGUE_ID}

Бот показывает игроков по командам с очками и именами менеджеров.
⚡ Обрабатывается топ-10 менеджеров."""
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

async def run_bot():
    global bot_application, bot_running
    
    logger.info("🚀 Запуск FPL Bot...")
    await clear_bot_connections()
    
    bot_application = Application.builder().token(BOT_TOKEN).updater(None).build()
    bot_application.add_handler(CommandHandler("start", start_command))
    bot_application.add_handler(CommandHandler("points", points_command))
    
    await bot_application.initialize()
    await bot_application.start()
    
    logger.info("✅ Бот запущен!")
    
    last_update_id = 0
    consecutive_errors = 0
    
    try:
        while bot_running:
            try:
                updates = await bot_application.bot.get_updates(
                    offset=last_update_id + 1,
                    timeout=10,
                    limit=100
                )
                
                consecutive_errors = 0
                
                for update in updates:
                    last_update_id = update.update_id
                    await bot_application.process_update(update)
                
                if not updates:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                consecutive_errors += 1
                error_msg = str(e)
                
                if "Conflict" in error_msg:
                    logger.warning(f"Конфликт polling: {error_msg}")
                    await asyncio.sleep(10)
                    if consecutive_errors % 3 == 0:
                        await clear_bot_connections()
                else:
                    logger.error(f"Ошибка polling: {e}")
                    await asyncio.sleep(5)
                
                if consecutive_errors >= 10:
                    logger.error("Слишком много ошибок, остановка")
                    break
                    
    except KeyboardInterrupt:
        logger.info("Остановка бота")
    finally:
        bot_running = False
        await bot_application.stop()
        await bot_application.shutdown()

def signal_handler(sig, frame):
    global bot_running
    logger.info("Получен сигнал остановки")
    bot_running = False
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")

if __name__ == '__main__':
    main()
