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
import psutil
import fcntl
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fpl_bot.log')
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger('werkzeug').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)

# Настройки
BOT_TOKEN = "8554755843:AAHZrdxLhNTDkr4P_G-zreyH2Poa_gsL6XY"
LEAGUE_ID = 980121

# Глобальные переменные
bot_application = None
bot_running = True
lock_fd = None

# Flask приложение
app = Flask(__name__)

@app.route('/')
def home():
    return f"FPL Bot is running! 🤖⚽ Started at {datetime.now()}"

@app.route('/health')
def health():
    return {
        "status": "healthy", 
        "bot": "running" if bot_running else "stopped",
        "timestamp": datetime.now().isoformat()
    }

@app.route('/restart', methods=['POST'])
def restart_bot():
    """Endpoint для перезапуска бота"""
    global bot_running
    bot_running = False
    return {"status": "restarting"}

def acquire_lock():
    """Обеспечивает запуск только одного экземпляра"""
    global lock_fd
    lock_file = '/tmp/fpl_bot.lock'
    try:
        lock_fd = open(lock_file, 'w')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(f"{os.getpid()}:{datetime.now()}")
        lock_fd.flush()
        logger.info(f"✅ Lock acquired: PID {os.getpid()}")
        return lock_fd
    except IOError:
        logger.error("❌ Another instance is already running!")
        sys.exit(1)

def kill_existing_instances():
    """Убивает существующие экземпляры бота"""
    current_pid = os.getpid()
    current_script = os.path.basename(__file__)
    killed_count = 0
    
    try:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['pid'] != current_pid and proc.info['cmdline']:
                    cmdline = ' '.join(proc.info['cmdline'])
                    if current_script in cmdline and 'python' in proc.info['name'].lower():
                        logger.info(f"🔪 Killing existing instance: {proc.info['pid']}")
                        proc.kill()
                        killed_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    except Exception as e:
        logger.warning(f"Error killing processes: {e}")
    
    if killed_count > 0:
        logger.info(f"✅ Killed {killed_count} existing instances")
        time.sleep(5)  # Ждем завершения процессов

def run_flask():
    """Запуск Flask сервера"""
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
    """Очистка подключений бота"""
    try:
        logger.info("🧹 Clearing bot connections...")
        
        # Удаляем webhook
        webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        try:
            response = requests.post(webhook_url, json={'drop_pending_updates': True}, timeout=10)
            logger.info(f"Webhook deletion: {response.status_code}")
        except Exception as e:
            logger.warning(f"Webhook deletion failed: {e}")
        
        # Ждем дольше для очистки
        await asyncio.sleep(8)
        
        # Агрессивная очистка pending updates
        updates_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
        for attempt in range(5):
            try:
                response = requests.post(
                    updates_url, 
                    json={'offset': -1, 'limit': 100, 'timeout': 1}, 
                    timeout=8
                )
                if response.status_code == 200:
                    data = response.json()
                    if data.get('result'):
                        last_update_id = data['result'][-1]['update_id']
                        requests.post(
                            updates_url, 
                            json={'offset': last_update_id + 1}, 
                            timeout=8
                        )
                        logger.info(f"Cleared updates up to ID: {last_update_id}")
                    break
            except Exception as e:
                logger.warning(f"Update clearing attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(3)
        
        await asyncio.sleep(5)
        logger.info("✅ Bot connections cleared")
    except Exception as e:
        logger.error(f"Error clearing connections: {e}")
        await asyncio.sleep(10)

def make_fpl_request(url, timeout=20, max_retries=3):
    """Выполнение запроса к FPL API"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Rate limit
                wait_time = (attempt + 1) * 5
                logger.warning(f"Rate limited, waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1} for {url}")
        except Exception as e:
            logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
        
        if attempt < max_retries - 1:
            time.sleep((attempt + 1) * 3)
    
    logger.error(f"All attempts failed for: {url}")
    return None

def get_current_gameweek():
    """Получение текущего gameweek"""
    try:
        data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        if not data or 'events' not in data:
            return None
        
        events = data['events']
        
        # Ищем текущий gameweek
        for event in events:
            if event.get('is_current', False):
                return event['id']
        
        # Если текущего нет, ищем следующий
        for event in events:
            if event.get('is_next', False):
                return event['id']
        
        # Если и следующего нет, ищем незавершенный
        for event in events:
            if not event.get('finished', True):
                return event['id']
        
        # Возвращаем последний
        return events[-1]['id'] if events else None
    except Exception as e:
        logger.error(f"Error getting gameweek: {e}")
        return None

def get_league_standings():
    """Получение таблицы лиги"""
    try:
        data = make_fpl_request(f"https://fantasy.premierleague.com/api/leagues-classic/{LEAGUE_ID}/standings/")
        if not data or 'standings' not in data or 'results' not in data['standings']:
            return []
        return data['standings']['results'][:15]  # Увеличиваем до 15
    except Exception as e:
        logger.error(f"Error getting standings: {e}")
        return []

def get_manager_picks_batch(manager_ids, gameweek):
    """Пакетное получение составов менеджеров"""
    def fetch_picks(manager_id):
        try:
            url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{gameweek}/picks/"
            data = make_fpl_request(url, timeout=15)
            return manager_id, data.get('picks', []) if data else []
        except Exception as e:
            logger.error(f"Error fetching picks for {manager_id}: {e}")
            return manager_id, []
    
    results = {}
    # Уменьшаем количество одновременных запросов
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(fetch_picks, mid): mid for mid in manager_ids}
        for future in futures:
            try:
                manager_id, picks = future.result(timeout=20)
                results[manager_id] = picks
            except Exception as e:
                logger.error(f"Future failed for manager {futures[future]}: {e}")
                results[futures[future]] = []
    return results

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда получения очков"""
    try:
        message_obj = await update.message.reply_text("🔄 Загружаю данные...")
        
        # Получаем текущий gameweek
        current_gw = get_current_gameweek()
        if not current_gw:
            await message_obj.edit_text("❌ Не удалось получить текущий gameweek")
            return
        
        await message_obj.edit_text(f"📅 Текущий Gameweek: {current_gw}\n🔄 Загружаю данные игроков...")
        
        # Получаем данные игроков
        bootstrap_data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        if not bootstrap_data or 'elements' not in bootstrap_data:
            await message_obj.edit_text("❌ Не удалось получить данные игроков")
            return
        
        players = {p['id']: p for p in bootstrap_data['elements']}
        teams = {t['id']: t['name'] for t in bootstrap_data['teams']}
        
        await message_obj.edit_text(f"📅 GW{current_gw}\n🔄 Загружаю live данные...")
        
        # Получаем live данные
        live_data = make_fpl_request(f"https://fantasy.premierleague.com/api/event/{current_gw}/live/")
        if not live_data or 'elements' not in live_data:
            await message_obj.edit_text("❌ Не удалось получить live данные")
            return
        
        live_points = {}
        for item in live_data['elements']:
            if 'stats' in item and 'total_points' in item['stats']:
                live_points[item['id']] = item['stats']['total_points']
        
        await message_obj.edit_text(f"📅 GW{current_gw}\n🔄 Загружаю данные лиги...")
        
        # Получаем менеджеров
        managers = get_league_standings()
        if not managers:
            await message_obj.edit_text("❌ Не удалось получить данные лиги")
            return
        
        await message_obj.edit_text(f"📅 GW{current_gw}\n🔄 Загружаю составы менеджеров...")
        
        # Получаем составы
        manager_ids = [m['entry'] for m in managers]
        all_picks = get_manager_picks_batch(manager_ids, current_gw)
        
        successful_picks = len([p for p in all_picks.values() if p])
        if successful_picks == 0:
            await message_obj.edit_text("❌ Не удалось получить составы менеджеров")
            return
        
        # Обрабатываем данные
        team_players = {}
        manager_names = {m['entry']: m['entry_name'] for m in managers}
        
        for manager_id, picks in all_picks.items():
            if not picks:
                continue
            
            manager_name = manager_names.get(manager_id, f"Manager {manager_id}")
            
            # Берем только основной состав (11 игроков)
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
            await message_obj.edit_text("❌ Не найдено данных об игроках")
            return
        
        # Формируем сообщение
        message = f"🏆 *Лига {LEAGUE_ID} - GW{current_gw}*\n"
        message += f"👥 Данные от {successful_picks} менеджеров\n"
        message += f"⏰ Обновлено: {datetime.now().strftime('%H:%M')}\n\n"
        
        # Сортируем команды по количеству игроков
        sorted_teams = sorted(team_players.items(), key=lambda x: len(x[1]), reverse=True)[:8]
        
        for team_name, players_list in sorted_teams:
            message += f"⚽ *{team_name.upper()}*\n"
            # Сортируем игроков по очкам с учетом множителя
            sorted_players = sorted(players_list, key=lambda x: x['points'] * x['multiplier'], reverse=True)[:6]
            
            for player in sorted_players:
                total_points = player['points'] * player['multiplier']
                multiplier_text = ""
                if player['multiplier'] == 2:
                    multiplier_text = " (C)"
                elif player['multiplier'] == 1.5:
                    multiplier_text = " (VC)"
                
                message += f"• {player['name']}{multiplier_text} - {total_points} pts ({player['manager']})\n"
            message += "\n"
        
        # Разбиваем длинные сообщения
        if len(message) > 4000:
            parts = message.split('\n\n')
            current_message = f"🏆 *Лига {LEAGUE_ID} - GW{current_gw}*\n👥 Данные от {successful_picks} менеджеров\n⏰ {datetime.now().strftime('%H:%M')}\n\n"
            
            await message_obj.delete()
            
            for part in parts[1:]:
                if len(current_message + part) > 3800:
                    await update.message.reply_text(current_message, parse_mode='Markdown')
                    current_message = part + "\n\n"
                else:
                    current_message += part + "\n\n"
            
            if current_message.strip():
                await update.message.reply_text(current_message, parse_mode='Markdown')
        else:
            await message_obj.edit_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error in points_command: {e}")
        try:
            await update.message.reply_text(f"❌ Произошла ошибка: {str(e)[:100]}...")
        except:
            pass

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда старт"""
    welcome_text = f"""🤖 *FPL League Bot v2.0*

*Команды:*
/points - Получить очки текущего gameweek для лиги {LEAGUE_ID}

🔥 *Новые возможности:*
• Улучшенная стабильность
• Быстрая загрузка данных
• Защита от конфликтов
• Подробная статистика

⚡ Обрабатывается топ-15 менеджеров лиги."""
    
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

async def run_bot():
    """Основной цикл бота"""
    global bot_application, bot_running
    
    logger.info("🚀 Starting FPL Bot v2.0...")
    
    # Очищаем подключения
    await clear_bot_connections()
    
    # Создаем приложение
    bot_application = Application.builder().token(BOT_TOKEN).build()
    bot_application.add_handler(CommandHandler("start", start_command))
    bot_application.add_handler(CommandHandler("points", points_command))
    
    await bot_application.initialize()
    await bot_application.start()
    
    logger.info("✅ Bot started successfully!")
    
    last_update_id = 0
    consecutive_errors = 0
    conflict_count = 0
    
    try:
        while bot_running:
            try:
                updates = await bot_application.bot.get_updates(
                    offset=last_update_id + 1,
                    timeout=30,
                    limit=100
                )
                
                # Сбрасываем счетчики при успехе
                consecutive_errors = 0
                conflict_count = 0
                
                for update in updates:
                    last_update_id = update.update_id
                    try:
                        await bot_application.process_update(update)
                    except Exception as e:
                        logger.error(f"Error processing update {update.update_id}: {e}")
                
                if not updates:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                error_msg = str(e)
                
                if "Conflict" in error_msg:
                    conflict_count += 1
                    logger.warning(f"Polling conflict #{conflict_count}: {error_msg}")
                    
                    if conflict_count >= 3:
                        logger.error("Too many conflicts, clearing connections...")
                        await clear_bot_connections()
                        conflict_count = 0
                        await asyncio.sleep(30)
                    else:
                        await asyncio.sleep(20 + (conflict_count * 10))
                        
                elif "timeout" in error_msg.lower():
                    logger.warning("Timeout error, continuing...")
                    await asyncio.sleep(5)
                    
                else:
                    consecutive_errors += 1
                    logger.error(f"Polling error #{consecutive_errors}: {e}")
                    await asyncio.sleep(min(consecutive_errors * 3, 60))
                
                if consecutive_errors >= 10:
                    logger.error("Too many consecutive errors, stopping bot")
                    break
                    
    except KeyboardInterrupt:
        logger.info("Bot stopping due to keyboard interrupt...")
    except Exception as e:
        logger.error(f"Critical error in bot loop: {e}")
    finally:
        bot_running = False
        if bot_application:
            try:
                await bot_application.stop()
                await bot_application.shutdown()
                logger.info("✅ Bot shutdown complete")
            except Exception as e:
                logger.error(f"Error during shutdown: {e}")

def signal_handler(sig, frame):
    """Обработчик сигналов остановки"""
    global bot_running, lock_fd
    logger.info(f"Received signal {sig}, stopping bot...")
    bot_running = False
    
    if lock_fd:
        try:
            lock_fd.close()
        except:
            pass
    
    sys.exit(0)

def main():
    """Главная функция"""
    global lock_fd
    
    # Устанавливаем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Убиваем существующие экземпляры
        kill_existing_instances()
        
        # Получаем блокировку
        lock_fd = acquire_lock()
        
        # Запускаем Flask в отдельном потоке
        flask_thread = Thread(target=run_flask, daemon=True)
        flask_thread.start()
        logger.info("🌐 Flask server started")
        
        # Запускаем бота
        asyncio.run(run_bot())
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Critical error in main: {e}")
    finally:
        if lock_fd:
            try:
                lock_fd.close()
                os.unlink('/tmp/fpl_bot.lock')
            except:
                pass
        logger.info("🔒 Lock released, exiting")

if __name__ == '__main__':
    main()
