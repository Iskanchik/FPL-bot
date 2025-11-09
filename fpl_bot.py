import requests
import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from flask import Flask
from threading import Thread
import os
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime, timezone

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Отключаем предупреждения werkzeug
logging.getLogger('werkzeug').setLevel(logging.ERROR)

# Настройки
BOT_TOKEN = "8554755843:AAHZrdxLhNTDkr4P_G-zreyH2Poa_gsL6XY"  # ← ЗАМЕНИТЕ НА СВОЙ ТОКЕН!
LEAGUE_ID = 980121

# Flask приложение для Render
app = Flask(__name__)

@app.route('/')
def home():
    return "FPL Bot is running! 🤖⚽"

@app.route('/health')
def health():
    return {"status": "healthy", "bot": "running"}

def run_flask():
    """Запуск Flask с production сервером"""
    port = int(os.environ.get('PORT', 5000))
    
    try:
        # Пытаемся использовать waitress (production WSGI server)
        from waitress import serve
        logger.info(f"🚀 Starting production server on port {port}")
        serve(app, host='0.0.0.0', port=port, threads=4)
    except ImportError:
        try:
            # Если waitress нет, пытаемся gunicorn
            import gunicorn.app.wsgiapp as wsgi
            logger.info(f"🚀 Starting gunicorn server on port {port}")
            # Это для случая если gunicorn установлен
            app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)
        except ImportError:
            # Fallback на встроенный сервер с отключенными предупреждениями
            logger.warning("⚠️ Using development server (install waitress for production)")
            import warnings
            warnings.filterwarnings("ignore", message=".*development server.*")
            
            app.run(
                host='0.0.0.0', 
                port=port, 
                debug=False, 
                use_reloader=False, 
                threaded=True
            )

def make_fpl_request(url, timeout=15, max_retries=3):
    """Улучшенные запросы к FPL API с лучшей обработкой ошибок"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Making request to: {url} (attempt {attempt + 1})")
            response = requests.get(url, headers=headers, timeout=timeout)
            
            logger.info(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info(f"Successfully parsed JSON data")
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                    logger.error(f"Response content: {response.text[:500]}")
            else:
                logger.error(f"HTTP error {response.status_code}: {response.text[:200]}")
                
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout on attempt {attempt + 1}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error on attempt {attempt + 1}")
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
        
        if attempt < max_retries - 1:
            sleep_time = (attempt + 1) * 2
            logger.info(f"Waiting {sleep_time} seconds before retry...")
            time.sleep(sleep_time)
    
    logger.error(f"All {max_retries} attempts failed for URL: {url}")
    return None

def get_current_gameweek():
    """Улучшенное получение текущего gameweek"""
    try:
        logger.info("Fetching current gameweek...")
        data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        
        if not data:
            logger.error("No data received from bootstrap-static")
            return None
        
        if 'events' not in data:
            logger.error("No 'events' key in bootstrap data")
            logger.error(f"Available keys: {list(data.keys())}")
            return None
        
        events = data['events']
        logger.info(f"Found {len(events)} events")
        
        # Логируем информацию о событиях для отладки
        for i, event in enumerate(events[:5]):  # Первые 5 для отладки
            logger.info(f"Event {i+1}: ID={event.get('id')}, Name='{event.get('name')}', "
                       f"Current={event.get('is_current')}, Next={event.get('is_next')}, "
                       f"Finished={event.get('finished')}")
        
        # Ищем текущий gameweek
        current_gw = None
        for event in events:
            if event.get('is_current', False):
                current_gw = event['id']
                logger.info(f"Found current gameweek: {current_gw}")
                break
        
        # Если текущий не найден, ищем следующий
        if not current_gw:
            for event in events:
                if event.get('is_next', False):
                    current_gw = event['id']
                    logger.info(f"Found next gameweek: {current_gw}")
                    break
        
        # Если и следующий не найден, берем первый незавершенный
        if not current_gw:
            for event in events:
                if not event.get('finished', True):
                    current_gw = event['id']
                    logger.info(f"Found first unfinished gameweek: {current_gw}")
                    break
        
        # В крайнем случае берем последний
        if not current_gw and events:
            current_gw = events[-1]['id']
            logger.info(f"Using last gameweek: {current_gw}")
        
        return current_gw
        
    except Exception as e:
        logger.error(f"Error getting current gameweek: {e}")
        return None

def get_league_standings():
    """Получить топ менеджеров лиги с улучшенной обработкой"""
    try:
        logger.info(f"Fetching league standings for league {LEAGUE_ID}")
        data = make_fpl_request(f"https://fantasy.premierleague.com/api/leagues-classic/{LEAGUE_ID}/standings/")
        
        if not data:
            logger.error("No data received from league standings")
            return []
        
        if 'standings' not in data:
            logger.error("No 'standings' key in league data")
            logger.error(f"Available keys: {list(data.keys())}")
            return []
        
        if 'results' not in data['standings']:
            logger.error("No 'results' key in standings data")
            logger.error(f"Available keys in standings: {list(data['standings'].keys())}")
            return []
        
        results = data['standings']['results']
        logger.info(f"Found {len(results)} managers in league")
        
        # Берем только топ-10 для быстроты
        top_managers = results[:10]
        logger.info(f"Using top {len(top_managers)} managers")
        
        return top_managers
        
    except Exception as e:
        logger.error(f"Error getting league standings: {e}")
        return []

def get_manager_picks_batch(manager_ids, gameweek):
    """Получить составы менеджеров параллельно с улучшенной обработкой"""
    def fetch_picks(manager_id):
        try:
            url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{gameweek}/picks/"
            logger.info(f"Fetching picks for manager {manager_id}, GW {gameweek}")
            data = make_fpl_request(url, timeout=10)
            
            if data and 'picks' in data:
                logger.info(f"Successfully got {len(data['picks'])} picks for manager {manager_id}")
                return manager_id, data['picks']
            else:
                logger.warning(f"No picks data for manager {manager_id}")
                return manager_id, []
        except Exception as e:
            logger.error(f"Error fetching picks for manager {manager_id}: {e}")
            return manager_id, []
    
    results = {}
    logger.info(f"Fetching picks for {len(manager_ids)} managers")
    
    with ThreadPoolExecutor(max_workers=3) as executor:  # Уменьшили до 3 для стабильности
        futures = {executor.submit(fetch_picks, mid): mid for mid in manager_ids}
        
        for future in futures:
            try:
                manager_id, picks = future.result(timeout=15)
                results[manager_id] = picks
            except Exception as e:
                logger.error(f"Error getting result for manager {futures[future]}: {e}")
                results[futures[future]] = []
    
    successful_fetches = len([p for p in results.values() if p])
    logger.info(f"Successfully fetched picks for {successful_fetches}/{len(manager_ids)} managers")
    
    return results

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для получения очков лиги с улучшенной обработкой ошибок"""
    try:
        await update.message.reply_text("🔄 Загружаю данные...")
        
        # Шаг 1: Получаем текущий gameweek
        logger.info("Step 1: Getting current gameweek")
        current_gw = get_current_gameweek()
        if not current_gw:
            await update.message.reply_text("❌ Не удалось получить текущий gameweek. Попробуйте позже.")
            return
        
        await update.message.reply_text(f"📅 Текущий Gameweek: {current_gw}")
        
        # Шаг 2: Получаем данные игроков и команд
        logger.info("Step 2: Getting bootstrap data")
        bootstrap_data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        if not bootstrap_data:
            await update.message.reply_text("❌ Не удалось получить данные игроков")
            return
        
        if 'elements' not in bootstrap_data or 'teams' not in bootstrap_data:
            await update.message.reply_text("❌ Неполные данные от FPL API")
            return
        
        players = {p['id']: p for p in bootstrap_data['elements']}
        teams = {t['id']: t['name'] for t in bootstrap_data['teams']}
        logger.info(f"Loaded {len(players)} players and {len(teams)} teams")
        
        # Шаг 3: Получаем live очки
        logger.info("Step 3: Getting live data")
        live_data = make_fpl_request(f"https://fantasy.premierleague.com/api/event/{current_gw}/live/")
        if not live_data:
            await update.message.reply_text("❌ Не удалось получить live данные. Возможно, gameweek еще не начался.")
            return
        
        if 'elements' not in live_data:
            await update.message.reply_text("❌ Неполные live данные")
            return
        
        live_points = {}
        for item in live_data['elements']:
            if 'stats' in item and 'total_points' in item['stats']:
                live_points[item['id']] = item['stats']['total_points']
        
        logger.info(f"Loaded live points for {len(live_points)} players")
        
        # Шаг 4: Получаем менеджеров лиги
        logger.info("Step 4: Getting league standings")
        managers = get_league_standings()
        if not managers:
            await update.message.reply_text("❌ Не удалось получить данные лиги")
            return
        
        await update.message.reply_text(f"👥 Найдено {len(managers)} менеджеров в лиге")
        
        # Шаг 5: Получаем составы параллельно
        logger.info("Step 5: Getting manager picks")
        manager_ids = [m['entry'] for m in managers]
        all_picks = get_manager_picks_batch(manager_ids, current_gw)
        
        successful_picks = len([p for p in all_picks.values() if p])
        if successful_picks == 0:
            await update.message.reply_text("❌ Не удалось получить составы менеджеров")
            return
        
        await update.message.reply_text(f"✅ Получены составы {successful_picks} менеджеров")
        
        # Шаг 6: Группируем игроков по командам
        logger.info("Step 6: Processing data")
        team_players = {}
        manager_names = {m['entry']: m['entry_name'] for m in managers}
        
        for manager_id, picks in all_picks.items():
            if not picks:
                continue
                
            manager_name = manager_names.get(manager_id, f"Manager {manager_id}")
            
            # Берем только стартовый состав (первые 11)
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
        
        # Формируем ответ
        message = f"🏆 Лига {LEAGUE_ID} - GW{current_gw}\n"
        message += f"👥 Данные от {successful_picks} менеджеров\n\n"
        
        # Показываем топ-5 команд по количеству игроков
        sorted_teams = sorted(team_players.items(), key=lambda x: len(x[1]), reverse=True)[:5]
        
        for team_name, players_list in sorted_teams:
            message += f"⚽ **{team_name.upper()}**\n"
            
            # Сортируем игроков по очкам
            sorted_players = sorted(players_list, key=lambda x: x['points'] * x['multiplier'], reverse=True)[:5]
            
            for player in sorted_players:
                total_points = player['points'] * player['multiplier']
                multiplier_text = f" (C)" if player['multiplier'] == 2 else f" (VC)" if player['multiplier'] == 1.5 else ""
                message += f"• {player['name']}{multiplier_text} - {total_points} pts ({player['manager']})\n"
            
            message += "\n"
        
        # Разбиваем сообщение если оно слишком длинное
        if len(message) > 4000:
            parts = message.split('\n\n')
            current_message = f"🏆 Лига {LEAGUE_ID} - GW{current_gw}\n"
            current_message += f"👥 Данные от {successful_picks} менеджеров\n\n"
            
            for part in parts[1:]:  # Пропускаем заголовок
                if len(current_message + part) > 3500:
                    await update.message.reply_text(current_message)
                    current_message = part + "\n\n"
                else:
                    current_message += part + "\n\n"
            
            if current_message.strip():
                await update.message.reply_text(current_message)
        else:
            await update.message.reply_text(message)
        
        logger.info("Successfully completed points command")
        
    except Exception as e:
        logger.error(f"Error in points_command: {e}")
        await update.message.reply_text(f"❌ Произошла ошибка: {str(e)}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    welcome_text = f"""
🤖 **FPL League Bot**

**Команды:**
/points - Получить очки текущего gameweek для лиги {LEAGUE_ID}

Бот показывает игроков, сгруппированных по командам Премьер-лиги, с очками и именами менеджеров.

⚡ Обрабатывается топ-10 менеджеров для быстроты работы.
    """
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

async def run_bot():
    """Запуск бота с ручным управлением"""
    logger.info("🚀 Запуск FPL Bot...")
    
    # Очистка webhook
    try:
        webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        requests.post(webhook_url, json={'drop_pending_updates': True}, timeout=5)
        logger.info("✅ Webhook очищен")
    except:
        pass
    
    # Создание приложения БЕЗ updater
    application = Application.builder().token(BOT_TOKEN).updater(None).build()
    
    # Добавление обработчиков
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("points", points_command))
    
    # Инициализация
    await application.initialize()
    await application.start()
    
    logger.info("✅ Бот успешно запущен!")
    
    # Ручной polling loop
    try:
        while True:
            try:
                # Получаем обновления
                updates = await application.bot.get_updates(
                    offset=getattr(run_bot, 'last_update_id', 0) + 1,
                    timeout=10,
                    limit=100
                )
                
                # Обрабатываем каждое обновление
                for update in updates:
                    run_bot.last_update_id = update.update_id
                    
                    # Обрабатываем обновление
                    await application.process_update(update)
                
                # Небольшая пауза если нет обновлений
                if not updates:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Ошибка в polling loop: {e}")
                await asyncio.sleep(5)
                
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    finally:
        await application.stop()
        await application.shutdown()

def main():
    """Главная функция - синхронная"""
    # Запуск Flask в отдельном потоке
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Запуск бота в основном потоке
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print("Бот остановлен")

if __name__ == '__main__':
    main()
