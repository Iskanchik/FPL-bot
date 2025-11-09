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

def make_fpl_request(url, timeout=10, max_retries=2):
    """Быстрые запросы к FPL API"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.warning(f"Request failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
    
    return None

def get_current_gameweek():
    """Получить текущий gameweek"""
    try:
        data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        if not data or 'events' not in data:
            return None
        
        for event in data['events']:
            if event.get('is_current', False):
                return event['id']
        
        # Если текущий не найден, берем первый незавершенный
        for event in data['events']:
            if not event.get('finished', True):
                return event['id']
        
        return data['events'][-1]['id']
    except:
        return None

def get_league_standings():
    """Получить топ менеджеров лиги"""
    try:
        data = make_fpl_request(f"https://fantasy.premierleague.com/api/leagues-classic/{LEAGUE_ID}/standings/")
        if not data or 'standings' not in data:
            return []
        
        # Берем только топ-10 для быстроты
        return data['standings']['results'][:10]
    except:
        return []

def get_manager_picks_batch(manager_ids, gameweek):
    """Получить составы менеджеров параллельно"""
    def fetch_picks(manager_id):
        url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{gameweek}/picks/"
        data = make_fpl_request(url, timeout=8)
        if data and 'picks' in data:
            return manager_id, data['picks']
        return manager_id, []
    
    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_picks, mid): mid for mid in manager_ids}
        for future in futures:
            try:
                manager_id, picks = future.result(timeout=10)
                results[manager_id] = picks
            except:
                results[futures[future]] = []
    
    return results

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для получения очков лиги"""
    try:
        await update.message.reply_text("🔄 Загружаю данные...")
        
        # Шаг 1: Получаем базовые данные
        current_gw = get_current_gameweek()
        if not current_gw:
            await update.message.reply_text("❌ Не удалось получить текущий gameweek")
            return
        
        # Шаг 2: Получаем данные игроков и команд
        bootstrap_data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        if not bootstrap_data:
            await update.message.reply_text("❌ Не удалось получить данные игроков")
            return
        
        players = {p['id']: p for p in bootstrap_data['elements']}
        teams = {t['id']: t['name'] for t in bootstrap_data['teams']}
        
        # Шаг 3: Получаем live очки
        live_data = make_fpl_request(f"https://fantasy.premierleague.com/api/event/{current_gw}/live/")
        if not live_data:
            await update.message.reply_text("❌ Не удалось получить live данные")
            return
        
        live_points = {item['id']: item['stats']['total_points'] for item in live_data['elements']}
        
        # Шаг 4: Получаем менеджеров лиги
        managers = get_league_standings()
        if not managers:
            await update.message.reply_text("❌ Не удалось получить данные лиги")
            return
        
        await update.message.reply_text(f"📊 Обрабатываю топ-{len(managers)} менеджеров...")
        
        # Шаг 5: Получаем составы параллельно
        manager_ids = [m['entry'] for m in managers]
        all_picks = get_manager_picks_batch(manager_ids, current_gw)
        
        # Шаг 6: Группируем игроков по командам
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
        message += f"👥 Топ-{len([p for p in all_picks.values() if p])} менеджеров\n\n"
        
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
            current_message += f"👥 Топ-{len([p for p in all_picks.values() if p])} менеджеров\n\n"
            
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

async def main():
    """Запуск бота"""
    logger.info("🚀 Запуск FPL Bot...")
    
    # Запуск Flask в отдельном потоке
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Очистка webhook
    try:
        webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        requests.post(webhook_url, json={'drop_pending_updates': True}, timeout=5)
        logger.info("✅ Webhook очищен")
    except:
        pass
    
    # Простой способ запуска без Updater
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Добавление обработчиков
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("points", points_command))
    
    logger.info("✅ Бот успешно запущен!")
    
    # Запуск polling
    await application.run_polling(
        drop_pending_updates=True,
        close_loop=False
    )

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен")
