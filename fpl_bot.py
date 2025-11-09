import requests
import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from flask import Flask
from threading import Thread
import os
import json

# Настройки
BOT_TOKEN = "8554755843:AAFpoM3sRxuvgSutlQLrObjquNt2xdJAT9k"
LEAGUE_ID = 980121

# Flask приложение для Render (чтобы не засыпал)
app = Flask(__name__)

@app.route('/')
def home():
    return "FPL Bot is running! 🤖⚽"

@app.route('/health')
def health():
    return {"status": "healthy", "bot": "running"}

def run_flask():
    """Запуск Flask в отдельном потоке"""
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def aggressive_clear_bot():
    """Агрессивная очистка всех подключений бота"""
    try:
        print("🔥 Starting aggressive bot cleanup...")
        
        # 1. Удаляем webhook
        delete_webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        response = requests.post(delete_webhook_url, json={'drop_pending_updates': True})
        print(f"Webhook delete response: {response.json()}")
        
        # 2. Получаем и очищаем все pending updates
        for i in range(5):  # Делаем несколько попыток
            get_updates_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            params = {'offset': -1, 'limit': 100, 'timeout': 1}
            response = requests.get(get_updates_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('ok') and data.get('result'):
                    print(f"Cleared {len(data['result'])} pending updates (attempt {i+1})")
                    if len(data['result']) == 0:
                        break
                else:
                    print(f"No pending updates (attempt {i+1})")
                    break
            else:
                print(f"Error getting updates: {response.status_code}")
            
            time.sleep(2)
        
        # 3. Проверяем статус бота
        get_me_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getMe"
        response = requests.get(get_me_url)
        if response.status_code == 200:
            bot_info = response.json()
            print(f"Bot info: {bot_info}")
        else:
            print(f"Error getting bot info: {response.status_code}")
        
        print("✅ Bot cleanup completed")
        
    except Exception as e:
        print(f"Error during bot cleanup: {e}")

def make_fpl_request(url, max_retries=3):
    """Делает запрос к FPL API с повторными попытками"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            print(f"Making request to {url} (attempt {attempt + 1})")
            response = requests.get(url, headers=headers, timeout=30)
            
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                if response.text.strip():
                    try:
                        data = response.json()
                        print(f"Successfully parsed JSON data")
                        return data
                    except json.JSONDecodeError as e:
                        print(f"JSON decode error: {e}")
                        print(f"Response text: {response.text[:200]}...")
                else:
                    print("Empty response body")
            else:
                print(f"HTTP error: {response.status_code}")
                
        except requests.exceptions.Timeout:
            print(f"Timeout on attempt {attempt + 1}")
        except requests.exceptions.ConnectionError:
            print(f"Connection error on attempt {attempt + 1}")
        except Exception as e:
            print(f"Unexpected error on attempt {attempt + 1}: {e}")
        
        if attempt < max_retries - 1:
            print(f"Waiting 5 seconds before retry...")
            time.sleep(5)
    
    print(f"Failed to get data from {url} after {max_retries} attempts")
    return None

# FPL API функции
def get_current_gameweek():
    """Get current gameweek number"""
    try:
        data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        
        if not data or 'events' not in data:
            print("No events data found")
            return None
        
        print(f"Found {len(data['events'])} events")
        
        current_gw = None
        
        # Вариант 1: is_current = True
        for event in data['events']:
            if event.get('is_current', False):
                current_gw = event['id']
                print(f"Found current gameweek (is_current): {current_gw}")
                break
        
        # Вариант 2: is_next = False и finished = False
        if not current_gw:
            for event in data['events']:
                if not event.get('finished', True) and not event.get('is_next', False):
                    current_gw = event['id']
                    print(f"Found current gameweek (not finished): {current_gw}")
                    break
        
        # Вариант 3: берем первый незавершенный
        if not current_gw:
            for event in data['events']:
                if not event.get('finished', True):
                    current_gw = event['id']
                    print(f"Found current gameweek (first unfinished): {current_gw}")
                    break
        
        # Вариант 4: если все завершены, берем последний
        if not current_gw:
            current_gw = data['events'][-1]['id']
            print(f"Using last gameweek: {current_gw}")
        
        return current_gw
        
    except Exception as e:
        print(f"Error getting gameweek: {e}")
        return None

def get_league_managers():
    """Get all managers in the league"""
    try:
        data = make_fpl_request(f"https://fantasy.premierleague.com/api/leagues-classic/{LEAGUE_ID}/standings/")
        
        if not data or 'standings' not in data or 'results' not in data['standings']:
            print("No league standings data found")
            return []
            
        return data['standings']['results']
    except Exception as e:
        print(f"Error getting managers: {e}")
        return []

def get_manager_picks(manager_id, gameweek):
    """Get manager's picks for specific gameweek"""
    try:
        data = make_fpl_request(f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{gameweek}/picks/")
        
        if not data or 'picks' not in data:
            print(f"No picks data found for manager {manager_id}")
            return {'picks': []}
            
        return data
    except Exception as e:
        print(f"Error getting picks for manager {manager_id}: {e}")
        return {'picks': []}

def get_live_data(gameweek):
    """Get live points data for gameweek"""
    try:
        data = make_fpl_request(f"https://fantasy.premierleague.com/api/event/{gameweek}/live/")
        
        if not data or 'elements' not in data:
            print(f"No live data found for gameweek {gameweek}")
            return {'elements': []}
            
        return data
    except Exception as e:
        print(f"Error getting live data: {e}")
        return {'elements': []}

def get_bootstrap_data():
    """Get player and team data"""
    try:
        data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        
        if not data or 'elements' not in data or 'teams' not in data:
            print("No bootstrap data found")
            return {'elements': [], 'teams': []}
            
        return data
    except Exception as e:
        print(f"Error getting bootstrap data: {e}")
        return {'elements': [], 'teams': []}

# Telegram bot команды
async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug command to check API data"""
    try:
        await update.message.reply_text("🔍 Checking FPL API...")
        
        data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        
        if not data:
            await update.message.reply_text("❌ Failed to connect to FPL API")
            return
        
        debug_info = "🔍 Debug Info:\n\n"
        debug_info += f"✅ API Connection: OK\n"
        debug_info += f"Total events: {len(data.get('events', []))}\n"
        debug_info += f"Total players: {len(data.get('elements', []))}\n"
        debug_info += f"Total teams: {len(data.get('teams', []))}\n\n"
        
        if 'events' in data and data['events']:
            debug_info += "Recent gameweeks:\n"
            for event in data['events'][-5:]:
                status = []
                if event.get('is_current'): status.append('CURRENT')
                if event.get('is_next'): status.append('NEXT')
                if event.get('finished'): status.append('FINISHED')
                
                debug_info += f"GW{event['id']}: {event['name']} - {', '.join(status) if status else 'ACTIVE'}\n"
        
        await update.message.reply_text(debug_info)
        
    except Exception as e:
        await update.message.reply_text(f"Debug error: {str(e)}")

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /points command"""
    await update.message.reply_text("🔄 Fetching league points data...")
    
    try:
        current_gw = get_current_gameweek()
        if not current_gw:
            await update.message.reply_text("❌ Could not determine current gameweek. FPL API might be down.")
            return
        
        bootstrap_data = get_bootstrap_data()
        if not bootstrap_data['elements'] or not bootstrap_data['teams']:
            await update.message.reply_text("❌ Could not fetch player/team data from FPL API")
            return
            
        players = {p['id']: p for p in bootstrap_data['elements']}
        teams = {t['id']: t['name'] for t in bootstrap_data['teams']}
        
        managers = get_league_managers()
        if not managers:
            await update.message.reply_text("❌ Could not fetch league managers. Check league ID.")
            return
        
        live_data = get_live_data(current_gw)
        live_points = {item['id']: item['stats']['total_points'] for item in live_data['elements']}
        
        team_players = {}
        processed_managers = 0
        
        for manager in managers:
            manager_name = manager['entry_name']
            manager_id = manager['entry']
            
            picks_data = get_manager_picks(manager_id, current_gw)
            
            if picks_data['picks']:
                processed_managers += 1
                
                for pick in picks_data['picks'][:11]:
                    player_id = pick['element']
                    if player_id not in players:
                        continue
                        
                    player = players[player_id]
                    team_id = player['team']
                    team_name = teams.get(team_id, 'Unknown')
                    
                    points = live_points.get(player_id, 0)
                    
                    if team_name not in team_players:
                        team_players[team_name] = []
                    
                    team_players[team_name].append({
                        'name': player['web_name'],
                        'manager': manager_name,
                        'points': points
                    })
        
        if not team_players:
            await update.message.reply_text(f"❌ No player data found. Processed {processed_managers} managers.")
            return
        
        message = f"🏆 League {LEAGUE_ID} - Gameweek {current_gw} Points\n"
        message += f"📊 Processed {processed_managers} managers\n\n"
        
        for team_name in sorted(team_players.keys()):
            message += f"⚽ {team_name.upper()}\n"
            
            sorted_players = sorted(team_players[team_name], key=lambda x: x['points'], reverse=True)
            
            for player in sorted_players:
                message += f"• {player['name']} ({player['manager']}) - {player['points']} pts\n"
            
            message += "\n"
        
        if len(message) > 4000:
            messages = []
            current_msg = f"🏆 League {LEAGUE_ID} - Gameweek {current_gw} Points\n📊 Processed {processed_managers} managers\n\n"
            
            for team_name in sorted(team_players.keys()):
                team_section = f"⚽ {team_name.upper()}\n"
                sorted_players = sorted(team_players[team_name], key=lambda x: x['points'], reverse=True)
                
                for player in sorted_players:
                    team_section += f"• {player['name']} ({player['manager']}) - {player['points']} pts\n"
                team_section += "\n"
                
                if len(current_msg + team_section) > 4000:
                    messages.append(current_msg)
                    current_msg = team_section
                else:
                    current_msg += team_section
            
            messages.append(current_msg)
            
            for msg in messages:
                await update.message.reply_text(msg)
        else:
            await update.message.reply_text(message)
            
    except Exception as e:
        await update.message.reply_text(f"❌ Error fetching data: {str(e)}")
        print(f"Error in points_command: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    welcome_text = f"""
🤖 FPL League Bot

Commands:
/points - Get current gameweek points for league {LEAGUE_ID}
/debug - Show API connection debug info

The bot will show all players organized by their real Premier League teams with points and manager names.
    """
    await update.message.reply_text(welcome_text)

def main():
    """Start the bot"""
    print("🚀 Starting FPL Bot with aggressive cleanup...")
    
    # Агрессивная очистка всех подключений
    aggressive_clear_bot()
    time.sleep(5)  # Ждем дольше
    
    # Запускаем Flask в отдельном потоке
    flask_thread = Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    print("Flask server started")
    
    # Запускаем Telegram бота с увеличенными задержками
    max_retries = 5
    for attempt in range(max_retries):
        try:
            print(f"🔄 Bot start attempt {attempt + 1}/{max_retries}")
            
            # Дополнительная очистка перед каждой попыткой
            if attempt > 0:
                aggressive_clear_bot()
                time.sleep(10)  # Ждем еще дольше
            
            application = Application.builder().token(BOT_TOKEN).build()
            
            application.add_handler(CommandHandler("start", start_command))
            application.add_handler(CommandHandler("points", points_command))
            application.add_handler(CommandHandler("debug", debug_command))
            
            print("Bot handlers added")
            print("Starting polling with extended timeouts...")
            
            # Запускаем с максимальными таймаутами
            application.run_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES,
                timeout=30,
                pool_timeout=30,
                connect_timeout=30,
                read_timeout=30,
                write_timeout=30
            )
            break  # Если успешно запустился, выходим из цикла
            
        except Exception as e:
            print(f"❌ Error starting bot (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10  # Увеличиваем время ожидания
                print(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print("💀 Failed to start bot after all attempts")

if __name__ == '__main__':
    main()
