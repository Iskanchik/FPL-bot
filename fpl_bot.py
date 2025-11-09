import requests
import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from flask import Flask
from threading import Thread
import os
import json
import asyncio

# Настройки
BOT_TOKEN = "8554755843:AAHZrdxLhNTDkr4P_G-zreyH2Poa_gsL6XY"  # Замените на ваш новый токен
LEAGUE_ID = 980121

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
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def make_fpl_request(url, timeout=15, max_retries=2):
    """Улучшенная функция для запросов к FPL API"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Connection': 'keep-alive',
    }
    
    for attempt in range(max_retries):
        try:
            print(f"🔄 Request to {url} (attempt {attempt + 1}/{max_retries})")
            
            response = requests.get(url, headers=headers, timeout=timeout)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"✅ Success: {len(str(data))} chars received")
                    return data
                except json.JSONDecodeError:
                    print("❌ Invalid JSON response")
                    return None
            else:
                print(f"❌ HTTP {response.status_code}")
                
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout on attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(3)
    
    print(f"❌ Failed after {max_retries} attempts")
    return None

def get_current_gameweek():
    """Get current gameweek with timeout"""
    try:
        print("🔄 Getting current gameweek...")
        data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        
        if not data or 'events' not in data:
            return None
        
        # Найти текущий gameweek
        for event in data['events']:
            if event.get('is_current', False):
                print(f"✅ Current gameweek: {event['id']}")
                return event['id']
        
        # Если не найден, взять первый незавершенный
        for event in data['events']:
            if not event.get('finished', True):
                print(f"✅ Active gameweek: {event['id']}")
                return event['id']
        
        # Последний gameweek
        gw = data['events'][-1]['id']
        print(f"✅ Last gameweek: {gw}")
        return gw
        
    except Exception as e:
        print(f"❌ Error getting gameweek: {e}")
        return None

def get_league_managers():
    """Get league managers with timeout"""
    try:
        print("🔄 Getting league managers...")
        data = make_fpl_request(f"https://fantasy.premierleague.com/api/leagues-classic/{LEAGUE_ID}/standings/")
        
        if not data or 'standings' not in data:
            return []
        
        managers = data['standings']['results']
        print(f"✅ Found {len(managers)} managers")
        return managers
        
    except Exception as e:
        print(f"❌ Error getting managers: {e}")
        return []

def get_bootstrap_data():
    """Get players and teams data with timeout"""
    try:
        print("🔄 Getting bootstrap data...")
        data = make_fpl_request("https://fantasy.premierleague.com/api/bootstrap-static/")
        
        if not data:
            return {'elements': [], 'teams': []}
        
        players_count = len(data.get('elements', []))
        teams_count = len(data.get('teams', []))
        print(f"✅ Bootstrap: {players_count} players, {teams_count} teams")
        
        return data
        
    except Exception as e:
        print(f"❌ Error getting bootstrap: {e}")
        return {'elements': [], 'teams': []}

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /points command with timeout protection"""
    await update.message.reply_text("🔄 Fetching league points data...")
    
    try:
        # Шаг 1: Получить текущий gameweek
        current_gw = get_current_gameweek()
        if not current_gw:
            await update.message.reply_text("❌ Could not get current gameweek. FPL API might be down.")
            return
        
        await update.message.reply_text(f"📅 Current gameweek: {current_gw}")
        
        # Шаг 2: Получить данные игроков и команд
        bootstrap_data = get_bootstrap_data()
        if not bootstrap_data.get('elements') or not bootstrap_data.get('teams'):
            await update.message.reply_text("❌ Could not get players/teams data")
            return
        
        players = {p['id']: p for p in bootstrap_data['elements']}
        teams = {t['id']: t['name'] for t in bootstrap_data['teams']}
        
        await update.message.reply_text(f"✅ Loaded {len(players)} players and {len(teams)} teams")
        
        # Шаг 3: Получить менеджеров лиги
        managers = get_league_managers()
        if not managers:
            await update.message.reply_text("❌ Could not get league managers")
            return
        
        await update.message.reply_text(f"👥 Found {len(managers)} managers")
        
        # Шаг 4: Получить live данные
        print("🔄 Getting live data...")
        live_data = make_fpl_request(f"https://fantasy.premierleague.com/api/event/{current_gw}/live/")
        
        if not live_data or 'elements' not in live_data:
            await update.message.reply_text("❌ Could not get live points data")
            return
        
        live_points = {item['id']: item['stats']['total_points'] for item in live_data['elements']}
        await update.message.reply_text(f"📊 Live points loaded for {len(live_points)} players")
        
        # Шаг 5: Обработать данные менеджеров
        team_players = {}
        processed = 0
        
        for i, manager in enumerate(managers[:10]):  # Ограничиваем до 10 менеджеров для теста
            manager_name = manager['entry_name']
            manager_id = manager['entry']
            
            print(f"🔄 Processing manager {i+1}/{len(managers[:10])}: {manager_name}")
            
            picks_data = make_fpl_request(f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{current_gw}/picks/")
            
            if picks_data and 'picks' in picks_data:
                processed += 1
                
                for pick in picks_data['picks'][:11]:  # Только стартовый состав
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
                        'points': points
                    })
        
        if not team_players:
            await update.message.reply_text("❌ No player data found")
            return
        
        # Формируем ответ
        message = f"🏆 League {LEAGUE_ID} - GW{current_gw}\n"
        message += f"📊 Processed {processed} managers\n\n"
        
        for team_name in sorted(team_players.keys())[:5]:  # Показываем только 5 команд
            message += f"⚽ {team_name.upper()}\n"
            
            sorted_players = sorted(team_players[team_name], key=lambda x: x['points'], reverse=True)[:5]
            
            for player in sorted_players:
                message += f"• {player['name']} ({player['manager']}) - {player['points']} pts\n"
            
            message += "\n"
        
        await update.message.reply_text(message)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        print(f"Error in points_command: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    welcome_text = f"""
🤖 FPL League Bot

Commands:
/points - Get current gameweek points for league {LEAGUE_ID}

The bot shows players organized by Premier League teams with points and manager names.
    """
    await update.message.reply_text(welcome_text)

async def main():
    """Start the bot"""
    print("🚀 Starting FPL Bot...")
    
    # Запуск Flask
    flask_thread = Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    # Очистка webhook
    try:
        webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        requests.post(webhook_url, json={'drop_pending_updates': True}, timeout=10)
        print("✅ Webhook cleared")
    except:
        pass
    
    # Создание приложения
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("points", points_command))
    
    # Запуск
    await application.initialize()
    await application.start()
    
    await application.updater.start_polling(
        drop_pending_updates=True,
        timeout=20,
        pool_timeout=20
    )
    
    print("✅ Bot started successfully!")
    
    # Держим бота запущенным
    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")
