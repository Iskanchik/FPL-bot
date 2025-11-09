import requests
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from flask import Flask, request
from threading import Thread
import os
import json

# Настройки
BOT_TOKEN = "8340413924:AAHpWBHdQxpiyuQIRvzafb-qe2CYLY491IY"
LEAGUE_ID = 980121

# Flask приложение
app = Flask(__name__)

# Глобальная переменная для приложения
telegram_app = None

@app.route('/')
def home():
    return "FPL Bot is running! 🤖⚽"

@app.route('/health')
def health():
    return {"status": "healthy", "bot": "running"}

@app.route(f'/webhook/{BOT_TOKEN}', methods=['POST'])
async def webhook():
    """Handle incoming updates via webhook"""
    if telegram_app:
        update = Update.de_json(request.get_json(), telegram_app.bot)
        await telegram_app.process_update(update)
    return 'OK'

# FPL API функции
def get_current_gameweek():
    """Get current gameweek number"""
    try:
        response = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
        response.raise_for_status()
        data = response.json()
        
        print(f"API Response status: {response.status_code}")
        
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
        response = requests.get(f"https://fantasy.premierleague.com/api/leagues-classic/{LEAGUE_ID}/standings/")
        response.raise_for_status()
        data = response.json()
        return data['standings']['results']
    except Exception as e:
        print(f"Error getting managers: {e}")
        return []

def get_manager_picks(manager_id, gameweek):
    """Get manager's picks for specific gameweek"""
    try:
        response = requests.get(f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{gameweek}/picks/")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error getting picks for manager {manager_id}: {e}")
        return {'picks': []}

def get_live_data(gameweek):
    """Get live points data for gameweek"""
    try:
        response = requests.get(f"https://fantasy.premierleague.com/api/event/{gameweek}/live/")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error getting live data: {e}")
        return {'elements': []}

def get_bootstrap_data():
    """Get player and team data"""
    try:
        response = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error getting bootstrap data: {e}")
        return {'elements': [], 'teams': []}

# Telegram bot команды
async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug command to check API data"""
    try:
        response = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
        data = response.json()
        
        debug_info = "🔍 Debug Info:\n\n"
        debug_info += f"API Status: {response.status_code}\n"
        debug_info += f"Total events: {len(data['events'])}\n\n"
        
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
            await update.message.reply_text("❌ Could not determine current gameweek")
            return
        
        bootstrap_data = get_bootstrap_data()
        players = {p['id']: p for p in bootstrap_data['elements']}
        teams = {t['id']: t['name'] for t in bootstrap_data['teams']}
        
        managers = get_league_managers()
        if not managers:
            await update.message.reply_text("❌ Could not fetch league managers")
            return
        
        live_data = get_live_data(current_gw)
        live_points = {item['id']: item['stats']['total_points'] for item in live_data['elements']}
        
        team_players = {}
        
        for manager in managers:
            manager_name = manager['entry_name']
            manager_id = manager['entry']
            
            picks_data = get_manager_picks(manager_id, current_gw)
            
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
            await update.message.reply_text("❌ No player data found")
            return
        
        message = f"🏆 League {LEAGUE_ID} - Gameweek {current_gw} Points\n\n"
        
        for team_name in sorted(team_players.keys()):
            message += f"⚽ {team_name.upper()}\n"
            
            sorted_players = sorted(team_players[team_name], key=lambda x: x['points'], reverse=True)
            
            for player in sorted_players:
                message += f"• {player['name']} ({player['manager']}) - {player['points']} pts\n"
            
            message += "\n"
        
        if len(message) > 4000:
            messages = []
            current_msg = f"🏆 League {LEAGUE_ID} - Gameweek {current_gw} Points\n\n"
            
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
/debug - Show gameweek debug info

The bot will show all players organized by their real Premier League teams with points and manager names.
    """
    await update.message.reply_text(welcome_text)

def setup_webhook(app_url):
    """Setup webhook for the bot"""
    webhook_url = f"{app_url}/webhook/{BOT_TOKEN}"
    
    # Удаляем webhook
    delete_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
    requests.post(delete_url)
    
    # Устанавливаем новый webhook
    set_webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
    response = requests.post(set_webhook_url, json={'url': webhook_url})
    
    print(f"Webhook setup response: {response.json()}")

def main():
    """Start the bot"""
    global telegram_app
    
    print("Starting FPL Bot with webhook...")
    
    # Создаем приложение
    telegram_app = Application.builder().token(BOT_TOKEN).build()
    
    telegram_app.add_handler(CommandHandler("start", start_command))
    telegram_app.add_handler(CommandHandler("points", points_command))
    telegram_app.add_handler(CommandHandler("debug", debug_command))
    
    # Получаем URL приложения
    app_url = os.environ.get('RENDER_EXTERNAL_URL', 'https://your-app.onrender.com')
    
    # Настраиваем webhook
    setup_webhook(app_url)
    
    print("Bot handlers added")
    print(f"Webhook set to: {app_url}/webhook/{BOT_TOKEN}")
    
    # Запускаем Flask
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

if __name__ == '__main__':
    main()
