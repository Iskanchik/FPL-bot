Инструкции по .env и деплою
Создайте файл .env в корне проекта (или задайте переменные окружения на хосте) со следующими переменными:

FPL_BOT_TOKEN — токен вашего Telegram бота (получить через BotFather). ВАЖНО: никогда не коммитите реальный токен в репозиторий.
FPL_LEAGUE_ID — ID лиги Fantasy Premier League, по умолчанию 980121.
PORT — порт для Flask health/readiness endpoint, по умолчанию 5000.
Пример .env (не коммитить в git):

FPL_BOT_TOKEN=123456:ABC-DEF...
FPL_LEAGUE_ID=980121
PORT=5000
Рекомендации:

Сразу после того как вы заметили токен в публичном репозитории — отзовите/пересоздайте его в BotFather.
Для локальной разработки используйте python-dotenv или экспортируйте переменные в окружение:
export FPL_BOT_TOKEN="..."
export FPL_BOT_TOKEN="..."
export PORT=5000
На продакшене задавайте переменные окружения через механизм провайдера (systemd unit, Docker secrets, render.com settings, Heroku config vars и т.д.).
Запуск локально:

Установите зависимости: pip install -r requirements.txt (убедитесь, что в requirements.txt присутствует httpx)

Экспортируйте переменные окружения или создайте .env.

Запустите: python fpl_bot.py

Дополнительно:

Рекомендую перевести все секреты в секретное хранилище (Vault/Secrets manager) при размещении в продакшене.
Добавьте .env в .gitignore.
