async def run_bot():
    global bot_application, bot_loop, http_client, bot_running
    logger.info('Starting bot...')
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=50)
    http_client = httpx.AsyncClient(limits=limits, timeout=10.0)
    bot_application = Application.builder().token(BOT_TOKEN).build()
    bot_application.add_handler(CommandHandler('start', start_command))
    bot_application.add_handler(CommandHandler('points', points_command))

    await bot_application.initialize()
    await bot_application.start()

    bot_loop = asyncio.get_running_loop()

    # debug: confirm bot is reachable
    try:
        me = await bot_application.bot.get_me()
        logger.info('Bot started as @%s (id=%s)', getattr(me, 'username', 'unknown'), getattr(me, 'id', 'unknown'))
    except Exception:
        logger.exception('Failed to get_me')

    # If webhook mode configured -> register webhook; otherwise start polling
    if USE_WEBHOOK:
        await _register_webhook_if_needed()
    else:
        try:
            # Start long polling so updates from Telegram are received
            await bot_application.updater.start_polling()
            logger.info('Polling started')
        except Exception:
            logger.exception('Failed to start polling')

    logger.info('Bot started')
    try:
        await stop_event.wait()
    finally:
        logger.info('Shutdown initiated')
        bot_running = False

        # Stop polling if used
        if not USE_WEBHOOK:
            try:
                await bot_application.updater.stop()
                logger.info('Updater stopped')
            except Exception:
                logger.exception('Error stopping updater')

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
            logger.exception('Error closing http client')
