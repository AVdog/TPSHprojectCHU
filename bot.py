"""
Telegram-бот для статистики видео.
Принимает запросы на русском языке, возвращает числовые ответы.
"""

import os
from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters
from dotenv import load_dotenv

from database import Database, parse_query

load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
db = Database()


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений."""
    user_message = update.message.text
    query_result = parse_query(user_message)
    query_type = query_result.get("type")

    if query_type == "unknown":
        await update.message.reply_text(
            "Не понял вопрос. Попробуйте спросить по-другому.\n\n"
            "Примеры вопросов:\n"
            "• Сколько всего видео есть в системе?\n"
            "• Сколько видео набрало больше 100000 просмотров?\n"
            "• На сколько просмотров выросли все видео 28 ноября 2025?\n"
            "• Сколько разных видео получали новые просмотры 27 ноября 2025?"
        )
        return

    try:
        if db.conn is None:
            await db.connect()

        result = None

        if query_type == "total_videos":
            result = await db.get_total_videos()
        elif query_type == "videos_by_creator_date":
            result = await db.get_videos_by_creator_and_date(
                query_result["creator_id"],
                query_result["start_date"],
                query_result["end_date"],
            )
        elif query_type == "videos_with_views_threshold":
            result = await db.get_videos_with_views_more_than(query_result["threshold"])
        elif query_type == "total_views_on_date":
            result = await db.get_total_views_on_date(query_result["date"])
        elif query_type == "videos_with_new_views_on_date":
            result = await db.get_videos_with_new_views_on_date(query_result["date"])

        await update.message.reply_text(str(result))

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start."""
    await update.message.reply_text(
        "Привет! Я бот для статистики видео 🎬\n\n"
        "Задайте вопрос о видео на русском языке.\n\n"
        "Примеры:\n"
        "• Сколько всего видео есть в системе?\n"
        "• Сколько видео набрало больше 100000 просмотров?\n"
        "• На сколько просмотров выросли все видео 28 ноября 2025?"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help."""
    await start(update, context)


async def handle_start_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка /start и /help."""
    if update.message.text == "/start":
        await start(update, context)
    elif update.message.text == "/help":
        await help_command(update, context)


def main():
    """Запуск бота."""
    app = Application.builder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.COMMAND, handle_start_help))
    print("Bot is running...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
