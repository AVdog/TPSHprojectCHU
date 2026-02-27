"""
Telegram-бот для статистики видео.
Использует pattern matching для генерации SQL запросов.
Возвращает ОДНО число в ответ.
"""

import os
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters
from telegram.request import HTTPXRequest
from dotenv import load_dotenv

from database import Database, parse_query

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    logger.error("TELEGRAM_TOKEN not found in environment!")
    raise ValueError("TELEGRAM_TOKEN is required")

db = Database()


async def post_init(application: Application) -> None:
    """Инициализация при запуске бота."""
    logger.info("Bot initialized successfully")
    try:
        await db.connect()
        logger.info("Database connection established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")


async def post_shutdown(application: Application) -> None:
    """Очистка при остановке бота."""
    logger.info("Shutting down...")
    await db.close()


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений. Возвращает ОДНО число."""
    user_message = update.message.text
    logger.info(f"Received message from {update.effective_user.id}: {user_message}")

    # Генерируем SQL через patterns
    sql = parse_query(user_message)

    if not sql or sql == "UNKNOWN":
        await update.message.reply_text(
            "Не понял вопрос. Попробуйте спросить по-другому.\n\n"
            "Примеры:\n"
            "• Сколько всего видео?\n"
            "• Какое общее количество лайков?\n"
            "• Сколько видео набрало больше 100000 просмотров?\n"
            "• Сколько видео появилось за май 2025?"
        )
        return

    logger.info(f"Generated SQL: {sql}")

    try:
        # Выполняем SQL и возвращаем число
        result = await db.execute_sql(sql)
        await update.message.reply_text(str(result))
        logger.info(f"Query result: {result}")

    except Exception as e:
        logger.error(f"Error executing query: {e}")
        await update.message.reply_text(f"Ошибка выполнения запроса: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start."""
    logger.info(f"Start command from {update.effective_user.id}")
    await update.message.reply_text(
        "Привет! Я бот для статистики видео 🎬\n\n"
        "Задайте вопрос, я отвечу числом.\n\n"
        "Примеры:\n"
        "• Сколько всего видео?\n"
        "• Какое общее количество лайков?\n"
        "• Сколько видео набрало больше 100000 просмотров?"
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


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка ошибок."""
    logger.error(f"Exception while handling an update: {context.error}")


def main():
    """Запуск бота."""
    # Настройка HTTP запросов с увеличенными таймаутами
    request = HTTPXRequest(
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=30.0,
    )

    app = (
        Application.builder()
        .token(TOKEN)
        .request(request)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # Добавляем обработчики
    app.add_handler(MessageHandler(filters.COMMAND, handle_start_help))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Добавляем обработчик ошибок
    app.add_error_handler(error_handler)

    logger.info("Bot is starting with polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
