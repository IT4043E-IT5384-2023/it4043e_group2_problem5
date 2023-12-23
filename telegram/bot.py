import logging
import telegram
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, Updater, Application

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

class SharedContext:
    def __init__(self):
        self.bot = None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(update.effective_chat.id)
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Your centic.io Alert System")

def send_notification(chat_id: int, message: str, bot: telegram.Bot) -> None:
    bot.send_message(chat_id=chat_id, text=message)

if __name__ == '__main__':
    application = ApplicationBuilder().token('6649302734:AAHRn_UtVt3VbvXIZy8BTU1OJT9wEbE6ZR4').build()
    
    start_handler = CommandHandler('start', start)
    application.add_handler(start_handler)

    bot = telegram.Bot("6649302734:AAHRn_UtVt3VbvXIZy8BTU1OJT9wEbE6ZR4")

    
    application.run_polling()