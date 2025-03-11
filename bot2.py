import logging
import asyncio
import nest_asyncio

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    ContextTypes,
    filters,
)

# Patch the event loop for environments that require nested loops.
nest_asyncio.apply()

# Set up logging.
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Define the target URL prefix.
TARGET_PREFIX = "https://t.me/TeraBoxFastestDLBot?start="

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Processes incoming messages with captions. It extracts caption entities that contain
    URLs starting with TARGET_PREFIX and, if any, sends all matching links in one reply.
    """
    caption = update.message.caption if update.message.caption else None
    # Process only if a caption and caption_entities exist.
    if caption and update.message.caption_entities:
        links_found = []
        for entity in update.message.caption_entities:
            # For 'text_link' type, the URL is provided directly.
            if entity.type == "text_link":
                url = entity.url
                if url.startswith(TARGET_PREFIX):
                    links_found.append(url)
            # For 'url' type, extract the URL substring from the caption.
            elif entity.type == "url":
                start = entity.offset
                end = entity.offset + entity.length
                url = caption[start:end]
                if url.startswith(TARGET_PREFIX):
                    links_found.append(url)
        # If any valid links were found, join them into one message and send.
        if links_found:
            message_text = "\n".join(links_found)
            await update.message.reply_text(message_text)
    # If no caption or no valid links, do nothing.

async def main():
    """
    Builds and runs the bot.
    """
    app = ApplicationBuilder().token("7660007316:AAHis4NuPllVzH-7zsYhXGfgokiBxm_Tml0").build()
    
    # Handle messages that include a caption.
    app.add_handler(MessageHandler(filters.Caption(), handle_message))
    
    # Start the bot.
    await app.run_polling()

if __name__ == '__main__':
    asyncio.run(main())
