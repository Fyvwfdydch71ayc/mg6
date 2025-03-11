import asyncio
import nest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

nest_asyncio.apply()

from telegram import Update, InputMediaVideo
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# Bot configuration
BOT_TOKEN = "7563387302:AAFqlJRb1i2WOeKdru3g2Hk_1jEwAphmhH0"
ADMIN_ID = 6773787379
CHANNEL_VIDEO_ID = -1002413444468      # For merged native videos
CHANNEL_DOCUMENT_ID = -1002439803455   # For video documents sent immediately

# MongoDB configuration
MONGO_URL = ("mongodb+srv://kunalrepowala9:YSz5yjMYV0SaPwtU@cluster0.gxm19.mongodb.net/"
             "?retryWrites=true&w=majority&appName=Cluster0")
mongo_client = AsyncIOMotorClient(MONGO_URL)
db = mongo_client["Cluster0"]
processed_collection = db["processed_media_bot"]

# Global storage and counters
album_storage = {}         # Stores native videos for merging per chat
video_merge_count = 0      # Count of native videos merged successfully
doc_sent_count = 0         # Count of document videos sent directly
failed_merge_count = 0     # Count of merge errors

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

async def flush_album(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> list:
    """
    Flush the native video album for the given chat_id by sending media groups
    in chunks of 10 to the target video channel. A 3-second delay is added between groups.
    Any errors during merging are caught and forwarded to the admin.
    Returns a list of message IDs of successfully sent media groups.
    """
    global video_merge_count, failed_merge_count
    merged_ids = []
    if chat_id in album_storage and album_storage[chat_id].get("groupA"):
        groups = album_storage.pop(chat_id)
        for group in chunks(groups["groupA"], 10):
            try:
                msgs = await context.bot.send_media_group(chat_id=CHANNEL_VIDEO_ID, media=group)
                merged_ids.extend([msg.message_id for msg in msgs])
                video_merge_count += len(group)
            except Exception as e:
                failed_merge_count += len(group)
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"Error merging group of {len(group)} native video(s): {e}"
                )
            await asyncio.sleep(3)
    return merged_ids

async def media_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for incoming messages from the admin.
      - Native video messages (update.message.video) are added for merging.
      - Video documents (update.message.document with a video MIME type) are sent immediately.
    Captions are removed (set to empty) and duplicate media (based on file_id) are ignored using MongoDB.
    """
    if update.effective_user and update.effective_user.id == ADMIN_ID:
        chat_id = update.effective_chat.id
        file_id = None
        media = None
        # Remove captions by setting caption to an empty string.
        caption = ""
        target = None

        # Process native video messages (for merging)
        if update.message.video:
            file_id = update.message.video.file_id
            media = InputMediaVideo(media=file_id, caption=caption)
            target = "native"
        # Process documents: if the document's MIME type indicates a video, send immediately.
        elif update.message.document:
            doc = update.message.document
            if doc.mime_type and doc.mime_type.startswith("video"):
                file_id = doc.file_id
                target = "document"
            else:
                return  # Not a video document, ignore.
        else:
            return  # Not a video or video document, ignore.

        # Check for duplicate processing using MongoDB.
        if await processed_collection.find_one({"file_id": file_id}):
            return
        await processed_collection.insert_one({"file_id": file_id})

        # Process native video: add to album for merging.
        if target == "native":
            if chat_id not in album_storage:
                album_storage[chat_id] = {"groupA": []}
            album_storage[chat_id]["groupA"].append(media)
            if len(album_storage[chat_id]["groupA"]) >= 10:
                await flush_album(chat_id, context)
        # Process video document: send immediately without merging.
        elif target == "document":
            global doc_sent_count
            try:
                await context.bot.send_document(chat_id=CHANNEL_DOCUMENT_ID, document=file_id, caption=caption)
                doc_sent_count += 1
            except Exception as e:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"Error sending document video: {e}"
                )

async def flush_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for /flush command: manually flushes any pending native videos for merging.
    """
    if update.effective_user and update.effective_user.id == ADMIN_ID:
        chat_id = update.effective_chat.id
        if chat_id in album_storage and album_storage[chat_id].get("groupA"):
            merged_ids = await flush_album(chat_id, context)
            await update.message.reply_text(f"Flushed native video group(s) with message IDs: {merged_ids}")
        else:
            await update.message.reply_text("No native videos waiting to be flushed.")

async def status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for /status command: displays the status to the admin.
    Shows:
      • Native videos merged
      • Document videos sent
      • Failed merges
      • Pending native videos for the next merge (e.g., "3/10")
    """
    if update.effective_user and update.effective_user.id == ADMIN_ID:
        chat_id = update.effective_chat.id
        pending = len(album_storage.get(chat_id, {}).get("groupA", []))
        status_text = (
            f"Status:\n"
            f"✅ Native Videos merged: {video_merge_count}\n"
            f"📄 Document Videos sent: {doc_sent_count}\n"
            f"❌ Failed merges: {failed_merge_count}\n"
            f"⏳ Pending native videos for next merge: {pending}/10"
        )
        await update.message.reply_text(status_text)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Simple start command handler."""
    await update.message.reply_text(
        "Bot started.\n"
        "Send native videos for merging (captions removed) and video documents will be sent immediately."
    )

def main():
    application = Application.builder().token(BOT_TOKEN).build()

    # Register handlers.
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("flush", flush_handler))
    application.add_handler(CommandHandler("status", status_handler))
    application.add_handler(MessageHandler(filters.ALL, media_handler))

    application.run_polling()

if __name__ == '__main__':
    main()
