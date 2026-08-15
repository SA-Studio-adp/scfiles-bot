"""
broadcast.py — the /broadcast conversation, registered on the NOTIFY bot
(notify_bot.py). Admin-only (reuses the same admin list as the main bot —
config.state.ADMIN_IDS is shared across the whole process regardless of
which bot's Application is asking).

Flow
────
  /broadcast
    → "Send me the message — text, or a photo with a caption"
    → shows a preview + recipient count (channels + tracked users, deduped)
    → [✅ Send] [❌ Cancel]
    → sends to every recipient, skipping/counting failures (blocked bots,
      bot kicked from a group, etc — never lets one failure stop the rest)

Recipients = every channel/group ever registered via /addchannel (any
category, deduplicated) + every user who has ever DMed this bot (tracked
in cmd_start/cmd_uploads — see notify_bot.py).
"""
import asyncio

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (ContextTypes, ConversationHandler, CommandHandler,
                           MessageHandler, CallbackQueryHandler, filters)
from telegram.constants import ParseMode

from config import logger
from auth import is_admin
from utils import bold
import db

BC_CONTENT, BC_CONFIRM = range(2)

# Small delay between sends so a large broadcast doesn't trip Telegram's
# global rate limit (roughly 30 messages/second across all chats).
_SEND_DELAY = 0.05


async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ <b>Access denied.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    await update.message.reply_text(
        "📢 <b>Broadcast</b>\n\n"
        "Send me the message to broadcast — plain text, or a photo with a caption.\n"
        "Sent as HTML formatting. Send /cancel to abort.",
        parse_mode=ParseMode.HTML)
    return BC_CONTENT


async def broadcast_content_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if msg.photo:
        ctx.user_data["broadcast"] = {"photo_file_id": msg.photo[-1].file_id, "caption": msg.caption or ""}
        preview = msg.caption or "<i>(no caption)</i>"
    else:
        text = msg.text or ""
        if not text.strip():
            await update.message.reply_text("❌ Send some text, or a photo with a caption.", parse_mode=ParseMode.HTML)
            return BC_CONTENT
        ctx.user_data["broadcast"] = {"text": text}
        preview = text

    channel_ids = await db.get_all_channel_ids()
    user_ids = await db.get_all_user_ids()
    total = len(set(channel_ids) | {str(u) for u in user_ids})

    await update.message.reply_text(
        f"📢 <b>Confirm Broadcast</b>\n{'─'*26}\n"
        f"{preview}\n{'─'*26}\n"
        f"📡 Channels/groups: {bold(len(channel_ids))}\n"
        f"👤 Users: {bold(len(user_ids))}\n"
        f"📊 Total recipients: {bold(total)}",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Send", callback_data="bc_send"),
            InlineKeyboardButton("❌ Cancel", callback_data="bc_cancel"),
        ]]))
    return BC_CONFIRM


async def broadcast_confirm_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    content = ctx.user_data.pop("broadcast", None)
    if q.data == "bc_cancel" or not content:
        await q.edit_message_text("❌ <b>Broadcast cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    await q.edit_message_text("⏳ <i>Sending broadcast…</i>", parse_mode=ParseMode.HTML)

    channel_ids = await db.get_all_channel_ids()
    user_ids = [str(u) for u in await db.get_all_user_ids()]
    recipients = list(dict.fromkeys(channel_ids + user_ids))  # dedup, keep order

    sent, failed = 0, 0
    for chat_id in recipients:
        try:
            if "photo_file_id" in content:
                await ctx.bot.send_photo(chat_id, content["photo_file_id"],
                                         caption=content.get("caption") or None, parse_mode=ParseMode.HTML)
            else:
                await ctx.bot.send_message(chat_id, content["text"], parse_mode=ParseMode.HTML)
            sent += 1
        except Exception as e:
            failed += 1
            logger.warning("Broadcast failed for %s: %s", chat_id, e)
        await asyncio.sleep(_SEND_DELAY)

    await q.edit_message_text(
        f"✅ <b>Broadcast complete</b>\n📤 Sent: {bold(sent)}\n❌ Failed: {bold(failed)}",
        parse_mode=ParseMode.HTML)
    return ConversationHandler.END


async def cmd_broadcast_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.pop("broadcast", None)
    await update.message.reply_text("❌ <b>Broadcast cancelled.</b>", parse_mode=ParseMode.HTML)
    return ConversationHandler.END


def build_broadcast_conversation() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CommandHandler("broadcast", cmd_broadcast)],
        states={
            BC_CONTENT: [MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, broadcast_content_msg)],
            BC_CONFIRM: [CallbackQueryHandler(broadcast_confirm_cb, pattern="^bc_")],
        },
        fallbacks=[CommandHandler("cancel", cmd_broadcast_cancel)],
        per_message=False,
    )
