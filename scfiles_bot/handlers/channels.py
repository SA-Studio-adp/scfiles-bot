"""handlers/channels.py — /addchannel /removechannel /listchannels and the
PreDVD/HD/All category picker callback (v4 channel-notification feature)."""
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

import notify
from auth import is_admin
from utils import esc, code

def _channel_category_kb(chat_id) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(notify.CATEGORY_LABEL[cat], callback_data=f"chnl_{cat}_{chat_id}")
        for cat in notify.CATEGORIES
    ], [InlineKeyboardButton("❌ Cancel", callback_data="chnl_cancel")]])

async def cmd_addchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Run this INSIDE the target channel/group (bot must be an admin there),
    or in a DM with the chat id as an argument: /addchannel -1001234567890"""
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ <b>Access denied.</b>", parse_mode=ParseMode.HTML)
    if ctx.args:
        try: chat_id = int(ctx.args[0].strip())
        except ValueError:
            return await update.message.reply_text("❌ Invalid chat ID.", parse_mode=ParseMode.HTML)
    else:
        chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"📡 <b>Register this channel</b>\n🆔 {code(chat_id)}\n\n"
        f"What kind of uploads should be posted here?",
        reply_markup=_channel_category_kb(chat_id), parse_mode=ParseMode.HTML)

async def channel_category_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id):
        return await q.answer("⛔ Access denied.", show_alert=True)
    if q.data == "chnl_cancel":
        await q.edit_message_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return
    _, cat, chat_id = q.data.split("_", 2)
    title = q.message.chat.title if q.message and q.message.chat else ""
    notify.add_channel(cat, chat_id, title or "")
    await q.edit_message_text(
        f"✅ <b>Registered!</b>\n🆔 {code(chat_id)}\n"
        f"📂 Category: <b>{notify.CATEGORY_LABEL[cat]}</b>\n\n"
        f"<i>Run /addchannel again here to also add it to another category.</i>",
        parse_mode=ParseMode.HTML)

async def cmd_removechannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ <b>Access denied.</b>", parse_mode=ParseMode.HTML)
    args = ctx.args
    if len(args) < 2 or args[0].lower() not in notify.CATEGORIES:
        return await update.message.reply_text(
            "📡 <b>Remove Channel</b>\n\n"
            "Usage: <code>/removechannel &lt;predvd|hd|all&gt; &lt;chat_id&gt;</code>",
            parse_mode=ParseMode.HTML)
    cat, chat_id = args[0].lower(), args[1].strip()
    _, removed = notify.remove_channel(cat, chat_id)
    if removed:
        await update.message.reply_text(
            f"✅ Removed {code(chat_id)} from <b>{notify.CATEGORY_LABEL[cat]}</b>.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            f"ℹ️ {code(chat_id)} wasn't registered under <b>{notify.CATEGORY_LABEL[cat]}</b>.", parse_mode=ParseMode.HTML)

async def cmd_listchannels(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ <b>Access denied.</b>", parse_mode=ParseMode.HTML)
    channels = notify.list_channels()
    if not channels:
        return await update.message.reply_text(
            "📭 No channels registered yet.\n<i>Use /addchannel inside a channel to register it.</i>",
            parse_mode=ParseMode.HTML)
    lines = []
    for cat in notify.CATEGORIES:
        bucket = channels.get(cat, {})
        if not bucket: continue
        lines.append(f"\n{notify.CATEGORY_LABEL[cat]}")
        for cid, title in bucket.items():
            lines.append(f"  • {code(cid)}  {esc(title) if title else ''}")
    await update.message.reply_text(
        "📡 <b>Registered Channels</b>\n" + "\n".join(lines), parse_mode=ParseMode.HTML)
