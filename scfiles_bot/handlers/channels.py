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
        try:
            chat_id = int(ctx.args[0].strip())
        except ValueError:
            return await update.message.reply_text("❌ Invalid chat ID.", parse_mode=ParseMode.HTML)
        # We're not in the chat right now — ask the ADMIN bot to look it up.
        # This also doubles as proof the admin bot is actually a member.
        try:
            chat = await ctx.bot.get_chat(chat_id)
            chat_type, chat_title = chat.type, (chat.title or "")
        except Exception as e:
            return await update.message.reply_text(
                f"❌ Couldn't look up {code(chat_id)} — is the admin bot a member of it?\n"
                f"<code>{esc(e)}</code>", parse_mode=ParseMode.HTML)
    else:
        chat_id, chat_type, chat_title = (update.effective_chat.id, update.effective_chat.type,
                                          update.effective_chat.title or "")

    if chat_type == "private":
        return await update.message.reply_text(
            "❌ <b>That's a person's chat, not a channel or group.</b>\n"
            "Run /addchannel inside the target channel/group instead — or in a DM with its "
            "chat ID as an argument, e.g. <code>/addchannel -1001234567890</code>.",
            parse_mode=ParseMode.HTML)

    ctx.user_data["pending_channel"] = {"chat_id": chat_id, "title": chat_title}

    warn = ""
    notify_check = await notify.verify_notify_bot_in_chat(chat_id)
    if notify_check is None:
        warn = ("\n\n⚠️ <i>The notify bot doesn't seem to be in this chat yet — add it too "
                "(as admin), or notifications won't be able to send here.</i>")

    await update.message.reply_text(
        f"📡 <b>Register this {esc(chat_type)}</b>\n🆔 {code(chat_id)}"
        f"{'  ·  ' + esc(chat_title) if chat_title else ''}\n\n"
        f"What kind of uploads should be posted here?{warn}",
        reply_markup=_channel_category_kb(chat_id), parse_mode=ParseMode.HTML)

async def channel_category_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id):
        return await q.answer("⛔ Access denied.", show_alert=True)
    if q.data == "chnl_cancel":
        ctx.user_data.pop("pending_channel", None)
        await q.edit_message_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return
    _, cat, chat_id = q.data.split("_", 2)
    pending = ctx.user_data.pop("pending_channel", {})
    title = pending.get("title") or (q.message.chat.title if q.message and q.message.chat else "")
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
