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
    """Registers a channel/group for notifications. The ADMIN bot never
    needs to be a member anywhere — only the NOTIFY bot does (it's the one
    that actually posts). Two ways to point this at a chat:
      1. /addchannel -1001234567890   (if you already know the chat ID)
      2. Forward any message FROM the target channel/group into this DM —
         no command needed at all, see handle_forwarded_message() below.
    Either way, the target chat is then verified via the NOTIFY bot's own
    getChat — so the notify bot must already be an admin there."""
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ <b>Access denied.</b>", parse_mode=ParseMode.HTML)

    if ctx.args:
        try:
            chat_id = int(ctx.args[0].strip())
        except ValueError:
            return await update.message.reply_text("❌ Invalid chat ID.", parse_mode=ParseMode.HTML)
        return await _register_channel_flow(update, ctx, chat_id)

    fwd_chat = _forwarded_chat(update.message)
    if fwd_chat:
        return await _register_channel_flow(update, ctx, fwd_chat.id, fwd_chat.title or "")

    await update.message.reply_text(
        "📡 <b>Register a channel/group</b>\n\n"
        "The admin bot doesn't need to be added anywhere for this — only the "
        "<b>notify bot</b> does (as admin, in the target chat).\n\n"
        "Then either:\n"
        f"• Send {code('/addchannel -1001234567890')} with its chat ID, or\n"
        "• Forward any message FROM that channel/group here (no command needed, just forward it)",
        parse_mode=ParseMode.HTML)


async def handle_forwarded_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """An admin forwarding any message from a channel/group into this DM —
    with no /addchannel command at all — starts the same registration flow.
    Silently ignored for non-admins and non-forwarded messages."""
    if not update.effective_user or not is_admin(update.effective_user.id):
        return
    fwd_chat = _forwarded_chat(update.message)
    if not fwd_chat:
        return
    await _register_channel_flow(update, ctx, fwd_chat.id, fwd_chat.title or "")


async def _register_channel_flow(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                  chat_id: int, chat_title: str = ""):
    """Shared by cmd_addchannel and handle_forwarded_message — verifies the
    chat via the NOTIFY bot only and shows the category picker."""
    info = await notify.verify_notify_bot_in_chat(chat_id)
    if info is None:
        return await update.message.reply_text(
            f"❌ <b>Couldn't verify {code(chat_id)} via the notify bot.</b>\n"
            "Make sure the <b>notify bot</b> (not this admin bot) has been added there as "
            "admin, then try again.", parse_mode=ParseMode.HTML)

    chat_type = info.get("type")
    chat_title = chat_title or info.get("title", "")
    if chat_type == "private":
        return await update.message.reply_text(
            "❌ <b>That's a person's chat, not a channel or group.</b>",
            parse_mode=ParseMode.HTML)

    ctx.user_data["pending_channel"] = {"chat_id": chat_id, "title": chat_title}
    await update.message.reply_text(
        f"📡 <b>Register this {esc(chat_type)}</b>\n🆔 {code(chat_id)}"
        f"{'  ·  ' + esc(chat_title) if chat_title else ''}\n\n"
        f"What kind of uploads should be posted here?",
        reply_markup=_channel_category_kb(chat_id), parse_mode=ParseMode.HTML)

def _forwarded_chat(message):
    """Returns the source Chat of a forwarded message, or None. Handles
    both the modern MessageOrigin API (Bot API 7.0+) and the legacy
    forward_from_chat field, whichever the running PTB/Bot API version
    populates."""
    fwd_chat = getattr(message, "forward_from_chat", None)
    if fwd_chat:
        return fwd_chat
    origin = getattr(message, "forward_origin", None)
    return getattr(origin, "chat", None) if origin else None

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
    await notify.add_channel(cat, chat_id, title or "")
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
    removed = await notify.remove_channel(cat, chat_id)
    if removed:
        await update.message.reply_text(
            f"✅ Removed {code(chat_id)} from <b>{notify.CATEGORY_LABEL[cat]}</b>.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            f"ℹ️ {code(chat_id)} wasn't registered under <b>{notify.CATEGORY_LABEL[cat]}</b>.", parse_mode=ParseMode.HTML)

async def cmd_listchannels(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ <b>Access denied.</b>", parse_mode=ParseMode.HTML)
    channels = await notify.list_channels()
    if not channels:
        return await update.message.reply_text(
            "📭 No channels registered yet.\n<i>Use /addchannel, or just forward a message "
            "from the target channel/group here.</i>",
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
