"""
handlers/notify_flow.py — the shared "send a notification for this
upload?" mini-conversation, tacked onto the end of every add/edit flow
(addmovie, addseries, addcollection, editseries-new-episode).

Usage from a calling handler, right after a successful save:

    from handlers.notify_flow import start_notify_flow
    from handlers.states import NOTIFY_ASK

    ... await api_post(...) succeeds ...
    return await start_notify_flow(
        update_or_query, ctx,
        kind="movie",                 # movie | series | episode | collection
        item={"title": ..., "year": ..., "genre": ..., "overview": ..., "id": ...},
        poster_url=poster_url_or_None,
    )   # returns NOTIFY_ASK — register the states below in that conversation

Register these four states (imported from handlers.states) in the calling
ConversationHandler, pointing at the handlers in THIS module:

    NOTIFY_ASK:     [CallbackQueryHandler(notify_ask_cb,     pattern="^ntf_")]
    NOTIFY_CAT:     [CallbackQueryHandler(notify_cat_cb,     pattern="^ntf_")]
    NOTIFY_TITLE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, notify_title_msg)]
    NOTIFY_CONFIRM: [CallbackQueryHandler(notify_confirm_cb, pattern="^ntf_")]
"""
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

import notify
from utils import esc, bold, code, italic
from handlers.states import NOTIFY_ASK, NOTIFY_CAT, NOTIFY_TITLE, NOTIFY_CONFIRM


def _ask_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔔 Yes, notify",  callback_data="ntf_ask_yes"),
        InlineKeyboardButton("🔕 No, skip",     callback_data="ntf_ask_no"),
    ]])

def _cat_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(notify.CATEGORY_LABEL[cat], callback_data=f"ntf_cat_{cat}")
        for cat in notify.CATEGORIES
    ]])

def _confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Send", callback_data="ntf_confirm_yes"),
        InlineKeyboardButton("❌ Cancel", callback_data="ntf_confirm_no"),
    ]])


async def start_notify_flow(update, ctx: ContextTypes.DEFAULT_TYPE, *,
                             kind: str, item: dict, poster_url: str = None):
    """Call this instead of `return ConversationHandler.END` right after a
    successful save. `item` should already be TMDB-enriched (title, year,
    genre, overview) by the caller. Sends the Yes/No prompt and returns the
    NOTIFY_ASK state."""
    ctx.user_data["pending_notify"] = {
        "kind": kind, "item": item, "poster_url": poster_url,
    }
    text = "🔔 <b>Send a notification for this upload?</b>"
    if update.callback_query:
        await update.callback_query.message.reply_text(text, reply_markup=_ask_kb(), parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, reply_markup=_ask_kb(), parse_mode=ParseMode.HTML)
    return NOTIFY_ASK


async def notify_ask_cb(update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "ntf_ask_no":
        ctx.user_data.pop("pending_notify", None)
        await q.edit_message_text("🔕 <b>Skipped notification.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    await q.edit_message_text("📂 <b>Which category should this go to?</b>",
                              reply_markup=_cat_kb(), parse_mode=ParseMode.HTML)
    return NOTIFY_CAT


async def notify_cat_cb(update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    cat = q.data.split("_", 2)[2]
    pending = ctx.user_data.get("pending_notify")
    if not pending:
        await q.edit_message_text("❌ <b>Session expired.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    pending["category"] = cat
    suggested = pending["item"].get("title") or pending["item"].get("id", "")
    await q.edit_message_text(
        f"✏️ <b>Enter a title for the notification</b>\n"
        f"<i>Suggested:</i> {code(suggested)}\n\n"
        f"Send the title text, or send {code('-')} to use the suggested one:",
        parse_mode=ParseMode.HTML)
    return NOTIFY_TITLE


async def notify_title_msg(update, ctx: ContextTypes.DEFAULT_TYPE):
    pending = ctx.user_data.get("pending_notify")
    if not pending:
        await update.message.reply_text("❌ <b>Session expired.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    t = update.message.text.strip()
    title = pending["item"].get("title") or pending["item"].get("id", "") if t == "-" else t
    pending["title"] = title

    cat = pending["category"]
    kind = pending["kind"]
    summary = (
        f"📢 <b>Confirm Notification</b>\n{'─'*26}\n"
        f"📝 Title: {bold(title)}\n"
        f"🏷 Type: {code(kind)}\n"
        f"📂 Category: {bold(notify.CATEGORY_LABEL.get(cat, cat))}\n"
        f"🖼 Poster: {'✅ attached' if pending.get('poster_url') else '⚠️ none (text-only message)'}\n"
        f"{'─'*26}"
    )
    await update.message.reply_text(summary, reply_markup=_confirm_kb(), parse_mode=ParseMode.HTML)
    return NOTIFY_CONFIRM


async def notify_confirm_cb(update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    pending = ctx.user_data.pop("pending_notify", None)
    if q.data == "ntf_confirm_no" or not pending:
        await q.edit_message_text("❌ <b>Notification cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    await q.edit_message_text("⏳ <i>Sending notification…</i>", parse_mode=ParseMode.HTML)
    sent = await notify.notify_upload(
        pending["kind"], pending["item"], pending.get("poster_url"),
        category=pending["category"], title_override=pending.get("title"),
    )
    if sent > 0:
        await q.edit_message_text(f"✅ <b>Notification sent</b> to {bold(sent)} channel(s)/group(s).",
                                  parse_mode=ParseMode.HTML)
    elif sent == 0:
        await q.edit_message_text(
            "⚠️ <b>Channels are registered, but delivery failed for all of them.</b>\n"
            "<i>Check the bot logs — usually a Telegram API rejection (bad HTML in "
            "messages.py, or the notify bot isn't admin in that chat).</i>", parse_mode=ParseMode.HTML)
    else:  # -1: NOTIFY_BOT_TOKEN unset, or no channels registered for this category
        await q.edit_message_text(
            "⚠️ <b>Nothing was sent.</b>\n"
            "<i>Check NOTIFY_BOT_TOKEN is set and channels are registered for this category "
            "(/listchannels).</i>", parse_mode=ParseMode.HTML)
    return ConversationHandler.END
