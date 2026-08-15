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

    NOTIFY_ASK:     [CallbackQueryHandler(notify_ask_cb,     pattern="^ntf_"),
                      MessageHandler(filters.StatusUpdate.WEB_APP_DATA, notify_schedule_webapp_data)]
    NOTIFY_CAT:     [CallbackQueryHandler(notify_cat_cb,     pattern="^ntf_")]
    NOTIFY_TITLE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, notify_title_msg)]
    NOTIFY_CONFIRM: [CallbackQueryHandler(notify_confirm_cb, pattern="^ntf_")]

The middle button in the initial Yes/No prompt ("🗓 Schedule Notification")
opens BOT_WEB_URL/schedule as a Telegram Web App — a date/time picker (IST)
that sends its pick back as a web_app_data message and closes itself. That
message is caught by notify_schedule_webapp_data (registered in the SAME
NOTIFY_ASK state, since a Web App button doesn't produce a callback_data
event on click — only once the picker sends data back does the bot hear
anything), which stores pending["scheduled_at"] and continues into the
normal category/title/confirm flow. At NOTIFY_CONFIRM, a pending
scheduled_at means the notification is stored in MongoDB instead of sent
immediately — see scheduler.py's job_send_scheduled_notifications.
"""
from datetime import datetime
import json

import pytz
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

import notify
import db
from config import BOT_WEB_URL, IST
from utils import esc, bold, code, italic
from handlers.states import NOTIFY_ASK, NOTIFY_CAT, NOTIFY_TITLE, NOTIFY_CONFIRM


def _ask_kb() -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton("🔔 Yes, notify",  callback_data="ntf_ask_yes"),
        InlineKeyboardButton("🔕 No, skip",     callback_data="ntf_ask_no"),
    ]]
    if BOT_WEB_URL and BOT_WEB_URL.startswith("https://"):
        # Web App buttons require a real public HTTPS URL — Telegram
        # rejects the button outright otherwise, so only show it when one
        # is actually configured.
        rows.insert(1, [InlineKeyboardButton(
            "🗓 Schedule Notification",
            web_app=WebAppInfo(url=f"{BOT_WEB_URL}/schedule"))])
    return InlineKeyboardMarkup(rows)

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


async def notify_schedule_webapp_data(update, ctx: ContextTypes.DEFAULT_TYPE):
    """Fired when the schedule-picker Web App (web/schedule_picker.py)
    calls Telegram.WebApp.sendData() and closes itself — arrives as a
    normal message with a web_app_data payload, NOT a callback query."""
    pending = ctx.user_data.get("pending_notify")
    if not pending:
        await update.message.reply_text("❌ <b>Session expired.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    try:
        data = json.loads(update.message.web_app_data.data)
        picked = data["scheduled_at_ist"]              # "YYYY-MM-DDTHH:MM", IST wall-clock
        naive_ist = datetime.strptime(picked, "%Y-%m-%dT%H:%M")
        scheduled_ist = IST.localize(naive_ist)
    except Exception:
        await update.message.reply_text(
            "❌ <b>Couldn't read the picked time.</b> Try again.", parse_mode=ParseMode.HTML)
        return NOTIFY_ASK
    # Store as naive UTC ISO (no offset suffix) — same convention db.py
    # already uses for uploads' `ts`, so plain string comparison in
    # MongoDB ($lte) sorts correctly without any offset-mismatch risk.
    scheduled_utc = scheduled_ist.astimezone(pytz.utc).replace(tzinfo=None)
    pending["scheduled_at"] = scheduled_utc.isoformat()
    pending["scheduled_at_display"] = scheduled_ist.strftime("%d %b %Y, %H:%M IST")
    await update.message.reply_text(
        f"🗓 Scheduled for {bold(pending['scheduled_at_display'])}\n\n"
        f"📂 <b>Which category should this go to?</b>",
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
    schedule_line = (f"🗓 Scheduled: {bold(pending['scheduled_at_display'])}\n"
                     if pending.get("scheduled_at") else "")
    summary = (
        f"📢 <b>Confirm Notification</b>\n{'─'*26}\n"
        f"📝 Title: {bold(title)}\n"
        f"🏷 Type: {code(kind)}\n"
        f"📂 Category: {bold(notify.CATEGORY_LABEL.get(cat, cat))}\n"
        f"{schedule_line}"
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

    if pending.get("scheduled_at"):
        await db.add_scheduled_notification({
            "kind": pending["kind"], "item": pending["item"], "poster_url": pending.get("poster_url"),
            "category": pending["category"], "title": pending.get("title"),
            "scheduled_at": pending["scheduled_at"],
            "created_by": update.effective_user.id if update.effective_user else None,
        })
        await q.edit_message_text(
            f"🗓 <b>Notification scheduled</b> for {bold(pending['scheduled_at_display'])}.",
            parse_mode=ParseMode.HTML)
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
