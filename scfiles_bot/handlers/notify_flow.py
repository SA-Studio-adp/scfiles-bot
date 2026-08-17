"""
handlers/notify_flow.py - the "send a notification for this upload?"
prompt, decoupled entirely from the upload conversation it follows.

Usage from a calling handler, right after a successful save:

    from handlers.notify_flow import start_notify_flow

    ... await api_post(...) succeeds ...
    return await start_notify_flow(
        update_or_query, ctx,
        kind="movie",                 # movie | series | episode | collection
        item={"title": ..., "year": ..., "genre": ..., "overview": ..., "id": ...},
        poster_url=poster_url_or_None,
    )   # always returns ConversationHandler.END - the calling (upload)
        # conversation ends here, immediately, regardless of which of the
        # three buttons below gets tapped (or none of them, ever).

Three buttons, each fully independent - none of them leaves the calling
conversation "waiting":

  i)   Yes, notify   -> a SEPARATE mini-conversation (own entry point,
                         registered via build_notify_yes_conversation())
                         asks category -> title -> confirm, then sends now.
  ii)  Yes, schedule  -> opens web/schedule_picker.py as a Telegram Web
                         App. That page talks DIRECTLY to our own web
                         server (POST /schedule/submit) - not through
                         Telegram's sendData()/web_app_data message
                         delivery, which is what a Web App button opened
                         from an inline keyboard is not reliably given for.
                         Tapping this button produces no bot-side event at
                         all, so nothing is ever "waiting" for it either.
  iii) No, skip       -> a standalone CallbackQueryHandler (not part of
                         any conversation), just edits the message.

Each prompt gets its own token (notify_tokens.py) instead of using shared
ctx.user_data, so multiple prompts (e.g. from uploading several things in
a row) can be pending at once without one clobbering another.
"""
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

import notify
import notify_tokens
from config import BOT_WEB_URL
from utils import bold, code
from handlers.states import NOTIFY_CAT, NOTIFY_TITLE, NOTIFY_CONFIRM


def _ask_kb(token: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🔔 Yes, notify", callback_data=f"ntf_yes:{token}")],
    ]
    if BOT_WEB_URL and BOT_WEB_URL.startswith("https://"):
        # Web App buttons require a real public HTTPS URL - Telegram
        # rejects the button outright otherwise, so only show it when one
        # is actually configured.
        rows.append([InlineKeyboardButton(
            "🗓 Yes, schedule",
            web_app=WebAppInfo(url=f"{BOT_WEB_URL}/schedule?token={token}"))])
    rows.append([InlineKeyboardButton("🔕 No, skip", callback_data=f"ntf_no:{token}")])
    return InlineKeyboardMarkup(rows)


async def start_notify_flow(update, ctx: ContextTypes.DEFAULT_TYPE, *,
                             kind: str, item: dict, poster_url: str = None):
    """Sends the Yes/Schedule/No prompt and ALWAYS returns
    ConversationHandler.END for the calling (upload) conversation - the
    prompt itself is handled entirely independently (see module docstring)."""
    chat_id = update.effective_chat.id if update.effective_chat else None
    token = notify_tokens.create({"kind": kind, "item": item, "poster_url": poster_url, "chat_id": chat_id})
    text = "🔔 <b>Send a notification for this upload?</b>"
    if update.callback_query:
        await update.callback_query.message.reply_text(text, reply_markup=_ask_kb(token), parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, reply_markup=_ask_kb(token), parse_mode=ParseMode.HTML)
    return ConversationHandler.END


# ── "No, skip" - standalone, not part of any conversation ────────────────
async def notify_no_cb(update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    token = q.data.split(":", 1)[1] if ":" in q.data else ""
    notify_tokens.pop(token)
    await q.edit_message_text("🔕 <b>Skipped notification.</b>", parse_mode=ParseMode.HTML)


# ── "Yes, notify" mini-conversation: entry -> category -> title -> confirm ─
def _cat_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(notify.CATEGORY_LABEL[cat], callback_data=f"ntfc_cat_{cat}")
        for cat in notify.CATEGORIES
    ]])

def _confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Send", callback_data="ntfc_confirm_yes"),
        InlineKeyboardButton("❌ Cancel", callback_data="ntfc_confirm_no"),
    ]])


async def notify_yes_entry(update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    token = q.data.split(":", 1)[1] if ":" in q.data else ""
    pending = notify_tokens.pop(token)
    if not pending:
        await q.edit_message_text("❌ <b>This prompt has expired.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    ctx.user_data["pending_notify"] = pending
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
    if q.data == "ntfc_confirm_no" or not pending:
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


async def notify_yes_cancel(update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.pop("pending_notify", None)
    await update.message.reply_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
    return ConversationHandler.END
