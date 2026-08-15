"""
notify_bot.py — the small, separate python-telegram-bot Application for
NOTIFY_BOT_TOKEN. It ONLY answers /start and /uploads (both wording-
editable in messages.py) — it never handles admin commands, and it's the
bot that actually posts upload notifications (see notify.py).

Runs as an independent polling loop alongside the main admin bot; skipped
entirely if NOTIFY_BOT_TOKEN isn't set.
"""
from datetime import datetime

from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

from config import logger
from utils import esc
import notify
import db
from broadcast import build_broadcast_conversation

try:
    from messages import (TEMPLATES as _TEMPLATES, WEBSITE_LINK as _WEBSITE_LINK,
                          CHANNEL_HANDLE as _CHANNEL_HANDLE, REQUESTS_HANDLES as _REQUESTS_HANDLES)
except ImportError:
    _TEMPLATES, _WEBSITE_LINK = {}, "https://yourwebsite.com"
    _CHANNEL_HANDLE, _REQUESTS_HANDLES = "", ""


def _fmt(name: str, **kw) -> str:
    """Uses notify.safe_format so an unsupplied {placeholder} in a
    hand-edited template renders blank instead of crashing the command
    entirely (see notify.safe_format's docstring)."""
    return notify.safe_format(_TEMPLATES.get(name, ""), **kw)

def _website_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🌐 Visit Website", url=_WEBSITE_LINK)]])


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user:
        await db.add_user(user.id, user.first_name or "")
    # channel_handle/requests_handles/website_link are supplied here so
    # BOT_START can reference them directly if you've customised it to
    # (as-is, the default BOT_START already does) — no separate footer is
    # appended after BOT_START, since that would duplicate this info.
    text = _fmt("BOT_START", channel_handle=esc(_CHANNEL_HANDLE),
                requests_handles=esc(_REQUESTS_HANDLES), website_link=esc(_WEBSITE_LINK))
    await update.message.reply_text(
        text, parse_mode=ParseMode.HTML,
        disable_web_page_preview=True, reply_markup=_website_kb())


def _relative_time(iso_ts: str) -> str:
    try:
        then = datetime.fromisoformat(iso_ts)
    except Exception:
        return "recently"
    delta = datetime.utcnow() - then
    mins = int(delta.total_seconds() // 60)
    if mins < 1:   return "just now"
    if mins < 60:  return f"{mins}m ago"
    hrs = mins // 60
    if hrs < 24:   return f"{hrs}h ago"
    return f"{hrs // 24}d ago"


async def cmd_uploads(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user:
        await db.add_user(user.id, user.first_name or "")
    entries = await notify.load_recent_uploads(10)
    if not entries:
        await update.message.reply_text(_fmt("BOT_UPLOADS_EMPTY"), parse_mode=ParseMode.HTML)
        return
    lines = [_fmt("BOT_UPLOADS_HEADER", count=len(entries))]
    for e in entries:
        lines.append(_fmt(
            "BOT_UPLOADS_ITEM",
            icon=notify.KIND_ICON.get(e.get("kind", ""), "•"),
            title=esc(e.get("title", "?")),
            category_label=notify.CATEGORY_LABEL.get(e.get("category", ""), e.get("category", "")),
            when=esc(_relative_time(e.get("ts", ""))),
        ))
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML,
                                    disable_web_page_preview=True)


def build_notify_app(token: str) -> Application:
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("uploads", cmd_uploads))
    app.add_handler(build_broadcast_conversation())
    return app


async def register_commands(app: Application):
    """Re-registers the notify bot's command list — called on every startup
    so BotFather's command menu always reflects what's actually handled."""
    await app.bot.set_my_commands([
        BotCommand("start",     "About this channel"),
        BotCommand("uploads",   "Show the last 10 uploads"),
        BotCommand("broadcast", "Admin: broadcast to every channel/user"),
    ])
    logger.info("Notify-bot commands registered ✅")
