"""
notify_bot.py — the small, separate python-telegram-bot Application for
NOTIFY_BOT_TOKEN. It ONLY answers /start and /uploads (both wording-
editable in messages.py) — it never handles admin commands, and it's the
bot that actually posts upload notifications (see notify.py).

Runs as an independent polling loop alongside the main admin bot; skipped
entirely if NOTIFY_BOT_TOKEN isn't set.
"""
from datetime import datetime

from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

from config import logger
import notify

try:
    from messages import TEMPLATES as _TEMPLATES, PROMO_LINK as _PROMO_LINK
except ImportError:
    _TEMPLATES, _PROMO_LINK = {}, "https://t.me/"


def _fmt(name: str, **kw) -> str:
    return _TEMPLATES.get(name, "").format(promo_link=_PROMO_LINK, **kw)


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        _fmt("BOT_START"), parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)


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
    entries = notify.load_recent_uploads(10)
    if not entries:
        await update.message.reply_text(_fmt("BOT_UPLOADS_EMPTY"), parse_mode=ParseMode.MARKDOWN_V2)
        return
    lines = [_fmt("BOT_UPLOADS_HEADER", count=len(entries))]
    for e in entries:
        lines.append(_fmt(
            "BOT_UPLOADS_ITEM",
            icon=notify.KIND_ICON.get(e.get("kind", ""), "•"),
            title=notify.md_escape(e.get("title", "?")),
            category_label=notify.CATEGORY_LABEL.get(e.get("category", ""), e.get("category", "")),
            when=notify.md_escape(_relative_time(e.get("ts", ""))),
        ))
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2,
                                    disable_web_page_preview=True)


def build_notify_app(token: str) -> Application:
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("uploads", cmd_uploads))
    return app


async def register_commands(app: Application):
    """Re-registers the notify bot's command list — called on every startup
    so BotFather's command menu always reflects what's actually handled."""
    await app.bot.set_my_commands([
        BotCommand("start",   "About this channel"),
        BotCommand("uploads", "Show the last 10 uploads"),
    ])
    logger.info("Notify-bot commands registered ✅")
