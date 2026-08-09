"""errors.py — the global PTB error handler: logs the traceback, DMs every
admin a short summary, and lets the user know something broke."""
from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from config import state, logger
from utils import esc

async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Log every unhandled exception and notify all admins in Telegram."""
    import traceback
    tb = "".join(traceback.format_exception(type(ctx.error), ctx.error,
                                             ctx.error.__traceback__))
    logger.error("Unhandled exception:\n%s", tb)

    # Build a short user-friendly summary
    err_short = str(ctx.error)[:200]
    msg = (
        f"⚠️ <b>Bot Error</b>\n"
        f"<code>{esc(type(ctx.error).__name__)}: {esc(err_short)}</code>\n\n"
        f"Update: <code>{esc(str(update)[:200]) if update else 'N/A'}</code>"
    )

    # Notify every configured admin
    for admin_id in state.ADMIN_IDS:
        try:
            await ctx.bot.send_message(admin_id, msg, parse_mode=ParseMode.HTML)
        except Exception:
            pass  # Never crash the error handler itself

    # If the error happened mid-conversation, try to reply to the user too
    if isinstance(update, Update):
        dest = (update.message or
                (update.callback_query.message if update.callback_query else None))
        if dest:
            try:
                await dest.reply_text(
                    "❌ <b>An error occurred.</b> The admins have been notified.\n"
                    f"<i>If it keeps happening, use /cancel and try again.</i>",
                    parse_mode=ParseMode.HTML)
            except Exception:
                pass
