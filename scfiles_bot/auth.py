"""auth.py — admin authorisation: is_admin(), the @admin_only decorator,
and the /addadmin /removeadmin /listadmins commands."""
from functools import wraps

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

from config import state, logger
from config import _ENV_ADMIN_IDS
from utils import code
import db

# ── auth ──────────────────────────────────────────────────────────────────────
def is_admin(uid: int) -> bool:
    # If no admins configured at all, everyone is admin (open mode)
    return not state.ADMIN_IDS or uid in state.ADMIN_IDS

def admin_only(func):
    """Works on both CommandHandler (update.message) and CallbackQueryHandler."""
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or not is_admin(user.id):
            dest = update.message or getattr(update.callback_query, "message", None)
            if dest:
                await dest.reply_text("⛔ <b>Access denied.</b>", parse_mode=ParseMode.HTML)
            return ConversationHandler.END
        return await func(update, ctx)
    return wrapper

# ── admin management commands ─────────────────────────────────────────────────
async def cmd_addadmin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Add a new admin. Only existing admins can use this."""
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text(
            "\u26d4 <b>Access denied.</b>", parse_mode=ParseMode.HTML)
    args = ctx.args
    if not args:
        return await update.message.reply_text(
            "\U0001f464 <b>Add Admin</b>\n\n"
            "Usage: <code>/addadmin &lt;user_id&gt;</code>\n"
            "<i>Tip: forward any message to @userinfobot to find a user ID</i>",
            parse_mode=ParseMode.HTML)
    try:
        new_id = int(args[0].strip())
    except ValueError:
        return await update.message.reply_text(
            "\u274c Invalid user ID \u2014 must be a plain number.",
            parse_mode=ParseMode.HTML)
    if new_id in state.ADMIN_IDS:
        return await update.message.reply_text(
            f"\u2139\ufe0f {code(new_id)} is already an admin.",
            parse_mode=ParseMode.HTML)
    state.ADMIN_IDS.append(new_id)
    await db.add_admin(new_id)
    logger.info("Admin added: %d by %d", new_id, update.effective_user.id)
    await update.message.reply_text(
        f"\u2705 <b>Admin added!</b>\n"
        f"\U0001f464 {code(new_id)}\n"
        f"\U0001f4ca Total admins: <b>{len(state.ADMIN_IDS)}</b>",
        parse_mode=ParseMode.HTML)

async def cmd_removeadmin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Remove an admin. Only existing admins can use this. Cannot remove yourself."""
    caller = update.effective_user.id
    if not is_admin(caller):
        return await update.message.reply_text(
            "\u26d4 <b>Access denied.</b>", parse_mode=ParseMode.HTML)
    args = ctx.args
    if not args:
        return await update.message.reply_text(
            "\U0001f464 <b>Remove Admin</b>\n\n"
            "Usage: <code>/removeadmin &lt;user_id&gt;</code>",
            parse_mode=ParseMode.HTML)
    try:
        rem_id = int(args[0].strip())
    except ValueError:
        return await update.message.reply_text(
            "\u274c Invalid user ID \u2014 must be a plain number.",
            parse_mode=ParseMode.HTML)
    if rem_id == caller:
        return await update.message.reply_text(
            "\u274c You cannot remove yourself from the admin list.",
            parse_mode=ParseMode.HTML)
    if rem_id not in state.ADMIN_IDS:
        return await update.message.reply_text(
            f"\u2139\ufe0f {code(rem_id)} is not in the admin list.",
            parse_mode=ParseMode.HTML)
    state.ADMIN_IDS.remove(rem_id)
    await db.remove_admin(rem_id)
    logger.info("Admin removed: %d by %d", rem_id, caller)
    await update.message.reply_text(
        f"\u2705 <b>Admin removed!</b>\n"
        f"\U0001f464 {code(rem_id)}\n"
        f"\U0001f4ca Total admins: <b>{len(state.ADMIN_IDS)}</b>",
        parse_mode=ParseMode.HTML)

async def cmd_listadmins(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """List all current admins."""
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text(
            "\u26d4 <b>Access denied.</b>", parse_mode=ParseMode.HTML)
    if not state.ADMIN_IDS:
        return await update.message.reply_text(
            "\u2139\ufe0f No admin IDs set \u2014 all users have admin access.",
            parse_mode=ParseMode.HTML)
    env_ids = set(_ENV_ADMIN_IDS)
    lines = []
    for uid in state.ADMIN_IDS:
        tag = " <i>(env)</i>" if uid in env_ids else " <i>(added)</i>"
        lines.append(f"\u2022 {code(uid)}{tag}")
    await update.message.reply_text(
        f"\U0001f464 <b>Admin List</b>  ({len(state.ADMIN_IDS)} total)\n\n"
        + "\n".join(lines)
        + "\n\n<i>Use /addadmin &lt;id&gt; or /removeadmin &lt;id&gt; to manage.</i>",
        parse_mode=ParseMode.HTML)
