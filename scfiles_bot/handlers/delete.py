"""handlers/delete.py — /delmovie /delseries /delcollection: each collects
an ID then shows a confirm/cancel button before the actual DELETE call."""
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

from auth import admin_only
from utils import bold, code
from api_client import api_delete, api_err
from handlers.states import DM_ID, DS_ID, DC_ID

@admin_only
async def cmd_delmovie(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗑 <b>Delete Movie</b>\n\nEnter the movie <b>ID</b>:", parse_mode=ParseMode.HTML)
    return DM_ID

async def dm_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    mid = update.message.text.strip()
    await update.message.reply_text(
        f"⚠️ <b>Confirm delete</b>\n\nDelete movie {code(mid)}?\n<i>This cannot be undone.</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes, delete", callback_data=f"delconfirm_movie_{mid}"),
            InlineKeyboardButton("❌ Cancel",       callback_data="delcancel"),
        ]]), parse_mode=ParseMode.HTML)
    return ConversationHandler.END

@admin_only
async def cmd_delseries(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗑 <b>Delete Series</b>\n\nEnter the series <b>ID</b>:", parse_mode=ParseMode.HTML)
    return DS_ID

async def ds_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sid = update.message.text.strip()
    await update.message.reply_text(
        f"⚠️ <b>Confirm delete</b>\n\nDelete series {code(sid)}?\n<i>This cannot be undone.</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes, delete", callback_data=f"delconfirm_series_{sid}"),
            InlineKeyboardButton("❌ Cancel",       callback_data="delcancel"),
        ]]), parse_mode=ParseMode.HTML)
    return ConversationHandler.END

@admin_only
async def cmd_delcollection(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗑 <b>Delete Collection</b>\n\nEnter the collection <b>ID</b>:", parse_mode=ParseMode.HTML)
    return DC_ID

async def dc_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.message.text.strip()
    await update.message.reply_text(
        f"⚠️ <b>Confirm delete</b>\n\nDelete collection {code(cid)}?\n<i>This cannot be undone.</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes, delete", callback_data=f"delconfirm_col_{cid}"),
            InlineKeyboardButton("❌ Cancel",       callback_data="delcancel"),
        ]]), parse_mode=ParseMode.HTML)
    return ConversationHandler.END

async def del_confirm_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    _, dtype, did = q.data.split("_", 2)
    await q.edit_message_text("⏳ <i>Deleting…</i>", parse_mode=ParseMode.HTML)
    if dtype == "movie":
        r = await api_delete(f"/api/movies/{did}")
        if r and r.get("success"):
            await q.edit_message_text(f"✅ Movie {code(did)} deleted.  Remaining: {bold(r['count'])}", parse_mode=ParseMode.HTML)
        else:
            await q.edit_message_text(f"❌ {code(api_err(r))}", parse_mode=ParseMode.HTML)
    elif dtype == "series":
        r = await api_delete(f"/api/series/{did}")
        if r and r.get("success"):
            await q.edit_message_text(f"✅ Series {code(did)} deleted.  Remaining: {bold(r['count'])}", parse_mode=ParseMode.HTML)
        else:
            await q.edit_message_text(f"❌ {code(api_err(r))}", parse_mode=ParseMode.HTML)
    elif dtype == "col":
        r = await api_delete(f"/api/collections/{did}")
        if r and r.get("success"):
            await q.edit_message_text(f"✅ Collection {code(did)} deleted.  Total: {bold(r['total'])}", parse_mode=ParseMode.HTML)
        else:
            await q.edit_message_text(f"❌ {code(api_err(r))}", parse_mode=ParseMode.HTML)

async def del_cancel_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("Cancelled")
    await update.callback_query.edit_message_text("❌ <b>Deletion cancelled.</b>", parse_mode=ParseMode.HTML)
