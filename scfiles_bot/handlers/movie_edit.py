"""handlers/movie_edit.py — /editmovie: pick a field via buttons, then send
a new value (backend upserts via POST)."""
import json

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

from auth import admin_only
from utils import esc, bold, code
from api_client import api_get, api_post, api_err
from keyboards import back_kb
from handlers.states import EM_ID, EM_VALUE

@admin_only
async def cmd_editmovie(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✏️ <b>Edit Movie</b>\n\nEnter the movie <b>ID</b>:", parse_mode=ParseMode.HTML)
    return EM_ID

async def em_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    mid    = update.message.text.strip()
    movies = await api_get("/api/movies") or []
    movie  = next((m for m in movies if m["id"]==mid), None)
    if not movie:
        await update.message.reply_text(f"❌ {code(mid)} not found. Try again:", parse_mode=ParseMode.HTML)
        return EM_ID
    ctx.user_data["edit_movie"] = movie
    await update.message.reply_text(
        f"✏️ <b>Editing:</b> {code(mid)}\n\nChoose field to edit:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Extras",    callback_data="ef_extras"),
             InlineKeyboardButton("📥 Downloads", callback_data="ef_downloads")],
            [InlineKeyboardButton("💬 Subtitles", callback_data="ef_subtitles"),
             InlineKeyboardButton("🎬 TMDB ID",   callback_data="ef_tmdb_id")],
            [InlineKeyboardButton("🆔 Slug ID",   callback_data="ef_id")],
        ]), parse_mode=ParseMode.HTML)
    return EM_VALUE

async def em_field_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    field = q.data[3:]
    ctx.user_data["edit_field"] = field
    current = ctx.user_data.get("edit_movie",{}).get(field,"")
    await q.edit_message_text(
        f"✏️ Editing {bold(field)}\n\nCurrent:\n<pre>{esc(json.dumps(current, ensure_ascii=False))}</pre>\n\nEnter new value:",
        parse_mode=ParseMode.HTML)
    return EM_VALUE

async def em_value(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    field = ctx.user_data.get("edit_field")
    if not field:
        await update.message.reply_text("👆 Choose a field using the buttons above.", parse_mode=ParseMode.HTML)
        return EM_VALUE
    movie = ctx.user_data.get("edit_movie",{})
    try:    val = json.loads(update.message.text.strip())
    except: val = update.message.text.strip()
    movie[field] = val
    r = await api_post("/api/movies", movie)   # backend uses POST as upsert
    ctx.user_data.clear()
    if r and r.get("success"):
        await update.message.reply_text(
            f"✅ {code(movie.get('id','?'))} updated!  Field {code(field)} saved.",
            parse_mode=ParseMode.HTML, reply_markup=back_kb())
    else:
        await update.message.reply_text(f"❌ <b>Failed:</b> {code(api_err(r))}", parse_mode=ParseMode.HTML)
    return ConversationHandler.END
