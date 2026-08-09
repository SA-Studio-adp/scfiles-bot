"""handlers/tmdb_search.py — the /tmdb search conversation."""
import asyncio

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

from auth import admin_only
from utils import code
from tmdb import tmdb_movie, tmdb_tv, tmdb_search, fmt_movie, fmt_tv, poster
from handlers.states import TQ_TYPE, TQ_QUERY

@admin_only
async def cmd_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔍 <b>TMDB Search</b>\n\nWhat are you looking for?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🎬 Movie",   callback_data="tq_movie"),
            InlineKeyboardButton("📺 TV Show", callback_data="tq_tv"),
        ]]), parse_mode=ParseMode.HTML)
    return TQ_TYPE

async def tq_type_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    ctx.user_data["tmdb_type"] = "tv" if q.data=="tq_tv" else "movie"
    label = "TV Show" if q.data=="tq_tv" else "Movie"
    await q.edit_message_text(f"🔍 <b>TMDB {label}</b>\n\nEnter title to search:", parse_mode=ParseMode.HTML)
    return TQ_QUERY

async def tq_query(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    mtype   = ctx.user_data.pop("tmdb_type","movie")
    results = await tmdb_search(update.message.text.strip(), mtype)
    ctx.user_data.clear()
    if not results:
        await update.message.reply_text("❌ <b>No results found.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    await update.message.reply_text(f"🔍 Found <b>{len(results)}</b> result(s):", parse_mode=ParseMode.HTML)
    getter = tmdb_movie if mtype=="movie" else tmdb_tv
    fulls  = await asyncio.gather(*[getter(r["id"]) for r in results[:3]])
    for full in fulls:
        if not full: continue
        fmt = fmt_movie(full) if mtype=="movie" else fmt_tv(full)
        cap = fmt + f"\n\n🆔 TMDB ID: {code(full['id'])}"
        p   = poster(full)
        if p: await update.message.reply_photo(p, caption=cap, parse_mode=ParseMode.HTML)
        else: await update.message.reply_text(cap, parse_mode=ParseMode.HTML)
    return ConversationHandler.END
