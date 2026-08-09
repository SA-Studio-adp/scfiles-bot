"""handlers/collection_add.py — the /addcollection conversation: slug →
name → banner → BGM → repeatable [TMDB id → quality button → dl link] loop
→ confirm, plus the channel notification on save."""
import asyncio

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

import notify
from auth import admin_only
from utils import esc, bold, code, italic
from api_client import api_post, api_err
from tmdb import tmdb_movie, fmt_movie, poster
from keyboards import back_kb

@admin_only
async def cmd_addcollection(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.pop("col", None)
    await update.message.reply_text(
        "🗂 <b>Add Collection</b>  Step 1/4\n\n"
        "Enter the collection <b>Slug ID</b>:\n"
        "<i>e.g. vijay · hiphop-tamizha · marvel-mcu</i>",
        parse_mode=ParseMode.HTML)
    return AC_ID

async def ac_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    slug = update.message.text.strip()
    if not slug:
        await update.message.reply_text("❌ Slug cannot be empty. Try again:", parse_mode=ParseMode.HTML)
        return AC_ID
    ctx.user_data["col"] = {"col_id": slug, "col_name": "", "col_banner": "",
                            "col_bgmusic": "", "movies": []}
    await update.message.reply_text(
        "🗂 <b>Add Collection</b>  Step 2/4\n\nEnter the collection <b>Name</b>:",
        parse_mode=ParseMode.HTML)
    return AC_NAME

async def ac_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["col"]["col_name"] = update.message.text.strip()
    await update.message.reply_text(
        f"🗂 <b>Add Collection</b>  Step 3/4\n\n"
        f"Enter the <b>Banner Image URL</b> or {code('-')} to skip:",
        parse_mode=ParseMode.HTML)
    return AC_BANNER

async def ac_banner(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    ctx.user_data["col"]["col_banner"] = "" if t == "-" else t
    await update.message.reply_text(
        f"🗂 <b>Add Collection</b>  Step 4/4\n\n"
        f"Enter the <b>Background Music URL</b> or {code('-')} to skip:",
        parse_mode=ParseMode.HTML)
    return AC_BGMUSIC

async def ac_bgmusic(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    ctx.user_data["col"]["col_bgmusic"] = "" if t == "-" else t
    cnt = len(ctx.user_data["col"]["movies"])
    await update.message.reply_text(
        f"🎬 <b>Movie #{cnt + 1}</b>\n\n"
        f"Enter the <b>TMDB Movie ID</b> for this movie:\n"
        f"<i>Find it at themoviedb.org — in the URL</i>",
        parse_mode=ParseMode.HTML)
    return AC_MOV_TMDB

# ── Movie TMDB lookup ─────────────────────────────────────────────────────────
async def ac_mov_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if not t.isdigit():
        await update.message.reply_text(
            "❌ Enter a valid <b>numeric</b> TMDB Movie ID:", parse_mode=ParseMode.HTML)
        return AC_MOV_TMDB

    tid = int(t)
    wait = await update.message.reply_text("⏳ <i>Fetching from TMDB…</i>", parse_mode=ParseMode.HTML)
    info = await tmdb_movie(tid)
    await wait.delete()

    # Store pending movie info in user_data
    ctx.user_data["col"]["_pending"] = {
        "tmdb_id": tid,
        "title":   info.get("title", f"Movie {tid}") if info else f"Movie {tid}",
    }

    if info:
        p   = poster(info)
        cap = fmt_movie(info) + f"\n\n🎬 TMDB ID: {code(tid)}\n\n📥 <b>Select quality:</b>"
        kb  = InlineKeyboardMarkup([[
            InlineKeyboardButton("📱 360p",  callback_data="acq_360"),
            InlineKeyboardButton("📺 720p",  callback_data="acq_720"),
            InlineKeyboardButton("🎬 1080p", callback_data="acq_1080"),
        ]])
        if p:
            await update.message.reply_photo(p, caption=cap, parse_mode=ParseMode.HTML, reply_markup=kb)
        else:
            await update.message.reply_text(cap, parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        await update.message.reply_text(
            f"⚠️ TMDB ID {code(tid)} not found — proceeding anyway.\n\n📥 <b>Select quality:</b>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📱 360p",  callback_data="acq_360"),
                InlineKeyboardButton("📺 720p",  callback_data="acq_720"),
                InlineKeyboardButton("🎬 1080p", callback_data="acq_1080"),
            ]]),
            parse_mode=ParseMode.HTML)
    return AC_MOV_QUAL

# ── Quality button handler ────────────────────────────────────────────────────
async def ac_mov_qual_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    quality_map = {"acq_360": "360p", "acq_720": "720p", "acq_1080": "1080p"}
    quality = quality_map.get(q.data, "720p")
    ctx.user_data["col"]["_pending"]["quality"] = quality

    pending = ctx.user_data["col"]["_pending"]
    title   = pending.get("title", "Movie")
    await q.edit_message_caption(
        caption=q.message.caption + f"\n\n✅ Quality: <b>{quality}</b>",
        parse_mode=ParseMode.HTML
    ) if q.message.caption else await q.edit_message_text(
        q.message.text + f"\n\n✅ Quality: <b>{quality}</b>",
        parse_mode=ParseMode.HTML
    )
    await q.message.reply_text(
        f"🔗 <b>{esc(title)}</b>  [{quality}]\n\nEnter the <b>download link</b>:",
        parse_mode=ParseMode.HTML)
    return AC_MOV_DL

# ── Download link entry ───────────────────────────────────────────────────────
async def ac_mov_dl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    dl_url  = update.message.text.strip()
    pending = ctx.user_data["col"].pop("_pending", {})
    title   = pending.get("title", "Movie")
    quality = pending.get("quality", "720p")
    tmdb_id = pending.get("tmdb_id", 0)

    ctx.user_data["col"]["movies"].append({
        "id":       (title.lower().replace(" ", "-").replace("'", "")
                     if title != f"Movie {tmdb_id}" else f"movie-{tmdb_id}"),
        "tmdb_id":  tmdb_id,
        "quality":  quality,
        "download": dl_url,
    })

    cnt = len(ctx.user_data["col"]["movies"])
    await update.message.reply_text(
        f"✅ <b>{esc(title)}</b>  [{quality}] added!\n"
        f"📊 Collection has <b>{cnt}</b> movie(s)\n\nWhat next?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Add Another Movie", callback_data="acm_more"),
            InlineKeyboardButton("✅ Save Collection",   callback_data="acm_done"),
        ]]),
        parse_mode=ParseMode.HTML)
    return AC_MOV_MORE

# ── More / Done button handler ────────────────────────────────────────────────
async def ac_mov_more_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()

    if q.data == "acm_more":
        cnt = len(ctx.user_data["col"]["movies"])
        await q.edit_message_text(
            f"🎬 <b>Movie #{cnt + 1}</b>\n\nEnter the <b>TMDB Movie ID</b>:",
            parse_mode=ParseMode.HTML)
        return AC_MOV_TMDB

    # acm_done — show confirm summary
    col    = ctx.user_data["col"]
    movies = col["movies"]
    lines  = [f"  {i+1}. {esc(mv.get('title', mv['id']))} [{mv['quality']}]"
              for i, mv in enumerate(movies[:10])]
    if len(movies) > 10:
        lines.append(f"  <i>... and {len(movies) - 10} more</i>")

    await q.edit_message_text(
        f"✅ <b>Confirm Collection</b>\n{'─'*28}\n"
        f"🆔 Slug:    {code(col['col_id'])}\n"
        f"📛 Name:    {bold(col['col_name'])}\n"
        f"🖼 Banner:  {italic(col['col_banner'] or '—')}\n"
        f"🎵 BGM:     {italic(col['col_bgmusic'] or '—')}\n"
        f"🎬 Movies:  <b>{len(movies)}</b>\n"
        + "\n".join(lines) +
        f"\n{'─'*28}",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Confirm & Save", callback_data="col_confirm"),
            InlineKeyboardButton("❌ Cancel",          callback_data="col_cancel"),
        ]]),
        parse_mode=ParseMode.HTML)
    return AC_MOV_MORE

async def ac_confirm_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "col_cancel":
        ctx.user_data.clear()
        await q.edit_message_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    col = ctx.user_data.pop("col", {}); ctx.user_data.clear()
    await q.edit_message_text("⏳ <i>Creating collection…</i>", parse_mode=ParseMode.HTML)
    r = await api_post("/api/collections", {
        "id":       col["col_id"],
        "name":     col["col_name"],
        "banner":   col.get("col_banner", ""),
        "bg-music": col.get("col_bgmusic", ""),
        "movies":   col.get("movies", []),
    })
    if r and r.get("success"):
        await q.edit_message_text(
            f"✅ <b>Collection created!</b>\n"
            f"📛 {bold(col['col_name'])}  |  🎬 {len(col.get('movies', []))} movies\n"
            f"📊 Total collections: {bold(r['total'])}",
            parse_mode=ParseMode.HTML, reply_markup=back_kb())
        asyncio.create_task(notify.notify_upload("collection", col))
    else:
        await q.edit_message_text(
            f"❌ <b>Failed:</b> {code(api_err(r))}\n"
            f"<i>If backend was sleeping, try again in 30s.</i>",
            parse_mode=ParseMode.HTML)
    return ConversationHandler.END
