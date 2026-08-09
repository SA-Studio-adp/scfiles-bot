"""handlers/movie_add.py — the /addmovie conversation (TMDB id → extras →
480p/720p/1080p → subtitles → position → confirm) plus the fire-and-forget
channel notification once the movie is saved."""
import asyncio

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

import notify
from auth import admin_only
from utils import esc, bold, code, italic
from api_client import api_post, api_err
from tmdb import tmdb_movie, fmt_movie, poster
from keyboards import yes_no_kb, back_kb
from handlers.states import (AM_TMDB, AM_EXTRA, AM_DL480, AM_DL720, AM_DL1080,
                              AM_SUB, AM_POS, AM_CONFIRM)

@admin_only
async def cmd_addmovie(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 <b>Add Movie</b>  Step 1/7\n\nEnter <b>TMDB Movie ID</b>:\n"
        "<i>Find it at themoviedb.org — in the URL</i>", parse_mode=ParseMode.HTML)
    return AM_TMDB

async def am_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if not t.isdigit():
        await update.message.reply_text("❌ Enter a valid <b>numeric</b> TMDB ID.", parse_mode=ParseMode.HTML)
        return AM_TMDB
    info = await tmdb_movie(int(t))
    ctx.user_data["movie"] = {"tmdb_id": int(t), "downloads": {}, "subtitles": {}}
    if info:
        ctx.user_data["movie"]["id"] = (
            (info.get("title","") or "").lower().replace(" ","-").replace("'",""))
        p   = poster(info)
        cap = (fmt_movie(info) +
               f"\n\n✏️ Suggested ID: {code(ctx.user_data['movie']['id'])}"
               f"\n\n<b>Step 2/7</b> — Enter <b>extras</b>\n"
               f"<i>e.g. PreDVD · Tamil Audio · WEB-DL</i>\nOr send {code('-')} to skip")
        if p: await update.message.reply_photo(p, caption=cap, parse_mode=ParseMode.HTML)
        else: await update.message.reply_text(cap, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            f"⚠️ TMDB not found.\n<b>Step 2/7</b> — Enter extras or {code('-')} to skip:",
            parse_mode=ParseMode.HTML)
    return AM_EXTRA

async def am_extra(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    ctx.user_data["movie"]["extras"] = "" if t=="-" else t
    await update.message.reply_text(
        f"📥 <b>Step 3/7</b> — Enter <b>480p link</b>\nOr {code('-')} to skip:", parse_mode=ParseMode.HTML)
    return AM_DL480

async def am_dl480(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if t != "-": ctx.user_data["movie"]["downloads"]["480"] = t
    await update.message.reply_text(
        f"📥 <b>Step 4/7</b> — Enter <b>720p link</b>\nOr {code('-')} to skip:", parse_mode=ParseMode.HTML)
    return AM_DL720

async def am_dl720(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if t != "-": ctx.user_data["movie"]["downloads"]["720"] = t
    await update.message.reply_text(
        f"📥 <b>Step 5/7</b> — Enter <b>1080p link</b>\nOr {code('-')} to skip:", parse_mode=ParseMode.HTML)
    return AM_DL1080

async def am_dl1080(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if t != "-": ctx.user_data["movie"]["downloads"]["1080"] = t
    await update.message.reply_text(
        f"💬 <b>Step 6/7</b> — Enter <b>subtitle link(s)</b>\n"
        f"<i>Format:</i> {code('lang|url')} <i>— one per line, e.g.</i>\n"
        f"{code('en|https://example.com/en.srt')}\n{code('ta|https://example.com/ta.srt')}\n"
        f"Or {code('-')} to skip:", parse_mode=ParseMode.HTML)
    return AM_SUB

async def am_sub(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    subs = {}
    if t != "-":
        for line in t.splitlines():
            line = line.strip()
            if not line:
                continue
            if "|" in line:
                lang, url = line.split("|", 1)
                lang, url = lang.strip(), url.strip()
            else:
                lang, url = "en", line  # no language given — default to English
            if lang and url:
                subs[lang] = url
    ctx.user_data["movie"]["subtitles"] = subs
    await update.message.reply_text(
        "📌 <b>Step 7/7</b> — Choose position:",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⬆️ Add to Top",    callback_data="pos_top"),
            InlineKeyboardButton("⬇️ Add to Bottom", callback_data="pos_bottom"),
        ]]), parse_mode=ParseMode.HTML)
    return AM_POS

async def am_pos_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    pos = "top" if q.data=="pos_top" else "bottom"
    ctx.user_data["movie"]["pos"] = pos
    m   = ctx.user_data["movie"]
    dls = " · ".join(str(k)+"p" for k in sorted(m.get("downloads",{}).keys())) or "none"
    subs = m.get("subtitles", {})
    sub_info = ", ".join(subs.keys()) if subs else "—"
    await q.edit_message_text(
        f"✅ <b>Confirm Movie</b>\n{'─'*26}\n"
        f"🆔 ID:       {code(m.get('id','?'))}\n🎬 TMDB:     {code(m.get('tmdb_id','?'))}\n"
        f"📝 Extras:   {italic(m.get('extras','') or '—')}\n📥 Links:    {code(dls)}\n"
        f"💬 Subs:     {italic(sub_info)}\n"
        f"📌 Position: {bold(pos)}\n{'─'*26}",
        reply_markup=yes_no_kb("mov_confirm","mov_cancel"), parse_mode=ParseMode.HTML)
    return AM_CONFIRM

async def am_confirm_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "mov_cancel":
        ctx.user_data.clear()
        await q.edit_message_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    m   = ctx.user_data.pop("movie", {}); m.setdefault("subtitles", {})
    pos = m.pop("pos", "bottom")
    await q.edit_message_text("⏳ <i>Adding movie…</i>", parse_mode=ParseMode.HTML)
    r = await api_post("/api/movies", {**m, "position": pos})
    ctx.user_data.clear()
    if r and r.get("success"):
        await q.edit_message_text(
            f"✅ <b>Movie added to {pos}!</b>\n📊 Total: {bold(r['count'])}",
            parse_mode=ParseMode.HTML, reply_markup=back_kb())
        asyncio.create_task(_notify_movie(m))
    else:
        await q.edit_message_text(f"❌ <b>Failed:</b> {code(api_err(r))}", parse_mode=ParseMode.HTML)
    return ConversationHandler.END

async def _notify_movie(m: dict):
    """Fire-and-forget channel notification for a newly added movie."""
    info = await tmdb_movie(int(m["tmdb_id"])) if str(m.get("tmdb_id","")).isdigit() else None
    payload = dict(m)
    if info:
        payload["title"]    = info.get("title", m.get("id"))
        payload["year"]     = (info.get("release_date") or "")[:4]
        payload["rating"]   = round(info.get("vote_average", 0), 1)
        payload["overview"] = info.get("overview", "")
    await notify.notify_upload("movie", payload, poster(info) if info else None)
