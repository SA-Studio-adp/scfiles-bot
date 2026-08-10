"""handlers/collection_edit.py — the /editcollection conversation: edit
slug/name/banner/BGM, add/remove movies on an existing collection. Renaming
the slug cleans up the old backend entry via DELETE."""
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

from auth import admin_only
from utils import esc, bold, code, italic
from api_client import api_get, api_post, api_delete, api_err
from tmdb import tmdb_movie, fmt_movie, poster
from keyboards import back_kb
from handlers.states import (EC_ID, EC_ACTION, EC_FIELD_VALUE,
                              EC_MOV_TMDB, EC_MOV_QUAL, EC_MOV_DL, EC_DEL_MOV_PICK)

def _ec_summary(col: dict) -> str:
    movies = col.get("movies", [])
    lines  = [f"  {i+1}. {esc(mv.get('id', mv.get('title','?')))} [{mv.get('quality','?')}]"
              for i, mv in enumerate(movies[:10])]
    if len(movies) > 10:
        lines.append(f"  <i>... and {len(movies)-10} more</i>")
    return (
        f"🆔 Slug:   {code(col['col_id'])}\n"
        f"📛 Name:   {bold(col.get('col_name','') or '—')}\n"
        f"🖼 Banner: {italic(col.get('col_banner','') or '—')}\n"
        f"🎵 BGM:    {italic(col.get('col_bgmusic','') or '—')}\n"
        f"🎬 Movies: <b>{len(movies)}</b>\n" + ("\n".join(lines) if lines else "")
    )

def _ec_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆔 Edit Slug ID", callback_data="ecf_col_id"),
         InlineKeyboardButton("📛 Edit Name",    callback_data="ecf_col_name")],
        [InlineKeyboardButton("🖼 Edit Banner",  callback_data="ecf_col_banner"),
         InlineKeyboardButton("🎵 Edit BGM",     callback_data="ecf_col_bgmusic")],
        [InlineKeyboardButton("➕ Add Movie",    callback_data="ec_addmov"),
         InlineKeyboardButton("🗑 Remove Movie", callback_data="ec_delmov")],
        [InlineKeyboardButton("✅ Done / Save",  callback_data="ec_done"),
         InlineKeyboardButton("❌ Cancel",        callback_data="ec_cancel")],
    ])

async def _ec_show_menu_msg(message, ctx: ContextTypes.DEFAULT_TYPE):
    col = ctx.user_data["edit_col"]
    await message.reply_text(
        f"✏️ <b>Editing Collection</b>\n{'─'*26}\n{_ec_summary(col)}\n{'─'*26}\n\nWhat do you want to do?",
        reply_markup=_ec_menu_kb(), parse_mode=ParseMode.HTML)
    return EC_ACTION

async def _ec_show_menu_cb(q, ctx: ContextTypes.DEFAULT_TYPE):
    col = ctx.user_data["edit_col"]
    await q.edit_message_text(
        f"✏️ <b>Editing Collection</b>\n{'─'*26}\n{_ec_summary(col)}\n{'─'*26}\n\nWhat do you want to do?",
        reply_markup=_ec_menu_kb(), parse_mode=ParseMode.HTML)
    return EC_ACTION

@admin_only
async def cmd_editcollection(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cols = await api_get("/api/collections") or {}
    if not cols:
        await update.message.reply_text("📭 No collections found.", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    lines = [f"• {code(k)} → {bold(v.get('name','?'))}  ({len(v.get('movies',[]))} movies)"
             for k, v in list(cols.items())[:20]]
    await update.message.reply_text(
        "✏️ <b>Edit Collection</b>\n\nEnter the <b>Collection Slug ID</b>:\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML)
    return EC_ID

async def ec_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sid  = update.message.text.strip()
    cols = await api_get("/api/collections") or {}
    data = cols.get(sid)
    if not data:
        await update.message.reply_text(f"❌ {code(sid)} not found. Try again:", parse_mode=ParseMode.HTML)
        return EC_ID
    ctx.user_data["edit_col"] = {
        "_orig_id":   sid,
        "col_id":     sid,
        "col_name":   data.get("name",""),
        "col_banner": data.get("banner",""),
        "col_bgmusic":data.get("bg-music",""),
        "movies":     list(data.get("movies", [])),
    }
    return await _ec_show_menu_msg(update.message, ctx)

async def ec_action_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    d = q.data

    if d == "ec_cancel":
        ctx.user_data.clear()
        await q.edit_message_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    if d == "ec_done":
        col      = ctx.user_data.pop("edit_col", {}); ctx.user_data.clear()
        orig_id  = col.pop("_orig_id", col.get("col_id"))
        new_id   = col["col_id"]
        payload  = {"id": new_id, "name": col.get("col_name",""), "banner": col.get("col_banner",""),
                    "bg-music": col.get("col_bgmusic",""), "movies": col.get("movies", [])}
        await q.edit_message_text("⏳ <i>Saving…</i>", parse_mode=ParseMode.HTML)
        r = await api_post("/api/collections", payload)
        if r and r.get("success") and new_id != orig_id:
            # Slug was renamed — clean up the old entry so it doesn't linger as a duplicate
            await api_delete(f"/api/collections/{orig_id}")
        if r and r.get("success"):
            renamed = f"\n🔁 Renamed: {code(orig_id)} → {code(new_id)}" if new_id != orig_id else ""
            await q.edit_message_text(
                f"✅ <b>Collection updated!</b>\n"
                f"📛 {bold(payload['name'])}  |  🎬 {len(payload['movies'])} movies{renamed}",
                parse_mode=ParseMode.HTML, reply_markup=back_kb())
        else:
            await q.edit_message_text(f"❌ <b>Failed:</b> {code(api_err(r))}", parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    if d.startswith("ecf_"):
        field   = d[4:]   # col_id / col_name / col_banner / col_bgmusic
        ctx.user_data["edit_ec_field"] = field
        current = ctx.user_data["edit_col"].get(field, "")
        label   = {"col_id":"Slug ID","col_name":"Name","col_banner":"Banner URL","col_bgmusic":"BGM URL"}[field]
        skip_note = "" if field in ("col_id","col_name") else f"\nOr {code('-')} to clear"
        await q.edit_message_text(
            f"✏️ Editing {bold(label)}\n\nCurrent: {code(current or '—')}\n\nEnter new value:{skip_note}",
            parse_mode=ParseMode.HTML)
        return EC_FIELD_VALUE

    if d == "ec_addmov":
        await q.edit_message_text(
            "🎬 <b>Add Movie to Collection</b>\n\nEnter the <b>TMDB Movie ID</b>:", parse_mode=ParseMode.HTML)
        return EC_MOV_TMDB

    # ec_delmov
    movies = ctx.user_data["edit_col"].get("movies", [])
    if not movies:
        await q.answer("No movies to remove.", show_alert=True)
        return EC_ACTION
    rows = []
    for i, mv in enumerate(movies):
        label = f"{mv.get('id', mv.get('title','?'))[:24]} [{mv.get('quality','?')}]"
        rows.append([InlineKeyboardButton(f"🗑 {label}", callback_data=f"ecdelmov_{i}")])
    rows.append([InlineKeyboardButton("« Back", callback_data="ec_back")])
    await q.edit_message_text("🗑 <b>Select movie to remove:</b>",
                              reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)
    return EC_DEL_MOV_PICK

async def ec_field_value(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    field = ctx.user_data.get("edit_ec_field")
    t     = update.message.text.strip()
    col   = ctx.user_data["edit_col"]
    if field in ("col_id", "col_name") and (not t or t == "-"):
        await update.message.reply_text("❌ This field cannot be empty. Enter a value:", parse_mode=ParseMode.HTML)
        return EC_FIELD_VALUE
    if field in ("col_banner", "col_bgmusic") and t == "-":
        t = ""
    if field == "col_id":
        t = t.lower().replace(" ", "-")
    col[field] = t
    return await _ec_show_menu_msg(update.message, ctx)

async def ec_mov_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if not t.isdigit():
        await update.message.reply_text("❌ Enter a valid <b>numeric</b> TMDB Movie ID:", parse_mode=ParseMode.HTML)
        return EC_MOV_TMDB
    tid  = int(t)
    wait = await update.message.reply_text("⏳ <i>Fetching from TMDB…</i>", parse_mode=ParseMode.HTML)
    info = await tmdb_movie(tid)
    await wait.delete()
    ctx.user_data["edit_col"]["_pending"] = {
        "tmdb_id": tid, "title": info.get("title", f"Movie {tid}") if info else f"Movie {tid}",
    }
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("📱 360p",  callback_data="ecq_360"),
        InlineKeyboardButton("📺 720p",  callback_data="ecq_720"),
        InlineKeyboardButton("🎬 1080p", callback_data="ecq_1080"),
    ]])
    if info:
        p   = poster(info)
        cap = fmt_movie(info) + f"\n\n🎬 TMDB ID: {code(tid)}\n\n📥 <b>Select quality:</b>"
        if p: await update.message.reply_photo(p, caption=cap, parse_mode=ParseMode.HTML, reply_markup=kb)
        else: await update.message.reply_text(cap, parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        await update.message.reply_text(
            f"⚠️ TMDB ID {code(tid)} not found — proceeding anyway.\n\n📥 <b>Select quality:</b>",
            reply_markup=kb, parse_mode=ParseMode.HTML)
    return EC_MOV_QUAL

async def ec_mov_qual_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    quality_map = {"ecq_360": "360p", "ecq_720": "720p", "ecq_1080": "1080p"}
    quality = quality_map.get(q.data, "720p")
    ctx.user_data["edit_col"]["_pending"]["quality"] = quality
    pending = ctx.user_data["edit_col"]["_pending"]
    title   = pending.get("title", "Movie")
    if q.message.caption:
        await q.edit_message_caption(caption=q.message.caption + f"\n\n✅ Quality: <b>{quality}</b>",
                                     parse_mode=ParseMode.HTML)
    else:
        await q.edit_message_text(q.message.text + f"\n\n✅ Quality: <b>{quality}</b>", parse_mode=ParseMode.HTML)
    await q.message.reply_text(
        f"🔗 <b>{esc(title)}</b>  [{quality}]\n\nEnter the <b>download link</b>:", parse_mode=ParseMode.HTML)
    return EC_MOV_DL

async def ec_mov_dl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    dl_url  = update.message.text.strip()
    pending = ctx.user_data["edit_col"].pop("_pending", {})
    title   = pending.get("title", "Movie")
    quality = pending.get("quality", "720p")
    tmdb_id = pending.get("tmdb_id", 0)
    ctx.user_data["edit_col"]["movies"].append({
        "id":       (title.lower().replace(" ", "-").replace("'", "")
                     if title != f"Movie {tmdb_id}" else f"movie-{tmdb_id}"),
        "tmdb_id":  tmdb_id,
        "quality":  quality,
        "download": dl_url,
    })
    await update.message.reply_text(f"✅ <b>{esc(title)}</b> [{quality}] added to collection!", parse_mode=ParseMode.HTML)
    return await _ec_show_menu_msg(update.message, ctx)

async def ec_del_mov_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "ec_back":
        return await _ec_show_menu_cb(q, ctx)
    idx    = int(q.data.split("_", 1)[1])
    movies = ctx.user_data["edit_col"].get("movies", [])
    if 0 <= idx < len(movies):
        removed = movies.pop(idx)
        await q.answer(f"Removed {removed.get('id','movie')}", show_alert=False)
    return await _ec_show_menu_cb(q, ctx)
