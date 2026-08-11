"""handlers/series_edit.py — the /editseries conversation: add episode
(via a season-button picker), edit an existing episode's links/subtitles
in place, or remove an episode."""
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

from auth import admin_only
from utils import esc, bold, code, italic
from api_client import api_get, api_post, api_err
from tmdb import tmdb_tv, poster
from keyboards import yes_no_kb, back_kb, ep_more_kb
from handlers.states import (ESS_ID, ESS_ACTION, ESS_SN, ESS_EP, ESS_EP360, ESS_EP720,
                              ESS_EP1080, ESS_EP_SUB, ESS_EP_MORE, ESS_DEL_EP,
                              ESS_SN_PICK, ESS_EDIT_PICK)
from handlers.series_common import (_ep_save, _parse_subtitles, _fmt_subtitles,
                                     _series_summary, _link_input, _q_prompt,
                                     _season_picker_kb, _session_upload_summary)
from handlers.notify_flow import start_notify_flow

@admin_only
async def cmd_editseries(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    items = await api_get("/api/series") or []
    if not items:
        await update.message.reply_text("📭 No series found.", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    lines = [f"• {code(s.get('id','?'))}  [{len(s.get('seasons',[]))}S]" for s in items[:20]]
    await update.message.reply_text(
        "✏️ <b>Edit Series</b>\n\nEnter the <b>Series ID</b>:\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML)
    return ESS_ID

async def ess_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sid   = update.message.text.strip()
    items = await api_get("/api/series") or []
    sr    = next((s for s in items if s["id"]==sid), None)
    if not sr:
        await update.message.reply_text(f"❌ {code(sid)} not found. Try again:", parse_mode=ParseMode.HTML)
        return ESS_ID
    ctx.user_data["edit_series"] = sr
    ctx.user_data["cur_ep"]      = {}
    # Snapshot of season numbers that existed BEFORE this edit session, plus
    # a running log of every (season, episode) touched this session — used
    # to build the "S1 . EP 13-16 has been uploaded" / "new season" notify
    # text once the session is saved (see ess_ep_more_cb).
    ctx.user_data["orig_season_numbers"] = {s["season_number"] for s in sr.get("seasons", [])}
    ctx.user_data["session_episodes"] = []
    await update.message.reply_text(
        f"✏️ <b>Editing:</b> {code(sid)}\n{_series_summary(sr.get('seasons',[]))}\n\nWhat do you want to do?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Episode",    callback_data="ess_add"),
             InlineKeyboardButton("✏️ Edit Episode",   callback_data="ess_edit")],
            [InlineKeyboardButton("🗑 Remove Episode", callback_data="ess_del"),
             InlineKeyboardButton("❌ Cancel",          callback_data="ess_cancel")],
        ]), parse_mode=ParseMode.HTML)
    return ESS_ACTION

async def ess_action_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "ess_cancel":
        ctx.user_data.clear()
        await q.edit_message_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    sr = ctx.user_data["edit_series"]
    if q.data == "ess_add":
        await q.edit_message_text(
            "🎬 <b>Add Episode</b>\n\nChoose a season, or add a new one:",
            reply_markup=_season_picker_kb(sr.get("seasons", [])), parse_mode=ParseMode.HTML)
        return ESS_SN_PICK
    if q.data == "ess_edit":
        rows = []
        for s in sr.get("seasons",[]):
            row = [InlineKeyboardButton(
                f"S{s['season_number']}E{ep['ep_number']}",
                callback_data=f"essedit_{s['season_number']}_{ep['ep_number']}")
                   for ep in s["episodes"]]
            for i in range(0, len(row), 4): rows.append(row[i:i+4])
        if not rows:
            await q.answer("This series has no episodes yet.", show_alert=True)
            return ESS_ACTION
        rows.append([InlineKeyboardButton("❌ Cancel", callback_data="ess_cancel")])
        await q.edit_message_text("✏️ <b>Select episode to edit:</b>",
                                  reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)
        return ESS_EDIT_PICK
    # ess_del — fallback branch
    rows = []
    for s in sr.get("seasons",[]):
        row = [InlineKeyboardButton(
            f"S{s['season_number']}E{ep['ep_number']}",
            callback_data=f"ess_delep_{s['season_number']}_{ep['ep_number']}")
               for ep in s["episodes"]]
        for i in range(0, len(row), 4): rows.append(row[i:i+4])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="ess_cancel")])
    await q.edit_message_text("🗑 <b>Select episode to remove:</b>",
                              reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)
    return ESS_DEL_EP

async def ess_del_ep_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "ess_cancel":
        ctx.user_data.clear()
        await q.edit_message_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    parts = q.data.split("_")   # ess_delep_SN_EP
    sn = int(parts[2]); epn = int(parts[3])
    sr = ctx.user_data["edit_series"]; removed = False
    for s in sr.get("seasons",[]):
        if s["season_number"] == sn:
            before = len(s["episodes"])
            s["episodes"] = [e for e in s["episodes"] if e["ep_number"]!=epn]
            removed = len(s["episodes"]) < before
    sr["seasons"] = [s for s in sr["seasons"] if s["episodes"]]
    await q.edit_message_text("⏳ <i>Saving…</i>", parse_mode=ParseMode.HTML)
    r = await api_post("/api/series", sr)   # backend uses POST as upsert
    ctx.user_data.clear()
    if r and r.get("success") and removed:
        await q.edit_message_text(
            f"✅ <b>S{sn}E{epn} removed!</b>\n📊 Total series: {bold(r['count'])}",
            parse_mode=ParseMode.HTML, reply_markup=back_kb())
    else:
        await q.edit_message_text(
            f"❌ Episode not found or save failed.\n{code(api_err(r))}", parse_mode=ParseMode.HTML)
    return ConversationHandler.END

async def ess_edit_pick_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """User picked an existing episode to edit — preload its current links and
    jump straight into the quality-entry chain (skips season/ep-number prompts)."""
    q = update.callback_query; await q.answer()
    if q.data == "ess_cancel":
        ctx.user_data.clear()
        await q.edit_message_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    parts = q.data.split("_")   # essedit_SN_EP
    sn = int(parts[1]); epn = int(parts[2])
    sr = ctx.user_data["edit_series"]
    season = next((s for s in sr.get("seasons",[]) if s["season_number"]==sn), None)
    ep = next((e for e in (season["episodes"] if season else []) if e["ep_number"]==epn), None)
    if not ep:
        await q.edit_message_text("❌ Episode not found.", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    links = ep.get("links", {})
    ctx.user_data["cur_ep"] = {
        "sn": sn, "ep": epn,
        "360p": links.get("360p",""), "720p": links.get("720p",""), "1080p": links.get("1080p",""),
        "subtitles": ep.get("subtitles", {}),   # pre-load existing subtitles for edit
    }
    await q.edit_message_text(
        f"✏️ <b>Editing S{sn}E{epn}</b>\n" + _q_prompt("360p", links.get("360p","")),
        parse_mode=ParseMode.HTML)
    return ESS_EP360

async def ess_sn_pick_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "ess_cancel":
        ctx.user_data.clear()
        await q.edit_message_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    if q.data == "esssn_new":
        await q.edit_message_text("Enter the <b>new season number</b>:", parse_mode=ParseMode.HTML)
        return ESS_SN
    sn = int(q.data.split("_", 1)[1])
    sr = ctx.user_data["edit_series"]
    season  = next((s for s in sr.get("seasons",[]) if s["season_number"]==sn), None)
    next_ep = (len(season["episodes"])+1) if season else 1
    ctx.user_data["cur_ep"] = {"sn": sn, "ep": next_ep}
    await q.edit_message_text(
        f"🎞 <b>S{sn}  ·  Episode {next_ep}</b>\n\nEnter episode number or {code('-')} to use {code(str(next_ep))}:",
        parse_mode=ParseMode.HTML)
    return ESS_EP

async def ess_sn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Only reached via the '➕ New Season' button — free-text entry for a season
    number that doesn't exist yet."""
    t = update.message.text.strip()
    if not t.isdigit():
        await update.message.reply_text("❌ Enter a valid season number:", parse_mode=ParseMode.HTML)
        return ESS_SN
    sn      = int(t); sr = ctx.user_data["edit_series"]
    season  = next((s for s in sr.get("seasons",[]) if s["season_number"]==sn), None)
    next_ep = (len(season["episodes"])+1) if season else 1
    ctx.user_data["cur_ep"] = {"sn": sn, "ep": next_ep}
    await update.message.reply_text(
        f"🎞 <b>S{sn}E{next_ep}</b>\nEnter episode number or {code('-')} to use {code(str(next_ep))}:",
        parse_mode=ParseMode.HTML)
    return ESS_EP

async def ess_ep(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if t != "-":
        if not t.isdigit():
            await update.message.reply_text(f"❌ Enter a number or {code('-')}:", parse_mode=ParseMode.HTML)
            return ESS_EP
        ctx.user_data["cur_ep"]["ep"] = int(t)
    cur = ctx.user_data["cur_ep"]; sn, ep = cur["sn"], cur["ep"]
    await update.message.reply_text(
        f"🎞 <b>S{sn}E{ep}</b>\n" + _q_prompt("360p", cur.get("360p","")), parse_mode=ParseMode.HTML)
    return ESS_EP360

async def ess_ep360(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cur = ctx.user_data["cur_ep"]
    cur["360p"] = _link_input(update.message.text, cur.get("360p",""))
    sn, ep = cur["sn"], cur["ep"]
    await update.message.reply_text(
        f"🎞 <b>S{sn}E{ep}</b>\n" + _q_prompt("720p", cur.get("720p","")), parse_mode=ParseMode.HTML)
    return ESS_EP720

async def ess_ep720(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cur = ctx.user_data["cur_ep"]
    cur["720p"] = _link_input(update.message.text, cur.get("720p",""))
    sn, ep = cur["sn"], cur["ep"]
    await update.message.reply_text(
        f"🎞 <b>S{sn}E{ep}</b>\n" + _q_prompt("1080p", cur.get("1080p","")), parse_mode=ParseMode.HTML)
    return ESS_EP1080

async def ess_ep1080(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cur = ctx.user_data["cur_ep"]
    cur["1080p"] = _link_input(update.message.text, cur.get("1080p",""))
    sn, ep = cur["sn"], cur["ep"]
    existing_subs = cur.get("subtitles", {})
    cur_sub_hint  = _fmt_subtitles(existing_subs)
    await update.message.reply_text(
        f"💬 <b>S{sn}E{ep}</b> — Enter <b>subtitle link(s)</b>\n"
        f"<i>Format:</i> {code('lang|url')} — one per line\n"
        f"<i>Current:</i> {cur_sub_hint}\n"
        f"Or {code('-')} to keep existing, {code('clear')} to remove all:",
        parse_mode=ParseMode.HTML)
    return ESS_EP_SUB

async def ess_ep_sub(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t   = update.message.text.strip()
    cur = ctx.user_data["cur_ep"]
    if t.lower() in ("clear", "remove", "none"):
        subs = {}
    elif t == "-":
        subs = cur.get("subtitles", {})   # keep unchanged
    else:
        subs = _parse_subtitles(t)
    sn, epn = cur["sn"], cur["ep"]
    lnk = {k:v for k,v in [("360p",cur.get("360p","")),("720p",cur.get("720p","")),
                           ("1080p",cur.get("1080p",""))] if v}
    seasons = ctx.user_data["edit_series"].setdefault("seasons",[])
    _ep_save(seasons, sn, epn, lnk, subs)
    ctx.user_data.setdefault("session_episodes", []).append((sn, epn))
    total_eps = sum(len(s["episodes"]) for s in seasons)
    ctx.user_data["cur_ep"] = {}
    sub_info  = ", ".join(subs.keys()) if subs else "none"
    await update.message.reply_text(
        f"✅ <b>S{sn}E{epn}</b> ready  [{' · '.join(lnk.keys()) or 'no links'}]  💬 subs: {sub_info}\n"
        f"📊 Total: <b>{total_eps}</b> ep(s)\n\nWhat next?",
        reply_markup=ep_more_kb(), parse_mode=ParseMode.HTML)
    return ESS_EP_MORE

async def ess_ep_more_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "ep_more":
        sr = ctx.user_data["edit_series"]
        await q.edit_message_text(
            "Choose a season for the next episode:",
            reply_markup=_season_picker_kb(sr.get("seasons", [])), parse_mode=ParseMode.HTML)
        return ESS_SN_PICK
    sr        = ctx.user_data["edit_series"]; seasons = sr["seasons"]
    total_eps = sum(len(s["episodes"]) for s in seasons)
    summary   = "\n".join(f"  S{s['season_number']}: {len(s['episodes'])} ep(s)" for s in seasons)
    session_episodes     = ctx.user_data.get("session_episodes", [])
    orig_season_numbers  = ctx.user_data.get("orig_season_numbers", set())
    await q.edit_message_text("⏳ <i>Saving…</i>", parse_mode=ParseMode.HTML)
    r = await api_post("/api/series", sr)   # backend uses POST as upsert
    ctx.user_data.clear()
    if not (r and r.get("success")):
        await q.edit_message_text(f"❌ <b>Failed:</b> {code(api_err(r))}", parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    await q.edit_message_text(
        f"✅ <b>Series updated!</b>\n{summary}\n📊 Total episodes: <b>{total_eps}</b>",
        parse_mode=ParseMode.HTML, reply_markup=back_kb())

    tid = str(sr.get("tmdb_id", ""))
    info = await tmdb_tv(int(tid)) if tid.isdigit() else None
    item = dict(sr)
    poster_url = None
    if info:
        item["title"]    = info.get("name", sr.get("id"))
        item["year"]     = (info.get("first_air_date") or "")[:4]
        item["genres"]   = info.get("genres", [])
        item["overview"] = info.get("overview", "")
        poster_url = poster(info)
    episode_line, is_new_season = _session_upload_summary(session_episodes, orig_season_numbers)
    item["episode_line"] = episode_line or summary.strip().replace("\n", " · ")
    item["event_label"]  = "New Season Added" if is_new_season else "New Episode(s) Added"
    return await start_notify_flow(update, ctx, kind="episode", item=item, poster_url=poster_url)
