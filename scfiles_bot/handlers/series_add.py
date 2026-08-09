"""handlers/series_add.py — the /addseries conversation."""
import asyncio

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

import notify
from auth import admin_only
from utils import esc, bold, code, italic
from api_client import api_post, api_err
from tmdb import tmdb_tv, fmt_tv, poster
from keyboards import yes_no_kb, back_kb, ep_more_kb
from handlers.states import (AS_TMDB, AS_SN, AS_EP, AS_EP360, AS_EP720, AS_EP1080,
                              AS_EP_SUB, AS_EP_MORE)
from handlers.series_common import _ep_save, _parse_subtitles, _series_summary

@admin_only
async def cmd_addseries(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📺 <b>Add Series</b>\n\nEnter <b>TMDB TV Show ID</b>:\n"
        "<i>Find it at themoviedb.org — in the URL</i>", parse_mode=ParseMode.HTML)
    return AS_TMDB

async def as_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if not t.isdigit():
        await update.message.reply_text("❌ Enter a valid <b>numeric</b> TMDB ID.", parse_mode=ParseMode.HTML)
        return AS_TMDB
    info = await tmdb_tv(int(t))
    sid  = ((info.get("name","") or "").lower().replace(" ","-").replace("'","")
            if info else f"series-{t}")
    ctx.user_data["series"] = {"tmdb_id": str(t), "id": sid, "seasons": []}
    ctx.user_data["cur_ep"] = {}
    if info:
        p   = poster(info)
        cap = (fmt_tv(info) + f"\n\n✏️ Suggested ID: {code(sid)}\n\n"
               "Enter <b>Season Number</b> for the first episode:")
        if p: await update.message.reply_photo(p, caption=cap, parse_mode=ParseMode.HTML)
        else: await update.message.reply_text(cap, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            "⚠️ TMDB not found.\nEnter <b>Season Number</b> for the first episode:",
            parse_mode=ParseMode.HTML)
    return AS_SN

# FIX: _ep_save takes the seasons list directly — no ctx coupling
async def as_sn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if not t.isdigit():
        await update.message.reply_text(f"❌ Enter a valid season number, e.g. {code('1')}:", parse_mode=ParseMode.HTML)
        return AS_SN
    sn      = int(t); seasons = ctx.user_data["series"]["seasons"]
    season  = next((s for s in seasons if s["season_number"]==sn), None)
    next_ep = (len(season["episodes"])+1) if season else 1
    ctx.user_data["cur_ep"] = {"sn": sn, "ep": next_ep}
    await update.message.reply_text(
        f"🎞 <b>Season {sn}  ·  Episode {next_ep}</b>\n\n"
        f"Enter <b>episode number</b> or {code('-')} to use {code(str(next_ep))}:",
        parse_mode=ParseMode.HTML)
    return AS_EP

async def as_ep(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if t != "-":
        if not t.isdigit():
            await update.message.reply_text(f"❌ Enter a number or {code('-')}:", parse_mode=ParseMode.HTML)
            return AS_EP
        ctx.user_data["cur_ep"]["ep"] = int(t)
    sn = ctx.user_data["cur_ep"]["sn"]; ep = ctx.user_data["cur_ep"]["ep"]
    await update.message.reply_text(
        f"🎞 <b>S{sn}E{ep}</b> — Enter <b>360p link</b> or {code('-')} to skip:", parse_mode=ParseMode.HTML)
    return AS_EP360

async def as_ep360(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    ctx.user_data["cur_ep"]["360p"] = "" if t=="-" else t
    sn = ctx.user_data["cur_ep"]["sn"]; ep = ctx.user_data["cur_ep"]["ep"]
    await update.message.reply_text(
        f"🎞 <b>S{sn}E{ep}</b> — Enter <b>720p link</b> or {code('-')} to skip:", parse_mode=ParseMode.HTML)
    return AS_EP720

async def as_ep720(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    ctx.user_data["cur_ep"]["720p"] = "" if t=="-" else t
    sn = ctx.user_data["cur_ep"]["sn"]; ep = ctx.user_data["cur_ep"]["ep"]
    await update.message.reply_text(
        f"🎞 <b>S{sn}E{ep}</b> — Enter <b>1080p link</b> or {code('-')} to skip:", parse_mode=ParseMode.HTML)
    return AS_EP1080

async def as_ep1080(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    ctx.user_data["cur_ep"]["1080p"] = "" if t=="-" else t
    sn = ctx.user_data["cur_ep"]["sn"]; ep = ctx.user_data["cur_ep"]["ep"]
    await update.message.reply_text(
        f"💬 <b>S{sn}E{ep}</b> — Enter <b>subtitle link(s)</b>\n"
        f"<i>Format:</i> {code('lang|url')} — one per line, e.g.\n"
        f"{code('en|https://example.com/en.srt')}\n{code('ta|https://example.com/ta.srt')}\n"
        f"Or {code('-')} to skip:", parse_mode=ParseMode.HTML)
    return AS_EP_SUB

async def as_ep_sub(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    subs = _parse_subtitles(update.message.text)
    ep = ctx.user_data["cur_ep"]; sn = ep["sn"]; epn = ep["ep"]
    lnk = {k:v for k,v in [("360p",ep.get("360p","")),("720p",ep.get("720p","")),
                           ("1080p",ep.get("1080p",""))] if v}
    _ep_save(ctx.user_data["series"]["seasons"], sn, epn, lnk, subs)
    seasons   = ctx.user_data["series"]["seasons"]
    total_eps = sum(len(s["episodes"]) for s in seasons)
    ctx.user_data["cur_ep"] = {}
    sub_info  = ", ".join(subs.keys()) if subs else "none"
    await update.message.reply_text(
        f"✅ <b>S{sn}E{epn}</b> saved  [{' · '.join(lnk.keys()) or 'no links'}]  💬 subs: {sub_info}\n"
        f"📊 Total so far: <b>{total_eps}</b> ep(s) across <b>{len(seasons)}</b> season(s)\n\nWhat next?",
        reply_markup=ep_more_kb(), parse_mode=ParseMode.HTML)
    return AS_EP_MORE

async def as_ep_more_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "ep_more":
        await q.edit_message_text("Enter <b>Season Number</b> for next episode:", parse_mode=ParseMode.HTML)
        return AS_SN
    sr      = ctx.user_data["series"]
    summary = _series_summary(sr["seasons"])
    await q.edit_message_text(
        f"✅ <b>Confirm Series</b>\n{'─'*26}\n"
        f"🆔 ID:   {code(sr['id'])}\n📺 TMDB: {code(sr['tmdb_id'])}\n{summary}\n{'─'*26}",
        reply_markup=yes_no_kb("sr_confirm","sr_cancel"), parse_mode=ParseMode.HTML)
    return AS_EP_MORE

async def as_confirm_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "sr_cancel":
        ctx.user_data.clear()
        await q.edit_message_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    sr = ctx.user_data.pop("series", {}); ctx.user_data.clear()
    await q.edit_message_text("⏳ <i>Saving series…</i>", parse_mode=ParseMode.HTML)
    r = await api_post("/api/series", sr)
    if r and r.get("success"):
        await q.edit_message_text(
            f"✅ <b>Series saved!</b>\n📊 Total series: {bold(r['count'])}",
            parse_mode=ParseMode.HTML, reply_markup=back_kb())
        asyncio.create_task(_notify_series(sr))
    else:
        await q.edit_message_text(f"❌ <b>Failed:</b> {code(api_err(r))}", parse_mode=ParseMode.HTML)
    return ConversationHandler.END

async def _notify_series(sr: dict):
    """Fire-and-forget channel notification for a newly added series."""
    tid = str(sr.get("tmdb_id",""))
    info = await tmdb_tv(int(tid)) if tid.isdigit() else None
    payload = dict(sr)
    if info:
        payload["title"]    = info.get("name", sr.get("id"))
        payload["year"]     = (info.get("first_air_date") or "")[:4]
        payload["rating"]   = round(info.get("vote_average", 0), 1)
        payload["overview"] = info.get("overview", "")
    await notify.notify_upload("series", payload, poster(info) if info else None)
