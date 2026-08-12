"""handlers/basic.py — /start /help /cancel /status /stats /movies /series
/collections /logs /backup /backupzip /setbackup."""
import io, os
from datetime import datetime

import aiohttp
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode

from config import state, IST, LOG_FILE, BACKEND_URL
from auth import is_admin
from utils import esc, bold, code, italic
from api_client import sess, api_get, fetch_all
from backup import do_backup, make_zip
from keyboards import main_kb, back_kb
import db

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    name = esc(user.first_name or "there")
    role = "🔑 <b>Admin</b>" if is_admin(user.id) else "👤 User"
    await update.message.reply_text(
        f"👋 Hey, <b>{name}</b>! {role}\n\n"
        f"🎛 <b>SCFiles Backend Manager</b>\n"
        f"<i>Manage movies, series & collections right from Telegram.</i>\n\n"
        f"Choose an action:", reply_markup=main_kb(), parse_mode=ParseMode.HTML)

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 <b>Commands</b>\n\n"
        "<b>Info</b>\n/start · /status · /stats\n/movies · /series · /collections\n\n"
        "<b>Add Content</b> <i>(admin)</i>\n/addmovie · /addseries · /addcollection\n\n"
        "<b>Manage</b> <i>(admin)</i>\n/editmovie · /editseries · /editcollection\n/delmovie · /delseries · /delcollection\n\n"
        "<b>Admins</b> <i>(admin)</i>\n/addadmin · /removeadmin · /listadmins\n\n"
        "<b>Channels</b> <i>(admin)</i>\n/addchannel · /removechannel · /listchannels\n\n"
        "<b>TMDB</b>\n/tmdb — search movies &amp; TV shows\n\n"
        "<b>Backup</b> <i>(admin)</i>\n/backup · /backupzip · /setbackup\n\n"
        "<b>Tools</b>\n/logs · /cancel", parse_mode=ParseMode.HTML)

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("❌ <b>Cancelled.</b>\n<i>Use /start to go back.</i>",
                                    parse_mode=ParseMode.HTML)
    return ConversationHandler.END

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    now    = datetime.now(IST)
    uptime = str(now - state.BOT_STARTED_AT).split(".")[0]
    bk     = state.LAST_BACKUP_AT.strftime("%d %b %H:%M") if state.LAST_BACKUP_AT else "Never"
    try:
        s = await sess(); t0 = datetime.now()
        async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
            ms   = (datetime.now()-t0).total_seconds()*1000
            ic   = "🟢" if r.status==200 else "🟡"
            body = esc((await r.text())[:80])
        txt = (f"🖥 <b>System Status</b>\n{'─'*28}\n"
               f"🤖 Bot    → 🟢 Online  |  ⏱ <code>{uptime}</code>\n"
               f"💾 Backup → <code>{bk}</code>\n{'─'*28}\n"
               f"🌐 Backend → {ic} HTTP {r.status}  |  ⚡ <code>{ms:.0f}ms</code>\n"
               f"🔗 <code>{esc(BACKEND_URL)}</code>\n📨 <code>{body}</code>")
    except Exception as e:
        txt = (f"🖥 <b>System Status</b>\n"
               f"🤖 Bot: 🟢 Online  |  ⏱ <code>{uptime}</code>\n"
               f"🌐 Backend: 🔴 Offline\n❗ <code>{esc(str(e))}</code>")
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=back_kb())

async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    mv, sr, co = await fetch_all()
    total_eps = sum(len(ep.get("episodes",[]))
                    for s in sr for ep in s.get("seasons",[]))
    total_dl  = sum(len(m.get("downloads",{})) for m in mv)
    await update.message.reply_text(
        f"📊 <b>Database Statistics</b>\n{'─'*28}\n"
        f"🎬 Movies        → <b>{len(mv)}</b>  ({total_dl} download links)\n"
        f"📺 Series        → <b>{len(sr)}</b>  ({total_eps} total episodes)\n"
        f"🗂 Collections  → <b>{len(co)}</b>\n{'─'*28}\n"
        f"🕐 <i>{datetime.now(IST).strftime('%d %b %Y, %H:%M IST')}</i>",
        parse_mode=ParseMode.HTML, reply_markup=back_kb())

async def cmd_movies(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    items = (await api_get("/api/movies?limit=15") or [])[:15]
    if not items:
        return await update.message.reply_text("📭 No movies found.", parse_mode=ParseMode.HTML)
    lines = []
    for m in items:
        dls = ",".join(str(k)+"p" for k in sorted(m.get("downloads",{}).keys()))
        ex  = f"  <i>{esc(m['extras'])}</i>" if m.get("extras") else ""
        lines.append(f"• {code(m.get('id','?'))} {code(m.get('tmdb_id','?'))}{ex}  [{dls or '—'}]")
    await update.message.reply_text(
        f"🎬 <b>Recent Movies</b> ({len(items)} total)\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML, reply_markup=back_kb())

async def cmd_series(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    items = await api_get("/api/series") or []
    if not items:
        return await update.message.reply_text("📭 No series found.", parse_mode=ParseMode.HTML)
    lines = []
    for s in items[:15]:
        sns = len(s.get("seasons",[]))
        eps = sum(len(sn.get("episodes",[])) for sn in s.get("seasons",[]))
        lines.append(f"• {code(s.get('id','?'))} {code(s.get('tmdb_id','?'))}  [{sns}S / {eps}ep]")
    await update.message.reply_text(
        f"📺 <b>Recent Series</b> ({len(items)} total)\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML, reply_markup=back_kb())

async def cmd_collections(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cols = await api_get("/api/collections") or {}
    if not cols:
        return await update.message.reply_text("📭 No collections.", parse_mode=ParseMode.HTML)
    lines = [f"• {code(k)} → {bold(v.get('name','?'))}  ({len(v.get('movies',[]))} movies)"
             for k,v in list(cols.items())[:15]]
    await update.message.reply_text(
        f"🗂 <b>Collections</b> ({len(cols)} total)\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML, reply_markup=back_kb())

async def cmd_logs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ Access denied.", parse_mode=ParseMode.HTML)
    if not os.path.exists(LOG_FILE):
        return await update.message.reply_text("📭 No log file yet.", parse_mode=ParseMode.HTML)
    with open(LOG_FILE,"rb") as f:
        f.seek(0,2); size=f.tell(); f.seek(max(0,size-8192)); tail=f.read()
    buf = io.BytesIO(tail)
    ts  = datetime.now(IST).strftime("%Y-%m-%d_%H-%M-%S")
    buf.name = f"scfiles_logs_{ts}.txt"
    await update.message.reply_document(
        buf, caption=f"📋 <b>Logs</b>  {len(tail)//1024 or 1}KB · <code>{ts}</code>",
        parse_mode=ParseMode.HTML)

async def cmd_backup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ Access denied.", parse_mode=ParseMode.HTML)
    m = await update.message.reply_text("💾 <i>Starting backup…</i>", parse_mode=ParseMode.HTML)
    ok, info = await do_backup(ctx.bot, state.BACKUP_TARGET or str(update.effective_chat.id))
    if ok: await m.edit_text(f"✅ <b>Backup done!</b>\n{code(info)}", parse_mode=ParseMode.HTML)
    else:  await m.edit_text(f"❌ <b>Backup failed:</b>\n{code(info)}", parse_mode=ParseMode.HTML)

async def cmd_backupzip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ Access denied.", parse_mode=ParseMode.HTML)
    m = await update.message.reply_text("📦 <i>Building ZIP…</i>", parse_mode=ParseMode.HTML)
    try:
        data, ts = await make_zip()
        buf = io.BytesIO(data); buf.name = f"backup_all_{ts}.zip"
        await update.message.reply_document(buf, caption=f"✅ <b>Backup ZIP</b>  {code(ts)}", parse_mode=ParseMode.HTML)
        await m.delete()
    except Exception as e:
        await m.edit_text(f"❌ <b>ZIP failed:</b>\n{code(e)}", parse_mode=ParseMode.HTML)

async def cmd_setbackup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ Access denied.", parse_mode=ParseMode.HTML)
    args = ctx.args
    if not args:
        return await update.message.reply_text(
            f"📦 <b>Backup channel:</b> {code(state.BACKUP_TARGET or 'Not set')}\n\n"
            f"Usage: <code>/setbackup &lt;chat_id&gt;</code>", parse_mode=ParseMode.HTML)
    state.BACKUP_TARGET = args[0].strip()
    await db.set_backup_target(state.BACKUP_TARGET)
    await update.message.reply_text(f"✅ Backup chat → {code(state.BACKUP_TARGET)}", parse_mode=ParseMode.HTML)
