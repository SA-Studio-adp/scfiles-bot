"""handlers/menu.py — the inline "main menu" callback handler (all the
m_* callback_data buttons on /start)."""
import io
from datetime import datetime

import aiohttp
from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from config import state, IST
from auth import is_admin
from utils import esc, bold, code
from api_client import sess, api_get, fetch_all
from backup import do_backup, make_zip
from keyboards import main_kb, back_kb

async def menu_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer(); d = q.data

    if d == "m_back":
        user = q.from_user; name = esc(user.first_name or "there")
        role = "🔑 <b>Admin</b>" if is_admin(user.id) else "👤 User"
        await q.edit_message_text(
            f"👋 Hey, <b>{name}</b>! {role}\n\n"
            f"🎛 <b>SCFiles Backend Manager</b>\n"
            f"<i>Manage movies, series & collections.</i>\n\nChoose an action:",
            reply_markup=main_kb(), parse_mode=ParseMode.HTML)
        return

    if d in ("m_addmovie","m_addseries","m_addcollection","m_editmovie"):
        cmd_map = {"m_addmovie":"/addmovie","m_addseries":"/addseries",
                   "m_addcollection":"/addcollection","m_editmovie":"/editmovie"}
        await q.edit_message_text(
            f"Use {code(cmd_map[d])} to start.\n<i>Send it in the chat.</i>",
            parse_mode=ParseMode.HTML)
        return

    if d == "m_status":
        now    = datetime.now(IST); uptime = str(now-state.BOT_STARTED_AT).split(".")[0]
        bk     = state.LAST_BACKUP_AT.strftime("%d %b %H:%M") if state.LAST_BACKUP_AT else "Never"
        try:
            s = await sess(); t0 = datetime.now()
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                ms = (datetime.now()-t0).total_seconds()*1000
                ic = "🟢" if r.status==200 else "🟡"
            txt = (f"🖥 <b>Status</b>\n{'─'*24}\n"
                   f"🤖 Bot: 🟢 Online  ⏱ {code(uptime)}\n"
                   f"💾 Backup: {code(bk)}\n{'─'*24}\n"
                   f"🌐 Backend: {ic} {code(f'{ms:.0f}ms')}")
        except Exception as e:
            txt = f"🤖 Bot: 🟢  |  🌐 Backend: 🔴\n{code(str(e))}"
        await q.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_stats":
        mv, sr, co = await fetch_all()
        total_eps  = sum(len(ep.get("episodes",[]))
                         for s in sr for ep in s.get("seasons",[]))
        await q.edit_message_text(
            f"📊 <b>Stats</b>\n{'─'*24}\n"
            f"🎬 Movies: {bold(len(mv))}\n"
            f"📺 Series: {bold(len(sr))}  ({total_eps} episodes)\n"
            f"🗂 Collections: {bold(len(co))}",
            parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_backup":
        await q.edit_message_text("💾 <i>Running backup…</i>", parse_mode=ParseMode.HTML)
        ok, info = await do_backup(ctx.bot, state.BACKUP_TARGET or str(q.message.chat_id))
        txt = f"✅ <b>Backup done!</b>  {code(info)}" if ok else f"❌ <b>Failed:</b> {code(info)}"
        await q.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_backupzip":
        await q.edit_message_text("📦 <i>Building ZIP…</i>", parse_mode=ParseMode.HTML)
        try:
            data, ts = await make_zip(); buf = io.BytesIO(data); buf.name = f"backup_all_{ts}.zip"
            await q.message.reply_document(buf, caption=f"✅ <b>Backup ZIP</b>  {code(ts)}", parse_mode=ParseMode.HTML)
            await q.edit_message_text("✅ ZIP sent above.", parse_mode=ParseMode.HTML, reply_markup=back_kb())
        except Exception as e:
            await q.edit_message_text(f"❌ <b>ZIP failed:</b> {code(e)}", parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_movies":
        items = (await api_get("/api/movies?limit=10") or [])[:10]
        lines = [f"• {code(m.get('id','?'))}  {code(m.get('tmdb_id','?'))}" for m in items]
        await q.edit_message_text("🎬 <b>Recent Movies</b>\n\n"+("\n".join(lines) or "None"),
                                  parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_series":
        items = await api_get("/api/series") or []
        lines = [f"• {code(s.get('id','?'))}  [{len(s.get('seasons',[]))}S]" for s in items[:10]]
        await q.edit_message_text("📺 <b>Recent Series</b>\n\n"+("\n".join(lines) or "None"),
                                  parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_cols":
        cols  = await api_get("/api/collections") or {}
        lines = [f"• {code(k)} → {bold(v.get('name','?'))}" for k,v in list(cols.items())[:10]]
        await q.edit_message_text("🗂 <b>Collections</b>\n\n"+("\n".join(lines) or "None"),
                                  parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_tmdb":
        await q.edit_message_text("Use /tmdb to search TMDB.", parse_mode=ParseMode.HTML, reply_markup=back_kb())
