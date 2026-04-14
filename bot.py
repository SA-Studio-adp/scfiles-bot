"""
SCFiles Backend Manager Bot — python-telegram-bot edition
Upgraded UI, improved UX, robust HTML parse mode throughout.
"""

import asyncio, io, json, logging, os, zipfile
from datetime import datetime, timedelta

import aiohttp, pytz
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (Application, CommandHandler, CallbackQueryHandler,
                           MessageHandler, ConversationHandler, ContextTypes, filters)
from telegram.constants import ParseMode

# ── HTML escape ──────────────────────────────────────────────────────────────
def esc(v) -> str:
    return str(v).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def bold(v) -> str:  return f"<b>{esc(v)}</b>"
def code(v) -> str:  return f"<code>{esc(v)}</code>"
def italic(v) -> str: return f"<i>{esc(v)}</i>"

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_FILE = os.environ.get("LOG_FILE", "bot.log")
_fmt     = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_fmtr    = logging.Formatter(_fmt)
_sh = logging.StreamHandler(); _sh.setFormatter(_fmtr)
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8"); _fh.setFormatter(_fmtr)
logging.basicConfig(level=logging.INFO, handlers=[_sh, _fh])
logger = logging.getLogger("scfiles-bot")

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN      = os.environ["TELEGRAM_TOKEN"]
BACKEND_URL    = os.environ["BACKEND_URL"].rstrip("/")
TMDB_API_KEY   = os.environ.get("TMDB_API_KEY","").strip()   # optional — bot uses backend proxy
ADMIN_IDS      = [int(x) for x in os.environ.get("ADMIN_IDS","").split(",") if x.strip()]
BACKUP_CHAT_ID = os.environ.get("BACKUP_CHAT_ID","").strip()
WEB_HOST       = os.environ.get("WEB_HOST","0.0.0.0")
WEB_PORT       = int(os.environ.get("WEB_PORT","8080"))
BOT_WEB_URL    = os.environ.get("BOT_WEB_URL","").rstrip("/")
AUTO_PING_MIN  = int(os.environ.get("AUTO_PING_INTERVAL_MIN","4"))
BACKUP_CFG     = os.environ.get("BACKUP_CONFIG_FILE",".backup_config.json")

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG  = "https://image.tmdb.org/t/p/w500"
IST       = pytz.timezone("Asia/Kolkata")

# ── Runtime state ─────────────────────────────────────────────────────────────
BOT_STARTED_AT = datetime.now(IST)
LAST_BACKUP_AT = None
LAST_PING_AT   = None
BACKUP_TARGET  = BACKUP_CHAT_ID

# ── Conversation states ───────────────────────────────────────────────────────
(AM_TMDB, AM_EXTRA, AM_DL480, AM_DL720, AM_DL1080, AM_POS, AM_CONFIRM,
 AS_TMDB, AS_JSON, AS_CONFIRM,
 AC_ID, AC_NAME, AC_BANNER, AC_BGMUSIC, AC_MOVIES,
 DM_ID, DS_ID, DC_ID,
 EM_ID, EM_VALUE,
 TQ_TYPE, TQ_QUERY) = range(22)

# ── Auth ──────────────────────────────────────────────────────────────────────
def is_admin(uid: int) -> bool:
    return not ADMIN_IDS or uid in ADMIN_IDS

def admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("⛔ <b>Access denied.</b>", parse_mode=ParseMode.HTML)
            return ConversationHandler.END
        return await func(update, ctx)
    wrapper.__name__ = func.__name__
    return wrapper

# ── Backup config ─────────────────────────────────────────────────────────────
def load_backup_target() -> str:
    if os.path.exists(BACKUP_CFG):
        try:
            with open(BACKUP_CFG) as f:
                v = json.load(f).get("backup_chat_id","")
                if v: return str(v)
        except Exception: pass
    return BACKUP_CHAT_ID

def save_backup_target(cid: str):
    with open(BACKUP_CFG,"w") as f:
        json.dump({"backup_chat_id": str(cid)}, f)

# ── HTTP session ──────────────────────────────────────────────────────────────
_SESSION: aiohttp.ClientSession | None = None

async def sess() -> aiohttp.ClientSession:
    global _SESSION
    if _SESSION is None or _SESSION.closed:
        _SESSION = aiohttp.ClientSession()
    return _SESSION

async def _wake_backend():
    """Ping backend and wait up to 30s for it to wake (Render.com free tier sleeps)."""
    s = await sess()
    for attempt in range(4):
        try:
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status < 500:
                    return True
        except Exception:
            pass
        if attempt < 3:
            logger.info("Backend wake attempt %d/4 — waiting 8s…", attempt+1)
            await asyncio.sleep(8)
    return False

async def api_get(path: str):
    s = await sess()
    for attempt in range(2):
        try:
            async with s.get(f"{BACKEND_URL}{path}", timeout=aiohttp.ClientTimeout(total=20)) as r:
                return await r.json()
        except Exception as e:
            logger.error("API GET %s (attempt %d): %s", path, attempt+1, e)
            if attempt == 0:
                await _wake_backend()
    return None

async def api_post(path: str, data: dict):
    s = await sess()
    for attempt in range(2):
        try:
            async with s.post(f"{BACKEND_URL}{path}", json=data, timeout=aiohttp.ClientTimeout(total=20)) as r:
                return await r.json()
        except Exception as e:
            logger.error("API POST %s (attempt %d): %s", path, attempt+1, e)
            if attempt == 0:
                await _wake_backend()
    return None

async def api_delete(path: str):
    s = await sess()
    for attempt in range(2):
        try:
            async with s.delete(f"{BACKEND_URL}{path}", timeout=aiohttp.ClientTimeout(total=20)) as r:
                return await r.json()
        except Exception as e:
            logger.error("API DEL %s (attempt %d): %s", path, attempt+1, e)
            if attempt == 0:
                await _wake_backend()
    return None

# ── TMDB ──────────────────────────────────────────────────────────────────────
async def tmdb_get(path: str) -> dict | None:
    """Fetch TMDB data — uses backend proxy (no bot-side API key needed)."""
    try:
        s = await sess()
        # Use backend's TMDB proxy endpoint
        url = f"{BACKEND_URL}/api/tmdb?path={path}&language=en-US"
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.json()
        # Fallback: direct TMDB call if API key is configured
        if TMDB_API_KEY:
            sep = "&" if "?" in path else "?"
            async with s.get(f"{TMDB_BASE}{path}{sep}api_key={TMDB_API_KEY}&language=en-US",
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json() if r.status == 200 else None
    except Exception as e:
        logger.error("TMDB %s: %s", path, e)
    return None

async def tmdb_movie(tid: int): return await tmdb_get(f"/movie/{tid}?")
async def tmdb_tv(tid: int):    return await tmdb_get(f"/tv/{tid}?")
async def tmdb_search(q: str, t: str="movie") -> list:
    import urllib.parse
    r = await tmdb_get(f"/search/{t}?query={urllib.parse.quote(q)}")
    return (r or {}).get("results",[])[:5]

def fmt_movie(m: dict) -> str:
    title   = esc(m.get("title","?"))
    year    = esc((m.get("release_date") or "")[:4])
    rating  = m.get("vote_average",0)
    runtime = m.get("runtime",0)
    genres  = esc(", ".join(g["name"] for g in m.get("genres",[])) or "N/A")
    ov      = esc((m.get("overview") or "No overview.")[:350])
    lang    = esc((m.get("original_language") or "").upper())
    status  = esc(m.get("status",""))
    return (
        f"🎬 <b>{title}</b> <i>({year})</i>\n"
        f"⭐ <b>{rating:.1f}</b>/10  •  ⏱ {runtime} min  •  🌐 {lang}\n"
        f"🎭 {genres}\n"
        f"📌 {status}\n\n"
        f"📝 <i>{ov}</i>"
    )

def fmt_tv(t: dict) -> str:
    name    = esc(t.get("name","?"))
    year    = esc((t.get("first_air_date") or "")[:4])
    rating  = t.get("vote_average",0)
    seasons = t.get("number_of_seasons","?")
    eps     = t.get("number_of_episodes","?")
    genres  = esc(", ".join(g["name"] for g in t.get("genres",[])) or "N/A")
    ov      = esc((t.get("overview") or "No overview.")[:350])
    status  = esc(t.get("status",""))
    return (
        f"📺 <b>{name}</b> <i>({year})</i>\n"
        f"⭐ <b>{rating:.1f}</b>/10  •  {seasons} seasons / {eps} eps\n"
        f"🎭 {genres}\n"
        f"📌 {status}\n\n"
        f"📝 <i>{ov}</i>"
    )

def poster(info: dict) -> str | None:
    p = info.get("poster_path")
    return f"{TMDB_IMG}{p}" if p else None

# ── Backup ────────────────────────────────────────────────────────────────────
async def _payloads() -> dict[str,bytes]:
    out = {}
    for fname, path in [("movies.json","/api/movies"),("series.json","/api/series"),("collections.json","/api/collections")]:
        d = await api_get(path)
        if d is None: raise RuntimeError(f"Cannot fetch {path}")
        out[fname] = json.dumps(d, indent=2, ensure_ascii=False).encode()
    return out

async def make_zip() -> tuple[bytes,str]:
    ts  = datetime.now(IST).strftime("%Y-%m-%d_%H-%M")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for name, data in (await _payloads()).items():
            zf.writestr(f"{ts}_{name}", data)
    buf.seek(0); return buf.read(), ts

async def do_backup(bot, target=None) -> tuple[bool,str]:
    global LAST_BACKUP_AT
    dest_str = str(target or BACKUP_TARGET).strip()
    if not dest_str:
        return False, "No backup chat configured. Use /setbackup &lt;chat_id&gt;"
    try:    dest = int(dest_str)
    except: dest = dest_str
    ts = datetime.now(IST).strftime("%Y-%m-%d_%H-%M")
    try:
        await bot.send_message(dest, f"💾 <b>Auto-Backup</b> — <code>{ts}</code>", parse_mode=ParseMode.HTML)
        for fname, data in (await _payloads()).items():
            buf = io.BytesIO(data); buf.name = f"{ts}_{fname}"
            await bot.send_document(dest, buf, caption=f"📦 <code>{ts}_{fname}</code>", parse_mode=ParseMode.HTML)
        LAST_BACKUP_AT = datetime.now(IST)
        logger.info("Backup done → %s", dest_str)
        return True, dest_str
    except Exception as e:
        logger.error("Backup → %s failed: %s", dest_str, e)
        return False, str(e)

# ── Keyboards ─────────────────────────────────────────────────────────────────
def main_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🎬 Movies",        callback_data="m_movies"),
         InlineKeyboardButton("📺 Series",         callback_data="m_series")],
        [InlineKeyboardButton("🗂 Collections",    callback_data="m_cols"),
         InlineKeyboardButton("🔍 TMDB Search",    callback_data="m_tmdb")],
        [InlineKeyboardButton("📊 Stats",          callback_data="m_stats"),
         InlineKeyboardButton("🌐 Status",         callback_data="m_status")],
        [InlineKeyboardButton("💾 Backup Now",     callback_data="m_backup"),
         InlineKeyboardButton("📦 Backup ZIP",     callback_data="m_backupzip")],
        [InlineKeyboardButton("➕ Add Movie",      callback_data="m_addmovie"),
         InlineKeyboardButton("📺 Add Series",     callback_data="m_addseries")],
        [InlineKeyboardButton("🗂 Add Collection", callback_data="m_addcollection"),
         InlineKeyboardButton("✏️ Edit Movie",     callback_data="m_editmovie")],
    ]
    if BOT_WEB_URL:
        rows.append([InlineKeyboardButton("🩺 Open Web Dashboard ↗", url=BOT_WEB_URL)])
    return InlineKeyboardMarkup(rows)

def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("« Back to Menu", callback_data="m_back")]])

# ── /start ────────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    name = esc(user.first_name or "there")
    role = "🔑 <b>Admin</b>" if is_admin(user.id) else "👤 User"
    await update.message.reply_text(
        f"👋 Hey, <b>{name}</b>! {role}\n\n"
        f"🎛 <b>SCFiles Backend Manager</b>\n"
        f"<i>Manage your movie/series backend right from Telegram.</i>\n\n"
        f"Choose an action below:",
        reply_markup=main_kb(), parse_mode=ParseMode.HTML
    )

# ── /help ─────────────────────────────────────────────────────────────────────
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 <b>Available Commands</b>\n\n"
        "<b>📋 Info</b>\n"
        "/start — Main menu\n"
        "/status — Server &amp; bot health\n"
        "/stats — Database statistics\n"
        "/movies — List recent movies\n"
        "/series — List recent series\n"
        "/collections — List all collections\n\n"
        "<b>➕ Add Content</b> <i>(admin)</i>\n"
        "/addmovie — Add a movie with TMDB data\n"
        "/addseries — Add a series\n"
        "/addcollection — Create a collection\n\n"
        "<b>✏️ Manage</b> <i>(admin)</i>\n"
        "/editmovie — Edit any movie field\n"
        "/delmovie — Delete a movie\n"
        "/delseries — Delete a series\n"
        "/delcollection — Delete a collection\n\n"
        "<b>🔍 TMDB</b> <i>(admin)</i>\n"
        "/tmdb — Search movie or TV metadata\n\n"
        "<b>💾 Backup</b> <i>(admin)</i>\n"
        "/backup — Send JSON backups to channel\n"
        "/backupzip — Download all data as ZIP\n"
        "/setbackup — Set backup channel ID\n\n"
        "<b>🛠 Tools</b> <i>(admin)</i>\n"
        "/logs — View recent bot logs\n"
        "/cancel — Cancel current operation",
        parse_mode=ParseMode.HTML
    )

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "❌ <b>Cancelled.</b>\n<i>Use /start to go back to the menu.</i>",
        parse_mode=ParseMode.HTML
    )
    return ConversationHandler.END

# ── /status ───────────────────────────────────────────────────────────────────
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    now    = datetime.now(IST)
    uptime = str(now - BOT_STARTED_AT).split(".")[0]
    bk     = LAST_BACKUP_AT.strftime("%d %b %H:%M") if LAST_BACKUP_AT else "Never"
    try:
        s  = await sess(); t0 = datetime.now()
        async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
            ms   = (datetime.now()-t0).total_seconds()*1000
            code = r.status; body = esc((await r.text())[:80])
        ic  = "🟢" if code == 200 else "🟡"
        bar = "█" * min(int(ms/50),20)
        txt = (
            f"🖥 <b>System Status</b>\n"
            f"{'─'*28}\n"
            f"🤖 Bot       → 🟢 <b>Online</b>\n"
            f"⏱ Uptime    → <code>{uptime}</code>\n"
            f"💾 Backup    → <code>{bk}</code>\n"
            f"{'─'*28}\n"
            f"🌐 Backend   → {ic} <b>HTTP {code}</b>\n"
            f"⚡ Latency   → <code>{ms:.0f}ms</code>  <code>{bar}</code>\n"
            f"🔗 URL       → <code>{esc(BACKEND_URL)}</code>\n"
            f"📨 Response  → <code>{body}</code>"
        )
    except Exception as e:
        txt = (
            f"🖥 <b>System Status</b>\n"
            f"{'─'*28}\n"
            f"🤖 Bot    → 🟢 <b>Online</b>  |  ⏱ <code>{uptime}</code>\n"
            f"🌐 Backend → 🔴 <b>Offline</b>\n"
            f"❗ <code>{esc(str(e))}</code>"
        )
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=back_kb())

# ── /stats ────────────────────────────────────────────────────────────────────
async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    mv = await api_get("/api/movies") or []
    sr = await api_get("/api/series") or []
    co = await api_get("/api/collections") or {}
    now = datetime.now(IST)
    # count total episodes
    total_eps = sum(
        len(ep.get("episodes",[]))
        for s in sr
        for ep in s.get("seasons",[])
    )
    total_dl = sum(len(m.get("downloads",{})) for m in mv)
    await update.message.reply_text(
        f"📊 <b>Database Statistics</b>\n"
        f"{'─'*28}\n"
        f"🎬 Movies       → <b>{len(mv)}</b>\n"
        f"📥 Download links → <b>{total_dl}</b>\n"
        f"{'─'*28}\n"
        f"📺 Series       → <b>{len(sr)}</b>\n"
        f"🎞 Total episodes → <b>{total_eps}</b>\n"
        f"{'─'*28}\n"
        f"🗂 Collections  → <b>{len(co)}</b>\n"
        f"{'─'*28}\n"
        f"🕐 <i>{now.strftime('%d %b %Y, %H:%M:%S IST')}</i>",
        parse_mode=ParseMode.HTML, reply_markup=back_kb()
    )

# ── /movies ───────────────────────────────────────────────────────────────────
async def cmd_movies(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    items = await api_get("/api/movies?limit=15") or []
    if not items:
        return await update.message.reply_text("📭 No movies found.", parse_mode=ParseMode.HTML)
    lines = []
    for m in items[:15]:
        mid   = code(m.get("id","?"))
        tid   = code(m.get("tmdb_id","?"))
        extra = f" <i>· {esc(m['extras'])}</i>" if m.get("extras") else ""
        dls   = ", ".join(f"{k}p" for k in sorted(m.get("downloads",{}).keys()))
        lines.append(f"• {mid}  {tid}{extra}  [{dls or '—'}]")
    await update.message.reply_text(
        f"🎬 <b>Recent Movies</b>  ({len(items)} shown)\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML, reply_markup=back_kb()
    )

# ── /series ───────────────────────────────────────────────────────────────────
async def cmd_series(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    items = await api_get("/api/series?limit=15") or []
    if not items:
        return await update.message.reply_text("📭 No series found.", parse_mode=ParseMode.HTML)
    lines = []
    for s in items[:15]:
        sid  = code(s.get("id","?"))
        tid  = code(s.get("tmdb_id","?"))
        sns  = len(s.get("seasons",[]))
        eps  = sum(len(sn.get("episodes",[])) for sn in s.get("seasons",[]))
        lines.append(f"• {sid}  {tid}  [{sns}S / {eps}ep]")
    await update.message.reply_text(
        f"📺 <b>Recent Series</b>  ({len(items)} shown)\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML, reply_markup=back_kb()
    )

# ── /collections ──────────────────────────────────────────────────────────────
async def cmd_collections(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cols = await api_get("/api/collections") or {}
    if not cols:
        return await update.message.reply_text("📭 No collections found.", parse_mode=ParseMode.HTML)
    lines = [
        f"• {code(k)}  →  {bold(v.get('name','?'))}  ({len(v.get('movies',[]))} movies)"
        for k,v in list(cols.items())[:15]
    ]
    await update.message.reply_text(
        f"🗂 <b>Collections</b>  ({len(cols)} total)\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML, reply_markup=back_kb()
    )

# ── /logs ─────────────────────────────────────────────────────────────────────
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
        buf, caption=f"📋 <b>Bot Logs</b>  |  Last {len(tail)//1024 or 1}KB  |  <code>{ts}</code>",
        parse_mode=ParseMode.HTML
    )

# ── /backup ───────────────────────────────────────────────────────────────────
async def cmd_backup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ Access denied.", parse_mode=ParseMode.HTML)
    m = await update.message.reply_text("💾 <i>Starting backup…</i>", parse_mode=ParseMode.HTML)
    dest = BACKUP_TARGET or str(update.effective_chat.id)
    ok, info = await do_backup(ctx.bot, dest)
    if ok:
        await m.edit_text(f"✅ <b>Backup complete!</b>\nSent to <code>{esc(info)}</code>", parse_mode=ParseMode.HTML)
    else:
        await m.edit_text(f"❌ <b>Backup failed</b>\n<code>{esc(info)}</code>", parse_mode=ParseMode.HTML)

# ── /backupzip ────────────────────────────────────────────────────────────────
async def cmd_backupzip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ Access denied.", parse_mode=ParseMode.HTML)
    m = await update.message.reply_text("📦 <i>Building ZIP…</i>", parse_mode=ParseMode.HTML)
    try:
        data, ts = await make_zip()
        buf = io.BytesIO(data); buf.name = f"backup_all_{ts}.zip"
        await update.message.reply_document(
            buf, caption=f"✅ <b>Backup ZIP ready</b>  |  <code>{ts}</code>", parse_mode=ParseMode.HTML
        )
        await m.delete()
    except Exception as e:
        await m.edit_text(f"❌ <b>ZIP failed</b>\n<code>{esc(e)}</code>", parse_mode=ParseMode.HTML)

# ── /setbackup ────────────────────────────────────────────────────────────────
async def cmd_setbackup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global BACKUP_TARGET
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("⛔ Access denied.", parse_mode=ParseMode.HTML)
    args = ctx.args
    if not args:
        return await update.message.reply_text(
            f"📦 <b>Backup Channel</b>\n\nCurrent: <code>{esc(BACKUP_TARGET or 'Not set')}</code>\n\n"
            f"Usage: /setbackup &lt;chat_id&gt;\n<i>For channels use the -100xxxxxxxxxx format</i>",
            parse_mode=ParseMode.HTML
        )
    BACKUP_TARGET = args[0].strip()
    save_backup_target(BACKUP_TARGET)
    await update.message.reply_text(
        f"✅ <b>Backup channel updated!</b>\n<code>{esc(BACKUP_TARGET)}</code>",
        parse_mode=ParseMode.HTML
    )

# ═══════════════════════════════════════════════════════════════
#  ADD MOVIE  conversation
# ═══════════════════════════════════════════════════════════════
@admin_only
async def cmd_addmovie(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 <b>Add Movie</b>  —  Step 1/6\n\n"
        "Enter the <b>TMDB Movie ID</b>:\n"
        "<i>Find it at themoviedb.org — it's in the URL</i>",
        parse_mode=ParseMode.HTML
    )
    return AM_TMDB

async def am_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if not t.isdigit():
        await update.message.reply_text("❌ Enter a valid <b>numeric</b> TMDB ID.", parse_mode=ParseMode.HTML)
        return AM_TMDB
    info = await tmdb_movie(int(t))
    ctx.user_data["movie"] = {"tmdb_id": int(t), "downloads": {}, "subtitles": {}}
    if info:
        ctx.user_data["movie"]["id"] = (info.get("title","") or "").lower().replace(" ","-").replace("'","")
        p   = poster(info)
        cap = (fmt_movie(info) +
               f"\n\n✏️ Suggested ID: <code>{esc(ctx.user_data['movie']['id'])}</code>"
               f"\n\n<b>Step 2/6</b> — Enter <b>extras</b>\n<i>e.g. PreDVD · Tamil Audio · HQ · WEB-DL</i>\nOr send <code>-</code> to skip")
        if p: await update.message.reply_photo(p, caption=cap, parse_mode=ParseMode.HTML)
        else: await update.message.reply_text(cap, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            "⚠️ TMDB not found. Proceeding anyway.\n\n"
            "<b>Step 2/6</b> — Enter extras or <code>-</code>:", parse_mode=ParseMode.HTML
        )
    return AM_EXTRA

async def am_extra(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    ctx.user_data["movie"]["extras"] = "" if t=="-" else t
    await update.message.reply_text(
        "📥 <b>Step 3/6</b> — Enter <b>480p download link</b>\nOr <code>-</code> to skip",
        parse_mode=ParseMode.HTML
    )
    return AM_DL480

async def am_dl480(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if t != "-": ctx.user_data["movie"]["downloads"]["480"] = t
    await update.message.reply_text(
        "📥 <b>Step 4/6</b> — Enter <b>720p download link</b>\nOr <code>-</code> to skip",
        parse_mode=ParseMode.HTML
    )
    return AM_DL720

async def am_dl720(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if t != "-": ctx.user_data["movie"]["downloads"]["720"] = t
    await update.message.reply_text(
        "📥 <b>Step 5/6</b> — Enter <b>1080p download link</b>\nOr <code>-</code> to skip",
        parse_mode=ParseMode.HTML
    )
    return AM_DL1080

async def am_dl1080(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if t != "-": ctx.user_data["movie"]["downloads"]["1080"] = t
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("⬆️ Add to Top",    callback_data="pos_top"),
        InlineKeyboardButton("⬇️ Add to Bottom", callback_data="pos_bottom"),
    ]])
    await update.message.reply_text(
        "📌 <b>Step 6/6</b> — Choose position:",
        reply_markup=kb, parse_mode=ParseMode.HTML
    )
    return AM_POS

async def am_pos_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    pos = "top" if q.data == "pos_top" else "bottom"
    ctx.user_data["movie"]["pos"] = pos
    m  = ctx.user_data["movie"]
    dls = " · ".join(f"{k}p" for k in sorted(m.get("downloads",{}).keys())) or "none"
    await q.edit_message_text(
        f"✅ <b>Confirm Movie</b>\n"
        f"{'─'*28}\n"
        f"🆔 ID:       <code>{esc(m.get('id','?'))}</code>\n"
        f"🎬 TMDB:     <code>{esc(m.get('tmdb_id','?'))}</code>\n"
        f"📝 Extras:   <i>{esc(m.get('extras','') or '—')}</i>\n"
        f"📥 Links:    <code>{dls}</code>\n"
        f"📌 Position: <b>{pos}</b>\n"
        f"{'─'*28}\n"
        f"Type <b>yes</b> to add or <b>no</b> to cancel:",
        parse_mode=ParseMode.HTML
    )
    return AM_CONFIRM

async def am_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip().lower()
    if t == "no":
        ctx.user_data.clear()
        await update.message.reply_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    if t != "yes":
        await update.message.reply_text("Please type <b>yes</b> or <b>no</b>.", parse_mode=ParseMode.HTML)
        return AM_CONFIRM
    m = ctx.user_data.pop("movie", {})
    m.setdefault("subtitles", {})
    pos = m.pop("pos", "bottom")   # remove 'pos' — backend uses 'position' param, not stored field
    payload = {**m, "position": pos}
    r = await api_post("/api/movies", payload)
    ctx.user_data.clear()
    if r and r.get("success"):
        await update.message.reply_text(
            f"✅ <b>Movie added!</b>  Position: <b>{esc(pos)}</b>\n"
            f"📊 Total movies: <b>{r['count']}</b>",
            parse_mode=ParseMode.HTML, reply_markup=back_kb()
        )
    else:
        err = esc(r.get("error", r) if isinstance(r, dict) else r)
        await update.message.reply_text(
            f"❌ <b>Failed:</b> <code>{err}</code>", parse_mode=ParseMode.HTML
        )
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════════
#  ADD SERIES  conversation
# ═══════════════════════════════════════════════════════════════
@admin_only
async def cmd_addseries(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📺 <b>Add Series</b>  —  Step 1/3\n\nEnter the <b>TMDB TV Show ID</b>:",
        parse_mode=ParseMode.HTML
    )
    return AS_TMDB

async def as_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if not t.isdigit():
        await update.message.reply_text("❌ Enter a valid <b>numeric</b> TMDB ID.", parse_mode=ParseMode.HTML)
        return AS_TMDB
    info = await tmdb_tv(int(t))
    ctx.user_data["series"] = {"tmdb_id": str(t), "seasons": []}
    if info:
        ctx.user_data["series"]["id"] = (info.get("name","") or "").lower().replace(" ","-").replace("'","")
        p   = poster(info)
        ep_template = (
            "<pre>[\n  {\n    \"season_number\": 1,\n    \"episodes\": [\n"
            "      {\"ep_number\": 1, \"links\": {\"360p\": \"URL\", \"720p\": \"URL\"}, \"subtitle\": \"\"}\n"
            "    ]\n  }\n]</pre>"
        )
        cap = (fmt_tv(info) +
               f"\n\n✏️ Suggested ID: <code>{esc(ctx.user_data['series']['id'])}</code>\n\n"
               f"<b>Step 2/3</b> — Paste <b>episode JSON</b>:\n"
               + ep_template)
        if p: await update.message.reply_photo(p, caption=cap, parse_mode=ParseMode.HTML)
        else: await update.message.reply_text(cap, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            "⚠️ TMDB not found.\n\n<b>Step 2/3</b> — Paste episode JSON:", parse_mode=ParseMode.HTML
        )
    return AS_JSON

async def as_json(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        seasons = json.loads(update.message.text.strip())
        if not isinstance(seasons, list): raise ValueError("Must be a JSON array")
        for s in seasons:
            if "season_number" not in s or "episodes" not in s: raise ValueError("Missing season_number or episodes")
            for ep in s["episodes"]:
                if "ep_number" not in ep or "links" not in ep: raise ValueError("Missing ep_number or links")
                ep.setdefault("subtitle","")
    except Exception as e:
        await update.message.reply_text(
            f"❌ <b>Invalid JSON:</b> <code>{esc(e)}</code>\n\nTry again:",
            parse_mode=ParseMode.HTML
        )
        return AS_JSON
    ctx.user_data["series"]["seasons"] = seasons
    total = sum(len(s.get("episodes",[])) for s in seasons)
    sr = ctx.user_data["series"]
    await update.message.reply_text(
        f"✅ <b>Confirm Series</b>\n"
        f"{'─'*28}\n"
        f"🆔 ID:      <code>{esc(sr.get('id','?'))}</code>\n"
        f"📺 TMDB:    <code>{esc(sr.get('tmdb_id','?'))}</code>\n"
        f"🗂 Seasons: <b>{len(seasons)}</b>  |  🎞 Episodes: <b>{total}</b>\n"
        f"📌 Position: <b>top</b> (always)\n"
        f"{'─'*28}\n"
        f"Type <b>yes</b> to add or <b>no</b> to cancel:",
        parse_mode=ParseMode.HTML
    )
    return AS_CONFIRM

async def as_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip().lower()
    if t == "no":
        ctx.user_data.clear()
        await update.message.reply_text("❌ <b>Cancelled.</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    if t != "yes":
        await update.message.reply_text("Type <b>yes</b> or <b>no</b>.", parse_mode=ParseMode.HTML)
        return AS_CONFIRM
    sr = ctx.user_data.pop("series", {})
    sr.pop("position", None)   # backend always unshifts series to top, no position field needed
    r  = await api_post("/api/series", sr)
    ctx.user_data.clear()
    if r and r.get("success"):
        await update.message.reply_text(
            f"✅ <b>Series added to top!</b>  📊 Total: <b>{r['count']}</b>",
            parse_mode=ParseMode.HTML, reply_markup=back_kb()
        )
    else:
        err = esc(r.get("error", r) if isinstance(r, dict) else r)
        await update.message.reply_text(
            f"❌ <b>Failed:</b> <code>{err}</code>", parse_mode=ParseMode.HTML
        )
        await update.message.reply_text(f"❌ <b>Failed:</b> <code>{esc(r.get("error", r) if isinstance(r, dict) else r)}</code>", parse_mode=ParseMode.HTML)
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════════
#  ADD COLLECTION  conversation
# ═══════════════════════════════════════════════════════════════
@admin_only
async def cmd_addcollection(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🗂 <b>Add Collection</b>  —  Step 1/5\n\n"
        "Enter collection <b>ID</b> (slug format):\n"
        "<i>Example: vijay · marvel-mcu · dc-comics</i>",
        parse_mode=ParseMode.HTML
    )
    return AC_ID

async def ac_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["col"] = {"col_id": update.message.text.strip()}
    await update.message.reply_text(
        "🗂 <b>Add Collection</b>  —  Step 2/5\n\nEnter collection <b>Name</b>:",
        parse_mode=ParseMode.HTML
    )
    return AC_NAME

async def ac_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["col"]["col_name"] = update.message.text.strip()
    await update.message.reply_text(
        "🗂 <b>Add Collection</b>  —  Step 3/5\n\n"
        "Enter <b>banner URL</b> or <code>-</code> to skip:",
        parse_mode=ParseMode.HTML
    )
    return AC_BANNER

async def ac_banner(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    ctx.user_data["col"]["col_banner"] = "" if t=="-" else t
    await update.message.reply_text(
        "🗂 <b>Add Collection</b>  —  Step 4/5\n\n"
        "Enter <b>bg-music URL</b> or <code>-</code> to skip:",
        parse_mode=ParseMode.HTML
    )
    return AC_BGMUSIC

async def ac_bgmusic(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    ctx.user_data["col"]["col_bgmusic"] = "" if t=="-" else t
    await update.message.reply_text(
        "🗂 <b>Add Collection</b>  —  Step 5/5\n\n"
        "Send <b>movie IDs</b> separated by commas or new lines:\n\n"
        "<i>Example:</i>\n"
        "<code>meesaya-murukku, natpe-thunai, naan-sirithal</code>\n\n"
        "💡 <i>The bot will auto-fetch quality and download links from your movies database.</i>",
        parse_mode=ParseMode.HTML
    )
    return AC_MOVIES

async def ac_movies(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Accept comma or newline separated movie IDs
    raw = update.message.text.strip()
    ids = [x.strip() for x in raw.replace("\n", ",").split(",") if x.strip()]

    if not ids:
        await update.message.reply_text("❌ No IDs entered. Try again:", parse_mode=ParseMode.HTML)
        return AC_MOVIES

    wait_msg = await update.message.reply_text(
        f"⏳ <i>Waking backend &amp; looking up {len(ids)} movie(s)…</i>", parse_mode=ParseMode.HTML
    )

    # Wake backend first (it may be sleeping on Render free tier)
    all_movies = await api_get("/api/movies") or []

    if not all_movies and len(ids) > 0:
        # Backend returned empty — could be genuinely empty or still waking
        await wait_msg.edit_text(
            "⏳ <i>Backend is waking up, retrying in 10s…</i>", parse_mode=ParseMode.HTML
        )
        await asyncio.sleep(10)
        all_movies = await api_get("/api/movies") or []

    movie_map = {m["id"]: m for m in all_movies}

    found     = []
    not_found = []
    for mid in ids:
        m = movie_map.get(mid)
        if m:
            downloads = m.get("downloads", {})
            if "1080" in downloads:   quality, dl_url = "1080p", downloads["1080"]
            elif "720" in downloads:  quality, dl_url = "720p",  downloads["720"]
            elif "480" in downloads:  quality, dl_url = "480p",  downloads["480"]
            elif downloads:
                k = next(iter(downloads))
                quality, dl_url = f"{k}p", downloads[k]
            else:
                quality, dl_url = "N/A", ""
            found.append({"id": mid, "tmdb_id": m.get("tmdb_id", 0), "quality": quality, "download": dl_url})
        else:
            not_found.append(mid)

    await wait_msg.delete()

    if not found:
        nf = ", ".join(code(x) for x in not_found[:10])
        await update.message.reply_text(
            f"❌ <b>None of the IDs were found</b> in the movies database.\n\n"
            f"Not found: {nf}\n\n"
            f"Use /movies to see valid IDs and try again.",
            parse_mode=ParseMode.HTML
        )
        return AC_MOVIES

    col = ctx.user_data.pop("col", {})
    r   = await api_post("/api/collections", {
        "id":       col["col_id"],
        "name":     col["col_name"],
        "banner":   col.get("col_banner", ""),
        "bg-music": col.get("col_bgmusic", ""),
        "movies":   found,
    })
    ctx.user_data.clear()

    if r and r.get("success"):
        warn = ""
        if not_found:
            nf_list = ", ".join(esc(x) for x in not_found)
            warn = f"\n\n⚠️ <i>Not found (skipped): {nf_list}</i>"
        col_name = esc(col["col_name"])
        await update.message.reply_text(
            f"✅ <b>Collection created!</b>\n"
            f"📛 Name: <b>{col_name}</b>\n"
            f"🎬 Movies added: <b>{len(found)}</b>  |  📊 Total collections: <b>{r['total']}</b>"
            f"{warn}",
            parse_mode=ParseMode.HTML, reply_markup=back_kb()
        )
    else:
        err = esc(r.get("error", str(r)) if isinstance(r, dict) else str(r))
        await update.message.reply_text(
            f"❌ <b>Collection creation failed</b>\n<code>{err}</code>\n\n"
            f"<i>The backend may be sleeping. Try again in 30 seconds.</i>",
            parse_mode=ParseMode.HTML
        )
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════════
#  DELETE  conversations
# ═══════════════════════════════════════════════════════════════
@admin_only
async def cmd_delmovie(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🗑 <b>Delete Movie</b>\n\nEnter the movie <b>ID</b> to delete:",
        parse_mode=ParseMode.HTML
    )
    return DM_ID

async def dm_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    mid = update.message.text.strip()
    kb  = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, delete", callback_data=f"delconfirm_movie_{mid}"),
        InlineKeyboardButton("❌ Cancel",       callback_data="delcancel"),
    ]])
    await update.message.reply_text(
        f"⚠️ <b>Confirm deletion</b>\n\nDelete movie <code>{esc(mid)}</code>?\n<i>This cannot be undone.</i>",
        reply_markup=kb, parse_mode=ParseMode.HTML
    )
    return ConversationHandler.END

@admin_only
async def cmd_delseries(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🗑 <b>Delete Series</b>\n\nEnter the series <b>ID</b> to delete:",
        parse_mode=ParseMode.HTML
    )
    return DS_ID

async def ds_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sid = update.message.text.strip()
    kb  = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, delete", callback_data=f"delconfirm_series_{sid}"),
        InlineKeyboardButton("❌ Cancel",       callback_data="delcancel"),
    ]])
    await update.message.reply_text(
        f"⚠️ <b>Confirm deletion</b>\n\nDelete series <code>{esc(sid)}</code>?\n<i>This cannot be undone.</i>",
        reply_markup=kb, parse_mode=ParseMode.HTML
    )
    return ConversationHandler.END

@admin_only
async def cmd_delcollection(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🗑 <b>Delete Collection</b>\n\nEnter the collection <b>ID</b> to delete:",
        parse_mode=ParseMode.HTML
    )
    return DC_ID

async def dc_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.message.text.strip()
    kb  = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, delete", callback_data=f"delconfirm_col_{cid}"),
        InlineKeyboardButton("❌ Cancel",       callback_data="delcancel"),
    ]])
    await update.message.reply_text(
        f"⚠️ <b>Confirm deletion</b>\n\nDelete collection <code>{esc(cid)}</code>?\n<i>This cannot be undone.</i>",
        reply_markup=kb, parse_mode=ParseMode.HTML
    )
    return ConversationHandler.END

# ── Delete confirmation callback ──────────────────────────────────────────────
async def del_confirm_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    parts = q.data.split("_", 2)   # delconfirm_TYPE_ID
    dtype = parts[1]; did = parts[2]
    if dtype == "movie":
        r = await api_delete(f"/api/movies/{did}")
        if r and r.get("success"):
            await q.edit_message_text(f"✅ Movie <code>{esc(did)}</code> deleted.  Remaining: <b>{r['count']}</b>", parse_mode=ParseMode.HTML)
        else:
            await q.edit_message_text(f"❌ <b>Failed:</b> <code>{esc(r.get("error", r) if isinstance(r, dict) else r)}</code>", parse_mode=ParseMode.HTML)
    elif dtype == "series":
        r = await api_delete(f"/api/series/{did}")
        if r and r.get("success"):
            await q.edit_message_text(f"✅ Series <code>{esc(did)}</code> deleted.  Remaining: <b>{r['count']}</b>", parse_mode=ParseMode.HTML)
        else:
            await q.edit_message_text(f"❌ <b>Failed:</b> <code>{esc(r.get("error", r) if isinstance(r, dict) else r)}</code>", parse_mode=ParseMode.HTML)
    elif dtype == "col":
        r = await api_delete(f"/api/collections/{did}")
        if r and r.get("success"):
            await q.edit_message_text(f"✅ Collection <code>{esc(did)}</code> deleted.  Total: <b>{r['total']}</b>", parse_mode=ParseMode.HTML)
        else:
            await q.edit_message_text(f"❌ <b>Failed:</b> <code>{esc(r.get("error", r) if isinstance(r, dict) else r)}</code>", parse_mode=ParseMode.HTML)

async def del_cancel_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("Cancelled")
    await update.callback_query.edit_message_text("❌ <b>Deletion cancelled.</b>", parse_mode=ParseMode.HTML)

# ═══════════════════════════════════════════════════════════════
#  EDIT MOVIE  conversation
# ═══════════════════════════════════════════════════════════════
@admin_only
async def cmd_editmovie(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✏️ <b>Edit Movie</b>\n\nEnter the movie <b>ID</b>:",
        parse_mode=ParseMode.HTML
    )
    return EM_ID

async def em_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    mid    = update.message.text.strip()
    movies = await api_get("/api/movies") or []
    movie  = next((m for m in movies if m["id"] == mid), None)
    if not movie:
        await update.message.reply_text(f"❌ Movie <code>{esc(mid)}</code> not found. Try again or /cancel:", parse_mode=ParseMode.HTML)
        return EM_ID
    ctx.user_data["edit_movie"] = movie
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Extras",    callback_data="ef_extras"),
         InlineKeyboardButton("📥 Downloads", callback_data="ef_downloads")],
        [InlineKeyboardButton("💬 Subtitles", callback_data="ef_subtitles"),
         InlineKeyboardButton("🎬 TMDB ID",   callback_data="ef_tmdb_id")],
        [InlineKeyboardButton("🆔 Slug ID",   callback_data="ef_id")],
    ])
    await update.message.reply_text(
        f"✏️ <b>Edit Movie:</b> <code>{esc(mid)}</code>\n\nChoose a field to edit:",
        reply_markup=kb, parse_mode=ParseMode.HTML
    )
    return EM_VALUE

async def em_field_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    field = q.data[3:]
    ctx.user_data["edit_field"] = field
    current = ctx.user_data.get("edit_movie",{}).get(field,"")
    await q.edit_message_text(
        f"✏️ Editing <b>{esc(field)}</b>\n\n"
        f"Current value:\n<pre>{esc(json.dumps(current, ensure_ascii=False))}</pre>\n\n"
        f"Enter new value <i>(JSON for objects/arrays)</i>:",
        parse_mode=ParseMode.HTML
    )
    return EM_VALUE

async def em_value(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    field = ctx.user_data.get("edit_field")
    if not field:
        await update.message.reply_text("👆 Choose a field using the buttons above.", parse_mode=ParseMode.HTML)
        return EM_VALUE
    movie = ctx.user_data.get("edit_movie",{})
    try: val = json.loads(update.message.text.strip())
    except: val = update.message.text.strip()
    movie[field] = val
    r = await api_post("/api/movies", movie)
    ctx.user_data.clear()
    if r and r.get("success"):
        await update.message.reply_text(
            f"✅ <b>Updated!</b>\n<code>{esc(movie.get('id','?'))}</code> → field <code>{esc(field)}</code> saved.",
            parse_mode=ParseMode.HTML, reply_markup=back_kb()
        )
    else:
        await update.message.reply_text(f"❌ <b>Failed:</b> <code>{esc(r.get("error", r) if isinstance(r, dict) else r)}</code>", parse_mode=ParseMode.HTML)
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════════
#  TMDB SEARCH  conversation
# ═══════════════════════════════════════════════════════════════
@admin_only
async def cmd_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🎬 Movie",   callback_data="tq_movie"),
        InlineKeyboardButton("📺 TV Show", callback_data="tq_tv"),
    ]])
    await update.message.reply_text(
        "🔍 <b>TMDB Search</b>\n\nWhat are you looking for?",
        reply_markup=kb, parse_mode=ParseMode.HTML
    )
    return TQ_TYPE

async def tq_type_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    ctx.user_data["tmdb_type"] = "tv" if q.data == "tq_tv" else "movie"
    label = "TV Show" if q.data == "tq_tv" else "Movie"
    await q.edit_message_text(
        f"🔍 <b>TMDB {label} Search</b>\n\nEnter title to search:",
        parse_mode=ParseMode.HTML
    )
    return TQ_QUERY

async def tq_query(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    mtype   = ctx.user_data.pop("tmdb_type","movie")
    results = await tmdb_search(update.message.text.strip(), mtype)
    ctx.user_data.clear()
    if not results:
        await update.message.reply_text(
            "❌ <b>No results found.</b>\n<i>Try a different query.</i>",
            parse_mode=ParseMode.HTML
        )
        return ConversationHandler.END
    await update.message.reply_text(
        f"🔍 Found <b>{len(results)}</b> result(s):",
        parse_mode=ParseMode.HTML
    )
    for res in results[:3]:
        full = await (tmdb_movie(res["id"]) if mtype=="movie" else tmdb_tv(res["id"]))
        if not full: continue
        cap = (fmt_movie(full) if mtype=="movie" else fmt_tv(full)) + f"\n\n🆔 TMDB ID: <code>{esc(full['id'])}</code>"
        p   = poster(full)
        if p: await update.message.reply_photo(p, caption=cap, parse_mode=ParseMode.HTML)
        else: await update.message.reply_text(cap, parse_mode=ParseMode.HTML)
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════════
#  MENU CALLBACKS
# ═══════════════════════════════════════════════════════════════
async def menu_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer(); d = q.data

    if d == "m_back":
        user = q.from_user; name = esc(user.first_name or "there")
        role = "🔑 <b>Admin</b>" if is_admin(user.id) else "👤 User"
        await q.edit_message_text(
            f"👋 Hey, <b>{name}</b>! {role}\n\n"
            f"🎛 <b>SCFiles Backend Manager</b>\n"
            f"<i>Manage your movie/series backend right from Telegram.</i>\n\n"
            f"Choose an action below:",
            reply_markup=main_kb(), parse_mode=ParseMode.HTML
        )
        return

    # Redirect add/edit commands via menu buttons
    if d in ("m_addmovie","m_addseries","m_addcollection","m_editmovie"):
        cmd_map = {"m_addmovie":"/addmovie","m_addseries":"/addseries",
                   "m_addcollection":"/addcollection","m_editmovie":"/editmovie"}
        await q.edit_message_text(
            f"Use {code(cmd_map[d])} command to start.\n<i>Or send it directly in the chat.</i>",
            parse_mode=ParseMode.HTML
        )
        return

    if d == "m_status":
        now    = datetime.now(IST); uptime = str(now - BOT_STARTED_AT).split(".")[0]
        bk     = LAST_BACKUP_AT.strftime("%d %b %H:%M") if LAST_BACKUP_AT else "Never"
        try:
            s = await sess(); t0 = datetime.now()
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                ms = (datetime.now()-t0).total_seconds()*1000; ic = "🟢" if r.status==200 else "🟡"; body = esc((await r.text())[:60])
            txt = (f"🖥 <b>System Status</b>\n{'─'*26}\n"
                   f"🤖 Bot     → 🟢 <b>Online</b>\n⏱ Uptime → <code>{uptime}</code>\n💾 Backup → <code>{bk}</code>\n{'─'*26}\n"
                   f"🌐 Backend → {ic} <b>HTTP {r.status}</b>\n⚡ Latency → <code>{ms:.0f}ms</code>\n🔗 <code>{esc(BACKEND_URL)}</code>")
        except Exception as e:
            txt = f"🖥 <b>System Status</b>\n🤖 Bot: 🟢  |  🌐 Backend: 🔴\n❗ <code>{esc(e)}</code>"
        await q.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_stats":
        mv = await api_get("/api/movies") or []; sr = await api_get("/api/series") or []; co = await api_get("/api/collections") or {}
        total_eps = sum(len(ep.get("episodes",[])) for s in sr for ep in s.get("seasons",[]))
        await q.edit_message_text(
            f"📊 <b>Database Stats</b>\n{'─'*26}\n"
            f"🎬 Movies: <b>{len(mv)}</b>\n📺 Series: <b>{len(sr)}</b>  🎞 Episodes: <b>{total_eps}</b>\n🗂 Collections: <b>{len(co)}</b>",
            parse_mode=ParseMode.HTML, reply_markup=back_kb()
        )

    elif d == "m_backup":
        await q.edit_message_text("💾 <i>Running backup…</i>", parse_mode=ParseMode.HTML)
        ok, info = await do_backup(ctx.bot, BACKUP_TARGET or str(q.message.chat_id))
        await q.edit_message_text(
            f"✅ <b>Backup complete!</b>\nSent to <code>{esc(info)}</code>" if ok
            else f"❌ <b>Backup failed</b>\n<code>{esc(info)}</code>",
            parse_mode=ParseMode.HTML, reply_markup=back_kb()
        )

    elif d == "m_backupzip":
        await q.edit_message_text("📦 <i>Building ZIP…</i>", parse_mode=ParseMode.HTML)
        try:
            data, ts = await make_zip(); buf = io.BytesIO(data); buf.name = f"backup_all_{ts}.zip"
            await q.message.reply_document(buf, caption=f"✅ <b>Backup ZIP</b>  <code>{ts}</code>", parse_mode=ParseMode.HTML)
            await q.edit_message_text("✅ ZIP sent above.", parse_mode=ParseMode.HTML, reply_markup=back_kb())
        except Exception as e:
            await q.edit_message_text(f"❌ <b>ZIP failed:</b>\n<code>{esc(e)}</code>", parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_movies":
        items = await api_get("/api/movies?limit=10") or []
        lines = [f"• {code(m.get('id','?'))}  {code(m.get('tmdb_id','?'))}  [{', '.join(str(k)+'p' for k in sorted(m.get('downloads',{}).keys())) or '—'}]" for m in items[:10]]
        await q.edit_message_text("🎬 <b>Recent Movies</b>\n\n"+("\n".join(lines) or "None"), parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_series":
        items = await api_get("/api/series?limit=10") or []
        lines = [f"• {code(s.get('id','?'))}  [{len(s.get('seasons',[]))}S]" for s in items[:10]]
        await q.edit_message_text("📺 <b>Recent Series</b>\n\n"+("\n".join(lines) or "None"), parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_cols":
        cols = await api_get("/api/collections") or {}
        lines = [f"• {code(k)}  →  {bold(v.get('name','?'))}  ({len(v.get('movies',[]))})" for k,v in list(cols.items())[:10]]
        await q.edit_message_text("🗂 <b>Collections</b>\n\n"+("\n".join(lines) or "None"), parse_mode=ParseMode.HTML, reply_markup=back_kb())

    elif d == "m_tmdb":
        await q.edit_message_text(
            "🔍 Use /tmdb to search TMDB metadata.\n<i>Supports movies and TV shows.</i>",
            parse_mode=ParseMode.HTML, reply_markup=back_kb()
        )

# ═══════════════════════════════════════════════════════════════
#  WEB DASHBOARD  (upgraded)
# ═══════════════════════════════════════════════════════════════
async def web_dashboard(req: web.Request) -> web.Response:
    now = datetime.now(IST); uptime = str(now - BOT_STARTED_AT).split(".")[0]
    b_status="offline"; b_code="N/A"; b_ms=0.0; b_err=""
    try:
        s = await sess(); t0 = datetime.now()
        async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
            b_code=str(r.status); b_ms=(datetime.now()-t0).total_seconds()*1000
            b_status="online" if r.status==200 else "degraded"
    except Exception as e: b_err=str(e)

    movies = await api_get("/api/movies") or []
    series = await api_get("/api/series") or []
    cols   = await api_get("/api/collections") or {}
    bk = LAST_BACKUP_AT.strftime("%d %b %Y, %H:%M IST") if LAST_BACKUP_AT else "Never"
    pg = LAST_PING_AT.strftime("%H:%M:%S IST")           if LAST_PING_AT   else "Never"

    total_eps = sum(len(ep.get("episodes",[])) for s in series for ep in s.get("seasons",[]))
    total_dls = sum(len(m.get("downloads",{})) for m in movies)

    lat_w = min(max(int(b_ms/8),4),100)
    lat_c = "#00e5a0" if b_ms<400 else ("#ffb340" if b_ms<1000 else "#ff4d6d")
    s_cls = "ok" if b_status=="online" else ("warn" if b_status=="degraded" else "bad")
    s_ico = {"online":"🟢","degraded":"🟡","offline":"🔴"}.get(b_status,"⚪")

    def _mv_row(m):
        mid     = m.get('id','?')
        tmdb    = m.get('tmdb_id','?')
        extras  = m.get('extras','') or '<span class="dim">—</span>'
        dls     = m.get('downloads',{})
        dl_html = ('<span class="pill">' + ' · '.join(str(k)+'p' for k in sorted(dls.keys())) + '</span>') if dls else '<span class="dim">—</span>'
        pos_html = '<span class="pos-top">TOP</span>' if m.get('pos') == 'top' else ''
        return f"<tr><td><code>{mid}</code></td><td><code>{tmdb}</code></td><td>{extras}</td><td>{dl_html}</td><td>{pos_html}</td></tr>"
    mv_rows = "".join(_mv_row(m) for m in movies[:10])
    def _sr_row(s):
        sid  = s.get('id','?')
        tmdb = s.get('tmdb_id','?')
        sns  = len(s.get('seasons',[]))
        eps  = sum(len(sn.get('episodes',[]))for sn in s.get('seasons',[]))
        return f"<tr><td><code>{sid}</code></td><td><code>{tmdb}</code></td><td><b>{sns}</b></td><td>{eps}</td></tr>"
    sr_rows = "".join(_sr_row(s) for s in series[:10])
    def _co_row(k, v):
        name = v.get('name','?')
        cnt  = len(v.get('movies',[]))
        return f"<tr><td><code>{k}</code></td><td><b>{name}</b></td><td>{cnt}</td></tr>"
    co_rows = "".join(_co_row(k,v) for k,v in list(cols.items())[:10])
    empty = "<tr><td colspan='5' class='empty'>No data</td></tr>"

    err_row = (f"<div class='srow'><span class='lbl'>Error</span><span class='bad' style='font-size:.72rem'>{b_err[:60]}</span></div>"
               if b_err else "")

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>SCFiles Dashboard</title>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg: #07080f; --s1: #0d1018; --s2: #12161f; --s3: #181d28;
      --border: #1e2535; --border2: #252d3d;
      --text: #e2e8f4; --muted: #4a5878; --dim: #2d3850;
      --ok: #00d98b; --warn: #f59e0b; --bad: #f43f5e;
      --accent: #6366f1; --accent2: #8b5cf6; --accent3: #06b6d4;
      --mono: 'JetBrains Mono', monospace;
      --sans: 'Syne', sans-serif;
    }}
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      background: var(--bg); color: var(--text);
      font-family: var(--sans); min-height: 100vh;
      padding: 0; overflow-x: hidden;
    }}

    /* ── Noise texture overlay ── */
    body::before {{
      content: ''; position: fixed; inset: 0; z-index: 0; pointer-events: none;
      background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.04'/%3E%3C/svg%3E");
      opacity: 0.4;
    }}

    /* ── Glow blobs ── */
    .blob {{
      position: fixed; border-radius: 50%; filter: blur(120px); pointer-events: none; z-index: 0;
    }}
    .blob-1 {{ width: 600px; height: 600px; top: -200px; left: -100px;
      background: radial-gradient(circle, rgba(99,102,241,.12), transparent 70%); }}
    .blob-2 {{ width: 500px; height: 500px; bottom: -100px; right: -100px;
      background: radial-gradient(circle, rgba(6,182,212,.08), transparent 70%); }}

    .wrap {{ position: relative; z-index: 1; max-width: 1200px; margin: 0 auto; padding: 2rem 1.5rem; }}

    /* ── Header ── */
    header {{
      display: flex; align-items: center; justify-content: space-between;
      padding: 1.5rem 2rem; margin-bottom: 2rem;
      background: rgba(13,16,24,.8); backdrop-filter: blur(20px);
      border-bottom: 1px solid var(--border); position: sticky; top: 0; z-index: 100;
    }}
    .brand {{ display: flex; align-items: center; gap: 1rem; }}
    .brand-icon {{
      width: 38px; height: 38px; border-radius: 10px;
      background: linear-gradient(135deg, var(--accent), var(--accent2));
      display: flex; align-items: center; justify-content: center; font-size: 1.1rem;
      box-shadow: 0 0 20px rgba(99,102,241,.3);
    }}
    .brand-name {{ font-size: 1.1rem; font-weight: 800; letter-spacing: -.5px; }}
    .brand-name span {{ color: var(--accent); }}
    .brand-sub {{ font-size: .65rem; color: var(--muted); font-family: var(--mono); margin-top: 1px; }}
    .header-right {{ display: flex; align-items: center; gap: 1rem; }}
    .live-pill {{
      display: flex; align-items: center; gap: 6px;
      background: rgba(0,217,139,.08); border: 1px solid rgba(0,217,139,.2);
      padding: 4px 10px; border-radius: 20px;
      font-size: .7rem; font-family: var(--mono); color: var(--ok);
    }}
    .live-dot {{ width: 6px; height: 6px; border-radius: 50%; background: var(--ok); animation: pulse 2s infinite; }}
    @keyframes pulse {{ 0%,100% {{ opacity:1; transform:scale(1); }} 50% {{ opacity:.4; transform:scale(.8); }} }}
    .time-chip {{ font-family: var(--mono); font-size: .68rem; color: var(--muted); }}

    /* ── KPI cards ── */
    .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 1rem; margin-bottom: 1.5rem; }}
    .kpi {{
      background: var(--s1); border: 1px solid var(--border); border-radius: 16px;
      padding: 1.3rem; position: relative; overflow: hidden;
      transition: border-color .2s, transform .2s;
    }}
    .kpi:hover {{ border-color: var(--border2); transform: translateY(-2px); }}
    .kpi::after {{
      content: ''; position: absolute; inset: 0; border-radius: 16px;
      background: linear-gradient(135deg, rgba(99,102,241,.04), transparent);
      pointer-events: none;
    }}
    .kpi-label {{ font-size: .65rem; font-family: var(--mono); color: var(--muted); text-transform: uppercase; letter-spacing: 1.5px; }}
    .kpi-val {{
      font-size: 2.4rem; font-weight: 800; margin-top: .3rem; font-family: var(--mono);
      background: linear-gradient(135deg, var(--text), var(--muted));
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }}
    .kpi-val.accent {{ background: linear-gradient(135deg, var(--accent), var(--accent2)); -webkit-background-clip: text; }}
    .kpi-val.cyan   {{ background: linear-gradient(135deg, var(--accent3), var(--accent));  -webkit-background-clip: text; }}
    .kpi-sub {{ font-size: .65rem; color: var(--muted); margin-top: .2rem; font-family: var(--mono); }}

    /* ── Status panels ── */
    .panels {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1.5rem; }}
    @media(max-width: 640px) {{ .panels {{ grid-template-columns: 1fr; }} }}
    .panel {{
      background: var(--s1); border: 1px solid var(--border); border-radius: 16px; padding: 1.5rem;
    }}
    .panel-hdr {{
      display: flex; align-items: center; gap: .5rem;
      font-family: var(--mono); font-size: .68rem; text-transform: uppercase;
      letter-spacing: 1.5px; color: var(--muted); margin-bottom: 1.2rem;
      padding-bottom: .8rem; border-bottom: 1px solid var(--border);
    }}
    .srow {{
      display: flex; justify-content: space-between; align-items: center;
      padding: .5rem 0; border-bottom: 1px solid rgba(255,255,255,.03); font-size: .84rem;
    }}
    .srow:last-child {{ border-bottom: none; }}
    .srow .lbl {{ color: var(--muted); font-size: .8rem; }}
    .chip {{
      font-family: var(--mono); font-size: .72rem;
      background: var(--s3); border: 1px solid var(--border2);
      padding: 2px 8px; border-radius: 6px; color: var(--text);
    }}
    .ok  {{ color: var(--ok);  font-weight: 700; }}
    .warn {{ color: var(--warn); font-weight: 700; }}
    .bad  {{ color: var(--bad);  font-weight: 700; }}
    .lat-bar {{ height: 2px; background: var(--border); border-radius: 2px; margin-top: 1rem; overflow: hidden; }}
    .lat-fill {{ height: 100%; border-radius: 2px; transition: width .6s; }}

    /* ── Action buttons ── */
    .actions {{ display: flex; gap: .75rem; flex-wrap: wrap; margin-bottom: 2rem; }}
    .btn {{
      display: inline-flex; align-items: center; gap: .5rem;
      padding: .6rem 1.2rem; border-radius: 10px; font-weight: 700;
      font-size: .8rem; text-decoration: none; transition: all .2s;
      border: 1px solid transparent; font-family: var(--sans);
    }}
    .btn-primary {{
      background: linear-gradient(135deg, var(--accent), var(--accent2));
      color: #fff; box-shadow: 0 4px 20px rgba(99,102,241,.25);
    }}
    .btn-primary:hover {{ transform: translateY(-2px); box-shadow: 0 6px 28px rgba(99,102,241,.35); }}
    .btn-ghost {{ border-color: var(--border); color: var(--text); background: var(--s1); }}
    .btn-ghost:hover {{ border-color: var(--accent); color: var(--accent); background: var(--s2); }}

    /* ── Tables ── */
    .section {{ margin-bottom: 2rem; }}
    .section-hdr {{
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: .85rem;
    }}
    .section-title {{
      font-family: var(--mono); font-size: .68rem; text-transform: uppercase;
      letter-spacing: 1.5px; color: var(--muted); display: flex; align-items: center; gap: .5rem;
    }}
    .count-badge {{
      background: var(--s3); border: 1px solid var(--border2); border-radius: 10px;
      padding: 1px 7px; font-size: .6rem; color: var(--muted);
    }}
    .table-card {{
      background: var(--s1); border: 1px solid var(--border); border-radius: 16px; overflow: hidden;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: .82rem; }}
    thead tr {{ background: var(--s2); }}
    th {{
      padding: .7rem 1rem; text-align: left;
      font-family: var(--mono); font-size: .62rem;
      text-transform: uppercase; letter-spacing: 1px; color: var(--muted);
      font-weight: 600; border-bottom: 1px solid var(--border);
    }}
    td {{ padding: .65rem 1rem; border-top: 1px solid rgba(255,255,255,.03); }}
    tbody tr {{ transition: background .15s; }}
    tbody tr:hover td {{ background: rgba(255,255,255,.02); }}
    code {{
      font-family: var(--mono); font-size: .73rem; color: var(--accent3);
      background: rgba(6,182,212,.08); padding: 1px 6px; border-radius: 4px;
    }}
    .pill {{
      display: inline-flex; gap: 4px; font-size: .65rem; font-family: var(--mono); color: var(--muted);
    }}
    .pill span {{
      background: var(--s3); border: 1px solid var(--border2);
      padding: 0px 5px; border-radius: 4px; color: var(--text);
    }}
    .pos-top {{
      background: rgba(99,102,241,.15); border: 1px solid rgba(99,102,241,.3);
      color: var(--accent); font-size: .6rem; font-family: var(--mono);
      padding: 1px 6px; border-radius: 4px; font-weight: 700;
    }}
    .dim {{ color: var(--dim); }}
    .empty {{
      text-align: center; color: var(--muted); padding: 2rem !important;
      font-size: .8rem; font-family: var(--mono);
    }}

    /* ── Footer ── */
    footer {{
      text-align: center; color: var(--muted); font-size: .68rem;
      font-family: var(--mono); padding: 2rem 0 1rem;
      border-top: 1px solid var(--border); margin-top: 1rem;
    }}
    footer a {{ color: var(--accent); text-decoration: none; }}
  </style>
</head>
<body>
<div class="blob blob-1"></div>
<div class="blob blob-2"></div>

<header>
  <div class="brand">
    <div class="brand-icon">🎛</div>
    <div>
      <div class="brand-name">SC<span>Files</span></div>
      <div class="brand-sub">backend manager</div>
    </div>
  </div>
  <div class="header-right">
    <div class="live-pill"><div class="live-dot"></div>LIVE</div>
    <div class="time-chip">{now.strftime("%H:%M:%S IST")}</div>
  </div>
</header>

<div class="wrap">

  <!-- KPI row -->
  <div class="kpi-grid">
    <div class="kpi">
      <div class="kpi-label">Movies</div>
      <div class="kpi-val accent">{len(movies)}</div>
      <div class="kpi-sub">{total_dls} download links</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Series</div>
      <div class="kpi-val accent">{len(series)}</div>
      <div class="kpi-sub">{total_eps} total episodes</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Collections</div>
      <div class="kpi-val accent">{len(cols)}</div>
      <div class="kpi-sub">curated playlists</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Backend Latency</div>
      <div class="kpi-val cyan">{b_ms:.0f}<span style="font-size:1.2rem;-webkit-text-fill-color:var(--muted)">ms</span></div>
      <div class="kpi-sub">last measured</div>
    </div>
  </div>

  <!-- Status panels -->
  <div class="panels">
    <div class="panel">
      <div class="panel-hdr">🤖 Bot Health</div>
      <div class="srow"><span class="lbl">Status</span><span class="ok">● ONLINE</span></div>
      <div class="srow"><span class="lbl">Uptime</span><span class="chip">{uptime}</span></div>
      <div class="srow"><span class="lbl">Last Backup</span><span class="chip">{bk}</span></div>
      <div class="srow"><span class="lbl">Last Ping</span><span class="chip">{pg}</span></div>
      <div class="srow"><span class="lbl">Backup Chat</span><span class="chip">{BACKUP_TARGET or '—'}</span></div>
    </div>
    <div class="panel">
      <div class="panel-hdr">🌐 Backend Health</div>
      <div class="srow"><span class="lbl">Status</span><span class="{s_cls}">● {b_status.upper()}</span></div>
      <div class="srow"><span class="lbl">HTTP Code</span><span class="chip">{b_code}</span></div>
      <div class="srow"><span class="lbl">Latency</span><span class="chip">{b_ms:.0f}ms</span></div>
      <div class="srow"><span class="lbl">Endpoint</span>
        <span class="chip" style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block">{BACKEND_URL}</span>
      </div>
      {err_row}
      <div class="lat-bar"><div class="lat-fill" style="width:{lat_w}%;background:{lat_c}"></div></div>
    </div>
  </div>

  <!-- Actions -->
  <div class="actions">
    <a class="btn btn-primary" href="/backup/all">📦 Download Backup ZIP</a>
    <a class="btn btn-ghost"   href="/logs">📋 View Logs</a>
    <a class="btn btn-ghost"   href="/health">📡 JSON API</a>
    <a class="btn btn-ghost"   href="javascript:location.reload()">🔄 Refresh</a>
  </div>

  <!-- Movies table -->
  <div class="section">
    <div class="section-hdr">
      <div class="section-title">🎬 Recent Movies <span class="count-badge">{len(movies)}</span></div>
    </div>
    <div class="table-card">
      <table>
        <thead><tr><th>ID</th><th>TMDB</th><th>Extras</th><th>Downloads</th><th>Pos</th></tr></thead>
        <tbody>{mv_rows or empty}</tbody>
      </table>
    </div>
  </div>

  <!-- Series table -->
  <div class="section">
    <div class="section-hdr">
      <div class="section-title">📺 Recent Series <span class="count-badge">{len(series)}</span></div>
    </div>
    <div class="table-card">
      <table>
        <thead><tr><th>ID</th><th>TMDB</th><th>Seasons</th><th>Episodes</th></tr></thead>
        <tbody>{sr_rows or empty}</tbody>
      </table>
    </div>
  </div>

  <!-- Collections table -->
  <div class="section">
    <div class="section-hdr">
      <div class="section-title">🗂 Collections <span class="count-badge">{len(cols)}</span></div>
    </div>
    <div class="table-card">
      <table>
        <thead><tr><th>ID</th><th>Name</th><th>Movies</th></tr></thead>
        <tbody>{co_rows or empty}</tbody>
      </table>
    </div>
  </div>

  <footer>
    SCFiles Bot Dashboard &nbsp;·&nbsp; Auto-refresh every 60s
    &nbsp;·&nbsp; {now.strftime("%d %b %Y, %H:%M:%S IST")}
  </footer>
</div>

<script>
  // Auto-refresh countdown
  let t = 60;
  const tick = () => {{
    t--;
    if (t <= 0) location.reload();
    setTimeout(tick, 1000);
  }};
  tick();
  
  // Animate KPI values on load
  document.querySelectorAll('.kpi-val').forEach(el => {{
    const target = parseInt(el.textContent);
    if (isNaN(target) || target > 9999) return;
    let current = 0;
    const step = target / 30;
    const timer = setInterval(() => {{
      current = Math.min(current + step, target);
      el.textContent = Math.floor(current);
      if (current >= target) clearInterval(timer);
    }}, 30);
  }});
</script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")

async def web_health(req: web.Request) -> web.Response:
    now = datetime.now(IST); backend = {"status":"offline","http_status":None,"latency_ms":None,"error":None}
    try:
        s = await sess(); t0 = datetime.now()
        async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
            backend["http_status"]=r.status; backend["latency_ms"]=round((datetime.now()-t0).total_seconds()*1000,2)
            backend["status"]="online" if r.status==200 else "degraded"
    except Exception as e: backend["error"]=str(e)
    mv=await api_get("/api/movies") or []; sr=await api_get("/api/series") or []; co=await api_get("/api/collections") or {}
    total_eps=sum(len(ep.get("episodes",[])) for s in sr for ep in s.get("seasons",[]))
    return web.json_response({
        "bot": {"status":"online","uptime_seconds":int((now-BOT_STARTED_AT).total_seconds()),
                "last_backup_at":LAST_BACKUP_AT.isoformat() if LAST_BACKUP_AT else None,
                "last_ping_at":LAST_PING_AT.isoformat() if LAST_PING_AT else None},
        "backend": backend,
        "db": {"movies":len(mv),"series":len(sr),"collections":len(co),"total_episodes":total_eps},
        "time": now.isoformat()
    })

async def web_backup_zip(req: web.Request) -> web.Response:
    data, ts = await make_zip()
    return web.Response(body=data, headers={"Content-Type":"application/zip",
        "Content-Disposition":f'attachment; filename="backup_all_{ts}.zip"'})

async def web_logs(req: web.Request) -> web.Response:
    if not os.path.exists(LOG_FILE):
        return web.Response(text="No log file yet.", content_type="text/plain")
    with open(LOG_FILE,"rb") as f:
        f.seek(0,2); size=f.tell(); f.seek(max(0,size-65536)); tail=f.read()
    return web.Response(body=tail, headers={"Content-Type":"text/plain; charset=utf-8",
        "Content-Disposition":"inline; filename=bot.log"})

# ── Scheduler ──────────────────────────────────────────────────────────────────
async def job_backup(app: Application):
    ok, info = await do_backup(app.bot)
    logger.info("Scheduled backup → ok=%s info=%s", ok, info)

async def job_ping():
    global LAST_PING_AT
    urls = [BACKEND_URL]
    if BOT_WEB_URL: urls.append(f"{BOT_WEB_URL}/health")
    s = await sess()
    for url in urls:
        try:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                logger.info("Ping %s → %s", url, r.status)
        except Exception as e: logger.warning("Ping %s: %s", url, e)
    LAST_PING_AT = datetime.now(IST)

# ── Main ───────────────────────────────────────────────────────────────────────
async def main():
    global BACKUP_TARGET
    BACKUP_TARGET = load_backup_target()
    logger.info("="*60)
    logger.info("SCFiles Bot starting…")
    logger.info("Backend : %s", BACKEND_URL)
    logger.info("Admins  : %s", ADMIN_IDS or "ALL")
    logger.info("Backup  : %s", BACKUP_TARGET or "NOT SET")
    logger.info("="*60)

    # Web server
    web_app = web.Application()
    web_app.router.add_get("/",           web_dashboard)
    web_app.router.add_get("/health",     web_health)
    web_app.router.add_get("/backup/all", web_backup_zip)
    web_app.router.add_get("/logs",       web_logs)
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, WEB_HOST, WEB_PORT).start()
    logger.info("Web dashboard on %s:%s", WEB_HOST, WEB_PORT)

    # Bot
    app = Application.builder().token(BOT_TOKEN).build()

    await app.bot.set_my_commands([BotCommand(c,d) for c,d in [
        ("start","Main menu"),("help","All commands"),("status","Server health"),
        ("stats","DB statistics"),("movies","List movies"),("series","List series"),
        ("collections","List collections"),("addmovie","Add movie"),
        ("addseries","Add series"),("addcollection","Add collection"),
        ("editmovie","Edit movie field"),("delmovie","Delete movie"),
        ("delseries","Delete series"),("delcollection","Delete collection"),
        ("tmdb","TMDB metadata search"),("backup","Send backup files"),
        ("backupzip","Backup as ZIP"),("setbackup","Set backup channel"),
        ("logs","View bot logs"),("cancel","Cancel operation"),
    ]])

    # Conversations
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("addmovie", cmd_addmovie)],
        states={AM_TMDB:[MessageHandler(filters.TEXT & ~filters.COMMAND, am_tmdb)],
                AM_EXTRA:[MessageHandler(filters.TEXT & ~filters.COMMAND, am_extra)],
                AM_DL480:[MessageHandler(filters.TEXT & ~filters.COMMAND, am_dl480)],
                AM_DL720:[MessageHandler(filters.TEXT & ~filters.COMMAND, am_dl720)],
                AM_DL1080:[MessageHandler(filters.TEXT & ~filters.COMMAND, am_dl1080)],
                AM_POS:[CallbackQueryHandler(am_pos_cb, pattern="^pos_")],
                AM_CONFIRM:[MessageHandler(filters.TEXT & ~filters.COMMAND, am_confirm)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("addseries", cmd_addseries)],
        states={AS_TMDB:[MessageHandler(filters.TEXT & ~filters.COMMAND, as_tmdb)],
                AS_JSON:[MessageHandler(filters.TEXT & ~filters.COMMAND, as_json)],
                AS_CONFIRM:[MessageHandler(filters.TEXT & ~filters.COMMAND, as_confirm)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("addcollection", cmd_addcollection)],
        states={AC_ID:[MessageHandler(filters.TEXT & ~filters.COMMAND, ac_id)],
                AC_NAME:[MessageHandler(filters.TEXT & ~filters.COMMAND, ac_name)],
                AC_BANNER:[MessageHandler(filters.TEXT & ~filters.COMMAND, ac_banner)],
                AC_BGMUSIC:[MessageHandler(filters.TEXT & ~filters.COMMAND, ac_bgmusic)],
                AC_MOVIES:[MessageHandler(filters.TEXT & ~filters.COMMAND, ac_movies)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("delmovie", cmd_delmovie)],
        states={DM_ID:[MessageHandler(filters.TEXT & ~filters.COMMAND, dm_id)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("delseries", cmd_delseries)],
        states={DS_ID:[MessageHandler(filters.TEXT & ~filters.COMMAND, ds_id)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("delcollection", cmd_delcollection)],
        states={DC_ID:[MessageHandler(filters.TEXT & ~filters.COMMAND, dc_id)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("editmovie", cmd_editmovie)],
        states={EM_ID:[MessageHandler(filters.TEXT & ~filters.COMMAND, em_id)],
                EM_VALUE:[CallbackQueryHandler(em_field_cb, pattern="^ef_"),
                          MessageHandler(filters.TEXT & ~filters.COMMAND, em_value)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("tmdb", cmd_tmdb)],
        states={TQ_TYPE:[CallbackQueryHandler(tq_type_cb, pattern="^tq_")],
                TQ_QUERY:[MessageHandler(filters.TEXT & ~filters.COMMAND, tq_query)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))

    # Simple commands
    for cmd, fn in [("start",cmd_start),("help",cmd_help),("cancel",cmd_cancel),
                    ("status",cmd_status),("stats",cmd_stats),("movies",cmd_movies),
                    ("series",cmd_series),("collections",cmd_collections),("logs",cmd_logs),
                    ("backup",cmd_backup),("backupzip",cmd_backupzip),("setbackup",cmd_setbackup)]:
        app.add_handler(CommandHandler(cmd, fn))

    # Callbacks
    app.add_handler(CallbackQueryHandler(menu_cb,       pattern="^m_"))
    app.add_handler(CallbackQueryHandler(del_confirm_cb, pattern="^delconfirm_"))
    app.add_handler(CallbackQueryHandler(del_cancel_cb,  pattern="^delcancel"))

    # Scheduler
    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(job_backup, "interval", days=2, args=[app])
    scheduler.add_job(job_ping,   "interval", minutes=AUTO_PING_MIN,
                      next_run_time=datetime.now(IST)+timedelta(seconds=30))
    scheduler.start()
    logger.info("Scheduler: backup every 2d, ping every %dm", AUTO_PING_MIN)

    try:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
        logger.info("Bot polling started ✅")
        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down…")
        await app.updater.stop(); await app.stop(); await app.shutdown()
        await runner.cleanup(); scheduler.shutdown(wait=False)
        global _SESSION
        if _SESSION and not _SESSION.closed: await _SESSION.close()

if __name__ == "__main__":
    asyncio.run(main())
