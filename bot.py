"""
SCFiles Backend Manager Bot — Pyrogram Edition
"""

import os
import json
import io
import logging
import zipfile
import aiohttp
from datetime import datetime, timedelta

import aiohttp
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters
)
from telegram.constants import ParseMode
from telegram.helpers import escape_markdown

LOG_FILE = os.environ.get("LOG_FILE", "bot.log")
_log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    _stream_handler = logging.StreamHandler()
    _stream_handler.setFormatter(logging.Formatter(_log_format))
    _file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    _file_handler.setFormatter(logging.Formatter(_log_format))
    logger.addHandler(_stream_handler)
    logger.addHandler(_file_handler)

# ──────────────────────────── CONFIG ────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
BACKEND_URL      = os.environ["BACKEND_URL"].rstrip("/")
TMDB_API_KEY     = os.environ["TMDB_API_KEY"]
ADMIN_IDS        = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
BACKUP_CHAT_ID   = os.environ.get("BACKUP_CHAT_ID", "")      # legacy fallback
BACKUP_CONFIG_FILE = os.environ.get("BACKUP_CONFIG_FILE", ".backup_config.json")
WEB_HOST         = os.environ.get("WEB_HOST", "0.0.0.0")
WEB_PORT         = int(os.environ.get("WEB_PORT", "8080"))
BOT_WEB_URL      = os.environ.get("BOT_WEB_URL", "").rstrip("/")
AUTO_PING_INTERVAL_MIN = int(os.environ.get("AUTO_PING_INTERVAL_MIN", "5"))

TMDB_BASE        = "https://api.themoviedb.org/3"
TMDB_IMG         = "https://image.tmdb.org/t/p/w500"
BOT_STARTED_AT   = datetime.now()
LAST_BACKUP_AT   = None
LAST_AUTO_PING_AT = None
BACKUP_CHAT_TARGET = BACKUP_CHAT_ID
BOT_COMMANDS = [
    ("start", "Main menu"),
    ("help", "Show available commands"),
    ("status", "Bot + backend health details"),
    ("stats", "Database statistics"),
    ("movies", "List recent movies"),
    ("series", "List recent series"),
    ("collections", "List collections"),
    ("addmovie", "Add a movie (admin)"),
    ("addseries", "Add a series (admin)"),
    ("addcollection", "Add a collection (admin)"),
    ("editmovie", "Edit a movie field (admin)"),
    ("delmovie", "Delete a movie (admin)"),
    ("delseries", "Delete a series (admin)"),
    ("delcollection", "Delete a collection (admin)"),
    ("tmdb", "Search TMDB metadata (admin)"),
    ("backup", "Run manual backup (admin)"),
    ("backupall", "Download all data as ZIP (admin)"),
    ("setbackup", "Set backup channel/chat ID (admin)"),
    ("logs", "Get recent bot logs (admin)"),
    ("cancel", "Cancel current operation"),
]

# ──────────────────────────── STATES ────────────────────────────
(
    ADD_MOVIE_TMDB, ADD_MOVIE_EXTRA, ADD_MOVIE_DL480, ADD_MOVIE_DL720,
    ADD_MOVIE_DL1080, ADD_MOVIE_CONFIRM,
    ADD_SERIES_TMDB, ADD_SERIES_EPISODES, ADD_SERIES_CONFIRM,
    ADD_COL_ID, ADD_COL_NAME, ADD_COL_BANNER, ADD_COL_CONFIRM,
    DEL_MOVIE_ID, DEL_SERIES_ID, DEL_COL_ID,
    SEARCH_TMDB_Q, SEARCH_TYPE,
    EDIT_MOVIE_ID, EDIT_FIELD, EDIT_VALUE,
) = range(21)

# ──────────────────────────── HELPERS ────────────────────────────
def is_admin(user_id: int) -> bool:
    return not ADMIN_IDS or user_id in ADMIN_IDS

def admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not is_admin(uid):
            await update.message.reply_text("⛔ Access denied.")
            return ConversationHandler.END
        return await func(update, ctx)
    wrapper.__name__ = func.__name__
    return wrapper

async def api_get(path: str) -> dict | list | None:
    try:
        s = await get_session()
        async with s.get(f"{BACKEND_URL}{path}", timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.json()
    except Exception as e:
        logger.error("API GET %s: %s", path, e)
        return None

async def api_post(path: str, data: dict):
    try:
        s = await get_session()
        async with s.post(f"{BACKEND_URL}{path}", json=data,
                          timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.json()
    except Exception as e:
        logger.error("API POST %s: %s", path, e)
        return None

async def api_delete(path: str):
    try:
        s = await get_session()
        async with s.delete(f"{BACKEND_URL}{path}",
                            timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.json()
    except Exception as e:
        logger.error("API DELETE %s: %s", path, e)
        return None

# ─────────────── TMDB helpers ───────────────
async def tmdb_movie(tid: int) -> dict | None:
    try:
        s = await get_session()
        async with s.get(
            f"{TMDB_BASE}/movie/{tid}?api_key={TMDB_API_KEY}&language=en-US",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            return await r.json() if r.status == 200 else None
    except Exception as e:
        logger.error("TMDB movie %s: %s", tid, e)
        return None

async def tmdb_tv(tid: int) -> dict | None:
    try:
        s = await get_session()
        async with s.get(
            f"{TMDB_BASE}/tv/{tid}?api_key={TMDB_API_KEY}&language=en-US",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            return await r.json() if r.status == 200 else None
    except Exception as e:
        logger.error("TMDB tv %s: %s", tid, e)
        return None

async def tmdb_search(query: str, media_type: str = "movie") -> list:
    try:
        s = await get_session()
        async with s.get(
            f"{TMDB_BASE}/search/{media_type}?api_key={TMDB_API_KEY}&query={query}&language=en-US",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status == 200:
                return (await r.json()).get("results", [])[:5]
    except Exception as e:
        logger.error("TMDB search: %s", e)
    return []

def fmt_movie(m: dict) -> str:
    title   = m.get("title", "N/A")
    year    = (m.get("release_date") or "")[:4]
    rating  = m.get("vote_average", 0)
    runtime = m.get("runtime", 0)
    genres  = ", ".join(g["name"] for g in m.get("genres", []))
    ov      = (m.get("overview") or "No overview.")[:300]
    return (
        f"🎬 **{title}** ({year})\n"
        f"⭐ {rating:.1f}/10  •  ⏱ {runtime} min\n"
        f"🎭 {genres or 'N/A'}\n\n"
        f"📝 {ov}"
    )

def fmt_tv(t: dict) -> str:
    name    = t.get("name", "N/A")
    year    = (t.get("first_air_date") or "")[:4]
    rating  = t.get("vote_average", 0)
    seasons = t.get("number_of_seasons", "?")
    eps     = t.get("number_of_episodes", "?")
    genres  = ", ".join(g["name"] for g in t.get("genres", []))
    ov      = (t.get("overview") or "No overview.")[:300]
    return (
        f"📺 **{name}** ({year})\n"
        f"⭐ {rating:.1f}/10  •  {seasons} seasons / {eps} eps\n"
        f"🎭 {genres or 'N/A'}\n\n"
        f"📝 {ov}"
    )

def poster_url(info: dict) -> str | None:
    p = info.get("poster_path")
    return f"{TMDB_IMG}{p}" if p else None

# ─────────────── Backup helpers ───────────────
async def _collect_payloads() -> dict[str, bytes]:
    result = {}
    for fname, path in [
        ("movies.json",      "/api/movies"),
        ("series.json",      "/api/series"),
        ("collections.json", "/api/collections"),
    ]:
        data = await api_get(path)
        if data is None:
            raise RuntimeError(f"Could not fetch {path}")
        result[fname] = json.dumps(data, indent=2, ensure_ascii=False).encode()
    return result

async def create_zip() -> tuple[bytes, str]:
    payloads = await _collect_payloads()
    ts  = datetime.now().strftime("%Y-%m-%d_%H-%M")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in payloads.items():
            zf.writestr(f"{ts}_{name}", data)
    buf.seek(0)
    return buf.read(), ts

async def perform_backup(bot: Client, target: str | int | None = None) -> tuple[bool, str]:
    global LAST_BACKUP_AT
    dest = str(target or BACKUP_TARGET).strip()
    if not dest:
        return False, "No backup chat configured. Use /setbackup <chat_id>"
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    try:
        await bot.send_message(int(dest), f"💾 **Auto-Backup** — {ts}")
        for fname, data in (await _collect_payloads()).items():
            buf = io.BytesIO(data)
            buf.name = f"{ts}_{fname}"
            await bot.send_document(int(dest), buf, caption=f"`{ts}_{fname}`")
        LAST_BACKUP_AT = datetime.now()
        logger.info("Backup done → %s", dest)
        return True, dest
    except Exception as e:
        logger.error("Backup failed → %s: %s", dest, e)
        return False, str(e)

# ─────────────── Pyrogram client ───────────────
# in_memory=True: no session file — avoids file-lock / permission issues on deployment
pyro = Client(
    "scfiles_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
)

def main_menu_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🎬 Movies",       callback_data="menu_movies"),
         InlineKeyboardButton("📺 Series",        callback_data="menu_series")],
        [InlineKeyboardButton("🗂 Collections",   callback_data="menu_cols"),
         InlineKeyboardButton("🔍 TMDB Search",   callback_data="menu_tmdb")],
        [InlineKeyboardButton("📊 Stats",         callback_data="menu_stats"),
         InlineKeyboardButton("🌐 Server Status", callback_data="menu_status")],
        [InlineKeyboardButton("💾 Backup Now", callback_data="menu_backup"),
         InlineKeyboardButton("📥 Backup All ZIP", callback_data="menu_backup_all")],
    ]
    if BOT_WEB_URL:
        kb.append([InlineKeyboardButton("🩺 Open Web Dashboard", url=BOT_WEB_URL)])
    await update.message.reply_text(
        "🎛 *SCFiles Backend Manager*\n\nChoose an action:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lines = [f"/{name} — {description}" for name, description in BOT_COMMANDS]
    text = "📖 *Available Commands*\n\n" + "\n".join(lines)
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def register_bot_commands(application: Application):
    await application.bot.set_my_commands(
        [BotCommand(command=name, description=description[:256]) for name, description in BOT_COMMANDS]
    )
    logger.info("Registered %s bot commands with Telegram.", len(BOT_COMMANDS))

def read_log_tail(limit_bytes: int = 32_768) -> bytes:
    if not os.path.exists(LOG_FILE):
        return b""
    with open(LOG_FILE, "rb") as fp:
        fp.seek(0, os.SEEK_END)
        size = fp.tell()
        fp.seek(max(0, size - limit_bytes))
        return fp.read()

def load_backup_chat_target() -> str:
    if os.path.exists(BACKUP_CONFIG_FILE):
        try:
            with open(BACKUP_CONFIG_FILE, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                val = str(data.get("backup_chat_id", "")).strip()
                if val:
                    return val
        except Exception as e:
            logger.warning("Failed to load %s: %s", BACKUP_CONFIG_FILE, e)
    return str(BACKUP_CHAT_ID).strip()

def save_backup_chat_target(chat_id: str):
    with open(BACKUP_CONFIG_FILE, "w", encoding="utf-8") as fp:
        json.dump({"backup_chat_id": str(chat_id)}, fp, indent=2)

# ──────────────────────────── SERVER STATUS ────────────────────────────
async def check_status(update_or_query, is_query=False):
    send = update_or_query.edit_message_text if is_query else update_or_query.message.reply_text
    now = datetime.now()
    uptime = now - BOT_STARTED_AT
    bot_health = "🟢 Online"
    try:
        start = now
        async with aiohttp.ClientSession() as s:
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                elapsed = (datetime.now() - start).total_seconds() * 1000
                status  = "🟢 Online" if r.status == 200 else f"🟡 Status {r.status}"
                body    = escape_markdown((await r.text())[:80], version=2)
                msg     = (
                    f"*Health Details*\n\n"
                    f"🤖 Bot: {bot_health}\n"
                    f"⏱ Bot Uptime: `{escape_markdown(str(uptime).split('.')[0], version=2)}`\n\n"
                    f"🖥 Backend: {status}\n"
                    f"🔗 `{escape_markdown(BACKEND_URL, version=2)}`\n"
                    f"⚡ Response: `{elapsed:.0f}ms`\n"
                    f"📨 Body: `{body[:80]}`\n"
                    f"🕐 Checked: `{escape_markdown(now.strftime('%Y-%m-%d %H:%M:%S'), version=2)}`"
                )
    except Exception as e:
        err = escape_markdown(str(e), version=2)
        msg = (
            f"*Health Details*\n\n"
            f"🤖 Bot: {bot_health}\n"
            f"⏱ Bot Uptime: `{escape_markdown(str(uptime).split('.')[0], version=2)}`\n\n"
            f"🖥 Backend: 🔴 Offline\n"
            f"🔗 `{escape_markdown(BACKEND_URL, version=2)}`\n"
            f"❗ Error: `{err}`\n"
            f"🕐 Checked: `{escape_markdown(now.strftime('%Y-%m-%d %H:%M:%S'), version=2)}`"
        )
    await send(msg, parse_mode=ParseMode.MARKDOWN_V2)

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await check_status(update)

# ──────────────────────────── STATS ────────────────────────────
async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    movies      = await api_get("/api/movies") or []
    series      = await api_get("/api/series") or []
    collections = await api_get("/api/collections") or {}
    msg = (
        f"📊 *Database Statistics*\n\n"
        f"🎬 Movies: *{len(movies)}*\n"
        f"📺 Series: *{len(series)}*\n"
        f"🗂 Collections: *{len(collections)}*\n\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

@pyro.on_message(filters.command("stats") & filters.private)
async def cmd_stats(_, msg: Message):
    movies = await api_get("/api/movies") or []
    series = await api_get("/api/series") or []
    cols   = await api_get("/api/collections") or {}
    await msg.reply(
        f"📊 **Database Stats**\n\n"
        f"🎬 Movies: **{len(movies)}**\n"
        f"📺 Series: **{len(series)}**\n"
        f"🗂 Collections: **{len(cols)}**\n\n"
        f"🕐 `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`"
    )

@pyro.on_message(filters.command("movies") & filters.private)
async def cmd_movies(_, msg: Message):
    items = await api_get("/api/movies?limit=15") or []
    if not items:
        return await msg.reply("No movies found.")
    lines = [
        f"• `{m.get('id','?')}` | TMDB `{m.get('tmdb_id','?')}` | {m.get('extras','') or '—'}"
        for m in items[:15]
    ]
    await msg.reply("🎬 **Recent Movies**\n\n" + "\n".join(lines))

@pyro.on_message(filters.command("series") & filters.private)
async def cmd_series_list(_, msg: Message):
    items = await api_get("/api/series?limit=15") or []
    if not items:
        return await msg.reply("No series found.")
    lines = [
        f"• `{s.get('id','?')}` | TMDB `{s.get('tmdb_id','?')}` | {len(s.get('seasons',[]))} season(s)"
        for s in items[:15]
    ]
    await msg.reply("📺 **Recent Series**\n\n" + "\n".join(lines))

@pyro.on_message(filters.command("collections") & filters.private)
async def cmd_collections(_, msg: Message):
    cols = await api_get("/api/collections") or {}
    if not cols:
        return await msg.reply("No collections found.")
    lines = [
        f"• `{k}` — {v.get('name','?')} ({len(v.get('movies',[]))} movies)"
        for k, v in list(cols.items())[:15]
    ]
    await msg.reply("🗂 **Collections**\n\n" + "\n".join(lines))

@pyro.on_message(filters.command("logs") & filters.private)
async def cmd_logs(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    if not os.path.exists(LOG_FILE):
        return await msg.reply("📭 No log file yet.")
    with open(LOG_FILE, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        f.seek(max(0, size - 8192))
        tail = f.read()
    buf = io.BytesIO(tail)
    ts  = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    buf.name = f"scfiles_logs_{ts}.txt"
    await msg.reply_document(buf, caption=f"📋 Last {len(tail)//1024 or 1}KB of logs · {ts}")

@pyro.on_message(filters.command("backup") & filters.private)
async def cmd_backup(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    m = await msg.reply("💾 Starting backup…")
    dest = BACKUP_TARGET or str(msg.chat.id)
    ok, info = await perform_backup(pyro, dest)
    await m.edit_text(
        f"✅ Backup sent to `{info}`." if ok else f"❌ Backup failed:\n`{info}`"
    )

@pyro.on_message(filters.command("backupzip") & filters.private)
async def cmd_backupzip(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    m = await msg.reply("📦 Building ZIP…")
    try:
        data, ts = await create_zip()
        buf = io.BytesIO(data)
        buf.name = f"backup_all_{ts}.zip"
        await msg.reply_document(buf, caption=f"✅ Backup ZIP `{ts}`")
        await m.delete()
    except Exception as e:
        await m.edit_text(f"❌ ZIP failed:\n`{e}`")

@pyro.on_message(filters.command("setbackup") & filters.private)
async def cmd_setbackup(_, msg: Message):
    global BACKUP_TARGET
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.reply(
            f"📦 Current backup chat: `{BACKUP_TARGET or 'Not set'}`\n\n"
            f"Usage: `/setbackup <chat_id>`"
        )
    BACKUP_TARGET = parts[1].strip()
    save_backup_target(BACKUP_TARGET)
    await msg.reply(f"✅ Backup chat set to `{BACKUP_TARGET}`.")

# ── Conversation starters ────────────────────────────────────

@pyro.on_message(filters.command("addmovie") & filters.private)
async def cmd_addmovie(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_ADD_MOVIE_TMDB)
    await msg.reply("🎬 **Add Movie**\n\nEnter the **TMDB Movie ID**:")

@pyro.on_message(filters.command("addseries") & filters.private)
async def cmd_addseries(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_ADD_SERIES_TMDB)
    await msg.reply("📺 **Add Series**\n\nEnter the **TMDB TV Show ID**:")

@pyro.on_message(filters.command("addcollection") & filters.private)
async def cmd_addcollection(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_ADD_COL_ID)
    await msg.reply(
        "🗂 **Add Collection**\n\n"
        "Steps: ID → Name → Banner → BG Music → Movies JSON\n\n"
        "Enter collection **ID** (slug, e.g. `vijay`):"
    )

@pyro.on_message(filters.command("delmovie") & filters.private)
async def cmd_delmovie(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_DEL_MOVIE)
    await msg.reply("🗑 Enter the **movie ID** to delete:")

@pyro.on_message(filters.command("delseries") & filters.private)
async def cmd_delseries(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_DEL_SERIES)
    await msg.reply("🗑 Enter the **series ID** to delete:")

@pyro.on_message(filters.command("delcollection") & filters.private)
async def cmd_delcollection(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_DEL_COL)
    await msg.reply("🗑 Enter the **collection ID** to delete:")

@pyro.on_message(filters.command("editmovie") & filters.private)
async def cmd_editmovie(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_EDIT_MOVIE_ID)
    await msg.reply("✏️ Enter the **movie ID** to edit:")

@pyro.on_message(filters.command("tmdb") & filters.private)
async def cmd_tmdb(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🎬 Movie",   callback_data="tmdb_movie"),
        InlineKeyboardButton("📺 TV Show", callback_data="tmdb_tv"),
    ]])
    set_state(msg.from_user.id, S_TMDB_QUERY, tmdb_type="movie")
    await msg.reply("🔍 **TMDB Search** — What type?", reply_markup=kb)

# ═══════════════════════════════════════
#  UNIVERSAL TEXT HANDLER (state machine)
# ═══════════════════════════════════════
@pyro.on_message(filters.text & filters.private)
async def on_text(_, msg: Message):
    # Guard: from_user can be None for channel posts
    if not msg.from_user:
        return

    uid  = msg.from_user.id
    text = (msg.text or "").strip()

    # Commands are already handled above; Pyrogram gives them priority
    if text.startswith("/"):
        return

    state = get_state(uid)
    if state is None:
        return

    logger.info("State [%s] uid=%s text=%r", state, uid, text[:60])

    try:
        await _dispatch(msg, uid, state, text)
    except Exception as e:
        logger.exception("State handler [%s] crashed: %s", state, e)
        await msg.reply(f"⚠️ Error: `{e}`\n\nUse /cancel to reset.")


async def _dispatch(msg: Message, uid: int, state: str, text: str):
    d = get_data(uid)   # current conversation data (mutable dict reference)

    # ── ADD MOVIE ──────────────────────────────────────────────────────
    if state == S_ADD_MOVIE_TMDB:
        if not text.isdigit():
            return await msg.reply("❌ Enter a valid numeric TMDB ID.")
        tid  = int(text)
        info = await tmdb_movie(tid)
        mdata = {"tmdb_id": tid, "downloads": {}, "subtitles": {}}
        if info:
            mdata["id"] = (info.get("title") or "").lower().replace(" ", "-").replace("'", "")
            p       = poster_url(info)
            caption = (
                fmt_movie(info)
                + f"\n\nSuggested ID: `{mdata['id']}`"
                + "\n\nEnter **extras** (e.g. `PreDVD - Tamil Audio`) or `-` to skip:"
            )
            if p:
                await msg.reply_photo(p, caption=caption)
            else:
                await msg.reply(caption)
        else:
            full = await tmdb_tv(r["id"])
            if full:
                msg    = fmt_tmdb_tv(full) + f"\n\n🆔 TMDB ID: `{full['id']}`"
                poster = poster_url(full.get("poster_path"))
                if poster:
                    await update.message.reply_photo(poster, caption=msg, parse_mode=ParseMode.MARKDOWN)
                else:
                    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ──────────────────────────── BACKUP ────────────────────────────
async def perform_backup(app: Application, target_chat_id: str | int | None = None) -> tuple[bool, str]:
    """Fetch all data and send as JSON files to backup chat/channel."""
    global LAST_BACKUP_AT
    resolved_target = str(target_chat_id or BACKUP_CHAT_TARGET).strip()
    if not resolved_target:
        logger.warning("Backup channel not set, skipping backup.")
        return False, "Backup chat/channel is not configured."
    endpoints = {
        "movies.json":      "/api/movies",
        "series.json":      "/api/series",
        "collections.json": "/api/collections",
    }
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    try:
        await app.bot.send_message(resolved_target, f"💾 *Auto-Backup* — {ts}", parse_mode=ParseMode.MARKDOWN)
        for filename, path in endpoints.items():
            data = await api_get(path)
            if data is not None:
                content = json.dumps(data, indent=2, ensure_ascii=False).encode()
                fname   = f"{ts}_{filename}"
                await app.bot.send_document(
                    resolved_target,
                    document=content,
                    filename=fname,
                    caption=f"📦 `{fname}`",
                    parse_mode=ParseMode.MARKDOWN,
                )
        LAST_BACKUP_AT = datetime.now()
        logger.info("Backup completed at %s and sent to %s", ts, resolved_target)
        return True, resolved_target
    except Exception as e:
        logger.error("Backup failed for target %s: %s", resolved_target, e)
        return False, str(e)

async def collect_backup_payloads() -> dict[str, bytes]:
    endpoints = {
        "movies.json": "/api/movies",
        "series.json": "/api/series",
        "collections.json": "/api/collections",
    }
    payloads: dict[str, bytes] = {}
    for filename, path in endpoints.items():
        data = await api_get(path)
        if data is None:
            raise RuntimeError(f"Unable to fetch {path}")
        payloads[filename] = json.dumps(data, indent=2, ensure_ascii=False).encode()
    return payloads

async def create_backup_zip_bytes() -> tuple[bytes, str]:
    payloads = await collect_backup_payloads()
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname, content in payloads.items():
            zf.writestr(f"{ts}_{fname}", content)
    zip_buffer.seek(0)
    return zip_buffer.read(), ts

async def auto_ping_services():
    global LAST_AUTO_PING_AT
    targets = [("backend", BACKEND_URL)]
    if BOT_WEB_URL:
        targets.append(("bot", f"{BOT_WEB_URL}/health"))
    try:
        async with aiohttp.ClientSession() as s:
            for name, url in targets:
                try:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        logger.info("Auto ping %s => %s (%s)", name, r.status, url)
                except Exception as e:
                    logger.warning("Auto ping failed for %s (%s): %s", name, url, e)
    finally:
        LAST_AUTO_PING_AT = datetime.now()

async def web_health_handler(request: web.Request) -> web.Response:
    now = datetime.now()
    uptime = str((now - BOT_STARTED_AT)).split(".")[0]
    backend_status = "offline"
    backend_code = "N/A"
    backend_latency = "N/A"
    backend_latency_ms = 0.0
    backend_error = ""
    try:
        start = datetime.now()
        async with aiohttp.ClientSession() as s:
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                backend_code = str(r.status)
                backend_latency_ms = (datetime.now() - start).total_seconds() * 1000
                backend_latency = f"{backend_latency_ms:.0f}ms"
                backend_status = "online" if r.status == 200 else "degraded"
    except Exception as exc:
        backend_error = str(exc)

    backup_text = LAST_BACKUP_AT.strftime("%Y-%m-%d %H:%M:%S") if LAST_BACKUP_AT else "Never"
    ping_text = LAST_AUTO_PING_AT.strftime("%Y-%m-%d %H:%M:%S") if LAST_AUTO_PING_AT else "Never"
    movies = await api_get("/api/movies") or []
    series = await api_get("/api/series") or []
    collections = await api_get("/api/collections") or {}
    latency_width = min(max(int(backend_latency_ms / 10), 5), 100) if backend_latency_ms else 5
    latency_color = "var(--ok)" if backend_latency_ms and backend_latency_ms < 400 else ("var(--warn)" if backend_latency_ms < 1000 else "var(--bad)")
    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>SCFiles | System Health</title>
    <style>
      :root {{
        --bg: #080c14; --card: #111b2d; --card-hover: #16243d; --border: #1f2a44;
        --text: #f0f4f8; --muted: #8a9ab5; --ok: #10b981; --warn: #fbbf24; --bad: #ef4444;
        --accent: #3b82f6; --accent-glow: rgba(59, 130, 246, 0.2);
      }}
      body {{
        font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 1000px;
        margin: 0 auto; padding: 2rem 1rem; background: var(--bg); color: var(--text); line-height: 1.5;
      }}
      header {{ margin-bottom: 2.5rem; display: flex; justify-content: space-between; align-items: flex-end; }}
      h1 {{ margin: 0; font-size: 1.8rem; letter-spacing: -0.5px; }}
      .refresh-indicator {{ font-size: 0.8rem; color: var(--muted); display: flex; align-items: center; gap: 6px; }}
      .pulse {{
        width: 8px; height: 8px; background: var(--ok); border-radius: 50%;
        box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); animation: pulse 2s infinite;
      }}
      @keyframes pulse {{
        0% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }}
        70% {{ transform: scale(1); box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }}
        100% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }}
      }}
      .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; margin-bottom: 1.5rem; }}
      .stats-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 1.5rem; }}
      .card {{
        background: var(--card); border: 1px solid var(--border); border-radius: 16px;
        padding: 1.25rem; transition: transform 0.2s ease, background 0.2s ease;
      }}
      .card:hover {{ background: var(--card-hover); }}
      .kpi-label {{ color: var(--muted); font-size: 0.85rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px; }}
      .kpi-value {{ font-size: 2rem; font-weight: 800; margin-top: 0.2rem; color: var(--accent); }}
      h2 {{ font-size: 1.1rem; margin-top: 0; color: var(--muted); display: flex; align-items: center; gap: 8px; }}
      .status-row {{ display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.05); }}
      .status-row:last-of-type {{ border-bottom: none; }}
      .tag {{ font-family: monospace; padding: 2px 8px; border-radius: 6px; background: rgba(255,255,255,0.05); font-size: 0.9rem; }}
      .btn-group {{ display: flex; gap: 12px; margin-top: 2rem; }}
      .button {{ flex: 1; text-align: center; padding: 0.8rem; border-radius: 10px; font-weight: 600; text-decoration: none; transition: all 0.2s; border: 1px solid var(--accent); }}
      .btn-primary {{ background: var(--accent); color: white; box-shadow: 0 4px 14px var(--accent-glow); }}
      .btn-outline {{ color: var(--accent); }}
      .button:hover {{ filter: brightness(1.1); transform: translateY(-1px); }}
      .latency-bar {{ width: 100%; height: 4px; background: rgba(255,255,255,0.1); border-radius: 2px; margin-top: 8px; overflow: hidden; }}
      .latency-fill {{ height: 100%; }}
      .ok {{ color: var(--ok); }} .warn {{ color: var(--warn); }} .bad {{ color: var(--bad); }} .muted {{ color: var(--muted); }}
    </style>
  </head>
  <body>
    <header>
      <div>
        <h1>SCFiles Bot Dashboard</h1>
        <div class="refresh-indicator"><div class="pulse"></div>Live • Updated {now.strftime("%H:%M:%S")}</div>
      </div>
    </header>
    <div class="stats-grid">
      <div class="card"><div class="kpi-label">Movies</div><div class="kpi-value">{len(movies)}</div></div>
      <div class="card"><div class="kpi-label">Series</div><div class="kpi-value">{len(series)}</div></div>
      <div class="card"><div class="kpi-label">Collections</div><div class="kpi-value">{len(collections)}</div></div>
    </div>
    <div class="card">
      <div class="grid">
        <div class="card">
          <h2><span>🤖</span> Bot Health</h2>
          <div class="status-row"><span>Status</span><span class="ok" style="font-weight:bold; text-transform:uppercase">Online</span></div>
          <div class="status-row"><span>Uptime</span><span class="tag">{uptime}</span></div>
          <div class="status-row"><span>Last Backup</span><span class="muted">{backup_text}</span></div>
          <div class="status-row"><span>Last Ping</span><span class="muted">{ping_text}</span></div>
        </div>
        <div class="card">
          <h2><span>🌐</span> Backend Health</h2>
          <div class="status-row"><span>Status</span><span class="{'ok' if backend_status == 'online' else 'warn' if backend_status == 'degraded' else 'bad'}" style="font-weight:bold; text-transform:uppercase">{backend_status}</span></div>
          <div class="status-row"><span>Latency</span><span>{backend_latency}</span></div>
          <div class="latency-bar"><div class="latency-fill" style="width: {latency_width}%; background: {latency_color};"></div></div>
          <div class="status-row" style="margin-top:12px"><span>Response Code</span><span class="tag">{backend_code}</span></div>
          <p class="muted" style="font-size:0.75rem; margin-top:10px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">URL: {BACKEND_URL}</p>
        </div>
      </div>
    </div>
    <div class="btn-group">
      <a class="button btn-primary" href="/backup/all">📦 Backup Database</a>
      <a class="button btn-outline" href="/logs">📋 View Logs</a>
      <a class="button btn-outline" href="/health">🔍 JSON Raw Data</a>
    </div>
    <script>setTimeout(() => location.reload(), 60000);</script>
  </body>
</html>
"""
    return web.Response(text=html, content_type="text/html")

async def web_json_health_handler(request: web.Request) -> web.Response:
    now = datetime.now()
    backend = {"status": "offline", "http_status": None, "latency_ms": None, "error": None}
    try:
        start = datetime.now()
        async with aiohttp.ClientSession() as s:
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                backend["http_status"] = r.status
                backend["latency_ms"] = round((datetime.now() - start).total_seconds() * 1000, 2)
                backend["status"] = "online" if r.status == 200 else "degraded"
    except Exception as exc:
        backend["error"] = str(exc)
    return web.json_response({
        "bot": {
            "status": "online",
            "uptime_seconds": int((now - BOT_STARTED_AT).total_seconds()),
            "last_backup_at": LAST_BACKUP_AT.isoformat() if LAST_BACKUP_AT else None,
            "last_auto_ping_at": LAST_AUTO_PING_AT.isoformat() if LAST_AUTO_PING_AT else None,
        },
        "backend": backend,
        "time": now.isoformat(),
    })

async def web_backup_all_handler(request: web.Request) -> web.StreamResponse:
    zip_bytes, ts = await create_backup_zip_bytes()
    return web.Response(
        body=zip_bytes,
        headers={
            "Content-Type": "application/zip",
            "Content-Disposition": f'attachment; filename="backup_all_{ts}.zip"',
        },
    )

async def web_logs_handler(request: web.Request) -> web.StreamResponse:
    tail = read_log_tail()
    if not tail:
        return web.Response(text="No logs available yet.", content_type="text/plain")
    return web.Response(
        body=tail,
        headers={
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Disposition": 'inline; filename="bot.log"',
        },
    )

    # ── Movie position buttons ──
    if data in ("pos_top", "pos_bottom"):
        if get_state(uid) != S_ADD_MOVIE_POS:
            return await cb.answer("Session expired. Use /cancel and start again.", show_alert=True)
        pos = "top" if data == "pos_top" else "bottom"
        d   = get_data(uid)
        d["pos"] = pos
        set_state(uid, S_ADD_MOVIE_CONFIRM, **d)
        dl  = ", ".join(f"{k}p" for k in sorted(d.get("downloads", {}).keys())) or "none"
        await cb.answer(f"✅ Position: {pos}")
        await cb.message.edit_text(
            f"✅ **Confirm Movie**\n\n"
            f"ID: `{d.get('id', '?')}`\n"
            f"TMDB ID: `{d.get('tmdb_id', '?')}`\n"
            f"Extras: `{d.get('extras', '') or '—'}`\n"
            f"Downloads: `{dl}`\n"
            f"Position: `{pos}`\n\n"
            f"Type **yes** to confirm or **no** to cancel:"
        )
        return
    await update.message.reply_text("💾 Starting backup…")
    fallback_chat = str(update.effective_chat.id)
    success, info = await perform_backup(ctx.application, target_chat_id=BACKUP_CHAT_TARGET or fallback_chat)
    if success:
        await update.message.reply_text(f"✅ Backup sent to `{info}`.", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(
            f"❌ Backup failed: `{info}`\n\nSet/verify backup channel using `/setbackup <chat_id>`.",
            parse_mode=ParseMode.MARKDOWN,
        )

async def cmd_backupall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Access denied.")
        return
    await update.message.reply_text("📦 Preparing backup ZIP…")
    try:
        zip_bytes, ts = await create_backup_zip_bytes()
        await update.message.reply_document(
            document=zip_bytes,
            filename=f"backup_all_{ts}.zip",
            caption="✅ Backup ZIP ready.",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to build ZIP backup.\n`{e}`", parse_mode=ParseMode.MARKDOWN)

@admin_only
async def cmd_setbackup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global BACKUP_CHAT_TARGET
    if not ctx.args:
        current = BACKUP_CHAT_TARGET or "Not configured"
        await update.message.reply_text(
            f"📦 Current backup chat: `{current}`\n\nUsage:\n`/setbackup <chat_id>`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    chat_id = ctx.args[0].strip()
    BACKUP_CHAT_TARGET = chat_id
    save_backup_chat_target(chat_id)
    await update.message.reply_text(
        f"✅ Backup chat updated to `{chat_id}`.\nAll auto/manual backups will use this chat.",
        parse_mode=ParseMode.MARKDOWN,
    )

@admin_only
async def cmd_logs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tail = read_log_tail()
    if not tail:
        await update.message.reply_text("📭 No logs available yet.")
        return
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    await update.message.reply_document(
        document=tail,
        filename=f"bot_logs_{ts}.txt",
        caption=f"📋 Last {max(1, len(tail)//1024)}KB logs",
    )

    # ── Menu buttons ──
    await cb.answer()

    if data == "menu_status":
        now    = datetime.now()
        uptime = str(now - BOT_STARTED_AT).split(".")[0]
        try:
            s = await get_session()
            t0 = datetime.now()
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                ms   = (datetime.now() - t0).total_seconds() * 1000
                ic   = "🟢" if r.status == 200 else "🟡"
                body = (await r.text())[:60]
            txt = (f"🤖 Bot: 🟢  |  ⏱ `{uptime}`\n"
                   f"🖥 Backend: {ic}  ⚡ `{ms:.0f}ms`\n`{BACKEND_URL}`\n📨 `{body}`")
        except Exception as e:
            txt = f"🤖 Bot: 🟢  |  ⏱ `{uptime}`\n🖥 Backend: 🔴\n❗ `{e}`"
        await cb.message.edit_text(txt)

    elif data == "menu_stats":
        movies = await api_get("/api/movies") or []
        series = await api_get("/api/series") or []
        cols   = await api_get("/api/collections") or {}
        await cb.message.edit_text(
            f"📊 **Stats**\n\n"
            f"🎬 Movies: **{len(movies)}**\n"
            f"📺 Series: **{len(series)}**\n"
            f"🗂 Collections: **{len(cols)}**"
        )

    elif data == "menu_backup":
        await q.edit_message_text("💾 Running backup…")
        success, info = await perform_backup(ctx.application, target_chat_id=BACKUP_CHAT_TARGET or str(q.message.chat_id))
        if success:
            await q.edit_message_text(f"✅ Backup sent to `{info}`.", parse_mode=ParseMode.MARKDOWN)
        else:
            await q.edit_message_text(f"❌ Backup failed: {info}")
    elif data == "menu_backup_all":
        await q.edit_message_text("📦 Building backup ZIP…")
        try:
            zip_bytes, ts = await create_backup_zip_bytes()
            await q.message.reply_document(
                document=zip_bytes,
                filename=f"backup_all_{ts}.zip",
                caption="✅ Backup ZIP is ready.",
            )
            await q.edit_message_text("✅ Backup ZIP sent.")
        except Exception as e:
            await q.edit_message_text(f"❌ Could not build backup ZIP: {e}")
    elif data == "menu_movies":
        items = await api_get("/api/movies?limit=10") or []
        lines = [f"• `{m.get('id','?')}` | `{m.get('tmdb_id','?')}`" for m in items[:10]]
        await cb.message.edit_text("🎬 **Recent Movies**\n\n" + ("\n".join(lines) or "None"))

    elif data == "menu_series":
        items = await api_get("/api/series?limit=10") or []
        lines = [f"• `{s.get('id','?')}` | {len(s.get('seasons',[]))} season(s)" for s in items[:10]]
        await cb.message.edit_text("📺 **Recent Series**\n\n" + ("\n".join(lines) or "None"))

    elif data == "menu_cols":
        cols  = await api_get("/api/collections") or {}
        lines = [f"• `{k}` — {v.get('name','?')}" for k, v in list(cols.items())[:10]]
        await cb.message.edit_text("🗂 **Collections**\n\n" + ("\n".join(lines) or "None"))

    elif data == "menu_tmdb":
        await q.edit_message_text("Use /tmdb command to search TMDB.")

# ──────────────────────────── CANCEL ────────────────────────────
async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("❌ Operation cancelled.")
    return ConversationHandler.END

async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled bot error: %s", ctx.error)
    if update and isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ Something went wrong. Please try again.")
        except Exception:
            pass

# ──────────────────────────── MAIN ────────────────────────────
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    web_runner = None

    # Add movie conv
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("addmovie", cmd_addmovie)],
        states={
            ADD_MOVIE_TMDB:    [MessageHandler(filters.TEXT & ~filters.COMMAND, addmovie_tmdb)],
            ADD_MOVIE_EXTRA:   [MessageHandler(filters.TEXT & ~filters.COMMAND, addmovie_extra)],
            ADD_MOVIE_DL480:   [MessageHandler(filters.TEXT & ~filters.COMMAND, addmovie_dl480)],
            ADD_MOVIE_DL720:   [MessageHandler(filters.TEXT & ~filters.COMMAND, addmovie_dl720)],
            ADD_MOVIE_DL1080:  [MessageHandler(filters.TEXT & ~filters.COMMAND, addmovie_dl1080)],
            ADD_MOVIE_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, addmovie_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))

    # Add series conv
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("addseries", cmd_addseries)],
        states={
            ADD_SERIES_TMDB:     [MessageHandler(filters.TEXT & ~filters.COMMAND, addseries_tmdb)],
            ADD_SERIES_EPISODES: [MessageHandler(filters.TEXT & ~filters.COMMAND, addseries_episodes)],
            ADD_SERIES_CONFIRM:  [MessageHandler(filters.TEXT & ~filters.COMMAND, addseries_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))

    # Add collection conv
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("addcollection", cmd_addcollection)],
        states={
            ADD_COL_ID:     [MessageHandler(filters.TEXT & ~filters.COMMAND, addcol_id)],
            ADD_COL_NAME:   [MessageHandler(filters.TEXT & ~filters.COMMAND, addcol_name)],
            ADD_COL_BANNER: [MessageHandler(filters.TEXT & ~filters.COMMAND, addcol_banner)],
            ADD_COL_CONFIRM:[MessageHandler(filters.TEXT & ~filters.COMMAND, addcol_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))

    # Delete convs
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("delmovie", cmd_delmovie)],
        states={DEL_MOVIE_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, delmovie_id)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("delseries", cmd_delseries)],
        states={DEL_SERIES_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, delseries_id)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("delcollection", cmd_delcollection)],
        states={DEL_COL_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, delcol_id)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))

    # Edit movie conv
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("editmovie", cmd_editmovie)],
        states={
            EDIT_MOVIE_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, editmovie_id)],
            EDIT_FIELD:    [CallbackQueryHandler(editmovie_field_cb, pattern="^editfield_")],
            EDIT_VALUE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, editmovie_value)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))

    # TMDB search conv
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("tmdb", cmd_tmdb)],
        states={
            SEARCH_TYPE:   [CallbackQueryHandler(tmdb_type_cb, pattern="^tmdb_")],
            SEARCH_TMDB_Q: [MessageHandler(filters.TEXT & ~filters.COMMAND, tmdb_query)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    ))

    # Simple commands
    app.add_handler(CommandHandler("start",         cmd_start))
    app.add_handler(CommandHandler("help",          cmd_help))
    app.add_handler(CommandHandler("status",        cmd_status))
    app.add_handler(CommandHandler("stats",         cmd_stats))
    app.add_handler(CommandHandler("movies",        cmd_movies))
    app.add_handler(CommandHandler("series",        cmd_series))
    app.add_handler(CommandHandler("collections",   cmd_collections))
    app.add_handler(CommandHandler("backup",        cmd_backup))
    app.add_handler(CommandHandler("backupall",     cmd_backupall))
    app.add_handler(CommandHandler("setbackup",     cmd_setbackup))
    app.add_handler(CommandHandler("logs",          cmd_logs))
    app.add_handler(CommandHandler("cancel",        cmd_cancel))
    app.add_error_handler(on_error)

    # Menu inline callbacks
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_"))

    # Scheduler: backup every 2 days
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        perform_backup,
        trigger="interval",
        days=2,
        args=[app],
        next_run_time=datetime.now() + timedelta(seconds=10),  # first run shortly after start (remove in prod)
    )
    scheduler.add_job(
        auto_ping_services,
        trigger="interval",
        minutes=AUTO_PING_INTERVAL_MIN,
        next_run_time=datetime.now() + timedelta(seconds=30),
    )


# ═══════════════════════════════════════
#  WEB DASHBOARD
# ═══════════════════════════════════════
async def web_dashboard(req: web.Request) -> web.Response:
    now    = datetime.now()
    uptime = str(now - BOT_STARTED_AT).split(".")[0]
    b_status = "offline"; b_code = "N/A"; b_ms = 0.0; b_err = ""
    try:
        s = await get_session()
        t0 = datetime.now()
        async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
            b_code   = str(r.status)
            b_ms     = (datetime.now() - t0).total_seconds() * 1000
            b_status = "online" if r.status == 200 else "degraded"
    except Exception as e:
        b_err = str(e)

    async def on_startup(application: Application):
        global BACKUP_CHAT_TARGET
        BACKUP_CHAT_TARGET = load_backup_chat_target()
        nonlocal web_runner
        web_app = web.Application()
        web_app.router.add_get("/", web_health_handler)
        web_app.router.add_get("/health", web_json_health_handler)
        web_app.router.add_get("/backup/all", web_backup_all_handler)
        web_app.router.add_get("/logs", web_logs_handler)
        web_runner = web.AppRunner(web_app)
        await web_runner.setup()
        site = web.TCPSite(web_runner, host=WEB_HOST, port=WEB_PORT)
        await site.start()
        await register_bot_commands(application)
        if not BACKUP_CHAT_TARGET:
            logger.warning("No backup chat configured. Use /setbackup <chat_id>.")
            for admin_id in ADMIN_IDS[:3]:
                try:
                    await application.bot.send_message(
                        admin_id,
                        "⚠️ Backup channel is not configured.\nUse `/setbackup <chat_id>` to enable auto backups.",
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except Exception as e:
                    logger.warning("Could not notify admin %s about backup setup: %s", admin_id, e)
        scheduler.start()
        logger.info("Scheduler started — backup every 2 days, auto ping every %s minutes.", AUTO_PING_INTERVAL_MIN)
        logger.info("Web service started on %s:%s", WEB_HOST, WEB_PORT)

    async def on_shutdown(application: Application):
        if web_runner:
            await web_runner.cleanup()
        scheduler.shutdown(wait=False)
        logger.info("Scheduler and web service shut down.")

    app.post_init = on_startup
    app.post_shutdown = on_shutdown

    logger.info("Bot starting…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
