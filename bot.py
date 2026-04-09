"""
SCFiles Backend Manager Bot
A Telegram bot to manage the SCFiles backend server (movies, series, collections)
with TMDB metadata, backup, and web service monitoring.
"""

import os
import json
import io
import logging
import zipfile
import aiohttp
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters
)
from telegram.constants import ParseMode
from telegram.helpers import escape_markdown

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

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
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{BACKEND_URL}{path}", timeout=aiohttp.ClientTimeout(total=15)) as r:
                return await r.json()
    except Exception as e:
        logger.error(f"API GET {path}: {e}")
        return None

async def api_post(path: str, data: dict) -> dict | None:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{BACKEND_URL}{path}", json=data,
                              timeout=aiohttp.ClientTimeout(total=15)) as r:
                return await r.json()
    except Exception as e:
        logger.error(f"API POST {path}: {e}")
        return None

async def api_delete(path: str) -> dict | None:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.delete(f"{BACKEND_URL}{path}",
                                timeout=aiohttp.ClientTimeout(total=15)) as r:
                return await r.json()
    except Exception as e:
        logger.error(f"API DELETE {path}: {e}")
        return None

async def tmdb_movie(tmdb_id: int) -> dict | None:
    try:
        async with aiohttp.ClientSession() as s:
            url = f"{TMDB_BASE}/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    return await r.json()
    except Exception as e:
        logger.error(f"TMDB movie {tmdb_id}: {e}")
    return None

async def tmdb_tv(tmdb_id: int) -> dict | None:
    try:
        async with aiohttp.ClientSession() as s:
            url = f"{TMDB_BASE}/tv/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    return await r.json()
    except Exception as e:
        logger.error(f"TMDB tv {tmdb_id}: {e}")
    return None

async def tmdb_search(query: str, media_type="movie") -> list:
    try:
        async with aiohttp.ClientSession() as s:
            url = f"{TMDB_BASE}/search/{media_type}?api_key={TMDB_API_KEY}&query={query}&language=en-US"
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("results", [])[:5]
    except Exception as e:
        logger.error(f"TMDB search: {e}")
    return []

def fmt_tmdb_movie(m: dict) -> str:
    title    = m.get("title", "N/A")
    year     = (m.get("release_date") or "")[:4]
    rating   = m.get("vote_average", 0)
    overview = (m.get("overview") or "No overview.")[:300]
    genres   = ", ".join(g["name"] for g in m.get("genres", []))
    runtime  = m.get("runtime", 0)
    return (
        f"🎬 *{title}* ({year})\n"
        f"⭐ {rating:.1f}/10  •  ⏱ {runtime} min\n"
        f"🎭 {genres or 'N/A'}\n\n"
        f"📝 {overview}"
    )

def fmt_tmdb_tv(t: dict) -> str:
    name     = t.get("name", "N/A")
    year     = (t.get("first_air_date") or "")[:4]
    rating   = t.get("vote_average", 0)
    overview = (t.get("overview") or "No overview.")[:300]
    seasons  = t.get("number_of_seasons", "?")
    episodes = t.get("number_of_episodes", "?")
    genres   = ", ".join(g["name"] for g in t.get("genres", []))
    return (
        f"📺 *{name}* ({year})\n"
        f"⭐ {rating:.1f}/10  •  {seasons} seasons / {episodes} eps\n"
        f"🎭 {genres or 'N/A'}\n\n"
        f"📝 {overview}"
    )

def poster_url(path: str | None) -> str | None:
    if path:
        return f"{TMDB_IMG}{path}"
    return None

# ──────────────────────────── /start /help ────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("🎬 Movies", callback_data="menu_movies"),
         InlineKeyboardButton("📺 Series", callback_data="menu_series")],
        [InlineKeyboardButton("🗂 Collections", callback_data="menu_collections"),
         InlineKeyboardButton("🔍 TMDB Search", callback_data="menu_tmdb")],
        [InlineKeyboardButton("📊 Stats", callback_data="menu_stats"),
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

# ──────────────────────────── LIST MOVIES ────────────────────────────
async def cmd_movies(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    movies = await api_get("/api/movies?limit=10") or []
    if not movies:
        await update.message.reply_text("No movies found.")
        return
    lines = []
    for m in movies[:10]:
        mid  = m.get("id", "?")
        extra = m.get("extras", "")
        tid  = m.get("tmdb_id", "")
        lines.append(f"• `{mid}` | TMDB: `{tid}` | {extra}")
    await update.message.reply_text(
        f"🎬 *Recent Movies (top 10)*\n\n" + "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN
    )

# ──────────────────────────── LIST SERIES ────────────────────────────
async def cmd_series(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    series = await api_get("/api/series?limit=10") or []
    if not series:
        await update.message.reply_text("No series found.")
        return
    lines = []
    for s in series[:10]:
        sid   = s.get("id", "?")
        tid   = s.get("tmdb_id", "")
        seasons = len(s.get("seasons", []))
        lines.append(f"• `{sid}` | TMDB: `{tid}` | {seasons} season(s)")
    await update.message.reply_text(
        f"📺 *Recent Series (top 10)*\n\n" + "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN
    )

# ──────────────────────────── LIST COLLECTIONS ────────────────────────────
async def cmd_collections(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cols = await api_get("/api/collections") or {}
    if not cols:
        await update.message.reply_text("No collections found.")
        return
    lines = [f"• `{k}` — {v.get('name','?')} ({len(v.get('movies',[]))} movies)"
             for k, v in list(cols.items())[:15]]
    await update.message.reply_text(
        f"🗂 *Collections*\n\n" + "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN
    )

# ──────────────────────────── ADD MOVIE ────────────────────────────
@admin_only
async def cmd_addmovie(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 *Add Movie*\n\nEnter the *TMDB Movie ID*:",
        parse_mode=ParseMode.MARKDOWN
    )
    return ADD_MOVIE_TMDB

async def addmovie_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tid = update.message.text.strip()
    if not tid.isdigit():
        await update.message.reply_text("❌ Please enter a valid numeric TMDB ID.")
        return ADD_MOVIE_TMDB
    ctx.user_data["new_movie"] = {"tmdb_id": int(tid)}
    info = await tmdb_movie(int(tid))
    if info:
        ctx.user_data["new_movie"]["id"] = info.get("title","").lower().replace(" ","-")
        poster = poster_url(info.get("poster_path"))
        caption = fmt_tmdb_movie(info) + f"\n\nSuggested ID: `{ctx.user_data['new_movie']['id']}`\n\nEnter *extras* (e.g. `PreDVD - Tamil Audio`) or skip with `-`:"
        if poster:
            await update.message.reply_photo(poster, caption=caption, parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(caption, parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text("⚠️ TMDB not found. Enter *extras* or `-` to skip:")
    return ADD_MOVIE_EXTRA

async def addmovie_extra(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    val = update.message.text.strip()
    ctx.user_data["new_movie"]["extras"] = "" if val == "-" else val
    await update.message.reply_text("📥 Enter *480p download link* (or `-` to skip):", parse_mode=ParseMode.MARKDOWN)
    return ADD_MOVIE_DL480

async def addmovie_dl480(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.setdefault("new_movie", {}).setdefault("downloads", {})
    val = update.message.text.strip()
    if val != "-": ctx.user_data["new_movie"]["downloads"]["480"] = val
    await update.message.reply_text("📥 Enter *720p download link* (or `-` to skip):", parse_mode=ParseMode.MARKDOWN)
    return ADD_MOVIE_DL720

async def addmovie_dl720(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    val = update.message.text.strip()
    if val != "-": ctx.user_data["new_movie"]["downloads"]["720"] = val
    await update.message.reply_text("📥 Enter *1080p download link* (or `-` to skip):", parse_mode=ParseMode.MARKDOWN)
    return ADD_MOVIE_DL1080

async def addmovie_dl1080(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    val = update.message.text.strip()
    if val != "-": ctx.user_data["new_movie"]["downloads"]["1080"] = val
    movie = ctx.user_data["new_movie"]
    summary = (
        f"✅ *Confirm Movie*\n\n"
        f"ID: `{movie.get('id','?')}`\n"
        f"TMDB: `{movie.get('tmdb_id','?')}`\n"
        f"Extras: `{movie.get('extras','')}`\n"
        f"Downloads: `{json.dumps(movie.get('downloads', {}))}`\n\n"
        f"Type *yes* to confirm or *no* to cancel:"
    )
    await update.message.reply_text(summary, parse_mode=ParseMode.MARKDOWN)
    return ADD_MOVIE_CONFIRM

async def addmovie_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text.strip().lower() != "yes":
        await update.message.reply_text("❌ Cancelled.")
        ctx.user_data.clear()
        return ConversationHandler.END
    movie = ctx.user_data.pop("new_movie", {})
    movie.setdefault("subtitles", {})
    result = await api_post("/api/movies", movie)
    if result and result.get("success"):
        await update.message.reply_text(f"✅ Movie added! Total movies: *{result['count']}*", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ Failed to add movie.\n`{result}`", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ──────────────────────────── ADD SERIES ────────────────────────────
@admin_only
async def cmd_addseries(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📺 *Add Series*\n\nEnter the *TMDB TV Show ID*:", parse_mode=ParseMode.MARKDOWN)
    return ADD_SERIES_TMDB

async def addseries_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tid = update.message.text.strip()
    if not tid.isdigit():
        await update.message.reply_text("❌ Please enter a valid numeric TMDB ID.")
        return ADD_SERIES_TMDB
    ctx.user_data["new_series"] = {"tmdb_id": str(tid), "seasons": []}
    info = await tmdb_tv(int(tid))
    if info:
        ctx.user_data["new_series"]["id"] = info.get("name","").lower().replace(" ","-")
        poster = poster_url(info.get("poster_path"))
        caption = fmt_tmdb_tv(info) + f"\n\nSuggested ID: `{ctx.user_data['new_series']['id']}`\n\n📋 Now paste *episode data* as JSON:\n\n```json\n[\n  {{\n    \"season_number\": 1,\n    \"episodes\": [\n      {{\"ep_number\":1,\"links\":{{\"360p\":\"URL\",\"720p\":\"URL\"}},\"subtitle\":\"\"}}\n    ]\n  }}\n]\n```"
        if poster:
            await update.message.reply_photo(poster, caption=caption, parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(caption, parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text("⚠️ TMDB not found. Paste episode JSON:")
    return ADD_SERIES_EPISODES

async def addseries_episodes(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    try:
        seasons = json.loads(raw)
        if not isinstance(seasons, list):
            raise ValueError("Must be a list")
        ctx.user_data["new_series"]["seasons"] = seasons
    except Exception as e:
        await update.message.reply_text(f"❌ Invalid JSON: {e}\n\nTry again:")
        return ADD_SERIES_EPISODES
    s = ctx.user_data["new_series"]
    summary = (
        f"✅ *Confirm Series*\n\n"
        f"ID: `{s.get('id','?')}`\n"
        f"TMDB: `{s.get('tmdb_id','?')}`\n"
        f"Seasons: `{len(s['seasons'])}`\n\n"
        f"Type *yes* to confirm or *no* to cancel:"
    )
    await update.message.reply_text(summary, parse_mode=ParseMode.MARKDOWN)
    return ADD_SERIES_CONFIRM

async def addseries_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text.strip().lower() != "yes":
        await update.message.reply_text("❌ Cancelled.")
        ctx.user_data.clear()
        return ConversationHandler.END
    series = ctx.user_data.pop("new_series", {})
    result = await api_post("/api/series", series)
    if result and result.get("success"):
        await update.message.reply_text(f"✅ Series added! Total: *{result['count']}*", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ Failed.\n`{result}`", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ──────────────────────────── ADD COLLECTION ────────────────────────────
@admin_only
async def cmd_addcollection(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗂 *Add Collection*\n\nEnter collection *ID* (slug, e.g. `marvel-mcu`):", parse_mode=ParseMode.MARKDOWN)
    return ADD_COL_ID

async def addcol_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["new_col"] = {"id": update.message.text.strip()}
    await update.message.reply_text("Enter collection *Name*:", parse_mode=ParseMode.MARKDOWN)
    return ADD_COL_NAME

async def addcol_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["new_col"]["name"] = update.message.text.strip()
    await update.message.reply_text("Enter *banner URL* (or `-` to skip):", parse_mode=ParseMode.MARKDOWN)
    return ADD_COL_BANNER

async def addcol_banner(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    val = update.message.text.strip()
    ctx.user_data["new_col"]["banner"] = "" if val == "-" else val
    await update.message.reply_text("Enter *movie IDs* (comma-separated, e.g. `aadu-3,youth`):", parse_mode=ParseMode.MARKDOWN)
    return ADD_COL_CONFIRM

async def addcol_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    movies = [m.strip() for m in update.message.text.split(",") if m.strip()]
    col = ctx.user_data.pop("new_col", {})
    payload = {
        "id": col["id"],
        "name": col["name"],
        "banner": col.get("banner", ""),
        "bg-music": "",
        "movies": movies,
    }
    result = await api_post("/api/collections", payload)
    if result and result.get("success"):
        await update.message.reply_text(f"✅ Collection created! Total: *{result['total']}*", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ Failed.\n`{result}`", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ──────────────────────────── DELETE ────────────────────────────
@admin_only
async def cmd_delmovie(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗑 Enter the *movie ID* to delete:", parse_mode=ParseMode.MARKDOWN)
    return DEL_MOVIE_ID

async def delmovie_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    mid = update.message.text.strip()
    result = await api_delete(f"/api/movies/{mid}")
    if result and result.get("success"):
        await update.message.reply_text(f"✅ Movie `{mid}` deleted. Remaining: *{result['count']}*", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ {result or 'Failed'}", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

@admin_only
async def cmd_delseries(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗑 Enter the *series ID* to delete:", parse_mode=ParseMode.MARKDOWN)
    return DEL_SERIES_ID

async def delseries_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sid = update.message.text.strip()
    result = await api_delete(f"/api/series/{sid}")
    if result and result.get("success"):
        await update.message.reply_text(f"✅ Series `{sid}` deleted. Remaining: *{result['count']}*", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ {result or 'Failed'}", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

@admin_only
async def cmd_delcollection(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗑 Enter the *collection ID* to delete:", parse_mode=ParseMode.MARKDOWN)
    return DEL_COL_ID

async def delcol_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.message.text.strip()
    result = await api_delete(f"/api/collections/{cid}")
    if result and result.get("success"):
        await update.message.reply_text(f"✅ Collection `{cid}` deleted. Total: *{result['total']}*", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ {result or 'Failed'}", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ──────────────────────────── EDIT MOVIE ────────────────────────────
@admin_only
async def cmd_editmovie(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✏️ Enter the *movie ID* to edit:", parse_mode=ParseMode.MARKDOWN)
    return EDIT_MOVIE_ID

async def editmovie_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    mid = update.message.text.strip()
    movies = await api_get("/api/movies") or []
    movie = next((m for m in movies if m["id"] == mid), None)
    if not movie:
        await update.message.reply_text("❌ Movie not found.")
        return EDIT_MOVIE_ID
    ctx.user_data["edit_movie"] = movie.copy()
    fields = ["extras", "downloads", "subtitles", "tmdb_id", "id"]
    kb = [[InlineKeyboardButton(f, callback_data=f"editfield_{f}")] for f in fields]
    await update.message.reply_text(
        f"🎬 Found: `{mid}`\n\nChoose field to edit:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN
    )
    return EDIT_FIELD

async def editmovie_field_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    field = update.callback_query.data.replace("editfield_", "")
    ctx.user_data["edit_field"] = field
    current = ctx.user_data["edit_movie"].get(field, "")
    await update.callback_query.edit_message_text(
        f"Current `{field}`: `{json.dumps(current)}`\n\nEnter new value (JSON for objects):",
        parse_mode=ParseMode.MARKDOWN
    )
    return EDIT_VALUE

async def editmovie_value(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    raw   = update.message.text.strip()
    field = ctx.user_data.get("edit_field")
    movie = ctx.user_data.get("edit_movie", {})
    try:
        val = json.loads(raw)
    except Exception:
        val = raw
    movie[field] = val
    result = await api_post("/api/movies", movie)
    if result and result.get("success"):
        await update.message.reply_text(f"✅ Movie updated!", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ Failed.\n`{result}`", parse_mode=ParseMode.MARKDOWN)
    ctx.user_data.clear()
    return ConversationHandler.END

# ──────────────────────────── TMDB SEARCH ────────────────────────────
@admin_only
async def cmd_tmdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("🎬 Movie", callback_data="tmdb_movie"),
         InlineKeyboardButton("📺 TV Show", callback_data="tmdb_tv")]
    ]
    await update.message.reply_text(
        "🔍 *TMDB Search*\n\nWhat do you want to search?",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN
    )
    return SEARCH_TYPE

async def tmdb_type_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    ctx.user_data["tmdb_type"] = update.callback_query.data.replace("tmdb_", "")
    await update.callback_query.edit_message_text("🔍 Enter your search query:")
    return SEARCH_TMDB_Q

async def tmdb_query(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.message.text.strip()
    mtype = ctx.user_data.get("tmdb_type", "movie")
    results = await tmdb_search(q, mtype)
    if not results:
        await update.message.reply_text("❌ No results found.")
        return ConversationHandler.END
    for r in results[:3]:
        if mtype == "movie":
            full = await tmdb_movie(r["id"])
            if full:
                msg    = fmt_tmdb_movie(full) + f"\n\n🆔 TMDB ID: `{full['id']}`"
                poster = poster_url(full.get("poster_path"))
                if poster:
                    await update.message.reply_photo(poster, caption=msg, parse_mode=ParseMode.MARKDOWN)
                else:
                    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
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
async def perform_backup(app: Application):
    """Fetch all data and send as JSON files to BACKUP_CHAT_ID."""
    global LAST_BACKUP_AT
    if not BACKUP_CHAT_TARGET:
        logger.warning("Backup channel not set, skipping backup.")
        return
    endpoints = {
        "movies.json":      "/api/movies",
        "series.json":      "/api/series",
        "collections.json": "/api/collections",
    }
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    await app.bot.send_message(BACKUP_CHAT_TARGET, f"💾 *Auto-Backup* — {ts}", parse_mode=ParseMode.MARKDOWN)
    for filename, path in endpoints.items():
        data = await api_get(path)
        if data is not None:
            content = json.dumps(data, indent=2, ensure_ascii=False).encode()
            fname   = f"{ts}_{filename}"
            await app.bot.send_document(
                BACKUP_CHAT_TARGET,
                document=content,
                filename=fname,
                caption=f"📦 `{fname}`",
                parse_mode=ParseMode.MARKDOWN,
            )
    LAST_BACKUP_AT = datetime.now()
    logger.info(f"Backup completed at {ts}")

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
    backend_error = ""
    try:
        start = datetime.now()
        async with aiohttp.ClientSession() as s:
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                backend_code = str(r.status)
                backend_latency = f"{(datetime.now() - start).total_seconds() * 1000:.0f}ms"
                backend_status = "online" if r.status == 200 else "degraded"
    except Exception as exc:
        backend_error = str(exc)

    backup_text = LAST_BACKUP_AT.strftime("%Y-%m-%d %H:%M:%S") if LAST_BACKUP_AT else "Never"
    ping_text = LAST_AUTO_PING_AT.strftime("%Y-%m-%d %H:%M:%S") if LAST_AUTO_PING_AT else "Never"
    movies = await api_get("/api/movies") or []
    series = await api_get("/api/series") or []
    collections = await api_get("/api/collections") or {}
    html = f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>SCFiles Bot Health</title>
    <style>
      :root {{
        --bg:#0b1220; --card:#101a2e; --text:#e6edf7; --muted:#9fb0cc;
        --ok:#3ddc97; --warn:#ffcf5a; --bad:#ff6b6b; --accent:#4da3ff;
      }}
      body {{ font-family: Inter, Arial, sans-serif; max-width: 980px; margin: 2rem auto; padding: 0 1rem; background:var(--bg); color:var(--text); }}
      .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; margin-bottom:1rem; }}
      .card {{ background:var(--card); border:1px solid #1f2a44; border-radius:12px; padding:1rem; margin-bottom:1rem; }}
      .kpi {{ font-size:1.6rem; font-weight:700; margin-top:.4rem; }}
      .ok {{ color: var(--ok); }}
      .warn {{ color: var(--warn); }}
      .bad {{ color: var(--bad); }}
      a.button {{ display: inline-block; padding: .7rem 1rem; background:var(--accent); color: #fff; border-radius: 8px; text-decoration: none; margin-right:8px; }}
      code {{ background: #17243b; padding: 2px 6px; border-radius: 4px; color:#d8e6ff; }}
      .muted {{ color:var(--muted); font-size:.92rem; }}
    </style>
  </head>
  <body>
    <h1>SCFiles Bot Dashboard</h1>
    <p class="muted">Auto refresh every 60 seconds • Last updated: {now.strftime("%Y-%m-%d %H:%M:%S")}</p>
    <div class="grid">
      <div class="card"><div class="muted">Movies</div><div class="kpi">{len(movies)}</div></div>
      <div class="card"><div class="muted">Series</div><div class="kpi">{len(series)}</div></div>
      <div class="card"><div class="muted">Collections</div><div class="kpi">{len(collections)}</div></div>
    </div>
    <div class="card">
      <h2>Bot Health</h2>
      <p>Status: <span class="ok">online</span></p>
      <p>Uptime: <code>{uptime}</code></p>
      <p>Last backup: <code>{backup_text}</code></p>
      <p>Last auto ping: <code>{ping_text}</code></p>
    </div>
    <div class="card">
      <h2>Backend Health</h2>
      <p>Status: <span class="{'ok' if backend_status == 'online' else 'warn' if backend_status == 'degraded' else 'bad'}">{backend_status}</span></p>
      <p>URL: <code>{BACKEND_URL}</code></p>
      <p>Status code: <code>{backend_code}</code></p>
      <p>Latency: <code>{backend_latency}</code></p>
      <p>Error: <code>{backend_error or 'None'}</code></p>
    </div>
    <a class="button" href="/backup/all">Backup All (Download ZIP)</a>
    <a class="button" href="/health">View JSON Health</a>
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

async def cmd_backup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Access denied.")
        return
    await update.message.reply_text("💾 Starting backup…")
    await perform_backup(ctx.application)
    await update.message.reply_text("✅ Backup sent!")

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

# ──────────────────────────── CALLBACK ROUTER ────────────────────────────
async def menu_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "menu_status":
        await check_status(q, is_query=True)
    elif data == "menu_stats":
        movies      = await api_get("/api/movies") or []
        series      = await api_get("/api/series") or []
        collections = await api_get("/api/collections") or {}
        await q.edit_message_text(
            f"📊 *Stats*\n\n🎬 Movies: *{len(movies)}*\n📺 Series: *{len(series)}*\n🗂 Collections: *{len(collections)}*",
            parse_mode=ParseMode.MARKDOWN
        )
    elif data == "menu_backup":
        await q.edit_message_text("💾 Running backup…")
        await perform_backup(ctx.application)
        await q.edit_message_text("✅ Backup done!")
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
        movies = await api_get("/api/movies?limit=10") or []
        lines  = [f"• `{m['id']}` | TMDB `{m.get('tmdb_id','?')}`" for m in movies[:10]]
        await q.edit_message_text("🎬 *Recent Movies*\n\n" + "\n".join(lines) or "None", parse_mode=ParseMode.MARKDOWN)
    elif data == "menu_series":
        series = await api_get("/api/series?limit=10") or []
        lines  = [f"• `{s['id']}` | {len(s.get('seasons',[]))} season(s)" for s in series[:10]]
        await q.edit_message_text("📺 *Recent Series*\n\n" + "\n".join(lines) or "None", parse_mode=ParseMode.MARKDOWN)
    elif data == "menu_collections":
        cols  = await api_get("/api/collections") or {}
        lines = [f"• `{k}` — {v.get('name','?')}" for k, v in list(cols.items())[:10]]
        await q.edit_message_text("🗂 *Collections*\n\n" + "\n".join(lines) or "None", parse_mode=ParseMode.MARKDOWN)
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

    # Remove the "first run shortly after start" in production:
    # scheduler.add_job(perform_backup, trigger="interval", days=2, args=[app])

    app.job_queue  # ensure job queue is ready

    async def on_startup(application: Application):
        global BACKUP_CHAT_TARGET
        BACKUP_CHAT_TARGET = load_backup_chat_target()
        nonlocal web_runner
        web_app = web.Application()
        web_app.router.add_get("/", web_health_handler)
        web_app.router.add_get("/health", web_json_health_handler)
        web_app.router.add_get("/backup/all", web_backup_all_handler)
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
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
