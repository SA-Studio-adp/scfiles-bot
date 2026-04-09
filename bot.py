"""
SCFiles Backend Manager Bot
A Telegram bot to manage the SCFiles backend server (movies, series, collections)
with TMDB metadata, backup, and web service monitoring.
"""

import os
import json
import asyncio
import logging
import aiohttp
import aiofiles
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters
)
from telegram.constants import ParseMode

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
BACKUP_CHAT_ID   = os.environ.get("BACKUP_CHAT_ID", "")      # chat/channel to send backups to

TMDB_BASE        = "https://api.themoviedb.org/3"
TMDB_IMG         = "https://image.tmdb.org/t/p/w500"

# ──────────────────────────── STATES ────────────────────────────
(
    ADD_MOVIE_TMDB, ADD_MOVIE_EXTRA, ADD_MOVIE_DL480, ADD_MOVIE_DL720,
    ADD_MOVIE_DL1080, ADD_MOVIE_CONFIRM,
    ADD_SERIES_TMDB, ADD_SERIES_SEASON, ADD_SERIES_EPISODES, ADD_SERIES_CONFIRM,
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
        [InlineKeyboardButton("💾 Backup Now", callback_data="menu_backup")],
    ]
    await update.message.reply_text(
        "🎛 *SCFiles Backend Manager*\n\nChoose an action:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "📖 *Available Commands*\n\n"
        "/start — Main menu\n"
        "/status — Check server health\n"
        "/stats — Database statistics\n"
        "/movies — List recent movies\n"
        "/series — List recent series\n"
        "/collections — List collections\n"
        "/addmovie — Add a movie\n"
        "/addseries — Add a series\n"
        "/addcollection — Add a collection\n"
        "/delmovie — Delete a movie by ID\n"
        "/delseries — Delete a series by ID\n"
        "/delcollection — Delete a collection by ID\n"
        "/editmovie — Edit a movie field\n"
        "/tmdb — Search TMDB metadata\n"
        "/backup — Trigger manual backup\n"
        "/cancel — Cancel current operation\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# ──────────────────────────── SERVER STATUS ────────────────────────────
async def check_status(update_or_query, is_query=False):
    send = update_or_query.edit_message_text if is_query else update_or_query.message.reply_text
    try:
        start = datetime.now()
        async with aiohttp.ClientSession() as s:
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                elapsed = (datetime.now() - start).total_seconds() * 1000
                status  = "🟢 Online" if r.status == 200 else f"🟡 Status {r.status}"
                body    = await r.text()
                msg     = (
                    f"*Server Status*\n\n"
                    f"{status}\n"
                    f"🔗 `{BACKEND_URL}`\n"
                    f"⚡ Response: `{elapsed:.0f}ms`\n"
                    f"📨 Body: `{body[:80]}`"
                )
    except Exception as e:
        msg = f"🔴 *Server Offline*\n\n`{e}`"
    await send(msg, parse_mode=ParseMode.MARKDOWN)

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
    if not BACKUP_CHAT_ID:
        logger.warning("BACKUP_CHAT_ID not set, skipping backup.")
        return
    endpoints = {
        "movies.json":      "/api/movies",
        "series.json":      "/api/series",
        "collections.json": "/api/collections",
    }
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    await app.bot.send_message(BACKUP_CHAT_ID, f"💾 *Auto-Backup* — {ts}", parse_mode=ParseMode.MARKDOWN)
    for filename, path in endpoints.items():
        data = await api_get(path)
        if data is not None:
            content = json.dumps(data, indent=2, ensure_ascii=False).encode()
            fname   = f"{ts}_{filename}"
            await app.bot.send_document(
                BACKUP_CHAT_ID,
                document=content,
                filename=fname,
                caption=f"📦 `{fname}`",
                parse_mode=ParseMode.MARKDOWN,
            )
    logger.info(f"Backup completed at {ts}")

async def cmd_backup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Access denied.")
        return
    await update.message.reply_text("💾 Starting backup…")
    await perform_backup(ctx.application)
    await update.message.reply_text("✅ Backup sent!")

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

# ──────────────────────────── MAIN ────────────────────────────
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

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
    app.add_handler(CommandHandler("cancel",        cmd_cancel))

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

    # Remove the "first run shortly after start" in production:
    # scheduler.add_job(perform_backup, trigger="interval", days=2, args=[app])

    app.job_queue  # ensure job queue is ready

    async def on_startup(application: Application):
        scheduler.start()
        logger.info("Scheduler started — backup every 2 days.")

    app.post_init = on_startup

    logger.info("Bot starting…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
