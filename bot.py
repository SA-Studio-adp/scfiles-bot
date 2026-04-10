"""
SCFiles Backend Manager Bot — Pyrogram Edition
"""

import asyncio
import io
import json
import logging
import os
import zipfile
from datetime import datetime, timedelta

import aiohttp
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, filters
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ─────────────────────── LOG FILE SETUP ───────────────────────
LOG_FILE = "bot.log"

class TeeHandler(logging.Handler):
    def __init__(self, path: str):
        super().__init__()
        self.path = path
        self.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        ))
    def emit(self, record):
        try:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(self.format(record) + "\n")
        except Exception:
            pass

_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
))
_file_handler = TeeHandler(LOG_FILE)
logging.basicConfig(level=logging.INFO, handlers=[_stream_handler, _file_handler])
logger = logging.getLogger("scfiles-bot")

# ─────────────────────── CONFIG ───────────────────────
def _require(key: str) -> str:
    v = os.environ.get(key, "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {key}")
    return v

try:
    API_ID       = int(_require("API_ID"))
    API_HASH     = _require("API_HASH")
    BOT_TOKEN    = _require("TELEGRAM_TOKEN")
    BACKEND_URL  = _require("BACKEND_URL").rstrip("/")
    TMDB_API_KEY = _require("TMDB_API_KEY")
except RuntimeError as e:
    logger.critical(str(e))
    raise

ADMIN_IDS       = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
BACKUP_CHAT_ID  = os.environ.get("BACKUP_CHAT_ID", "").strip()
WEB_HOST        = os.environ.get("WEB_HOST", "0.0.0.0")
WEB_PORT        = int(os.environ.get("WEB_PORT", "8080"))
BOT_WEB_URL     = os.environ.get("BOT_WEB_URL", "").rstrip("/")
AUTO_PING_MIN   = int(os.environ.get("AUTO_PING_INTERVAL_MIN", "5"))
BACKUP_CFG_FILE = os.environ.get("BACKUP_CONFIG_FILE", ".backup_config.json")

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG  = "https://image.tmdb.org/t/p/w500"

# ─────────────── Runtime state ───────────────
BOT_STARTED_AT = datetime.now()
LAST_BACKUP_AT = None
LAST_PING_AT   = None
BACKUP_TARGET  = BACKUP_CHAT_ID

# ─────────────── Conversation states ───────────────
_user_state: dict[int, dict] = {}

S_ADD_MOVIE_TMDB    = "add_movie_tmdb"
S_ADD_MOVIE_EXTRA   = "add_movie_extra"
S_ADD_MOVIE_DL480   = "add_movie_dl480"
S_ADD_MOVIE_DL720   = "add_movie_dl720"
S_ADD_MOVIE_DL1080  = "add_movie_dl1080"
S_ADD_MOVIE_POS     = "add_movie_pos"
S_ADD_MOVIE_CONFIRM = "add_movie_confirm"
S_ADD_SERIES_TMDB   = "add_series_tmdb"
S_ADD_SERIES_JSON   = "add_series_json"
S_ADD_SERIES_CONFIRM= "add_series_confirm"
S_ADD_COL_ID        = "add_col_id"
S_ADD_COL_NAME      = "add_col_name"
S_ADD_COL_BANNER    = "add_col_banner"
S_ADD_COL_BGMUSIC   = "add_col_bgmusic"
S_ADD_COL_MOVIES    = "add_col_movies"
S_DEL_MOVIE         = "del_movie"
S_DEL_SERIES        = "del_series"
S_DEL_COL           = "del_col"
S_EDIT_MOVIE_ID     = "edit_movie_id"
S_EDIT_MOVIE_VALUE  = "edit_movie_value"
S_TMDB_QUERY        = "tmdb_query"

def get_state(uid: int) -> str | None:
    return _user_state.get(uid, {}).get("state")

def get_data(uid: int) -> dict:
    return _user_state.get(uid, {}).get("data", {})

def set_state(uid: int, state: str, **data):
    _user_state[uid] = {"state": state, "data": data}

def update_data(uid: int, **kwargs):
    if uid in _user_state:
        _user_state[uid]["data"].update(kwargs)

def clear_state(uid: int):
    _user_state.pop(uid, None)

def is_admin(uid: int) -> bool:
    return not ADMIN_IDS or uid in ADMIN_IDS

# ─────────────── Backup config persistence ───────────────
def load_backup_target() -> str:
    if os.path.exists(BACKUP_CFG_FILE):
        try:
            with open(BACKUP_CFG_FILE) as f:
                v = json.load(f).get("backup_chat_id", "")
                if v:
                    return str(v)
        except Exception:
            pass
    return BACKUP_CHAT_ID

def save_backup_target(chat_id: str):
    with open(BACKUP_CFG_FILE, "w") as f:
        json.dump({"backup_chat_id": str(chat_id)}, f)

# ─────────────── Shared aiohttp session ───────────────
_SESSION: aiohttp.ClientSession | None = None

async def get_session() -> aiohttp.ClientSession:
    global _SESSION
    if _SESSION is None or _SESSION.closed:
        _SESSION = aiohttp.ClientSession()
    return _SESSION

async def api_get(path: str):
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
        [InlineKeyboardButton("💾 Backup Now",    callback_data="menu_backup"),
         InlineKeyboardButton("📦 Backup ZIP",    callback_data="menu_backup_zip")],
    ]
    if BOT_WEB_URL:
        rows.append([InlineKeyboardButton("🩺 Web Dashboard", url=BOT_WEB_URL)])
    return InlineKeyboardMarkup(rows)

# ═══════════════════════════════════════
#  COMMAND HANDLERS
# ═══════════════════════════════════════

@pyro.on_message(filters.command("start") & filters.private)
async def cmd_start(_, msg: Message):
    await msg.reply("🎛 **SCFiles Backend Manager**\n\nChoose an action:",
                    reply_markup=main_menu_kb())

@pyro.on_message(filters.command("help") & filters.private)
async def cmd_help(_, msg: Message):
    await msg.reply(
        "📖 **Commands**\n\n"
        "/start — Main menu\n"
        "/status — Server health\n"
        "/stats — DB statistics\n"
        "/movies — List recent movies\n"
        "/series — List recent series\n"
        "/collections — List collections\n"
        "/addmovie — Add a movie _(admin)_\n"
        "/addseries — Add a series _(admin)_\n"
        "/addcollection — Add a collection _(admin)_\n"
        "/editmovie — Edit a movie field _(admin)_\n"
        "/delmovie — Delete a movie _(admin)_\n"
        "/delseries — Delete a series _(admin)_\n"
        "/delcollection — Delete a collection _(admin)_\n"
        "/tmdb — Search TMDB _(admin)_\n"
        "/backup — Send backup files _(admin)_\n"
        "/backupzip — Download backup as ZIP _(admin)_\n"
        "/setbackup — Set backup channel _(admin)_\n"
        "/logs — View deployment logs _(admin)_\n"
        "/cancel — Cancel current operation"
    )

@pyro.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(_, msg: Message):
    clear_state(msg.from_user.id)
    await msg.reply("❌ Operation cancelled.")

@pyro.on_message(filters.command("status") & filters.private)
async def cmd_status(_, msg: Message):
    now    = datetime.now()
    uptime = str(now - BOT_STARTED_AT).split(".")[0]
    try:
        s = await get_session()
        t0 = datetime.now()
        async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
            ms   = (datetime.now() - t0).total_seconds() * 1000
            code = r.status
            body = (await r.text())[:80]
        ic  = "🟢" if code == 200 else "🟡"
        txt = (
            f"**Health Report**\n\n"
            f"🤖 Bot: 🟢 Online  |  ⏱ `{uptime}`\n\n"
            f"🖥 Backend: {ic} `{code}`  |  ⚡ `{ms:.0f}ms`\n"
            f"🔗 `{BACKEND_URL}`\n"
            f"📨 `{body}`"
        )
    except Exception as e:
        txt = (
            f"**Health Report**\n\n"
            f"🤖 Bot: 🟢 Online  |  ⏱ `{uptime}`\n\n"
            f"🖥 Backend: 🔴 Offline\n❗ `{e}`"
        )
    await msg.reply(txt)

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
            await msg.reply("⚠️ TMDB not found.\nEnter **extras** or `-` to skip:")
        set_state(uid, S_ADD_MOVIE_EXTRA, **mdata)

    elif state == S_ADD_MOVIE_EXTRA:
        d["extras"] = "" if text == "-" else text
        set_state(uid, S_ADD_MOVIE_DL480, **d)
        await msg.reply("📥 Enter **480p download link** (or `-` to skip):")

    elif state == S_ADD_MOVIE_DL480:
        if text != "-":
            d["downloads"]["480"] = text
        set_state(uid, S_ADD_MOVIE_DL720, **d)
        await msg.reply("📥 Enter **720p download link** (or `-` to skip):")

    elif state == S_ADD_MOVIE_DL720:
        if text != "-":
            d["downloads"]["720"] = text
        set_state(uid, S_ADD_MOVIE_DL1080, **d)
        await msg.reply("📥 Enter **1080p download link** (or `-` to skip):")

    elif state == S_ADD_MOVIE_DL1080:
        if text != "-":
            d["downloads"]["1080"] = text
        set_state(uid, S_ADD_MOVIE_POS, **d)
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("⬆️ Top",    callback_data="pos_top"),
            InlineKeyboardButton("⬇️ Bottom", callback_data="pos_bottom"),
        ]])
        await msg.reply("📌 Where should this movie appear?", reply_markup=kb)

    elif state == S_ADD_MOVIE_POS:
        await msg.reply("👆 Please tap **Top** or **Bottom** above.")

    elif state == S_ADD_MOVIE_CONFIRM:
        if text.lower() == "no":
            clear_state(uid)
            return await msg.reply("❌ Cancelled.")
        if text.lower() != "yes":
            return await msg.reply("Type **yes** to confirm or **no** to cancel.")
        d.setdefault("subtitles", {})
        pos     = d.get("pos", "bottom")
        payload = {**d, "position": pos}   # 'position' = API insertion param
        result  = await api_post("/api/movies", payload)
        clear_state(uid)
        if result and result.get("success"):
            await msg.reply(f"✅ Movie added to **{pos}**! Total: **{result['count']}**")
        else:
            await msg.reply(f"❌ Failed: `{result}`")

    # ── ADD SERIES ────────────────────────────────────────────────────
    elif state == S_ADD_SERIES_TMDB:
        if not text.isdigit():
            return await msg.reply("❌ Enter a valid numeric TMDB ID.")
        tid  = int(text)
        info = await tmdb_tv(tid)
        sdata = {"tmdb_id": str(tid), "seasons": []}   # tmdb_id is STRING in series.json
        if info:
            sdata["id"] = (info.get("name") or "").lower().replace(" ", "-").replace("'", "")
            p       = poster_url(info)
            caption = (
                fmt_tv(info)
                + f"\n\nSuggested ID: `{sdata['id']}`\n\n"
                + "📋 Paste **episode data** as JSON:\n"
                + "```\n[\n  {\n    \"season_number\": 1,\n    \"episodes\": [\n"
                + "      {\"ep_number\": 1, \"links\": {\"360p\": \"URL\", \"720p\": \"URL\"}, \"subtitle\": \"\"}\n"
                + "    ]\n  }\n]\n```"
            )
            if p:
                await msg.reply_photo(p, caption=caption)
            else:
                await msg.reply(caption)
        else:
            await msg.reply("⚠️ TMDB not found. Paste episode JSON:")
        set_state(uid, S_ADD_SERIES_JSON, **sdata)

    elif state == S_ADD_SERIES_JSON:
        try:
            seasons = json.loads(text)
            if not isinstance(seasons, list):
                raise ValueError("Must be a JSON array [ ... ]")
            for s in seasons:
                if "season_number" not in s or "episodes" not in s:
                    raise ValueError("Each season needs 'season_number' and 'episodes'")
                for ep in s["episodes"]:
                    if "ep_number" not in ep or "links" not in ep:
                        raise ValueError("Each episode needs 'ep_number' and 'links'")
                    ep.setdefault("subtitle", "")
        except (json.JSONDecodeError, ValueError) as e:
            return await msg.reply(
                f"❌ Invalid JSON: `{e}`\n\n"
                "Required format:\n"
                "```\n[{\"season_number\":1,\"episodes\":"
                "[{\"ep_number\":1,\"links\":{\"360p\":\"URL\",\"720p\":\"URL\"},\"subtitle\":\"\"}]}]\n```"
            )
        d["seasons"] = seasons
        total_eps    = sum(len(s.get("episodes", [])) for s in seasons)
        set_state(uid, S_ADD_SERIES_CONFIRM, **d)
        await msg.reply(
            f"✅ **Confirm Series**\n\n"
            f"ID: `{d.get('id','?')}`\n"
            f"TMDB ID: `{d.get('tmdb_id','?')}`\n"
            f"Seasons: `{len(seasons)}`  |  Episodes: `{total_eps}`\n"
            f"Position: `top` _(always)_\n\n"
            f"Type **yes** to confirm or **no** to cancel:"
        )

    elif state == S_ADD_SERIES_CONFIRM:
        if text.lower() == "no":
            clear_state(uid)
            return await msg.reply("❌ Cancelled.")
        if text.lower() != "yes":
            return await msg.reply("Type **yes** to confirm or **no** to cancel.")
        d["position"] = "top"
        result = await api_post("/api/series", d)
        clear_state(uid)
        if result and result.get("success"):
            await msg.reply(f"✅ Series added to **top**! Total: **{result['count']}**")
        else:
            await msg.reply(f"❌ Failed: `{result}`")

    # ── ADD COLLECTION ────────────────────────────────────────────────
    elif state == S_ADD_COL_ID:
        set_state(uid, S_ADD_COL_NAME, col_id=text)
        await msg.reply("Enter collection **Name**:")

    elif state == S_ADD_COL_NAME:
        d["col_name"] = text
        set_state(uid, S_ADD_COL_BANNER, **d)
        await msg.reply("Enter **banner URL** (or `-` to skip):")

    elif state == S_ADD_COL_BANNER:
        d["col_banner"] = "" if text == "-" else text
        set_state(uid, S_ADD_COL_BGMUSIC, **d)
        await msg.reply("Enter **bg-music URL** (or `-` to skip):")

    elif state == S_ADD_COL_BGMUSIC:
        d["col_bgmusic"] = "" if text == "-" else text
        set_state(uid, S_ADD_COL_MOVIES, **d)
        await msg.reply(
            "📋 Paste **movies list** as JSON array.\n\n"
            "Each item needs: `id`, `tmdb_id` (int), `quality`, `download`\n\n"
            "```\n[\n"
            "  {\n"
            "    \"id\": \"movie-slug\",\n"
            "    \"tmdb_id\": 12345,\n"
            "    \"quality\": \"1080p\",\n"
            "    \"download\": \"https://cdn-scfiles.vercel.app/dl/...\"\n"
            "  }\n"
            "]\n```"
        )

    elif state == S_ADD_COL_MOVIES:
        try:
            movies_list = json.loads(text)
            if not isinstance(movies_list, list):
                raise ValueError("Must be a JSON array")
            for m in movies_list:
                missing = [k for k in ("id", "tmdb_id", "quality", "download") if k not in m]
                if missing:
                    raise ValueError(f"Movie object missing keys: {missing}")
        except (json.JSONDecodeError, ValueError) as e:
            return await msg.reply(f"❌ Invalid JSON: `{e}`\n\nTry again:")
        payload = {
            "id":       d["col_id"],
            "name":     d["col_name"],
            "banner":   d.get("col_banner", ""),
            "bg-music": d.get("col_bgmusic", ""),
            "movies":   movies_list,
        }
        result = await api_post("/api/collections", payload)
        clear_state(uid)
        if result and result.get("success"):
            await msg.reply(
                f"✅ Collection **{d['col_name']}** created!\n"
                f"Movies: **{len(movies_list)}** | Total: **{result['total']}**"
            )
        else:
            await msg.reply(f"❌ Failed: `{result}`")

    # ── DELETE ────────────────────────────────────────────────────────
    elif state == S_DEL_MOVIE:
        result = await api_delete(f"/api/movies/{text}")
        clear_state(uid)
        if result and result.get("success"):
            await msg.reply(f"✅ Movie `{text}` deleted. Remaining: **{result['count']}**")
        else:
            await msg.reply(f"❌ {result or 'Failed'}")

    elif state == S_DEL_SERIES:
        result = await api_delete(f"/api/series/{text}")
        clear_state(uid)
        if result and result.get("success"):
            await msg.reply(f"✅ Series `{text}` deleted. Remaining: **{result['count']}**")
        else:
            await msg.reply(f"❌ {result or 'Failed'}")

    elif state == S_DEL_COL:
        result = await api_delete(f"/api/collections/{text}")
        clear_state(uid)
        if result and result.get("success"):
            await msg.reply(f"✅ Collection `{text}` deleted. Total: **{result['total']}**")
        else:
            await msg.reply(f"❌ {result or 'Failed'}")

    # ── EDIT MOVIE ────────────────────────────────────────────────────
    elif state == S_EDIT_MOVIE_ID:
        movies = await api_get("/api/movies") or []
        movie  = next((m for m in movies if m["id"] == text), None)
        if not movie:
            return await msg.reply("❌ Movie not found. Try again or /cancel:")
        set_state(uid, S_EDIT_MOVIE_VALUE, edit_movie=movie, edit_field=None)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✏️ {f}", callback_data=f"ef_{f}")]
            for f in ["extras", "downloads", "subtitles", "tmdb_id", "id"]
        ])
        await msg.reply(f"🎬 Found: `{text}`\n\nChoose field to edit:", reply_markup=kb)

    elif state == S_EDIT_MOVIE_VALUE:
        field = d.get("edit_field")
        if not field:
            return await msg.reply("👆 Please choose a field using the buttons above.")
        movie = d.get("edit_movie", {})
        try:
            val = json.loads(text)
        except Exception:
            val = text
        movie[field] = val
        result = await api_post("/api/movies", movie)
        clear_state(uid)
        if result and result.get("success"):
            await msg.reply(f"✅ `{movie.get('id')}` updated! Field `{field}` saved.")
        else:
            await msg.reply(f"❌ Failed: `{result}`")

    # ── TMDB SEARCH ───────────────────────────────────────────────────
    elif state == S_TMDB_QUERY:
        mtype   = d.get("tmdb_type", "movie")
        results = await tmdb_search(text, mtype)
        clear_state(uid)
        if not results:
            return await msg.reply("❌ No results found.")
        for r in results[:3]:
            full = await tmdb_movie(r["id"]) if mtype == "movie" else await tmdb_tv(r["id"])
            if not full:
                continue
            cap = (fmt_movie(full) if mtype == "movie" else fmt_tv(full)) + f"\n\n🆔 TMDB ID: `{full['id']}`"
            p   = poster_url(full)
            if p:
                await msg.reply_photo(p, caption=cap)
            else:
                await msg.reply(cap)


# ═══════════════════════════════════════
#  CALLBACK QUERY HANDLER
# ═══════════════════════════════════════
@pyro.on_callback_query()
async def on_cb(_, cb: CallbackQuery):
    if not cb.from_user:
        return
    uid  = cb.from_user.id
    data = cb.data or ""

    # ── TMDB type buttons ──
    if data in ("tmdb_movie", "tmdb_tv"):
        mtype = "tv" if data == "tmdb_tv" else "movie"
        set_state(uid, S_TMDB_QUERY, tmdb_type=mtype)
        await cb.answer()
        await cb.message.edit_text("🔍 Enter your search query:")
        return

    # ── Edit field buttons ──
    if data.startswith("ef_"):
        field = data[3:]
        update_data(uid, edit_field=field)
        current = get_data(uid).get("edit_movie", {}).get(field, "")
        await cb.answer(f"Editing: {field}")
        await cb.message.edit_text(
            f"Current `{field}`:\n`{json.dumps(current)}`\n\n"
            "Enter new value (JSON for objects/arrays):"
        )
        return

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
        await cb.message.edit_text("💾 Running backup…")
        dest = BACKUP_TARGET or str(cb.message.chat.id)
        ok, info = await perform_backup(pyro, dest)
        await cb.message.edit_text(
            f"✅ Backup sent to `{info}`." if ok else f"❌ Backup failed:\n`{info}`"
        )

    elif data == "menu_backup_zip":
        await cb.message.edit_text("📦 Building ZIP…")
        try:
            zip_data, ts = await create_zip()
            buf = io.BytesIO(zip_data)
            buf.name = f"backup_all_{ts}.zip"
            await cb.message.reply_document(buf, caption=f"✅ Backup ZIP `{ts}`")
            await cb.message.edit_text("✅ ZIP sent above.")
        except Exception as e:
            await cb.message.edit_text(f"❌ ZIP failed:\n`{e}`")

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
        await cb.message.edit_text("Use /tmdb to search TMDB.")


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

    movies = await api_get("/api/movies") or []
    series = await api_get("/api/series") or []
    cols   = await api_get("/api/collections") or {}

    bk = LAST_BACKUP_AT.strftime("%Y-%m-%d %H:%M") if LAST_BACKUP_AT else "Never"
    pg = LAST_PING_AT.strftime("%Y-%m-%d %H:%M")   if LAST_PING_AT   else "Never"

    lat_w = min(max(int(b_ms / 8), 4), 100)
    lat_c = "#10b981" if b_ms < 400 else ("#fbbf24" if b_ms < 1000 else "#ef4444")
    s_cls = "ok" if b_status == "online" else ("warn" if b_status == "degraded" else "bad")
    s_ico = {"online": "🟢", "degraded": "🟡", "offline": "🔴"}.get(b_status, "⚪")

    mv_rows = "".join(
        f"<tr><td><code>{m.get('id','?')}</code></td>"
        f"<td><code>{m.get('tmdb_id','?')}</code></td>"
        f"<td>{m.get('extras','') or '—'}</td>"
        f"<td>{'✅' if m.get('downloads') else '—'}</td></tr>"
        for m in movies[:8]
    )
    sr_rows = "".join(
        f"<tr><td><code>{s.get('id','?')}</code></td>"
        f"<td><code>{s.get('tmdb_id','?')}</code></td>"
        f"<td>{len(s.get('seasons',[]))}</td></tr>"
        for s in series[:8]
    )
    co_rows = "".join(
        f"<tr><td><code>{k}</code></td><td>{v.get('name','?')}</td>"
        f"<td>{len(v.get('movies',[]))}</td></tr>"
        for k, v in list(cols.items())[:8]
    )
    empty = "<tr><td colspan='4' style='text-align:center;color:var(--muted);padding:1.5rem'>No data</td></tr>"

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>SCFiles · Dashboard</title>
  <link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@400;600;700&display=swap" rel="stylesheet">
  <style>
    :root{{--bg:#060a12;--surface:#0c1220;--surface2:#111b2e;--border:#1c2840;
      --text:#dde8f8;--muted:#5d7a9e;--ok:#00e5a0;--warn:#ffb340;--bad:#ff4d6d;
      --accent:#4f8ef7;--accent2:#7c5bf7;--mono:'Space Mono',monospace;--sans:'DM Sans',sans-serif;}}
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;padding:2rem 1.5rem;max-width:1100px;margin:0 auto;}}
    body::before{{content:'';position:fixed;inset:0;z-index:-1;
      background:radial-gradient(ellipse 80% 60% at 10% 10%,rgba(79,142,247,.07),transparent 60%),
                 radial-gradient(ellipse 60% 50% at 90% 80%,rgba(124,91,247,.06),transparent 60%);}}
    header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:2rem;flex-wrap:wrap;gap:1rem;}}
    .logo{{display:flex;align-items:center;gap:.75rem;}}
    .logo-icon{{width:42px;height:42px;border-radius:10px;background:linear-gradient(135deg,var(--accent),var(--accent2));display:flex;align-items:center;justify-content:center;font-size:1.3rem;}}
    h1{{font-family:var(--mono);font-size:1.25rem;}} h1 span{{color:var(--accent);}}
    .live{{display:flex;align-items:center;gap:6px;font-size:.78rem;color:var(--muted);font-family:var(--mono);}}
    .dot{{width:7px;height:7px;border-radius:50%;background:var(--ok);animation:blink 1.8s infinite;}}
    @keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.2}}}}
    .kpi-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:1rem;margin-bottom:1.5rem;}}
    .kpi{{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:1.2rem 1.4rem;transition:background .2s;}}
    .kpi:hover{{background:var(--surface2);}}
    .kpi-label{{font-size:.7rem;font-family:var(--mono);color:var(--muted);text-transform:uppercase;letter-spacing:1px;}}
    .kpi-val{{font-size:2.2rem;font-weight:700;margin-top:.25rem;font-family:var(--mono);
      background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;}}
    .kpi-sub{{font-size:.7rem;color:var(--muted);margin-top:.15rem;}}
    .panels{{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem;}}
    @media(max-width:600px){{.panels{{grid-template-columns:1fr;}}}}
    .panel{{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:1.4rem;}}
    .panel-title{{font-family:var(--mono);font-size:.72rem;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:1rem;}}
    .row{{display:flex;justify-content:space-between;align-items:center;padding:.45rem 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:.86rem;}}
    .row:last-child{{border-bottom:none;}}
    .label{{color:var(--muted);}} .tag{{font-family:var(--mono);font-size:.76rem;background:rgba(255,255,255,.05);padding:2px 7px;border-radius:5px;}}
    .ok{{color:var(--ok);font-weight:600;}} .warn{{color:var(--warn);font-weight:600;}} .bad{{color:var(--bad);font-weight:600;}}
    .lat-track{{height:3px;background:rgba(255,255,255,.07);border-radius:2px;margin-top:.8rem;overflow:hidden;}}
    .lat-fill{{height:100%;border-radius:2px;}}
    .section{{margin-bottom:1.5rem;}}
    .section-title{{font-family:var(--mono);font-size:.72rem;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:.75rem;}}
    .table-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden;}}
    table{{width:100%;border-collapse:collapse;font-size:.84rem;}}
    th{{background:var(--surface2);padding:.65rem 1rem;text-align:left;font-family:var(--mono);font-size:.68rem;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);}}
    td{{padding:.6rem 1rem;border-top:1px solid rgba(255,255,255,.04);}}
    tr:hover td{{background:rgba(255,255,255,.02);}}
    code{{font-family:var(--mono);font-size:.76rem;color:var(--accent);background:rgba(79,142,247,.1);padding:1px 5px;border-radius:4px;}}
    .actions{{display:flex;gap:.75rem;flex-wrap:wrap;margin-bottom:1.5rem;}}
    .btn{{display:inline-flex;align-items:center;gap:.4rem;padding:.6rem 1.1rem;border-radius:9px;font-weight:600;font-size:.84rem;text-decoration:none;transition:all .2s;border:1px solid transparent;}}
    .btn-primary{{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;box-shadow:0 4px 16px rgba(79,142,247,.2);}}
    .btn-primary:hover{{transform:translateY(-2px);}}
    .btn-outline{{border-color:var(--border);color:var(--text);background:var(--surface);}}
    .btn-outline:hover{{border-color:var(--accent);color:var(--accent);}}
    footer{{text-align:center;color:var(--muted);font-size:.72rem;font-family:var(--mono);padding-top:2rem;border-top:1px solid var(--border);margin-top:1rem;}}
  </style>
</head>
<body>
<header>
  <div class="logo">
    <div class="logo-icon">🎛</div>
    <div><h1>SC<span>Files</span></h1><div style="font-size:.68rem;color:var(--muted);font-family:var(--mono)">backend manager</div></div>
  </div>
  <div class="live"><div class="dot"></div>LIVE · {now.strftime("%H:%M:%S")}</div>
</header>

<div class="kpi-row">
  <div class="kpi"><div class="kpi-label">Movies</div><div class="kpi-val">{len(movies)}</div><div class="kpi-sub">in database</div></div>
  <div class="kpi"><div class="kpi-label">Series</div><div class="kpi-val">{len(series)}</div><div class="kpi-sub">in database</div></div>
  <div class="kpi"><div class="kpi-label">Collections</div><div class="kpi-val">{len(cols)}</div><div class="kpi-sub">in database</div></div>
  <div class="kpi"><div class="kpi-label">Latency</div>
    <div class="kpi-val" style="font-size:1.5rem">{b_ms:.0f}<span style="font-size:.9rem;-webkit-text-fill-color:var(--muted)">ms</span></div>
    <div class="kpi-sub">backend ping</div></div>
</div>

<div class="panels">
  <div class="panel">
    <div class="panel-title">🤖 Bot</div>
    <div class="row"><span class="label">Status</span><span class="ok">ONLINE</span></div>
    <div class="row"><span class="label">Uptime</span><span class="tag">{uptime}</span></div>
    <div class="row"><span class="label">Last Backup</span><span class="tag">{bk}</span></div>
    <div class="row"><span class="label">Last Ping</span><span class="tag">{pg}</span></div>
    <div class="row"><span class="label">Backup Chat</span><span class="tag">{BACKUP_TARGET or '—'}</span></div>
  </div>
  <div class="panel">
    <div class="panel-title">🌐 Backend</div>
    <div class="row"><span class="label">Status</span><span class="{s_cls}">{s_ico} {b_status.upper()}</span></div>
    <div class="row"><span class="label">HTTP Code</span><span class="tag">{b_code}</span></div>
    <div class="row"><span class="label">Latency</span><span class="tag">{b_ms:.0f}ms</span></div>
    <div class="row"><span class="label">URL</span><span class="tag" style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{BACKEND_URL}</span></div>
    {"<div class='row'><span class='label'>Error</span><span class='bad' style='font-size:.75rem'>"+b_err[:60]+"</span></div>" if b_err else ""}
    <div class="lat-track"><div class="lat-fill" style="width:{lat_w}%;background:{lat_c}"></div></div>
  </div>
</div>

<div class="actions">
  <a class="btn btn-primary" href="/backup/all">📦 Download Backup ZIP</a>
  <a class="btn btn-outline"  href="/logs">📋 View Logs</a>
  <a class="btn btn-outline"  href="/health">📡 JSON Health</a>
  <a class="btn btn-outline"  href="javascript:location.reload()">🔄 Refresh</a>
</div>

<div class="section">
  <div class="section-title">🎬 Recent Movies</div>
  <div class="table-wrap"><table>
    <thead><tr><th>ID</th><th>TMDB ID</th><th>Extras</th><th>Links</th></tr></thead>
    <tbody>{mv_rows or empty}</tbody>
  </table></div>
</div>
<div class="section">
  <div class="section-title">📺 Recent Series</div>
  <div class="table-wrap"><table>
    <thead><tr><th>ID</th><th>TMDB ID</th><th>Seasons</th></tr></thead>
    <tbody>{sr_rows or empty}</tbody>
  </table></div>
</div>
<div class="section">
  <div class="section-title">🗂 Collections</div>
  <div class="table-wrap"><table>
    <thead><tr><th>ID</th><th>Name</th><th>Movies</th></tr></thead>
    <tbody>{co_rows or empty}</tbody>
  </table></div>
</div>

<footer>SCFiles Bot Dashboard · Auto-refresh 60s · {now.strftime("%Y-%m-%d %H:%M:%S")}</footer>
<script>setTimeout(()=>location.reload(),60000);</script>
</body></html>"""
    return web.Response(text=html, content_type="text/html")


async def web_health_json(req: web.Request) -> web.Response:
    now     = datetime.now()
    backend = {"status": "offline", "http_status": None, "latency_ms": None, "error": None}
    try:
        s = await get_session()
        t0 = datetime.now()
        async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
            backend["http_status"] = r.status
            backend["latency_ms"]  = round((datetime.now() - t0).total_seconds() * 1000, 2)
            backend["status"]      = "online" if r.status == 200 else "degraded"
    except Exception as e:
        backend["error"] = str(e)
    movies = await api_get("/api/movies") or []
    series = await api_get("/api/series") or []
    cols   = await api_get("/api/collections") or {}
    return web.json_response({
        "bot":     {"status": "online",
                    "uptime_seconds": int((now - BOT_STARTED_AT).total_seconds()),
                    "last_backup_at": LAST_BACKUP_AT.isoformat() if LAST_BACKUP_AT else None,
                    "last_ping_at":   LAST_PING_AT.isoformat()   if LAST_PING_AT   else None},
        "backend": backend,
        "db":      {"movies": len(movies), "series": len(series), "collections": len(cols)},
        "time":    now.isoformat(),
    })

async def web_backup_zip(req: web.Request) -> web.Response:
    data, ts = await create_zip()
    return web.Response(body=data, headers={
        "Content-Type":        "application/zip",
        "Content-Disposition": f'attachment; filename="backup_all_{ts}.zip"',
    })

async def web_logs(req: web.Request) -> web.Response:
    if not os.path.exists(LOG_FILE):
        return web.Response(text="No log file yet.", content_type="text/plain")
    with open(LOG_FILE, "rb") as f:
        f.seek(0, 2); size = f.tell()
        f.seek(max(0, size - 32768))  # last 32 KB
        tail = f.read()
    return web.Response(body=tail, headers={
        "Content-Type":        "text/plain; charset=utf-8",
        "Content-Disposition": "inline; filename=bot.log",
    })


# ═══════════════════════════════════════
#  SCHEDULER JOBS
# ═══════════════════════════════════════
async def job_backup():
    ok, info = await perform_backup(pyro)
    logger.info("Scheduled backup → ok=%s info=%s", ok, info)

async def job_ping():
    global LAST_PING_AT
    urls = [BACKEND_URL]
    if BOT_WEB_URL:
        urls.append(f"{BOT_WEB_URL}/health")
    s = await get_session()
    for url in urls:
        try:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                logger.info("Ping %s → %s", url, r.status)
        except Exception as e:
            logger.warning("Ping failed %s: %s", url, e)
    LAST_PING_AT = datetime.now()


# ═══════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════
async def main():
    global BACKUP_TARGET
    BACKUP_TARGET = load_backup_target()

    logger.info("=" * 60)
    logger.info("SCFiles Bot starting…")
    logger.info("Backend URL : %s", BACKEND_URL)
    logger.info("Admin IDs   : %s", ADMIN_IDS or "ALL (open)")
    logger.info("Backup chat : %s", BACKUP_TARGET or "NOT SET")
    logger.info("Web port    : %s", WEB_PORT)
    logger.info("=" * 60)

    # Web server
    web_app = web.Application()
    web_app.router.add_get("/",           web_dashboard)
    web_app.router.add_get("/health",     web_health_json)
    web_app.router.add_get("/backup/all", web_backup_zip)
    web_app.router.add_get("/logs",       web_logs)
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, WEB_HOST, WEB_PORT).start()
    logger.info("Web dashboard running on %s:%s", WEB_HOST, WEB_PORT)

    # Scheduler
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(job_backup, "interval", days=2)
    scheduler.add_job(job_ping,   "interval", minutes=AUTO_PING_MIN,
                      next_run_time=datetime.now() + timedelta(seconds=30))
    scheduler.start()
    logger.info("Scheduler started — backup every 2 days, ping every %d min", AUTO_PING_MIN)

    # Start Pyrogram
    await pyro.start()
    me = await pyro.get_me()
    logger.info("Bot online: @%s (id=%s)", me.username, me.id)

    if not BACKUP_TARGET:
        logger.warning("No backup chat configured — use /setbackup <chat_id>")
        for aid in ADMIN_IDS[:3]:
            try:
                await pyro.send_message(
                    aid,
                    "⚠️ **Backup channel not configured.**\n"
                    "Use `/setbackup <chat_id>` to enable auto-backups."
                )
            except Exception:
                pass

    try:
        await asyncio.Event().wait()   # run forever
    finally:
        logger.info("Shutting down…")
        global _SESSION
        if _SESSION and not _SESSION.closed:
            await _SESSION.close()
        await pyro.stop()
        await runner.cleanup()
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(main())
