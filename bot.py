"""
SCFiles Backend Manager Bot — Pyrogram Edition
Manages movies, series, collections via Telegram with TMDB metadata,
auto-backup every 2 days, and a rich web dashboard.
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

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("scfiles-bot")

# ─────────────────────── CONFIG ───────────────────────
API_ID           = int(os.environ["API_ID"])
API_HASH         = os.environ["API_HASH"]
BOT_TOKEN        = os.environ["TELEGRAM_TOKEN"]
BACKEND_URL      = os.environ["BACKEND_URL"].rstrip("/")
TMDB_API_KEY     = os.environ["TMDB_API_KEY"]
ADMIN_IDS        = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
BACKUP_CHAT_ID   = os.environ.get("BACKUP_CHAT_ID", "").strip()
WEB_HOST         = os.environ.get("WEB_HOST", "0.0.0.0")
WEB_PORT         = int(os.environ.get("WEB_PORT", "8080"))
BOT_WEB_URL      = os.environ.get("BOT_WEB_URL", "").rstrip("/")
AUTO_PING_MIN    = int(os.environ.get("AUTO_PING_INTERVAL_MIN", "5"))
BACKUP_CFG_FILE  = os.environ.get("BACKUP_CONFIG_FILE", ".backup_config.json")

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG  = "https://image.tmdb.org/t/p/w500"

# ─────────────── Runtime state ───────────────
BOT_STARTED_AT   = datetime.now()
LAST_BACKUP_AT   = None
LAST_PING_AT     = None
BACKUP_TARGET    = BACKUP_CHAT_ID      # mutable at runtime

# ─────────────── Conversation state store ───────────────
# key: user_id → {"state": STATE_NAME, "data": {...}}
_user_state: dict[int, dict] = {}

# ─────────────── State names ───────────────
S_IDLE = None
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
S_ADD_COL_MOVIES    = "add_col_movies"
S_DEL_MOVIE         = "del_movie"
S_DEL_SERIES        = "del_series"
S_DEL_COL           = "del_col"
S_EDIT_MOVIE_ID     = "edit_movie_id"
S_EDIT_MOVIE_FIELD  = "edit_movie_field"
S_EDIT_MOVIE_VALUE  = "edit_movie_value"
S_TMDB_TYPE         = "tmdb_type"
S_TMDB_QUERY        = "tmdb_query"

def get_state(uid: int) -> str | None:
    return _user_state.get(uid, {}).get("state")

def set_state(uid: int, state: str | None, **data):
    if state is None:
        _user_state.pop(uid, None)
    else:
        _user_state[uid] = {"state": state, "data": data}

def get_data(uid: int) -> dict:
    return _user_state.get(uid, {}).get("data", {})

def update_data(uid: int, **kwargs):
    if uid in _user_state:
        _user_state[uid]["data"].update(kwargs)

def clear_state(uid: int):
    _user_state.pop(uid, None)

# ─────────────── Auth ───────────────
def is_admin(uid: int) -> bool:
    return not ADMIN_IDS or uid in ADMIN_IDS

# ─────────────── Backup config persistence ───────────────
def load_backup_target() -> str:
    if os.path.exists(BACKUP_CFG_FILE):
        try:
            with open(BACKUP_CFG_FILE) as f:
                val = json.load(f).get("backup_chat_id", "")
                if val:
                    return str(val)
        except Exception:
            pass
    return BACKUP_CHAT_ID

def save_backup_target(chat_id: str):
    with open(BACKUP_CFG_FILE, "w") as f:
        json.dump({"backup_chat_id": str(chat_id)}, f)

# ─────────────── HTTP helpers ───────────────
async def api_get(path: str):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{BACKEND_URL}{path}", timeout=aiohttp.ClientTimeout(total=15)) as r:
                return await r.json()
    except Exception as e:
        logger.error("API GET %s: %s", path, e)
        return None

async def api_post(path: str, data: dict):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{BACKEND_URL}{path}", json=data, timeout=aiohttp.ClientTimeout(total=15)) as r:
                return await r.json()
    except Exception as e:
        logger.error("API POST %s: %s", path, e)
        return None

async def api_delete(path: str):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.delete(f"{BACKEND_URL}{path}", timeout=aiohttp.ClientTimeout(total=15)) as r:
                return await r.json()
    except Exception as e:
        logger.error("API DELETE %s: %s", path, e)
        return None

async def tmdb_movie(tid: int):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{TMDB_BASE}/movie/{tid}?api_key={TMDB_API_KEY}&language=en-US",
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json() if r.status == 200 else None
    except Exception as e:
        logger.error("TMDB movie %s: %s", tid, e)
        return None

async def tmdb_tv(tid: int):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{TMDB_BASE}/tv/{tid}?api_key={TMDB_API_KEY}&language=en-US",
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json() if r.status == 200 else None
    except Exception as e:
        logger.error("TMDB tv %s: %s", tid, e)
        return None

async def tmdb_search(query: str, media_type="movie") -> list:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{TMDB_BASE}/search/{media_type}?api_key={TMDB_API_KEY}&query={query}&language=en-US",
                timeout=aiohttp.ClientTimeout(total=10),
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
    ov      = (m.get("overview") or "No overview.")[:350]
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
    ov      = (t.get("overview") or "No overview.")[:350]
    return (
        f"📺 **{name}** ({year})\n"
        f"⭐ {rating:.1f}/10  •  {seasons} seasons / {eps} eps\n"
        f"🎭 {genres or 'N/A'}\n\n"
        f"📝 {ov}"
    )

def poster(info: dict) -> str | None:
    p = info.get("poster_path")
    return f"{TMDB_IMG}{p}" if p else None

# ─────────────── Backup helpers ───────────────
async def _collect_payloads() -> dict[str, bytes]:
    out = {}
    for fname, path in [("movies.json", "/api/movies"),
                        ("series.json", "/api/series"),
                        ("collections.json", "/api/collections")]:
        data = await api_get(path)
        if data is None:
            raise RuntimeError(f"Could not fetch {path}")
        out[fname] = json.dumps(data, indent=2, ensure_ascii=False).encode()
    return out

async def create_zip() -> tuple[bytes, str]:
    payloads = await _collect_payloads()
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
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
        payloads = await _collect_payloads()
        for name, data in payloads.items():
            buf = io.BytesIO(data)
            buf.name = f"{ts}_{name}"
            await bot.send_document(int(dest), buf, caption=f"`{ts}_{name}`")
        LAST_BACKUP_AT = datetime.now()
        logger.info("Backup done → %s", dest)
        return True, dest
    except Exception as e:
        logger.error("Backup failed → %s: %s", dest, e)
        return False, str(e)

# ─────────────── Pyrogram app ───────────────
pyro = Client("scfiles_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

def main_menu_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🎬 Movies", callback_data="menu_movies"),
         InlineKeyboardButton("📺 Series", callback_data="menu_series")],
        [InlineKeyboardButton("🗂 Collections", callback_data="menu_cols"),
         InlineKeyboardButton("🔍 TMDB Search", callback_data="menu_tmdb")],
        [InlineKeyboardButton("📊 Stats", callback_data="menu_stats"),
         InlineKeyboardButton("🌐 Server Status", callback_data="menu_status")],
        [InlineKeyboardButton("💾 Backup Now", callback_data="menu_backup"),
         InlineKeyboardButton("📦 Backup ZIP", callback_data="menu_backup_zip")],
    ]
    if BOT_WEB_URL:
        rows.append([InlineKeyboardButton("🩺 Web Dashboard", url=BOT_WEB_URL)])
    return InlineKeyboardMarkup(rows)

# ─────────── /start ───────────
@pyro.on_message(filters.command("start") & filters.private)
async def cmd_start(_, msg: Message):
    await msg.reply(
        "🎛 **SCFiles Backend Manager**\n\nChoose an action:",
        reply_markup=main_menu_kb(),
    )

# ─────────── /help ───────────
@pyro.on_message(filters.command("help") & filters.private)
async def cmd_help(_, msg: Message):
    cmds = [
        "/start — Main menu",
        "/status — Server health",
        "/stats — DB statistics",
        "/movies — List recent movies",
        "/series — List recent series",
        "/collections — List collections",
        "/addmovie — Add movie (admin)",
        "/addseries — Add series (admin)",
        "/addcollection — Add collection (admin)",
        "/editmovie — Edit movie field (admin)",
        "/delmovie — Delete movie (admin)",
        "/delseries — Delete series (admin)",
        "/delcollection — Delete collection (admin)",
        "/tmdb — TMDB metadata search (admin)",
        "/backup — Send backup to channel (admin)",
        "/backupzip — Download all as ZIP (admin)",
        "/setbackup — Set backup channel (admin)",
        "/cancel — Cancel current operation",
    ]
    await msg.reply("📖 **Commands**\n\n" + "\n".join(cmds))

# ─────────── /cancel ───────────
@pyro.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(_, msg: Message):
    clear_state(msg.from_user.id)
    await msg.reply("❌ Operation cancelled.")

# ─────────── /status ───────────
@pyro.on_message(filters.command("status") & filters.private)
async def cmd_status(_, msg: Message):
    await _send_status(msg)

async def _send_status(msg: Message, prefix=""):
    now    = datetime.now()
    uptime = str(now - BOT_STARTED_AT).split(".")[0]
    try:
        t0 = datetime.now()
        async with aiohttp.ClientSession() as s:
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                ms   = (datetime.now() - t0).total_seconds() * 1000
                code = r.status
                body = (await r.text())[:80]
                icon = "🟢" if code == 200 else "🟡"
        text = (
            f"{prefix}**Health Report**\n\n"
            f"🤖 Bot: 🟢 Online | ⏱ `{uptime}`\n"
            f"🖥 Backend: {icon} `{code}` | ⚡ `{ms:.0f}ms`\n"
            f"🔗 `{BACKEND_URL}`\n"
            f"📨 `{body}`\n"
            f"🕐 `{now.strftime('%Y-%m-%d %H:%M:%S')}`"
        )
    except Exception as e:
        text = (
            f"{prefix}**Health Report**\n\n"
            f"🤖 Bot: 🟢 Online | ⏱ `{uptime}`\n"
            f"🖥 Backend: 🔴 Offline\n"
            f"❗ `{e}`"
        )
    await msg.reply(text)

# ─────────── /stats ───────────
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

# ─────────── /movies ───────────
@pyro.on_message(filters.command("movies") & filters.private)
async def cmd_movies(_, msg: Message):
    items = await api_get("/api/movies?limit=15") or []
    if not items:
        return await msg.reply("No movies found.")
    lines = [f"• `{m['id']}` | TMDB `{m.get('tmdb_id','?')}` | {m.get('extras','')}" for m in items[:15]]
    await msg.reply("🎬 **Recent Movies**\n\n" + "\n".join(lines))

# ─────────── /series ───────────
@pyro.on_message(filters.command("series") & filters.private)
async def cmd_series(_, msg: Message):
    items = await api_get("/api/series?limit=15") or []
    if not items:
        return await msg.reply("No series found.")
    lines = [f"• `{s['id']}` | TMDB `{s.get('tmdb_id','?')}` | {len(s.get('seasons',[]))} season(s)" for s in items[:15]]
    await msg.reply("📺 **Recent Series**\n\n" + "\n".join(lines))

# ─────────── /collections ───────────
@pyro.on_message(filters.command("collections") & filters.private)
async def cmd_collections(_, msg: Message):
    cols = await api_get("/api/collections") or {}
    if not cols:
        return await msg.reply("No collections found.")
    lines = [f"• `{k}` — {v.get('name','?')} ({len(v.get('movies',[]))} movies)" for k, v in list(cols.items())[:15]]
    await msg.reply("🗂 **Collections**\n\n" + "\n".join(lines))

# ─────────── /backup ───────────
@pyro.on_message(filters.command("backup") & filters.private)
async def cmd_backup(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    m = await msg.reply("💾 Starting backup…")
    dest = BACKUP_TARGET or str(msg.chat.id)
    ok, info = await perform_backup(pyro, dest)
    await m.edit(f"✅ Backup sent to `{info}`." if ok else f"❌ Backup failed: `{info}`")

# ─────────── /backupzip ───────────
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
        await m.edit(f"❌ ZIP failed: `{e}`")

# ─────────── /setbackup ───────────
@pyro.on_message(filters.command("setbackup") & filters.private)
async def cmd_setbackup(_, msg: Message):
    global BACKUP_TARGET
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.reply(f"📦 Current backup chat: `{BACKUP_TARGET or 'Not set'}`\n\nUsage: `/setbackup <chat_id>`")
    chat_id = parts[1].strip()
    BACKUP_TARGET = chat_id
    save_backup_target(chat_id)
    await msg.reply(f"✅ Backup chat set to `{chat_id}`.")

# ═══════════════════════════════════════════════════
#  ADD MOVIE  (conversation)
# ═══════════════════════════════════════════════════
@pyro.on_message(filters.command("addmovie") & filters.private)
async def cmd_addmovie(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_ADD_MOVIE_TMDB)
    await msg.reply("🎬 **Add Movie**\n\nEnter the **TMDB Movie ID**:")

# ═══════════════════════════════════════════════════
#  ADD SERIES  (conversation)
# ═══════════════════════════════════════════════════
@pyro.on_message(filters.command("addseries") & filters.private)
async def cmd_addseries(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_ADD_SERIES_TMDB)
    await msg.reply("📺 **Add Series**\n\nEnter the **TMDB TV Show ID**:")

# ═══════════════════════════════════════════════════
#  ADD COLLECTION  (conversation)
# ═══════════════════════════════════════════════════
@pyro.on_message(filters.command("addcollection") & filters.private)
async def cmd_addcollection(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_ADD_COL_ID)
    await msg.reply("🗂 **Add Collection**\n\nEnter collection **ID** (slug, e.g. `marvel-mcu`):")

# ═══════════════════════════════════════════════════
#  DELETE commands
# ═══════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════
#  EDIT MOVIE  (conversation)
# ═══════════════════════════════════════════════════
@pyro.on_message(filters.command("editmovie") & filters.private)
async def cmd_editmovie(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_EDIT_MOVIE_ID)
    await msg.reply("✏️ Enter the **movie ID** to edit:")

# ═══════════════════════════════════════════════════
#  TMDB SEARCH  (conversation)
# ═══════════════════════════════════════════════════
@pyro.on_message(filters.command("tmdb") & filters.private)
async def cmd_tmdb(_, msg: Message):
    if not is_admin(msg.from_user.id):
        return await msg.reply("⛔ Access denied.")
    set_state(msg.from_user.id, S_TMDB_TYPE)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Movie", callback_data="tmdb_movie"),
         InlineKeyboardButton("📺 TV Show", callback_data="tmdb_tv")]
    ])
    await msg.reply("🔍 **TMDB Search** — What type?", reply_markup=kb)

# ═══════════════════════════════════════════════════
#  UNIVERSAL TEXT HANDLER  (state machine)
# ═══════════════════════════════════════════════════
@pyro.on_message(filters.text & filters.private & ~filters.command(["start","help","cancel",
    "status","stats","movies","series","collections","addmovie","addseries","addcollection",
    "editmovie","delmovie","delseries","delcollection","tmdb","backup","backupzip","setbackup"]))
async def on_text(_, msg: Message):
    uid   = msg.from_user.id
    state = get_state(uid)
    text  = msg.text.strip()

    if state is None:
        return  # ignore unsolicited text

    # ─── ADD MOVIE ───
    if state == S_ADD_MOVIE_TMDB:
        if not text.isdigit():
            return await msg.reply("❌ Enter a valid numeric TMDB ID.")
        tid = int(text)
        info = await tmdb_movie(tid)
        data = {"tmdb_id": tid, "downloads": {}, "subtitles": {}}
        if info:
            data["id"] = info.get("title", "").lower().replace(" ", "-").replace("'", "")
            p = poster(info)
            caption = fmt_movie(info) + f"\n\nSuggested ID: `{data['id']}`\n\nEnter **extras** (e.g. `PreDVD - Tamil Audio`) or `-` to skip:"
            if p:
                await msg.reply_photo(p, caption=caption)
            else:
                await msg.reply(caption)
        else:
            await msg.reply("⚠️ TMDB not found. Enter **extras** or `-`:")
        update_data(uid, **data)
        set_state(uid, S_ADD_MOVIE_EXTRA, **get_data(uid))
        return

    if state == S_ADD_MOVIE_EXTRA:
        update_data(uid, extras="" if text == "-" else text)
        set_state(uid, S_ADD_MOVIE_DL480, **get_data(uid))
        return await msg.reply("📥 Enter **480p download link** (or `-` to skip):")

    if state == S_ADD_MOVIE_DL480:
        if text != "-":
            d = get_data(uid)
            d["downloads"]["480"] = text
            update_data(uid, downloads=d["downloads"])
        set_state(uid, S_ADD_MOVIE_DL720, **get_data(uid))
        return await msg.reply("📥 Enter **720p download link** (or `-` to skip):")

    if state == S_ADD_MOVIE_DL720:
        if text != "-":
            d = get_data(uid)
            d["downloads"]["720"] = text
            update_data(uid, downloads=d["downloads"])
        set_state(uid, S_ADD_MOVIE_DL1080, **get_data(uid))
        return await msg.reply("📥 Enter **1080p download link** (or `-` to skip):")

    if state == S_ADD_MOVIE_DL1080:
        if text != "-":
            d = get_data(uid)
            d["downloads"]["1080"] = text
            update_data(uid, downloads=d["downloads"])
        set_state(uid, S_ADD_MOVIE_POS, **get_data(uid))
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("⬆️ Top", callback_data="pos_top"),
            InlineKeyboardButton("⬇️ Bottom", callback_data="pos_bottom"),
        ]])
        return await msg.reply("📌 Where should this movie be added?", reply_markup=kb)

    if state == S_ADD_MOVIE_CONFIRM:
        if text.lower() != "yes":
            clear_state(uid)
            return await msg.reply("❌ Cancelled.")
        d = get_data(uid)
        d.setdefault("subtitles", {})
        result = await api_post("/api/movies", d)
        clear_state(uid)
        if result and result.get("success"):
            pos = d.get("position", "bottom")
            return await msg.reply(f"✅ Movie added to **{pos}**! Total: **{result['count']}**")
        return await msg.reply(f"❌ Failed: `{result}`")

    # ─── ADD SERIES ───
    if state == S_ADD_SERIES_TMDB:
        if not text.isdigit():
            return await msg.reply("❌ Enter a valid numeric TMDB ID.")
        tid = int(text)
        info = await tmdb_tv(tid)
        data = {"tmdb_id": str(tid), "seasons": []}
        if info:
            data["id"] = info.get("name", "").lower().replace(" ", "-").replace("'", "")
            p = poster(info)
            caption = (
                fmt_tv(info)
                + f"\n\nSuggested ID: `{data['id']}`\n\n"
                + "📋 Paste **episode data** as JSON:\n"
                + "```json\n[\n  {\n    \"season_number\": 1,\n    \"episodes\": [\n"
                + "      {\"ep_number\":1,\"links\":{\"360p\":\"URL\",\"720p\":\"URL\"},\"subtitle\":\"\"}\n"
                + "    ]\n  }\n]\n```"
            )
            if p:
                await msg.reply_photo(p, caption=caption)
            else:
                await msg.reply(caption)
        else:
            await msg.reply("⚠️ TMDB not found. Paste episode JSON:")
        update_data(uid, **data)
        set_state(uid, S_ADD_SERIES_JSON, **get_data(uid))
        return

    if state == S_ADD_SERIES_JSON:
        try:
            seasons = json.loads(text)
            if not isinstance(seasons, list):
                raise ValueError("Must be a list")
            update_data(uid, seasons=seasons)
        except Exception as e:
            return await msg.reply(f"❌ Invalid JSON: {e}\n\nTry again:")
        d = get_data(uid)
        set_state(uid, S_ADD_SERIES_CONFIRM, **d)
        return await msg.reply(
            f"✅ **Confirm Series**\n\n"
            f"ID: `{d.get('id','?')}`\n"
            f"TMDB: `{d.get('tmdb_id','?')}`\n"
            f"Seasons: `{len(d['seasons'])}`\n\n"
            f"Type **yes** to confirm or **no** to cancel:"
        )

    if state == S_ADD_SERIES_CONFIRM:
        if text.lower() != "yes":
            clear_state(uid)
            return await msg.reply("❌ Cancelled.")
        d = get_data(uid)
        d["position"] = "top"          # series always go to top
        result = await api_post("/api/series", d)
        clear_state(uid)
        if result and result.get("success"):
            return await msg.reply(f"✅ Series added to **top**! Total: **{result['count']}**")
        return await msg.reply(f"❌ Failed: `{result}`")

    # ─── ADD COLLECTION ───
    if state == S_ADD_COL_ID:
        set_state(uid, S_ADD_COL_NAME, col_id=text)
        return await msg.reply("Enter collection **Name**:")

    if state == S_ADD_COL_NAME:
        update_data(uid, col_name=text)
        set_state(uid, S_ADD_COL_BANNER, **get_data(uid))
        return await msg.reply("Enter **banner URL** (or `-` to skip):")

    if state == S_ADD_COL_BANNER:
        update_data(uid, col_banner="" if text == "-" else text)
        set_state(uid, S_ADD_COL_MOVIES, **get_data(uid))
        return await msg.reply("Enter **movie IDs** (comma-separated, e.g. `aadu-3,youth`):")

    if state == S_ADD_COL_MOVIES:
        d = get_data(uid)
        movies_list = [m.strip() for m in text.split(",") if m.strip()]
        payload = {
            "id": d["col_id"],
            "name": d["col_name"],
            "banner": d.get("col_banner", ""),
            "bg-music": "",
            "movies": movies_list,
            # Collections API inserts new key at the end (bottom) by design
        }
        result = await api_post("/api/collections", payload)
        clear_state(uid)
        if result and result.get("success"):
            return await msg.reply(f"✅ Collection created at **bottom**! Total: **{result['total']}**")
        return await msg.reply(f"❌ Failed: `{result}`")

    # ─── DELETE ───
    if state == S_DEL_MOVIE:
        result = await api_delete(f"/api/movies/{text}")
        clear_state(uid)
        if result and result.get("success"):
            return await msg.reply(f"✅ Movie `{text}` deleted. Remaining: **{result['count']}**")
        return await msg.reply(f"❌ {result or 'Failed'}")

    if state == S_DEL_SERIES:
        result = await api_delete(f"/api/series/{text}")
        clear_state(uid)
        if result and result.get("success"):
            return await msg.reply(f"✅ Series `{text}` deleted. Remaining: **{result['count']}**")
        return await msg.reply(f"❌ {result or 'Failed'}")

    if state == S_DEL_COL:
        result = await api_delete(f"/api/collections/{text}")
        clear_state(uid)
        if result and result.get("success"):
            return await msg.reply(f"✅ Collection `{text}` deleted. Total: **{result['total']}**")
        return await msg.reply(f"❌ {result or 'Failed'}")

    # ─── EDIT MOVIE ───
    if state == S_EDIT_MOVIE_ID:
        movies = await api_get("/api/movies") or []
        movie  = next((m for m in movies if m["id"] == text), None)
        if not movie:
            return await msg.reply("❌ Movie not found. Enter a valid ID:")
        update_data(uid, edit_movie=movie)
        set_state(uid, S_EDIT_MOVIE_FIELD, **get_data(uid))
        fields = ["extras", "downloads", "subtitles", "tmdb_id", "id"]
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f, callback_data=f"ef_{f}")] for f in fields])
        return await msg.reply(f"🎬 Found: `{text}`\n\nChoose field to edit:", reply_markup=kb)

    if state == S_EDIT_MOVIE_VALUE:
        d = get_data(uid)
        field = d.get("edit_field")
        movie = d.get("edit_movie", {})
        try:
            val = json.loads(text)
        except Exception:
            val = text
        movie[field] = val
        result = await api_post("/api/movies", movie)
        clear_state(uid)
        if result and result.get("success"):
            return await msg.reply("✅ Movie updated!")
        return await msg.reply(f"❌ Failed: `{result}`")

    # ─── TMDB QUERY ───
    if state == S_TMDB_QUERY:
        d = get_data(uid)
        mtype = d.get("tmdb_type", "movie")
        results = await tmdb_search(text, mtype)
        clear_state(uid)
        if not results:
            return await msg.reply("❌ No results found.")
        for r in results[:3]:
            if mtype == "movie":
                full = await tmdb_movie(r["id"])
                if full:
                    cap  = fmt_movie(full) + f"\n\n🆔 TMDB ID: `{full['id']}`"
                    p    = poster(full)
                    if p: await msg.reply_photo(p, caption=cap)
                    else: await msg.reply(cap)
            else:
                full = await tmdb_tv(r["id"])
                if full:
                    cap  = fmt_tv(full) + f"\n\n🆔 TMDB ID: `{full['id']}`"
                    p    = poster(full)
                    if p: await msg.reply_photo(p, caption=cap)
                    else: await msg.reply(cap)

# ═══════════════════════════════════════════════════
#  CALLBACK QUERY HANDLER
# ═══════════════════════════════════════════════════
@pyro.on_callback_query()
async def on_cb(_, cb: CallbackQuery):
    uid  = cb.from_user.id
    data = cb.data

    # TMDB type selection
    if data in ("tmdb_movie", "tmdb_tv"):
        mtype = data.replace("tmdb_", "")
        set_state(uid, S_TMDB_QUERY, tmdb_type=mtype)
        await cb.message.edit("🔍 Enter your search query:")
        return await cb.answer()

    # Edit field selection
    if data.startswith("ef_"):
        field = data[3:]
        d = get_data(uid)
        current = d.get("edit_movie", {}).get(field, "")
        update_data(uid, edit_field=field)
        set_state(uid, S_EDIT_MOVIE_VALUE, **get_data(uid))
        await cb.message.edit(
            f"Current `{field}`: `{json.dumps(current)}`\n\nEnter new value (JSON for objects):"
        )
        return await cb.answer()

    # Movie position selection (inline buttons after 1080p)
    if data in ("pos_top", "pos_bottom"):
        if get_state(uid) != S_ADD_MOVIE_POS:
            return await cb.answer("Session expired. Please start again.", show_alert=True)
        pos = "top" if data == "pos_top" else "bottom"
        update_data(uid, position=pos)
        d = get_data(uid)
        set_state(uid, S_ADD_MOVIE_CONFIRM, **d)
        await cb.answer(f"Position: {pos}")
        await cb.message.edit(
            f"✅ **Confirm Movie**\n\n"
            f"ID: `{d.get('id','?')}`\n"
            f"TMDB: `{d.get('tmdb_id','?')}`\n"
            f"Extras: `{d.get('extras','')}`\n"
            f"Position: `{pos}`\n"
            f"Downloads: `{json.dumps(d.get('downloads', {}))}`\n\n"
            f"Type **yes** to confirm or **no** to cancel:"
        )
        return

    # Menu buttons
    await cb.answer()

    if data == "menu_status":
        now    = datetime.now()
        uptime = str(now - BOT_STARTED_AT).split(".")[0]
        try:
            t0 = datetime.now()
            async with aiohttp.ClientSession() as s:
                async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    ms = (datetime.now() - t0).total_seconds() * 1000
                    ic = "🟢" if r.status == 200 else "🟡"
                    body = (await r.text())[:60]
            text = (f"🤖 Bot: 🟢 Online | ⏱ `{uptime}`\n"
                    f"🖥 Backend: {ic} ⚡`{ms:.0f}ms`\n`{BACKEND_URL}`\n📨`{body}`")
        except Exception as e:
            text = f"🤖 Bot: 🟢 | 🖥 Backend: 🔴\n❗`{e}`"
        await cb.message.edit(text)

    elif data == "menu_stats":
        movies = await api_get("/api/movies") or []
        series = await api_get("/api/series") or []
        cols   = await api_get("/api/collections") or {}
        await cb.message.edit(
            f"📊 **Stats**\n\n🎬 Movies: **{len(movies)}**\n📺 Series: **{len(series)}**\n🗂 Collections: **{len(cols)}**"
        )

    elif data == "menu_backup":
        await cb.message.edit("💾 Running backup…")
        dest = BACKUP_TARGET or str(cb.message.chat.id)
        ok, info = await perform_backup(pyro, dest)
        await cb.message.edit(f"✅ Sent to `{info}`." if ok else f"❌ Failed: {info}")

    elif data == "menu_backup_zip":
        await cb.message.edit("📦 Building ZIP…")
        try:
            zip_data, ts = await create_zip()
            buf = io.BytesIO(zip_data)
            buf.name = f"backup_all_{ts}.zip"
            await cb.message.reply_document(buf, caption=f"✅ Backup ZIP `{ts}`")
            await cb.message.edit("✅ ZIP sent.")
        except Exception as e:
            await cb.message.edit(f"❌ ZIP failed: {e}")

    elif data == "menu_movies":
        items = await api_get("/api/movies?limit=10") or []
        lines = [f"• `{m['id']}` | `{m.get('tmdb_id','?')}`" for m in items[:10]]
        await cb.message.edit("🎬 **Recent Movies**\n\n" + ("\n".join(lines) or "None"))

    elif data == "menu_series":
        items = await api_get("/api/series?limit=10") or []
        lines = [f"• `{s['id']}` | {len(s.get('seasons',[]))} season(s)" for s in items[:10]]
        await cb.message.edit("📺 **Recent Series**\n\n" + ("\n".join(lines) or "None"))

    elif data == "menu_cols":
        cols  = await api_get("/api/collections") or {}
        lines = [f"• `{k}` — {v.get('name','?')}" for k, v in list(cols.items())[:10]]
        await cb.message.edit("🗂 **Collections**\n\n" + ("\n".join(lines) or "None"))

    elif data == "menu_tmdb":
        await cb.message.edit("Use /tmdb command to search TMDB.")

# ═══════════════════════════════════════════════════
#  WEB DASHBOARD
# ═══════════════════════════════════════════════════
async def web_dashboard(req: web.Request) -> web.Response:
    now    = datetime.now()
    uptime = str(now - BOT_STARTED_AT).split(".")[0]
    b_status = "offline"; b_code = "N/A"; b_latency_ms = 0.0; b_err = ""
    try:
        t0 = datetime.now()
        async with aiohttp.ClientSession() as s:
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                b_code        = str(r.status)
                b_latency_ms  = (datetime.now() - t0).total_seconds() * 1000
                b_status      = "online" if r.status == 200 else "degraded"
    except Exception as e:
        b_err = str(e)

    movies = await api_get("/api/movies") or []
    series = await api_get("/api/series") or []
    cols   = await api_get("/api/collections") or {}

    bk_text = LAST_BACKUP_AT.strftime("%Y-%m-%d %H:%M:%S") if LAST_BACKUP_AT else "Never"
    pg_text = LAST_PING_AT.strftime("%Y-%m-%d %H:%M:%S") if LAST_PING_AT else "Never"

    lat_w  = min(max(int(b_latency_ms / 8), 4), 100)
    lat_c  = "#10b981" if b_latency_ms < 400 else ("#fbbf24" if b_latency_ms < 1000 else "#ef4444")
    s_icon = {"online": "🟢", "degraded": "🟡", "offline": "🔴"}.get(b_status, "⚪")

    # Recent movies table rows
    movie_rows = "".join(
        f"<tr><td><code>{m.get('id','?')}</code></td><td><code>{m.get('tmdb_id','?')}</code></td>"
        f"<td>{m.get('extras','—')}</td><td>{'480/720/1080' if m.get('downloads') else '—'}</td></tr>"
        for m in movies[:8]
    )
    series_rows = "".join(
        f"<tr><td><code>{s.get('id','?')}</code></td><td><code>{s.get('tmdb_id','?')}</code></td>"
        f"<td>{len(s.get('seasons',[]))}</td></tr>"
        for s in series[:8]
    )
    col_rows = "".join(
        f"<tr><td><code>{k}</code></td><td>{v.get('name','?')}</td><td>{len(v.get('movies',[]))}</td></tr>"
        for k, v in list(cols.items())[:8]
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>SCFiles · Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;600;700&display=swap" rel="stylesheet">
  <style>
    :root{{
      --bg:#060a12;--surface:#0c1220;--surface2:#111b2e;--border:#1c2840;
      --text:#dde8f8;--muted:#5d7a9e;--ok:#00e5a0;--warn:#ffb340;--bad:#ff4d6d;
      --accent:#4f8ef7;--accent2:#7c5bf7;--mono:'Space Mono',monospace;--sans:'DM Sans',sans-serif;
    }}
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;}}

    /* animated mesh bg */
    body::before{{
      content:'';position:fixed;inset:0;z-index:0;
      background:radial-gradient(ellipse 80% 60% at 10% 10%,rgba(79,142,247,.07) 0%,transparent 60%),
                 radial-gradient(ellipse 60% 50% at 90% 80%,rgba(124,91,247,.06) 0%,transparent 60%);
      pointer-events:none;
    }}

    .wrap{{position:relative;z-index:1;max-width:1100px;margin:0 auto;padding:2rem 1.5rem;}}

    /* HEADER */
    header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:2.5rem;gap:1rem;flex-wrap:wrap;}}
    .logo{{display:flex;align-items:center;gap:.75rem;}}
    .logo-icon{{width:40px;height:40px;border-radius:10px;background:linear-gradient(135deg,var(--accent),var(--accent2));display:flex;align-items:center;justify-content:center;font-size:1.2rem;}}
    h1{{font-family:var(--mono);font-size:1.3rem;letter-spacing:-.5px;}}
    h1 span{{color:var(--accent);}}
    .live-badge{{display:flex;align-items:center;gap:6px;font-size:.78rem;color:var(--muted);font-family:var(--mono);}}
    .dot{{width:7px;height:7px;border-radius:50%;background:var(--ok);animation:blink 1.8s infinite;}}
    @keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.2}}}}

    /* KPI CARDS */
    .kpi-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:1rem;margin-bottom:1.5rem;}}
    .kpi{{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:1.2rem 1.4rem;position:relative;overflow:hidden;transition:transform .2s;}}
    .kpi:hover{{transform:translateY(-3px);}}
    .kpi::after{{content:'';position:absolute;inset:0;background:linear-gradient(135deg,rgba(79,142,247,.04),transparent);pointer-events:none;}}
    .kpi-label{{font-size:.72rem;font-family:var(--mono);color:var(--muted);text-transform:uppercase;letter-spacing:1px;}}
    .kpi-val{{font-size:2.4rem;font-weight:700;margin-top:.3rem;font-family:var(--mono);background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;}}
    .kpi-sub{{font-size:.72rem;color:var(--muted);margin-top:.2rem;}}

    /* STATUS PANEL */
    .status-grid{{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem;}}
    @media(max-width:640px){{.status-grid{{grid-template-columns:1fr;}}}}
    .panel{{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:1.4rem;}}
    .panel-title{{font-family:var(--mono);font-size:.78rem;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:1rem;display:flex;align-items:center;gap:.5rem;}}
    .row{{display:flex;justify-content:space-between;align-items:center;padding:.5rem 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:.88rem;}}
    .row:last-child{{border-bottom:none;}}
    .row .label{{color:var(--muted);}}
    .tag{{font-family:var(--mono);font-size:.8rem;background:rgba(255,255,255,.05);padding:2px 8px;border-radius:6px;}}
    .ok{{color:var(--ok);font-weight:600;}} .warn{{color:var(--warn);font-weight:600;}} .bad{{color:var(--bad);font-weight:600;}}

    /* LATENCY BAR */
    .lat-track{{height:3px;background:rgba(255,255,255,.07);border-radius:2px;margin-top:.8rem;overflow:hidden;}}
    .lat-fill{{height:100%;border-radius:2px;transition:width .6s ease;}}

    /* TABLES */
    .section{{margin-bottom:1.5rem;}}
    .section-header{{font-family:var(--mono);font-size:.78rem;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:.8rem;display:flex;align-items:center;justify-content:space-between;}}
    .section-header a{{color:var(--accent);text-decoration:none;font-size:.72rem;}}
    .table-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden;}}
    table{{width:100%;border-collapse:collapse;font-size:.85rem;}}
    th{{background:var(--surface2);padding:.7rem 1rem;text-align:left;font-family:var(--mono);font-size:.7rem;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);font-weight:400;}}
    td{{padding:.65rem 1rem;border-top:1px solid rgba(255,255,255,.04);}}
    tr:hover td{{background:rgba(255,255,255,.02);}}
    code{{font-family:var(--mono);font-size:.78rem;color:var(--accent);background:rgba(79,142,247,.1);padding:1px 5px;border-radius:4px;}}

    /* ACTIONS */
    .actions{{display:flex;gap:.75rem;flex-wrap:wrap;margin-bottom:2rem;}}
    .btn{{display:inline-flex;align-items:center;gap:.4rem;padding:.65rem 1.2rem;border-radius:10px;font-weight:600;font-size:.85rem;text-decoration:none;transition:all .2s;border:1px solid transparent;cursor:pointer;}}
    .btn-primary{{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;box-shadow:0 4px 20px rgba(79,142,247,.25);}}
    .btn-primary:hover{{transform:translateY(-2px);box-shadow:0 6px 24px rgba(79,142,247,.35);}}
    .btn-outline{{border-color:var(--border);color:var(--text);background:var(--surface);}}
    .btn-outline:hover{{border-color:var(--accent);color:var(--accent);}}

    footer{{text-align:center;color:var(--muted);font-size:.75rem;font-family:var(--mono);padding-top:2rem;border-top:1px solid var(--border);}}
  </style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="logo">
      <div class="logo-icon">🎛</div>
      <div><h1>SC<span>Files</span></h1><div style="font-size:.72rem;color:var(--muted);font-family:var(--mono);">backend manager</div></div>
    </div>
    <div class="live-badge"><div class="dot"></div>LIVE · {now.strftime("%H:%M:%S")}</div>
  </header>

  <!-- KPI -->
  <div class="kpi-row">
    <div class="kpi"><div class="kpi-label">Movies</div><div class="kpi-val">{len(movies)}</div><div class="kpi-sub">in database</div></div>
    <div class="kpi"><div class="kpi-label">Series</div><div class="kpi-val">{len(series)}</div><div class="kpi-sub">in database</div></div>
    <div class="kpi"><div class="kpi-label">Collections</div><div class="kpi-val">{len(cols)}</div><div class="kpi-sub">in database</div></div>
    <div class="kpi"><div class="kpi-label">Latency</div><div class="kpi-val" style="font-size:1.6rem;">{b_latency_ms:.0f}<span style="font-size:1rem;-webkit-text-fill-color:var(--muted);">ms</span></div><div class="kpi-sub">backend ping</div></div>
  </div>

  <!-- STATUS PANELS -->
  <div class="status-grid">
    <div class="panel">
      <div class="panel-title">🤖 Bot</div>
      <div class="row"><span class="label">Status</span><span class="ok">ONLINE</span></div>
      <div class="row"><span class="label">Uptime</span><span class="tag">{uptime}</span></div>
      <div class="row"><span class="label">Last Backup</span><span class="tag">{bk_text}</span></div>
      <div class="row"><span class="label">Last Ping</span><span class="tag">{pg_text}</span></div>
      <div class="row"><span class="label">Backup Chat</span><span class="tag">{BACKUP_TARGET or '—'}</span></div>
    </div>
    <div class="panel">
      <div class="panel-title">🌐 Backend</div>
      <div class="row"><span class="label">Status</span><span class="{'ok' if b_status=='online' else 'warn' if b_status=='degraded' else 'bad'}">{s_icon} {b_status.upper()}</span></div>
      <div class="row"><span class="label">HTTP Code</span><span class="tag">{b_code}</span></div>
      <div class="row"><span class="label">Latency</span><span class="tag">{b_latency_ms:.0f}ms</span></div>
      <div class="row"><span class="label">URL</span><span class="tag" style="max-width:180px;overflow:hidden;text-overflow:ellipsis;">{BACKEND_URL}</span></div>
      {"<div class='row'><span class='label'>Error</span><span class='bad' style='font-size:.78rem'>"+b_err[:50]+"</span></div>" if b_err else ""}
      <div class="lat-track"><div class="lat-fill" style="width:{lat_w}%;background:{lat_c};"></div></div>
    </div>
  </div>

  <!-- ACTIONS -->
  <div class="actions">
    <a class="btn btn-primary" href="/backup/all">📦 Download Backup ZIP</a>
    <a class="btn btn-outline" href="/health">📡 JSON Health</a>
    <a class="btn btn-outline" href="javascript:location.reload()">🔄 Refresh</a>
  </div>

  <!-- MOVIES TABLE -->
  <div class="section">
    <div class="section-header"><span>🎬 Recent Movies</span><span>Top 8</span></div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>ID</th><th>TMDB ID</th><th>Extras</th><th>Downloads</th></tr></thead>
        <tbody>{movie_rows or "<tr><td colspan='4' style='text-align:center;color:var(--muted)'>No data</td></tr>"}</tbody>
      </table>
    </div>
  </div>

  <!-- SERIES TABLE -->
  <div class="section">
    <div class="section-header"><span>📺 Recent Series</span><span>Top 8</span></div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>ID</th><th>TMDB ID</th><th>Seasons</th></tr></thead>
        <tbody>{series_rows or "<tr><td colspan='3' style='text-align:center;color:var(--muted)'>No data</td></tr>"}</tbody>
      </table>
    </div>
  </div>

  <!-- COLLECTIONS TABLE -->
  <div class="section">
    <div class="section-header"><span>🗂 Collections</span><span>Top 8</span></div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>ID</th><th>Name</th><th>Movies</th></tr></thead>
        <tbody>{col_rows or "<tr><td colspan='3' style='text-align:center;color:var(--muted)'>No data</td></tr>"}</tbody>
      </table>
    </div>
  </div>

  <footer>SCFiles Bot Dashboard · Auto-refresh every 60s · {now.strftime("%Y-%m-%d %H:%M:%S")}</footer>
</div>
<script>setTimeout(()=>location.reload(),60000);</script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")

async def web_health_json(req: web.Request) -> web.Response:
    now = datetime.now()
    backend = {"status": "offline", "http_status": None, "latency_ms": None, "error": None}
    try:
        t0 = datetime.now()
        async with aiohttp.ClientSession() as s:
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
        "bot": {
            "status": "online",
            "uptime_seconds": int((now - BOT_STARTED_AT).total_seconds()),
            "last_backup_at": LAST_BACKUP_AT.isoformat() if LAST_BACKUP_AT else None,
            "last_ping_at": LAST_PING_AT.isoformat() if LAST_PING_AT else None,
        },
        "backend": backend,
        "db": {"movies": len(movies), "series": len(series), "collections": len(cols)},
        "time": now.isoformat(),
    })

async def web_backup_zip(req: web.Request) -> web.Response:
    data, ts = await create_zip()
    return web.Response(
        body=data,
        headers={
            "Content-Type": "application/zip",
            "Content-Disposition": f'attachment; filename="backup_all_{ts}.zip"',
        },
    )

# ═══════════════════════════════════════════════════
#  SCHEDULER JOBS
# ═══════════════════════════════════════════════════
async def job_backup():
    ok, info = await perform_backup(pyro)
    logger.info("Scheduled backup → ok=%s info=%s", ok, info)

async def job_ping():
    global LAST_PING_AT
    urls = [BACKEND_URL]
    if BOT_WEB_URL:
        urls.append(f"{BOT_WEB_URL}/health")
    async with aiohttp.ClientSession() as s:
        for url in urls:
            try:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    logger.info("Ping %s → %s", url, r.status)
            except Exception as e:
                logger.warning("Ping failed %s → %s", url, e)
    LAST_PING_AT = datetime.now()

# ═══════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════
async def main():
    global BACKUP_TARGET
    BACKUP_TARGET = load_backup_target()

    # Web server
    web_app = web.Application()
    web_app.router.add_get("/", web_dashboard)
    web_app.router.add_get("/health", web_health_json)
    web_app.router.add_get("/backup/all", web_backup_zip)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_HOST, WEB_PORT)
    await site.start()
    logger.info("Web dashboard running on %s:%s", WEB_HOST, WEB_PORT)

    # Scheduler
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(job_backup, "interval", days=2)
    scheduler.add_job(job_ping,   "interval", minutes=AUTO_PING_MIN,
                      next_run_time=datetime.now() + timedelta(seconds=20))
    scheduler.start()

    # Notify admins if backup not configured
    if not BACKUP_TARGET:
        logger.warning("No backup chat set. Use /setbackup <chat_id>")

    # Start bot
    await pyro.start()
    logger.info("Bot started.")

    if not BACKUP_TARGET:
        for aid in ADMIN_IDS[:3]:
            try:
                await pyro.send_message(aid,
                    "⚠️ Backup channel is not configured.\nUse `/setbackup <chat_id>` to enable auto backups.")
            except Exception:
                pass

    try:
        await asyncio.get_event_loop().create_future()  # run forever
    finally:
        await pyro.stop()
        await runner.cleanup()
        scheduler.shutdown(wait=False)

if __name__ == "__main__":
    asyncio.run(main())
