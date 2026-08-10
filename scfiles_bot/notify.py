"""
notify.py — SCFiles channel-notification engine
─────────────────────────────────────────────────────────────────────────────
Posts a TMDB portrait poster + MarkdownV2 caption to your channels/groups
whenever an admin chooses to notify about a movie/series/collection/new
episode — using a SEPARATE bot token (NOTIFY_BOT_TOKEN) so your admin bot
and your public "poster" bot are different bots.

Flow (see handlers/notify_flow.py for the conversation states)
────────────────────────────────────────────────────────────
  upload succeeds
    → "Send a notification for this upload?"  [Yes] [No]
    → "Which category?"  [📀 PreDVD] [🎬 HD] [🌐 All]
    → "Enter a title for the notification:"  (free text)
    → confirmation summary → [✅ Confirm] [❌ Cancel]
    → sent to every channel registered under that category (+ "all")

Registering a channel (/addchannel, in handlers/channels.py) verifies the
target is actually a channel/group — not a person's private chat — before
letting an admin register it.

Public API
──────────
  await notify_upload(kind, item, poster_url, category, title, session=None)
  await verify_notify_bot_in_chat(chat_id)
  log_upload(...) / load_recent_uploads(n)
"""
import json, logging, os, re
from datetime import datetime

import aiohttp

try:
    from messages import TEMPLATES as _TEMPLATES, PROMO_LINK as _PROMO_LINK
except ImportError:
    _TEMPLATES, _PROMO_LINK = {}, "https://t.me/"

logger = logging.getLogger("scfiles-bot.notify")

NOTIFY_BOT_TOKEN = os.environ.get("NOTIFY_BOT_TOKEN", "").strip()
CHANNELS_CFG      = os.environ.get("CHANNELS_CONFIG_FILE", ".channels_config.json")
UPLOADS_LOG_FILE  = os.environ.get("UPLOADS_LOG_FILE", ".uploads_log.json")
UPLOADS_LOG_MAX   = 50   # keep this many, /uploads shows the most recent 10
TG_API            = "https://api.telegram.org/bot{token}/{method}"

CATEGORIES = ("predvd", "hd", "all")
CATEGORY_LABEL = {"predvd": "📀 PreDVD", "hd": "🎬 HD", "all": "🌐 All"}
KIND_ICON = {"movie": "🎬", "series": "📺", "collection": "🗂", "episode": "📺"}

# ── channel config: {category: {chat_id: title}} ─────────────────────────
def load_channels() -> dict:
    if os.path.exists(CHANNELS_CFG):
        try:
            with open(CHANNELS_CFG) as f:
                data = json.load(f)
                for k, v in list(data.items()):
                    if isinstance(v, list):
                        data[k] = {str(x): "" for x in v}
                return data
        except Exception:
            logger.exception("Failed to load %s", CHANNELS_CFG)
    return {}

def save_channels(channels: dict):
    with open(CHANNELS_CFG, "w") as f:
        json.dump(channels, f, indent=2, ensure_ascii=False)

def add_channel(category: str, chat_id, title: str = "") -> dict:
    category = category.strip().lower()
    channels = load_channels()
    bucket = channels.setdefault(category, {})
    bucket[str(chat_id)] = title or bucket.get(str(chat_id), "")
    save_channels(channels)
    return channels

def remove_channel(category: str, chat_id) -> tuple[dict, bool]:
    category = category.strip().lower()
    channels = load_channels()
    bucket = channels.get(category, {})
    removed = str(chat_id) in bucket
    bucket.pop(str(chat_id), None)
    if bucket:
        channels[category] = bucket
    else:
        channels.pop(category, None)
    save_channels(channels)
    return channels, removed

def list_channels() -> dict:
    return load_channels()

# ── chat-type verification (used by /addchannel) ──────────────────────────
async def verify_notify_bot_in_chat(chat_id):
    """Checks (via the NOTIFY bot) that it can see this chat, so we know
    notifications will actually be deliverable. Returns the getChat result
    dict, or None if the notify bot isn't a member / can't reach it."""
    if not NOTIFY_BOT_TOKEN:
        return None
    async with aiohttp.ClientSession() as session:
        data = await _tg_call(session, "getChat", {"chat_id": chat_id})
    if data and data.get("ok"):
        return data.get("result")
    return None

# ── upload history (for the notify-bot's /uploads command) ───────────────
def log_upload(kind: str, title: str, category: str):
    entries = load_recent_uploads(UPLOADS_LOG_MAX)
    entries.insert(0, {
        "kind": kind, "title": title, "category": category,
        "ts": datetime.utcnow().isoformat(),
    })
    entries = entries[:UPLOADS_LOG_MAX]
    try:
        with open(UPLOADS_LOG_FILE, "w") as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
    except Exception:
        logger.exception("Failed to write %s", UPLOADS_LOG_FILE)

def load_recent_uploads(n: int = 10) -> list:
    if not os.path.exists(UPLOADS_LOG_FILE):
        return []
    try:
        with open(UPLOADS_LOG_FILE) as f:
            return json.load(f)[:n]
    except Exception:
        logger.exception("Failed to read %s", UPLOADS_LOG_FILE)
        return []

# ── MarkdownV2 escaping (per Telegram Bot API) ────────────────────────────
_MDV2_SPECIAL = r"_*[]()~`>#+-=|{}.!"

def md_escape(v) -> str:
    s = str(v if v is not None else "")
    return re.sub(f"([{re.escape(_MDV2_SPECIAL)}])", r"\\\1", s)

# ── routing ────────────────────────────────────────────────────────────────
def _categories_for_selection(selected: str) -> list:
    """The admin picks ONE category for this upload. 'all' channels always
    receive it too, unless 'all' was itself the pick (avoids duplicates)."""
    selected = (selected or "hd").lower()
    return ["all"] if selected == "all" else [selected, "all"]

def _genre_line(genres) -> str:
    if not genres:
        return "—"
    names = [g.get("name", "") if isinstance(g, dict) else str(g) for g in genres]
    return " / ".join(n for n in names if n)

def build_context(kind: str, item: dict, title_override: str = None):
    """Returns (template_name, format_dict). `item` should already carry
    TMDB-enriched fields: title, year, overview, genres (list) or genre (str),
    and movie_count for collections."""
    title = title_override or item.get("title") or item.get("id", "?")
    if kind == "movie":
        template = "MOVIE"
        ctx = {
            "title":    md_escape(title),
            "year":     md_escape(item.get("year", "")),
            "genre":    md_escape(item.get("genre") or _genre_line(item.get("genres"))),
            "overview": md_escape((item.get("overview") or "")[:600]),
            "promo_link": _PROMO_LINK,
        }
    elif kind == "series":
        template = "SERIES"
        ctx = {
            "title":    md_escape(title),
            "year":     md_escape(item.get("year", "")),
            "genre":    md_escape(item.get("genre") or _genre_line(item.get("genres"))),
            "overview": md_escape((item.get("overview") or "")[:600]),
            "promo_link": _PROMO_LINK,
        }
    elif kind == "episode":
        template = "EPISODE_UPDATE"
        ctx = {
            "title":        md_escape(title),
            "episode_line": md_escape(item.get("episode_line", "")),
            "year":         md_escape(item.get("year", "")),
            "genre":        md_escape(item.get("genre") or _genre_line(item.get("genres"))),
            "overview":     md_escape((item.get("overview") or "")[:600]),
            "promo_link":   _PROMO_LINK,
        }
    else:  # collection
        template = "COLLECTION"
        ctx = {
            "title":       md_escape(title),
            "movie_count": md_escape(len(item.get("movies", []))),
            "promo_link":  _PROMO_LINK,
        }
    return template, ctx

# ── sending ────────────────────────────────────────────────────────────────
async def _tg_call(session: aiohttp.ClientSession, method: str, payload: dict):
    if not NOTIFY_BOT_TOKEN:
        logger.warning("NOTIFY_BOT_TOKEN not set — skipping Telegram API call")
        return None
    url = TG_API.format(token=NOTIFY_BOT_TOKEN, method=method)
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as r:
            data = await r.json()
            if not data.get("ok"):
                logger.error("Notify %s failed for chat %s: %s", method, payload.get("chat_id"), data)
            return data
    except Exception as e:
        logger.error("Notify %s error: %s", method, e)
        return None

async def notify_upload(kind: str, item: dict, poster_url: str = None, category: str = "hd",
                         title_override: str = None, session: aiohttp.ClientSession = None) -> int:
    """Sends the notification and logs it. Returns the number of chats it
    was sent to (0 if NOTIFY_BOT_TOKEN is unset or no channels match)."""
    if not NOTIFY_BOT_TOKEN:
        return 0
    channels  = load_channels()
    cats      = _categories_for_selection(category)
    chat_ids  = set()
    for cat in cats:
        chat_ids.update(channels.get(cat, {}).keys())
    if not chat_ids:
        logger.info("No channels configured for category(ies) %s — skipping notify", cats)
        return 0

    template_name, ctx = build_context(kind, item, title_override)
    raw = _TEMPLATES.get(template_name, "")
    if not raw:
        logger.error("No template named %s in messages.py", template_name)
        return 0
    try:
        caption = raw.format(**ctx)
    except Exception:
        logger.exception("Template format failed for %s", template_name)
        return 0

    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True
    sent = 0
    try:
        for chat_id in chat_ids:
            if poster_url:
                r = await _tg_call(session, "sendPhoto", {
                    "chat_id": chat_id, "photo": poster_url,
                    "caption": caption, "parse_mode": "MarkdownV2",
                })
            else:
                r = await _tg_call(session, "sendMessage", {
                    "chat_id": chat_id, "text": caption,
                    "parse_mode": "MarkdownV2", "disable_web_page_preview": False,
                })
            if r and r.get("ok"):
                sent += 1
    finally:
        if close_session:
            await session.close()

    log_upload(kind, title_override or item.get("title") or item.get("id", "?"), category)
    return sent
