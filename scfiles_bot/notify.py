"""
notify.py — SCFiles channel-notification engine
─────────────────────────────────────────────────────────────────────────────
Posts a TMDB portrait poster + HTML-formatted caption to your channels/
groups whenever an admin chooses to notify about a movie/series/
collection/new episode — using a SEPARATE bot token (NOTIFY_BOT_TOKEN) so
your admin bot and your public "poster" bot are different bots.

The poster is embedded WITHOUT sendPhoto — it's a single sendMessage whose
text starts with an invisible zero-width-joiner link
(`<a href="POSTER_URL">&#8205;</a>`), which makes Telegram fetch and show
POSTER_URL as the message's link-preview image, with the actual caption
text right below it, and nothing visible for the anchor itself. This keeps
the poster + title + details + button all as ONE message, and (unlike
sendPhoto's 1024-char caption cap) sendMessage allows up to 4096 chars.
`link_preview_options` (Bot API 7.0+) is also set for the same URL with
prefer_large_media/show_above_text, for clients that support it; the
invisible-anchor trick is what makes it work everywhere else.

Flow (see handlers/notify_flow.py for the conversation states)
────────────────────────────────────────────────────────────
  upload succeeds
    → "Send a notification for this upload?"  [Yes] [No]
    → "Which category?"  [📀 PreDVD] [🎬 HD] [🌐 All]
    → "Enter a title for the notification:"  (free text)
    → confirmation summary → [✅ Confirm] [❌ Cancel]
    → sent to every channel registered under that category (+ "all")

Persistence for registered channels and the upload-history log now lives
entirely in MongoDB — see db.py. This module just calls those functions.

Public API
──────────
  await notify_upload(kind, item, poster_url, category, title, session=None)
  await verify_notify_bot_in_chat(chat_id)
  await log_upload(...) / await load_recent_uploads(n)
"""
import logging, os

import aiohttp

from utils import esc as _esc
import db

try:
    from messages import (TEMPLATES as _TEMPLATES, WEBSITE_LINK as _WEBSITE_LINK,
                          CHANNEL_HANDLE as _CHANNEL_HANDLE, REQUESTS_HANDLES as _REQUESTS_HANDLES,
                          WATCH_BUTTON_PREFIX as _WATCH_BUTTON_PREFIX)
except ImportError:
    _TEMPLATES, _WEBSITE_LINK = {}, "https://yourwebsite.com"
    _CHANNEL_HANDLE, _REQUESTS_HANDLES, _WATCH_BUTTON_PREFIX = "", "", "Watch . "

logger = logging.getLogger("scfiles-bot.notify")

NOTIFY_BOT_TOKEN = os.environ.get("NOTIFY_BOT_TOKEN", "").strip()
TG_API            = "https://api.telegram.org/bot{token}/{method}"

CATEGORIES = ("predvd", "hd", "all")
CATEGORY_LABEL = {"predvd": "📀 PreDVD", "hd": "🎬 HD", "all": "🌐 All"}
KIND_ICON = {"movie": "🎬", "series": "📺", "collection": "🗂", "episode": "📺"}

# ── channel config: {category: {chat_id: title}} — all delegate to db.py ──
async def load_channels() -> dict:
    return await db.get_channels()

async def add_channel(category: str, chat_id, title: str = ""):
    await db.add_channel(category, chat_id, title)

async def remove_channel(category: str, chat_id) -> bool:
    return await db.remove_channel(category, chat_id)

async def list_channels() -> dict:
    return await db.get_channels()

# ── upload history — delegate to db.py ────────────────────────────────────
async def log_upload(kind: str, title: str, category: str):
    await db.log_upload(kind, title, category)

async def load_recent_uploads(n: int = 10) -> list:
    return await db.get_recent_uploads(n)

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

# ── HTML escaping (parse_mode=HTML everywhere in this module) ────────────
def md_escape(v) -> str:
    """Kept as an alias (some deployments may still import this name) —
    now escapes for HTML, since that's the parse_mode this module uses."""
    return _esc(v)

def _embed_image_html(url: str) -> str:
    """The invisible-anchor trick: an <a> tag around a zero-width-joiner
    character. Telegram fetches `url` for the message's link-preview image
    (with link_preview_options steering size/position below), while the
    anchor itself renders nothing visible in the message text."""
    safe_url = str(url).replace("&", "&amp;").replace('"', "&quot;")
    return f'<a href="{safe_url}">&#8205;</a>'

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

# ── slug / watch-button / footer ──────────────────────────────────────────
_URL_PATH = {"movie": "movie", "series": "series", "episode": "series", "collection": "collections"}

def _slug_id(kind: str, item: dict) -> str:
    """The slug used when the item was originally uploaded — collections
    store it as col_id, movies/series (and new-episode updates, which
    carry the parent series' dict) store it as id."""
    if kind == "collection":
        return str(item.get("col_id") or item.get("id", ""))
    return str(item.get("id", ""))

def watch_url(kind: str, item: dict) -> str:
    path = _URL_PATH.get(kind, "movie")
    slug = _slug_id(kind, item)
    return f"{_WEBSITE_LINK.rstrip('/')}/{path}?id={slug}"

def _watch_button(kind: str, item: dict, title: str) -> dict:
    """Inline keyboard with the 'Watch . <name>' button linking to the
    website, attached below every notification regardless of parse_mode."""
    return {"inline_keyboard": [[{"text": f"{_WATCH_BUTTON_PREFIX}{title}", "url": watch_url(kind, item)}]]}

def _footer() -> str:
    raw = _TEMPLATES.get("FOOTER", "")
    try:
        return raw.format(channel_handle=_CHANNEL_HANDLE, requests_handles=_REQUESTS_HANDLES,
                          website_link=_WEBSITE_LINK)
    except Exception:
        logger.exception("FOOTER template format failed")
        return ""

def build_context(kind: str, item: dict, title_override: str = None):
    """Returns (template_key, format_dict). `item` should already carry
    TMDB-enriched fields: title, year, overview, genres (list) or genre
    (str), and movie_count for collections. Values are HTML-escaped here —
    messages.py templates are plain text / basic HTML tags, not MarkdownV2."""
    title = title_override or item.get("title") or item.get("id", "?")
    if kind == "movie":
        template_key = "MOVIE"
        ctx = {
            "title":    _esc(title),
            "year":     _esc(item.get("year", "")),
            "genre":    _esc(item.get("genre") or _genre_line(item.get("genres"))),
            "overview": _esc((item.get("overview") or "")[:900]),
        }
    elif kind == "series":
        template_key = "SERIES"
        ctx = {
            "title":    _esc(title),
            "year":     _esc(item.get("year", "")),
            "genre":    _esc(item.get("genre") or _genre_line(item.get("genres"))),
            "overview": _esc((item.get("overview") or "")[:900]),
        }
    elif kind == "episode":
        template_key = "EPISODE_UPDATE"
        ctx = {
            "title":        _esc(title),
            "event_label":  _esc(item.get("event_label", "New Episode(s) Added")),
            "episode_line": _esc(item.get("episode_line", "")),
            "year":         _esc(item.get("year", "")),
            "genre":        _esc(item.get("genre") or _genre_line(item.get("genres"))),
            "overview":     _esc((item.get("overview") or "")[:900]),
        }
    else:  # collection
        template_key = "COLLECTION"
        ctx = {
            "title":       _esc(title),
            "movie_count": _esc(len(item.get("movies", []))),
        }
    return template_key, ctx

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
    was sent to (0 if NOTIFY_BOT_TOKEN is unset or no channels match).

    ONE sendMessage per channel: if there's a poster, an invisible anchor
    tag embeds it as the message's link-preview image (see _embed_image_html
    docstring above) so it renders like an attached photo without a
    separate sendPhoto call — title/details/button all stay in one message."""
    if not NOTIFY_BOT_TOKEN:
        return 0
    channels  = await load_channels()
    cats      = _categories_for_selection(category)
    chat_ids  = set()
    for cat in cats:
        chat_ids.update(channels.get(cat, {}).keys())
    if not chat_ids:
        logger.info("No channels configured for category(ies) %s — skipping notify", cats)
        return 0

    template_key, ctx = build_context(kind, item, title_override)
    raw = _TEMPLATES.get(template_key, "")
    if not raw:
        logger.error("Missing template %s in messages.py", template_key)
        return 0
    try:
        body = raw.format(**ctx)
    except Exception:
        logger.exception("Template format failed for %s", template_key)
        return 0

    display_title = title_override or item.get("title") or item.get("id", "?")
    text = (_embed_image_html(poster_url) if poster_url else "") + body + _footer()
    link_preview_options = (
        {"url": poster_url, "prefer_large_media": True, "show_above_text": True}
        if poster_url else {"is_disabled": True}
    )
    button = _watch_button(kind, item, display_title)

    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True
    sent = 0
    try:
        for chat_id in chat_ids:
            r = await _tg_call(session, "sendMessage", {
                "chat_id": chat_id, "text": text, "parse_mode": "HTML",
                "disable_web_page_preview": not bool(poster_url),
                "link_preview_options": link_preview_options,
                "reply_markup": button,
            })
            if r and r.get("ok"):
                sent += 1
    finally:
        if close_session:
            await session.close()

    await log_upload(kind, display_title, category)
    return sent

