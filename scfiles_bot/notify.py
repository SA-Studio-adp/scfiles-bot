"""
notify.py — SCFiles v4 channel-notification module
─────────────────────────────────────────────────────────────────────────────
Posts a poster + MarkdownV2 caption to your channels/groups whenever a
movie, series, or collection is uploaded — using a SEPARATE bot token
(NOTIFY_BOT_TOKEN) so your admin bot and your public "poster" bot are
different bots.

Setup
─────
1. Create a second bot with @BotFather and add it as admin to your channels.
2. Set env var:  NOTIFY_BOT_TOKEN=123456:ABC...
3. Customise wording in messages.py (same folder).
4. In each target channel/group, send /addchannel with the ADMIN bot
   (not the notify bot). It'll ask you to pick a category:
       📀 PreDVD   — gets PreDVD-tagged movies only
       🎬 HD       — gets everything except PreDVD movies
       🌐 All      — gets every movie (PreDVD + HD), every series, every collection
   A channel can be registered under more than one category by running
   /addchannel again and picking a different button.

Routing
───────
  movie (extras contains "predvd", case-insensitive)  -> "predvd" + "all"
  movie (anything else)                                -> "hd" + "all"
  series                                                -> "all"
  collection                                            -> "all"

Public API
──────────
  await notify_upload("movie", movie_dict, poster_url)
  await notify_upload("series", series_dict, poster_url)
  await notify_upload("collection", col_dict, poster_url)
"""
import json, logging, os, re
import aiohttp

try:
    from messages import TEMPLATES as _TEMPLATES
except ImportError:
    _TEMPLATES = {}

logger = logging.getLogger("scfiles-bot.notify")

NOTIFY_BOT_TOKEN = os.environ.get("NOTIFY_BOT_TOKEN", "").strip()
CHANNELS_CFG      = os.environ.get("CHANNELS_CONFIG_FILE", ".channels_config.json")
TG_API            = "https://api.telegram.org/bot{token}/{method}"

CATEGORIES = ("predvd", "hd", "all")
CATEGORY_LABEL = {"predvd": "📀 PreDVD", "hd": "🎬 HD", "all": "🌐 All"}

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

# ── MarkdownV2 escaping (per Telegram Bot API) ────────────────────────────
_MDV2_SPECIAL = r"_*[]()~`>#+-=|{}.!"

def md_escape(v) -> str:
    s = str(v if v is not None else "")
    return re.sub(f"([{re.escape(_MDV2_SPECIAL)}])", r"\\\1", s)

# ── routing ────────────────────────────────────────────────────────────────
def _movie_is_predvd(extras: str) -> bool:
    return bool(extras) and "predvd" in extras.lower()

def _categories_for(kind: str, item: dict) -> list[str]:
    if kind == "movie":
        return (["predvd", "all"] if _movie_is_predvd(item.get("extras", ""))
                else ["hd", "all"])
    return ["all"]

def _build_context(kind: str, item: dict, share_base: str = "") -> tuple[str, dict]:
    if kind == "movie":
        template = ("MOVIE_PREDVD" if _movie_is_predvd(item.get("extras", ""))
                    else "MOVIE_HD")
        qualities = " / ".join(f"{k}p" for k in sorted(item.get("downloads", {}).keys())) or "—"
        ctx = {
            "title":      md_escape(item.get("title") or item.get("id", "?")),
            "year":       md_escape(item.get("year", "")),
            "rating":     md_escape(item.get("rating", "N/A")),
            "qualities":  qualities,
            "overview":   md_escape((item.get("overview") or "")[:300]),
            "share_url":  f"{share_base}/movie/{item.get('id','')}" if share_base else "https://t.me",
        }
    elif kind == "series":
        template = "SERIES"
        eps = sum(len(s.get("episodes", [])) for s in item.get("seasons", []))
        ctx = {
            "title":       md_escape(item.get("title") or item.get("id", "?")),
            "year":        md_escape(item.get("year", "")),
            "rating":      md_escape(item.get("rating", "N/A")),
            "season_line": md_escape(f"{len(item.get('seasons',[]))} season(s) · {eps} episode(s)"),
            "overview":    md_escape((item.get("overview") or "")[:300]),
            "share_url":   f"{share_base}/series/{item.get('id','')}" if share_base else "https://t.me",
        }
    else:
        template = "COLLECTION"
        ctx = {
            "title":        md_escape(item.get("col_name") or item.get("name") or item.get("col_id", "?")),
            "movie_count":  md_escape(len(item.get("movies", []))),
            "share_url":    f"{share_base}/collection/{item.get('col_id') or item.get('id','')}" if share_base else "https://t.me",
        }
    return template, ctx

# ── sending ────────────────────────────────────────────────────────────────
async def _tg_call(session: aiohttp.ClientSession, method: str, payload: dict):
    if not NOTIFY_BOT_TOKEN:
        logger.warning("NOTIFY_BOT_TOKEN not set — skipping channel notification")
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

async def notify_upload(kind: str, item: dict, poster_url: str = None,
                         share_base: str = "", session: aiohttp.ClientSession = None):
    if not NOTIFY_BOT_TOKEN:
        return
    channels  = load_channels()
    cats      = _categories_for(kind, item)
    chat_ids  = set()
    for cat in cats:
        chat_ids.update(channels.get(cat, {}).keys())
    if not chat_ids:
        logger.info("No channels configured for category(ies) %s — skipping notify", cats)
        return

    template_name, ctx = _build_context(kind, item, share_base)
    raw = _TEMPLATES.get(template_name, "")
    if not raw:
        logger.error("No template named %s in messages.py", template_name)
        return
    try:
        caption = raw.format(**ctx)
    except Exception:
        logger.exception("Template format failed for %s", template_name)
        return

    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True
    try:
        for chat_id in chat_ids:
            if poster_url:
                await _tg_call(session, "sendPhoto", {
                    "chat_id": chat_id, "photo": poster_url,
                    "caption": caption, "parse_mode": "MarkdownV2",
                })
            else:
                await _tg_call(session, "sendMessage", {
                    "chat_id": chat_id, "text": caption,
                    "parse_mode": "MarkdownV2", "disable_web_page_preview": False,
                })
    finally:
        if close_session:
            await session.close()
