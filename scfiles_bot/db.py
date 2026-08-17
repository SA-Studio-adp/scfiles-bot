"""
db.py — MongoDB access layer for the SCFiles bot.
─────────────────────────────────────────────────────────────────────────────
Replaces every local .json state file the bot used to keep
(.backup_config.json, .admins_config.json, .channels_config.json,
.uploads_log.json) with MongoDB collections. This is the ONLY module that
imports motor / talks to Mongo directly — everything else (config.py,
auth.py, notify.py, handlers/*, backup.py) calls the functions below.

Movies/series/collections themselves are NOT stored here — those still
live in your existing backend (BACKEND_URL), unchanged. This module only
covers the bot's own operational state: who's an admin, where to send
backups, which channels are registered for notifications, and the
upload-history log.

Env vars
────────
  MONGODB_URI  — full connection string, e.g.
                 mongodb+srv://user:pass@cluster.mongodb.net
  MONGODB_DB   — database name (default: scfiles_admin_bot)

Collections used (created automatically on first write, no manual setup
needed — just an empty/existing MongoDB database):
  settings   — one document: {_id: "backup_target", chat_id: "..."}
  admins     — one document per admin: {_id: <user_id int>}
  channels   — one document per (category, chat_id) pair:
               {_id: "<category>:<chat_id>", category, chat_id, title}
  uploads    — one document per logged upload:
               {kind, title, category, ts}, newest first
  users      — one document per person who's DMed the notify bot:
               {_id: <user_id int>, first_name} — /broadcast's recipient list
  scheduled  — one document per scheduled notification:
               {kind, item, poster_url, category, title, scheduled_at (UTC
               ISO string), scheduled_at_display, sent (bool), created_by,
               confirmation_chat_id, confirmation_message_id — the "🗓
               scheduled" message the background job edits once sent}
"""
import logging
import os
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger("scfiles-bot.db")

MONGODB_URI = os.environ.get("MONGODB_URI", "").strip()
MONGODB_DB  = os.environ.get("MONGODB_DB", "scfiles_admin_bot").strip()

UPLOADS_LOG_MAX = 500   # backup.py dumps up to this many; /uploads shows 10

_client: AsyncIOMotorClient | None = None
_db = None


def _get_db():
    """Lazily creates the Mongo client on first use (mirrors api_client.py's
    lazy aiohttp session pattern) so import time never touches the network."""
    global _client, _db
    if _db is None:
        if not MONGODB_URI:
            raise RuntimeError(
                "MONGODB_URI is not set — the bot now requires MongoDB for "
                "admins/backup-target/channels/upload-history storage.")
        # 10s server-selection timeout (Mongo's own default is 30s) so a bad
        # URI makes main()'s startup db.ping() fail fast, not hang the boot.
        _client = AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=10000)
        _db = _client[MONGODB_DB]
    return _db

async def ping():
    """Call once at startup so a bad MONGODB_URI fails fast and loudly
    instead of silently breaking the first time a handler needs the DB."""
    await _get_db().command("ping")

async def close():
    global _client, _db
    if _client is not None:
        _client.close()
    _client, _db = None, None


# ── settings / backup target ─────────────────────────────────────────────
async def get_backup_target() -> str:
    doc = await _get_db().settings.find_one({"_id": "backup_target"})
    return str(doc["chat_id"]) if doc and doc.get("chat_id") else ""

async def set_backup_target(chat_id: str):
    await _get_db().settings.update_one(
        {"_id": "backup_target"}, {"$set": {"chat_id": str(chat_id)}}, upsert=True)


# ── admins ────────────────────────────────────────────────────────────────
async def get_admin_ids() -> list:
    cursor = _get_db().admins.find({}, {"_id": 1})
    return [doc["_id"] async for doc in cursor]

async def add_admin(user_id: int):
    await _get_db().admins.update_one(
        {"_id": int(user_id)}, {"$set": {"_id": int(user_id)}}, upsert=True)

async def remove_admin(user_id: int):
    await _get_db().admins.delete_one({"_id": int(user_id)})


# ── channels (category -> {chat_id: title}) ──────────────────────────────
async def get_channels() -> dict:
    channels: dict = {}
    cursor = _get_db().channels.find({})
    async for doc in cursor:
        channels.setdefault(doc["category"], {})[str(doc["chat_id"])] = doc.get("title", "")
    return channels

async def add_channel(category: str, chat_id, title: str = ""):
    category, chat_id = category.strip().lower(), str(chat_id)
    await _get_db().channels.update_one(
        {"_id": f"{category}:{chat_id}"},
        {"$set": {"category": category, "chat_id": chat_id, "title": title}},
        upsert=True)

async def remove_channel(category: str, chat_id) -> bool:
    category, chat_id = category.strip().lower(), str(chat_id)
    result = await _get_db().channels.delete_one({"_id": f"{category}:{chat_id}"})
    return result.deleted_count > 0


# ── upload history ────────────────────────────────────────────────────────
async def log_upload(kind: str, title: str, category: str):
    db = _get_db()
    await db.uploads.insert_one({
        "kind": kind, "title": title, "category": category,
        "ts": datetime.utcnow().isoformat(),
    })
    # Trim to the most recent UPLOADS_LOG_MAX so this collection doesn't
    # grow unbounded — cheap since uploads are infrequent.
    count = await db.uploads.count_documents({})
    if count > UPLOADS_LOG_MAX:
        excess = count - UPLOADS_LOG_MAX
        old_ids = [doc["_id"] async for doc in
                   db.uploads.find({}, {"_id": 1}).sort("ts", 1).limit(excess)]
        if old_ids:
            await db.uploads.delete_many({"_id": {"$in": old_ids}})

async def get_recent_uploads(n: int = 10) -> list:
    cursor = _get_db().uploads.find({}, {"_id": 0}).sort("ts", -1).limit(n)
    return [doc async for doc in cursor]


# ── users (who's DMed the notify bot — /broadcast's recipient list) ──────
async def add_user(user_id: int, first_name: str = ""):
    await _get_db().users.update_one(
        {"_id": int(user_id)}, {"$set": {"_id": int(user_id), "first_name": first_name}},
        upsert=True)

async def get_all_user_ids() -> list:
    cursor = _get_db().users.find({}, {"_id": 1})
    return [doc["_id"] async for doc in cursor]

async def get_all_channel_ids() -> list:
    """Every distinct registered channel/group chat_id, across all
    categories, deduplicated — used by /broadcast."""
    ids = set()
    cursor = _get_db().channels.find({}, {"chat_id": 1})
    async for doc in cursor:
        ids.add(str(doc["chat_id"]))
    return list(ids)


# ── scheduled notifications ───────────────────────────────────────────────
async def add_scheduled_notification(doc: dict):
    """Returns the raw Mongo _id (ObjectId) — pass it straight into
    set_scheduled_confirmation_message / mark_scheduled_notification_sent,
    no string conversion needed."""
    doc = dict(doc)
    doc["sent"] = False
    result = await _get_db().scheduled.insert_one(doc)
    return result.inserted_id

async def set_scheduled_confirmation_message(_id, chat_id, message_id):
    """Records which chat/message the "🗓 scheduled" confirmation was sent
    as, so the background job can EDIT it in place once the notification
    actually sends (see scheduler.job_send_scheduled_notifications)."""
    await _get_db().scheduled.update_one(
        {"_id": _id}, {"$set": {"confirmation_chat_id": chat_id, "confirmation_message_id": message_id}})

async def get_due_scheduled_notifications(now_iso: str) -> list:
    """Unsent notifications whose scheduled_at has already passed."""
    cursor = _get_db().scheduled.find({"sent": False, "scheduled_at": {"$lte": now_iso}})
    return [doc async for doc in cursor]

async def mark_scheduled_notification_sent(_id):
    await _get_db().scheduled.update_one({"_id": _id}, {"$set": {"sent": True}})

async def get_pending_scheduled_notifications() -> list:
    """All not-yet-sent scheduled notifications (any time), newest first —
    used for an admin-facing /listscheduled-type view if needed later."""
    cursor = _get_db().scheduled.find({"sent": False}).sort("scheduled_at", 1)
    return [doc async for doc in cursor]
