"""
config.py — environment configuration, logging, and shared mutable runtime
state for the SCFiles bot. Every other module imports from here instead of
holding its own copy of admin IDs / backup target / etc, so a change made
in one handler is visible everywhere immediately.

Persistence (admins, backup target, notify channels, upload history) lives
entirely in MongoDB now — see db.py. This module only holds the in-memory
`state` that gets seeded from Mongo at startup (main.py) and kept in sync
by whichever handler changes it.
"""
import logging, os
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed — fine in production, just set real env vars

import pytz

# ── logging ────────────────────────────────────────────────────────────────
LOG_FILE = os.environ.get("LOG_FILE", "bot.log")
_fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
_sh  = logging.StreamHandler(); _sh.setFormatter(_fmt)
_fh  = logging.FileHandler(LOG_FILE, encoding="utf-8"); _fh.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_sh, _fh])
logger = logging.getLogger("scfiles-bot")

# ── env config ────────────────────────────────────────────────────────────
BOT_TOKEN      = os.environ["TELEGRAM_TOKEN"]
BACKEND_URL    = os.environ["BACKEND_URL"].rstrip("/")
TMDB_API_KEY   = os.environ.get("TMDB_API_KEY", "").strip()
_ENV_ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS","").split(",") if x.strip()]
BACKUP_CHAT_ID = os.environ.get("BACKUP_CHAT_ID","").strip()   # only used if Mongo has none set yet
WEB_HOST       = os.environ.get("WEB_HOST","0.0.0.0")
WEB_PORT       = int(os.environ.get("WEB_PORT","8080"))
BOT_WEB_URL    = os.environ.get("BOT_WEB_URL","").rstrip("/")
AUTO_PING_MIN  = int(os.environ.get("AUTO_PING_INTERVAL_MIN","4"))
ADMIN_TOKEN    = os.environ.get("ADMIN_TOKEN","changeme")   # protects /admin

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG  = "https://image.tmdb.org/t/p/original"
TMDB_BACKDROP_IMG = "https://image.tmdb.org/t/p/original"   # landscape backdrops, full quality
IST       = pytz.timezone("Asia/Kolkata")

# ── shared mutable runtime state ─────────────────────────────────────────
# A single object (not bare module globals) so every module that does
# `from config import state` sees live updates when another module sets
# state.BACKUP_TARGET, state.ADMIN_IDS.append(...), etc — no `global`
# statements needed anywhere else in the codebase.
class _State:
    def __init__(self):
        self.BOT_STARTED_AT = datetime.now(IST)
        self.LAST_BACKUP_AT = None
        self.LAST_PING_AT   = None
        self.BACKUP_TARGET  = BACKUP_CHAT_ID   # overwritten from Mongo at startup if set there
        # seeded from ADMIN_IDS env var, extended at runtime by /addadmin
        # (and by whatever's already in Mongo — see main.py startup)
        self.ADMIN_IDS: list[int] = list(_ENV_ADMIN_IDS)

state = _State()
