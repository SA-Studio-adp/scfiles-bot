"""utils.py — small stateless helpers used across the whole bot."""
import time

# ── HTML formatting helpers (parse_mode=HTML everywhere) ────────────────
def esc(v) -> str:
    return str(v).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def bold(v):   return f"<b>{esc(v)}</b>"
def code(v):   return f"<code>{esc(v)}</code>"
def italic(v): return f"<i>{esc(v)}</i>"

def progress_bar(pct: float, width: int = 20) -> str:
    """Text progress bar, e.g. progress_bar(35) -> '███████░░░░░░░░░░░░░ 35%'."""
    pct = max(0, min(100, pct))
    filled = int(width * pct / 100)
    return "█" * filled + "░" * (width - filled) + f" {pct:.0f}%"

# ── TTL cache (30 s) ──────────────────────────────────────────────────────────
_CACHE: dict[str, tuple[float, object]] = {}
_CACHE_TTL = 30

def _cache_get(key: str):
    e = _CACHE.get(key)
    return e[1] if e and time.monotonic() - e[0] < _CACHE_TTL else None

def _cache_set(key: str, val):
    _CACHE[key] = (time.monotonic(), val)

def _cache_bust(*prefixes: str):
    """Invalidate all cache entries whose key starts with any of the given prefixes."""
    for p in prefixes:
        for k in list(_CACHE):
            if k.startswith(p):
                del _CACHE[k]

def _cache_bust_all():
    """Nuke the entire cache — used after any write operation."""
    _CACHE.clear()
