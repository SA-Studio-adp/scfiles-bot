"""api_client.py — the shared aiohttp session and the thin JSON wrappers
(api_get/post/put/delete) used to talk to the SCFiles backend, plus a
30s TTL response cache so /stats, /movies etc don't hammer the backend."""
import asyncio, re, time

import aiohttp

from config import BACKEND_URL, logger

# ── TTL cache (30 s) ──────────────────────────────────────────────────────
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

# ── http session / api wrappers ───────────────────────────────────────────
_SESSION: aiohttp.ClientSession | None = None
_SESSION_LOCK: asyncio.Lock | None = None

async def sess() -> aiohttp.ClientSession:
    global _SESSION, _SESSION_LOCK
    # Initialise the lock lazily — asyncio.Lock() requires a running event loop
    # and cannot be created safely at module import time (Python 3.10+).
    if _SESSION_LOCK is None:
        _SESSION_LOCK = asyncio.Lock()
    async with _SESSION_LOCK:
        if _SESSION is None or _SESSION.closed:
            _SESSION = aiohttp.ClientSession()
    return _SESSION

async def _wake():
    s = await sess()
    for _ in range(4):
        try:
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status < 500: return True
        except Exception: pass
        await asyncio.sleep(8)
    return False

async def api_get(path: str, *, bust: bool = False):
    if not bust:
        cached = _cache_get(path)
        if cached is not None: return cached
    s = await sess()
    for attempt in range(2):
        try:
            async with s.get(f"{BACKEND_URL}{path}",
                             timeout=aiohttp.ClientTimeout(total=20)) as r:
                data = await r.json()
                _cache_set(path, data)
                return data
        except Exception as e:
            logger.error("API GET %s #%d: %s", path, attempt+1, e)
            if attempt == 0: await _wake()
    return None

async def api_post(path: str, data: dict):
    _cache_bust_all()  # clear all cached GET responses after any write
    s = await sess()
    for attempt in range(2):
        try:
            async with s.post(f"{BACKEND_URL}{path}", json=data,
                              timeout=aiohttp.ClientTimeout(total=20)) as r:
                return await r.json()
        except Exception as e:
            logger.error("API POST %s #%d: %s", path, attempt+1, e)
            if attempt == 0: await _wake()
    return None

async def api_put(path: str, data: dict):
    """Alias kept for compatibility — backend has no PUT; always uses POST upsert."""
    # Strip trailing /{id} to get the base endpoint (e.g. /api/series/{id} → /api/series)
    base = re.sub(r'/[^/]+$', '', path) if path.count('/') > 2 else path
    return await api_post(base, data)


async def api_delete(path: str):
    _cache_bust_all()  # clear all cached GET responses after any delete
    s = await sess()
    for attempt in range(2):
        try:
            async with s.delete(f"{BACKEND_URL}{path}",
                                timeout=aiohttp.ClientTimeout(total=20)) as r:
                return await r.json()
        except Exception as e:
            logger.error("API DEL %s #%d: %s", path, attempt+1, e)
            if attempt == 0: await _wake()
    return None

def api_err(r) -> str:
    if r is None: return "No response (backend may be sleeping)"
    if isinstance(r, dict): return r.get("error", str(r))
    return str(r)

async def fetch_all(bust: bool = False) -> tuple[list, list, dict]:
    """Fetch movies, series, collections concurrently."""
    mv, sr, co = await asyncio.gather(
        api_get("/api/movies",      bust=bust),
        api_get("/api/series",      bust=bust),
        api_get("/api/collections", bust=bust),
    )
    return mv or [], sr or [], co or {}



async def close_session():
    """Called once during shutdown."""
    global _SESSION
    if _SESSION and not _SESSION.closed:
        await _SESSION.close()
