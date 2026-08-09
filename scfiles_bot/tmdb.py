"""tmdb.py — TMDB lookups (via the backend's /api/tmdb proxy, falling back
to a direct TMDB call if TMDB_API_KEY is set), plus the HTML formatters
used to preview a movie/show before it's added."""
import urllib.parse

import aiohttp

from config import BACKEND_URL, TMDB_BASE, TMDB_IMG, TMDB_API_KEY, logger
from api_client import sess
from utils import esc

async def tmdb_get(path: str):
    try:
        s   = await sess()
        url = f"{BACKEND_URL}/api/tmdb?path={urllib.parse.quote(path, safe='/?&=')}&language=en-US"
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200: return await r.json()
        if TMDB_API_KEY:
            sep = "&" if "?" in path else "?"
            async with s.get(f"{TMDB_BASE}{path}{sep}api_key={TMDB_API_KEY}&language=en-US",
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json() if r.status == 200 else None
    except Exception as e:
        logger.error("TMDB %s: %s", path, e)
    return None

async def tmdb_movie(tid: int): return await tmdb_get(f"/movie/{tid}?")
async def tmdb_tv(tid: int):    return await tmdb_get(f"/tv/{tid}?")
async def tmdb_search(q: str, t: str = "movie") -> list:
    r = await tmdb_get(f"/search/{t}?query={urllib.parse.quote(q)}")
    return (r or {}).get("results", [])[:5]

def fmt_movie(m: dict) -> str:
    return (f"🎬 <b>{esc(m.get('title','?'))}</b> <i>({esc((m.get('release_date') or '')[:4])})</i>\n"
            f"⭐ <b>{m.get('vote_average',0):.1f}</b>/10  •  ⏱ {m.get('runtime',0)}m\n"
            f"🎭 {esc(', '.join(g['name'] for g in m.get('genres',[])) or 'N/A')}  •  📌 {esc(m.get('status',''))}\n\n"
            f"📝 <i>{esc((m.get('overview') or 'No overview.')[:300])}</i>")

def fmt_tv(t: dict) -> str:
    return (f"📺 <b>{esc(t.get('name','?'))}</b> <i>({esc((t.get('first_air_date') or '')[:4])})</i>\n"
            f"⭐ <b>{t.get('vote_average',0):.1f}</b>/10  •  {t.get('number_of_seasons','?')}S / {t.get('number_of_episodes','?')}ep\n"
            f"🎭 {esc(', '.join(g['name'] for g in t.get('genres',[])) or 'N/A')}  •  📌 {esc(t.get('status',''))}\n\n"
            f"📝 <i>{esc((t.get('overview') or 'No overview.')[:300])}</i>")

def poster(info: dict):
    p = info.get("poster_path")
    if not p:
        return None
    # Backend TMDB proxy already returns full URLs (attachImages prefixes them).
    # Only prepend TMDB_IMG when using the direct TMDB API fallback (raw paths).
    if str(p).startswith("http"):
        return p
    return f"{TMDB_IMG}{p}"
