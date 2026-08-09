"""scheduler.py — the two background jobs: periodic backup, periodic
self-ping (keeps free-tier hosts from cold-sleeping)."""
import asyncio
from datetime import datetime

import aiohttp

from config import state, IST, BACKEND_URL, BOT_WEB_URL, logger
from api_client import sess
from backup import do_backup

async def job_backup(app):
    ok, info = await do_backup(app.bot)
    logger.info("Scheduled backup → ok=%s info=%s", ok, info)

async def job_ping():
    urls = [BACKEND_URL]
    if BOT_WEB_URL: urls.append(f"{BOT_WEB_URL}/health")
    s = await sess()
    for url in urls:
        try:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                logger.info("Ping %s → %s", url, r.status)
        except Exception as e: logger.warning("Ping %s: %s", url, e)
    state.LAST_PING_AT = datetime.now(IST)
