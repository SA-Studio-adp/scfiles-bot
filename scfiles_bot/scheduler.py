"""scheduler.py — background jobs: periodic backup, periodic self-ping
(keeps free-tier hosts from cold-sleeping), and firing due scheduled
notifications (see handlers/notify_flow.py's Schedule Notification button)."""
import asyncio
from datetime import datetime

import aiohttp

from config import state, IST, BACKEND_URL, BOT_WEB_URL, logger
from api_client import sess
from backup import do_backup
import db
import notify

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

async def job_send_scheduled_notifications():
    """Runs every minute — sends any scheduled notification whose time has
    arrived (see handlers/notify_flow.py's notify_confirm_cb, which stores
    these in Mongo instead of sending immediately when a time was picked
    via the Schedule Notification Web App button)."""
    now_iso = datetime.utcnow().isoformat()
    try:
        due = await db.get_due_scheduled_notifications(now_iso)
    except Exception as e:
        logger.error("job_send_scheduled_notifications: couldn't query Mongo: %s", e)
        return
    for doc in due:
        try:
            sent = await notify.notify_upload(
                doc["kind"], doc["item"], doc.get("poster_url"),
                category=doc.get("category", "hd"), title_override=doc.get("title"),
            )
            logger.info("Scheduled notification %s sent -> %s recipient(s)", doc.get("_id"), sent)
        except Exception as e:
            logger.error("Scheduled notification %s failed to send: %s", doc.get("_id"), e)
        finally:
            # Mark sent regardless of outcome — a stuck failing entry would
            # otherwise retry forever every minute; the failure is already
            # logged above for follow-up.
            await db.mark_scheduled_notification_sent(doc["_id"])
