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
import notify_tokens

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

async def job_send_scheduled_notifications(app=None):
    """Runs every minute — sends any scheduled notification whose time has
    arrived (see web/schedule_picker.py's submit endpoint, which stores
    these in Mongo instead of sending immediately once a time/category/
    title is picked on the Schedule Notification Web App page).

    `app` (the ADMIN bot's Application, optional) is used to EDIT the
    "🗓 scheduled" confirmation message in place into a "✅ sent to N
    channels" result — if not supplied (or the edit fails, e.g. the
    message was deleted), sending still proceeds normally, just without
    that follow-up edit.
    """
    # Housekeeping: expired, never-acted-on notify-prompt tokens don't need
    # to live forever in memory.
    notify_tokens.sweep_expired()

    now_iso = datetime.utcnow().isoformat()
    try:
        due = await db.get_due_scheduled_notifications(now_iso)
    except Exception as e:
        logger.error("job_send_scheduled_notifications: couldn't query Mongo: %s", e)
        return
    for doc in due:
        sent = None
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

        chat_id = doc.get("confirmation_chat_id")
        message_id = doc.get("confirmation_message_id")
        if app and chat_id and message_id:
            title = doc.get("title", "")
            when = doc.get("scheduled_at_display", "")
            if sent is not None and sent > 0:
                new_text = f'✅ <b>"{title}"</b> sent to <b>{sent}</b> group(s)/channel(s) successfully at <b>{when}</b>.'
            elif sent == 0:
                new_text = f'⚠️ <b>"{title}"</b> — scheduled send at <b>{when}</b> failed for every channel. Check bot logs.'
            else:
                new_text = f'⚠️ <b>"{title}"</b> — scheduled send at <b>{when}</b> found no channels registered for that category.'
            try:
                await app.bot.edit_message_text(chat_id=chat_id, message_id=message_id,
                                                text=new_text, parse_mode="HTML")
            except Exception as e:
                logger.warning("Couldn't edit scheduled-confirmation message %s/%s: %s",
                               chat_id, message_id, e)
