"""
main.py — SCFiles Bot entrypoint. Builds the aiohttp web server and the
python-telegram-bot Application, wires every handler from the split
modules together, and runs both until shutdown.

Run with:  python -m scfiles_bot.main   (or: python run.py from the repo root)
"""
import asyncio, warnings
from datetime import datetime, timedelta

from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, BotCommand
from telegram.ext import (Application, CommandHandler, CallbackQueryHandler,
                           MessageHandler, ConversationHandler, filters)

from config import (state, logger, IST, BOT_TOKEN, BACKEND_URL, WEB_HOST, WEB_PORT,
                     AUTO_PING_MIN, ADMIN_TOKEN)
import db
from errors import error_handler
from scheduler import job_backup, job_ping

from web.dashboard import web_dashboard, web_health, web_backup_zip, web_logs, web_admin_logs
from web.admin_panel import web_admin

from handlers.states import *  # noqa: F401,F403 — all AM_/AS_/AC_/DM_/.../EC_ constants

from handlers import basic as h_basic
from handlers import channels as h_channels
from handlers import movie_add as h_movie_add
from handlers import movie_edit as h_movie_edit
from handlers import series_add as h_series_add
from handlers import series_edit as h_series_edit
from handlers import collection_add as h_col_add
from handlers import collection_edit as h_col_edit
from handlers import delete as h_delete
from handlers import tmdb_search as h_tmdb
from handlers import menu as h_menu
from handlers import notify_flow as h_notify
from notify import NOTIFY_BOT_TOKEN
from notify_bot import build_notify_app, register_commands as register_notify_commands


async def main():
    # MongoDB is now required infrastructure (replaces the old local .json
    # state files) — fail fast and loudly if it's unreachable, rather than
    # letting the first /addadmin or /setbackup silently break later.
    try:
        await db.ping()
        logger.info("MongoDB connection OK (db=%s)", db.MONGODB_DB)
    except Exception as e:
        logger.error("MongoDB connection FAILED: %s", e)
        logger.error("Set MONGODB_URI (and optionally MONGODB_DB) and try again.")
        raise

    state.BACKUP_TARGET = await db.get_backup_target()
    # Load any admins that were added at runtime in a previous session
    for uid in await db.get_admin_ids():
        if uid not in state.ADMIN_IDS:
            state.ADMIN_IDS.append(uid)
    logger.info("=" * 56)
    logger.info("SCFiles Bot starting")
    logger.info("Backend : %s", BACKEND_URL)
    logger.info("Admins  : %s", state.ADMIN_IDS or "ALL")
    logger.info("Backup  : %s", state.BACKUP_TARGET or "NOT SET")
    logger.info("Admin UI: /admin  (ADMIN_TOKEN set: %s)", bool(ADMIN_TOKEN != "changeme"))
    logger.info("=" * 56)

    # ── web server ────────────────────────────────────────────────────────
    web_app = web.Application()
    web_app.router.add_get("/",            web_dashboard)
    web_app.router.add_get("/health",      web_health)
    web_app.router.add_get("/backup/all",  web_backup_zip)
    web_app.router.add_get("/logs",        web_logs)        # public (raw log)
    web_app.router.add_get("/admin",       web_admin)       # token-protected admin panel
    web_app.router.add_get("/admin/logs",  web_admin_logs)  # token-protected log for panel
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, WEB_HOST, WEB_PORT).start()
    logger.info("Web server on %s:%s", WEB_HOST, WEB_PORT)

    # ── telegram bot ──────────────────────────────────────────────────────
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .build()
    )

    CB_ALL = filters.TEXT & ~filters.COMMAND

    warnings.filterwarnings(
        "ignore",
        message=".*per_message=False.*CallbackQueryHandler.*",
        category=UserWarning,
    )

    # Shared post-upload notify states — spliced into every conversation
    # that can end in a successful upload (addmovie/addseries/addcollection/
    # editseries). See handlers/notify_flow.py.
    def _notify_states():
        return {
            NOTIFY_ASK:     [CallbackQueryHandler(h_notify.notify_ask_cb,     pattern="^ntf_")],
            NOTIFY_CAT:     [CallbackQueryHandler(h_notify.notify_cat_cb,     pattern="^ntf_")],
            NOTIFY_TITLE:   [MessageHandler(CB_ALL, h_notify.notify_title_msg)],
            NOTIFY_CONFIRM: [CallbackQueryHandler(h_notify.notify_confirm_cb, pattern="^ntf_")],
        }

    # ── add movie ─────────────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("addmovie", h_movie_add.cmd_addmovie)],
        states={
            AM_TMDB:   [MessageHandler(CB_ALL, h_movie_add.am_tmdb)],
            AM_EXTRA:  [MessageHandler(CB_ALL, h_movie_add.am_extra)],
            AM_DL480:  [MessageHandler(CB_ALL, h_movie_add.am_dl480)],
            AM_DL720:  [MessageHandler(CB_ALL, h_movie_add.am_dl720)],
            AM_DL1080: [MessageHandler(CB_ALL, h_movie_add.am_dl1080)],
            AM_SUB:    [MessageHandler(CB_ALL, h_movie_add.am_sub)],
            AM_POS:    [CallbackQueryHandler(h_movie_add.am_pos_cb,     pattern="^pos_")],
            AM_CONFIRM:[CallbackQueryHandler(h_movie_add.am_confirm_cb, pattern="^mov_(confirm|cancel)")],
            **_notify_states(),
        },
        fallbacks=[CommandHandler("cancel", h_basic.cmd_cancel)],
        per_message=False))

    # ── add series ────────────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("addseries", h_series_add.cmd_addseries)],
        states={
            AS_TMDB:   [MessageHandler(CB_ALL, h_series_add.as_tmdb)],
            AS_SN:     [MessageHandler(CB_ALL, h_series_add.as_sn)],
            AS_EP:     [MessageHandler(CB_ALL, h_series_add.as_ep)],
            AS_EP360:  [MessageHandler(CB_ALL, h_series_add.as_ep360)],
            AS_EP720:  [MessageHandler(CB_ALL, h_series_add.as_ep720)],
            AS_EP1080: [MessageHandler(CB_ALL, h_series_add.as_ep1080)],
            AS_EP_SUB: [MessageHandler(CB_ALL, h_series_add.as_ep_sub)],
            AS_EP_MORE:[CallbackQueryHandler(h_series_add.as_ep_more_cb, pattern="^ep_"),
                        CallbackQueryHandler(h_series_add.as_confirm_cb, pattern="^sr_")],
            **_notify_states(),
        },
        fallbacks=[CommandHandler("cancel", h_basic.cmd_cancel)],
        per_message=False))

    # ── edit series ───────────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("editseries", h_series_edit.cmd_editseries)],
        states={
            ESS_ID:      [MessageHandler(CB_ALL, h_series_edit.ess_id)],
            ESS_ACTION:  [CallbackQueryHandler(h_series_edit.ess_action_cb,  pattern="^ess_")],
            ESS_EDIT_PICK:[CallbackQueryHandler(h_series_edit.ess_edit_pick_cb, pattern="^(essedit_|ess_cancel)")],
            ESS_SN_PICK: [CallbackQueryHandler(h_series_edit.ess_sn_pick_cb, pattern="^(esssn_|ess_cancel)")],
            ESS_SN:      [MessageHandler(CB_ALL, h_series_edit.ess_sn)],
            ESS_EP:      [MessageHandler(CB_ALL, h_series_edit.ess_ep)],
            ESS_EP360:   [MessageHandler(CB_ALL, h_series_edit.ess_ep360)],
            ESS_EP720:   [MessageHandler(CB_ALL, h_series_edit.ess_ep720)],
            ESS_EP1080:  [MessageHandler(CB_ALL, h_series_edit.ess_ep1080)],
            ESS_EP_SUB:  [MessageHandler(CB_ALL, h_series_edit.ess_ep_sub)],
            ESS_EP_MORE: [CallbackQueryHandler(h_series_edit.ess_ep_more_cb, pattern="^ep_")],
            ESS_DEL_EP:  [CallbackQueryHandler(h_series_edit.ess_del_ep_cb,  pattern="^ess_")],
            **_notify_states(),
        },
        fallbacks=[CommandHandler("cancel", h_basic.cmd_cancel)],
        per_message=False))

    # ── add collection ────────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("addcollection", h_col_add.cmd_addcollection)],
        states={
            AC_ID:       [MessageHandler(CB_ALL, h_col_add.ac_id)],
            AC_NAME:     [MessageHandler(CB_ALL, h_col_add.ac_name)],
            AC_BANNER:   [MessageHandler(CB_ALL, h_col_add.ac_banner)],
            AC_BGMUSIC:  [MessageHandler(CB_ALL, h_col_add.ac_bgmusic)],
            AC_MOV_TMDB: [MessageHandler(CB_ALL, h_col_add.ac_mov_tmdb)],
            AC_MOV_QUAL: [CallbackQueryHandler(h_col_add.ac_mov_qual_cb, pattern="^acq_")],
            AC_MOV_DL:   [MessageHandler(CB_ALL, h_col_add.ac_mov_dl)],
            AC_MOV_MORE: [CallbackQueryHandler(h_col_add.ac_mov_more_cb, pattern="^acm_"),
                          CallbackQueryHandler(h_col_add.ac_confirm_cb,  pattern="^col_")],
            **_notify_states(),
        },
        fallbacks=[CommandHandler("cancel", h_basic.cmd_cancel)],
        per_message=False))

    # ── edit collection ───────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("editcollection", h_col_edit.cmd_editcollection)],
        states={
            EC_ID:          [MessageHandler(CB_ALL, h_col_edit.ec_id)],
            EC_ACTION:      [CallbackQueryHandler(h_col_edit.ec_action_cb, pattern="^(ecf_|ec_)")],
            EC_FIELD_VALUE: [MessageHandler(CB_ALL, h_col_edit.ec_field_value)],
            EC_MOV_TMDB:    [MessageHandler(CB_ALL, h_col_edit.ec_mov_tmdb)],
            EC_MOV_QUAL:    [CallbackQueryHandler(h_col_edit.ec_mov_qual_cb, pattern="^ecq_")],
            EC_MOV_DL:      [MessageHandler(CB_ALL, h_col_edit.ec_mov_dl)],
            EC_DEL_MOV_PICK:[CallbackQueryHandler(h_col_edit.ec_del_mov_cb, pattern="^(ecdelmov_|ec_back)")],
        },
        fallbacks=[CommandHandler("cancel", h_basic.cmd_cancel)],
        per_message=False))

    # ── delete ────────────────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("delmovie", h_delete.cmd_delmovie)],
        states={DM_ID: [MessageHandler(CB_ALL, h_delete.dm_id)]},
        fallbacks=[CommandHandler("cancel", h_basic.cmd_cancel)],
        per_message=False))

    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("delseries", h_delete.cmd_delseries)],
        states={DS_ID: [MessageHandler(CB_ALL, h_delete.ds_id)]},
        fallbacks=[CommandHandler("cancel", h_basic.cmd_cancel)],
        per_message=False))

    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("delcollection", h_delete.cmd_delcollection)],
        states={DC_ID: [MessageHandler(CB_ALL, h_delete.dc_id)]},
        fallbacks=[CommandHandler("cancel", h_basic.cmd_cancel)],
        per_message=False))

    # ── edit movie ────────────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("editmovie", h_movie_edit.cmd_editmovie)],
        states={
            EM_ID:   [MessageHandler(CB_ALL, h_movie_edit.em_id)],
            EM_VALUE:[CallbackQueryHandler(h_movie_edit.em_field_cb, pattern="^ef_"),
                      MessageHandler(CB_ALL, h_movie_edit.em_value)],
        },
        fallbacks=[CommandHandler("cancel", h_basic.cmd_cancel)],
        per_message=False))

    # ── tmdb search ───────────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("tmdb", h_tmdb.cmd_tmdb)],
        states={
            TQ_TYPE: [CallbackQueryHandler(h_tmdb.tq_type_cb, pattern="^tq_")],
            TQ_QUERY:[MessageHandler(CB_ALL, h_tmdb.tq_query)],
        },
        fallbacks=[CommandHandler("cancel", h_basic.cmd_cancel)],
        per_message=False))

    # ── standalone commands ───────────────────────────────────────────────
    # NOTE: cmd_editseries / cmd_editcollection are NOT listed here — they're
    # already entry_points in their ConversationHandlers above.
    from auth import cmd_addadmin, cmd_removeadmin, cmd_listadmins
    for cmd, fn in [
        ("start",          h_basic.cmd_start),
        ("help",           h_basic.cmd_help),
        ("cancel",         h_basic.cmd_cancel),
        ("status",         h_basic.cmd_status),
        ("stats",          h_basic.cmd_stats),
        ("movies",         h_basic.cmd_movies),
        ("series",         h_basic.cmd_series),
        ("collections",    h_basic.cmd_collections),
        ("logs",           h_basic.cmd_logs),
        ("backup",         h_basic.cmd_backup),
        ("backupzip",      h_basic.cmd_backupzip),
        ("setbackup",      h_basic.cmd_setbackup),
        ("addadmin",       cmd_addadmin),
        ("removeadmin",    cmd_removeadmin),
        ("listadmins",     cmd_listadmins),
        ("addchannel",     h_channels.cmd_addchannel),
        ("removechannel",  h_channels.cmd_removechannel),
        ("listchannels",   h_channels.cmd_listchannels),
    ]:
        app.add_handler(CommandHandler(cmd, fn))

    # ── global error handler ──────────────────────────────────────────────
    app.add_error_handler(error_handler)

    # ── global callback handlers ──────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(h_menu.menu_cb,             pattern="^m_"))
    app.add_handler(CallbackQueryHandler(h_delete.del_confirm_cb,    pattern="^delconfirm_"))
    app.add_handler(CallbackQueryHandler(h_delete.del_cancel_cb,     pattern="^delcancel"))
    app.add_handler(CallbackQueryHandler(h_channels.channel_category_cb, pattern="^chnl_"))
    # Forwarding a message from a channel/group into a DM (no command) also
    # starts channel registration — see handlers/channels.py.
    app.add_handler(MessageHandler(
        filters.FORWARDED & filters.ChatType.PRIVATE & ~filters.COMMAND,
        h_channels.handle_forwarded_message))

    # ── scheduler ─────────────────────────────────────────────────────────
    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(job_backup, "interval", days=2, args=[app])
    scheduler.add_job(job_ping,   "interval", minutes=AUTO_PING_MIN,
                       next_run_time=datetime.now(IST) + timedelta(seconds=30))
    scheduler.start()
    logger.info("Scheduler: backup every 2d, ping every %dm", AUTO_PING_MIN)

    try:
        notify_app = None
        await app.initialize()
        await app.start()

        for attempt in range(4):
            try:
                await app.bot.set_my_commands([BotCommand(c, d) for c, d in [
                    ("start",          "Main menu"),
                    ("help",           "All commands"),
                    ("status",         "Server health"),
                    ("stats",          "DB statistics"),
                    ("movies",         "List movies"),
                    ("series",         "List series"),
                    ("collections",    "List collections"),
                    ("addmovie",       "Add a movie"),
                    ("addseries",      "Add a series"),
                    ("addcollection",  "Add a collection"),
                    ("editmovie",      "Edit a movie field"),
                    ("editseries",     "Add/edit/remove episodes"),
                    ("editcollection", "Edit a collection"),
                    ("delmovie",       "Delete a movie"),
                    ("delseries",      "Delete a series"),
                    ("delcollection",  "Delete a collection"),
                    ("tmdb",           "Search TMDB"),
                    ("backup",         "Send backup files"),
                    ("backupzip",      "Download backup ZIP"),
                    ("setbackup",      "Set backup channel"),
                    ("addadmin",       "Add a new admin"),
                    ("removeadmin",    "Remove an admin"),
                    ("listadmins",     "List all admins"),
                    ("addchannel",     "Register this channel for notifications"),
                    ("removechannel",  "Unregister a channel"),
                    ("listchannels",   "List registered channels"),
                    ("logs",           "View bot logs"),
                    ("cancel",         "Cancel current operation"),
                ]])
                logger.info("Bot commands registered ✅")
                break
            except Exception as e:
                if attempt < 3:
                    logger.warning("set_my_commands attempt %d failed: %s — retrying in 5s…", attempt + 1, e)
                    await asyncio.sleep(5)
                else:
                    logger.error("set_my_commands failed after 4 attempts: %s — continuing anyway", e)

        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook cleared — waiting 15 s for old instance to finish its long-poll…")
        except Exception as e:
            logger.warning("delete_webhook: %s", e)
        await asyncio.sleep(15)

        for _poll_attempt in range(12):
            try:
                await app.updater.start_polling(
                    drop_pending_updates=True,
                    allowed_updates=Update.ALL_TYPES,
                    bootstrap_retries=-1,
                )
                logger.info("Bot polling started ✅")
                break
            except Exception as e:
                if "Conflict" in type(e).__name__ or "409" in str(e):
                    wait = min(10 * (_poll_attempt + 1), 30)
                    logger.info(
                        "409 Conflict — old instance still polling (attempt %d/12). "
                        "Waiting %ds for it to die…", _poll_attempt + 1, wait)
                    await asyncio.sleep(wait)
                else:
                    logger.error("start_polling failed: %s", e)
                    raise

        # ── notify bot (optional second Application) ────────────────────
        notify_app = None
        if NOTIFY_BOT_TOKEN:
            try:
                notify_app = build_notify_app(NOTIFY_BOT_TOKEN)
                notify_app.add_error_handler(error_handler)  # was missing — failures were silent
                await notify_app.initialize()
                await notify_app.start()
                await register_notify_commands(notify_app)
                try:
                    await notify_app.bot.delete_webhook(drop_pending_updates=True)
                except Exception as e:
                    logger.warning("notify bot delete_webhook: %s", e)
                await notify_app.updater.start_polling(
                    drop_pending_updates=True,
                    allowed_updates=Update.ALL_TYPES,
                    bootstrap_retries=-1,
                )
                logger.info("Notify bot polling started ✅ (/start, /uploads)")
            except Exception as e:
                logger.error("Notify bot failed to start: %s — continuing without it", e)
                notify_app = None
        else:
            logger.info("NOTIFY_BOT_TOKEN not set — channel notifications disabled")

        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down…")
        try:
            if app.updater.running:
                await app.updater.stop()
        except Exception as e:
            logger.warning("updater.stop(): %s", e)
        try:
            if app.running:
                await app.stop()
        except Exception as e:
            logger.warning("app.stop(): %s", e)
        await app.shutdown()
        if notify_app is not None:
            try:
                if notify_app.updater.running:
                    await notify_app.updater.stop()
                if notify_app.running:
                    await notify_app.stop()
                await notify_app.shutdown()
            except Exception as e:
                logger.warning("notify_app shutdown: %s", e)
        await runner.cleanup(); scheduler.shutdown(wait=False)
        from api_client import close_session
        await close_session()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
