"""
subs_bot.py — the subtitle-extraction bot, built on Pyrogram instead of
python-telegram-bot. Send it a video file, it extracts every subtitle
track and sends each back as "<name>.<lang>.srt", then deletes the
downloaded video and every generated subtitle file off the server.

WHY PYROGRAM AND NOT python-telegram-bot HERE (unlike the admin bot and
notify bot, which stay on PTB): Telegram's *cloud HTTP Bot API* — what
PTB talks to by default — caps bot file downloads at 20MB and uploads at
50MB, full stop, no config gets around it. Pyrogram instead speaks MTProto
directly to Telegram's servers (the same protocol official clients use),
which supports bot file transfers up to 2000MB with zero extra
infrastructure — no local Bot API server, no second deployed service, no
second URL to wire up. That's the whole reason this one module is on a
different library: it's the only bot here that actually moves big files.

Requires a free api_id/api_hash from https://my.telegram.org/apps (log
in with the phone number tied to the bot owner's account) — this is an
MTProto requirement for every Pyrogram client, bot or not. Set
TELEGRAM_API_ID / TELEGRAM_API_HASH / SUB_BOT_TOKEN in .env; the bot is
skipped entirely if SUB_BOT_TOKEN isn't set.
"""
import asyncio, os, shutil, tempfile, time

from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.types import BotCommand, Message

from config import logger
from utils import esc, bold, progress_bar
from subtitle_extract import (
    extract_embedded_subs, extract_hardcoded_subs_ocr,
    ExtractionError, SUBS_OCR_ENABLED,
)

SUB_BOT_TOKEN     = os.environ.get("SUB_BOT_TOKEN", "").strip()
TELEGRAM_API_ID   = int(os.environ.get("TELEGRAM_API_ID", "0") or 0)
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "").strip()

# Real MTProto ceiling for bot uploads/downloads is ~2000MB — default
# comfortably under that; override in .env if you know your files run
# close to the edge.
SUBS_MAX_FILE_MB = int(os.environ.get("SUBS_MAX_FILE_MB", "1900"))
_WORKDIR_ROOT = os.path.join(tempfile.gettempdir(), "scfiles-subs-bot")
_EDIT_MIN_INTERVAL = 3.0  # seconds — stay well under Telegram's edit rate limit

_VIDEO_EXT = (".mkv", ".mp4", ".avi", ".mov", ".webm", ".m4v", ".ts", ".wmv", ".flv")


def _cleanup(path: str):
    try:
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
    except Exception as e:
        logger.warning("subs_bot cleanup failed for %s: %s", path, e)


async def cmd_start(client: Client, message: Message):
    await message.reply_text(
        "🎬 <b>Subtitle Extractor</b>\n\n"
        "Send me a video file and I'll pull out every subtitle track and "
        "send each one back as <code>filename.lang.srt</code>.\n\n"
        f"Max file size: <b>{SUBS_MAX_FILE_MB}MB</b>.\n"
        "Nothing is kept on the server — your video and the extracted "
        "subtitles are deleted right after they're sent to you.",
    )


def _make_download_progress(status_msg: Message):
    """Returns a Pyrogram-compatible progress callback: async def
    cb(current, total). Throttled to _EDIT_MIN_INTERVAL so we don't hit
    Telegram's message-edit rate limit on a fast transfer."""
    state = {"last_edit": 0.0, "last_pct": -1}

    async def _cb(current: int, total: int):
        if not total:
            return
        pct = min(100, current * 100 // total)
        now = time.monotonic()
        if pct == state["last_pct"] or (now - state["last_edit"] < _EDIT_MIN_INTERVAL and pct != 100):
            return
        state["last_edit"], state["last_pct"] = now, pct
        try:
            await status_msg.edit_text(
                f"⏳ <b>Downloading video…</b>\n"
                f"<code>{progress_bar(pct)}</code>\n"
                f"{current / 1024 / 1024:.0f}MB / {total / 1024 / 1024:.0f}MB")
        except Exception:
            pass  # "message not modified" or a transient edit failure — not fatal

    return _cb


async def _run_ocr_with_progress(video_path: str, basename: str, workdir: str, status_msg: Message):
    """Runs extract_hardcoded_subs_ocr (in a worker thread — see
    subtitle_extract.py) while a separate ticker task polls its progress
    and updates status_msg every few seconds, so the bot doesn't go
    silent for however long the scan takes."""
    progress = {"done": 0, "total": 0}

    def _on_progress(done: int, total: int):
        progress["done"], progress["total"] = done, total

    async def _ticker():
        last_pct = -1
        while True:
            await asyncio.sleep(_EDIT_MIN_INTERVAL)
            total = progress["total"]
            if not total:
                continue
            pct = min(100, progress["done"] * 100 // total)
            if pct == last_pct:
                continue
            last_pct = pct
            try:
                await status_msg.edit_text(
                    f"🔎 <b>OCR-scanning for hardcoded subtitles…</b>\n"
                    f"<code>{progress_bar(pct)}</code>\n"
                    f"<i>This can take a while — one frame at a time.</i>")
            except Exception:
                pass

    ticker_task = asyncio.create_task(_ticker())
    try:
        return await extract_hardcoded_subs_ocr(video_path, basename, workdir, on_progress=_on_progress)
    finally:
        ticker_task.cancel()


async def handle_video(client: Client, message: Message):
    tg_media = message.video or message.document
    if tg_media is None:
        return

    orig_name = getattr(tg_media, "file_name", None) or "video.mkv"
    if message.document and not orig_name.lower().endswith(_VIDEO_EXT):
        # A document that isn't a recognizable video extension — ignore quietly
        # rather than trying (and failing) to probe it as one.
        return

    size = getattr(tg_media, "file_size", None) or 0
    if size and size > SUBS_MAX_FILE_MB * 1024 * 1024:
        await message.reply_text(
            f"❌ File is {size / 1024 / 1024:.1f}MB — over the "
            f"{SUBS_MAX_FILE_MB}MB limit configured for this bot.")
        return

    basename = os.path.splitext(orig_name)[0]
    workdir = os.path.join(_WORKDIR_ROOT, f"{message.chat.id}_{int(time.time() * 1000)}")
    os.makedirs(workdir, exist_ok=True)
    video_path = os.path.join(workdir, orig_name)

    status = await message.reply_text("⏳ Downloading video…")
    try:
        await message.download(file_name=video_path, progress=_make_download_progress(status))

        await status.edit_text("🔎 Scanning for subtitle tracks…")
        srt_paths, skipped = await extract_embedded_subs(video_path, basename, workdir)

        if not srt_paths and SUBS_OCR_ENABLED:
            ocr_path = await _run_ocr_with_progress(video_path, basename, workdir, status)
            if ocr_path:
                srt_paths.append(ocr_path)

        if not srt_paths:
            note = ""
            if skipped:
                note = "\n\nFound but couldn't convert:\n" + "\n".join(f"• {esc(s)}" for s in skipped)
            await status.edit_text(f"⚠️ No extractable subtitle tracks found in {bold(orig_name)}.{note}")
            return

        await status.edit_text(f"✅ Found {len(srt_paths)} subtitle track(s) — sending…")
        for srt_path in srt_paths:
            await message.reply_document(srt_path, file_name=os.path.basename(srt_path))

        if skipped:
            await message.reply_text(
                "ℹ️ Some tracks were skipped:\n" + "\n".join(f"• {esc(s)}" for s in skipped))
        await status.delete()

    except ExtractionError as e:
        await status.edit_text(f"❌ {esc(str(e))}")
    except Exception as e:
        logger.error("subs_bot extraction failed: %s", e, exc_info=True)
        await status.edit_text("❌ Something went wrong extracting subtitles.")
    finally:
        # Always clean up — downloaded video + every generated .srt — win or lose.
        _cleanup(workdir)


def build_subs_app() -> Client:
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        raise RuntimeError(
            "TELEGRAM_API_ID / TELEGRAM_API_HASH not set — Pyrogram needs these "
            "even for a bot token. Get them free at https://my.telegram.org/apps"
        )
    # in_memory=True: no .session file written to disk — fine for a
    # container that may not persist disk between deploys; just means a
    # fresh (fast) MTProto auth on every restart instead of reusing one.
    app = Client(
        "scfiles_subs_bot",
        api_id=TELEGRAM_API_ID,
        api_hash=TELEGRAM_API_HASH,
        bot_token=SUB_BOT_TOKEN,
        in_memory=True,
    )
    app.add_handler(MessageHandler(cmd_start, filters.command("start")))
    app.add_handler(MessageHandler(handle_video, filters.video | filters.document))
    return app


async def register_commands(app: Client):
    await app.set_bot_commands([BotCommand("start", "How to use this bot")])
    logger.info("Subs-bot commands registered ✅")
