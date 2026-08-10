"""backup.py — building the backup ZIP / sending backup docs to a chat."""
import asyncio, io, json, zipfile
from datetime import datetime

from telegram.constants import ParseMode

from config import state, IST, logger
from api_client import api_get

async def _payloads() -> dict:
    specs = [("/api/movies","movies.json"),("/api/series","series.json"),("/api/collections","collections.json")]
    results = await asyncio.gather(*[api_get(p, bust=True) for p,_ in specs])
    out = {}
    for (_, fname), data in zip(specs, results):
        if data is None: raise RuntimeError(f"Cannot fetch {fname}")
        out[fname] = json.dumps(data, indent=2, ensure_ascii=False).encode()
    # Always include the current admin list in the backup
    out["admins.json"] = json.dumps(
        {"admin_ids": state.ADMIN_IDS, "env_admin_ids": _ENV_ADMIN_IDS},
        indent=2, ensure_ascii=False
    ).encode()
    return out

async def make_zip():
    ts  = datetime.now(IST).strftime("%Y-%m-%d_%H-%M")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in (await _payloads()).items():
            zf.writestr(f"{ts}_{name}", data)
    buf.seek(0)
    return buf.read(), ts

async def do_backup(bot, target=None):
    dest_str = str(target or state.BACKUP_TARGET).strip()
    if not dest_str: return False, "No backup chat configured — use /setbackup"
    try:    dest = int(dest_str)
    except: dest = dest_str
    ts = datetime.now(IST).strftime("%Y-%m-%d_%H-%M")
    try:
        await bot.send_message(dest, f"💾 <b>Auto-Backup</b> — <code>{ts}</code>", parse_mode=ParseMode.HTML)
        for fname, data in (await _payloads()).items():
            buf = io.BytesIO(data); buf.name = f"{ts}_{fname}"
            await bot.send_document(dest, buf, caption=f"📦 <code>{ts}_{fname}</code>", parse_mode=ParseMode.HTML)
        state.LAST_BACKUP_AT = datetime.now(IST)
        return True, dest_str
    except Exception as e:
        logger.error("Backup failed: %s", e)
        return False, str(e)
