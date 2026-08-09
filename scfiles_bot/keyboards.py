"""keyboards.py — inline keyboards shared by several handlers."""
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import BOT_WEB_URL

def main_kb():
    rows = [
        [InlineKeyboardButton("🎬 Movies",        callback_data="m_movies"),
         InlineKeyboardButton("📺 Series",         callback_data="m_series")],
        [InlineKeyboardButton("🗂 Collections",    callback_data="m_cols"),
         InlineKeyboardButton("🔍 TMDB Search",    callback_data="m_tmdb")],
        [InlineKeyboardButton("📊 Stats",          callback_data="m_stats"),
         InlineKeyboardButton("🌐 Status",         callback_data="m_status")],
        [InlineKeyboardButton("💾 Backup Now",     callback_data="m_backup"),
         InlineKeyboardButton("📦 Backup ZIP",     callback_data="m_backupzip")],
        [InlineKeyboardButton("➕ Add Movie",      callback_data="m_addmovie"),
         InlineKeyboardButton("📺 Add Series",     callback_data="m_addseries")],
        [InlineKeyboardButton("🗂 Add Collection", callback_data="m_addcollection"),
         InlineKeyboardButton("✏️ Edit Movie",     callback_data="m_editmovie")],
    ]
    if BOT_WEB_URL:
        rows.append([InlineKeyboardButton("🩺 Web Dashboard ↗", url=BOT_WEB_URL)])
    return InlineKeyboardMarkup(rows)

def back_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("« Back to Menu", callback_data="m_back")]])

def yes_no_kb(yes_cb: str, no_cb: str):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirm", callback_data=yes_cb),
        InlineKeyboardButton("❌ Cancel",  callback_data=no_cb),
    ]])

def ep_more_kb():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("➕ Add another episode", callback_data="ep_more"),
        InlineKeyboardButton("✅ Done / Save",          callback_data="ep_done"),
    ]])

def mov_more_kb():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("➕ Add another movie", callback_data="mov_more"),
        InlineKeyboardButton("✅ Done / Save",        callback_data="mov_done"),
    ]])
