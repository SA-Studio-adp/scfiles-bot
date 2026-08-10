"""
messages.py — SCFiles channel-notification templates
─────────────────────────────────────────────────────────────────────────────
Edit the strings below to change what gets posted. No other file needs to
change — notify.py and notify_bot.py both import from here.

Each upload is ONE Telegram message: the TMDB poster with this whole
template as its caption underneath, plus a "Join our channel" button
attached below that. Title goes first in the template text, so it reads
title → year/genre → story even though it's all one caption under the
poster.

Syntax: Telegram Bot API MarkdownV2 — https://core.telegram.org/bots/api#markdownv2-style
  *bold*   _italic_   `code`
  Literal special chars ( _ * [ ] ( ) ~ ` > # + - = | { } . ! ) must be
  escaped with a backslash when NOT used for formatting. Placeholders in
  {curly_braces} below are filled in automatically — values that come from
  your database/TMDB (title, overview, year, genre, season_line, etc) are
  ALREADY escaped before being inserted, so don't escape them again here.

PROMO_LINK / PROMO_BUTTON_TEXT control the "Join our channel" button
attached under every upload notification (a button, not a text link —
buttons are guaranteed to render as tappable in every Telegram client).
"""

PROMO_LINK = "https://t.me/sc_files4"
PROMO_BUTTON_TEXT = "🔔 Join our Channel"

TEMPLATES = {

    "MOVIE": (
        '🎬 "{title}"\n\n'
        "📅 {year}  •  🎭 {genre}\n\n"
        '"{overview}"'
    ),

    "SERIES": (
        '📺 "{title}"\n\n'
        "📅 {year}  •  🎭 {genre}\n\n"
        '"{overview}"'
    ),

    # new episode(s) added to an existing series
    "EPISODE_UPDATE": (
        '📺 "{title}" — New Episode\\!\n\n'
        "🆕 {episode_line}\n"
        "📅 {year}  •  🎭 {genre}\n\n"
        '"{overview}"'
    ),

    "COLLECTION": (
        '🗂 "{title}" Collection\n\n'
        "🎬 {movie_count} movies inside"
    ),

    # ── notify-bot commands (sent by the SAME bot that posts uploads) ────
    "BOT_START": (
        "👋 *Welcome\\!*\n\n"
        "This channel is fed automatically — new *movies*, *series*, and "
        "*collections* get posted here the moment they're uploaded\\.\n\n"
        "Use /uploads to see the last 10 uploads\\."
    ),

    "BOT_UPLOADS_HEADER": "📋 *Last {count} uploads*\n",

    "BOT_UPLOADS_ITEM": "{icon} *{title}*  ·  {category_label}  ·  _{when}_",

    "BOT_UPLOADS_EMPTY": "📭 No uploads logged yet\\.",

}
