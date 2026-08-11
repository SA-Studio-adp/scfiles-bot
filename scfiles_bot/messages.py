"""
messages.py — SCFiles channel-notification templates
─────────────────────────────────────────────────────────────────────────────
Edit the strings below to change what gets posted. No other file needs to
change — notify.py and notify_bot.py both import from here.

Each upload is ONE Telegram message (parse_mode=HTML). When there's a
poster, notify.py prepends an invisible link to it so Telegram shows it as
a large image preview above this text — you don't need to (and can't)
reference the image from inside these templates; just write the text that
goes below/alongside it. A "Join our channel" BUTTON is attached under
every message automatically (not part of these templates either).

Syntax: Telegram Bot API HTML — https://core.telegram.org/bots/api#html-style
  <b>bold</b>   <i>italic</i>   <code>code</code>   <a href="url">text</a>
Only & < > need escaping in HTML, and placeholders in {curly_braces} below
are already escaped before being inserted (title, overview, year, genre,
season_line, movie_count) — don't escape them again here. Plain text with
no tags at all is perfectly valid too.

PROMO_LINK / PROMO_BUTTON_TEXT control the "Join our channel" button
attached under every upload notification.
"""

PROMO_LINK = "https://t.me/sc_files4"
PROMO_BUTTON_TEXT = "🔔 Join our Channel"

TEMPLATES = {

    "MOVIE": (
        '🎬 <b>"{title}"</b>\n\n'
        "📅 {year}  •  🎭 {genre}\n\n"
        '"{overview}"'
    ),

    "SERIES": (
        '📺 <b>"{title}"</b>\n\n'
        "📅 {year}  •  🎭 {genre}\n\n"
        '"{overview}"'
    ),

    # new episode(s) added to an existing series
    "EPISODE_UPDATE": (
        '📺 <b>"{title}"</b> — New Episode!\n\n'
        "🆕 {episode_line}\n"
        "📅 {year}  •  🎭 {genre}\n\n"
        '"{overview}"'
    ),

    "COLLECTION": (
        '🗂 <b>"{title}" Collection</b>\n\n'
        "🎬 {movie_count} movies inside"
    ),

    # ── notify-bot commands (sent by the SAME bot that posts uploads) ────
    "BOT_START": (
        "👋 <b>Welcome!</b>\n\n"
        "This channel is fed automatically — new <b>movies</b>, <b>series</b>, "
        "and <b>collections</b> get posted here the moment they're uploaded.\n\n"
        "Use /uploads to see the last 10 uploads."
    ),

    "BOT_UPLOADS_HEADER": "📋 <b>Last {count} uploads</b>\n",

    "BOT_UPLOADS_ITEM": "{icon} <b>{title}</b>  ·  {category_label}  ·  <i>{when}</i>",

    "BOT_UPLOADS_EMPTY": "📭 No uploads logged yet.",

}
