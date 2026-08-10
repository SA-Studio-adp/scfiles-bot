"""
messages.py — SCFiles channel-notification templates
─────────────────────────────────────────────────────────────────────────────
Edit the strings below to change what gets posted. No other file needs to
change — notify.py and notify_bot.py both import from here.

Syntax: Telegram Bot API MarkdownV2 — https://core.telegram.org/bots/api#markdownv2-style
  *bold*   _italic_   `code`   [text](url)
  Literal special chars ( _ * [ ] ( ) ~ ` > # + - = | { } . ! ) must be
  escaped with a backslash when NOT used for formatting. Placeholders in
  {curly_braces} below are filled in automatically — values that come from
  your database/TMDB (title, overview, year, genre, season_line, etc) are
  ALREADY escaped before being inserted, so don't escape them again here.

PROMO_LINK is the "Join our channel" link appended to every upload
notification — change it to your actual channel invite link.
"""

PROMO_LINK = "https://t.me/your_channel"

TEMPLATES = {

    # ── movie / series / collection upload notifications ─────────────────
    # Each notification is sent as a photo (TMDB portrait poster) with this
    # text as the caption underneath it.
    "MOVIE": (
        '🎬 "{title}"\n\n'
        "📅 {year}  •  🎭 {genre}\n\n"
        '📝 "{overview}"\n\n'
        "━━━━━━━━━━━━━━\n"
        "🔔 [Join our channel]({promo_link})"
    ),

    "SERIES": (
        '📺 "{title}"\n\n'
        "📅 {year}  •  🎭 {genre}\n\n"
        '📝 "{overview}"\n\n'
        "━━━━━━━━━━━━━━\n"
        "🔔 [Join our channel]({promo_link})"
    ),

    "EPISODE_UPDATE": (
        '📺 "{title}" — New Episode\\!\n\n'
        "🆕 {episode_line}\n"
        "📅 {year}  •  🎭 {genre}\n\n"
        '📝 "{overview}"\n\n'
        "━━━━━━━━━━━━━━\n"
        "🔔 [Join our channel]({promo_link})"
    ),

    "COLLECTION": (
        '🗂 "{title}" Collection\n\n'
        "🎬 {movie_count} movies inside\n\n"
        "━━━━━━━━━━━━━━\n"
        "🔔 [Join our channel]({promo_link})"
    ),

    # ── notify-bot commands (sent by the SAME bot that posts uploads) ────
    "BOT_START": (
        "👋 *Welcome\\!*\n\n"
        "This channel is fed automatically — new *movies*, *series*, and "
        "*collections* get posted here the moment they're uploaded\\.\n\n"
        "Use /uploads to see the last 10 uploads\\.\n\n"
        "━━━━━━━━━━━━━━\n"
        "🔔 [Join our channel]({promo_link})"
    ),

    "BOT_UPLOADS_HEADER": "📋 *Last {count} uploads*\n",

    "BOT_UPLOADS_ITEM": "{icon} *{title}*  ·  {category_label}  ·  _{when}_",

    "BOT_UPLOADS_EMPTY": "📭 No uploads logged yet\\.",

}
