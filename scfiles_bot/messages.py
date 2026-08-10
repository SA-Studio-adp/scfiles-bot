"""
messages.py — SCFiles channel-notification templates
─────────────────────────────────────────────────────────────────────────────
Edit the strings below to change what gets posted. No other file needs to
change — notify.py and notify_bot.py both import from here.

Why TITLE and BODY are separate keys
─────────────────────────────────────
Telegram always renders a photo's caption BELOW the photo — there's no way
to put text above an image in a single message. To get the layout
"Title → Poster → details", the bot sends TWO messages:
  1. a plain text message using the *_TITLE template
  2. the poster photo, with the *_BODY template as its caption, plus a
     "Join our channel" BUTTON (not a text link — buttons are the only
     Telegram element guaranteed to render as tappable in every client;
     markdown links inside photo captions are unreliable across clients)

Syntax: Telegram Bot API MarkdownV2 — https://core.telegram.org/bots/api#markdownv2-style
  *bold*   _italic_   `code`
  Literal special chars ( _ * [ ] ( ) ~ ` > # + - = | { } . ! ) must be
  escaped with a backslash when NOT used for formatting. Placeholders in
  {curly_braces} below are filled in automatically — values that come from
  your database/TMDB (title, overview, year, genre, season_line, etc) are
  ALREADY escaped before being inserted, so don't escape them again here.

PROMO_LINK / PROMO_BUTTON_TEXT control the "Join our channel" button
appended under every upload notification.
"""

PROMO_LINK = "https://t.me/sc_files4"
PROMO_BUTTON_TEXT = "🔔 Join our Channel"

TEMPLATES = {

    # ── movie ──────────────────────────────────────────────────────────
    "MOVIE_TITLE": '🎬 "{title}"',
    "MOVIE_BODY": (
        "📅 {year}  •  🎭 {genre}\n\n"
        '"{overview}"'
    ),

    # ── series ─────────────────────────────────────────────────────────
    "SERIES_TITLE": '📺 "{title}"',
    "SERIES_BODY": (
        "📅 {year}  •  🎭 {genre}\n\n"
        '"{overview}"'
    ),

    # ── new episode(s) added to an existing series ────────────────────
    "EPISODE_UPDATE_TITLE": '📺 "{title}" — New Episode\\!',
    "EPISODE_UPDATE_BODY": (
        "🆕 {episode_line}\n"
        "📅 {year}  •  🎭 {genre}\n\n"
        '"{overview}"'
    ),

    # ── collection ─────────────────────────────────────────────────────
    "COLLECTION_TITLE": '🗂 "{title}" Collection',
    "COLLECTION_BODY": "🎬 {movie_count} movies inside",

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
