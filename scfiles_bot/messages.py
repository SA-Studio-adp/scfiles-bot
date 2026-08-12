"""
messages.py — SCFiles channel-notification templates
─────────────────────────────────────────────────────────────────────────────
Edit the strings below to change what gets posted. No other file needs to
change — notify.py and notify_bot.py both import from here.

For the FULL list of HTML formatting tags available (bold, italic,
underline, strikethrough, quotes, code blocks, spoilers, etc) see
formats.md in this same folder — it's a copy-paste reference.

Each upload is ONE Telegram message (parse_mode=HTML). When there's a
poster, notify.py prepends an invisible link to it so Telegram shows it as
a large image preview above this text — you don't reference the image from
inside these templates. A FOOTER (below) is appended after every template,
and a "Watch · <name>" BUTTON linking to the website is attached under
every message — neither is part of the per-type templates, so you only
edit them once here.

Only & < > need escaping in HTML, and placeholders in {curly_braces} below
are already escaped before being inserted (title, overview, year, genre,
episode_line, event_label, movie_count) — don't escape them again here.
"""

# Used to build every "Watch · <name>" button below, AND mentioned in the
# footer's "For more visit" line:
#   movie      -> {WEBSITE_LINK}/movie?id=<slug>
#   series     -> {WEBSITE_LINK}/series?id=<slug>
#   collection -> {WEBSITE_LINK}/collections?id=<slug>
WEBSITE_LINK = "https://scfiles.vercel.app"

CHANNEL_HANDLE    = "@sc_files4"
REQUESTS_HANDLES  = "@sc_requests & @streamcenter_bot"
WATCH_BUTTON_PREFIX = "Watch . "   # button label = this + the movie/series/collection name

TEMPLATES = {

    "MOVIE": (
        '<blockquote>🎬 <b>{title}</b></blockquote>\n\n'
        "<blockquote>📅 {year}  •  🎭 {genre}</blockquote>\n\n"
        '<blockquote><i>{overview}</i></blockquote>'
    ),

    "SERIES": (
        '<blockquote>📺 <b>{title}</blockquote></b>\n\n'
        "<blockquote>📅 {year}  •  🎭 {genre}</blockquote>\n\n"
        '<blockquote><i>{overview}</i></blockquote>'
    ),

    # new episode(s) added to an existing series — {event_label} is filled
    # in automatically as "New Season Added" or "New Episode(s) Added"
    "EPISODE_UPDATE": (
        '📺 <b>"{title}"</b> — {event_label}!\n\n'
        "🆕 {episode_line}\n"
        "📅 {year}  •  🎭 {genre}\n\n"
        '"{overview}"'
    ),

    "COLLECTION": (
        '🗂 <b>"{title}" Collection</b>\n\n'
        "🎬 {movie_count} movies inside"
    ),

    # Appended after every one of the templates above.
    "FOOTER": (
        "<b>\n\nJoin our Channel :- {channel_handle}\n\n"
        "To Get Direct Files Use : {requests_handles}\n\n"
        "For more visit :- {website_link}</b>"
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
