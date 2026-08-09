"""
messages.py — SCFiles v4 channel-notification templates
─────────────────────────────────────────────────────────────────────────────
Edit the strings below to change what gets posted to your channels/groups
whenever a movie, series, or collection is uploaded. No other file needs to
change — bot.py imports TEMPLATES from here.

Syntax: Telegram Bot API MarkdownV2 — https://core.telegram.org/bots/api#markdownv2-style
  *bold*   _italic_   `code`   [text](url)   ||spoiler||
  Literal special chars ( _ * [ ] ( ) ~ ` > # + - = | { } . ! ) must be
  escaped with a backslash when they are NOT being used for formatting.

Placeholders in {curly_braces} are filled in automatically by notify.py.
Values that come from your database (title, overview, year, rating,
season_line, movie_count) are ALREADY MarkdownV2-escaped before being
inserted — don't escape them again in this file.
`qualities` and `share_url` are inserted raw (used inside a `code` span /
a link target, where escaping isn't wanted).

Available placeholders per template:
  MOVIE_HD / MOVIE_PREDVD : title, year, rating, qualities, overview, share_url
  SERIES                  : title, year, rating, season_line, overview, share_url
  COLLECTION              : title, movie_count, share_url
"""

TEMPLATES = {

    "MOVIE_HD": (
        "🎬 *{title}*\n"
        "📅 {year}  •  ⭐ {rating}/10\n"
        "📥 Quality: `{qualities}`\n\n"
        "{overview}\n\n"
        "🔗 [Get it here]({share_url})"
    ),

    "MOVIE_PREDVD": (
        "🎬 *{title}*  \\[PreDVD]\n"
        "📅 {year}  •  ⭐ {rating}/10\n"
        "📥 Quality: `{qualities}`\n\n"
        "⚠️ Early PreDVD print — quality may improve on a later re\\-upload\\.\n\n"
        "🔗 [Get it here]({share_url})"
    ),

    "SERIES": (
        "📺 *{title}*\n"
        "📅 {year}  •  ⭐ {rating}/10\n"
        "🆕 {season_line}\n\n"
        "{overview}\n\n"
        "🔗 [Get it here]({share_url})"
    ),

    "COLLECTION": (
        "🗂 *{title}* collection updated\n"
        "🎬 {movie_count} movies inside\n\n"
        "🔗 [Browse the collection]({share_url})"
    ),

}
