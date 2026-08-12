# Formatting reference for `messages.py`

Every notification is sent with `parse_mode: HTML`, so `messages.py`
templates use plain HTML tags — not Markdown. This is the full set
Telegram's Bot API supports. Copy-paste whatever you need directly into
the template strings in `messages.py`.

Reference: https://core.telegram.org/bots/api#html-style

## Text styles

| Style | Tag | Example | Result |
|---|---|---|---|
| Bold | `<b>...</b>` | `<b>Hello</b>` | **Hello** |
| Italic | `<i>...</i>` | `<i>Hello</i>` | *Hello* |
| Underline | `<u>...</u>` | `<u>Hello</u>` | <u>Hello</u> |
| Strikethrough | `<s>...</s>` | `<s>Hello</s>` | ~~Hello~~ |
| Spoiler (tap to reveal) | `<tg-spoiler>...</tg-spoiler>` | `<tg-spoiler>Hello</tg-spoiler>` | ▓▓▓▓▓ → Hello |
| Inline code | `<code>...</code>` | `<code>chat_id</code>` | `chat_id` |
| Code block | `<pre>...</pre>` | `<pre>some\ncode</pre>` | monospace block |
| Code block, language-tagged | `<pre><code class="language-python">...</code></pre>` | — | syntax-highlighted block |
| Quote block | `<blockquote>...</blockquote>` | `<blockquote>Hello</blockquote>` | indented quote bar |
| Expandable quote (collapsed by default) | `<blockquote expandable>...</blockquote>` | — | tap to expand |
| Link | `<a href="URL">text</a>` | `<a href="https://t.me/x">Join</a>` | tappable link |
| Mention by username | just write it | `@sc_files4` | tappable automatically |

You can nest tags: `<b>Bold <i>and italic</i></b>` works fine.

## Escaping — the only 3 characters that matter

In HTML mode, only these need escaping if they appear as **literal text**
(not as part of a tag):

| Character | Write as |
|---|---|
| `&` | `&amp;` |
| `<` | `&lt;` |
| `>` | `&gt;` |

Quotes (`"`), periods, dashes, parentheses, etc. do **not** need escaping —
that was only true for the old MarkdownV2 mode this bot no longer uses.

**You don't need to escape the `{placeholders}`** in messages.py (title,
overview, year, genre, episode_line, event_label, movie_count) — the bot
already escapes those values before inserting them, so typing a raw `&` or
`<` inside a template's own wording is the only thing you need to watch.

> **`CHANNEL_HANDLE` / `REQUESTS_HANDLES` / `WEBSITE_LINK`** (top of
> messages.py) are also auto-escaped before being inserted into the
> footer — but if you reuse them anywhere else, or add new constants like
> them, remember: a stray `&` in ANY text sent with `parse_mode=HTML`
> makes Telegram reject the **entire message** with a 400 "can't parse
> entities" error — silently, from the notify bot's side, unless
> something is watching for it. `tests/test_html_safety.py` checks every
> template for exactly this before you deploy.

## What you CANNOT do

- Put text *above* an image in the same message — that's why the poster
  is embedded as a link-preview (see notify.py's `_embed_image_html`)
  instead of a classic photo attachment; whatever you write in a template
  always renders below the poster.
- Use two different link colors/styles — Telegram renders all `<a href>`
  links the same way.
- Nest a `<pre>` block inside `<b>`/`<i>` — code blocks can't be combined
  with bold/italic in the same span.

## Where each template's placeholders come from

| Template | Placeholders |
|---|---|
| `MOVIE` | `{title}` `{year}` `{genre}` `{overview}` |
| `SERIES` | `{title}` `{year}` `{genre}` `{overview}` |
| `EPISODE_UPDATE` | `{title}` `{event_label}` `{episode_line}` `{year}` `{genre}` `{overview}` |
| `COLLECTION` | `{title}` `{movie_count}` |
| `FOOTER` (appended after every one above) | `{channel_handle}` `{requests_handles}` `{website_link}` |
| `BOT_START` | none |
| `BOT_UPLOADS_HEADER` | `{count}` |
| `BOT_UPLOADS_ITEM` | `{icon}` `{title}` `{category_label}` `{when}` |
| `BOT_UPLOADS_EMPTY` | none |

`event_label` is filled in automatically as `"New Season Added"` or
`"New Episode(s) Added"` depending on whether the /editseries session
touched a season that didn't exist before. `episode_line` looks like
`"S1 . EP 13-16 has been uploaded"` — grouped per season, with contiguous
episode numbers collapsed into a range automatically.

## Example: making the title a spoiler until tapped

```python
"MOVIE": (
    '🎬 <tg-spoiler><b>"{title}"</b></tg-spoiler>\n\n'
    "📅 {year}  •  🎭 {genre}\n\n"
    '"{overview}"'
),
```

## Example: quoting the story in a proper blockquote instead of plain quotes

```python
"MOVIE": (
    '🎬 <b>"{title}"</b>\n\n'
    "📅 {year}  •  🎭 {genre}\n\n"
    "<blockquote>{overview}</blockquote>"
),
```
