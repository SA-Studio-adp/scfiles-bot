# SCFiles Backend Manager Bot — v4 (modular)

Same bot, same features — split from one 3,100-line `bot.py` into small,
single-purpose modules. If a bug or bad edit breaks one module, it fails
loudly and stays contained instead of dragging down the whole process.

## Run it

```bash
pip install -r requirements.txt
cp .env.example .env      # fill in TELEGRAM_TOKEN, BACKEND_URL, MONGODB_URI at minimum
python run.py
```

## Storage: MongoDB, not local files

Every piece of the bot's own state — admins, the backup target, registered
notify channels, and upload history — lives in MongoDB now (`db.py`),
**not** local `.json` files. There's nothing to set up beyond pointing
`MONGODB_URI` at a database (a free MongoDB Atlas cluster works fine) —
collections are created automatically on first write. If `MONGODB_URI` is
missing or unreachable, the bot fails to start with a clear log message
rather than silently breaking the first time someone runs `/addadmin`.

**The 2-day Telegram backup still works exactly as before** — `/backup`
and the scheduled job still zip up `movies.json` / `series.json` /
`collections.json` from your backend, and now also include `admins.json`,
`channels.json`, and `uploads_log.json` pulled fresh from MongoDB, so the
Telegram backup remains a complete, restorable snapshot even though
nothing is kept on local disk.

## Layout

```
run.py                     ← start here (repo root)
scfiles_bot/
├── main.py                ← builds the Application, registers every handler, runs it
├── config.py               ← env vars, logging, the shared `state` object
├── db.py                    ← ALL MongoDB access (admins, backup target, channels, upload history)
├── utils.py                 ← esc/bold/code/italic HTML helpers
├── auth.py                   ← is_admin(), @admin_only, /addadmin /removeadmin /listadmins
├── api_client.py              ← aiohttp session, api_get/post/put/delete, 30s TTL cache
├── tmdb.py                     ← TMDB lookups + poster/format helpers
├── backup.py                    ← ZIP building, do_backup()
├── keyboards.py                  ← shared inline keyboards
├── errors.py                      ← global PTB error handler
├── scheduler.py                    ← periodic backup + self-ping jobs
├── notify.py                        ← channel-notification engine (routing, sending, upload history)
├── notify_tokens.py                  ← short-lived tokens for pending notify prompts
├── notify_bot.py                     ← second Application (NOTIFY_BOT_TOKEN): /start, /uploads, /broadcast
├── broadcast.py                       ← /broadcast conversation (notify bot, admin-only)
├── messages.py                        ← EDIT THIS to restyle every notification/command message
├── formats.md                          ← HTML formatting reference for messages.py (bold, italic, etc)
│
├── handlers/
│   ├── states.py             ← every ConversationHandler state constant
│   ├── basic.py                ← /start /help /status /stats /movies /series
│   │                              /collections /logs /backup /backupzip /setbackup
│   ├── channels.py               ← v4: /addchannel /removechannel /listchannels
│   ├── movie_add.py                ← /addmovie conversation
│   ├── movie_edit.py                ← /editmovie conversation
│   ├── series_common.py              ← shared helpers for series add/edit
│   ├── series_add.py                   ← /addseries conversation
│   ├── series_edit.py                   ← /editseries conversation
│   ├── collection_add.py                 ← /addcollection conversation
│   ├── collection_edit.py                 ← /editcollection conversation
│   ├── delete.py                            ← /delmovie /delseries /delcollection
│   ├── tmdb_search.py                         ← /tmdb conversation
│   ├── notify_flow.py                          ← shared "notify? → category → title → confirm"
│   └── menu.py                                  ← inline "main menu" callbacks
│
└── web/
    ├── dashboard.py           ← "/" status page, /health, /backup/all, /logs
    ├── admin_panel.py          ← token-protected /admin single-page app
    └── schedule_picker.py       ← "/schedule" Web App date/time picker page
```

## Adding a new command

1. Put the handler function in the relevant `handlers/*.py` (or a new file
   if it's a new area — add it to `handlers/`).
2. If it's a conversation, add its state constants to `handlers/states.py`.
3. Register it in `main.py` (import the module, add the `CommandHandler` /
   `ConversationHandler`).

Nothing else needs touching — `config.state` is shared automatically.

After any change — especially to imports — run the smoke tests
(`tests/README.md`): `python tests/test_handlers_smoke.py &&
python tests/test_confirm_flows_smoke.py`. They mock the network and just
verify every handler actually runs without raising, which catches the
"forgot an import when splitting/editing a file" class of bug immediately
instead of waiting for it to crash in production.

## v5: channel notifications

Two bots are involved:
- **Admin bot** (`TELEGRAM_TOKEN`) — you talk to this one, in DM; it runs
  all the `/add*` `/edit*` `/del*` commands. It never needs to be added to
  any channel or group.
- **Notify bot** (`NOTIFY_BOT_TOKEN`) — a separate bot that only posts
  uploads to your channels and answers `/start` and `/uploads` there. Add
  **this one** as admin to every channel/group you want notifications in —
  the admin bot doesn't need to be there too.

**Register a channel:** in a DM with the ADMIN bot, either:
- `/addchannel -1001234567890` if you already know the chat ID, or
- forward any message from the target channel/group into the DM — no
  command needed, Telegram hands over the chat ID from the forward itself

Either way, it's verified via the **notify bot's** own access to that chat
(so the notify bot must already be admin there) before letting you pick a
category: **📀 PreDVD / 🎬 HD / 🌐 All**. Channels registered under "All"
get every upload; "PreDVD"/"HD" channels only get uploads sent to that
category.

**Sending a notification:** after any successful `/addmovie`, `/addseries`,
`/addcollection`, or a saved `/editseries` (new episode/season), the bot
asks: "Send a notification?" → pick a category → enter a title → confirm.
The notify bot then posts ONE `sendMessage` (HTML parse mode): title,
year, genre, and the TMDB overview (quoted), with the TMDB poster embedded
as a large link-preview image above the text via the classic
invisible-anchor trick (`<a href="poster_url">&#8205;</a>` +
`link_preview_options`) — no `sendPhoto` call, so there's no 1024-char
caption limit either. A fixed **footer** (channel handle, request-bot
handles, website link — edit in `messages.py`'s `FOOTER` template) is
appended, and a **"Watch · &lt;name&gt;" button** linking to your website
is attached below, built from the item's slug ID:
- movie → `WEBSITE_LINK/movie?id=<slug>`
- series/episode update → `WEBSITE_LINK/pages/series?id=<slug>`
- collection → `WEBSITE_LINK/collections?id=<slug>`

**New season / new episode wording:** when `/editseries` saves, the bot
tracks exactly which (season, episode) pairs were touched in that session.
If any of them belong to a season that didn't exist before, the
notification says "New Season Added"; otherwise "New Episode(s) Added",
followed by a per-season line like `S1 . EP 13-16 has been uploaded`
(contiguous episode numbers collapse into a range automatically).

Wording lives in `scfiles_bot/messages.py` as one HTML template per content
type, plus `WEBSITE_LINK` / `CHANNEL_HANDLE` / `REQUESTS_HANDLES` /
`WATCH_BUTTON_PREFIX` at the top of the file. Full HTML formatting
reference (bold, italic, underline, quote blocks, spoilers, etc) is in
`scfiles_bot/formats.md`.

`/removechannel <predvd|hd|all> <chat_id>` and `/listchannels` manage the
registered list.

## Notify prompt: notify / schedule / skip

Every successful upload asks three fully independent questions — none of
them leave the bot "waiting", so you're always free to start the next
upload immediately:

1. **🔔 Yes, notify** — opens a short mini-conversation (category → title
   → confirm) and sends right away.
2. **🗓 Yes, schedule** — opens `BOT_WEB_URL/schedule` as a Telegram Web
   App. Only appears when `BOT_WEB_URL` is a real public **HTTPS** URL —
   Web App buttons flatly refuse `http://`/`localhost`, so without one
   configured the button is simply omitted.
3. **🔕 No, skip** — dismisses the prompt.

Each prompt gets its own short-lived token (`notify_tokens.py`), so
uploading several things in a row and leaving their prompts unresolved
never causes one to clobber another — and importantly, **the calling
upload conversation ends the instant the prompt is shown**, regardless of
which button (if any) gets tapped. This is deliberate: earlier versions
kept the upload conversation alive waiting for the prompt to resolve,
which could leave `/addmovie` unresponsive for an admin until they
resolved (or `/cancel`led) a stale prompt — that's no longer possible.

### How scheduling actually works

The picker page (`web/schedule_picker.py`) collects date/time (IST),
category, and title all on one page, then **POSTs directly to our own
server** (`/schedule/submit`) — deliberately *not* through Telegram's
`Web App sendData()` → `web_app_data` message delivery, which turned out
to be unreliable for Web App buttons opened from an inline keyboard.
Talking directly to our own endpoint is fully within our control and independently testable.

On submit, the server:
1. Writes the scheduled notification to MongoDB (the `scheduled`
   collection).
2. Immediately sends **"🗓 `<title>` has been scheduled for `<time>`"**
   into your chat via the admin bot, and records that message's
   chat/message ID.
3. A background job (`scheduler.job_send_scheduled_notifications`, every
   1 minute) sends anything whose time has arrived, then **edits that
   same confirmation message in place** to
   **"✅ `<title>` sent to N group(s)/channel(s) successfully at `<time>`"**
   (or a clear failure message if nothing was sent) — so you get a single
   message that updates itself once, rather than a second notification.
   A missed minute or two of bot downtime just means a slightly late
   send, not a lost one, since it's picked up on the next check.

## /broadcast

Run on the **notify bot** (not the admin bot), admin-only. Sends a
text-or-photo message to **every** registered channel/group (any
category, deduplicated) **and every user** who has ever DMed the notify
bot (tracked automatically on `/start` and `/uploads`).

```
/broadcast
→ "Send me the message — text, or a photo with a caption"
→ shows a preview + recipient count
→ [✅ Send] [❌ Cancel]
```

Sends are HTML-formatted, one at a time with a small delay to stay under
Telegram's rate limits, and a failure on one recipient (blocked bot,
kicked from a group, etc) never stops the rest — the final summary reports
sent vs failed counts.
