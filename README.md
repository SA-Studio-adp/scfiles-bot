# SCFiles Backend Manager Bot — v4 (modular)

Same bot, same features — split from one 3,100-line `bot.py` into small,
single-purpose modules. If a bug or bad edit breaks one module, it fails
loudly and stays contained instead of dragging down the whole process.

## Run it

```bash
pip install -r requirements.txt
cp .env.example .env      # fill in TELEGRAM_TOKEN and BACKEND_URL at minimum
python run.py
```

## Layout

```
run.py                     ← start here (repo root)
scfiles_bot/
├── main.py                ← builds the Application, registers every handler, runs it
├── config.py               ← env vars, logging, the shared `state` object
├── utils.py                 ← esc/bold/code/italic HTML helpers
├── auth.py                   ← is_admin(), @admin_only, /addadmin /removeadmin /listadmins
├── api_client.py              ← aiohttp session, api_get/post/put/delete, 30s TTL cache
├── tmdb.py                     ← TMDB lookups + poster/format helpers
├── backup.py                    ← ZIP building, do_backup()
├── keyboards.py                  ← shared inline keyboards
├── errors.py                      ← global PTB error handler
├── scheduler.py                    ← periodic backup + self-ping jobs
├── notify.py                        ← channel-notification engine (routing, sending, upload history)
├── notify_bot.py                     ← second Application (NOTIFY_BOT_TOKEN): /start, /uploads
├── messages.py                        ← EDIT THIS to restyle every notification/command message
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
    └── admin_panel.py          ← token-protected /admin single-page app
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
`/addcollection`, or a saved `/editseries` (new episode), the bot asks:
"Send a notification?" → pick a category → enter a title → confirm. The
notify bot then posts ONE `sendMessage` (HTML parse mode): title, year,
genre, and the TMDB overview (quoted), with the TMDB poster embedded as a
large link-preview image above the text via the classic invisible-anchor
trick (`<a href="poster_url">&#8205;</a>` + `link_preview_options`) — no
`sendPhoto` call, so there's no 1024-char caption limit either. A
**"🔔 Join our Channel" button** is attached below.

Wording lives in `scfiles_bot/messages.py` as one HTML template per content
type, plus `PROMO_LINK` / `PROMO_BUTTON_TEXT` for the button.

`/removechannel <predvd|hd|all> <chat_id>` and `/listchannels` manage the
registered list.
