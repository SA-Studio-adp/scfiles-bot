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

## v5: channel notifications

Two bots are involved:
- **Admin bot** (`TELEGRAM_TOKEN`) — you talk to this one; it runs all the
  `/add*` `/edit*` `/del*` commands.
- **Notify bot** (`NOTIFY_BOT_TOKEN`) — a separate bot that only posts
  uploads to your channels and answers `/start` and `/uploads` there. Add
  it to your channels/groups as admin too.

**Register a channel:** run `/addchannel` with the ADMIN bot, inside the
target channel/group (or in a DM with the chat ID as an argument). It
verifies the chat is actually a channel/group — not a person's DM — before
letting you pick a category: **📀 PreDVD / 🎬 HD / 🌐 All**. Channels
registered under "All" get every upload; "PreDVD"/"HD" channels only get
uploads sent to that category.

**Sending a notification:** after any successful `/addmovie`, `/addseries`,
`/addcollection`, or a saved `/editseries` (new episode), the bot asks:
"Send a notification?" → pick a category → enter a title → confirm. The
notify bot then posts the TMDB poster with title (quoted), year, genre, the
TMDB overview (quoted), and a "Join our channel" link — all editable in
`scfiles_bot/messages.py` (including `PROMO_LINK`).

`/removechannel <predvd|hd|all> <chat_id>` and `/listchannels` manage the
registered list.
