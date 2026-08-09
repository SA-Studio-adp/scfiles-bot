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
├── notify.py                        ← v4 channel-notification engine
├── messages.py                       ← v4 EDIT THIS to restyle channel captions
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

## v4: channel notifications

See the docstring at the top of `scfiles_bot/notify.py`. Short version:
set `NOTIFY_BOT_TOKEN`, edit `scfiles_bot/messages.py` for wording, and run
`/addchannel` inside each channel/group to pick **PreDVD / HD / All**.
