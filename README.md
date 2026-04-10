# 🎛 SCFiles Backend Manager Bot v2 (Pyrogram Edition)

> Full rewrite using **Pyrogram** — fixes the `/backup` issue and adds a premium web dashboard.

---

## ✅ What Changed from v1

| Area | v1 (python-telegram-bot) | v2 (Pyrogram) |
|---|---|---|
| `/backup` command | ❌ Broken (send_document issues) | ✅ Fixed — uses Pyrogram's native file sending |
| `/backupzip` | Worked sometimes | ✅ Reliable BytesIO ZIP send |
| Bot framework | python-telegram-bot 21.x | **Pyrogram 2.x** |
| Web dashboard | Basic HTML | ✅ Premium dark dashboard with tables |
| State machine | PTB ConversationHandler | ✅ Custom in-memory state machine |

---

## 🚀 Setup

### 1. Get Pyrogram credentials

Go to **https://my.telegram.org/apps** and create an app to get:
- `API_ID` (a number)
- `API_HASH` (a hex string)

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your values
```

| Variable | Description |
|---|---|
| `API_ID` | From my.telegram.org/apps |
| `API_HASH` | From my.telegram.org/apps |
| `TELEGRAM_TOKEN` | From [@BotFather](https://t.me/BotFather) |
| `BACKEND_URL` | Your SCFiles Vercel URL |
| `TMDB_API_KEY` | From [themoviedb.org](https://www.themoviedb.org/settings/api) |
| `ADMIN_IDS` | Your Telegram user ID (from [@userinfobot](https://t.me/userinfobot)) |
| `BACKUP_CHAT_ID` | Chat/channel ID for auto-backups |
| `BOT_WEB_URL` | Public URL of this server (optional) |

### 3. Install & Run

```bash
pip install -r requirements.txt
export $(cat .env | xargs)
python bot.py
```

### 4. Docker

```bash
docker build -t scfiles-bot .
docker run -d --env-file .env -p 8080:8080 scfiles-bot
```

---

## 📱 Commands

```
/start          — Main menu
/help           — Full command list
/status         — Server health check
/stats          — DB counts

/movies         — List recent movies
/series         — List recent series
/collections    — List all collections

/addmovie       — Add movie (guided flow + TMDB metadata)
/addseries      — Add series (TMDB + JSON episode input)
/addcollection  — Create collection

/editmovie      — Edit any movie field
/delmovie       — Delete a movie
/delseries      — Delete a series
/delcollection  — Delete a collection

/tmdb           — Search TMDB (movie or TV, shows poster)
/backup         — Send JSON backups to configured channel
/backupzip      — Download all data as a single ZIP
/setbackup      — Change backup channel ID

/cancel         — Cancel current operation
```

---

## 🌐 Web Dashboard (`:8080`)

| Route | Description |
|---|---|
| `/` | Full HTML dashboard — KPIs, status panels, data tables |
| `/health` | JSON health endpoint |
| `/backup/all` | Download backup ZIP directly from browser |

---

## 💾 Backup

- **Auto:** Every 2 days, sends 3 JSON files to `BACKUP_CHAT_ID`
- **Manual:** `/backup` — sends files to configured chat
- **ZIP:** `/backupzip` — sends a single ZIP to current chat

### Set backup channel at runtime:
```
/setbackup -100xxxxxxxxxx
```

---

## 🔧 Add Series — Episode JSON Format

```json
[
  {
    "season_number": 1,
    "episodes": [
      {
        "ep_number": 1,
        "links": {
          "360p": "https://cdn-scfiles.vercel.app/dl/...",
          "720p": "https://cdn-scfiles.vercel.app/dl/..."
        },
        "subtitle": ""
      }
    ]
  }
]
```
