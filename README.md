# 🎛 SCFiles Backend Manager Bot v2 (Pyrogram Edition)

A Telegram bot to fully manage your SCFiles backend — movies, series, collections — with TMDB metadata, automatic backups, server monitoring, and a built-in health web service.

---

## ✅ What Changed from v1

| Feature | Details |
|---|---|
| 🌐 Server Status | Ping your backend, see response time & health |
| 📊 Stats | Count of movies, series, collections |
| 🎬 Movies | List, Add (with TMDB metadata), Edit, Delete |
| 📺 Series | List, Add (JSON episode input), Delete |
| 🗂 Collections | List, Add, Delete |
| 🔍 TMDB Search | Search movies/TV shows, show poster + metadata |
| 💾 Auto Backup | Every 2 days, sends JSON files to your Telegram chat |
| 🧾 Backup All Download | Web service button to download all backend data in one ZIP |
| 🩺 Health Web Service | Shows bot + backend health details in browser |
| 🎨 Improved Web UI | Dashboard-style health page with auto refresh and DB counters |
| 📡 Auto Pinging | Periodically pings backend and bot health URL to reduce sleeping |
| 🤖 Auto Command Sync | Registers Telegram bot commands automatically on startup |
| ⚙️ Backup Channel Setup | Configure backup target chat at runtime with `/setbackup` |
| 📋 Runtime Logs | `/logs` command + `/logs` web endpoint for recent log tail |
| 🔐 Admin-only | Restrict write operations to specific user IDs |

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
| `TELEGRAM_TOKEN` | Bot token from [@BotFather](https://t.me/BotFather) |
| `BACKEND_URL` | Your SCFiles backend URL (e.g. `https://your-app.vercel.app`) |
| `TMDB_API_KEY` | API key from [themoviedb.org](https://www.themoviedb.org/settings/api) |
| `ADMIN_IDS` | Comma-separated Telegram user IDs for admin access |
| `BACKUP_CHAT_ID` | Chat/channel ID to receive automatic backups |
| `WEB_HOST` | Host for the built-in health web service (default: `0.0.0.0`) |
| `WEB_PORT` | Port for the web service (default: `8080`) |
| `BOT_WEB_URL` | Public URL of this bot deployment for self-ping (example: `https://your-bot.onrender.com`) |
| `AUTO_PING_INTERVAL_MIN` | Auto ping interval in minutes (default: `5`) |
| `LOG_FILE` | Bot log file path used by `/logs` and `/logs` web endpoint (default: `bot.log`) |

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

/tmdb          — Search TMDB for movie or TV show metadata
/backup        — Trigger a manual backup now
/backupall     — Download all backend data as one ZIP
/setbackup     — Set backup channel/chat ID
/logs          — Download recent bot logs
/cancel        — Cancel current operation
```

`/backup` now reports the exact destination chat ID and shows a setup hint if backup target is not configured.

---

## 🌐 Web Service

The bot also starts a small web service:

- `/` → HTML dashboard with bot/backend health + movie/series/collection counters
- `/health` → JSON health payload
- `/backup/all` → Download one ZIP with movies, series, and collections JSON
- `/logs` → View latest bot log tail in browser

Use `BOT_WEB_URL` so the auto ping job can hit your bot deployment and help keep it awake.

---

## 🌐 Web Dashboard (`:8080`)

| Route | Description |
|---|---|
| `/` | Full HTML dashboard — KPIs, status panels, data tables |
| `/health` | JSON health endpoint |
| `/backup/all` | Download backup ZIP directly from browser |

You can change the destination at runtime:

```bash
/setbackup <chat_id>
```

This value is persisted in `.backup_config.json` (configurable via `BACKUP_CONFIG_FILE`).

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

---

## 🐳 Run with Docker (optional)

```dockerfile
FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    WEB_HOST=0.0.0.0 \
    WEB_PORT=8080
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY bot.py /app/bot.py
EXPOSE 8080
CMD ["python", "bot.py"]
```

```bash
docker build -t scfiles-bot .
docker run -d --env-file .env scfiles-bot
```

The container exposes port `8080` for the health web service (`/`, `/health`, `/backup/all`).
