# 🎛 SCFiles Backend Manager Bot

A Telegram bot to fully manage your SCFiles backend — movies, series, collections — with TMDB metadata, automatic backups, and server monitoring.

---

## ✅ Features

| Feature | Details |
|---|---|
| 🌐 Server Status | Ping your backend, see response time & health |
| 📊 Stats | Count of movies, series, collections |
| 🎬 Movies | List, Add (with TMDB metadata), Edit, Delete |
| 📺 Series | List, Add (JSON episode input), Delete |
| 🗂 Collections | List, Add, Delete |
| 🔍 TMDB Search | Search movies/TV shows, show poster + metadata |
| 💾 Auto Backup | Every 2 days, sends JSON files to your Telegram chat |
| 🔐 Admin-only | Restrict write operations to specific user IDs |

---

## 🚀 Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your values
```

**Required variables:**

| Variable | Description |
|---|---|
| `TELEGRAM_TOKEN` | Bot token from [@BotFather](https://t.me/BotFather) |
| `BACKEND_URL` | Your SCFiles backend URL (e.g. `https://your-app.vercel.app`) |
| `TMDB_API_KEY` | API key from [themoviedb.org](https://www.themoviedb.org/settings/api) |
| `ADMIN_IDS` | Comma-separated Telegram user IDs for admin access |
| `BACKUP_CHAT_ID` | Chat/channel ID to receive automatic backups |

### 3. Run the bot

```bash
# Export variables from .env
export $(cat .env | xargs)

# Start the bot
python bot.py
```

---

## 📱 Commands

```
/start         — Main menu with buttons
/help          — Full command list
/status        — Check backend server health
/stats         — Movie/series/collection counts

/movies        — List recent movies
/series        — List recent series
/collections   — List all collections

/addmovie      — Add a movie (guided, with TMDB metadata)
/addseries     — Add a series (with JSON episode input)
/addcollection — Create a collection

/editmovie     — Edit any field of a movie

/delmovie      — Delete a movie by ID
/delseries     — Delete a series by ID
/delcollection — Delete a collection by ID

/tmdb          — Search TMDB for movie or TV show metadata
/backup        — Trigger a manual backup now
/cancel        — Cancel current operation
```

---

## 💾 Backup Format

Every 2 days (or on `/backup`), the bot sends three files to `BACKUP_CHAT_ID`:

- `YYYY-MM-DD_HH-MM_movies.json`
- `YYYY-MM-DD_HH-MM_series.json`
- `YYYY-MM-DD_HH-MM_collections.json`

---

## 🔐 Getting Your Telegram User ID

Send `/start` to [@userinfobot](https://t.me/userinfobot) on Telegram — it will reply with your numeric user ID.

---

## 🔧 Add Series — Episode JSON Format

When adding a series, paste this JSON structure:

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
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY bot.py .
CMD ["python", "bot.py"]
```

```bash
docker build -t scfiles-bot .
docker run -d --env-file .env scfiles-bot
```
