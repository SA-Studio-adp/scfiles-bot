# SCFiles Backend Manager Bot — v4 (modular)
FROM python:3.12-slim

# Prevent .pyc files & enable unbuffered logs (so `docker logs` shows output live)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# ffmpeg is required by the subtitle-extraction bot (embedded track pulls).
# tesseract-ocr is only needed if SUBS_OCR_ENABLED=1 (hardcoded/burned-in
# subtitle OCR fallback) — installed unconditionally here for simplicity
# since it's a small package; the bot just skips OCR if unused.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

# Install deps first so this layer is cached unless requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY run.py .
COPY scfiles_bot ./scfiles_bot

# All persistent state (admins, backup target, notify channels, upload
# history) now lives in MongoDB — see MONGODB_URI in .env. Nothing here
# needs a volume mount anymore; bot.log is written locally but is not
# critical to persist (also visible via `docker logs`).

EXPOSE 8080

# Basic container healthcheck against the bot's own /health endpoint
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/health', timeout=5).status==200 else 1)"

CMD ["python", "run.py"]
