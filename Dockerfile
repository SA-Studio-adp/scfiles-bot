FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    WEB_HOST=0.0.0.0 \
    WEB_PORT=8080 \
    LOG_FILE=/app/logs/bot.log \
    BACKUP_CONFIG_FILE=/app/data/.backup_config.json

WORKDIR /app
RUN mkdir -p /app/logs /app/data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .

EXPOSE 8080
CMD ["python", "bot.py"]
