# SCFiles Backend Manager Bot — v4 (modular)
FROM python:3.12-slim

# Prevent .pyc files & enable unbuffered logs (so `docker logs` shows output live)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps first so this layer is cached unless requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY run.py .
COPY scfiles_bot ./scfiles_bot

# Persisted config files (.backup_config.json, .admins_config.json,
# .channels_config.json, bot.log) are written to /app at runtime.
# Mount a volume here to keep them across container restarts:
#   docker run -v scfiles_data:/app ...
VOLUME ["/app"]

EXPOSE 8080

# Basic container healthcheck against the bot's own /health endpoint
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/health', timeout=5).status==200 else 1)"

CMD ["python", "run.py"]
