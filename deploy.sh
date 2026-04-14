#!/bin/bash
# SCFiles Bot — VPS Deploy Script
# Fixes the docker-compose 1.29.2 ContainerConfig error
set -e

echo "=== SCFiles Bot Deploy ==="

# Step 1: Remove old container if it exists (avoids ContainerConfig bug)
echo "[1/4] Removing old container..."
docker rm -f sc_bot 2>/dev/null || echo "  (no old container)"

# Step 2: Check if docker compose plugin is available (v2), else use old style
echo "[2/4] Detecting docker compose version..."
if docker compose version &>/dev/null; then
    COMPOSE="docker compose"
    echo "  Using: docker compose (v2 plugin) ✅"
else
    COMPOSE="docker-compose"
    echo "  Using: docker-compose (v1 legacy)"
fi

# Step 3: Build fresh
echo "[3/4] Building image..."
$COMPOSE build --no-cache

# Step 4: Start
echo "[4/4] Starting container..."
$COMPOSE up -d

echo ""
echo "=== Done! ==="
echo "Check logs:   docker logs sc_bot -f"
echo "Bot logs:     docker exec sc_bot cat /app/logs/bot.log"
echo "Dashboard:    http://$(hostname -I | awk '{print $1}'):8080"
