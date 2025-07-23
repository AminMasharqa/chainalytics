#!/bin/bash

log() {
    echo -e "\n[LOG] $1\n"
}

check_docker_status() {
    if [ $? -eq 0 ]; then
        log "✅ $1 started successfully"
    else
        log "❌ Failed to start $1"
        exit 1
    fi
}

# 1. Streaming services
log "📦 Starting Streaming Services..."
cd streaming || exit 1
docker compose up -d
check_docker_status "Streaming Services"
cd ..

# 2. Processing services
log "⚙️ Starting Processing Services..."
cd processing || exit 1
make build
sleep 10
make run-scaled
sleep 10
cd ..

# 3. Orchestration in a new terminal
log "🗂️ Launching Orchestration Services in a new terminal window..."

gnome-terminal -- bash -c "
cd $(pwd)/orchestration &&
docker compose up -d &&
echo '✅ Orchestration Services started.' || echo '❌ Failed';
exec bash"
