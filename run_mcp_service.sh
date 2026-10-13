#!/bin/bash
# ==============================================================================
# run_mcp_service.sh — Supervisor for SSH MCP Server + Persistent Tunnel
# ==============================================================================
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$DIR/logs"
mkdir -p "$LOG_DIR"

SERVER_LOG="$LOG_DIR/server.log"
TUNNEL_LOG="$LOG_DIR/tunnel.log"
URL_FILE="$DIR/live-url.txt"
INFO_FILE="$DIR/connection_details.txt"
JSON_FILE="$DIR/connection_info.json"

source "$DIR/.env"

log() {
    echo "[$(date -u +%FT%TZ)] $*"
}

cleanup() {
    log "Stopping SSH MCP server and tunnel..."
    [[ -n "${SERVER_PID:-}" ]] && kill -TERM "$SERVER_PID" 2>/dev/null || true
    [[ -n "${TUNNEL_PID:-}" ]] && kill -TERM "$TUNNEL_PID" 2>/dev/null || true
    exit 0
}

trap cleanup SIGTERM SIGINT EXIT

log "Starting SSH MCP Server on port $MCP_PORT..."
python3 "$DIR/server.py" >> "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

# Wait for local server to be responsive
log "Waiting for local server to become healthy..."
MAX_WAIT=20
WAITED=0
until curl -s "http://127.0.0.1:$MCP_PORT/health" | grep -q "healthy"; do
    sleep 0.5
    WAITED=$((WAITED + 1))
    if [ "$WAITED" -ge "$MAX_WAIT" ]; then
        log "ERROR: SSH MCP server failed to start within 10 seconds. Check $SERVER_LOG"
        exit 1
    fi
done
log "SSH MCP server is healthy at http://127.0.0.1:$MCP_PORT"

# Check if ngrok is available and configured
USE_NGROK=false
if command -v ngrok >/dev/null 2>&1; then
    USE_NGROK=true
fi

while true; do
    if [ "$USE_NGROK" = true ]; then
        log "Starting ngrok tunnel for http://127.0.0.1:$MCP_PORT..."
        ngrok http "$MCP_PORT" --log=stdout > "$TUNNEL_LOG" 2>&1 &
        TUNNEL_PID=$!
        
        # Wait for ngrok public url
        PUBLIC_URL=""
        for i in {1..30}; do
            sleep 0.5
            PUBLIC_URL=$(curl -s http://127.0.0.1:4040/api/tunnels 2>/dev/null | jq -r '.tunnels[0].public_url // empty' 2>/dev/null || true)
            if [ -n "$PUBLIC_URL" ] && [ "$PUBLIC_URL" != "null" ]; then
                break
            fi
        done

        if [ -n "$PUBLIC_URL" ] && [ "$PUBLIC_URL" != "null" ]; then
            SSE_URL="$PUBLIC_URL/sse"
            echo "$PUBLIC_URL" > "$URL_FILE"

            # Write JSON details
            cat > "$JSON_FILE" <<EOF
{
  "connection_name": "SSH Terminal (Workstation)",
  "host_url": "$SSE_URL",
  "access_mode": "Read and write",
  "header_name": "X-API-Key",
  "header_value": "$MCP_API_KEY",
  "base_scope": true,
  "public_url": "$PUBLIC_URL",
  "local_port": $MCP_PORT
}
EOF

            # Write formatted details for quick copy-pasting
            cat > "$INFO_FILE" <<EOF
======================================================================
           SSH MCP CONNECTION DETAILS FOR CLOUD AGENT
======================================================================

Connection name:
SSH Terminal (Workstation)

Prompt Guidance (optional):
You have full direct command execution and SSH access to the developer's workstation.
Use 'execute_command' to run shell commands, git operations, builds, linters, tests, and debugging tools.
Use 'ssh_execute' to run commands on remote servers or targets.
Use 'read_file', 'write_file', and 'list_directory' for filesystem manipulation.
Use 'system_status' to check system load, memory, disk, and host info.

Host URL:
$SSE_URL

Access mode:
Read and write

Credentials:
Type: Header
Header name: X-API-Key
Value: $MCP_API_KEY

Base Scope:
Checked (Enabled)

======================================================================
EOF

            log "=================================================================="
            log "Tunnel LIVE! Host URL: $SSE_URL"
            log "Details written to: $INFO_FILE"
            log "=================================================================="

            # Wait for tunnel process to finish
            wait "$TUNNEL_PID" || true
        else
            log "Failed to get ngrok URL, falling back..."
            kill -9 "$TUNNEL_PID" 2>/dev/null || true
        fi
    fi

    # Fallback to cloudflared if ngrok exits or fails
    log "Starting Cloudflare quick tunnel for http://127.0.0.1:$MCP_PORT..."
    stdbuf -oL -eL cloudflared tunnel --no-autoupdate --url "http://127.0.0.1:$MCP_PORT" 2>&1 | while IFS= read -r line; do
        echo "$line" >> "$TUNNEL_LOG"
        if [[ "$line" =~ https://([a-zA-Z0-9-]+\.trycloudflare\.com) ]]; then
            PUBLIC_URL="${BASH_REMATCH[0]}"
            SSE_URL="$PUBLIC_URL/sse"
            echo "$PUBLIC_URL" > "$URL_FILE"
            log "Cloudflare Tunnel LIVE! Host URL: $SSE_URL"
        fi
    done
    log "WARN: Tunnel exited. Restarting in 3 seconds..."
    sleep 3
done
