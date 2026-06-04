#!/bin/bash
# LOF Fund Monitor - Start Server Script (with auto-restart)
cd "$(dirname "$0")"

DIR="$(pwd)"
VENV_DIR="$DIR/.venv"

# Find the right Python
if [ -f "$VENV_DIR/bin/python" ]; then
    PYTHON="$VENV_DIR/bin/python"
else
    PYTHON=$(command -v python3 || command -v python)
fi

# Clean up old processes
pkill -f "server.py" 2>/dev/null
sleep 1

# Initialize database if needed
$PYTHON -c "
import asyncio
from db import init_db
asyncio.run(init_db())
print('Database initialized')
"

PORT="${FUND_PORT:-8080}"
echo "Starting LOF Fund Monitor server on port $PORT..."
echo "Access: http://localhost:$PORT"
echo "Admin:  http://localhost:$PORT/admin"
echo ""

# Run server in foreground with auto-restart
while true; do
    FUND_PORT="$PORT" $PYTHON -u server.py
    EXIT_CODE=$?
    echo "Server exited with code $EXIT_CODE, restarting in 3 seconds..."
    sleep 3
done
