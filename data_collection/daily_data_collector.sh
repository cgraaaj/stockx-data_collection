#!/bin/bash
# Daily Data Collection Script (NO SYNC version)
# Run via cron: 30 7 * * 1-5 /home/cgraaaj/cgr-trades/src/data_collection/daily_data_collector.sh

set -e

PROJECT_DIR="$HOME/cgr-trades"
LOG_DIR="$PROJECT_DIR/logs"
DATE=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/daily_data_collector_$(date +%Y%m%d_%H%M%S).log"

cd "$PROJECT_DIR"
source venv/bin/activate

echo "Starting data collection for $DATE" >> "$LOG_FILE"
python3 "$PROJECT_DIR/data_collection/opt-stk-data-to-db-upstox-nosync.py" "$DATE" >> "$LOG_FILE" 2>&1
echo "Data collection completed" >> "$LOG_FILE"
