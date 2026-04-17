#!/bin/bash

# Configuration
TARGET_BIN="/opt/omd/sites/dev/lmdtest/lmd.linux.amd64"
INTERVAL=0.1 # 100ms
PEAK_MEM=0

echo "Monitoring started for $TARGET_BIN..."

while true; do
    # 1. Wait for the process to start
    PID=$(pgrep -f "$TARGET_BIN" | head -n 1)

    if [ -n "$PID" ]; then
        echo "--- Process started (PID: $PID) ---"
        CURRENT_PEAK=0

        # 2. Monitor while the process is alive
        while kill -0 "$PID" 2>/dev/null; do
            # Get RSS memory in KB
            MEM_KB=$(ps -o rss= -p "$PID" | tr -d ' ')

            if [ -n "$MEM_KB" ]; then
                # Update peak
                if [ "$MEM_KB" -gt "$CURRENT_PEAK" ]; then
                    CURRENT_PEAK=$MEM_KB
                fi
                echo "Current: ${MEM_KB} KB | Peak: ${CURRENT_PEAK} KB"
            fi

            sleep "$INTERVAL"
        done

        # 3. Report peak on termination
        echo "--- Process terminated ---"
        echo "Final Peak Consumption: ${CURRENT_PEAK} KB"
        echo "Waiting for next execution..."
        echo "--------------------------"
    fi

    # Check for process start every 1 second to save CPU when idle
    sleep 1
done
