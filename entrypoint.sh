#!/usr/bin/env bash
set -e

MODE="${MODE:-static}"
TETRA_CHANNELS="${TETRA_CHANNELS:-427000000:71,427300000:17,427550000:23,427850000:1,427950000:16,428500000:7}"
VERSION="${APP_VERSION:-2026.11.04.2}"
RELEASE_DATE="${APP_RELEASE_DATE:-2026-11-04}"

cat << 'EOF'
 ███████████ ██████████ ███████████ ███████████     █████████                                 
░█░░░███░░░█░░███░░░░░█░█░░░███░░░█░░███░░░░░███   ███░░░░░███                                
░   ░███  ░  ░███  █ ░ ░   ░███  ░  ░███    ░███  ░███    ░███                                
    ░███     ░██████       ░███     ░██████████   ░███████████                                
    ░███     ░███░░█       ░███     ░███░░░░░███  ░███░░░░░███                                
    ░███     ░███ ░   █    ░███     ░███    ░███  ░███    ░███                                
    █████    ██████████    █████    █████   █████ █████   █████                               
   ░░░░░    ░░░░░░░░░░    ░░░░░    ░░░░░   ░░░░░ ░░░░░   ░░░░░                                
                                                                                              
                                                                                              
                                                                                              
  █████████    █████████    █████████   ██████   █████ ██████   █████ ██████████ ███████████  
 ███░░░░░███  ███░░░░░███  ███░░░░░███ ░░██████ ░░███ ░░██████ ░░███ ░░███░░░░░█░░███░░░░░███ 
░███    ░░░  ███     ░░░  ░███    ░███  ░███░███ ░███  ░███░███ ░███  ░███  █ ░  ░███    ░███ 
░░█████████ ░███          ░███████████  ░███░░███░███  ░███░░███░███  ░██████    ░██████████  
 ░░░░░░░░███░███          ░███░░░░░███  ░███ ░░██████  ░███ ░░██████  ░███░░█    ░███░░░░░███ 
 ███    ░███░░███     ███ ░███    ░███  ░███  ░░█████  ░███  ░░█████  ░███ ░   █ ░███    ░███ 
░░█████████  ░░█████████  █████   █████ █████  ░░█████ █████  ░░█████ ██████████ █████   █████
 ░░░░░░░░░    ░░░░░░░░░  ░░░░░   ░░░░░ ░░░░░    ░░░░░ ░░░░░    ░░░░░ ░░░░░░░░░░ ░░░░░   ░░░░░ 
EOF

echo "=============================================================================================="
echo "  Version      : ${VERSION}"
echo "  Release Date : ${RELEASE_DATE}"
echo "  Created by   : Minglarn"
echo "  Mode         : ${MODE}"
echo "=============================================================================================="

if [ "$MODE" = "scanner" ]; then
    echo "[entrypoint] Starting Auto-Discovery Scanner Orchestrator..."
    exec python3 /app/src/scanner_orchestrator.py
fi

echo "[entrypoint] Static mode - Configured channels: ${TETRA_CHANNELS}"

PIDS=()

cleanup() {
    echo "[entrypoint] Signal received, stopping background processes..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
    echo "[entrypoint] Shutdown complete."
    exit 0
}

trap cleanup SIGTERM SIGINT

IFS=',' read -ra ADDR <<< "$TETRA_CHANNELS"
i=1
for chan in "${ADDR[@]}"; do
    rx_port=$((42000 + i))
    tx_port=$((42100 + i))
    echo "[entrypoint] Starting tetra-kit decoder #${i} (${chan}) on UDP rx=${rx_port} -> tx=${tx_port}"
    /opt/tetra-kit/decoder/decoder -r "${rx_port}" -t "${tx_port}" >/dev/null 2>&1 &
    PIDS+=($!)
    i=$((i + 1))
done

echo "[entrypoint] Starting GNU Radio multi-channel demodulator..."
python3 /app/src/tetra_multi_rtl.py &
PIDS+=($!)

echo "[entrypoint] Starting TETRA JSON parser..."
python3 /app/src/tetra_parser.py &
PARSER_PID=$!
PIDS+=($PARSER_PID)

wait "$PARSER_PID"
cleanup
