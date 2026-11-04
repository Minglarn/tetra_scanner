#!/usr/bin/env bash
set -e

MODE="${MODE:-static}"
WEB_PORT="${WEB_PORT:-8080}"
VERSION="${APP_VERSION:-2026.11.04.29}"
RELEASE_DATE="${APP_RELEASE_DATE:-2026-11-04}"
LOG_DIR="${LOG_DIR:-/app/logs}"

mkdir -p "${LOG_DIR}"
ENV_CHECKSUM_FILE="${LOG_DIR}/.env_checksum"
CURRENT_ENV_CHECKSUM="$(printf '%s\n' \
    "MODE=${MODE:-}" \
    "SCAN_START_MHZ=${SCAN_START_MHZ:-}" \
    "SCAN_END_MHZ=${SCAN_END_MHZ:-}" \
    "IDLE_TIMEOUT_SEC=${IDLE_TIMEOUT_SEC:-}" \
    "BLACKLIST_STRIKES=${BLACKLIST_STRIKES:-}" \
    "BLACKLIST_TIME_SEC=${BLACKLIST_TIME_SEC:-}" \
    "RTL_DEVICE_INDEX=${RTL_DEVICE_INDEX:-}" \
    "RTL_GAIN=${RTL_GAIN:-}" \
    "RTL_PPM=${RTL_PPM:-}" \
    "CENTER_FREQ=${CENTER_FREQ:-}" \
    "TETRA_CHANNELS=${TETRA_CHANNELS:-}" | sha256sum | awk '{print $1}')"

if [ ! -f /app/.container_initialized ]; then
    rm -f /tmp/reload_config.flag /tmp/soft_config.flag /tmp/force_sweep.flag
    if [ -f "${ENV_CHECKSUM_FILE}" ]; then
        SAVED_ENV_CHECKSUM="$(cat "${ENV_CHECKSUM_FILE}" 2>/dev/null || true)"
        if [ "${SAVED_ENV_CHECKSUM}" != "${CURRENT_ENV_CHECKSUM}" ]; then
            echo "[entrypoint] Detected .env change, syncing runtime_config.json from .env..."
            rm -f "${LOG_DIR}/runtime_config.json"
        fi
    fi
    echo "${CURRENT_ENV_CHECKSUM}" > "${ENV_CHECKSUM_FILE}"
    touch /app/.container_initialized
fi

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
echo "  Initial Mode : ${MODE}"
echo "  WebUI Port   : ${WEB_PORT}"
echo "=============================================================================================="

echo "[entrypoint] Starting FastAPI WebUI on 0.0.0.0:${WEB_PORT}..."
uvicorn src.web_api:app --host 0.0.0.0 --port "${WEB_PORT}" --no-access-log --log-level warning &

echo "[entrypoint] Starting TETRA Orchestrator (supports hot-reload for static & scanner modes)..."
exec python3 /app/src/scanner_orchestrator.py
