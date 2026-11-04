#!/usr/bin/env bash
set -e

MODE="${MODE:-static}"
WEB_PORT="${WEB_PORT:-8080}"
VERSION="${APP_VERSION:-2026.11.04.24}"
RELEASE_DATE="${APP_RELEASE_DATE:-2026-11-04}"
LOG_DIR="${LOG_DIR:-/app/logs}"

if [ ! -f /app/.container_initialized ]; then
    rm -f "${LOG_DIR}/runtime_config.json" /tmp/reload_config.flag /tmp/soft_config.flag /tmp/force_sweep.flag
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
