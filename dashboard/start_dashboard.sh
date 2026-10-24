#!/bin/bash
# =============================================================================
# dashboard/start_dashboard.sh — Khởi chạy Web Dashboard trên Ubuntu / Linux
# =============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
PORT="${1:-8050}"

cd "$REPO_DIR"

echo "=========================================================="
echo " [INFO] KHOI CHAY BIG DATA LOG ANALYTICS DASHBOARD"
echo " Dia chi truy cap: http://localhost:${PORT}"
echo " Nguon du lieu   : Hadoop HDFS (/data/reports/ & /data/output/)"
echo "=========================================================="

python3 dashboard/server.py "$PORT"
