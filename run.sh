#!/usr/bin/env bash
set -euo pipefail

INPUT_XLSX="${INPUT_XLSX:-input.xlsx}"
INPUT_SHEET="${INPUT_SHEET:-}"
LIMIT_ROWS="${LIMIT_ROWS:-0}"
MAX_RPS="${MAX_RPS:-3}"
MAX_CONCURRENCY="${MAX_CONCURRENCY:-20}"
HTTP_TIMEOUT="${HTTP_TIMEOUT:-30}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

while getopts ":i:s:n:r:c:t:l:" opt; do
  case "$opt" in
    i) INPUT_XLSX="$OPTARG" ;;
    s) INPUT_SHEET="$OPTARG" ;;
    n) LIMIT_ROWS="$OPTARG" ;;
    r) MAX_RPS="$OPTARG" ;;
    c) MAX_CONCURRENCY="$OPTARG" ;;
    t) HTTP_TIMEOUT="$OPTARG" ;;
    l) LOG_LEVEL="$OPTARG" ;;
    *) echo "bad args"; exit 2 ;;
  esac
done

export INPUT_XLSX INPUT_SHEET LIMIT_ROWS MAX_RPS MAX_CONCURRENCY HTTP_TIMEOUT LOG_LEVEL

echo "[run.sh] Running: python3 main.py"
echo "[run.sh] INPUT_XLSX=$INPUT_XLSX  INPUT_SHEET=${INPUT_SHEET:-<active>}  LIMIT_ROWS=$LIMIT_ROWS"
echo "[run.sh] MAX_RPS=$MAX_RPS  MAX_CONCURRENCY=$MAX_CONCURRENCY  HTTP_TIMEOUT=$HTTP_TIMEOUT  LOG_LEVEL=$LOG_LEVEL"

python3 main.py