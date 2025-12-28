# #!/usr/bin/env bash
# set -euo pipefail

# # -----------------------------
# # venv bootstrap
# # -----------------------------
# VENV_DIR="${VENV_DIR:-.venv}"
# PYTHON_BIN="${PYTHON_BIN:-python3}"
# REQ_FILE="${REQ_FILE:-requirements.txt}"

# if [[ ! -d "$VENV_DIR" ]]; then
#   echo "[run.sh] Creating venv at $VENV_DIR"
#   "$PYTHON_BIN" -m venv "$VENV_DIR"
# fi

# # shellcheck disable=SC1091
# source "$VENV_DIR/bin/activate"

# # optional: keep pip sane
# python -m pip install --upgrade pip wheel setuptools >/dev/null

# if [[ -f "$REQ_FILE" ]]; then
#   echo "[run.sh] Installing deps from $REQ_FILE"
#   python -m pip install -r "$REQ_FILE"
# else
#   echo "[run.sh] WARNING: $REQ_FILE not found; skipping pip install"
# fi

# # -----------------------------
# # mode + args
# # -----------------------------
# MODE="${1:-sec}"
# if [[ "$MODE" != "sec" && "$MODE" != "court" && "$MODE" != "insider" && "$MODE" != "submissions" ]]; then
#   echo "Usage: ./run.sh [sec|court|insider|submissions] [-i input.xlsx] [-s sheet] [-n rows] [-r rps] [-c conc] [-t timeout] [-l log]"
#   exit 2
# fi
# shift || true

# INPUT_XLSX="${INPUT_XLSX:-input.xlsx}"
# INPUT_SHEET="${INPUT_SHEET:-}"
# LIMIT_ROWS="${LIMIT_ROWS:-0}"
# MAX_RPS="${MAX_RPS:-3}"
# MAX_CONCURRENCY="${MAX_CONCURRENCY:-20}"
# HTTP_TIMEOUT="${HTTP_TIMEOUT:-30}"
# LOG_LEVEL="${LOG_LEVEL:-INFO}"

# while getopts ":i:s:n:r:c:t:l:" opt; do
#   case "$opt" in
#     i) INPUT_XLSX="$OPTARG" ;;
#     s) INPUT_SHEET="$OPTARG" ;;
#     n) LIMIT_ROWS="$OPTARG" ;;
#     r) MAX_RPS="$OPTARG" ;;
#     c) MAX_CONCURRENCY="$OPTARG" ;;
#     t) HTTP_TIMEOUT="$OPTARG" ;;
#     l) LOG_LEVEL="$OPTARG" ;;
#     *) echo "bad args"; exit 2 ;;
#   esac
# done

# export INPUT_XLSX INPUT_SHEET LIMIT_ROWS MAX_RPS MAX_CONCURRENCY HTTP_TIMEOUT LOG_LEVEL

# if [[ "$MODE" == "sec" ]]; then
#   ENTRY="main.py"
# elif [[ "$MODE" == "court" ]]; then
#   ENTRY="main_court.py"
# elif [[ "$MODE" == "insider" ]]; then
#   ENTRY="main_insider.py"
# else
#   ENTRY="main_submissions.py"
# fi

# echo "[run.sh] Mode: $MODE"
# echo "[run.sh] Python: $(command -v python)"
# echo "[run.sh] Running: python $ENTRY"
# echo "[run.sh] INPUT_XLSX=$INPUT_XLSX  INPUT_SHEET=${INPUT_SHEET:-<active>}  LIMIT_ROWS=$LIMIT_ROWS"
# echo "[run.sh] MAX_RPS=$MAX_RPS  MAX_CONCURRENCY=$MAX_CONCURRENCY  HTTP_TIMEOUT=$HTTP_TIMEOUT  LOG_LEVEL=$LOG_LEVEL"

# python "$ENTRY"


#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-sec}"
if [[ "$MODE" != "sec" && "$MODE" != "court" && "$MODE" != "insider" && "$MODE" != "submissions" ]]; then
  echo "Usage: ./run.sh [sec|court|insider|submissions] [-i input.xlsx] [-s sheet] [-n rows] [-r rps] [-c conc] [-t timeout] [-l log] [-d days] [-k keywords]"
  exit 2
fi
shift || true

INPUT_XLSX="${INPUT_XLSX:-input.xlsx}"
INPUT_SHEET="${INPUT_SHEET:-}"
LIMIT_ROWS="${LIMIT_ROWS:-0}"
MAX_RPS="${MAX_RPS:-3}"
MAX_CONCURRENCY="${MAX_CONCURRENCY:-20}"
HTTP_TIMEOUT="${HTTP_TIMEOUT:-30}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
# Court Defaults
COURT_DAYS="${COURT_DAYS:-90,120,180}"
COURT_MOTION_KEYWORDS="${COURT_MOTION_KEYWORDS:-motion}"

while getopts ":i:s:n:r:c:t:l:d:k:" opt; do
  case "$opt" in
    i) INPUT_XLSX="$OPTARG" ;;
    s) INPUT_SHEET="$OPTARG" ;;
    n) LIMIT_ROWS="$OPTARG" ;;
    r) MAX_RPS="$OPTARG" ;;
    c) MAX_CONCURRENCY="$OPTARG" ;;
    t) HTTP_TIMEOUT="$OPTARG" ;;
    l) LOG_LEVEL="$OPTARG" ;;
    d) COURT_DAYS="$OPTARG" ;;
    k) COURT_MOTION_KEYWORDS="$OPTARG" ;;
    *) echo "bad args"; exit 2 ;;
  esac
done

export INPUT_XLSX INPUT_SHEET LIMIT_ROWS MAX_RPS MAX_CONCURRENCY HTTP_TIMEOUT LOG_LEVEL COURT_DAYS COURT_MOTION_KEYWORDS

if [[ "$MODE" == "sec" ]]; then
  ENTRY="main.py"
elif [[ "$MODE" == "court" ]]; then
  ENTRY="main_court.py"
elif [[ "$MODE" == "insider" ]]; then
  ENTRY="main_insider.py"
else
  ENTRY="main_submissions.py"
fi

echo "[run.sh] Mode: $MODE"
echo "[run.sh] Running: python3 $ENTRY"
echo "[run.sh] INPUT_XLSX=$INPUT_XLSX  INPUT_SHEET=${INPUT_SHEET:-<active>}  LIMIT_ROWS=$LIMIT_ROWS"
echo "[run.sh] MAX_RPS=$MAX_RPS  MAX_CONCURRENCY=$MAX_CONCURRENCY  HTTP_TIMEOUT=$HTTP_TIMEOUT  LOG_LEVEL=$LOG_LEVEL"
if [[ "$MODE" == "court" ]]; then
    echo "[run.sh] COURT_DAYS=$COURT_DAYS  COURT_MOTION_KEYWORDS=$COURT_MOTION_KEYWORDS"
fi

python3 "$ENTRY"