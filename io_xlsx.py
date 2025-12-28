from __future__ import annotations

import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


log = logging.getLogger("io_xlsx")


def _to_iso_date(val: Any) -> str | None:
    if val is None:
        return None

    if isinstance(val, (datetime, date)):
        return val.date().isoformat() if isinstance(val, datetime) else val.isoformat()

    s = str(val).strip()
    if not s:
        return None

    # already ISO
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]

    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass

    return None


def load_cik_event_dates_xlsx(path: str, sheet_name: str | None = None) -> list[tuple[str, str]]:
    """
    Reads .xlsx with columns: CIK | start_date | end_date
    We use start_date as event_date.
    """
    if not os.path.exists(path):
        log.error("Excel file not found: %s (cwd=%s)", path, os.getcwd())
        return []

    wb = load_workbook(path, read_only=True, data_only=False)
    ws = wb[sheet_name] if sheet_name else wb.active

    header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
    header = [str(x).strip().lower() if x is not None else "" for x in header_row]

    def find_col(names: set[str]) -> int | None:
        for i, h in enumerate(header):
            if h in names:
                return i
        return None

    cik_col = find_col({"cik", "cik10", "company_cik"}) or 0
    start_col = find_col({"start", "start_date", "from", "from_date"}) or 1

    out: list[tuple[str, str]] = []
    skipped = 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        if row is None or len(row) <= max(cik_col, start_col):
            continue

        cik_raw = row[cik_col]
        start_raw = row[start_col]

        if cik_raw is None or start_raw is None:
            skipped += 1
            continue

        cik_str = str(cik_raw).strip()
        if not cik_str:
            skipped += 1
            continue

        event_iso = _to_iso_date(start_raw)
        if not event_iso:
            skipped += 1
            continue

        out.append((cik_str, event_iso))

    log.info("Loaded %d rows (skipped %d)", len(out), skipped)
    return out


def write_rx_snapshot_xlsx(results: list[dict[str, Any]], path: str) -> None:
    """Write results to an .xlsx.

    This writer is schema-flexible: it will automatically add any new keys
    (e.g., q_* and a_* columns) without needing manual header edits.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "rx_snapshot"

    # Stable header ordering
    base = ["cik", "entityName", "event_date", "has_companyfacts"]

    all_keys: set[str] = set()
    for r in results:
        if isinstance(r, dict):
            all_keys.update(r.keys())

    q_keys = sorted(k for k in all_keys if k.startswith("q_"))
    a_keys = sorted(k for k in all_keys if k.startswith("a_"))
    other = sorted(k for k in all_keys if k not in set(base) and not k.startswith(("q_", "a_")))

    headers = [h for h in base if h in all_keys] + q_keys + a_keys + other

    ws.append(headers)

    # header style
    for col in range(1, len(headers) + 1):
        c = ws.cell(row=1, column=col)
        c.font = Font(bold=True)
        c.fill = PatternFill("solid", fgColor="F2F2F2")
        c.alignment = Alignment(horizontal="center", vertical="center")

    ratio_suffixes = {
        "cash_to_liab",
        "current_ratio",
        "quick_ratio",
        "debt_to_assets",
        "interest_coverage",
        "ocf_to_debt",
    }

    def is_numeric_col(h: str) -> bool:
        if h.endswith("_val"):
            return True
        if h.endswith("age_days"):
            return True
        # ratios (support prefixed q_*/a_*)
        for suf in ratio_suffixes:
            if h == suf or h.endswith("_" + suf):
                return True
        return False

    def to_number(x: Any) -> float | None:
        if x is None or x == "":
            return None
        if isinstance(x, bool):
            return float(int(x))
        if isinstance(x, (int, float)):
            return float(x)
        try:
            return float(str(x).strip())
        except Exception:
            return None

    for r in results:
        row: list[Any] = []
        for h in headers:
            v = r.get(h, "")
            if is_numeric_col(h):
                row.append(to_number(v))
            else:
                row.append(v)
        ws.append(row)

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"

    # widths
    for i, h in enumerate(headers, start=1):
        col = get_column_letter(i)
        if h in {"entityName"}:
            ws.column_dimensions[col].width = 40
        elif h.endswith("_error") or h.endswith("error"):
            ws.column_dimensions[col].width = 40
        elif h in {"cik"}:
            ws.column_dimensions[col].width = 12
        elif h.endswith("_tag") or h.endswith("_form") or h.endswith("_fp") or h.endswith("_filed"):
            ws.column_dimensions[col].width = 22
        elif h.endswith("report_end"):
            ws.column_dimensions[col].width = 14
        else:
            ws.column_dimensions[col].width = 16

    # number formats
    for row_idx in range(2, ws.max_row + 1):
        for col_idx, h in enumerate(headers, start=1):
            cell = ws.cell(row=row_idx, column=col_idx)
            if not isinstance(cell.value, (int, float)):
                continue

            if h.endswith("_val"):
                cell.number_format = "#,##0"
            elif h.endswith("age_days"):
                cell.number_format = "0"
            else:
                for suf in ratio_suffixes:
                    if h == suf or h.endswith("_" + suf):
                        cell.number_format = "0.000"
                        break

    wb.save(p)
    log.info("Wrote %s (%d rows)", str(p), len(results))
