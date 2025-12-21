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
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "rx_snapshot"

    # Define base metrics to be duplicated
    base_metrics = [
        "cash_val", "cash_tag",
        "liab_val", "liab_tag",
        "assets_val", "assets_tag",
        "assets_cur_val", "assets_cur_tag",
        "liab_cur_val", "liab_cur_tag",
        "ar_val", "ar_tag",
        "inv_val", "inv_tag",
        "debt_val", "debt_tag",
        "oi_val", "oi_tag",
        "int_val", "int_tag",
        "ocf_val", "ocf_tag",
        "cash_to_liab",
        "current_ratio",
        "quick_ratio",
        "debt_to_assets",
        "interest_coverage",
        "ocf_to_debt",
    ]

    # Construct headers: Metadata + Q columns + A columns + Error
    headers = ["cik", "entityName", "event_date", "has_companyfacts"]
    
    # Quarterly block
    headers.extend([
        "q_report_end", "q_age_days", "q_report_form", "q_report_fp", "q_report_filed", "q_coverage"
    ])
    headers.extend([f"q_{m}" for m in base_metrics])
    
    # Annual block
    headers.extend([
        "a_report_end", "a_age_days", "a_report_form", "a_report_fp", "a_report_filed", "a_coverage"
    ])
    headers.extend([f"a_{m}" for m in base_metrics])

    headers.append("error")

    ws.append(headers)

    # header style
    for col in range(1, len(headers) + 1):
        c = ws.cell(row=1, column=col)
        c.font = Font(bold=True)
        c.fill = PatternFill("solid", fgColor="F2F2F2")
        c.alignment = Alignment(horizontal="center", vertical="center")

    def num(x: Any) -> float | None:
        if x is None or x == "":
            return None
        if isinstance(x, (int, float)):
            return float(x)
        try:
            return float(x)
        except Exception:
            return None

    for r in results:
        # Build the row dynamically based on headers
        row = []
        for h in headers:
            val = r.get(h)
            
            # Apply number conversion for value/ratio columns
            if h.endswith("_val") or h in {"age_days", "coverage"} or h.endswith("_to_liab") or h.endswith("_ratio") or h.endswith("_coverage") or h.endswith("_to_debt"):
                 # Note: age_days/coverage are inside report_meta in xbrl_extract, 
                 # but build_rx_snapshot flattens them into "q_age_days", etc.
                 # So r.get(h) works directly.
                 # Check if it's a numeric column
                 if "val" in h or "ratio" in h or "to_" in h or "age_" in h or "coverage" in h:
                     val = num(val)
            
            # has_companyfacts is integer
            if h == "has_companyfacts":
                val = int(val) if val is not None else 0

            row.append(val)

        ws.append(row)

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"

    # widths
    for i, h in enumerate(headers, start=1):
        col = get_column_letter(i)
        if h in {"entityName", "error"}:
            ws.column_dimensions[col].width = 40
        elif h in {"cik"}:
            ws.column_dimensions[col].width = 12
        elif h.endswith("_tag") or "report_" in h:
            ws.column_dimensions[col].width = 22
        else:
            ws.column_dimensions[col].width = 16

    # number formats
    # Heuristic: if header ends with _val -> integer/number format
    # if header is a ratio -> 3 decimals
    
    val_suffixes = ("_val",)
    ratio_suffixes = ("_to_liab", "_ratio", "_to_assets", "_coverage", "_to_debt")

    for row_idx in range(2, ws.max_row + 1):
        for col_idx, h in enumerate(headers, start=1):
            cell = ws.cell(row=row_idx, column=col_idx)
            if isinstance(cell.value, (int, float)):
                if any(h.endswith(s) for s in val_suffixes):
                     cell.number_format = "#,##0"
                elif any(h.endswith(s) for s in ratio_suffixes):
                     cell.number_format = "0.000"

    wb.save(p)
    log.info("Wrote %s (%d rows)", str(p), len(results))