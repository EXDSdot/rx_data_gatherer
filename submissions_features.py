from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import httpx
from edgar_client import EdgarAsyncClient
from config import Settings


def _as_date_iso(s: Any) -> date | None:
    if s is None:
        return None
    if isinstance(s, date) and not isinstance(s, datetime):
        return s
    if isinstance(s, datetime):
        return s.date()
    txt = str(s).strip()
    if len(txt) < 10:
        return None
    try:
        return datetime.strptime(txt[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _norm_form(x: Any) -> str:
    # Normalize SEC "form" strings for robust matching.
    # e.g. "8-k", "8-K/A", "nt 10-q", "NT 10-Q"
    s = (str(x) if x is not None else "").strip().upper()
    # collapse internal whitespace
    s = " ".join(s.split())
    return s


@dataclass(frozen=True)
class SubmissionsWindows:
    days: tuple[int, ...] = (90, 180)


def _pick_recent_arrays(sub: dict[str, Any]) -> tuple[list[str], list[str]]:
    recent = (sub.get("filings", {}) or {}).get("recent", {}) or {}
    forms = recent.get("form", []) or []
    dates = recent.get("filingDate", []) or []
    # ensure lists
    forms = [str(x) for x in forms]
    dates = [str(x) for x in dates]
    return forms, dates


def _count_forms_in_window(
    forms: list[str],
    filing_dates: list[str],
    *,
    start: date,
    end: date,
    allow: set[str],
) -> int:
    n = 0
    for f_raw, d_raw in zip(forms, filing_dates):
        d = _as_date_iso(d_raw)
        if not d or not (start <= d <= end):
            continue
        f = _norm_form(f_raw)
        if f in allow:
            n += 1
    return n


def _days_since_last_form(
    forms: list[str],
    filing_dates: list[str],
    *,
    end: date,
    allow: set[str],
) -> int | None:
    last: date | None = None
    for f_raw, d_raw in zip(forms, filing_dates):
        d = _as_date_iso(d_raw)
        if not d or d > end:
            continue
        f = _norm_form(f_raw)
        if f not in allow:
            continue
        if last is None or d > last:
            last = d
    if last is None:
        return None
    return (end - last).days


async def fetch_submissions_snapshot_for_case(
    client: EdgarAsyncClient,
    settings: Settings,
    *,
    cik10: str,
    event_iso: str,
    windows: SubmissionsWindows,
) -> dict[str, Any]:
    log = logging.getLogger("submissions_features")

    event_d = _as_date_iso(event_iso)
    if event_d is None:
        return {"cik": cik10, "event_date": event_iso, "error": "bad event_date"}

    try:
        sub = await client.get_submissions(cik10)
    except httpx.HTTPStatusError as e:
        # Handle 404 gently without full traceback
        if e.response.status_code == 404:
            log.warning("submissions 404 Not Found | %s", cik10)
        else:
            log.exception("submissions http error | %s | %s", cik10, e)
        return {"cik": cik10, "event_date": event_iso, "error": str(e)}
    except Exception as e:
        log.exception("submissions failed | %s | %s", cik10, e)
        return {"cik": cik10, "event_date": event_iso, "error": str(e)}

    entity = sub.get("name") or ""
    forms, filing_dates = _pick_recent_arrays(sub)

    EIGHTK = {"8-K", "8-K/A"}
    NT_10K = {"NT 10-K"}
    NT_10Q = {"NT 10-Q"}
    TENK_TENQ = {"10-K", "10-K/A", "10-Q", "10-Q/A"}
    
    # Combined set for logging "used" forms
    ALL_INTERESTING = EIGHTK | NT_10K | NT_10Q | TENK_TENQ

    out: dict[str, Any] = {
        "cik": cik10,
        "entityName": entity,
        "event_date": event_d.isoformat(),
        "error": "",
    }

    # --- Collect the dates actually USED in calculations ---
    max_days = max(windows.days) if windows.days else 180
    earliest_start = event_d - timedelta(days=max_days)
    
    used_filings: list[dict[str, str]] = []

    for f_raw, d_raw in zip(forms, filing_dates):
        d = _as_date_iso(d_raw)
        if not d:
            continue
        
        # Only log if it's within the analysis window (start to event_date)
        if earliest_start <= d <= event_d:
            f_norm = _norm_form(f_raw)
            if f_norm in ALL_INTERESTING:
                used_filings.append({"date": d.isoformat(), "form": f_norm})

    # Store for main to write to Excel
    out["_used_filings"] = used_filings

    # Logging trace (optional, kept for debugging)
    if used_filings:
        items = [f"{u['date']}({u['form']})" for u in used_filings]
        log.info("CIK %s event %s USED forms: %s", cik10, event_iso, ", ".join(items))
    else:
        log.info("CIK %s event %s: No relevant forms found in window.", cik10, event_iso)
    # ---------------------------------------------------

    for nd in windows.days:
        start = event_d - timedelta(days=nd)

        eightk_cnt = _count_forms_in_window(forms, filing_dates, start=start, end=event_d, allow=EIGHTK)
        nt10k_cnt = _count_forms_in_window(forms, filing_dates, start=start, end=event_d, allow=NT_10K)
        nt10q_cnt = _count_forms_in_window(forms, filing_dates, start=start, end=event_d, allow=NT_10Q)

        out[f"eightk_count_{nd}d"] = eightk_cnt
        out[f"nt_10k_count_{nd}d"] = nt10k_cnt
        out[f"nt_10q_count_{nd}d"] = nt10q_cnt
        out[f"late_filer_flag_{nd}d"] = 1 if (nt10k_cnt + nt10q_cnt) > 0 else 0

        # simple “intensity” per 30d, nice for regressions
        out[f"eightk_per_30d_{nd}d"] = (eightk_cnt / nd) * 30.0 if nd > 0 else None

    out["days_since_last_10k_or_10q"] = _days_since_last_form(
        forms, filing_dates, end=event_d, allow=TENK_TENQ
    )

    return out