# court_metrics.py
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable

from court_client import CourtListenerAsyncClient


MOTION_RE = re.compile(r"\bmotion\b", re.IGNORECASE)


def _as_date(x: str | date | datetime | None) -> date | None:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    s = str(x).strip()
    if len(s) >= 10:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _entry_text(e: dict[str, Any]) -> str:
    # CourtListener fields vary a bit by source; keep it defensive.
    for k in ("description", "entry_text", "text", "short_description"):
        v = e.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _entry_date_filed(e: dict[str, Any]) -> date | None:
    # Typical field is date_filed; keep fallback options.
    for k in ("date_filed", "filed", "date_created"):
        d = _as_date(e.get(k))
        if d:
            return d
    return None


@dataclass(frozen=True)
class CourtWindows:
    days: tuple[int, ...] = (90, 120, 180)


async def _paginate_all_entries(
    client: CourtListenerAsyncClient,
    docket_id: int,
    *,
    page_size: int = 100,
    hard_cap_pages: int = 200,   # safety cap to avoid infinite/giant pulls
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1

    while page <= hard_cap_pages:
        js = await client.list_docket_entries(docket_id=docket_id, page=page, page_size=page_size)
        results = js.get("results") or []
        if isinstance(results, list):
            out.extend([r for r in results if isinstance(r, dict)])

        nxt = js.get("next")
        if not nxt:
            break
        page += 1

    return out


async def fetch_docket_and_counts(
    client: CourtListenerAsyncClient,
    *,
    court: str,
    docket_number: str,
    filed_date: str,
    cik: str | None = None,
    windows: CourtWindows = CourtWindows(),
    motion_keywords: Iterable[str] = ("motion",),
) -> dict[str, Any]:
    log = logging.getLogger("court_metrics")

    baseline = _as_date(filed_date)
    if baseline is None:
        return {
            "cik": cik or "",
            "court": court,
            "docket_number": docket_number,
            "error": f"bad filed_date: {filed_date!r}",
        }

    # 1) find docket
    try:
        # Fetch a few results to be safe, though usually the first is correct
        dockets = await client.list_dockets(court=court, docket_number=docket_number, page_size=5)
    except Exception as e:
        log.exception("dockets lookup failed | %s | %s | %s", court, docket_number, e)
        return {"cik": cik or "", "court": court, "docket_number": docket_number, "filed_date": str(baseline), "error": str(e)}

    res = dockets.get("results") or []
    if not res:
        return {
            "cik": cik or "",
            "court": court,
            "docket_number": docket_number,
            "filed_date": str(baseline),
            "docket_id": "",
            "found": 0,
            "error": "docket not found (check court code + docket_number formatting)",
        }

    # Pick first match
    docket = res[0] if isinstance(res[0], dict) else {}
    docket_id = docket.get("id") or ""
    case_name = docket.get("case_name") or ""

    # 2) pull entries (paginate) and compute counts in windows
    try:
        entries = await _paginate_all_entries(client, int(docket_id))
    except Exception as e:
        log.exception("docket-entries failed | docket_id=%s | %s", docket_id, e)
        return {
            "cik": cik or "",
            "court": court,
            "docket_number": docket_number,
            "filed_date": str(baseline),
            "docket_id": docket_id,
            "case_name": case_name,
            "found": 1,
            "error": str(e),
        }

    # precompile keyword regex (OR)
    kws = [re.escape(k) for k in motion_keywords if isinstance(k, str) and k.strip()]
    motion_re = re.compile(r"\b(" + "|".join(kws) + r")\b", re.IGNORECASE) if kws else MOTION_RE

    # counts
    max_days = max(windows.days) if windows.days else 0
    end_max = baseline + timedelta(days=max_days)

    # only consider entries with a usable filed date (and within max window)
    dated: list[tuple[date, dict[str, Any]]] = []
    for e in entries:
        d = _entry_date_filed(e)
        if d and baseline <= d <= end_max:
            dated.append((d, e))

    out: dict[str, Any] = {
        "cik": cik or "",
        "court": court,
        "docket_number": docket_number,
        "filed_date": str(baseline),
        "docket_id": docket_id,
        "case_name": case_name,
        "found": 1,
        "document_count": len(entries),  # Total documents on FreeLaw
    }

    for nd in windows.days:
        end = baseline + timedelta(days=nd)
        in_window = [e for (d, e) in dated if d <= end]

        docket_count = len(in_window)
        motion_count = 0
        for e in in_window:
            txt = _entry_text(e)
            if txt and motion_re.search(txt):
                motion_count += 1

        out[f"docket_count_{nd}d"] = docket_count
        out[f"motion_count_{nd}d"] = motion_count

    return out


async def fetch_court_metrics_for_case(
    client: CourtListenerAsyncClient,
    *,
    row: dict[str, Any],
    days: tuple[int, ...] = (90, 120, 180),
    motion_keywords: tuple[str, ...] = ("motion",),
) -> dict[str, Any]:
    return await fetch_docket_and_counts(
        client,
        court=str(row.get("court", "")).strip(),
        docket_number=str(row.get("docket_number", "")).strip(),
        filed_date=str(row.get("filed_date", "")).strip(),
        cik=str(row.get("cik", "")).strip() or None,
        windows=CourtWindows(days=days),
        motion_keywords=motion_keywords,
    )