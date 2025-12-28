from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable

import httpx

from config import (
    Settings,
    ANNUAL_FORMS,      # Imported
    QUARTERLY_FORMS,   # Imported
    ANCHOR_TAGS,
    TAG_CASH,
    TAG_LIAB_TOTAL,
    TAG_LIAB_CUR,
    TAG_LIAB_NONCUR,
    TAG_ASSETS,
    TAG_ASSETS_CUR,
    TAG_AR,
    TAG_INV,
    TAG_DEBT_CUR,
    TAG_DEBT_LT,
    TAG_OI,
    TAG_INT,
    TAG_OCF,
)
from edgar_client import EdgarAsyncClient


log = logging.getLogger("xbrl_extract")


def _iso_to_date(s: str) -> date:
    return datetime.strptime(s[:10], "%Y-%m-%d").date()


def _as_iso10(s: Any) -> str | None:
    if not isinstance(s, str) or len(s) < 10:
        return None
    return s[:10]


def _iter_tag_points(facts: dict[str, Any], tag: str) -> Iterable[tuple[str, dict[str, Any]]]:
    us_gaap = facts.get("facts", {}).get("us-gaap", {}) or {}
    node = us_gaap.get(tag, {}) or {}
    units = node.get("units", {}) or {}
    for unit, series in units.items():
        if isinstance(series, list):
            for pt in series:
                if isinstance(pt, dict):
                    yield unit, pt


@dataclass(frozen=True)
class Point:
    tag: str
    unit: str
    val: float
    end: str
    filed: str | None
    fp: str | None
    form: str | None
    accn: str | None


def point_for_end(
    facts: dict[str, Any],
    tag_candidates: list[str],
    end_iso: str,
    *,
    prefer_unit: str = "USD",
) -> Point | None:
    best: Point | None = None

    for tag in tag_candidates:
        for unit, pt in _iter_tag_points(facts, tag):
            end = _as_iso10(pt.get("end"))
            if end != end_iso:
                continue

            val = pt.get("val")
            if not isinstance(val, (int, float)):
                continue

            cand = Point(
                tag=tag,
                unit=unit,
                val=float(val),
                end=end,
                filed=_as_iso10(pt.get("filed")),
                fp=(pt.get("fp") or None),
                form=(pt.get("form") or None),
                accn=(pt.get("accn") or None),
            )

            def score(x: Point) -> tuple:
                # prefer USD; else arbitrary stable
                return (1 if x.unit == prefer_unit else 0, x.tag)

            if best is None or score(cand) > score(best):
                best = cand

    return best


def latest_report_end_within_window(
    facts: dict[str, Any],
    *,
    event_iso: str,
    max_age_days: int,
    allowed_forms: set[str],
) -> tuple[str | None, dict[str, Any]]:
    """
    Find a single report end date to use for ALL metrics:
    - must be <= event date
    - must be within max_age_days
    - must come from the allowed_forms set (e.g. only 10-Q or only 10-K)
    Choose the end date that MAXIMIZES input coverage (then latest end).
    """
    event_d = _iso_to_date(event_iso)

    ends_meta: dict[str, dict[str, Any]] = {}
    ends_coverage: dict[str, int] = {}

    # candidate ends: from all anchor tags
    for tag_list in ANCHOR_TAGS:
        for tag in tag_list:
            for _unit, pt in _iter_tag_points(facts, tag):
                end = _as_iso10(pt.get("end"))
                if not end:
                    continue

                form = (pt.get("form") or "").upper()
                if form and form not in allowed_forms:
                    continue

                end_d = _iso_to_date(end)
                if end_d > event_d:
                    continue

                age = (event_d - end_d).days
                if age < 0 or age > max_age_days:
                    continue

                ends_meta.setdefault(end, {
                    "form": (pt.get("form") or None),
                    "fp": (pt.get("fp") or None),
                    "filed": _as_iso10(pt.get("filed")),
                    "age_days": age,
                })

    if not ends_meta:
        return None, {}

    # coverage scoring: for each candidate end, count how many base inputs exist
    base_inputs = {
        "cash": TAG_CASH,
        "liab": TAG_LIAB_TOTAL,
        "assets": TAG_ASSETS,
        "assets_cur": TAG_ASSETS_CUR,
        "liab_cur": TAG_LIAB_CUR,
        "ar": TAG_AR,
        "inv": TAG_INV,
        "debt_cur": TAG_DEBT_CUR,
        "debt_lt": TAG_DEBT_LT,
        "oi": TAG_OI,
        "int": TAG_INT,
        "ocf": TAG_OCF,
    }

    for end in ends_meta.keys():
        cov = 0
        # liab check
        liab_total = point_for_end(facts, TAG_LIAB_TOTAL, end)
        if liab_total is not None:
            cov += 1
        else:
            lc = point_for_end(facts, TAG_LIAB_CUR, end)
            lnc = point_for_end(facts, TAG_LIAB_NONCUR, end)
            if lc is not None and lnc is not None:
                cov += 1
        # debt check
        dc = point_for_end(facts, TAG_DEBT_CUR, end)
        dl = point_for_end(facts, TAG_DEBT_LT, end)
        if dc is not None or dl is not None:
            cov += 1
        # rest
        for k, tags in base_inputs.items():
            if k in {"liab", "debt_cur", "debt_lt"}:
                continue
            if point_for_end(facts, tags, end) is not None:
                cov += 1
        ends_coverage[end] = cov

    # pick max coverage, then latest end
    best_end = sorted(ends_meta.keys(), key=lambda e: (ends_coverage.get(e, 0), e))[-1]
    meta = dict(ends_meta[best_end])
    meta["coverage"] = ends_coverage.get(best_end, 0)
    return best_end, meta


def total_liabilities_at_end(facts: dict[str, Any], end_iso: str) -> tuple[float | None, str | None, dict[str, Any]]:
    p = point_for_end(facts, TAG_LIAB_TOTAL, end_iso)
    if p is not None:
        return p.val, p.tag, {"unit": p.unit, "filed": p.filed, "fp": p.fp, "form": p.form, "accn": p.accn}

    lc = point_for_end(facts, TAG_LIAB_CUR, end_iso)
    lnc = point_for_end(facts, TAG_LIAB_NONCUR, end_iso)
    if lc is None or lnc is None:
        return None, None, {}

    return (lc.val + lnc.val), "LiabilitiesCurrent+LiabilitiesNoncurrent", {
        "unit": lc.unit or lnc.unit,
        "filed": lc.filed or lnc.filed,
        "fp": lc.fp or lnc.fp,
        "form": lc.form or lnc.form,
        "accn": lc.accn or lnc.accn,
    }


def total_debt_at_end(facts: dict[str, Any], end_iso: str) -> tuple[float | None, str | None, dict[str, Any]]:
    dc = point_for_end(facts, TAG_DEBT_CUR, end_iso)
    dl = point_for_end(facts, TAG_DEBT_LT, end_iso)

    if dc is None and dl is None:
        return None, None, {}

    val = 0.0
    used = []
    meta = {}

    if dc is not None:
        val += dc.val
        used.append(dc.tag)
        meta = {"unit": dc.unit, "filed": dc.filed, "fp": dc.fp, "form": dc.form, "accn": dc.accn}
    if dl is not None:
        val += dl.val
        used.append(dl.tag)
        if not meta:
            meta = {"unit": dl.unit, "filed": dl.filed, "fp": dl.fp, "form": dl.form, "accn": dl.accn}

    return val, "+".join(used), meta


def safe_div(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    if b == 0:
        return None
    return a / b


def _calculate_metrics_for_report(facts: dict[str, Any], report_end: str, rep_meta: dict[str, Any]) -> dict[str, Any]:
    # base points
    cash_p = point_for_end(facts, TAG_CASH, report_end)
    assets_p = point_for_end(facts, TAG_ASSETS, report_end)
    assets_cur_p = point_for_end(facts, TAG_ASSETS_CUR, report_end)
    liab_cur_p = point_for_end(facts, TAG_LIAB_CUR, report_end)
    ar_p = point_for_end(facts, TAG_AR, report_end)
    inv_p = point_for_end(facts, TAG_INV, report_end)
    oi_p = point_for_end(facts, TAG_OI, report_end)
    int_p = point_for_end(facts, TAG_INT, report_end)
    ocf_p = point_for_end(facts, TAG_OCF, report_end)

    liab_val, liab_tag, _ = total_liabilities_at_end(facts, report_end)
    debt_val, debt_tag, _ = total_debt_at_end(facts, report_end)

    cash_val = cash_p.val if cash_p else None
    assets_val = assets_p.val if assets_p else None
    assets_cur_val = assets_cur_p.val if assets_cur_p else None
    liab_cur_val = liab_cur_p.val if liab_cur_p else None
    ar_val = ar_p.val if ar_p else None
    inv_val = inv_p.val if inv_p else None
    oi_val = oi_p.val if oi_p else None
    int_val = abs(int_p.val) if int_p else None
    ocf_val = ocf_p.val if ocf_p else None

    # Ratios
    cash_to_liab = safe_div(cash_val, liab_val)
    current_ratio = safe_div(assets_cur_val, liab_cur_val)
    quick_ratio = None
    if liab_cur_val:
        if cash_val is not None and ar_val is not None:
            quick_ratio = safe_div(cash_val + ar_val, liab_cur_val)
        elif assets_cur_val is not None and inv_val is not None:
            quick_ratio = safe_div(assets_cur_val - inv_val, liab_cur_val)
    debt_to_assets = safe_div(debt_val, assets_val)
    interest_coverage = safe_div(oi_val, int_val)
    ocf_to_debt = safe_div(ocf_val, debt_val)

    return {
        "report_end": report_end,
        "report_meta": rep_meta,

        "cash_val": cash_val,
        "cash_tag": cash_p.tag if cash_p else None,
        "liab_val": liab_val,
        "liab_tag": liab_tag,
        "assets_val": assets_val,
        "assets_tag": assets_p.tag if assets_p else None,
        "assets_cur_val": assets_cur_val,
        "assets_cur_tag": assets_cur_p.tag if assets_cur_p else None,
        "liab_cur_val": liab_cur_val,
        "liab_cur_tag": liab_cur_p.tag if liab_cur_p else None,
        "ar_val": ar_val,
        "ar_tag": ar_p.tag if ar_p else None,
        "inv_val": inv_val,
        "inv_tag": inv_p.tag if inv_p else None,
        "debt_val": debt_val,
        "debt_tag": debt_tag,
        "oi_val": oi_val,
        "oi_tag": oi_p.tag if oi_p else None,
        "int_val": int_val,
        "int_tag": int_p.tag if int_p else None,
        "ocf_val": ocf_val,
        "ocf_tag": ocf_p.tag if ocf_p else None,

        "cash_to_liab": cash_to_liab,
        "current_ratio": current_ratio,
        "quick_ratio": quick_ratio,
        "debt_to_assets": debt_to_assets,
        "interest_coverage": interest_coverage,
        "ocf_to_debt": ocf_to_debt,
    }


def build_rx_snapshot(
    facts: dict[str, Any],
    *,
    event_iso: str,
    max_age_days: int,
) -> dict[str, Any]:
    """
    Returns double snapshot: Quarterly (q_) and Annual (a_) prefixes.
    """
    
    # 1. Quarterly (10-Q)
    q_end, q_meta = latest_report_end_within_window(
        facts, event_iso=event_iso, max_age_days=max_age_days, allowed_forms=QUARTERLY_FORMS
    )
    if q_end:
        q_metrics = _calculate_metrics_for_report(facts, q_end, q_meta)
    else:
        q_metrics = {}

    # 2. Annual (10-K, 20-F, 40-F)
    a_end, a_meta = latest_report_end_within_window(
        facts, event_iso=event_iso, max_age_days=max_age_days, allowed_forms=ANNUAL_FORMS
    )
    if a_end:
        a_metrics = _calculate_metrics_for_report(facts, a_end, a_meta)
    else:
        a_metrics = {}

    # 3. Merge with prefixes
    out: dict[str, Any] = {"has_companyfacts": 1, "error": ""}
    
    # helper to merge
    def merge(metrics: dict, prefix: str):
        # Flatten report_meta fields: age_days, form, fp, filed, coverage
        rep_meta = metrics.get("report_meta") or {}
        out[f"{prefix}report_end"] = metrics.get("report_end")
        out[f"{prefix}age_days"] = rep_meta.get("age_days")
        out[f"{prefix}report_form"] = rep_meta.get("form")
        out[f"{prefix}report_fp"] = rep_meta.get("fp")
        out[f"{prefix}report_filed"] = rep_meta.get("filed")
        out[f"{prefix}coverage"] = rep_meta.get("coverage")
        
        # Copy values
        keys = [
            "cash_val", "cash_tag", "liab_val", "liab_tag", "assets_val", "assets_tag",
            "assets_cur_val", "assets_cur_tag", "liab_cur_val", "liab_cur_tag",
            "ar_val", "ar_tag", "inv_val", "inv_tag", "debt_val", "debt_tag",
            "oi_val", "oi_tag", "int_val", "int_tag", "ocf_val", "ocf_tag",
            "cash_to_liab", "current_ratio", "quick_ratio", "debt_to_assets",
            "interest_coverage", "ocf_to_debt"
        ]
        for k in keys:
            out[f"{prefix}{k}"] = metrics.get(k)

    merge(q_metrics, "q_")
    merge(a_metrics, "a_")

    return out


async def fetch_rx_snapshot_for_case(
    client: EdgarAsyncClient,
    settings: Settings,
    *,
    cik10: str,
    event_iso: str,
) -> dict[str, Any]:
    try:
        facts = await client.get_company_facts(cik10)
        entity = facts.get("entityName") or ""

        snap = build_rx_snapshot(
            facts,
            event_iso=event_iso,
            max_age_days=settings.max_report_age_days,
        )

        return {
            "cik": cik10,
            "entityName": entity,
            "event_date": event_iso,
            **snap,
        }

    except httpx.HTTPStatusError as e:
        status = getattr(e.response, "status_code", None)
        if status == 404:
            return {
                "cik": cik10,
                "entityName": "",
                "event_date": event_iso,
                "has_companyfacts": 0,
                "error": "404 companyfacts",
            }
        log.exception("HTTPStatusError | %s | %s", cik10, e)
        return {
            "cik": cik10,
            "entityName": "",
            "event_date": event_iso,
            "has_companyfacts": 0,
            "error": str(e),
        }

    except Exception as e:
        log.exception("FAILED | %s | %s", cik10, e)
        return {
            "cik": cik10,
            "entityName": "",
            "event_date": event_iso,
            "has_companyfacts": 0,
            "error": str(e),
        }