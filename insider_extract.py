from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import httpx

from edgar_client import EdgarAsyncClient
from config import Settings


INSIDER_FORMS = {"4", "4/A"}  # keep it strict for now


def _as_date_iso(s: str | None) -> date | None:
    if not isinstance(s, str) or len(s) < 10:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _strip_ns(tag: str) -> str:
    # "{namespace}local" -> "local"
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_first_text(root: ET.Element, want: set[str]) -> str:
    want_l = {w.lower() for w in want}
    for el in root.iter():
        if _strip_ns(el.tag).lower() in want_l:
            if el.text and el.text.strip():
                return el.text.strip()
    return ""


def _iter_elems(root: ET.Element, local_name: str):
    ln = local_name.lower()
    for el in root.iter():
        if _strip_ns(el.tag).lower() == ln:
            yield el


def _get_child_text(parent: ET.Element, local_name: str) -> str:
    for el in parent.iter():
        if _strip_ns(el.tag).lower() == local_name.lower():
            if el.text and el.text.strip():
                return el.text.strip()
            return ""
    return ""


async def _get_text_via_edgar_client(client: EdgarAsyncClient, url: str) -> str:
    """
    Reuse the same limiter/semaphore/User-Agent as your SEC client.
    """
    async with client._sem:  # noqa: SLF001 (intentional reuse)
        async with client.limiter:
            req_started = time.perf_counter()
            resp = await client._client.get(url)  # noqa: SLF001
            latency = time.perf_counter() - req_started
            if client.stats is not None:
                await client.stats.record_request(resp.status_code, latency, req_started)
            resp.raise_for_status()
            return resp.text


def _archive_url(cik10: str, accn: str, primary_doc: str) -> str:
    cik_int = str(int(cik10))  # strip leading zeros for archives path
    accn_nodash = accn.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_nodash}/{primary_doc}"


def _parse_form4_xml(xml_text: str) -> dict[str, Any]:
    """
    Parse Form 4 ownership XML (not HTML).
    - Counts *non-derivative* transactions only (robust)
    - Extracts ALL reportingOwner CIKs (so unique_insiders works)
    - Classifies P=buy, S=sell (simple + defensible)
    """
    root = ET.fromstring(xml_text)

    # --- reporting owners (can be multiple) ---
    owner_ciks: set[str] = set()
    for ro in _iter_elems(root, "reportingOwner"):
        cik = _find_first_text(ro, {"rptOwnerCik", "reportingOwnerCik", "ownerCik", "rptOwnerCIK"})
        if cik:
            owner_ciks.add(cik.strip())

    # fallback: sometimes structure is weird
    if not owner_ciks:
        one = _find_first_text(root, {"rptOwnerCik", "reportingOwnerCik", "ownerCik", "rptOwnerCIK"})
        if one:
            owner_ciks.add(one.strip())

    # --- tx aggregation (non-derivatives only) ---
    tx_count = 0
    buy_tx = 0
    sell_tx = 0
    net_shares = 0.0
    net_value = 0.0
    dates: list[date] = []

    for tx in _iter_elems(root, "nonDerivativeTransaction"):
        code = ""
        shares: float | None = None
        price: float | None = None
        tx_date: date | None = None

        # Walk the subtree once; pick the fields we need.
        for node in tx.iter():
            tag = _strip_ns(node.tag).lower()

            if tag == "transactioncode":
                code = (node.text or "").strip().upper()

            elif tag == "transactionshares":
                v = _find_first_text(node, {"value"})
                try:
                    shares = float(v) if v else None
                except Exception:
                    shares = None

            elif tag == "transactionpricepershare":
                v = _find_first_text(node, {"value"})
                try:
                    price = float(v) if v else None
                except Exception:
                    price = None

            elif tag == "transactiondate":
                v = _find_first_text(node, {"value"})
                d = _as_date_iso(v)
                if d:
                    tx_date = d

        if shares is None:
            continue

        tx_count += 1
        if tx_date:
            dates.append(tx_date)

        # Simple classification that’s common in Form 4s:
        # P=open market purchase, S=open market sale
        if code == "P":
            buy_tx += 1
            net_shares += shares
            if price is not None:
                net_value += shares * price
        elif code == "S":
            sell_tx += 1
            net_shares -= shares
            if price is not None:
                net_value -= shares * price

    first_tx = min(dates).isoformat() if dates else ""
    last_tx = max(dates).isoformat() if dates else ""

    return {
        "owner_ciks": sorted(owner_ciks),  # <-- key change vs your old owner_cik
        "tx_count": tx_count,
        "buy_tx": buy_tx,
        "sell_tx": sell_tx,
        "net_shares": net_shares if tx_count else 0.0,
        "net_value_usd": net_value if tx_count else 0.0,
        "first_tx_date": first_tx,
        "last_tx_date": last_tx,
    }


async def fetch_insider_snapshot_for_case(
    client: EdgarAsyncClient,
    settings: Settings,
    *,
    cik10: str,
    event_iso: str,
    lookback_days: int = 180,
) -> dict[str, Any]:
    log = logging.getLogger("insider_extract")

    event_d = _as_date_iso(event_iso)
    if event_d is None:
        return {"cik": cik10, "event_date": event_iso, "lookback_days": lookback_days, "error": "bad event_date"}

    start_d = event_d - timedelta(days=lookback_days)

    try:
        sub = await client.get_submissions(cik10)
    except Exception as e:
        log.exception("submissions failed | %s | %s", cik10, e)
        return {"cik": cik10, "event_date": event_iso, "lookback_days": lookback_days, "error": str(e)}

    entity = sub.get("name") or ""
    recent = (sub.get("filings", {}) or {}).get("recent", {}) or {}

    forms = recent.get("form", []) or []
    filing_dates = recent.get("filingDate", []) or []
    accns = recent.get("accessionNumber", []) or []
    prim_docs = recent.get("primaryDocument", []) or []

    picked: list[tuple[str, str, str]] = []  # (accn, filed_iso, primary_doc)

    for form, fdate, accn, pdoc in zip(forms, filing_dates, accns, prim_docs):
        f = (form or "").strip().upper()
        fd = _as_date_iso(str(fdate)[:10] if isinstance(fdate, str) else str(fdate))
        if not fd:
            continue
        if not (start_d <= fd <= event_d):
            continue
        if f not in INSIDER_FORMS:
            continue
        if not isinstance(accn, str) or not isinstance(pdoc, str) or not accn or not pdoc:
            continue
        picked.append((accn, fd.isoformat(), pdoc))

    # aggregate across filings
    form4_filings = len(picked)
    tx_total = 0
    buy_total = 0
    sell_total = 0
    net_shares = 0.0
    net_value = 0.0
    insiders: set[str] = set()
    first_tx = ""
    last_tx = ""

    for accn, filed_iso, pdoc in picked:
        try:
            idx_url = _archive_dir_index_url(cik10, accn)
            idx = await _get_json_via_edgar_client(client, idx_url)
            xml_name = _pick_form4_xml_from_index(idx)

            # fallback: if primaryDocument is already XML, accept it
            if not xml_name and isinstance(pdoc, str) and pdoc.lower().endswith(".xml"):
                xml_name = pdoc

            if not xml_name:
                log.warning("no xml found | %s | %s | primary=%s", cik10, accn, pdoc)
                continue

            url = _archive_url(cik10, accn, xml_name)
            xml_text = await _get_text_via_edgar_client(client, url)
            one = _parse_form4_xml(xml_text)

        except httpx.HTTPError as e:
            log.warning("form4 fetch failed | %s | %s | %s", cik10, accn, e)
            continue
        except Exception as e:
            log.warning("form4 parse failed | %s | %s | %s", cik10, accn, e)
            continue

        for ocik in (one.get("owner_ciks") or []):
            if isinstance(ocik, str) and ocik.strip():
                insiders.add(ocik.strip())

        tx_total += int(one.get("tx_count", 0) or 0)
        buy_total += int(one.get("buy_tx", 0) or 0)
        sell_total += int(one.get("sell_tx", 0) or 0)
        net_shares += float(one.get("net_shares", 0.0) or 0.0)
        net_value += float(one.get("net_value_usd", 0.0) or 0.0)

        ft = one.get("first_tx_date") or ""
        lt = one.get("last_tx_date") or ""
        if ft and (not first_tx or ft < first_tx):
            first_tx = ft
        if lt and (not last_tx or lt > last_tx):
            last_tx = lt

    return {
        "cik": cik10,
        "entityName": entity,
        "event_date": event_d.isoformat(),
        "lookback_days": lookback_days,
        "form4_filings": form4_filings,
        "tx_count": tx_total,
        "buy_tx": buy_total,
        "sell_tx": sell_total,
        "net_shares": net_shares,
        "net_value_usd": net_value,
        "unique_insiders": len(insiders),
        "first_tx_date": first_tx,
        "last_tx_date": last_tx,
        "error": "",
    }

import json  # add at top

def _archive_dir_index_url(cik10: str, accn: str) -> str:
    cik_int = str(int(cik10))
    accn_nodash = accn.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_nodash}/index.json"


async def _get_json_via_edgar_client(client: EdgarAsyncClient, url: str) -> dict[str, Any]:
    txt = await _get_text_via_edgar_client(client, url)
    return json.loads(txt)


def _pick_form4_xml_from_index(index_json: dict[str, Any]) -> str:
    """
    Return best guess XML filename inside an accession folder.
    Prefers actual ownership XML; avoids FilingSummary / exhibit XML noise.
    """
    items = (((index_json.get("directory") or {}).get("item")) or [])
    if not isinstance(items, list):
        return ""

    xmls: list[str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        name = (it.get("name") or "").strip()
        low = name.lower()
        if not name or not low.endswith(".xml"):
            continue
        if low in {"filingsummary.xml"}:
            continue
        if low.startswith("r") and low.endswith(".xml"):  # R1.xml, R2.xml etc
            continue
        xmls.append(name)

    if not xmls:
        return ""

    # rank candidates
    def score(n: str) -> tuple[int, int]:
        low = n.lower()
        s = 0
        if "ownership" in low:
            s += 5
        if "doc4" in low or "form4" in low:
            s += 4
        if "primary" in low:
            s += 2
        if "xsl" in low:
            s += 1
        # prefer shorter filenames (often the main doc)
        return (s, -len(low))

    return sorted(xmls, key=score, reverse=True)[0]