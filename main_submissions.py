from __future__ import annotations

import asyncio
import logging
import os

from rich.console import Console
from rich.live import Live

from config import Settings
from edgar_client import EdgarAsyncClient, RunStats, progress_reporter, setup_logging
from io_xlsx import load_cik_event_dates_xlsx
from io_submissions_xlsx import write_submissions_snapshot_xlsx, write_used_dates_xlsx
from submissions_features import SubmissionsWindows, fetch_submissions_snapshot_for_case
from post_merge import merge_lopucki_to_features, generate_regression_file


def _parse_days_env(s: str) -> tuple[int, ...]:
    parts = [p.strip() for p in (s or "").split(",") if p.strip()]
    out: list[int] = []
    for p in parts:
        try:
            n = int(p)
            if n > 0:
                out.append(n)
        except Exception:
            continue
    return tuple(out) if out else (90, 180)


async def main() -> int:
    settings = Settings()
    setup_logging(settings.log_path)
    log = logging.getLogger("main_submissions")

    rows = load_cik_event_dates_xlsx(settings.input_xlsx, sheet_name=settings.input_sheet)
    if settings.limit_rows > 0:
        rows = rows[:settings.limit_rows]

    normalized: list[tuple[str, str]] = []
    for cik_raw, event_iso in rows:
        try:
            cik10 = EdgarAsyncClient.normalize_cik(cik_raw)
            if cik10 != "0000000000":
                normalized.append((cik10, event_iso))
        except Exception:
            continue

    log.info("Loaded %d cases (after CIK normalization)", len(normalized))

    days = _parse_days_env(os.getenv("SEC_DAYS", "90,180"))
    windows = SubmissionsWindows(days=days)

    console = Console()
    stats = RunStats(task_name="SEC SUBMISSIONS", total_units=len(normalized))
    client = EdgarAsyncClient(settings, stats=stats)

    live = Live("", console=console, refresh_per_second=10, transient=True)
    live.start()
    reporter = asyncio.create_task(progress_reporter(live, stats))

    try:
        async def _wrap_unit(coro):
            try:
                res = await coro
                # Check for CLI warnings (e.g. future dates) passed from the feature extractor
                if warning := res.get("_cli_warning"):
                    console.print(warning)
                return res
            finally:
                await stats.record_unit_done()

        tasks = [
            _wrap_unit(fetch_submissions_snapshot_for_case(client, settings, cik10=cik10, event_iso=event_iso, windows=windows))
            for (cik10, event_iso) in normalized
        ]
        results = await asyncio.gather(*tasks)

        out_path = os.getenv("OUT_XLSX") or "sec_submissions_features.xlsx"
        if not os.path.isabs(out_path):
            base_dir = os.path.dirname(__file__)
            out_path = os.path.join(base_dir, out_path)

        # 1. Write the main features Excel
        write_submissions_snapshot_xlsx(results, days=days, path=out_path)

        # 2. Write the "used dates" audit Excel
        used_rows = []
        for res in results:
            cik = str(res.get("cik", ""))
            ename = str(res.get("entityName", ""))
            edate = str(res.get("event_date", ""))
            
            filings = res.get("_used_filings", [])
            
            if not filings:
                used_rows.append({
                    "cik": cik, 
                    "entityName": ename, 
                    "event_date": edate,
                    "filing_date": "(none)",
                    "form": "(none)"
                })
            else:
                for f in filings:
                    used_rows.append({
                        "cik": cik,
                        "entityName": ename,
                        "event_date": edate,
                        "filing_date": f.get("date", ""),
                        "form": f.get("form", "")
                    })

        used_out_path = out_path.replace(".xlsx", "_used_dates.xlsx")
        if used_out_path == out_path:
            used_out_path = out_path + "_used_dates.xlsx"
        
        write_used_dates_xlsx(used_rows, used_out_path)
        
        log.info("Done features. Output: %s and %s", out_path, used_out_path)

        # 3. Merge with LoPucki BRD if available
        log.info("Starting merge with external dataset at %s...", settings.lopucki_xlsx)
        merged_file_path = merge_lopucki_to_features(
            features_path=out_path, 
            lopucki_path=settings.lopucki_xlsx
        )

        # 4. Generate Regression Data
        if merged_file_path and os.path.exists(merged_file_path):
            reg_out_path = out_path.replace(".xlsx", "_regression.xlsx")
            if reg_out_path == out_path:
                reg_out_path += "_regression.xlsx"
                
            generate_regression_file(merged_file_path, reg_out_path)

        return 0

    finally:
        stats.finished = True
        await reporter
        live.stop()
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))