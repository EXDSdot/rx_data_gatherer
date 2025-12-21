from __future__ import annotations

import asyncio
import logging
import os

from rich.console import Console
from rich.live import Live

from config import Settings
from edgar_client import EdgarAsyncClient, RunStats, progress_reporter, setup_logging
from io_xlsx import load_cik_event_dates_xlsx
from io_submissions_xlsx import write_submissions_snapshot_xlsx
from submissions_features import SubmissionsWindows, fetch_submissions_snapshot_for_case


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
                return await coro
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

        write_submissions_snapshot_xlsx(results, days=days, path=out_path)
        log.info("Done. Output: %s", out_path)
        return 0

    finally:
        stats.finished = True
        await reporter
        live.stop()
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))