from __future__ import annotations

import asyncio
import logging
import os

from rich.console import Console
from rich.live import Live

from config import Settings
from edgar_client import EdgarAsyncClient, RunStats, progress_reporter, setup_logging
from io_insider_xlsx import load_cik_event_dates_xlsx, write_insider_snapshot_xlsx
from insider_extract import fetch_insider_snapshot_for_case


async def main() -> int:
    settings = Settings()
    setup_logging(settings.log_path)
    log = logging.getLogger("main_insider")

    lookback_days = int(os.getenv("INSIDER_LOOKBACK_DAYS", "180"))
    out_xlsx = os.getenv("INSIDER_OUT_XLSX", "insider_snapshot.xlsx")

    rows = load_cik_event_dates_xlsx(settings.input_xlsx, sheet_name=settings.input_sheet)
    if settings.limit_rows > 0:
        rows = rows[: settings.limit_rows]

    normalized: list[tuple[str, str]] = []
    for cik_raw, event_iso in rows:
        try:
            cik10 = EdgarAsyncClient.normalize_cik(cik_raw)
            if cik10 != "0000000000":
                normalized.append((cik10, event_iso))
        except Exception:
            continue

    log.info("Loaded %d cases (after CIK normalization)", len(normalized))

    console = Console()
    stats = RunStats(task_name="INSIDER (Form 4)", total_units=len(normalized))
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
            _wrap_unit(
                fetch_insider_snapshot_for_case(
                    client,
                    settings,
                    cik10=cik10,
                    event_iso=event_iso,
                    lookback_days=lookback_days,
                )
            )
            for (cik10, event_iso) in normalized
        ]

        results = await asyncio.gather(*tasks)

        if not os.path.isabs(out_xlsx):
            base_dir = os.path.dirname(__file__)
            out_xlsx = os.path.join(base_dir, out_xlsx)

        write_insider_snapshot_xlsx(results, path=out_xlsx)
        log.info("Done. Output: %s", out_xlsx)
        return 0

    finally:
        stats.finished = True
        await reporter
        live.stop()
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))