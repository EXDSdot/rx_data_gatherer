from __future__ import annotations

import asyncio
import logging
import os

from rich.console import Console
from rich.live import Live

from config import Settings
from edgar_client import EdgarAsyncClient, RunStats, progress_reporter, setup_logging
from io_xlsx import load_cik_event_dates_xlsx, write_rx_snapshot_xlsx
from xbrl_extract import fetch_rx_snapshot_for_case


async def main() -> int:
    settings = Settings()
    setup_logging(settings.log_path)
    log = logging.getLogger("main")

    rows = load_cik_event_dates_xlsx(settings.input_xlsx, sheet_name=settings.input_sheet)
    if settings.limit_rows > 0:
        rows = rows[:settings.limit_rows]

    # normalize CIKs to 10-digit
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
    stats = RunStats(task_name="RX SNAPSHOT (XBRL)", total_units=len(normalized))
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
            _wrap_unit(fetch_rx_snapshot_for_case(client, settings, cik10=cik10, event_iso=event_iso))
            for (cik10, event_iso) in normalized
        ]

        results = await asyncio.gather(*tasks)

        out_path = settings.out_xlsx
        # if user provided only filename, write next to script
        if not os.path.isabs(out_path):
            base_dir = os.path.dirname(__file__)
            out_path = os.path.join(base_dir, out_path)

        write_rx_snapshot_xlsx(results, path=out_path)
        log.info("Done. Output: %s", out_path)
        return 0

    finally:
        stats.finished = True
        await reporter
        live.stop()
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))