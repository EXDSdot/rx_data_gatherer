# main_court.py
from __future__ import annotations

import asyncio
import logging
import os

from rich.console import Console
from rich.live import Live

from config import Settings
from edgar_client import RunStats, progress_reporter, setup_logging  # reuse progress infra
from io_xlsx import load_court_cases_xlsx, write_court_metrics_xlsx

from court_client import CourtSettings, CourtListenerAsyncClient
from court_metrics import fetch_court_metrics_for_case


def _parse_days(env_val: str | None) -> tuple[int, ...]:
    if not env_val:
        return (90, 120, 180)
    out: list[int] = []
    for part in env_val.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return tuple(out) if out else (90, 120, 180)


def _parse_keywords(env_val: str | None) -> tuple[str, ...]:
    if not env_val:
        return ("motion",)
    kws = [k.strip() for k in env_val.split(",") if k.strip()]
    return tuple(kws) if kws else ("motion",)


async def main() -> int:
    settings = Settings()
    setup_logging(settings.log_path)
    log = logging.getLogger("main_court")

    rows = load_court_cases_xlsx(settings.input_xlsx, sheet_name=settings.input_sheet)
    if settings.limit_rows > 0:
        rows = rows[: settings.limit_rows]

    log.info("Loaded %d court rows", len(rows))

    days = _parse_days(os.getenv("COURT_DAYS"))
    motion_keywords = _parse_keywords(os.getenv("COURT_MOTION_KEYWORDS"))

    console = Console()
    stats = RunStats(task_name="COURT METRICS", total_units=len(rows))

    court_settings = CourtSettings()  # reads COURTLISTENER_TOKEN, COURT_BASE_URL, etc
    client = CourtListenerAsyncClient(court_settings, stats=stats)

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
            _wrap_unit(fetch_court_metrics_for_case(client, row=row, days=days, motion_keywords=motion_keywords))
            for row in rows
        ]
        results = await asyncio.gather(*tasks)

        out_path = os.getenv("COURT_OUT_XLSX", "court_metrics.xlsx")
        if not os.path.isabs(out_path):
            base_dir = os.path.dirname(__file__)
            out_path = os.path.join(base_dir, out_path)

        write_court_metrics_xlsx(results, path=out_path)
        log.info("Done. Output: %s", out_path)
        return 0

    finally:
        stats.finished = True
        await reporter
        live.stop()
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))