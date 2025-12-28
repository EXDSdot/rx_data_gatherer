from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

from rich.console import Console
from rich.live import Live

from config import Settings
from edgar_client import EdgarAsyncClient, RunStats, progress_reporter, setup_logging
from io_xlsx import load_cik_event_dates_xlsx

async def fetch_and_save(client: EdgarAsyncClient, cik: str, output_dir: Path) -> str:
    """
    Fetches companyfacts JSON and saves it to disk.
    Returns status string for logging.
    """
    cik10 = client.normalize_cik(cik)
    file_path = output_dir / f"CIK{cik10}.json"

    # Optional: Skip if already exists to allow resuming
    if file_path.exists():
        return "exists"

    try:
        # fetch data (dict)
        data = await client.get_company_facts(cik10)
        
        # save raw json
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        
        return "ok"

    except Exception as e:
        # Check if it was a 404 (Not Found)
        if "404" in str(e):
            return "404"
        return f"err: {e}"

async def main() -> int:
    settings = Settings()
    setup_logging(settings.log_path)
    log = logging.getLogger("download_raw")

    # 1. Setup Output Directory
    out_dir = Path("json_data")
    out_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Saving JSONs to: {out_dir.resolve()}")

    # 2. Load Inputs
    rows = load_cik_event_dates_xlsx(settings.input_xlsx, sheet_name=settings.input_sheet)
    if settings.limit_rows > 0:
        rows = rows[:settings.limit_rows]

    # Deduplicate CIKs (we only need to download each company once)
    unique_ciks = sorted(list(set(row[0] for row in rows)))
    log.info(f"Loaded {len(rows)} rows. Unique CIKs to fetch: {len(unique_ciks)}")

    # 3. Init Client
    console = Console()
    stats = RunStats(task_name="DOWNLOAD JSON", total_units=len(unique_ciks))
    client = EdgarAsyncClient(settings, stats=stats)

    # 4. Run Loop
    live = Live("", console=console, refresh_per_second=10, transient=True)
    live.start()
    reporter = asyncio.create_task(progress_reporter(live, stats))

    try:
        async def _worker(cik):
            try:
                res = await fetch_and_save(client, cik, out_dir)
                if res == "ok" or res == "exists":
                    # We count 'exists' as success for stats usually, 
                    # but pure http stats in EdgarClient handle the network part.
                    pass
            finally:
                await stats.record_unit_done()

        # Create tasks
        tasks = [_worker(cik) for cik in unique_ciks]
        await asyncio.gather(*tasks)

        log.info("Download complete.")
        return 0

    finally:
        stats.finished = True
        await reporter
        live.stop()
        await client.aclose()

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))