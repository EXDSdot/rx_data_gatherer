from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any
import os
from pathlib import Path
import httpx
from aiolimiter import AsyncLimiter
from rich.live import Live
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from config import Settings

# -----------------------------
# Logging
# -----------------------------
def setup_logging(log_path: str, console_level: int = logging.ERROR) -> None:
    """
    - Console: quiet (ERROR only) so Rich progress line stays clean.
    - File: full DEBUG trace.
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    ch.setFormatter(fmt)

    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    root.handlers.clear()
    root.addHandler(ch)
    root.addHandler(fh)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

# -----------------------------
# Exceptions
# -----------------------------
class EdgarError(RuntimeError):
    pass

# -----------------------------
# Progress Stats
# -----------------------------
@dataclass
class RunStats:
    task_name: str
    total_units: int

    started_at: float = field(default_factory=time.perf_counter)
    finished: bool = False

    done_units: int = 0

    http_200: int = 0
    http_404: int = 0
    http_other: int = 0

    lat_sum: float = 0.0
    lat_n: int = 0

    gap_sum: float = 0.0
    gap_n: int = 0
    last_req_started_at: float | None = None

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    async def record_request(self, status_code: int, latency_s: float, req_started_at: float) -> None:
        async with self._lock:
            if self.last_req_started_at is not None:
                self.gap_sum += max(0.0, req_started_at - self.last_req_started_at)
                self.gap_n += 1
            self.last_req_started_at = req_started_at

            if status_code == 200:
                self.http_200 += 1
            elif status_code == 404:
                self.http_404 += 1
            else:
                self.http_other += 1

            self.lat_sum += latency_s
            self.lat_n += 1

    async def record_unit_done(self) -> None:
        async with self._lock:
            self.done_units += 1

    async def snapshot(self) -> dict[str, float | int | str]:
        async with self._lock:
            elapsed = max(1e-9, time.perf_counter() - self.started_at)
            total_http = self.http_200 + self.http_404 + self.http_other
            rps = total_http / elapsed
            avg_lat = (self.lat_sum / self.lat_n) if self.lat_n else 0.0
            avg_gap = (self.gap_sum / self.gap_n) if self.gap_n else 0.0
            pct = (100.0 * self.done_units / self.total_units) if self.total_units else 100.0
            ok_rate = (100.0 * self.http_200 / total_http) if total_http else 0.0

            return {
                "task": self.task_name,
                "pct": pct,
                "done": self.done_units,
                "total": self.total_units,
                "http200": self.http_200,
                "http404": self.http_404,
                "httperr": self.http_other,
                "ok_rate": ok_rate,
                "rps": rps,
                "avg_lat": avg_lat,
                "avg_gap": avg_gap,
                "elapsed": elapsed,
            }

async def progress_reporter(live: Live, stats: RunStats, refresh_s: float = 0.2) -> None:
    while True:
        snap = await stats.snapshot()

        ok_rate = float(snap["ok_rate"])
        ok_color = "green" if ok_rate >= 90 else ("yellow" if ok_rate >= 70 else "red")

        line = (
            f"[bold]{snap['task']}[/] | "
            f"{snap['pct']:.1f}% ({snap['done']}/{snap['total']}) | "
            f"[green]200[/]={snap['http200']} "
            f"[yellow]404[/]={snap['http404']} "
            f"[red]err[/]={snap['httperr']} | "
            f"ok=[{ok_color}]{ok_rate:.1f}%[/{ok_color}] | "
            f"rps={snap['rps']:.2f} | "
            f"avg_lat={snap['avg_lat']:.2f}s | "
            f"avg_gap={snap['avg_gap']:.2f}s | "
            f"t={snap['elapsed']:.0f}s"
        )

        live.update(line)

        if stats.finished and int(snap["done"]) >= int(snap["total"]):
            break

        await asyncio.sleep(refresh_s)

# -----------------------------
# EDGAR Async Client
# -----------------------------
class EdgarAsyncClient:
    BASE = "https://data.sec.gov"

    def __init__(self, settings: Settings, stats: RunStats | None = None) -> None:
        self.log = logging.getLogger(self.__class__.__name__)
        self.settings = settings
        self.stats = stats

        # --- TEMP: dump first successful companyfacts request+json to disk (for documentation) ---
        self._dump_first_companyfacts: bool = True  # set False (or comment) when done
        self._dumped_companyfacts: bool = False
        self._dump_dir: Path = Path(os.getenv("SEC_DUMP_DIR", ".")).resolve()

        self.limiter = AsyncLimiter(max_rate=settings.max_rps, time_period=1)
        self._sem = asyncio.Semaphore(settings.max_concurrency)

        headers = {
            "User-Agent": settings.user_agent,
            "Accept-Encoding": "gzip, deflate",
            # DO NOT set Host; httpx will set correct Host per-domain (data.sec.gov vs www.sec.gov)
        }

        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(settings.timeout_seconds),
            follow_redirects=True,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    @staticmethod
    def normalize_cik(cik: str | int) -> str:
        s = str(cik).strip()
        s = s.lstrip("0") or "0"
        if not s.isdigit():
            raise ValueError(f"CIK must be numeric; got {cik!r}")
        return s.zfill(10)

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential_jitter(initial=0.5, max=8.0),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
    async def _get_json(self, url: str) -> dict[str, Any]:
        async with self._sem:
            async with self.limiter:
                req_started = time.perf_counter()
                self.log.debug("GET %s", url)
                resp = await self._client.get(url)
                latency = time.perf_counter() - req_started

                if self.stats is not None:
                    await self.stats.record_request(resp.status_code, latency, req_started)

        resp.raise_for_status()

        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            raise EdgarError(f"Failed to decode JSON from {url}") from e

        # --- TEMP: dump first successful companyfacts response ---
        try:
            is_companyfacts = "/api/xbrl/companyfacts/CIK" in url
            if (
                self._dump_first_companyfacts
                and is_companyfacts
                and (not self._dumped_companyfacts)
                and resp.status_code == 200
                and isinstance(data, dict)
            ):
                self._dump_dir.mkdir(parents=True, exist_ok=True)

                # extract cik from URL for filename
                # URL looks like: .../companyfacts/CIK0000123456.json
                cik_part = url.rsplit("CIK", 1)[-1]
                cik10 = cik_part.replace(".json", "").strip()

                json_path = self._dump_dir / f"sec_companyfacts_first_CIK{cik10}.json"
                req_path = self._dump_dir / f"sec_companyfacts_first_CIK{cik10}.curl.txt"

                # write json
                with json_path.open("w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                # write reproducible curl (what you can paste into a doc)
                ua = self.settings.user_agent
                curl_cmd = (
                    "curl -sS --compressed \\\n"
                    f"  -H 'User-Agent: {ua}' \\\n"
                    "  -H 'Accept: application/json' \\\n"
                    f"  '{url}'\n"
                )
                with req_path.open("w", encoding="utf-8") as f:
                    f.write(curl_cmd)

                # mark done (so it only happens once)
                self._dumped_companyfacts = True

                # log to file (won't spam console since console level is ERROR)
                self.log.warning("DUMPED first companyfacts request to: %s", str(req_path))
                self.log.warning("DUMPED first companyfacts json to: %s", str(json_path))
        except Exception as dump_e:
            # never break the pipeline because of debug dumping
            self.log.debug("dump_first_companyfacts failed: %s", dump_e)

        return data

    async def get_company_facts(self, cik: str | int) -> dict[str, Any]:
        cik10 = self.normalize_cik(cik)
        url = f"{self.BASE}/api/xbrl/companyfacts/CIK{cik10}.json"
        return await self._get_json(url)

    async def get_submissions(self, cik: str | int) -> dict[str, Any]:
        cik10 = self.normalize_cik(cik)
        url = f"{self.BASE}/submissions/CIK{cik10}.json"
        return await self._get_json(url)