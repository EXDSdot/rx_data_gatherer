# court_client.py
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

import httpx
from aiolimiter import AsyncLimiter
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter


class CourtApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class CourtSettings:
    # CourtListener REST API v3 base (default used by their client lib).   [oai_citation:1‡Invalid URL](data:text/plain;charset=utf-8,Invalid%20citation)
    base_url: str = os.getenv("COURT_BASE_URL", "https://www.courtlistener.com/api/rest/v3")
    token: str = os.getenv("COURTLISTENER_TOKEN", "")  # optional

    max_rps: float = float(os.getenv("COURT_MAX_RPS", os.getenv("MAX_RPS", "3")))
    max_concurrency: int = int(os.getenv("COURT_MAX_CONCURRENCY", os.getenv("MAX_CONCURRENCY", "20")))
    timeout_seconds: float = float(os.getenv("COURT_HTTP_TIMEOUT", os.getenv("HTTP_TIMEOUT", "30")))


class CourtListenerAsyncClient:
    """
    Minimal async client for CourtListener REST API v3.

    Useful endpoints:
      - /dockets/
      - /docket-entries/
      - /courts/ (optional helper)

    Auth header format: Authorization: Token <token> (if provided).  [oai_citation:2‡Invalid URL](data:text/plain;charset=utf-8,Invalid%20citation)
    """

    def __init__(self, settings: CourtSettings, stats: Any | None = None) -> None:
        self.log = logging.getLogger(self.__class__.__name__)
        self.settings = settings
        self.stats = stats

        self.limiter = AsyncLimiter(max_rate=settings.max_rps, time_period=1)
        self._sem = asyncio.Semaphore(settings.max_concurrency)

        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        }
        if settings.token:
            headers["Authorization"] = f"Token {settings.token}"  #  [oai_citation:3‡Invalid URL](data:text/plain;charset=utf-8,Invalid%20citation)

        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(settings.timeout_seconds),
            follow_redirects=True,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential_jitter(initial=0.5, max=8.0),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
    async def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.settings.base_url.rstrip('/')}/{path.lstrip('/')}"
        params = params or {}

        async with self._sem:
            async with self.limiter:
                req_started = time.perf_counter()
                self.log.debug("GET %s params=%s", url, params)
                resp = await self._client.get(url, params=params)
                latency = time.perf_counter() - req_started

                # optional shared stats hook (your RunStats)
                if self.stats is not None:
                    try:
                        await self.stats.record_request(resp.status_code, latency, req_started)
                    except Exception:
                        pass

        resp.raise_for_status()

        try:
            return resp.json()
        except json.JSONDecodeError as e:
            raise CourtApiError(f"Failed to decode JSON from {url}") from e

    async def list_dockets(self, *, court: str, docket_number: str, page_size: int = 1) -> dict[str, Any]:
        # Django REST style: should filter on field names if exposed
        return await self._get_json(
            "dockets/",
            params={"court": court, "docket_number": docket_number, "page_size": page_size},
        )

    async def list_docket_entries(
        self,
        *,
        docket_id: int,
        page: int = 1,
        page_size: int = 100,
        extra_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params = {"docket": docket_id, "page": page, "page_size": page_size}
        if extra_params:
            params.update(extra_params)
        return await self._get_json("docket-entries/", params=params)

    async def list_courts(self, *, search: str, page_size: int = 5) -> dict[str, Any]:
        return await self._get_json("courts/", params={"search": search, "page_size": page_size})