from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# If you have a global RunStats or similar, import it; otherwise, we use a dummy.
try:
    from edgar_client import RunStats
except ImportError:
    RunStats = Any

log = logging.getLogger("court_client")

@dataclass(frozen=True)
class CourtSettings:
    base_url: str = "https://www.courtlistener.com/api/rest/v3"
    #token: str | None = None  # Loaded from env in main
    token  = "0812f348b4f652edc028b3c9ce8927d3827c1353"
    user_agent: str = "RxDataGatherer/1.0 (internal-research-tool)" # <--- NEW DEFAULT
    max_rps: float = 2.0
    max_concurrency: int = 5
    timeout_seconds: float = 30.0

class CourtListenerAsyncClient:
    def __init__(self, settings: CourtSettings, stats: Optional[RunStats] = None) -> None:
        self.settings = settings
        self.stats = stats
        
        # 1. Define Headers with User-Agent
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": settings.user_agent,  # <--- CRITICAL FIX
        }
        
        # 2. Add Token if present
        #
        headers["Authorization"] = "Authorization: Token 0812f348b4f652edc028b3c9ce8927d3827c1353"
        #else:
         #   log.warning("No CourtListener TOKEN found. You will likely be rate-limited or blocked (401/403).")

        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=settings.timeout_seconds,
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=settings.max_concurrency, 
                max_keepalive_connections=settings.max_concurrency
            ),
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "CourtListenerAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    @retry(
        retry=retry_if_exception_type((httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def _get_json(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.settings.base_url}/{endpoint}/"
        
        try:
            resp = await self._client.get(url, params=params)
            
            # 429 = Too Many Requests (Rate Limit)
            if resp.status_code == 429:
                log.warning("Rate limit hit (429).")
                resp.raise_for_status()

            # 403/401 handling
            if resp.status_code in (401, 403):
                log.error(f"Auth failed ({resp.status_code}) for {url}. Check TOKEN and User-Agent.")
            
            resp.raise_for_status()
            
            # If empty content
            if not resp.content:
                return {}
                
            return resp.json()

        except httpx.HTTPStatusError as e:
            # Pass through 404s cleanly (not found is a valid result state)
            if e.response.status_code == 404:
                return {}
            # Log detail for other errors
            log.error(f"HTTP Error {e.response.status_code} | {e.request.url} | {e.response.text[:200]}")
            raise e
        except Exception as e:
            log.error(f"Request failed: {url} | {e}")
            raise e

    # --- Endpoints ---

    async def list_dockets(
        self, 
        court: str, 
        docket_number: str, 
        page_size: int = 20
    ) -> dict[str, Any]:
        """
        Search for a docket by court and docket number.
        """
        params = {
            "court": court,
            "docket_number": docket_number,
            "page_size": page_size,
        }
        return await self._get_json("dockets", params=params)

    async def list_docket_entries(
        self, 
        docket_id: int, 
        page: int = 1,
        page_size: int = 100
    ) -> dict[str, Any]:
        """
        Get entries for a specific docket ID (obtained from list_dockets).
        """
        params = {
            "docket": docket_id,
            "page": page,
            "page_size": page_size,
        }
        return await self._get_json("docket-entries", params=params)