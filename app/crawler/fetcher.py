# ./app/crawler/fetcher.py
import requests
import asyncio
from typing import Tuple, Optional
from ..config import settings

# Minimal synchronous requests agent
def fetch_requests(url: str, timeout: int = 30) -> Tuple[Optional[int], Optional[str]]:
    """
    Return (status_code, text) or (None, None) on network error.
    """
    try:
        resp = requests.get(url, headers={"User-Agent": settings.USER_AGENT}, timeout=timeout, allow_redirects=True)
        # resp.raise_for_status()  # we want to return status and body for policy decisions
        return resp.status_code, resp.text
    except Exception as exc:
        print(f"[FETCH-REQUESTS] error fetching {url}: {exc}")
        return None, None


# Placeholder: Selenium agent (to be implemented per infra)
def fetch_selenium(url: str, timeout: int = 60) -> Tuple[Optional[int], Optional[str]]:
    """
    NOTE: requires Selenium infra — left as a placeholder.
    Return (status_code, text) or (None, None).
    """
    # Example: integrate with remote Chrome via webdriver.Remote or playwright.
    raise NotImplementedError("Selenium agent not implemented. Provide an implementation for fetch_selenium.")


# Placeholder: asyncio agent
async def fetch_asyncio(url: str, timeout: int = 30) -> Tuple[Optional[int], Optional[str]]:
    """
    Async HTTP client (aiohttp) would go here. We provide a placeholder.
    """
    raise NotImplementedError("Asyncio agent not implemented. Provide an aiohttp-based implementation.")
