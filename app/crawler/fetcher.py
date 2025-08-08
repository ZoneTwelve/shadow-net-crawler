# ./app/crawler/fetcher.py
import requests
from ..config import settings


def fetch(url: str) -> str | None:
    """GET with a custom User‑Agent and a 30‑second timeout."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": settings.USER_AGENT},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.text
    except Exception as exc:   # pragma: no cover
        print(f"[ERROR] fetch {url}: {exc}")
        return None