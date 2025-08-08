# ./app/crawler/processor.py
from typing import List, Tuple
from urllib.parse import urlparse

from ..config import settings
from ..utils import hash_url, save_file, json_payload
from .fetcher import fetch
from .parser import strip_noise, extract_links
from ..llm_client import summarize_html_to_markdown


def url_allowed(url: str, start_domain: str, scope: str) -> bool:
    """Apply blacklist / whitelist / scope rules."""
    if settings.BLACKLIST and any(b in url for b in settings.BLACKLIST):
        return False
    if settings.WHITELIST and not any(w in url for w in settings.WHITELIST):
        return False

    parsed = urlparse(url)
    netloc = parsed.netloc

    if scope == "no-limit":
        return True
    if scope == "all-sub-domain":
        return netloc.endswith(start_domain)
    if scope == "in-domain":
        return netloc == start_domain
    return False


def process_one_url(
    url: str,
    depth: int,
    start_domain: str,
    scope: str,
    depth_limit: int,
) -> List[Tuple[str, int]]:
    """
    Fetch → save raw HTML → markdown → discover links.
    Returns a list of (new_url, new_depth) that should be en‑queued.
    """
    raw_html = fetch(url)
    if not raw_html:
        return []   # fetch failed

    # ---- Clean HTML ----
    cleaned = strip_noise(raw_html)

    # ---- Persist raw HTML ----
    h = hash_url(url)
    save_file(f"{settings.DATA_DIR}/{h}.html", raw_html)

    # ---- Convert to markdown (LLM) ----
    try:
        markdown = summarize_html_to_markdown(cleaned, url)
    except Exception as exc:   # pragma: no cover
        print(f"[LLM ERROR] {url}: {exc}")
        markdown = f"<!-- LLM FAILED: {exc} -->\n\n# {url}"
    save_file(f"{settings.DATA_DIR}/{h}.md", markdown)

    # ---- Depth limiting ----
    if depth_limit != -1 and depth >= depth_limit:
        return []   # stop recursion here

    # ---- Link discovery ----
    discovered: List[Tuple[str, int]] = []
    for link in extract_links(raw_html, url):
        if url_allowed(link, start_domain, scope):
            discovered.append((link, depth + 1))
    return discovered