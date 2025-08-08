# ./app/config.py
import os
from urllib.parse import urlparse

class Settings:
    """Load environment variables once – used everywhere."""
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    USER_AGENT: str = os.getenv(
        "USER_AGENT", "crawler-llm/1.0 (+https://example.com)"
    )
    MAX_CHARS: int = int(os.getenv("MAX_CHARS", "100000"))

    # Default crawl parameters – can be overridden per‑domain via Redis
    DEFAULT_SCOPE: str = os.getenv("CRAWL_DEFAULT_SCOPE", "in-domain")
    DEFAULT_DEPTH: int = int(os.getenv("CRAWL_DEFAULT_DEPTH", "3"))   # -1 = unlimited

    BLACKLIST = {
        b.strip()
        for b in os.getenv("CRAWL_BLACKLIST", "").split(",")
        if b.strip()
    }
    WHITELIST = {
        w.strip()
        for w in os.getenv("CRAWL_WHITELIST", "").split(",")
        if w.strip()
    }

    DATA_DIR: str = os.getenv("DATA_DIR", "/data")
    PROGRESS_FILE: str = os.getenv(
        "PROGRESS_FILE", "/data/crawl_progress.json"
    )

    @staticmethod
    def domain_of(url: str) -> str:
        """Convenient helper – returns netloc part of a URL."""
        return urlparse(url).netloc

settings = Settings()