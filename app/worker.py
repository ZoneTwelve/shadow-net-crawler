# ./app/worker.py
import json
import time
from urllib.parse import urlparse

import redis

from app.config import settings
from app.utils import write_progress, json_payload
from app.crawler.processor import process_one_url

# --------------------------------------------------------------------------- #
# Redis connection & helper functions
# --------------------------------------------------------------------------- #
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)


def _config_for(domain: str) -> tuple[str, int]:
    """Read per‑domain crawl config from Redis, fall back to defaults."""
    cfg_key = f"config:{domain}"
    cfg = redis_client.hgetall(cfg_key)
    scope = cfg.get("scope", settings.DEFAULT_SCOPE)
    depth = int(cfg.get("depth", settings.DEFAULT_DEPTH))
    return scope, depth


def _queue_keys() -> list[str]:
    """Return all `queue:*` keys – used for a single BRPOP call."""
    return list(redis_client.scan_iter(match="queue:*", count=20))


def worker_loop() -> None:
    """
    Blocking loop:
      1. BRPOP from any domain queue (5 s timeout).
      2. Process the URL.
      3. Enqueue newly discovered URLs (deduped via a Redis set).
    """
    print("[WORKER] started – waiting for jobs…")
    while True:
        keys = _queue_keys()
        if not keys:
            time.sleep(2)
            continue

        # BRPOP returns (queue_key, payload) or None after timeout
        result = redis_client.brpop(keys, timeout=5)   # type: ignore[arg-type]
        if not result:
            # No job in any queue – loop again
            continue

        queue_key, payload = result
        try:
            job = json.loads(payload)
            url = job["url"]
            depth = int(job["depth"])
        except Exception as exc:   # pragma: no cover
            print(f"[ERROR] malformed payload {payload!r}: {exc}")
            continue

        domain = urlparse(url).netloc
        scope, depth_limit = _config_for(domain)

        # ------------------------------------------------------------------- #
        # Book‑keeping
        # ------------------------------------------------------------------- #
        seen_key = f"seen:{domain}"
        total_seen = redis_client.scard(seen_key)

        write_progress(total_seen, redis_client.llen(queue_key), url)
        print(f"[WORKER] ({domain}) depth={depth} {url}")

        # ------------------------------------------------------------------- #
        # Process the page
        # ------------------------------------------------------------------- #
        try:
            new_jobs = process_one_url(url, depth, domain, scope, depth_limit)
        except Exception as exc:   # pragma: no cover
            print(f"[ERROR] processing {url}: {exc}")
            new_jobs = []

        # ------------------------------------------------------------------- #
        # Enqueue discovered URLs – deduped via the per‑domain `seen:` set
        # ------------------------------------------------------------------- #
        for nxt, nxt_depth in new_jobs:
            if redis_client.sadd(seen_key, nxt):
                redis_client.rpush(
                    queue_key,
                    json_payload({"url": nxt, "depth": nxt_depth}),
                )

        # Polite pause – can be tuned per domain later if needed
        time.sleep(1)


if __name__ == "__main__":
    worker_loop()