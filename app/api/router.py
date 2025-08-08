# ./app/api/router.py
from fastapi import APIRouter, HTTPException
from urllib.parse import urlparse
import os
import json

from app.config import settings
from app.models import EnqueueCrawlRequest, QueueInfoResponse
from app.utils import json_payload
import redis

router = APIRouter()
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)


def _domain(url: str) -> str:
    return urlparse(url).netloc


def _ensure_config(domain: str, scope: str | None, depth: int | None) -> None:
    """Store per‑domain crawl defaults (if supplied) in a Redis hash."""
    cfg_key = f"config:{domain}"
    mapping: dict[str, str] = {}
    if scope:
        mapping["scope"] = scope
    if depth is not None:
        mapping["depth"] = str(depth)

    if mapping:
        redis_client.hset(cfg_key, mapping=mapping)

    # Guarantee the hash exists – workers will always read it.
    if not redis_client.exists(cfg_key):
        redis_client.hset(cfg_key, mapping={})


@router.post("/enqueue_crawl")
async def enqueue_crawl(req: EnqueueCrawlRequest):
    """Add a start URL to the crawling queue."""
    url = str(req.start_url)
    domain = _domain(url)

    queue_key = f"queue:{domain}"
    seen_key = f"seen:{domain}"

    _ensure_config(domain, req.scope, req.depth)

    # Only enqueue if this URL has never been seen for this domain
    if redis_client.sadd(seen_key, url):
        payload = json_payload({"url": url, "depth": 0})
        redis_client.rpush(queue_key, payload)
        return {"enqueued": True, "domain": domain}
    else:
        return {"enqueued": False, "reason": "already enqueued or seen"}


@router.get("/queues", response_model=QueueInfoResponse)
async def list_queues():
    """Return a map `{domain: pending_job_count}`."""
    result: dict[str, int] = {}
    for key in redis_client.scan_iter(match="queue:*"):
        domain = key.split(":", 1)[1]
        result[domain] = redis_client.llen(key)
    return {"queues": result}


@router.post("/enqueue_batch")
async def enqueue_batch(requests: list[EnqueueCrawlRequest]):   # type: ignore[valid-type]
    """Helper – enqueue many URLs in one call."""
    outcomes = []
    for req in requests:
        outcomes.append(await enqueue_crawl(req))
    return outcomes


@router.get("/progress")
async def get_progress():
    """Read the JSON file written by the worker."""
    if not os.path.exists(settings.PROGRESS_FILE):
        return {"status": "no crawl in progress"}
    with open(settings.PROGRESS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("/health")
async def health():
    return {"status": "ok"}