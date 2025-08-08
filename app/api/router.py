# ./app/api/router.py
from fastapi import APIRouter, HTTPException, Query
from urllib.parse import urlparse
import os
import json
import time
from typing import Optional

import redis

from app.config import settings
from app.models import EnqueueCrawlRequest, QueueInfoResponse
from app.utils import json_payload

router = APIRouter()
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)


def _domain(url: str) -> str:
    return urlparse(url).netloc


def _ensure_config(domain: str, scope: str | None, depth: int | None) -> None:
    cfg_key = f"config:{domain}"
    mapping: dict[str, str] = {}
    if scope:
        mapping["scope"] = scope
    if depth is not None:
        mapping["depth"] = str(depth)
    if mapping:
        redis_client.hset(cfg_key, mapping=mapping)
    if not redis_client.exists(cfg_key):
        redis_client.hset(cfg_key, mapping={})


@router.post("/enqueue_crawl")
async def enqueue_crawl(req: EnqueueCrawlRequest):
    url = str(req.start_url)
    domain = _domain(url)
    queue_key = f"queue:{domain}"
    seen_key = f"seen:{domain}"
    _ensure_config(domain, req.scope, req.depth)
    # Default payload: label/agency can be added by clients
    payload = json_payload({
        "id": None,
        "url": url,
        "depth": 0,
        "label": None,
        "agency": None,
        "scope": req.scope or settings.DEFAULT_SCOPE,
        "depth_limit": req.depth if req.depth is not None else settings.DEFAULT_DEPTH,
        "enqueued_at": int(time.time()),
        "attempts": 0,
        "forced": False,
    })
    if redis_client.sadd(seen_key, url):
        redis_client.rpush(queue_key, payload)
        return {"enqueued": True, "domain": domain}
    else:
        return {"enqueued": False, "reason": "already enqueued or seen"}


@router.post("/enqueue_batch")
async def enqueue_batch(requests: list[EnqueueCrawlRequest]):   # type: ignore[valid-type]
    outcomes = []
    for req in requests:
        outcomes.append(await enqueue_crawl(req))
    return outcomes


@router.get("/queues", response_model=QueueInfoResponse)
async def list_queues():
    result: dict[str, int] = {}
    for key in redis_client.scan_iter(match="queue:*"):
        domain = key.split(":", 1)[1]
        result[domain] = redis_client.llen(key)
    return {"queues": result}


@router.get("/progress")
async def get_progress():
    if not os.path.exists(settings.PROGRESS_FILE):
        return {"status": "no crawl in progress"}
    with open(settings.PROGRESS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("/health")
async def health():
    return {"status": "ok"}


# -------------------------- Cluster / Control endpoints --------------------------

@router.get("/workers")
async def list_workers():
    """List active workers and their metadata."""
    workers = {}
    for key in redis_client.scan_iter(match="workers:*"):
        if redis_client.type(key) != "hash":
            continue  # Skip keys that aren't hashes
        worker_id = key.split(":", 1)[1]
        workers[worker_id] = redis_client.hgetall(key)
    return {"workers": workers}


@router.post("/stop_worker/{worker_id}")
async def stop_worker(worker_id: str):
    """Mark a worker as stopping (graceful)."""
    key = f"workers:{worker_id}"
    if not redis_client.exists(key):
        raise HTTPException(status_code=404, detail="Worker not found")
    redis_client.hset(key, mapping={"status": "stopping"})
    return {"status": "marked for stop", "worker_id": worker_id}


@router.post("/kick_worker/{worker_id}")
async def kick_worker(worker_id: str):
    """Force mark offline and remove from workers:online."""
    key = f"workers:{worker_id}"
    if not redis_client.exists(key):
        raise HTTPException(status_code=404, detail="Worker not found")
    redis_client.hset(key, mapping={"status": "kicked"})
    redis_client.srem("workers:online", worker_id)
    return {"status": "kicked", "worker_id": worker_id}


@router.get("/abandoned/{domain}")
async def get_abandoned(domain: str):
    key = f"abandoned:{domain}"
    urls = list(redis_client.smembers(key))
    return {"domain": domain, "count": len(urls), "urls": urls}


@router.post("/recycle_abandoned/{domain}")
async def recycle_abandoned(domain: str, filter_domain: Optional[str] = Query(None, description="Only recycle URLs whose netloc matches this value")):
    """Push abandoned URLs for a domain back into its queue. Optionally filter by domain substring or exact match."""
    abandoned_key = f"abandoned:{domain}"
    queue_key = f"queue:{domain}"
    urls = list(redis_client.smembers(abandoned_key))
    recycled = 0
    for u in urls:
        if filter_domain and filter_domain not in urlparse(u).netloc:
            continue
        # remove from abandoned and requeue as forced
        redis_client.srem(abandoned_key, u)
        payload = json_payload({
            "id": None,
            "url": u,
            "depth": 0,
            "label": None,
            "agency": None,
            "scope": None,
            "depth_limit": None,
            "enqueued_at": int(time.time()),
            "attempts": 0,
            "forced": True,
        })
        redis_client.rpush(queue_key, payload)
        # ensure it's removed from seen so it gets processed as "new"
        redis_client.srem(f"seen:{domain}", u)
        recycled += 1
    return {"recycled": recycled}


@router.post("/force_recrawl")
async def force_recrawl(url: str, all_tasks: bool = False):
    """
    Force recrawl a URL or the entire domain:
      - all_tasks=True: clears the seen set for the domain (dangerous)
      - otherwise: removes this URL from seen and pushes a forced task
    """
    domain = _domain(url)
    queue_key = f"queue:{domain}"
    seen_key = f"seen:{domain}"
    if all_tasks:
        redis_client.delete(seen_key)
        return {"status": "cleared all seen for domain", "domain": domain}
    else:
        redis_client.srem(seen_key, url)
        payload = json_payload({
            "id": None,
            "url": url,
            "depth": 0,
            "label": None,
            "agency": None,
            "scope": None,
            "depth_limit": None,
            "enqueued_at": int(time.time()),
            "attempts": 0,
            "forced": True,
        })
        redis_client.rpush(queue_key, payload)
        return {"status": "url requeued", "url": url}


@router.get("/graph/{domain}")
async def get_graph(domain: str):
    """Export site graph edges stored as 'src_hash||dst_hash' in a set."""
    graph_key = f"graph:{domain}"
    edges = list(redis_client.smembers(graph_key))
    return {"domain": domain, "edges": edges}

