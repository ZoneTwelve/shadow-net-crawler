# ./app/worker_utils.py
import json
import uuid
import time
from typing import List, Optional, Dict
import redis

from app.config import settings

redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)


def make_task_id(url: str) -> str:
    """Stable-ish task id; use uuid to ensure unique tasks when forced."""
    return str(uuid.uuid4())


def register_worker(worker_id: str, labels: List[str], agency: str, ttl: int = 30) -> None:
    key = f"workers:{worker_id}"
    now = int(time.time())
    redis_client.hset(key, mapping={
        "id": worker_id,
        "labels": json.dumps(labels),
        "agency": agency,
        "status": "online",
        "started_at": str(now),
        "last_heartbeat": str(now),
    })
    redis_client.sadd("workers:online", worker_id)
    redis_client.expire(key, ttl)


def heartbeat(worker_id: str, ttl: int = 30) -> None:
    key = f"workers:{worker_id}"
    now = int(time.time())
    if redis_client.exists(key):
        redis_client.hset(key, mapping={"last_heartbeat": str(now)})
        redis_client.expire(key, ttl)
    else:
        # If vanished, re-register minimal info to avoid disappear
        redis_client.hset(key, mapping={"id": worker_id, "status": "online", "last_heartbeat": str(now)})
        redis_client.sadd("workers:online", worker_id)
        redis_client.expire(key, ttl)


def mark_worker_offline(worker_id: str) -> None:
    key = f"workers:{worker_id}"
    redis_client.hset(key, mapping={"status": "offline"})
    redis_client.srem("workers:online", worker_id)
    # leave record for inspection


def worker_should_stop(worker_id: str) -> bool:
    key = f"workers:{worker_id}"
    val = redis_client.hget(key, "status")
    return val == "stopping" or val == "kicked"


def try_acquire_task_lock(task_id: str, worker_id: str, lease: int = 120) -> bool:
    # lock key with SET NX EX
    lock_key = f"lock:task:{task_id}"
    return redis_client.set(lock_key, worker_id, nx=True, ex=lease)


def release_task_lock(task_id: str, worker_id: str) -> None:
    lock_key = f"lock:task:{task_id}"
    owner = redis_client.get(lock_key)
    if owner == worker_id:
        redis_client.delete(lock_key)


def increment_url_failure(url_hash: str, ttl_seconds: int = 60 * 60 * 24) -> int:
    key = f"failures:url:{url_hash}"
    new = redis_client.hincrby(key, "count", 1)
    redis_client.hset(key, "last_failed_at", int(time.time()))
    redis_client.expire(key, ttl_seconds)
    return new


def get_url_failures(url_hash: str) -> int:
    key = f"failures:url:{url_hash}"
    try:
        return int(redis_client.hget(key, "count") or 0)
    except Exception:
        return 0


def increment_worker_failure(worker_id: str, ttl_seconds: int = 60 * 10) -> int:
    key = f"failures:worker:{worker_id}"
    new = redis_client.incr(key)
    redis_client.expire(key, ttl_seconds)
    return new


def reset_worker_failures(worker_id: str) -> None:
    redis_client.delete(f"failures:worker:{worker_id}")


def push_graph_edge(domain: str, src_hash: str, dst_hash: str) -> None:
    redis_client.sadd(f"graph:{domain}", f"{src_hash}||{dst_hash}")


def move_url_to_abandoned(domain: str, url: str) -> None:
    redis_client.sadd(f"abandoned:{domain}", url)
