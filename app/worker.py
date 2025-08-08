# ./app/worker.py
import os
import json
import time
import uuid
import signal
from typing import Optional, List, Tuple
from urllib.parse import urlparse

import redis

from app.config import settings
from app.utils import write_progress, json_payload, hash_url, save_file
from app.crawler.parser import strip_noise, extract_links
from app.llm_client import summarize_html_to_markdown
from app.crawler.fetcher import fetch_requests, fetch_selenium, fetch_asyncio  # placeholders
from app.worker_utils import (
    register_worker,
    heartbeat,
    mark_worker_offline,
    worker_should_stop,
    try_acquire_task_lock,
    release_task_lock,
    increment_url_failure,
    get_url_failures,
    increment_worker_failure,
    push_graph_edge,
    move_url_to_abandoned,
    make_task_id,
)
from app.error_policy import decide_action

# Redis client
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

# Worker configuration via ENV
WORKER_ID = os.environ.get("WORKER_ID", str(uuid.uuid4()))
WORKER_LABELS = set([l for l in os.environ.get("WORKER_LABELS", "requests").split(",") if l])
WORKER_AGENCY = os.environ.get("WORKER_AGENCY", "requests")  # "requests" | "selenium" | "asyncio"
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "10"))
TASK_LEASE_SECONDS = int(os.environ.get("TASK_LEASE_SECONDS", "180"))
WORKER_FAIL_LIMIT = int(os.environ.get("WORKER_FAIL_LIMIT", "5"))
URL_FAIL_LIMIT = int(os.environ.get("URL_FAIL_LIMIT", "5"))
FAIL_WINDOW_SECONDS = int(os.environ.get("FAIL_WINDOW_SECONDS", 300))


# Graceful shutdown
_shutdown = False


def handle_sigterm(*_):
    global _shutdown
    _shutdown = True


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


def pick_fetcher(agency: str):
    if agency == "requests":
        return fetch_requests
    if agency == "selenium":
        return fetch_selenium
    if agency == "asyncio":
        return fetch_asyncio
    # default
    return fetch_requests


def _queue_names() -> List[str]:
    """Return queue keys. We'll try a scan for queue:* – tuned for small-medium deployments."""
    return list(redis_client.scan_iter(match="queue:*", count=200))


def _peek_head(queue_key: str) -> Optional[str]:
    return redis_client.lindex(queue_key, 0)


def _pop_specific_job(queue_key: str, payload: str) -> bool:
    """Remove a single occurrence of payload from the queue atomically using LREM."""
    # returns number removed
    removed = redis_client.lrem(queue_key, 1, payload)
    return removed > 0


def process_job_payload(job: dict) -> List[Tuple[str, int]]:
    """
    Internal processing performed when worker has exclusive lock for the task.
    Returns discovered (url, depth).
    """
    url = job["url"]
    depth = int(job.get("depth", 0))
    domain = urlparse(url).netloc

    # Choose fetcher: prefer job.agency > worker agency
    agency = job.get("agency") or WORKER_AGENCY
    fetcher = pick_fetcher(agency)

    # Fetch
    try:
        if agency == "requests":
            status, raw_html = fetcher(url)
        elif agency == "selenium":
            status, raw_html = fetcher(url)
        elif agency == "asyncio":
            # not awaited in this simple sync loop — placeholder
            status, raw_html = fetcher(url)
        else:
            status, raw_html = fetcher(url)
    except NotImplementedError as exc:
        print(f"[WORKER {WORKER_ID}] Agency not implemented: {agency} for {url}: {exc}")
        status, raw_html = None, None
    except Exception as exc:
        print(f"[WORKER {WORKER_ID}] Fetch exception for {url}: {exc}")
        status, raw_html = None, None

    if status is None:
        # treat as network failure -> increment URL failure
        uh = hash_url(url)
        attempts = increment_url_failure(uh)
        print(f"[WORKER {WORKER_ID}] network fail {url} (attempts={attempts})")
        return []

    # Determine action by status code
    should_retry, abandon_after, note = decide_action(status)
    if not should_retry and status >= 400 and status < 500:
        # client error - mark as failed (non-retriable)
        uh = hash_url(url)
        move_url_to_abandoned(domain, url)
        print(f"[WORKER {WORKER_ID}] non-retriable status {status} for {url} -> moved to abandoned")
        return []

    if should_retry and status >= 400:
        uh = hash_url(url)
        attempts = increment_url_failure(uh)
        if attempts >= (abandon_after or URL_FAIL_LIMIT):
            move_url_to_abandoned(domain, url)
            print(f"[WORKER {WORKER_ID}] {url} reached abandon threshold (status={status}) -> abandoned")
            return []
        # otherwise allow retry by returning [] and letting caller requeue logic handle attempts
        print(f"[WORKER {WORKER_ID}] retriable status {status} for {url} (attempts={attempts})")
        return []

    # If success (200-ish), continue processing
    if raw_html is None:
        return []

    # Persist raw HTML
    try:
        h = hash_url(url)
        save_file(f"{settings.DATA_DIR}/{h}.html", raw_html)
    except Exception as exc:
        print(f"[WORKER {WORKER_ID}] persist html failed {url}: {exc}")

    # Clean and summarize
    try:
        cleaned = strip_noise(raw_html)
        try:
            markdown = summarize_html_to_markdown(cleaned, url)
        except Exception as exc:
            print(f"[WORKER {WORKER_ID}] LLM summarize failed {url}: {exc}")
            markdown = f"<!-- LLM FAILED: {exc} -->\n\n# {url}"
        try:
            save_file(f"{settings.DATA_DIR}/{h}.md", markdown)
        except Exception as exc:
            print(f"[WORKER {WORKER_ID}] saving markdown failed {url}: {exc}")
    except Exception as exc:
        print(f"[WORKER {WORKER_ID}] parsing/summary pipeline failed for {url}: {exc}")

    # Discover links
    discovered = []
    try:
        for link in extract_links(raw_html, url):
            # Apply per-domain scope checks here or leave to master config (worker reads config)
            # We'll keep simple: only same domain links if default in-domain
            # Use the processor-level url_allowed if you prefer reusing that logic.
            discovered.append((link, depth + 1))
            # push graph edge
            push_graph_edge(domain, hash_url(url), hash_url(link))
    except Exception as exc:
        print(f"[WORKER {WORKER_ID}] link extraction failed {url}: {exc}")

    return discovered


def assign_back_to_queue_if_needed(job: dict, queue_key: str) -> None:
    """
    If the job should be retried (not abandoned), increment attempts and push back.
    """
    job["attempts"] = job.get("attempts", 0) + 1
    # Simple backoff: push to tail (could be delayed queue)
    redis_client.rpush(queue_key, json_payload(job))


def worker_loop():
    global _shutdown
    print(f"[WORKER] starting id={WORKER_ID} agency={WORKER_AGENCY} labels={WORKER_LABELS}")
    register_worker(WORKER_ID, list(WORKER_LABELS), WORKER_AGENCY, ttl=HEARTBEAT_INTERVAL * 3)
    last_hb = 0

    while not _shutdown:
        now = time.time()
        if now - last_hb >= HEARTBEAT_INTERVAL:
            heartbeat(WORKER_ID, ttl=HEARTBEAT_INTERVAL * 3)
            last_hb = now

        # Stop requested?
        if worker_should_stop(WORKER_ID):
            print(f"[WORKER {WORKER_ID}] stopping as requested.")
            mark_worker_offline(WORKER_ID)
            break

        # scan candidate queues
        queues = _queue_names()
        if not queues:
            time.sleep(1)
            continue

        job_acquired = False
        for q in queues:
            head = _peek_head(q)
            if not head:
                continue
            try:
                job = json.loads(head)
            except Exception:
                # malformed payload -> drop
                redis_client.lpop(q)
                continue

            # Label/agency matching
            job_label = job.get("label")
            job_agency = job.get("agency")
            # if job declares agency but this worker cannot support → skip
            if job_agency and job_agency != WORKER_AGENCY:
                continue
            # if job declares label and worker doesn't have it -> skip
            if job_label and job_label not in WORKER_LABELS:
                continue

            # Ensure a task id
            if not job.get("id"):
                job["id"] = make_task_id(job["url"])
                # write back: we will replace the head job representation in queue if we want, but we keep job local

            task_id = job["id"]
            lock_ok = try_acquire_task_lock(task_id, WORKER_ID, lease=TASK_LEASE_SECONDS)
            if not lock_ok:
                # someone else is processing - skip
                continue

            # Remove the job instance from queue (LREM by value)
            removed = _pop_specific_job(q, head)
            if not removed:
                # Could not remove (race). Release lock and continue
                release_task_lock(task_id, WORKER_ID)
                continue

            # At this point this worker owns the task
            job_acquired = True
            print(f"[WORKER {WORKER_ID}] processing {job['url']} from {q}")
            try:
                discovered = process_job_payload(job)
            except Exception as exc:
                print(f"[WORKER {WORKER_ID}] unexpected processing error: {exc}")
                discovered = []

            # Release lock
            release_task_lock(task_id, WORKER_ID)

            # If discovered links -> enqueue deduped
            domain = urlparse(job["url"]).netloc
            seen_key = f"seen:{domain}"
            queue_key = q  # same domain queue

            for nxt, nxt_depth in discovered:
                # dedupe
                try:
                    if redis_client.sadd(seen_key, nxt):
                        # build payload for nxt
                        payload = {
                            "id": None,
                            "url": nxt,
                            "depth": nxt_depth,
                            "label": None,
                            "agency": None,
                            "scope": None,
                            "depth_limit": None,
                            "enqueued_at": int(time.time()),
                            "attempts": 0,
                            "forced": False,
                        }
                        redis_client.rpush(queue_key, json_payload(payload))
                except Exception as exc:
                    print(f"[WORKER {WORKER_ID}] enqueue discovered failed for {nxt}: {exc}")

            # If job produced nothing and was a transient fetch failure -> requeue or abandon was handled in processing
            # If job attempts below threshold: we may requeue
            # Simple retry policy: if job was not abandoned and attempts < URL_FAIL_LIMIT -> re-enqueue with attempts increased
            uh = hash_url(job["url"])
            url_failures = get_url_failures(uh)
            if url_failures > 0 and url_failures < URL_FAIL_LIMIT:
                # requeue the job for retry
                assign_back_to_queue_if_needed(job, queue_key)
            # else if url_failures >= limit, processing moved it to abandoned already

            # progress write for UI
            total_seen = redis_client.scard(seen_key)
            write_progress(total_seen, redis_client.llen(queue_key), job["url"])

            # break to heartbeat loop to avoid tight spinning and continuous scanning
            break

        if not job_acquired:
            time.sleep(1)

    # shutdown cleanup
    print(f"[WORKER {WORKER_ID}] exiting.")
    mark_worker_offline(WORKER_ID)


if __name__ == "__main__":
    worker_loop()

