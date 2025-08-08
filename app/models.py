# ./app/models.py
from pydantic import BaseModel, AnyHttpUrl
from typing import Optional, List


class EnqueueCrawlRequest(BaseModel):
    """Payload for POST /enqueue_crawl."""
    start_url: AnyHttpUrl
    scope: Optional[str] = None          # in-domain | all-sub-domain | no-limit
    depth: Optional[int] = None          # -1 = unlimited


class EnqueueBatchRequest(BaseModel):
    """For POST /enqueue_batch – a list of EnqueueCrawlRequest."""
    items: List[EnqueueCrawlRequest]


class QueueInfoResponse(BaseModel):
    """Returned by GET /queues."""
    queues: dict[str, int]