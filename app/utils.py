# ./app/utils.py
import hashlib
import json
import os
from datetime import datetime
from typing import Any, Dict

from .config import settings


def hash_url(url: str) -> str:
    """Stable SHA‑256 hash for filenames."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def save_file(path: str, content: str, mode: str = "w", binary: bool = False) -> None:
    """Create parent directories and write the file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(
        path, mode + ("b" if binary else ""), encoding=None if binary else "utf-8"
    ) as f:
        f.write(content)


def write_progress(total_seen: int, queue_len: int, current_url: str | None) -> None:
    """One‑line JSON file that /progress reads."""
    payload = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "total_seen": total_seen,
        "queue_length": queue_len,
        "current_url": current_url,
    }
    with open(settings.PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    print(
        f"[PROGRESS] seen={total_seen} queue={queue_len} current={current_url}"
    )


def json_payload(data: Dict[str, Any]) -> str:
    """Serialize a dict → JSON string (ensures a plain string for Redis)."""
    return json.dumps(data, ensure_ascii=False)