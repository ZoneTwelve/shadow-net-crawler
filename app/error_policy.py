# ./app/error_policy.py
from typing import Tuple, Dict

# Simple table of responses and actions.
# Each code -> (retry: bool, abandon_after_attempts: int or None, note)
ERROR_POLICY: Dict[int, Tuple[bool, int | None, str]] = {
    200: (False, None, "OK"),
    204: (False, None, "No Content"),
    301: (True, None, "Redirect - follow if allowed"),
    302: (True, None, "Redirect - follow if allowed"),
    304: (False, None, "Not Modified"),
    400: (False, None, "Bad Request - do not retry"),
    401: (False, None, "Unauthorized - do not retry"),
    403: (False, None, "Forbidden - do not retry"),
    404: (False, None, "Not Found - do not retry"),
    408: (True, 5, "Request Timeout - retry with backoff"),
    429: (True, 10, "Too Many Requests - retry with backoff"),
    500: (True, 10, "Server Error - retry with backoff"),
    502: (True, 10, "Bad Gateway - retry"),
    503: (True, 10, "Service Unavailable - retry"),
    504: (True, 10, "Gateway Timeout - retry"),
}

# Default decisions for codes not explicitly listed
DEFAULT_POLICY = (True, 10, "Unknown - retry with backoff")


def decide_action(status_code: int) -> Tuple[bool, int | None, str]:
    """Return (should_retry, abandon_after_attempts, note)."""
    return ERROR_POLICY.get(status_code, DEFAULT_POLICY)
