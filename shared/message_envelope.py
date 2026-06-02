import time
import uuid
from typing import Any, Dict, Optional


def new_envelope(query: Dict[str, Any], *, retry_count: int = 0, query_id: Optional[str] = None) -> Dict[str, Any]:
    return {
        "query_id": query_id or str(uuid.uuid4()),
        "created_at": time.time(),
        "retry_count": retry_count,
        "query": query,
    }


def should_dlq(retry_count: int, max_retries: int) -> bool:
    return retry_count >= max_retries