import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
from confluent_kafka import Consumer, KafkaError, Producer

ROOT = os.getenv("PYTHONPATH", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, ROOT)
from shared.message_envelope import should_dlq  # noqa: E402

CACHE_URL = os.getenv("CACHE_URL", "http://cache_service:8001/query")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics_service:8002/event")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_MAIN = os.getenv("TOPIC_MAIN", "queries-main")
TOPIC_RETRY = os.getenv("TOPIC_RETRY", "queries-retry")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "queries-dlq")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "query-processors")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
CONSUMER_NAME = os.getenv("HOSTNAME", "consumer-1")

# Códigos HTTP que disparan reintento (falla temporal)
RETRYABLE_STATUS = {502, 503, 504, 429}


def _emit_metrics(
    client: httpx.Client,
    *,
    event_type: str,
    query_id: str,
    query_type: Optional[str],
    latency_ms: Optional[float],
    retry_count: int,
    zone_id: Optional[str] = None,
) -> None:
    try:
        client.post(
            METRICS_URL,
            json={
                "timestamp": time.time(),
                "event_type": event_type,
                "query_id": query_id,
                "query_type": query_type,
                "latency_ms": latency_ms,
                "retry_count": retry_count,
                "zone_id": zone_id,
                "consumer_id": CONSUMER_NAME,
            },
            timeout=2.0,
        )
    except Exception:
        pass


def _zone_from_query(q: Dict[str, Any]) -> Optional[str]:
    qt = (q.get("query_type") or "").upper()
    if qt in {"Q1", "Q2", "Q3", "Q5"}:
        return q.get("zone_id")
    if qt == "Q4":
        a, b = q.get("zone_id_a"), q.get("zone_id_b")
        return f"{a}|{b}" if a and b else None
    return None


def _process_query(http: httpx.Client, envelope: Dict[str, Any]) -> Tuple[bool, Optional[int]]:
    """success=False → reintento o DLQ según retry_count."""
    query = envelope["query"]
    start = time.perf_counter()
    try:
        r = http.post(CACHE_URL, json=query, timeout=30.0)
        latency_ms = (time.perf_counter() - start) * 1000
        if r.status_code in RETRYABLE_STATUS:
            return False, r.status_code
        r.raise_for_status()
        _emit_metrics(
            http,
            event_type="query_success",
            query_id=envelope["query_id"],
            query_type=query.get("query_type"),
            latency_ms=latency_ms,
            retry_count=envelope.get("retry_count", 0),
            zone_id=_zone_from_query(query),
        )
        return True, r.status_code
    except httpx.HTTPStatusError as e:
        code = e.response.status_code if e.response is not None else None
        if code in RETRYABLE_STATUS:
            return False, code
        _emit_metrics(
            http,
            event_type="query_failed",
            query_id=envelope["query_id"],
            query_type=query.get("query_type"),
            latency_ms=(time.perf_counter() - start) * 1000,
            retry_count=envelope.get("retry_count", 0),
        )
        return False, code
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout):
        return False, None
    except Exception:
        return False, None


def _publish(producer: Producer, topic: str, envelope: Dict[str, Any]) -> None:
    producer.produce(
        topic,
        key=envelope["query_id"].encode("utf-8"),
        value=json.dumps(envelope).encode("utf-8"),
    )
    producer.poll(0)


def _handle_failure(
    producer: Producer,
    http: httpx.Client,
    envelope: Dict[str, Any],
) -> None:
    retry_count = int(envelope.get("retry_count", 0)) + 1
    envelope["retry_count"] = retry_count
    qtype = envelope.get("query", {}).get("query_type")

    if should_dlq(retry_count, MAX_RETRIES):
        _publish(producer, TOPIC_DLQ, envelope)
        _emit_metrics(
            http,
            event_type="query_dlq",
            query_id=envelope["query_id"],
            query_type=qtype,
            latency_ms=None,
            retry_count=retry_count,
        )
        print(f"DLQ query_id={envelope['query_id']} retries={retry_count}", flush=True)
        return

    _publish(producer, TOPIC_RETRY, envelope)
    _emit_metrics(
        http,
        event_type="query_retry",
        query_id=envelope["query_id"],
        query_type=qtype,
        latency_ms=None,
        retry_count=retry_count,
    )


def _topics_subscribe() -> List[str]:
    return [TOPIC_MAIN, TOPIC_RETRY]


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(_topics_subscribe())

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})
    http = httpx.Client(limits=httpx.Limits(max_connections=50))

    print(
        f"Consumer {CONSUMER_NAME} group={GROUP_ID} topics={_topics_subscribe()}",
        flush=True,
    )

    processed = 0
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Kafka error: {msg.error()}", flush=True)
            continue

        try:
            envelope = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print(f"JSON inválido: {e}", flush=True)
            continue

        ok, _status = _process_query(http, envelope)
        if not ok:
            _handle_failure(producer, http, envelope)
        producer.flush(5)
        processed += 1
        if processed % 200 == 0:
            print(f"processed={processed}", flush=True)


if __name__ == "__main__":
    main()