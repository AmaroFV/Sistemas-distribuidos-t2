import json
import os
import sys
import time

from confluent_kafka import Producer

ROOT = os.getenv("PYTHONPATH", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, ROOT)
from shared.query_builder import build_query 
from shared.message_envelope import new_envelope 


def _delivery_report(err, msg) -> None:
    if err:
        print(f"ERROR produce: {err}", flush=True)


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    topic_main = os.getenv("TOPIC_MAIN", "queries-main")
    distribution = os.getenv("DISTRIBUTION", "zipf").strip().lower()
    if distribution not in {"zipf", "uniform"}:
        distribution = "zipf"
    zipf_s = float(os.getenv("ZIPF_S", "1.2"))
    total_requests = int(os.getenv("TOTAL_REQUESTS", "5000"))
    warmup_requests = int(os.getenv("WARMUP_REQUESTS", "200"))
    rate_rps = float(os.getenv("RATE_RPS", "0") or "0")

    total_to_send = warmup_requests + total_requests
    interval_s = (1.0 / rate_rps) if rate_rps > 0 else 0.0

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "linger.ms": 5,
    })

    print(
        f"Kafka producer → {topic_main} bootstrap={bootstrap} "
        f"total={total_to_send} rate_rps={rate_rps or 'max'}",
        flush=True,
    )

    sent = 0
    start = time.time()
    while sent < total_to_send:
        query = build_query(distribution, zipf_s=zipf_s)
        envelope = new_envelope(query)
        producer.produce(
            topic_main,
            key=envelope["query_id"].encode("utf-8"),
            value=json.dumps(envelope).encode("utf-8"),
            callback=_delivery_report,
        )
        producer.poll(0)
        sent += 1
        if interval_s > 0:
            time.sleep(interval_s)
        if sent % 500 == 0:
            elapsed = time.time() - start
            print(f"produced={sent} rps={sent/elapsed:.1f}", flush=True)

    producer.flush(30)
    elapsed = time.time() - start
    print(f"FINALIZADO produced={sent} elapsed_s={elapsed:.1f}", flush=True)

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()