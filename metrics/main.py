import asyncio
import csv
import os
import time
from typing import Optional

import pandas as pd
from confluent_kafka import Consumer, TopicPartition
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "data/events_log.csv")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPICS_FOR_BACKLOG = os.getenv(
    "TOPICS_FOR_BACKLOG", "queries-main,queries-retry,queries-dlq"
).split(",")

events_db: list[dict] = []
_pending_csv_rows: asyncio.Queue[list] = asyncio.Queue()
_flusher_task: Optional[asyncio.Task] = None
_backlog_task: Optional[asyncio.Task] = None
_latest_backlog: dict[str, int] = {}
_failure_started_at: Optional[float] = None
_recovery_samples: list[float] = []


class EventRecord(BaseModel):
    timestamp: float
    event_type: str
    query_type: Optional[str] = None
    latency_ms: Optional[float] = None
    zone_id: Optional[str] = None
    query_id: Optional[str] = None
    retry_count: Optional[int] = None
    consumer_id: Optional[str] = None


def init_csv() -> None:
    os.makedirs(os.path.dirname(CSV_FILE_PATH) or ".", exist_ok=True)
    if not os.path.exists(CSV_FILE_PATH):
        with open(CSV_FILE_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "timestamp",
                    "event_type",
                    "query_type",
                    "latency_ms",
                    "zone_id",
                    "query_id",
                    "retry_count",
                    "consumer_id",
                ]
            )


init_csv()


async def _csv_flusher() -> None:
    while True:
        await asyncio.sleep(float(os.getenv("FLUSH_INTERVAL_S", "1.0")))
        batch: list[list] = []
        try:
            while True:
                batch.append(_pending_csv_rows.get_nowait())
        except asyncio.QueueEmpty:
            pass
        if not batch:
            continue
        with open(CSV_FILE_PATH, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(batch)


def _fetch_backlog_sync() -> dict[str, int]:
    """Lag aproximado por tópico (suma de offsets altos por partición)."""
    out: dict[str, int] = {}
    for topic in TOPICS_FOR_BACKLOG:
        topic = topic.strip()
        if not topic:
            continue
        try:
            c = Consumer(
                {
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "group.id": f"backlog-monitor-{int(time.time())}",
                    "enable.auto.commit": False,
                }
            )
            meta = c.list_topics(topic=topic, timeout=5)
            if topic not in meta.topics:
                out[topic] = 0
                c.close()
                continue
            total = 0
            for p in meta.topics[topic].partitions.values():
                _, high = c.get_watermark_offsets(
                    TopicPartition(topic, p.id),
                    timeout=5,
                )
                total += high
            out[topic] = total
            c.close()
        except Exception:
            out[topic] = -1
    return out


async def _backlog_poller() -> None:
    global _failure_started_at
    while True:
        await asyncio.sleep(float(os.getenv("BACKLOG_POLL_SECONDS", "5")))
        _latest_backlog = await asyncio.to_thread(_fetch_backlog_sync)
        total = sum(v for v in _latest_backlog.values() if v >= 0)
        if total > 0 and _failure_started_at is None:
            _failure_started_at = time.time()
        if total == 0 and _failure_started_at is not None:
            _recovery_samples.append(time.time() - _failure_started_at)
            _failure_started_at = None


@app.on_event("startup")
async def startup() -> None:
    global _flusher_task, _backlog_task
    if _flusher_task is None:
        _flusher_task = asyncio.create_task(_csv_flusher())
    if (
        os.getenv("ENABLE_BACKLOG_POLL", "true").lower() == "true"
        and _backlog_task is None
    ):
        _backlog_task = asyncio.create_task(_backlog_poller())


@app.post("/event")
async def record_event(event: EventRecord):
    row = event.model_dump()
    events_db.append(row)
    _pending_csv_rows.put_nowait(
        [
            event.timestamp,
            event.event_type,
            event.query_type,
            event.latency_ms,
            event.zone_id,
            event.query_id,
            event.retry_count,
            event.consumer_id,
        ]
    )
    return {"status": "recorded"}


@app.post("/admin/mark_failure")
async def mark_failure():
    """Llamar al iniciar simulación de caída (para recovery_time)."""
    global _failure_started_at
    _failure_started_at = time.time()
    return {"failure_marked_at": _failure_started_at}


@app.get("/summary")
async def get_metrics_summary():
    if not events_db:
        return {"message": "Sin eventos registrados.", "backlog": _latest_backlog}

    df = pd.DataFrame(events_db)

    hits = len(df[df["event_type"] == "cache_hit"])
    misses = len(df[df["event_type"] == "cache_miss"])
    successes = len(df[df["event_type"] == "query_success"])
    retries = len(df[df["event_type"] == "query_retry"])
    dlq = len(df[df["event_type"] == "query_dlq"])
    failed = len(df[df["event_type"] == "query_failed"])

    cache_total = hits + misses
    hit_rate = (hits / cache_total) if cache_total > 0 else 0
    miss_rate = (misses / cache_total) if cache_total > 0 else 0

    success_df = df[df["event_type"] == "query_success"]
    if len(success_df) > 1:
        duration = success_df["timestamp"].max() - success_df["timestamp"].min()
        throughput = len(success_df) / duration if duration > 0 else 0
    else:
        throughput = 0

    latencies = (
        success_df["latency_ms"].dropna()
        if "latency_ms" in success_df
        else pd.Series(dtype=float)
    )
    p50 = float(latencies.quantile(0.5)) if len(latencies) else 0
    p95 = float(latencies.quantile(0.95)) if len(latencies) else 0

    total_attempts = successes + dlq + failed
    retry_rate = retries / total_attempts if total_attempts > 0 else 0
    recovery_rate = successes / (successes + dlq) if (successes + dlq) > 0 else 0
    dlq_rate = dlq / total_attempts if total_attempts > 0 else 0

    evictions = len(df[df["event_type"] == "eviction"])
    eviction_rate_per_min = 0.0
    if len(df) > 1:
        duration_s = df["timestamp"].max() - df["timestamp"].min()
        eviction_rate_per_min = (evictions / duration_s) * 60 if duration_s > 0 else 0

    hit_lat = df[(df["event_type"] == "cache_hit") & df["latency_ms"].notnull()][
        "latency_ms"
    ]
    miss_lat = df[(df["event_type"] == "cache_miss") & df["latency_ms"].notnull()][
        "latency_ms"
    ]
    t_cache = float(hit_lat.mean()) if not hit_lat.empty else 0.0
    t_db = float(miss_lat.mean()) if not miss_lat.empty else 0.0
    cache_efficiency = (
        ((hits * t_cache) - (misses * t_db)) / cache_total if cache_total > 0 else 0.0
    )

    backlog_total = sum(v for v in _latest_backlog.values() if v >= 0)
    recovery_time_s = (
        sum(_recovery_samples) / len(_recovery_samples) if _recovery_samples else None
    )

    return {
        "hit_rate": round(hit_rate, 4),
        "miss_rate": round(miss_rate, 4),
        "throughput_req_sec": round(throughput, 2),
        "latency_p50_ms": round(p50, 2),
        "latency_p95_ms": round(p95, 2),
        "retry_rate": round(retry_rate, 4),
        "recovery_rate": round(recovery_rate, 4),
        "dlq_rate": round(dlq_rate, 4),
        "backlog_size": backlog_total,
        "backlog_by_topic": _latest_backlog,
        "recovery_time_s": recovery_time_s,
        "evictions_total": int(evictions),
        "eviction_rate_per_min": round(eviction_rate_per_min, 4),
        "cache_efficiency": round(float(cache_efficiency), 4),
        "query_success": int(successes),
        "query_retry": int(retries),
        "query_dlq": int(dlq),
        "total_events": len(df),
    }
