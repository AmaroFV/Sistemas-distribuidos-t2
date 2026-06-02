from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import pandas as pd
import csv
import os

app = FastAPI()

# Ruta para persistir los datos en el volumen compartido
CSV_FILE_PATH = "data/events_log.csv"
events_db = []


class EventRecord(BaseModel):
    timestamp: float
    event_type: str  # Ahora recibirá "query_success", "query_retry", etc.
    query_type: Optional[str] = None
    latency_ms: Optional[float] = None
    zone_id: Optional[str] = None
    # --- NUEVO PARA KAFKA
    query_id: Optional[str] = None
    retry_count: Optional[int] = None
    consumer_id: Optional[str] = None


# Actualizacion de cabezera del CSV
def init_csv():
    os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
    if not os.path.exists(CSV_FILE_PATH):
        with open(CSV_FILE_PATH, mode="w", newline="") as file:
            writer = csv.writer(file)
            # --- MODIFICADO: Agregamos las nuevas columnas ---
            writer.writerow(
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


def save_event_to_csv(event: EventRecord):
    with open(CSV_FILE_PATH, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                event.timestamp,
                event.event_type,
                event.query_type,
                event.latency_ms,
                event.zone_id,
                # --- NUEVOS CAMBIOS PARA KAFKA
                event.query_id,
                event.retry_count,
                event.consumer_id,
            ]
        )


@app.post("/event")
async def record_event(event: EventRecord, background_tasks: BackgroundTasks):
    """Recibe y almacena eventos de tráfico y caché [cite: 44]"""
    events_db.append(event.dict())
    background_tasks.add_task(save_event_to_csv, event)
    return {"status": "recorded"}


@app.get("/summary")
async def get_metrics_summary():
    """Calcula métricas clave: Hit Rate, Throughput y Latencia [cite: 180]"""
    if not events_db:
        return {"message": "Sin eventos registrados."}

    df = pd.DataFrame(events_db)

    # Cálculo de Hit Rate [cite: 180]
    hits = len(df[df["event_type"] == "cache_hit"])
    misses = len(df[df["event_type"] == "cache_miss"])
    total = hits + misses
    hit_rate = (hits / total) if total > 0 else 0
    miss_rate = (misses / total) if total > 0 else 0

    # Cálculo de Throughput (Consultas/segundo) [cite: 180]
    if total > 1:
        duration = df["timestamp"].max() - df["timestamp"].min()
        throughput = total / duration if duration > 0 else 0
    else:
        throughput = 0

    # Latencias Percentiles p50 y p95 [cite: 180]
    latencies = df[df["latency_ms"].notnull()]["latency_ms"]
    p50 = latencies.quantile(0.5) if not latencies.empty else 0
    p95 = latencies.quantile(0.95) if not latencies.empty else 0

    # Eviction rate (evictions/min)
    evictions = len(df[df["event_type"] == "eviction"])
    # Usamos la misma ventana temporal del experimento si hay >1 evento, si no 0
    eviction_rate_per_min = 0
    if len(df) > 1:
        duration_s = df["timestamp"].max() - df["timestamp"].min()
        eviction_rate_per_min = (evictions / duration_s) * 60 if duration_s > 0 else 0

    # Cache efficiency: (hits*t_cache - misses*t_db) / total
    # Aproximación: t_cache = latencia promedio de hits, t_db = latencia promedio de misses
    hit_lat = df[(df["event_type"] == "cache_hit") & df["latency_ms"].notnull()][
        "latency_ms"
    ]
    miss_lat = df[(df["event_type"] == "cache_miss") & df["latency_ms"].notnull()][
        "latency_ms"
    ]
    t_cache = float(hit_lat.mean()) if not hit_lat.empty else 0.0
    t_db = float(miss_lat.mean()) if not miss_lat.empty else 0.0
    cache_efficiency = (
        ((hits * t_cache) - (misses * t_db)) / total if total > 0 else 0.0
    )

    return {
        "hit_rate": round(hit_rate, 4),
        "miss_rate": round(miss_rate, 4),
        "throughput_req_sec": round(throughput, 2),
        "latency_p50_ms": round(p50, 2),
        "latency_p95_ms": round(p95, 2),
        "evictions_total": int(evictions),
        "eviction_rate_per_min": round(eviction_rate_per_min, 4),
        "cache_efficiency": round(float(cache_efficiency), 4),
        "total_events": len(df),
    }
