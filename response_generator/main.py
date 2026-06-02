from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import pandas as pd
import numpy as np
import os
import random
import time

app = FastAPI()

FAIL_RATE = float(os.getenv("FAIL_RATE", "0"))  # 0.0–1.0 errores 503 aleatorios
FAIL_UNTIL = float(
    os.getenv("FAIL_UNTIL", "0")
)  # epoch: rechazar todo hasta este tiempo
ARTIFICIAL_LATENCY_MS = float(os.getenv("ARTIFICIAL_LATENCY_MS", "0"))


def _simulate_failure() -> None:
    if FAIL_UNTIL and time.time() < FAIL_UNTIL:
        raise HTTPException(status_code=503, detail="Servicio caído (simulación)")
    if FAIL_RATE > 0 and random.random() < FAIL_RATE:
        raise HTTPException(status_code=503, detail="Error temporal simulado")
    if ARTIFICIAL_LATENCY_MS > 0:
        time.sleep(ARTIFICIAL_LATENCY_MS / 1000.0)


# Zonas definidas en la rúbrica
ZONES = {
    "Z1": {
        "name": "Providencia",
        "lat_min": -33.445,
        "lat_max": -33.420,
        "lon_min": -70.640,
        "lon_max": -70.600,
    },
    "Z2": {
        "name": "Las Condes",
        "lat_min": -33.420,
        "lat_max": -33.390,
        "lon_min": -70.600,
        "lon_max": -70.550,
    },
    "Z3": {
        "name": "Maipú",
        "lat_min": -33.530,
        "lat_max": -33.490,
        "lon_min": -70.790,
        "lon_max": -70.740,
    },
    "Z4": {
        "name": "Santiago Centro",
        "lat_min": -33.460,
        "lat_max": -33.430,
        "lon_min": -70.670,
        "lon_max": -70.630,
    },
    "Z5": {
        "name": "Pudahuel",
        "lat_min": -33.470,
        "lat_max": -33.430,
        "lon_min": -70.810,
        "lon_max": -70.760,
    },
}

# Precalcular el área aproximada en km2 de cada bounding box para Q3 y Q4
ZONE_AREAS_KM2 = {}
for zid, z in ZONES.items():
    lat_diff = z["lat_max"] - z["lat_min"]
    lon_diff = z["lon_max"] - z["lon_min"]
    lat_rad = np.radians((z["lat_max"] + z["lat_min"]) / 2)
    area = (lat_diff * 111.32) * (lon_diff * 111.32 * np.cos(lat_rad))
    ZONE_AREAS_KM2[zid] = abs(area)

# Estructura en memoria
data_in_memory = {zid: pd.DataFrame() for zid in ZONES}


@app.on_event("startup")
def load_data():
    global data_in_memory
    print("Iniciando carga y filtrado de datos...")

    # El CSV debe estar montado en /app/data via docker-compose
    file_path = os.getenv("DATA_FILE", "data/region_metropolitana.csv")

    cols = ["latitude", "longitude", "area_in_meters", "confidence"]

    try:
        # Leemos en chunks de 100k líneas para no saturar la RAM
        chunk_iterator = pd.read_csv(file_path, usecols=cols, chunksize=100000)

        for chunk in chunk_iterator:
            for zid, z in ZONES.items():
                mask = (
                    (chunk["latitude"] >= z["lat_min"])
                    & (chunk["latitude"] <= z["lat_max"])
                    & (chunk["longitude"] >= z["lon_min"])
                    & (chunk["longitude"] <= z["lon_max"])
                )
                filtered = chunk[mask]
                if not filtered.empty:
                    data_in_memory[zid] = pd.concat([data_in_memory[zid], filtered])

        print("Carga lista. Registros en memoria por zona:")
        for zid in ZONES:
            print(f" - {ZONES[zid]['name']}: {len(data_in_memory[zid])}")

    except Exception as e:
        print(f"Error grave cargando los datos: {e}")
        raise


class QueryRequest(BaseModel):
    query_type: str
    zone_id: Optional[str] = None
    zone_id_a: Optional[str] = None
    zone_id_b: Optional[str] = None
    confidence_min: float = 0.0
    bins: int = 5


@app.post("/query")
def process_query(req: QueryRequest):

    _simulate_failure()

    q_type = req.query_type.upper()

    if q_type in ["Q1", "Q2", "Q3", "Q5"]:
        if req.zone_id not in data_in_memory:
            raise HTTPException(status_code=400, detail="Zone ID inválido")
        df = data_in_memory[req.zone_id]
        df_filtered = df[df["confidence"] >= req.confidence_min]

    if q_type == "Q1":
        return {"result": len(df_filtered)}

    elif q_type == "Q2":
        if df_filtered.empty:
            return {"avg_area": 0, "total_area": 0, "n": 0}
        areas = df_filtered["area_in_meters"]
        return {
            "avg_area": float(areas.mean()),
            "total_area": float(areas.sum()),
            "n": len(areas),
        }

    elif q_type == "Q3":
        count = len(df_filtered)
        density = count / ZONE_AREAS_KM2[req.zone_id]
        return {"density_per_km2": float(density)}

    elif q_type == "Q4":
        if req.zone_id_a not in data_in_memory or req.zone_id_b not in data_in_memory:
            raise HTTPException(status_code=400, detail="Zone IDs inválidos")

        df_a = data_in_memory[req.zone_id_a]
        df_a_filt = df_a[df_a["confidence"] >= req.confidence_min]
        da = len(df_a_filt) / ZONE_AREAS_KM2[req.zone_id_a]

        df_b = data_in_memory[req.zone_id_b]
        df_b_filt = df_b[df_b["confidence"] >= req.confidence_min]
        db = len(df_b_filt) / ZONE_AREAS_KM2[req.zone_id_b]

        return {
            "zone_a": float(da),
            "zone_b": float(db),
            "winner": req.zone_id_a if da > db else req.zone_id_b,
        }

    elif q_type == "Q5":
        scores = df["confidence"].dropna()
        if scores.empty:
            return {"histogram": []}

        counts, edges = np.histogram(scores, bins=req.bins, range=(0, 1))
        hist_data = [
            {
                "bucket": i,
                "min": float(edges[i]),
                "max": float(edges[i + 1]),
                "count": int(counts[i]),
            }
            for i in range(req.bins)
        ]
        return {"histogram": hist_data}

    else:
        raise HTTPException(status_code=400, detail="Tipo de consulta no soportado")
