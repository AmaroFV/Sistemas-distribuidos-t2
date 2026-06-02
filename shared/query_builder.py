import os
import random
from typing import Dict

import numpy as np

ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]

def pick_zone(distribution: str, *, zipf_s: float) -> str:
    if distribution == "uniform":
        return random.choice(ZONES)
    n = len(ZONES)
    while True:
        k = int(np.random.zipf(zipf_s))
        if 1 <= k <= n:
            return ZONES[k - 1]


def build_query(distribution: str, *, zipf_s: float) -> Dict:
    q = random.choice(QUERY_TYPES)
    confidence_min = random.choice([0.0, 0.5, 0.8])
    bins = random.choice([5, 10])
    mode = os.getenv("KEYSPACE_MODE", "").strip().lower()
    synthetic_suffix = ""
    if mode == "high_cardinality":
        synthetic_suffix = f":v{random.randint(1, 10_000)}"

    if q in {"Q1", "Q2", "Q3", "Q5"}:
        z = pick_zone(distribution, zipf_s=zipf_s)
        body = {
            "query_type": q,
            "zone_id": z,
            "confidence_min": confidence_min,
            "bins": bins,
        }
        if mode == "high_cardinality":
            body["variant"] = synthetic_suffix
        return body

    a = pick_zone(distribution, zipf_s=zipf_s)
    b = pick_zone(distribution, zipf_s=zipf_s)
    while b == a:
        b = pick_zone(distribution, zipf_s=zipf_s)
    return {
        "query_type": "Q4",
        "zone_id_a": a,
        "zone_id_b": b,
        "confidence_min": confidence_min,
        **(
            {"variant": synthetic_suffix}
            if os.getenv("KEYSPACE_MODE", "").strip().lower() == "high_cardinality"
            else {}
        ),
    }
