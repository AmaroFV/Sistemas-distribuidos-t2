## Sistemas Distribuidos - Tarea 1 (Entregable 1)

Arquitectura con 4 módulos (secuenciales) y un Redis:

- `traffic_generator`: simula tráfico (Zipf o Uniforme) y envía consultas Q1–Q5.
- `cache_service`: caché sobre Redis (TTL) que registra hits/misses y delega misses a `response_generator`.
- `response_generator`: precarga el dataset en memoria y responde Q1–Q5 en memoria.
- `metrics_service`: recibe eventos (`cache_hit`, `cache_miss`, `eviction`) y expone un resumen de métricas.

### Requisitos

- Docker y Docker Compose
- Crear una carpeta llamada "data", la cual contiene el csv
- Dataset `region_metropolitana.csv` dentro de `./data/region_metropolitana.csv`

Campos esperados del CSV: `latitude`, `longitude`, `area_in_meters`, `confidence`.

### Levantar el sistema

Desde la raíz del repo:

```bash
docker compose up --build
```

Servicios:

- Redis: `localhost:6379`
- http://localhost:8002/summary (para ver las metricas en el buscador)

### Configuración de experimentos (caché)

En `docker-compose.yml` puedes variar Redis (ejemplos):

- Tamaño: `--maxmemory 50mb | 200mb | 500mb`
- Política: `--maxmemory-policy allkeys-lru | allkeys-lfu | allkeys-random`

TTL del caché (en `cache_service`) con `TTL_SECONDS` (por defecto 60s).

### Generador de tráfico

Variables de entorno en `traffic_generator`:

- `DISTRIBUTION`: `zipf` o `uniform`
- `RATE_RPS`: consultas/segundo
- `TOTAL_REQUESTS`: número de consultas (no incluye warmup)
- `WARMUP_REQUESTS`: warmup inicial
- `ZIPF_S`: parámetro \(s\) de Zipf (default 1.2)

### Métricas disponibles

En `GET /summary`:

- `hit_rate`, `miss_rate`
- `throughput_req_sec`
- `latency_p50_ms`, `latency_p95_ms`
- `evictions_total`, `eviction_rate_per_min`
- `cache_efficiency` (aprox. usando latencia promedio hit/miss)


## Configuración de Kafka (Tarea 2)
Para la comunicación asíncrona, se utilizan los siguientes tópicos:
* `queries-main`: Tópico principal para consultas nuevas.
* `queries-retry`: Tópico para consultas que fallan temporalmente y necesitan reintento.
* `queries-dlq`: Dead Letter Queue para consultas que agotaron sus reintentos.

**Variables de entorno principales:**
* `KAFKA_BOOTSTRAP`: Dirección del broker de Kafka (ej. `kafka:29092` dentro de Docker).


