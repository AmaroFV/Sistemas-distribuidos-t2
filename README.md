# Tarea 2 SD

**Evolución del Sistema:** En la Tarea 1 se implementó un flujo síncrono y secuencial a través de HTTP. Para esta Tarea 2, el sistema se ha transformado por completo en una **arquitectura orientada a eventos asíncrona** utilizando **Apache Kafka** como bróker central de mensajería. Esto permite gestionar la contrapresión (*backpressure*), desacoplar los componentes y garantizar la resiliencia mediante colas de reintento y aislamiento de errores.

---

## Requisitos y Preparación del Dataset

El sistema completo se ejecuta dentro de contenedores orquestados de forma distribuida.

1. Asegúrese de tener instalados **Docker** y **Docker Compose**.
2. Cree una carpeta llamada `data` en la raíz del repositorio.
3. Coloque el archivo del dataset con el nombre exacto `region_metropolitana.csv` dentro de dicha carpeta: `./data/region_metropolitana.csv`.

### Campos esperados en el CSV:
El módulo de procesamiento (`response_generator`) cargará en memoria el archivo esperando estrictamente la siguiente estructura:
* `latitude` (float)
* `longitude` (float)
* `area_in_meters` (float)
* `confidence` (float)

---

## Arquitectura y Módulos del Sistema

El ecosistema distribuido está compuesto por los siguientes módulos que interactúan de forma asíncrona:

1. **`traffic_generator`:** Simula solicitudes masivas continuas ejecutando consultas geoespaciales específicas ($Q_1$ a $Q_5$).
2. **Kafka Producer:** Intercepta las solicitudes del generador de tráfico, las empaqueta dentro de un objeto de control estandarizado (*Envelope*) en formato JSON y las publica de inmediato en el clúster de mensajería.
3. **Clúster Apache Kafka:** Actúa como el buffer inmutable intermedia de almacenamiento de eventos. Gestiona de forma aislada tres tópicos principales:
   * `queries-main`: Tópico primario donde se encolan todas las consultas nuevas de los usuarios.
   * `queries-retry`: Tópico secundario para procesar reintentos con políticas de retraso (*backoff*).
   * `queries-dlq` (*Dead Letter Queue*): Cola de aislamiento para mensajes corruptos o fallos lógicos permanentes.
4. **Kafka Consumers (Workers):** Réplicas de ejecución que extraen los mensajes de los tópicos mediante un esquema *pull*, decodifican el *Envelope* e invocan la lógica de negocio.
5. **`cache_service`:** Capa intermedia conectada a un nodo **Redis** con políticas de expiración (TTL) para mitigar la carga de cómputo redundante.
6. **`response_generator`:** El motor de cómputo central que mantiene el dataset de edificios en memoria y resuelve analíticamente las consultas espaciales.
7. **`metrics_service` / Monitoreo:** Centraliza las telemetrías de salud del sistema, accesibles en tiempo real mediante el navegador en: `http://localhost:8002/summary`.

---

## Parámetros de Configuración de Experimentos

El comportamiento del clúster distributed puede alterarse modificando las variables de entorno dentro del archivo `docker-compose.yml`:

### 1. Variables de Ingesta (`traffic_generator`)
* `DISTRIBUTION`: Estrategia de tráfico (`zipf` o `uniform`).
* `RATE_RPS`: Cantidad de solicitudes inyectadas por segundo.
* `TOTAL_REQUESTS`: Volumen neto de consultas evaluadas (excluyendo la fase de calentamiento).
* `WARMUP_REQUESTS`: Consultas previas para la estabilización del sistema (*warmup*).
* `ZIPF_S`: Factor de sesgo de accesos para la distribución de Zipf (por defecto `1.2`).

### 2. Configuración de Infraestructura de Mensajería
* `KAFKA_BOOTSTRAP`: Dirección de red del bróker dentro de la malla virtual (`kafka:29092`).

### 3. Ajustes de la Capa de Almacenamiento Volátil (Redis / Caché)
* `TTL_SECONDS`: Tiempo de vida en segundos de las respuestas guardadas en el `cache_service` (por defecto `60s`).
* Modificación de recursos en las banderas de inicio de Redis:
  * Tamaño máximo de memoria: `--maxmemory 50mb` | `--maxmemory 200mb` | `--maxmemory 500mb`
  * Estrategias de desalojo: `--maxmemory-policy allkeys-lru` | `allkeys-lfu` | `allkeys-random`

---

## Estructura del Mensaje (Envelope JSON)

Cada evento encolado en Kafka se transmite bajo un contrato estructurado en JSON para asegurar el rastreo de extremo a extremo:

```json
{
  "query_id": "string (UUID v4 único para trazabilidad distribuida)",
  "query_type": "string (Identificador de consulta Q1 a Q5)",
  "payload": {
    "center_coordinates": [latitude, longitude],
    "radius_meters": float,
    "parameters": {}
  },
  "timestamp_creation": "long (Epoch Unix timestamp en milisegundos)",
  "retry_count": "int (Contador interno de intentos de procesamiento)"
}


