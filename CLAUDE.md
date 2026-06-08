# GPU Telemetry System — Project Design

## Overview

A GPU telemetry pipeline built around a **custom message queue service** as its core
infrastructure. Telemetry data is ingested by one or more Streamers, routed through the queue
to one or more Collectors that parse and persist it, and then exposed to consumers via a REST
API Gateway.

The initial data source for the Streamer is a **CSV file** containing DCGM GPU metrics,
looped continuously to simulate a live telemetry feed. The Streamer's ingestion layer is
designed with a clean internal interface so that additional data sources (live device APIs,
scrape endpoints, gRPC streams, etc.) can be added in future without touching the queue,
collector, or API Gateway. See [Future Scope](#future-scope) for planned sources.

All components are written in **Go**, containerised with **Docker**, and deployed on
**Kubernetes** using **Helm**.

---

## Data Source — DCGM CSV Schema (v1)

The initial data source is a CSV file containing NVIDIA GPU telemetry exported by the DCGM
exporter. Each row is one independent telemetry datapoint.

### CSV Columns

| Column        | Type      | Description                                              | Example                                      |
|---------------|-----------|----------------------------------------------------------|----------------------------------------------|
| `timestamp`   | string    | ISO 8601 UTC timestamp of the original measurement       | `2025-07-18T20:42:34Z`                       |
| `metric_name` | string    | DCGM metric identifier                                   | `DCGM_FI_DEV_GPU_UTIL`                       |
| `gpu_id`      | int       | GPU index on the host (0–7)                              | `0`                                          |
| `device`      | string    | Device node name                                         | `nvidia0`                                    |
| `uuid`        | string    | GPU hardware UUID — primary identifier for a GPU         | `GPU-5fd4f087-86f3-7a43-b711-4771313afc50`   |
| `modelName`   | string    | GPU model                                                | `NVIDIA H100 80GB HBM3`                      |
| `Hostname`    | string    | Host the GPU is installed in                             | `mtv5-dgx1-hgpu-031`                         |
| `container`   | string    | Kubernetes container name (nullable)                     | —                                            |
| `pod`         | string    | Kubernetes pod name (nullable)                           | —                                            |
| `namespace`   | string    | Kubernetes namespace (nullable)                          | —                                            |
| `value`       | float     | Metric reading                                           | `97`                                         |
| `labels_raw`  | string    | Raw Prometheus label string for full label fidelity      | `DCGM_FI_DRIVER_VERSION="535.129.03",...`    |

### DCGM Metric Names

| Metric name                  | Description                   | Unit        |
|------------------------------|-------------------------------|-------------|
| `DCGM_FI_DEV_GPU_UTIL`       | GPU compute utilisation       | %           |
| `DCGM_FI_DEV_MEM_COPY_UTIL`  | Memory copy engine utilisation | %          |
| `DCGM_FI_DEV_POWER_USAGE`    | Power draw                    | Watts       |
| `DCGM_FI_DEV_GPU_TEMP`       | GPU temperature               | °C          |
| `DCGM_FI_DEV_FB_USED`        | Framebuffer memory used       | MiB         |
| `DCGM_FI_DEV_FB_FREE`        | Framebuffer memory free       | MiB         |
| `DCGM_FI_DEV_DEC_UTIL`       | Video decoder utilisation     | %           |
| `DCGM_FI_DEV_ENC_UTIL`       | Video encoder utilisation     | %           |
| `DCGM_FI_DEV_SM_CLOCK`       | SM (shader) clock speed       | MHz         |
| `DCGM_FI_DEV_MEM_CLOCK`      | Memory clock speed            | MHz         |

### Key observations from sample data

- GPU UUID is the stable, globally unique identifier for a GPU across hosts and restarts.
  Use `uuid` as the primary key for the `GET /api/v1/gpus/{id}` path parameter.
- A single host runs up to 8 GPUs (gpu_id 0–7). The sample data covers 31 DGX hosts.
- `container`, `pod`, and `namespace` are nullable — GPUs may run bare-metal workloads.
- The CSV timestamp reflects the original DCGM scrape time. The streamer must override this
  with the wall-clock time at which it processes each row (see Streamer section).

---

## Components

### 1. Telemetry Streamer

Reads DCGM telemetry records from a CSV file and publishes each one to the message queue as a
JSON-encoded `TelemetryRecord`. The CSV is read in a continuous loop to simulate an unbounded
live telemetry feed.

**Behaviour:**
- Parse each CSV row into a structured `TelemetryRecord`.
- **Assign** `processed_at` to `time.Now()` at the moment the row is read — this is the
  canonical event timestamp used downstream. The original CSV timestamp is preserved as
  `source_timestamp` for auditability only.
- Publish the record to the configured topic on the message queue.
- After reaching the last row, wrap back to row 1 and continue (infinite loop).
- Sleep a configurable interval between publishes to control the stream rate.

**Scaling:**
- Multiple streamer instances run independently and concurrently.
- Instances do not coordinate with each other; the queue handles fan-in from all producers.
- Maximum 10 streamer instances.
- Instances are added or removed at runtime without stopping others.

**Go struct — TelemetryRecord (published message):**
```go
type TelemetryRecord struct {
    ID              string    `json:"id"`               // UUID generated by the streamer
    ProcessedAt     time.Time `json:"processed_at"`     // Wall-clock time — the canonical timestamp
    SourceTimestamp string    `json:"source_timestamp"` // Original CSV timestamp (audit only)
    MetricName      string    `json:"metric_name"`
    GpuID           int       `json:"gpu_id"`
    Device          string    `json:"device"`
    UUID            string    `json:"uuid"`             // GPU hardware UUID
    ModelName       string    `json:"model_name"`
    Hostname        string    `json:"hostname"`
    Container       *string   `json:"container,omitempty"`
    Pod             *string   `json:"pod,omitempty"`
    Namespace       *string   `json:"namespace,omitempty"`
    Value           float64   `json:"value"`
    LabelsRaw       string    `json:"labels_raw"`
    StreamerID      string    `json:"streamer_id"`      // Which streamer instance published this
}
```

**Configuration (env vars or config file):**
```
STREAMER_ID          # unique name for this instance, e.g. "streamer-0"
CSV_PATH             # path to the DCGM CSV file
PUBLISH_INTERVAL_MS  # sleep between publishes, default 100ms
TOPIC_NAME           # message queue topic, default "telemetry"
QUEUE_ADDR           # message queue service address
```

---

### 2. Telemetry Collector

Consumes messages from the queue, deserialises each `TelemetryRecord`, and persists it to the
storage backend. Multiple instances compete for messages on the same consumer group.

**Behaviour:**
- Poll the queue topic in a loop.
- Deserialise each message as `TelemetryRecord`.
- Persist the record to the storage backend (see Storage section).
- On malformed message: log the error, emit a metric, and discard. Do not retry indefinitely.
- After N failed delivery attempts (configured), route to a dead-letter topic.
- Commit the offset only after a successful persist (at-least-once semantics).

**Scaling:**
- Maximum 10 collector instances.
- All instances share the same consumer group ID; the queue delivers each message to exactly
  one instance (competing consumers / load balancing).
- Instances are added or removed at runtime without message loss or offset corruption.

**Configuration (env vars or config file):**
```
COLLECTOR_ID         # unique name for this instance, e.g. "collector-0"
CONSUMER_GROUP       # consumer group ID, default "telemetry-collectors"
TOPIC_NAME           # message queue topic, default "telemetry"
QUEUE_ADDR           # message queue address
DB_DSN               # storage backend connection string
BATCH_SIZE           # max messages per poll, default 50
DLQ_TOPIC            # dead-letter topic name, default "telemetry-dlq"
MAX_RETRIES          # retry limit before DLQ, default 3
```

---

### 3. API Gateway

A REST API exposing the persisted GPU telemetry. Written in Go using a framework that
auto-generates an OpenAPI 3.x specification from code annotations — no hand-authored
`openapi.yaml`.

**Recommended framework:** `go-swagger` or `huma` (both support spec generation from Go types
and handler annotations).

#### Endpoints

##### `GET /api/v1/gpus`

Returns a list of all distinct GPUs seen in the telemetry data.

**Response 200:**
```json
{
  "gpus": [
    {
      "uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
      "gpu_id": 0,
      "device": "nvidia0",
      "model_name": "NVIDIA H100 80GB HBM3",
      "hostname": "mtv5-dgx1-hgpu-031",
      "first_seen": "2025-07-18T20:42:34Z",
      "last_seen": "2025-07-18T20:42:37Z"
    }
  ],
  "total": 248
}
```

---

##### `GET /api/v1/gpus/{id}/telemetry`

Returns paginated telemetry records for a specific GPU, identified by its hardware UUID.

**Path parameter:**
- `id` — GPU hardware UUID (e.g. `GPU-5fd4f087-86f3-7a43-b711-4771313afc50`)

**Query parameters:**

| Parameter    | Type   | Required | Description                                      |
|--------------|--------|----------|--------------------------------------------------|
| `start_time` | string | No       | ISO 8601 timestamp — lower bound (inclusive)     |
| `end_time`   | string | No       | ISO 8601 timestamp — upper bound (inclusive)     |
| `metric`     | string | No       | Filter by DCGM metric name                       |
| `limit`      | int    | No       | Page size (default 100, max 1000)                |
| `cursor`     | string | No       | Opaque pagination cursor from previous response  |

**Response 200:**
```json
{
  "gpu_uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
  "records": [
    {
      "id": "01930a4e-7b3c-7f2d-a1b2-c3d4e5f60001",
      "processed_at": "2025-07-18T20:42:34.123Z",
      "metric_name": "DCGM_FI_DEV_GPU_UTIL",
      "gpu_id": 0,
      "device": "nvidia0",
      "uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
      "model_name": "NVIDIA H100 80GB HBM3",
      "hostname": "mtv5-dgx1-hgpu-031",
      "value": 97.0,
      "container": null,
      "pod": null,
      "namespace": null
    }
  ],
  "next_cursor": "eyJ0cyI6IjIwMjUtMDctMThUMjA6NDI6MzVaIn0=",
  "total": 2470
}
```

**Response 404** — GPU UUID not found.

**Time filtering:**
- `GET /api/v1/gpus/{id}/telemetry?start_time=2025-07-18T20:42:34Z&end_time=2025-07-18T20:42:37Z`
- Both `start_time` and `end_time` are optional independently.
- Filtering is applied on `processed_at` (the streamer wall-clock timestamp).

---

**Additional endpoints:**

| Method | Path            | Description                            |
|--------|-----------------|----------------------------------------|
| `GET`  | `/healthz`      | Liveness probe — returns `200 OK`      |
| `GET`  | `/readyz`       | Readiness probe — checks DB connection |
| `GET`  | `/docs`         | Swagger UI (auto-generated)            |
| `GET`  | `/openapi.json` | Raw OpenAPI 3.x spec (auto-generated)  |

---

### 4. Message Queue (Standalone Service)

A custom messaging system written in Go and deployed as an independent service — both locally
(Docker Compose) and on Kubernetes. Streamers and collectors connect to it over HTTP/1.1.
It is **not** an embedded library; it runs as its own process with its own container image and
Helm sub-chart.

**Functional requirements:**
- Named topics. Producers publish to a topic; consumers subscribe to a topic.
- Multiple concurrent producers on the same topic (fan-in).
- Competing consumers within a consumer group — each message delivered to exactly one instance.
- Multiple independent consumer groups each receive all messages independently (fan-out between groups).
- Consumer offset tracking per group — supports resume after restart without replaying from the start.
- In-order delivery per topic (FIFO).

**Design constraints:**
- Single-node deployment (no distributed consensus required for this exercise).
- Maximum 10 producer instances and 10 consumer instances.
- Must not drop messages under normal operating conditions.
- Must remain functional as producer and consumer instances are added or removed at runtime.

**Design decisions:**

| Concern             | Decision                                                                                      |
|---------------------|-----------------------------------------------------------------------------------------------|
| Storage             | In-memory ring buffer per topic + WAL (write-ahead log) file for crash durability             |
| Backpressure        | Return `HTTP 429 Too Many Requests` to producers when queue depth exceeds `HIGH_WATER_MARK`   |
| Consumer offset     | In-memory map of `(group_id → offset)`; fsynced to disk on every `POST /commit`              |
| Dead-letter         | Messages exceeding `MAX_RETRIES` nacks are moved to `<topic>-dlq` topic automatically        |
| Observability       | `GET /metrics` exposes Prometheus-format metrics: queue depth, publish rate, consume rate, consumer lag per group |
| Concurrency         | One goroutine per active consumer long-poll, mutex-guarded ring buffer, channel-based dispatch |

**Wire protocol — HTTP/1.1 + JSON**

The queue exposes a small REST API consumed by the streamer and collector Go clients:

| Method | Path                                      | Description                                          |
|--------|-------------------------------------------|------------------------------------------------------|
| `POST` | `/topics/{topic}/messages`                | Publish one message (producer)                       |
| `GET`  | `/topics/{topic}/messages?group={g}&limit={n}` | Long-poll fetch up to N messages (consumer)     |
| `POST` | `/topics/{topic}/commit`                  | Commit consumer group offset after processing        |
| `POST` | `/topics/{topic}/nack`                    | Nack a message — triggers retry or DLQ routing       |
| `GET`  | `/topics`                                 | List all topics and their current depth              |
| `GET`  | `/healthz`                                | Liveness probe                                       |
| `GET`  | `/metrics`                                | Prometheus metrics endpoint                          |

**Publish request body:**
```json
{
  "payload": "<base64-encoded message bytes>",
  "producer_id": "streamer-0"
}
```

**Fetch response body:**
```json
{
  "messages": [
    {
      "offset": 1042,
      "payload": "<base64-encoded message bytes>",
      "published_at": "2025-07-18T20:42:34.123Z",
      "producer_id": "streamer-0"
    }
  ],
  "next_offset": 1043
}
```

**Go client interface** (used by streamer and collector to talk to the queue service):
```go
// QueueClient is the HTTP client used by streamers and collectors.
type QueueClient interface {
    Publish(ctx context.Context, topic string, payload []byte) error
    Fetch(ctx context.Context, topic, groupID string, limit int) ([]Message, error)
    Commit(ctx context.Context, topic, groupID string, offset int64) error
    Nack(ctx context.Context, topic, groupID string, offset int64) error
    Close() error
}

type Message struct {
    Offset      int64
    Payload     []byte
    PublishedAt time.Time
    ProducerID  string
}
```

**Kubernetes deployment:**
- Deployed as a `Deployment` with `replicas: 1` (single-node for this exercise).
- Exposed inside the cluster via a `ClusterIP` `Service` on port `8080`.
- WAL file and offset state stored on a `PersistentVolumeClaim` (no data loss on pod restart).
- Liveness probe: `GET /healthz`.
- Readiness probe: `GET /healthz` (only becomes ready once WAL is replayed and ring buffer is warm).

**Configuration (env vars):**
```
MQ_PORT              # HTTP listen port, default 8080
MQ_HIGH_WATER_MARK   # max messages in ring buffer before backpressure, default 100000
MQ_WAL_PATH          # path to WAL file, default /data/wal.log
MQ_MAX_RETRIES       # nack retry limit before DLQ routing, default 3
MQ_POLL_TIMEOUT_MS   # max wait time on long-poll fetch, default 5000
```

---

## Data Flow

```
dcgm_metrics.csv
       │
       ▼ (loop, wall-clock timestamp injected)
Streamer ×1–10
       │  POST /topics/telemetry/messages  (HTTP/1.1 + JSON)
       ▼
┌─────────────────────────────────────────┐
│   Message Queue Service  (ClusterIP)    │
│                                         │
│  ring buffer  ·  WAL  ·  offset store  │
│  /topics  ·  /metrics  ·  /healthz     │
└──────────────────┬──────────────────────┘
                   │  GET /topics/telemetry/messages?group=collectors
       ┌───────────┼───────────┐
       ▼           ▼           ▼
  Collector-0  Collector-1  Collector-N  (×1–10)
       │           │           │   POST /topics/telemetry/commit
       └───────────┼───────────┘
                   ▼
          Storage (PostgreSQL
            + TimescaleDB)
                   │
                   ▼
            API Gateway
   ┌───────────────┼───────────────┐
   ▼               ▼               ▼
GET /api/v1/gpus  GET /{id}/telemetry  /docs
```

---

## Storage

Recommended: **PostgreSQL** with the **TimescaleDB** extension (optimised for time-series queries).

### Table: `gpus`

```sql
CREATE TABLE gpus (
    uuid        TEXT PRIMARY KEY,
    gpu_id      INT NOT NULL,
    device      TEXT NOT NULL,
    model_name  TEXT NOT NULL,
    hostname    TEXT NOT NULL,
    first_seen  TIMESTAMPTZ NOT NULL,
    last_seen   TIMESTAMPTZ NOT NULL
);
```

### Table: `telemetry` (TimescaleDB hypertable)

```sql
CREATE TABLE telemetry (
    id               UUID PRIMARY KEY,
    processed_at     TIMESTAMPTZ NOT NULL,
    source_timestamp TEXT,
    metric_name      TEXT NOT NULL,
    gpu_uuid         TEXT NOT NULL REFERENCES gpus(uuid),
    gpu_id           INT NOT NULL,
    device           TEXT NOT NULL,
    hostname         TEXT NOT NULL,
    value            DOUBLE PRECISION NOT NULL,
    container        TEXT,
    pod              TEXT,
    namespace        TEXT,
    labels_raw       TEXT,
    streamer_id      TEXT
);

SELECT create_hypertable('telemetry', 'processed_at');
CREATE INDEX ON telemetry (gpu_uuid, processed_at DESC);
CREATE INDEX ON telemetry (metric_name, processed_at DESC);
```

---

## Project Structure

```
telemetry-system/
├── cmd/
│   ├── streamer/        # main.go — streamer binary
│   ├── collector/       # main.go — collector binary
│   ├── gateway/         # main.go — API gateway binary
│   └── mq/              # main.go — message queue service binary
├── internal/
│   ├── csv/             # CSV parser and row-to-TelemetryRecord mapping
│   ├── mq/
│   │   ├── server/      # HTTP server, route handlers, long-poll dispatch
│   │   ├── ringbuffer/  # in-memory ring buffer per topic
│   │   ├── wal/         # write-ahead log for crash durability
│   │   └── offset/      # consumer group offset store (in-memory + disk)
│   ├── mqclient/        # Go HTTP client used by streamer and collector
│   ├── storage/         # DB layer (repository pattern)
│   ├── model/           # shared Go structs (TelemetryRecord, GPU, Message, etc.)
│   └── api/             # HTTP handlers, OpenAPI annotations
├── helm/
│   └── telemetry/
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│           ├── mq-deployment.yaml         # Message Queue Deployment
│           ├── mq-service.yaml            # ClusterIP Service (port 8080)
│           ├── mq-pvc.yaml                # PersistentVolumeClaim for WAL + offsets
│           ├── streamer-deployment.yaml
│           ├── collector-deployment.yaml
│           ├── gateway-deployment.yaml
│           ├── hpa.yaml                   # HorizontalPodAutoscaler for streamer + collector
│           ├── configmap.yaml
│           └── services.yaml
├── docker/
│   ├── Dockerfile.streamer
│   ├── Dockerfile.collector
│   ├── Dockerfile.gateway
│   └── Dockerfile.mq
├── docker-compose.yaml  # local dev: mq + streamer + collector + gateway + postgres
└── README.md
```

---

## Scaling Rules

| Component      | Min replicas | Max replicas | Scale metric                          |
|----------------|-------------|-------------|---------------------------------------|
| Streamer       | 1           | 10          | Manual (`kubectl scale`) or HPA       |
| Collector      | 1           | 10          | Consumer lag (custom HPA metric)      |
| API Gateway    | 1           | —           | CPU / request rate                    |
| Message Queue  | 1           | 1           | Single-node for this exercise; stateful PVC |

Dynamic scale-up/down must not:
- Stop other running instances
- Lose messages already in the queue
- Corrupt consumer group offsets

---

## Deployment — Docker + Kubernetes + Helm

### Docker images

One Dockerfile per binary. Use multi-stage builds:
```dockerfile
FROM golang:1.23-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /streamer ./cmd/streamer

FROM alpine:3.20
COPY --from=build /streamer /streamer
ENTRYPOINT ["/streamer"]
```

### Helm chart — `helm/telemetry/`

Key `values.yaml` knobs:

```yaml
streamer:
  replicas: 1                        # scale 1–10
  csvPath: /data/dcgm_metrics.csv
  publishIntervalMs: 100
  topicName: telemetry
  queueAddr: http://mq-service:8080  # ClusterIP service name

collector:
  replicas: 2                        # scale 1–10
  consumerGroup: telemetry-collectors
  topicName: telemetry
  batchSize: 50
  maxRetries: 3
  dlqTopic: telemetry-dlq
  queueAddr: http://mq-service:8080

gateway:
  replicas: 1
  port: 8080

mq:
  port: 8080
  highWaterMark: 100000              # max buffered messages before HTTP 429
  walPath: /data/wal.log             # mounted from PVC
  maxRetries: 3
  pollTimeoutMs: 5000
  persistence:
    enabled: true
    storageClass: standard
    size: 5Gi

db:
  dsn: "postgres://user:pass@postgres:5432/telemetry?sslmode=disable"
```

### HPA for Collectors (consumer lag metric)

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: collector-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: collector
  minReplicas: 1
  maxReplicas: 10
  metrics:
    - type: External
      external:
        metric:
          name: telemetry_consumer_lag
        target:
          type: AverageValue
          averageValue: "1000"
```

---

## Non-Goals (for this exercise)

- Authentication or authorisation on the API Gateway
- Multi-node / clustered message queue (the standalone service runs as a single replica)
- Long-term data retention policies
- Schema registry or message versioning
- TLS between internal services
- Cloud-provider-specific managed services (RDS, MSK, etc.)

---

## Future Scope

### Additional Streamer data sources

The Streamer's ingestion layer will be extended to support sources beyond CSV. Each new source
implements the same internal `Source` interface and produces the same `TelemetryRecord` output,
leaving the queue, collector, and API Gateway unchanged.

| Source type | Description |
|---|---|
| **HTTP scrape** | Pull metrics from a Prometheus-compatible `/metrics` endpoint on a configurable interval |
| **gRPC stream** | Consume a server-side streaming RPC from a live telemetry agent (e.g. DCGM gRPC exporter) |
| **Kafka / external MQ** | Bridge an existing upstream Kafka topic into the internal queue |
| **WebSocket** | Subscribe to a push-based telemetry feed over WebSocket |

When a new source is added, only `SOURCE_TYPE` and `SOURCE_URI` env vars need updating in the
Helm `values.yaml` — no changes to the Deployment spec or other components.

### Other planned improvements

- **API authentication** — JWT or API-key auth on the Gateway
- **TLS everywhere** — mTLS between all internal services via a service mesh (e.g. Linkerd)
- **Multi-node message queue** — partition-based horizontal scaling of the queue service beyond single-node
- **Schema registry** — versioned `TelemetryRecord` schema with backward-compatibility guarantees
- **Retention policies** — configurable time- and size-based data pruning on the storage layer