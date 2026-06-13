# gpqueue — GPU Telemetry Pipeline

A production-grade GPU telemetry pipeline built around a **custom message queue** as its core
infrastructure. DCGM GPU metrics are ingested by one or more Streamers, routed through the
queue to competing Collectors that persist them, and then served to consumers via a REST API
Gateway with auto-generated OpenAPI documentation.

## Project Status

> **Alpha — actively developed**

| Area | Status |
|---|---|
| Message queue service | Complete |
| Streamer (CSV source) | Complete |
| Collector + storage | Complete |
| REST API Gateway | Complete |
| OpenAPI / Swagger UI | Complete |
| Helm chart (Kubernetes) | Complete |
| Docker images (multi-stage) | Complete |
| WAL crash recovery | Complete |
| Dead-letter queue | Complete |
| HPA (consumer lag metric) | Defined in Helm, requires Prometheus adapter |
| Additional streamer sources (gRPC, HTTP scrape, Kafka) | Planned |
| API authentication | Planned |
| mTLS between services | Planned |

---

## Description

DCGM telemetry from a CSV source is streamed through 1–10 Streamer instances into the custom
Message Queue. From there, 1–10 competing Collector instances consume messages, persist them to
PostgreSQL + TimescaleDB, and the API Gateway exposes the data via REST.

![GPU Queue Architecture](data/architecture.png)


### Components

| Component | Description |
|---|---|
| **Message Queue** | Custom Go HTTP service with in-memory ring buffer, WAL for crash durability, consumer group offset tracking, and dead-letter queue routing |
| **Streamer** | Reads DCGM CSV in a continuous loop and publishes each row as a `TelemetryRecord` JSON message; 1–10 instances |
| **Collector** | Competes with peer instances to consume messages and persist them to TimescaleDB; 1–10 instances |
| **API Gateway** | REST API (huma + chi) with auto-generated OpenAPI 3.x spec and Swagger UI |
| **Storage** | PostgreSQL 16 + TimescaleDB — `gpus` table + `telemetry` hypertable with cursor-based pagination |

### Key design properties

- **At-least-once delivery** — collectors commit offsets only after a successful persist; failed messages are nacked and retried up to `MAX_RETRIES` before being routed to `telemetry-dlq`
- **Fan-in / fan-out** — multiple streamer producers share one topic; multiple independent consumer groups each receive the full message stream
- **Crash durability** — the queue replays its WAL on startup to restore the ring buffer; consumer offsets are fsynced on every commit
- **Backpressure** — the queue returns `HTTP 429` to producers when ring buffer depth exceeds `HIGH_WATER_MARK`
- **Zero-downtime scaling** — streamer and collector instances can be added or removed at runtime without losing messages or corrupting offsets

---

## Design Considerations

### Ring buffer + WAL

The queue stores messages in an **in-memory ring buffer** per topic. A ring buffer gives O(1)
append and read with a fixed, predictable memory footprint controlled by `HIGH_WATER_MARK`.

Durability is provided by a **write-ahead log (WAL)**: every publish is appended to a
sequential log file before the in-memory buffer is updated. On startup the server replays the
WAL to rebuild the ring buffer, so no messages are lost across pod restarts. The WAL file is
stored on a `PersistentVolumeClaim` in Kubernetes for exactly this reason.

Consumer group offsets are kept in a separate JSON file, also on the PVC, and fsynced on
every `POST /commit`. This decouples offset durability from message durability and keeps the
WAL append path fast.

---

### Consumer group model

Two distinct delivery semantics are supported simultaneously:

| Semantics | Mechanism |
|---|---|
| **Competing consumers** (load balancing) | Multiple consumers share a `group_id`; the queue tracks the per-group read cursor and advances it when any consumer fetches, so each message is delivered to exactly one instance in the group. |
| **Independent groups** (fan-out / broadcast) | Each group has its own offset; a second group starts reading from offset 0 and receives every message independently. |

This mirrors Kafka's consumer group model but implemented with a simple in-memory offset map,
which is sufficient for the fixed scale (≤10 consumers per group).

---

### At-least-once delivery

Collectors commit an offset (`POST /topics/{topic}/commit`) **only after** successfully
persisting the record to PostgreSQL. If the collector crashes before committing, the offset
is not advanced and the message will be redelivered on the next fetch — giving at-least-once
semantics.

Exactly-once would require distributed transactions between the queue and the database.
At-least-once is safe here because `INSERT ... ON CONFLICT (id) DO NOTHING` makes
`InsertTelemetry` idempotent: reprocessing the same record is a no-op.

---

### Dead-letter queue (DLQ)

When a message is nacked more than `MAX_RETRIES` times the queue automatically moves it to a
`<topic>-dlq` topic (`telemetry-dlq` by default). This prevents a single poison message from
blocking the consumer forever. The DLQ is itself a first-class topic and can be inspected or
replayed by any consumer group without special tooling.

---

### Backpressure

When the ring buffer depth reaches `HIGH_WATER_MARK` the queue rejects new publish requests
with `HTTP 429 Too Many Requests`. Producers (streamers) observe this and can back off or
shed load. This keeps memory usage bounded and makes the system's capacity limits explicit and
observable rather than silently degrading.

---

### HTTP/1.1 + JSON wire protocol

The queue exposes a plain REST + JSON API over HTTP/1.1. This was chosen over gRPC or a
binary protocol because:

- **No code-gen required** — any `curl` command or standard `http.Client` can talk to it.
- **Debuggability** — every request and response is human-readable in logs.
- **Long-poll fetch** — HTTP/1.1 long-polling is sufficient for the ≤10 consumers per group
  target; we do not need HTTP/2 server push or WebSocket streams at this scale.

The payload field is base64-encoded to allow arbitrary binary content while keeping the
envelope JSON-safe.

---

### Storage — PostgreSQL + TimescaleDB

PostgreSQL was chosen for its rich SQL support and strong transactional guarantees. The
TimescaleDB extension converts the `telemetry` table into a **hypertable** partitioned
automatically by `processed_at`. This gives:

- **Query performance** — range queries over time windows (the most common API access pattern)
  hit a single partition rather than scanning the full table.
- **Compression** — TimescaleDB's columnar compression can reduce telemetry storage by 90%+
  once data ages past the active partition.
- **Familiar SQL** — no new query language; existing PostgreSQL tooling (pg_dump, psql,
  monitoring) works unchanged.

The `gpu_uuid` + `processed_at DESC` index on the telemetry table satisfies the most frequent
query shape (`GET /api/v1/gpus/{id}/telemetry`) without a full table scan.

---

### Cursor-based pagination

The telemetry endpoint uses **opaque cursors** (`next_cursor`) rather than `OFFSET/LIMIT`.
Offset-based pagination on time-series data is fragile: new rows inserted while the client is
paginating shift later pages. Cursor-based pagination encodes the (`processed_at`, `id`) of
the last returned row and uses a keyset condition on the next query, ensuring stable,
consistent pages regardless of concurrent inserts.

The cursor is base64-encoded JSON, keeping the wire format opaque so the encoding can evolve
without breaking API clients.

---

### API framework — huma + chi

`huma` was chosen over `net/http` + `gorilla/mux` or `gin` because it generates a valid
**OpenAPI 3.x specification from Go struct annotations at runtime** — no hand-authored YAML,
no separate code-gen step. The spec and Swagger UI are available at `/openapi.json` and
`/docs` immediately, making the API self-documenting and testable without additional tooling.

`chi` provides the underlying router and is compatible with standard `net/http` middleware.

---

### Kubernetes deployment strategy

| Concern | Decision |
|---|---|
| **Message queue** | `Deployment` with `replicas: 1`; stateful WAL + offsets stored on a PVC so the pod can be rescheduled without data loss. |
| **Streamer** | Stateless `Deployment`; scales 1–10 via `kubectl scale` or HPA on CPU. |
| **Collector** | Stateless `Deployment`; scales 1–10 via HPA keyed on the `telemetry_consumer_lag` metric exported by the queue's `/metrics` endpoint. |
| **API Gateway** | Stateless `Deployment`; scales on CPU / request rate. |
| **PostgreSQL** | Single-pod `StatefulSet` with a PVC; sufficient for the exercise scope. |

All inter-service traffic is over `ClusterIP` services and never leaves the cluster. No TLS
is configured internally (see Future Scope).

---

### Testing strategy

The test suite is structured in three layers:

| Layer | Location | Tooling |
|---|---|---|
| **Unit tests** | `internal/*/` | Standard `testing` package; HTTP handlers tested with `httptest.Server`; DB methods tested with `go-sqlmock` (no real database required) |
| **Integration-style tests** | `internal/mq/server/` | Real in-process MQ server with temp-directory WAL; covers publish, fetch, commit, nack, DLQ, WAL replay, and Prometheus metrics end-to-end |
| **Config / helper tests** | `cmd/*/` | Tests for `loadConfig`, `getenv`, `getenvInt`, and `persist` in each binary |

The `storage.Repository` interface lets API handler tests inject a mock repository without a
running database. The `source.Source` interface similarly decouples streamer logic from the
CSV file for unit testing.

---

## Installation

### Prerequisites

| Tool | Minimum version | Install |
|---|---|---|
| Go | 1.25 | https://go.dev/dl |
| Docker | 24 | https://docs.docker.com/get-docker |
| kubectl | 1.29 | https://kubernetes.io/docs/tasks/tools |
| Helm | 3.14 | https://helm.sh/docs/intro/install |
| minikube **or** kind | latest | see below |

---

### Option A — minikube

```bash
# macOS
brew install minikube

# Linux
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

Start a cluster with enough resources for all services:

```bash
minikube start --cpus=4 --memory=6g --disk-size=20g
```

Verify:

```bash
minikube status
kubectl get nodes
```

---

### Option B — kind

```bash
# macOS
brew install kind

# Linux
go install sigs.k8s.io/kind@latest
```

Create a cluster:

```bash
kind create cluster --name gpqueue
kubectl cluster-info --context kind-gpqueue
```

> **Note:** kind does not provide an image registry by default. After `kind create cluster`, load
> images with `kind load docker-image <image>:<tag> --name gpqueue` instead of the
> `minikube-build` Make target. Build images normally with `docker build`, then load each one:
>
> ```bash
> docker build -f docker/Dockerfile.mq        -t gpqueue-mq:latest        .
> docker build -f docker/Dockerfile.streamer  -t gpqueue-streamer:latest  .
> docker build -f docker/Dockerfile.collector -t gpqueue-collector:latest .
> docker build -f docker/Dockerfile.gateway   -t gpqueue-gateway:latest   .
> docker pull timescale/timescaledb:latest-pg16
>
> kind load docker-image gpqueue-mq:latest        --name gpqueue
> kind load docker-image gpqueue-streamer:latest  --name gpqueue
> kind load docker-image gpqueue-collector:latest --name gpqueue
> kind load docker-image gpqueue-gateway:latest   --name gpqueue
> ```
>
> Then run `helm upgrade --install telemetry ./helm/telemetry` to deploy.

---

### Deploy to minikube (recommended path)

The Makefile builds all images directly inside minikube's Docker daemon (no registry push
required) and then installs the Helm release in one step.

**1. Clone the repository**

```bash
git clone <repo-url>
cd gpqueue
```

**2. Start minikube** (if not already running)

```bash
minikube start --cpus=4 --memory=6g --disk-size=20g
```

**3. Build images and deploy**

```bash
make minikube-deploy
```

This runs `docker build` for all four services inside minikube's daemon, pulls the
TimescaleDB image, and then runs `helm upgrade --install telemetry ./helm/telemetry`.

**4. Verify all pods are running**

```bash
kubectl get pods
```

Expected output (all `Running`):

```
NAME                                    READY   STATUS    RESTARTS   AGE
telemetry-collector-<hash>              1/1     Running   0          60s
telemetry-collector-<hash>              1/1     Running   0          60s
telemetry-gateway-<hash>                1/1     Running   0          60s
telemetry-mq-<hash>                     1/1     Running   0          60s
telemetry-postgres-<hash>               1/1     Running   0          60s
telemetry-streamer-<hash>               1/1     Running   0          60s
```

> The collector pods may restart once or twice while they wait for PostgreSQL to become ready —
> this is expected and they will settle once the DB is up.

**5. Open the API Gateway**

```bash
make minikube-open-gateway
```

This runs `minikube service telemetry-gateway` and opens the NodePort URL in your browser.
Navigate to `/docs` for the Swagger UI.

---

### Tear down

```bash
make minikube-undeploy          # removes the Helm release; PVCs are kept
minikube delete                 # destroys the cluster entirely
```

---

## Configuration

All services are configured via environment variables. Defaults are set in
`helm/telemetry/values.yaml` and injected via a Kubernetes `ConfigMap`.

### Message Queue

| Variable | Default | Description |
|---|---|---|
| `MQ_PORT` | `8080` | HTTP listen port |
| `MQ_HIGH_WATER_MARK` | `100000` | Max ring buffer depth before HTTP 429 backpressure |
| `MQ_WAL_PATH` | `/data/wal.log` | WAL file path (mounted from PVC) |
| `MQ_MAX_RETRIES` | `3` | Nack retry limit before dead-letter routing |
| `MQ_POLL_TIMEOUT_MS` | `5000` | Long-poll max wait (ms) |

### Streamer

| Variable | Default | Description |
|---|---|---|
| `STREAMER_ID` | `streamer-0` | Unique instance name |
| `CSV_PATH` | `/data/dcgm_metrics.csv` | Path to DCGM CSV file (bundled in image) |
| `PUBLISH_INTERVAL_MS` | `100` | Sleep between publishes (ms) |
| `TOPIC_NAME` | `telemetry` | Queue topic |
| `QUEUE_ADDR` | `http://localhost:8080` | Queue service address |

### Collector

| Variable | Default | Description |
|---|---|---|
| `COLLECTOR_ID` | `collector-0` | Unique instance name |
| `CONSUMER_GROUP` | `telemetry-collectors` | Consumer group ID |
| `TOPIC_NAME` | `telemetry` | Queue topic |
| `QUEUE_ADDR` | `http://localhost:8080` | Queue service address |
| `DB_DSN` | `postgres://postgres:postgres@localhost:5432/telemetry?sslmode=disable` | PostgreSQL DSN |
| `BATCH_SIZE` | `50` | Max messages per poll |
| `DLQ_TOPIC` | `telemetry-dlq` | Dead-letter topic |
| `MAX_RETRIES` | `3` | Retry limit before DLQ |

---

## Usage

### API Gateway endpoints

Once the gateway is running, all endpoints are reachable at the NodePort URL printed by
`minikube service telemetry-gateway`. Use `make minikube-port-forward` to expose the MQ
directly at `http://localhost:8080` instead.

#### List all GPUs

```bash
curl http://<gateway-url>/api/v1/gpus
```

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

#### Get telemetry for a GPU

```bash
curl "http://<gateway-url>/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry"
```

**Query parameters:**

| Parameter | Description |
|---|---|
| `start_time` | ISO 8601 lower bound on `processed_at` (inclusive) |
| `end_time` | ISO 8601 upper bound on `processed_at` (inclusive) |
| `metric` | Filter by DCGM metric name (e.g. `DCGM_FI_DEV_GPU_UTIL`) |
| `limit` | Page size — default `100`, max `1000` |
| `cursor` | Opaque cursor from the previous response's `next_cursor` field |

**Time-range + metric filter example:**

```bash
curl "http://<gateway-url>/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry\
?start_time=2025-07-18T20:42:34Z\
&end_time=2025-07-18T20:42:37Z\
```

**Paginate with cursor:**

```bash
# First page
curl "http://<gateway-url>/api/v1/gpus/<uuid>/telemetry?limit=100"

# Subsequent page — pass next_cursor from the previous response
curl "http://<gateway-url>/api/v1/gpus/<uuid>/telemetry?limit=100&cursor=<next_cursor>"
```

#### Health and documentation

| Endpoint | Description |
|---|---|
| `GET /healthz` | Liveness probe — returns `200 OK` |
| `GET /readyz` | Readiness probe — checks DB connection |
| `GET /docs` | Swagger UI (auto-generated from handler annotations) |
| `GET /openapi.json` | Raw OpenAPI 3.x specification |

---

### Message Queue API

Expose the queue locally:

```bash
make minikube-port-forward   # binds http://localhost:8080
```

| Endpoint | Description |
|---|---|
| `GET /topics` | List all topics and their current depth |
| `POST /topics/{topic}/messages` | Publish a message |
| `GET /topics/{topic}/messages?group={g}&limit={n}` | Long-poll fetch |
| `POST /topics/{topic}/commit` | Commit consumer group offset |
| `POST /topics/{topic}/nack` | Nack a message (retry or DLQ) |
| `GET /metrics` | Prometheus metrics (queue depth, publish rate, consumer lag) |
| `GET /healthz` | Liveness probe |

```bash
# Check topic depths
curl http://localhost:8080/topics

# Check Prometheus metrics
curl http://localhost:8080/metrics
```

---

### Scaling

Scale streamer or collector instances at runtime without downtime:

```bash
# Scale streamer to 3 instances
kubectl scale deployment telemetry-streamer --replicas=3

# Scale collectors to 5 instances
kubectl scale deployment telemetry-collector --replicas=5
```

Valid ranges: streamer `1–10`, collector `1–10`. The message queue handles fan-in and
competing-consumer load balancing automatically.

---

### Connecting to PostgreSQL directly

```bash
make minikube-port-forward-postgres   # binds localhost:5432
psql "postgres://postgres:postgres@localhost:5432/telemetry"
```

---

### Running tests locally

```bash
go test ./...          # all unit tests
go vet ./...           # static analysis
go build ./cmd/...     # compile all binaries
```

### Test coverage

Print a per-function coverage summary to the terminal:

```bash
make cover
```

Open an interactive HTML report in the browser (highlights which lines are hit or missed):

```bash
make cover-html
```

---

## Project Structure

```
gpqueue/
├── cmd/
│   ├── mq/              # Message queue service binary
│   ├── streamer/        # Streamer binary
│   ├── collector/       # Collector binary
│   └── gateway/         # API gateway binary
├── internal/
│   ├── model/           # Shared Go structs (TelemetryRecord, GPU, Message)
│   ├── mq/
│   │   ├── server/      # HTTP handlers and long-poll dispatch
│   │   ├── ringbuffer/  # In-memory ring buffer
│   │   ├── wal/         # Write-ahead log
│   │   └── offset/      # Consumer group offset store
│   ├── mqclient/        # HTTP client used by streamer and collector
│   ├── source/          # Streamer data source interface + CSV implementation
│   ├── storage/         # PostgreSQL repository (read + write)
│   ├── csv/             # CSV parser
│   └── api/             # REST handlers with huma annotations
├── helm/telemetry/      # Helm chart for all services
├── docker/              # Multi-stage Dockerfiles for each binary
├── db/                  # SQL schema (init.sql)
├── data/                # Sample DCGM CSV file
└── Makefile             # Build, deploy, and ops targets
```
