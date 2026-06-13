# AI Usage Log — GPU Telemetry System Design

This document captures every prompt submitted to Claude (claude.ai - only AI assitant used) during the design session
for the GPU Telemetry System, with a focus on the custom Message Queue service. Prompts are
listed in chronological order with the intent and outcome of each exchange.

---

## Session Overview

| Item | Detail |
|---|---|
| **Tool** | Claude (claude.ai) — Claude Sonnet 4.6 |
| **Session goal** | Design a GPU telemetry pipeline with a custom message queue and produce a `claude.md` project specification |
| **Total prompts** | 10 |
| **Primary artefacts produced** | `claude.md`, `architecture.png`, `AI-usage.md` |

---

## Prompt 1

**Prompt:**
> how message queuing service connects telemetry streamer with telemetry collector.

**Intent:**
Understand the architectural role of a message queue as the decoupling layer between data
producers (streamers) and data consumers (collectors).

**Outcome:**
Claude produced an interactive architecture diagram and an explanation covering the three
layers — streamers as producers, the broker with named topics and offset tracking, and
collectors as competing consumers. Key concepts introduced: temporal decoupling, fan-out,
at-least-once delivery, durable storage, and backpressure.

---

## Prompt 2

**Prompt:**
> what is the production criteria to build a scalable and high available message queue?

**Intent:**
Establish the non-negotiable engineering requirements that a production-grade message queue
must meet for both scalability and high availability.

**Outcome:**
Claude produced an interactive pillar card UI covering seven production criteria: data
partitioning & sharding, replication & durability, leader election & failover, throughput &
backpressure, delivery semantics, observability, and security & access control. Each pillar
included concrete configuration recommendations (e.g. `replication.factor=3`,
`min.insync.replicas=2`, `acks=all`). Cross-cutting notes covered the HA vs. scale tradeoff
table and the mandatory chaos testing schedule.

---

## Prompt 3

**Prompt:**
> Create me a claude.md file with all the description that i provided above about the project.

**Intent:**
Consolidate the architectural discussion from Prompts 1 and 2 into a structured project
specification document that can be committed to the repository.

**Outcome:**
Claude produced the first version of `claude.md` covering: project overview, the three-layer
pipeline (streamer → queue → collector), message queue design considerations, delivery
semantics, dead-letter queue, observability, and technology recommendations. The document
included the telemetry record schema, scaling rules, and a non-goals section.

---

## Prompt 4

**Prompt:**
> Let's design a basic claude.md file with below description:
>
> Designing a telemetry system which will have these main components:
>
> * Telemetry Streamer which reads telemetry from CSV and streams it periodically over the
>   custom message queue. The implementation should support the ability to dynamically scale
>   up/down the number of Streamers.
>   NOTE: The data to be streamed is provided as a CSV file. Each line in the CSV is an
>   independent telemetry datapoint. This data can be streamed in a loop to simulate continuous
>   stream of telemetry. The time at which a specific telemetry log is processed should be
>   considered as the timestamp of that telemetry.
>
> * Telemetry Collector: consumes telemetry from the custom message queue, parses and persists
>   it. The implementation should support the ability to dynamically scale up/down the number
>   of Collectors.
>
> * API Gateway: REST API exposing telemetry. The OpenApi spec for the REST API should be
>   auto generated
>
> * Messaging Queue: Messaging system will be used to connect the streamers with the
>   collectors, this is something you will need to implement either as a library or as a
>   service. Please spend some design cycles on thinking about scale, performance and
>   availability of this system. For the exercise we will not scale the nodes beyond 10
>   instances for the streamer/collector.

**Intent:**
Replace the generic first `claude.md` with a clean, requirements-driven specification that
precisely matches the project brief — four named components, explicit scaling constraints
(max 10 instances), wall-clock timestamp rule, and auto-generated OpenAPI.

**Outcome:**
Claude produced a fully restructured `claude.md` with four dedicated component sections
(Streamer, Collector, API Gateway, Message Queue), a data flow diagram, a telemetry record
schema, a scaling rules table, and a non-goals section. The message queue section included a
design considerations table and presented both embedded-library and standalone-service
implementation options.

---

## Prompt 5

**Prompt:**
> Also API gateway should expose three endpoints:
>
> * GET /api/v1/gpus
> * GET /api/v1/gpus/{id}/telemetry
> * GET /api/v1/gpus/{id}/telemetry?start_time=...&end_time=...
>
> Programming Language: Golang
> Deployment: Docker + Kubernetes
> Deployment Tooling: Helm


**Intent:**
The exact API endpoints the Gateway must expose, and the full technology
stack — Go, Docker, Kubernetes, Helm.

**Outcome:**
Claude read and analysed the CSV file, then produced a heavily updated `claude.md` adding:
- Three exact API endpoint specifications with request parameters and JSON response examples
- Idiomatic Go project structure (`cmd/`, `internal/`)
- Helm `values.yaml` knobs for all four components
- HPA definition for collector auto-scaling on consumer lag
- Multi-stage Docker build pattern

---

## Prompt 6

**Prompt:**
> Let message queue implemented as standalone service which can also be deployed on k8s cluster.
> Update the final claude.md file, rest looks good.

**Intent:**
Lock in the Message Queue implementation choice: it must be a **standalone HTTP service** with
its own binary, container image, Kubernetes Deployment, and PersistentVolumeClaim — not an
embedded library.

**Outcome:**
Claude made six targeted edits to `claude.md`:
- Rewrote the Message Queue section as a standalone HTTP/1.1 service with a 7-endpoint wire
  protocol table, publish/fetch JSON body examples, and a `QueueClient` Go interface
- Added Kubernetes deployment notes (ClusterIP Service, PVC for WAL + offsets, probes)
- Added `MQ_*` environment variable config block
- Updated the data flow diagram with HTTP verb annotations on arrows
- Expanded the project structure with `cmd/mq/`, `internal/mq/{server,ringbuffer,wal,offset}/`,
  `internal/mqclient/`, `Dockerfile.mq`, and three new Helm templates
- Updated the Helm `values.yaml` with a full `mq:` stanza and `queueAddr` in streamer/collector

---

## Prompt 7

**Prompt:**
> csv data for streaming can be the initial data source feature rest should be added future scope.

**Intent:**
Correct the framing from Prompt 6 — CSV is not a "reference implementation" but the actual
**v1 feature**. Other sources are explicitly future scope and should be documented as such,
not implied as already pluggable.

**Outcome:**
Claude made four targeted edits:
- **Overview** — CSV named as the initial (v1) data source; additional sources explicitly
  noted as future scope with a link to the new section
- **Data Source heading** — renamed from `(Reference Implementation)` to `(v1)`
- **Streamer section** — reverted to concrete CSV-only behaviour; `SOURCE_TYPE`/`SOURCE_URI`
  replaced with simple `CSV_PATH`
- **Non-Goals → Future Scope split** — Non-Goals kept as-is; a new **Future Scope** section
  added with a table of planned additional Streamer sources (HTTP scrape, gRPC stream,
  Kafka bridge, WebSocket) and other planned improvements (auth, TLS, multi-node queue,
  schema registry, retention)

---

## Prompt 8

**Prompt:**
> Create me an architecture diagram in PNG which can be used in README.md file

**Intent:**
Produce a publication-quality architecture diagram that visually represents the full
pipeline and can be embedded directly in the repository README.

**Outcome:**
Claude generated `architecture.png` (419 KB, 180 DPI, 3600×1980 px) using matplotlib,
depicting the full left-to-right pipeline with six colour-coded component zones (Data
Source, Streamer, Message Queue, Collector, Storage, API Gateway), annotated arrows with
HTTP verbs, internal Message Queue detail panels (HTTP API, ring buffer/WAL/offset
internals, topics), HPA labels on streamer and collector, a Helm manifest footer, and a
colour legend. Dark theme suitable for GitHub README rendering.


Above Prompts are taken from the claude session.

---

Once the CLAUDE.md file is structured and created. I used claude code on visual studio for below development processes:

* Bootstrapped the project structure.
* Bootstrapped the code for `cmd/`, `internal/`, `helm/`, `docker`, `db/`, `Makefile`.
* Developed unit test cases.

--- 

Manual Effort:
* Minikube install and setup the local k8s cluster for inital test.
* Updated the README.md fixes.
* Created the Dockerfile.
* Few helm chart fixes.
* After the initial instance of mq services deployed, troubleshooted the network issue between the services.