# Development roadmap

## Product direction

Build the systems-engineering version of a telemetry collector. Implement the
bounded processing pipeline and WAL ourselves; use standard OpenTelemetry
protocols and established tools for transport, monitoring, visualization, and
deployment. Start with logs only.

## Initial behavior and guarantees

- Ingest OpenTelemetry logs over OTLP/gRPC using the official protocol types.
- Bound in-memory work and reject new requests with `RESOURCE_EXHAUSTED` when
  capacity is exhausted. Keep request and record size limits configurable.
- Export logs to Loki over OTLP/HTTP with deadlines and retry backoff.
- Target at-least-once delivery after the WAL is implemented. A crash after
  export but before checkpointing can cause replay and duplicates.
- Do not promise global ordering across concurrent workers.
- Only claim durable acceptance after the WAL record has reached the configured
  sync point. The exact failure model and sync policy must be documented.
- Expose collector health and performance metrics to Prometheus; use Grafana
  for dashboards. Add pprof for local profiling.

## Milestones

### 0. Local project baseline — done

- Initialize the Go module and repository documentation.
- Use the installed Go toolchain and Docker Desktop engine.
- Keep runtime data and local secrets out of Git.

### 1. Functional, in-memory collector — implemented

1. Add a small configuration layer and a runnable `cmd/collector` entry point.
2. Implement the OTLP/gRPC logs receiver with validation and admission limits.
3. Add a bounded queue and a worker pool with cancellation and graceful shutdown.
4. Batch records and export them to Loki over OTLP/HTTP, with timeouts and
   bounded retry backoff.
5. Add a local Compose stack for Loki, Prometheus, and Grafana, plus a minimal
   dashboard and a sample log producer.
6. Expose collector health, queue depth, received/exported counts, failures,
   and ingestion latency.

**Acceptance:** A local producer sends logs through the collector and they can
be queried in Loki. Queue saturation has a defined response, and graceful
shutdown behavior is observable. This phase is in-memory; a process crash can
lose accepted work, and that limitation must be explicit.

### 2. Durability and recovery — implemented and verified

1. Define a versioned WAL record format with length framing and checksums.
2. Implement append, configured synchronization, startup replay, and corrupt
   tail handling.
3. Ensure the receiver acknowledges only after the configured durable append.
4. Checkpoint successfully exported records; compact acknowledged history safely.
5. Add bounded retries and graceful shutdown/restart behavior around replay.

**Acceptance:** Under the documented failure model, every acknowledged record
is available for delivery after a forced process restart. Tests cover truncation,
corruption, replay, duplicate delivery windows, and shutdown. Delivery remains
at-least-once, not exactly-once.

## Backlog

- Group multiple WAL appends under one filesystem sync to reduce sync overhead,
  while bounding the added acknowledgement delay and documenting the durability
  tradeoff.
- Add Kubernetes metrics collection to the Go telemetry collector. For example,
  implement a receiver that uses the Kubernetes API to collect pod status,
  restart counts, and deployment availability, then sends those metrics through
  the existing telemetry pipeline. This demonstrates Kubernetes API
  integration alongside the collector's reliability mechanisms.

### 3. Performance and resource limits

- Add a repeatable load driver and Go benchmarks for ingestion and WAL paths.
- Measure throughput, P95/P99 latency, memory, queue depth, and recovery time.
- Profile CPU, heap, goroutines, and blocking before optimizing.
- Record baseline and improved results with machine and configuration details.

**Acceptance:** Results are reproducible from documented commands, and each
optimization is supported by measurements.

### 4. Hardening and project polish

- Exercise concurrency with the race detector and failure injection.
- Document architecture, configuration, failure model, operational signals,
  and known tradeoffs.
- Add CI for formatting, static analysis, and the relevant correctness checks.
- Keep deployment local and simple; consider orchestration only after the core
  reliability behavior is solid.

## First implementation slice

Start Phase 1 with the receiver-to-queue path and explicit saturation behavior.
Then connect one worker to a Loki exporter before adding batching and multiple
workers. This keeps the first vertical slice small enough to debug while making
the pipeline measurable from the beginning.
