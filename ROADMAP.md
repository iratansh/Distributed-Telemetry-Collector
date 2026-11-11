# Development roadmap

## Product direction

Build a Go telemetry collector with explicit bounds and failure behavior. The
current delivery pipeline ingests OpenTelemetry logs over OTLP/gRPC and exports
them to Loki over OTLP/HTTP. Optional Kubernetes API collection publishes
cluster state metrics to Prometheus. The WAL, bounded queue, worker pool,
checkpointing, and retry behavior are implemented in this project; standard
protocols and established monitoring tools provide interoperability.

## Current guarantees

- The receiver validates requests and bounds queued work and pending bytes.
  Saturation returns `RESOURCE_EXHAUSTED` with retry advice.
- An accepted request is appended to the WAL and synchronized according to the
  configured `WAL_SYNC` policy before the receiver acknowledges it.
- Delivery is at least once. If Loki accepts a batch and the collector stops
  before its WAL checkpoint is durable, the batch can be replayed and duplicated.
- Worker concurrency means global delivery ordering is not guaranteed.
- Kubernetes collection is optional and periodic. Its snapshots describe pod,
  deployment, and node state; they do not carry application logs.

## Milestones

### 0. Local project baseline — complete

- Go module, local Compose stack, sample producer, and documentation.
- Loki, Prometheus, and Grafana with a provisioned dashboard.

### 1. Functional collector — complete

- OTLP/gRPC logs receiver, validation, admission limits, and bounded worker pool.
- Batching, Loki OTLP/HTTP export, timeouts, bounded retries, and backpressure.
- Health, readiness, Prometheus metrics, graceful shutdown, and local pprof.

### 2. Durability and recovery — complete

- Versioned, framed WAL records with checksums, append, configured filesystem
  synchronization, corrupt-tail truncation, startup replay, and compaction.
- WAL **checksums** validate that an individual frame is intact. WAL
  **checkpoints** record that a request was successfully exported, so startup
  can skip it. They solve different problems: a valid checksum does not say
  whether Loki received the request.
- Successful downstream batches write their checkpoint frames together and
  wait for one sync. Concurrent appends can also share a group sync.
- Bounded retry and shutdown behavior preserves at-least-once semantics.

**Verification:** tests cover checksums, truncation/corruption, replay,
checkpointing, compaction, retries, and shutdown. A crash after export but
before a durable checkpoint can produce duplicates; delivery is not exactly
once.

### 2.5 Kubernetes metrics integration — complete and live-verified

- Optional Kubernetes API collection for pod phase/counts, container restart
  counts, deployment desired/ready/available/updated replicas, node readiness,
  and node allocatable CPU and memory.
- Metrics use the existing Prometheus `/metrics` endpoint. Collection defaults
  to a 30-second interval and can be configured; API errors do not block log
  ingestion. Node allocatable values are capacity, not live utilization.
- RBAC and local kubeconfig configuration are documented in the README and
  `deploy/kubernetes/rbac.yaml`.

**Live check:** a local Minikube cluster ran a two-replica nginx deployment.
The collector reported two Running pods, zero restarts, two available replicas,
and one Ready node. The configured interval was not a measurement of snapshot
latency.

### 3. Performance and resource limits — complete

- Repeatable paced and unpaced OTLP/gRPC load driver reports accepted events/sec,
  request P50/P95/P99, RPC errors, and backpressure. It samples Prometheus queue
  depth, pending bytes, Go heap, goroutines, and process resident memory.
- Go benchmarks cover in-memory and WAL-backed pipeline admission, individual
  append/ack, and batched append/checkpoint behavior.
- Recovery benchmark writes durable pending records in a child process, exits
  without closing the WAL, then measures startup reconstruction, replay through
  the worker pool, and WAL checkpointing.
- CPU profile baseline is documented. The largest sampled costs were system
  calls, runtime condition waits, and GC scanning.

**Local baseline:** Apple M2 Pro, darwin/arm64, Go 1.26.2, 10 CPUs,
GOMAXPROCS=10. A paced 10,000-event run at 2,000 offered events/sec accepted
all events at 2,017.9 events/sec, with request P50/P95/P99 of 6.18/7.49/8.68
ms. Across 100 Prometheus samples, maximum queue depth was 1, pending bytes
59,330, Go heap 10.8 MB, process RSS 43.4 MB, and goroutines 23. A separate
unpaced 50,000-event burst accepted 16,000 events and rejected 340 requests
with `RESOURCE_EXHAUSTED`.

After an unclean child-process exit, a 10,000-event WAL scan and reconstruction
took 21.4 ms. Replay and checkpointing all 10,000 events took 1.64 seconds
(6,084 events/sec) with an immediate-success test exporter. The replay result
excludes real backend latency and retries. In a WAL microbenchmark, checkpointing
eight concurrently appended records as a batch took 10.3 ms, compared with
43.1 ms when checkpointing each record individually. These are local results,
not portable capacity guarantees. Commands and measurement details are in the
README.

### 4. Hardening and project polish — complete for local development

- GitHub Actions checks formatting, `go vet`, race-enabled tests, and package
  builds on pushes and pull requests. It does not publish or deploy the app.
- Fault tests cover corrupt and truncated WAL tails, forced process termination,
  unacknowledged replay, duplicate-delivery windows, retries, saturation, and
  shutdown deadlines.
- Local operation, Prometheus signals, Kubernetes kubeconfig/RBAC, durability
  guarantees, and benchmark limitations are documented in the README.
- No production deployment, release, or orchestration milestone is planned.

Re-run the load and recovery measurements when changing collector defaults, the
WAL format, or checkpoint behavior.
