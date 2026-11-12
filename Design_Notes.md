# Design notes

## Project scope

This is a local-development Go collector for studying bounded ingestion,
concurrency, durability, and at-least-once delivery. It is not intended to be
deployed as a production service, and the repository does not include a
production release or deployment pipeline.

## Data flows

### Application logs

```text
OTLP producer
  -> OTLP/gRPC receiver and validation
  -> WAL append and configured filesystem sync
  -> bounded queue and worker pool
  -> batched OTLP/HTTP export with bounded retries
  -> Grafana Loki
```

Prometheus scrapes the collector's `/metrics` endpoint. Grafana can display
collector metrics and query logs stored in Loki.

### Kubernetes state metrics

The optional Kubernetes collector periodically lists pods, deployments, and
nodes through the Kubernetes API. It updates Prometheus metrics at the same
`/metrics` endpoint. This is a periodic state snapshot, not an OTLP log stream
or a source of real-time events. The default collection interval is 30 seconds.
Node allocatable CPU and memory describe schedulable capacity, not live usage.

The feature was exercised locally against a Minikube nginx Deployment. The
collector process ran on the host using a kubeconfig; the collector itself is
not deployed into the cluster.

## Design decisions and tradeoffs

### OTLP/gRPC and Protobuf for ingestion

The receiver uses the standard OTLP logs service and generated Protobuf types.
This lets standard telemetry producers send logs without a project-specific
wire format. The tradeoff is that this implementation currently accepts logs;
metrics and traces do not share the receiver's ingestion path.

### Bounded queue and fixed worker pool

The queue, pending-byte budget, and worker count put explicit limits on work in
memory and concurrent exports. When capacity is exhausted, the collector
rejects new requests with `RESOURCE_EXHAUSTED` so the producer can retry. This
protects memory at the cost of requiring producers to handle backpressure; a
larger queue would absorb longer bursts but also increase memory use and
potential wait time. Worker concurrency improves throughput, but separate
workers can export batches out of order.

### WAL before acknowledgement

The receiver writes accepted requests to the WAL before returning success. With
`WAL_SYNC=true`, the receiver waits for filesystem synchronization, improving
survival across process, host, or power failure at the cost of disk I/O latency.
Group sync lets concurrent records share that I/O; the configured sync interval
adds a small bounded wait in exchange for fewer sync operations. Disabling sync
reduces acknowledgement latency but weakens durability.

The WAL stores data and checkpoints in one append-only file. This keeps startup
recovery ordered and straightforward, while requiring compaction to reclaim
acknowledged history. Checksums detect damaged frames; checkpoints record
successful delivery. A checksum cannot replace a checkpoint because record
integrity says nothing about whether Loki accepted the record.

### At-least-once delivery and bounded retries

The collector checkpoints a batch only after a successful downstream response.
If it stops after Loki accepts the batch but before the checkpoint is durable,
the batch is sent again after restart. This duplicate window favors avoiding
data loss over claiming exactly-once delivery. Retry limits and jitter help
recover from transient errors while bounding retry time and avoiding unlimited
retry load. Permanent failures and exhausted retries are counted as drops.

### Kubernetes metrics are snapshots

The Kubernetes API collector lists object state on an interval and updates
Prometheus gauges. Polling is simple and exposes current state without adding a
streaming watch lifecycle, but changes can remain unseen until the next
collection. The default interval is 30 seconds. These metrics describe cluster
state; application logs continue through OTLP independently.

### Prometheus scrape path for collector metrics

The collector exposes its operational and Kubernetes metrics in Prometheus
format for Prometheus to scrape. This keeps collector health, queue, resource,
and Kubernetes state metrics separate from the log export path to Loki. It also
means metric freshness depends on both the Kubernetes collection interval and
Prometheus's scrape interval.

## WAL design and delivery semantics

The WAL is an append-only file containing framed data and acknowledgement
records. Each frame includes a length, version, type, sequence number, payload,
and checksum.

- A checksum detects an incomplete or damaged frame. Recovery trusts the valid
  prefix and truncates from the first malformed frame, since later bytes cannot
  be safely interpreted as a continuous append-only log.
- A checkpoint (acknowledgement) records that a request was successfully
  exported. During recovery, data records without a matching checkpoint are
  replayed. Checksums establish integrity; checkpoints establish delivery
  progress.
- With `WAL_SYNC=true`, the receiver acknowledges only after a filesystem sync
  includes the new data record. Concurrent appends can share one group sync,
  bounded by `WAL_SYNC_INTERVAL` and `WAL_SYNC_MAX_BATCH`.
- After an export batch succeeds, its acknowledgement frames are appended
  together and covered by one sync. This reduces checkpoint sync overhead.
- If the downstream accepts a batch but the collector stops before its
  checkpoint is durable, the batch may be replayed and duplicated. Delivery is
  at least once, not exactly once.
- With `WAL_SYNC=false`, an acknowledged append may be lost after a host or
  power failure because the collector did not wait for filesystem durability.

## Bounds, retries, and concurrency

The collector reserves request queue capacity and an encoded-protobuf byte
budget before appending to the WAL. The append happens outside the pipeline
state lock so concurrent requests can participate in group sync. The fixed
worker pool batches requests and exports them concurrently. When the queue or
byte budget is full, new requests receive `RESOURCE_EXHAUSTED`; memory use does
not grow without bound. The byte budget does not include Go object overhead.

HTTP transport errors and selected retryable status codes use bounded
exponential backoff with jitter. `Retry-After` is honored up to the configured
maximum. Permanent errors or exhausted retries are counted as dropped. A
network timeout can leave the downstream result uncertain, so retrying may
also produce duplicates.

## Local verification and performance

The test suite covers WAL corruption and truncation, forced process restarts,
replay and checkpointing, retry behavior, queue saturation, partial success,
and graceful or forced shutdown. `make race`, `make vet`, and the GitHub Actions
workflow check concurrency and build correctness. The local Docker Compose
smoke test verifies that sample logs reach Loki.

On the documented Apple M2 Pro baseline, a 60-second paced local run accepted
120,000 events at about 2,002 events/sec with an OTLP request P99 of 17.28 ms.
All events were exported and pending work drained to zero. A simulated unclean
exit followed by replay and checkpointing recovered 10,000 events in about
1.64 seconds using an immediate-success test exporter. These are
machine-specific measurements; real backend latency and retries affect
recovery delivery time. See `README.md` for settings and full measurement
details.
