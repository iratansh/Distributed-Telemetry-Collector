Design Challenges:

* Backpressure: What happens when producers generate events faster than consumers can process them?
* Durability: Can the service recover buffered events after an unexpected crash?
* Concurrency: Can multiple workers process events safely without leaking goroutines?
* Delivery guarantees: Can you provide at-least-once delivery, and how do you handle duplicates?
* Performance: How do batch sizes and worker counts affect throughput, latency, and memory?


Workflow:

Application Logs / Producers -> Go Ingestion Service -> Bounded Queue + Worker Pool -> WAL / Disk Buffer

				    -> Exporter


Considerations:
Rate limiting

Buffer size

Durable Acknowledgement

Replay ordering

Retry Behavior


Technologies:

| Core service     | Go                          | Goroutines, channels, concurrency, networking, profiling |
| ---------------- | --------------------------- | -------------------------------------------------------- |
| Ingestion API    | gRPC                        | Efficient RPC and streaming support                      |
| Serialization    | Protocol Buffers            | Compact, typed message representation                    |
| Telemetry format | OpenTelemetry / OTLP        | Industry-standard interoperability                       |
| Processing       | Custom Go worker pool       | Demonstrates concurrency and backpressure                |
| Buffering        | Bounded Go channels / queue | Explicit memory limits and flow control                  |
| Durability       | Custom append-only WAL      | Crash recovery, replay, storage correctness              |
| Monitoring       | Prometheus                  | Throughput, latency, queue depth, failures               |
| Visualization    | Grafana                     | Dashboards and operational diagnostics                   |
| Log destination  | Grafana Loki                | Realistic downstream telemetry backend                   |
| Deployment       | Docker Compose              | Reproducible local environment                           |
| Benchmarking     | Go benchmarks + k6          | Performance measurement and load testing                 |
| Testing          | Go testing + race detector  | Concurrency correctness and regression tests             |


My Solution:


Ingestion receiver

Validation · Admission control · Request limits

Durable append-only log (WAL)

Append · Commit · Replay · Checkpoints

Bounded queue + worker pool

Batching · Backpressure · Retry scheduling

OTLP/HTTP exporter

Delivery · Timeouts · Retry with backoff

Grafana Loki

Stores and indexes log metadata for querying

Grafana dashboards

Search logs and inspect collector health
