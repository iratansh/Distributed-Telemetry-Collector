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

Limitations:
Loki Docs state that the max log-line size is 256KB, the consumer must align with that

WAL_SYNC=True: Collector writes the request and asks the OS to sync the file before acknowledging the request.

WAL_SYNC=False: Collector writes the request but doesn't wait for confirmation. Faster but less reliable, a machine or power failure could lose recent writes.

Design Decisions:

WAL writes: WAL uses a mutex so one request at a time appends its reocrds, and when sync is enabled, waits for that write's sync to finish before the next append can proceed. This can add latency especially when sync is on.  Requests can be handled concurrently in following parts of the collorector, and workers and export batches concurrently. It's the WAL append step that takes turns. Optimization I'm choosing: Group several appends under one sync, trading a small delay before acknowledging requests for fewer sync operations.

1 WAL Write File: This WAL write file will contian both request appends and request ACKS. At startup the collector scans the file, remembers each DATA record then removes that sequence from its recovery set when it sees a matching ACK. The ACK itself means that the downstream export succeeded for that request. The idea behind ACK is that it makes the entire process more reliable; if Loki accepts the request but before its ACK is safely written, the collector can replay it.

At least once processing: In the scenario above, this will most likely lead to a duplicate. In the observability world, we follow the mindset of "avoid losing data". For exactly once processing this would come with high overhead.

Check-sums: This is implemented in the WAL layer of things; when the collector is started and the collector reads the write file. It does a checksum run through the file. This is for data validation. If for instance the collector shut down mid way through a write: case 1 - the checksum will have already been computed for the record and written to the log file so the checksum recomputes the checksum for the record sees that the two vals dont match and truncates this record to the end of the file since it can't tell exactly where the malformed record ends. case 2 - the checksum itself is partially written,  then the record is simply incomplete and the WAL reader can't read the full record body so it truncates the file at that record's start.

Kubernetes API: This API exposes object state and doesn't push OTLP telemetry so it must be handled differently. I propose having the collector periodically list pods and deployments, and expose snapshot metrics at /metrics :8888 for Prometheus to scrape and store them. The tradeoff with this is that while it does work the metrics aren't in real-time? However these metrics are about K8 state and not logs: the collector periodically reads pod and deployment status and updates gauges. These aren't real-time events. 

Checkpointing: Check-sums check whether the WAL record is intact, checkpointing is determing whether the requests have already been exported. Do not replay checkpointed requests during recovery. On recovery the collector while remove checkpointed records from its in-mem pending set. At-least-once edge case: if the downstream accepts a batch but the collector crashes before its checkpoint is durable, that batch may be replayed and delivered twice.
