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
Kafka -> used for producer

Go -> processor 

Grafana -> Visualization
