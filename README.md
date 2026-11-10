# Distributed Telemetry Collector

A systems-engineering project: build a small Go collector for OpenTelemetry
logs, with explicit queueing, backpressure, durability, and recovery behavior.

The collector will receive OTLP/gRPC logs, validate and admit them into a
bounded pipeline, and export them to Grafana Loki. Prometheus metrics and
Grafana dashboards will make the collector's behavior visible. A custom
append-only write-ahead log (WAL) is planned for the reliability phase.

## Development environment

The project uses Go and Docker Compose. Protocol Buffers are consumed through
the official OpenTelemetry Go protocol packages, so generating `.pb.go` files
is not part of the initial setup.

Check the local tools:

```sh
go version
docker version
docker compose version
docker info
```

If Docker commands cannot connect to the engine, select the Docker Desktop
context and retry:

```sh
docker context use desktop-linux
docker info
```

The project module is initialized. As dependencies are added during Phase 1,
`go mod tidy` will resolve and record them.

## Current status

See [ROADMAP.md](ROADMAP.md) for the milestones, acceptance criteria, and the
delivery guarantees this project intends to provide.
