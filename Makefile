.PHONY: run produce load build fmt vet test race bench recovery up down smoke

run:
	go run ./cmd/collector
produce:
	go run ./cmd/producer
load:
	go run ./cmd/loadgen
build:
	go build -o bin/collector ./cmd/collector
	go build -o bin/producer ./cmd/producer
	go build -o bin/loadgen ./cmd/loadgen
	go build -o bin/recoverybench ./cmd/recoverybench
fmt:
	gofmt -w cmd internal
vet:
	go vet ./...
test:
	go test ./...
race:
	go test -race ./...
bench:
	go test -run='^$$' -bench='Benchmark(Pipeline|AppendAndAck|AppendBatchAndAck)' -benchmem ./internal/collector ./internal/wal
recovery:
	go run ./cmd/recoverybench
up:
	docker compose up -d --build
down:
	docker compose down
smoke:
	sh scripts/smoke.sh
