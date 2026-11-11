.PHONY: run produce load build fmt fmt-check vet test race bench recovery up down smoke

GO_FILES := $(shell find cmd internal -type f -name '*.go')

run:
	go run ./cmd/collector
produce:
	go run ./cmd/producer
load:
	go run ./cmd/loadgen
build:
	mkdir -p bin
	go build -o bin/collector ./cmd/collector
	go build -o bin/producer ./cmd/producer
	go build -o bin/loadgen ./cmd/loadgen
	go build -o bin/recoverybench ./cmd/recoverybench
fmt:
	gofmt -w $(GO_FILES)
fmt-check:
	@unformatted="$$(gofmt -l $(GO_FILES))"; \
	if [ -n "$$unformatted" ]; then \
		printf '%s\n' "$$unformatted"; \
		exit 1; \
	fi
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
