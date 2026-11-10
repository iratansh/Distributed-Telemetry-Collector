.PHONY: run produce build fmt vet test race up down smoke

run:
	go run ./cmd/collector
produce:
	go run ./cmd/producer
build:
	go build -o bin/collector ./cmd/collector
	go build -o bin/producer ./cmd/producer
fmt:
	gofmt -w cmd internal
vet:
	go vet ./...
test:
	go test ./...
race:
	go test -race ./...
up:
	docker compose up -d --build
down:
	docker compose down
smoke:
	sh scripts/smoke.sh
