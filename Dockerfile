FROM golang:1.26.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/collector ./cmd/collector && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/producer ./cmd/producer && \
    mkdir -p /out/data/wal && chmod 0770 /out/data /out/data/wal

FROM scratch
COPY --from=build --chown=65532:65532 --chmod=0770 /out/data /data
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/collector /collector
COPY --from=build /out/producer /producer
USER 65532:65532
EXPOSE 4317 8888
ENTRYPOINT ["/collector"]
