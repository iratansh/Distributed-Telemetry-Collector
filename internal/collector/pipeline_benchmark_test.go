package collector

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/iratansh/distributed-telemetry-collector/internal/config"
	"github.com/iratansh/distributed-telemetry-collector/internal/wal"
	"github.com/prometheus/client_golang/prometheus"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

var benchmarkRequest = request()

func BenchmarkPipelineAdmissionInMemory(b *testing.B) { benchmarkPipeline(b, false) }
func BenchmarkPipelineAdmissionWithWAL(b *testing.B)  { benchmarkPipeline(b, true) }

func benchmarkPipeline(b *testing.B, durable bool) {
	cfg := config.Config{QueueCapacity: 1 << 20, Workers: max(2, runtime.GOMAXPROCS(0)), BatchSize: 1, MaxRequestBytes: 1 << 20, MaxRecords: 1000, MaxPendingBytes: 1 << 40, BatchWait: time.Millisecond}
	metrics := NewMetrics(prometheus.NewRegistry())
	export := exportFunc(func(context.Context, *logs.ExportLogsServiceRequest) (int64, error) { return 0, nil })
	var p *Pipeline
	var log *wal.Log
	if durable {
		var replay []wal.Record
		var err error
		log, replay, err = wal.OpenWithOptions(b.TempDir(), wal.Options{Sync: true, SyncInterval: 2 * time.Millisecond, MaxSyncBatch: 32})
		if err != nil {
			b.Fatal(err)
		}
		p = NewWithWAL(cfg, export, metrics, slog.New(slog.NewTextHandler(io.Discard, nil)), log, replay)
	} else {
		p = New(cfg, export, metrics, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	b.ReportAllocs()
	b.SetBytes(int64(proto.Size(benchmarkRequest)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := p.Export(context.Background(), benchmarkRequest); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		b.Errorf("pipeline shutdown: %v", err)
	}
	if log != nil {
		if err := log.Close(); err != nil {
			b.Errorf("WAL close: %v", err)
		}
	}
}
