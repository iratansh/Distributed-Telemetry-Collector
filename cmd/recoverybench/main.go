// recoverybench measures WAL startup scan and reconstruction of unacknowledged
// requests. Seeding time is excluded from the reported recovery duration.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iratansh/distributed-telemetry-collector/internal/collector"
	"github.com/iratansh/distributed-telemetry-collector/internal/config"
	"github.com/iratansh/distributed-telemetry-collector/internal/wal"
	"github.com/prometheus/client_golang/prometheus"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
)

type report struct {
	RequestedEvents            int     `json:"requested_events"`
	RecoveredRequests          int     `json:"recovered_requests"`
	RecoveredEvents            int     `json:"recovered_events"`
	WALScanSeconds             float64 `json:"wal_scan_seconds"`
	ReplayAndCheckpointSeconds float64 `json:"replay_and_checkpoint_seconds"`
	RedeliveredEvents          int64   `json:"redelivered_events"`
	RedeliveredEventsPerSecond float64 `json:"redelivered_events_per_second"`
	SyncInterval               string  `json:"wal_sync_interval"`
	SyncMaxBatch               int     `json:"wal_sync_max_batch"`
	GOOS                       string  `json:"goos"`
	GOARCH                     string  `json:"goarch"`
	NumCPU                     int     `json:"num_cpu"`
	GOMAXPROCS                 int     `json:"gomaxprocs"`
	UncleanStopSimulated       bool    `json:"unclean_stop_simulated"`
}

var seedLogKeptOpen *wal.Log

func main() {
	events := flag.Int("events", 10000, "number of unacknowledged one-event requests to seed")
	interval := flag.Duration("sync-interval", 2*time.Millisecond, "WAL grouped sync interval")
	maxBatch := flag.Int("sync-max-batch", 32, "WAL records per grouped sync")
	seedOnly := flag.Bool("seed-only", false, "internal child mode: write a pending WAL and exit without closing it")
	walDir := flag.String("wal-dir", "", "internal child mode WAL directory")
	flag.Parse()
	if *events <= 0 || *interval <= 0 || *maxBatch <= 0 {
		fmt.Fprintln(os.Stderr, "events, sync-interval, and sync-max-batch must be positive")
		os.Exit(2)
	}
	options := wal.Options{Sync: true, SyncInterval: *interval, MaxSyncBatch: *maxBatch}
	if *seedOnly {
		seedLogKeptOpen = seedWAL(*walDir, *events, options)
		os.Exit(0)
	}
	dir, err := os.MkdirTemp("", "dtc-recoverybench-")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(dir)
	executable, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	seed := exec.Command(executable, "-seed-only", "-wal-dir", dir, "-events", strconv.Itoa(*events), "-sync-interval", interval.String(), "-sync-max-batch", strconv.Itoa(*maxBatch))
	if output, err := seed.CombinedOutput(); err != nil {
		fatal(fmt.Errorf("seed WAL child process: %w: %s", err, output))
	}

	started := time.Now()
	log, replay, err := wal.OpenWithOptions(dir, options)
	scanElapsed := time.Since(started)
	if err != nil {
		fatal(err)
	}
	recovered := 0
	for _, record := range replay {
		for _, resourceLogs := range record.Request.ResourceLogs {
			for _, scopeLogs := range resourceLogs.ScopeLogs {
				recovered += len(scopeLogs.LogRecords)
			}
		}
	}
	var delivered atomic.Int64
	registry := prometheus.NewRegistry()
	metrics := collector.NewMetrics(registry)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		QueueCapacity: 128, Workers: 4, BatchSize: 8, BatchWait: 200 * time.Millisecond,
		MaxRequestBytes: 1 << 20, MaxRecords: 1000, MaxPendingBytes: 1 << 30,
	}
	exporter := recoveryExporter{delivered: &delivered}
	replayStarted := time.Now()
	pipeline := collector.NewWithWAL(cfg, exporter, metrics, logger, log, replay)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	if err := pipeline.Shutdown(shutdownCtx); err != nil {
		_ = log.Close()
		fatal(err)
	}
	replayElapsed := time.Since(replayStarted)
	if err := log.Close(); err != nil {
		fatal(err)
	}
	secs := replayElapsed.Seconds()
	rate := 0.0
	if secs > 0 {
		rate = float64(delivered.Load()) / secs
	}
	_ = json.NewEncoder(os.Stdout).Encode(report{
		RequestedEvents: *events, RecoveredRequests: len(replay), RecoveredEvents: recovered,
		WALScanSeconds: scanElapsed.Seconds(), ReplayAndCheckpointSeconds: secs,
		RedeliveredEvents: delivered.Load(), RedeliveredEventsPerSecond: rate,
		SyncInterval: interval.String(), SyncMaxBatch: *maxBatch,
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		UncleanStopSimulated: true,
	})
}

func seedWAL(dir string, events int, options wal.Options) *wal.Log {
	if dir == "" {
		fatal(fmt.Errorf("internal seed mode requires -wal-dir"))
	}
	log, _, err := wal.OpenWithOptions(dir, options)
	if err != nil {
		fatal(err)
	}
	jobs := make(chan int)
	var seedWG sync.WaitGroup
	var seedErr atomic.Pointer[error]
	for range min(16, events) {
		seedWG.Add(1)
		go func() {
			defer seedWG.Done()
			for i := range jobs {
				if _, err := log.Append(makeRequest(i)); err != nil {
					wrapped := err
					seedErr.CompareAndSwap(nil, &wrapped)
				}
			}
		}()
	}
	for i := range events {
		jobs <- i
	}
	close(jobs)
	seedWG.Wait()
	if errPtr := seedErr.Load(); errPtr != nil {
		fatal(*errPtr)
	}
	return log
}

type recoveryExporter struct{ delivered *atomic.Int64 }

func (e recoveryExporter) Export(_ context.Context, request *logs.ExportLogsServiceRequest) (int64, error) {
	e.delivered.Add(int64(countRecords(request)))
	return 0, nil
}

func countRecords(request *logs.ExportLogsServiceRequest) int {
	count := 0
	for _, resourceLogs := range request.ResourceLogs {
		for _, scopeLogs := range resourceLogs.ScopeLogs {
			count += len(scopeLogs.LogRecords)
		}
	}
	return count
}

func makeRequest(sequence int) *logs.ExportLogsServiceRequest {
	now := uint64(time.Now().UnixNano())
	request := &logs.ExportLogsServiceRequest{}
	request.ResourceLogs = append(request.ResourceLogs, &logpb.ResourceLogs{
		Resource: &resource.Resource{Attributes: []*common.KeyValue{{Key: "service.name", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "recoverybench"}}}}},
		ScopeLogs: []*logpb.ScopeLogs{{Scope: &common.InstrumentationScope{Name: "recoverybench"}, LogRecords: []*logpb.LogRecord{{
			TimeUnixNano: now, ObservedTimeUnixNano: now,
			Body: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: fmt.Sprintf("recovery event=%d", sequence)}},
		}}}},
	})
	return request
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
