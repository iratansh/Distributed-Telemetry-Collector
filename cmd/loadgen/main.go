// loadgen sends a bounded number of concurrent OTLP/gRPC requests and reports
// accepted event throughput, request latency, and backpressure.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type report struct {
	StartedAt                  string      `json:"started_at"`
	Endpoint                   string      `json:"endpoint"`
	RequestedEvents            int         `json:"requested_events"`
	BatchSize                  int         `json:"batch_size"`
	Workers                    int         `json:"bounded_workers"`
	PayloadBytes               int         `json:"payload_bytes_per_event"`
	AcceptedEvents             int64       `json:"accepted_events"`
	AcceptedRequests           int64       `json:"accepted_requests"`
	BackpressureRequests       int64       `json:"resource_exhausted_requests"`
	OtherErrors                int64       `json:"other_rpc_errors"`
	MetricsScrapeErrors        int64       `json:"metrics_scrape_errors"`
	MetricsSamples             int64       `json:"metrics_samples"`
	MaxQueueDepth              float64     `json:"max_queue_depth"`
	MaxPendingBytes            float64     `json:"max_pending_bytes"`
	MaxGoroutines              float64     `json:"max_goroutines"`
	MaxHeapAllocBytes          float64     `json:"max_heap_alloc_bytes"`
	MaxProcessResidentMemory   float64     `json:"max_process_resident_memory_bytes"`
	ElapsedSeconds             float64     `json:"elapsed_seconds"`
	AcceptedEventsPerSecond    float64     `json:"accepted_events_per_second"`
	RequestLatencyMilliseconds percentiles `json:"successful_request_latency_ms"`
	GOOS                       string      `json:"goos"`
	GOARCH                     string      `json:"goarch"`
	NumCPU                     int         `json:"num_cpu"`
	GOMAXPROCS                 int         `json:"gomaxprocs"`
	OfferedEventsPerSecond     float64     `json:"offered_events_per_second"`
}

type percentiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}
type job struct{ first, count int }

func main() {
	endpoint := flag.String("endpoint", "localhost:4317", "OTLP/gRPC collector endpoint")
	events := flag.Int("events", 100000, "number of log events to offer")
	batch := flag.Int("batch", 100, "events per OTLP request")
	workers := flag.Int("workers", 16, "maximum concurrent OTLP requests")
	payloadBytes := flag.Int("payload-bytes", 64, "approximate body bytes per event")
	timeout := flag.Duration("timeout", 10*time.Second, "deadline per OTLP request")
	rate := flag.Float64("rate", 0, "offered events per second; 0 sends as fast as bounded workers allow")
	metricsEndpoint := flag.String("metrics-endpoint", "http://localhost:8888/metrics", "collector Prometheus endpoint to sample; empty disables sampling")
	flag.Parse()
	if *events <= 0 || *batch <= 0 || *workers <= 0 || *payloadBytes < 0 || *timeout <= 0 || *rate < 0 {
		fmt.Fprintln(os.Stderr, "events, batch, workers, timeout must be positive; payload-bytes and rate must be nonnegative")
		os.Exit(2)
	}
	r, err := run(*endpoint, *events, *batch, *workers, *payloadBytes, *timeout, *rate, *metricsEndpoint)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(endpoint string, events, batch, workers, payloadBytes int, timeout time.Duration, rate float64, metricsEndpoint string) (report, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return report{}, err
	}
	defer conn.Close()
	client := logs.NewLogsServiceClient(conn)
	startedAt := time.Now()
	var samples metricSamples
	sampleCtx, stopSamples := context.WithCancel(context.Background())
	var sampleWG sync.WaitGroup
	if metricsEndpoint != "" {
		sampleWG.Add(1)
		go func() {
			defer sampleWG.Done()
			sampleCollector(sampleCtx, metricsEndpoint, &samples)
		}()
	}
	jobs := make(chan job, workers)
	results := make(chan result, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				request := makeRequest(item, payloadBytes)
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				start := time.Now()
				response, err := client.Export(ctx, request)
				latency := time.Since(start)
				cancel()
				results <- result{events: item.count, response: response, err: err, latency: latency}
			}
		}()
	}
	go func() {
		interval := time.Duration(0)
		if rate > 0 {
			interval = time.Duration(float64(time.Second) * float64(batch) / rate)
		}
		for first := 0; first < events; first += batch {
			if interval > 0 {
				target := startedAt.Add(time.Duration(first/batch) * interval)
				if wait := time.Until(target); wait > 0 {
					time.Sleep(wait)
				}
			}
			jobs <- job{first: first, count: min(batch, events-first)}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	var acceptedEvents, acceptedRequests, exhausted, otherErrors int64
	latencies := make([]time.Duration, 0, (events+batch-1)/batch)
	for result := range results {
		if result.err != nil {
			if status.Code(result.err) == codes.ResourceExhausted {
				exhausted++
			} else {
				otherErrors++
			}
			continue
		}
		rejected := int(result.response.GetPartialSuccess().GetRejectedLogRecords())
		accepted := max(0, result.events-rejected)
		acceptedEvents += int64(accepted)
		if accepted > 0 {
			acceptedRequests++
			latencies = append(latencies, result.latency)
		}
	}
	stopSamples()
	sampleWG.Wait()
	maxima, sampleErrors, sampleCount := samples.snapshot()
	elapsed := time.Since(startedAt)
	secs := elapsed.Seconds()
	throughput := float64(0)
	if secs > 0 {
		throughput = float64(acceptedEvents) / secs
	}
	return report{
		StartedAt: startedAt.UTC().Format(time.RFC3339Nano), Endpoint: endpoint, RequestedEvents: events, BatchSize: batch, Workers: workers, PayloadBytes: payloadBytes,
		AcceptedEvents: acceptedEvents, AcceptedRequests: acceptedRequests, BackpressureRequests: exhausted, OtherErrors: otherErrors,
		ElapsedSeconds: secs, AcceptedEventsPerSecond: throughput, RequestLatencyMilliseconds: calculatePercentiles(latencies),
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), OfferedEventsPerSecond: rate,
		MetricsScrapeErrors: sampleErrors, MetricsSamples: sampleCount,
		MaxQueueDepth: maxima["collector_queue_depth"], MaxPendingBytes: maxima["collector_pending_bytes"],
		MaxGoroutines: maxima["go_goroutines"], MaxHeapAllocBytes: maxima["go_memstats_heap_alloc_bytes"],
		MaxProcessResidentMemory: maxima["process_resident_memory_bytes"],
	}, nil
}

type result struct {
	events   int
	response *logs.ExportLogsServiceResponse
	err      error
	latency  time.Duration
}

type metricSamples struct {
	mu      sync.Mutex
	maxima  map[string]float64
	errors  int64
	samples int64
}

var sampledMetricNames = []string{
	"collector_queue_depth",
	"collector_pending_bytes",
	"go_goroutines",
	"go_memstats_heap_alloc_bytes",
	"process_resident_memory_bytes",
}

func sampleCollector(ctx context.Context, endpoint string, samples *metricSamples) {
	client := &http.Client{Timeout: time.Second}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err == nil {
			response, requestErr := client.Do(request)
			if requestErr != nil {
				if ctx.Err() == nil {
					samples.recordError()
				}
			} else {
				values, parseErr := parseMetricSamples(io.LimitReader(response.Body, 4<<20))
				_ = response.Body.Close()
				if response.StatusCode < 200 || response.StatusCode >= 300 || parseErr != nil {
					samples.recordError()
				} else {
					samples.record(values)
				}
			}
		} else {
			samples.recordError()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func parseMetricSamples(body io.Reader) (map[string]float64, error) {
	wanted := make(map[string]bool, len(sampledMetricNames))
	for _, name := range sampledMetricNames {
		wanted[name] = true
	}
	values := make(map[string]float64, len(wanted))
	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := fields[0]
		if brace := strings.IndexByte(name, '{'); brace >= 0 {
			name = name[:brace]
		}
		if !wanted[name] {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		values[name] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func (s *metricSamples) record(values map[string]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxima == nil {
		s.maxima = make(map[string]float64, len(values))
	}
	for name, value := range values {
		if value > s.maxima[name] {
			s.maxima[name] = value
		}
	}
	s.samples++
}

func (s *metricSamples) recordError() {
	s.mu.Lock()
	s.errors++
	s.mu.Unlock()
}

func (s *metricSamples) snapshot() (map[string]float64, int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	maxima := make(map[string]float64, len(s.maxima))
	for name, value := range s.maxima {
		maxima[name] = value
	}
	return maxima, s.errors, s.samples
}

func makeRequest(item job, payloadBytes int) *logs.ExportLogsServiceRequest {
	padding := strings.Repeat("x", payloadBytes)
	now := uint64(time.Now().UnixNano())
	records := make([]*logpb.LogRecord, item.count)
	for i := range records {
		body := fmt.Sprintf("loadgen sequence=%d %s", item.first+i, padding)
		records[i] = &logpb.LogRecord{TimeUnixNano: now, ObservedTimeUnixNano: now, SeverityNumber: logpb.SeverityNumber_SEVERITY_NUMBER_INFO, SeverityText: "INFO", Body: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: body}}}
	}
	return &logs.ExportLogsServiceRequest{ResourceLogs: []*logpb.ResourceLogs{{Resource: &resource.Resource{Attributes: []*common.KeyValue{{Key: "service.name", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "telemetry-loadgen"}}}}}, ScopeLogs: []*logpb.ScopeLogs{{Scope: &common.InstrumentationScope{Name: "telemetry-loadgen"}, LogRecords: records}}}}}
}

func calculatePercentiles(values []time.Duration) percentiles {
	if len(values) == 0 {
		return percentiles{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	quantile := func(q float64) float64 {
		index := int(math.Ceil(q*float64(len(values)))) - 1
		return float64(values[max(index, 0)]) / float64(time.Millisecond)
	}
	return percentiles{P50: quantile(.50), P95: quantile(.95), P99: quantile(.99)}
}
