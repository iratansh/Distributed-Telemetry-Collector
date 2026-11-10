package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	endpoint := flag.String("endpoint", "localhost:4317", "OTLP/gRPC collector endpoint")
	service := flag.String("service", "telemetry-demo", "service.name resource attribute")
	count := flag.Int("count", 100, "total log records")
	batch := flag.Int("batch", 10, "records per request")
	interval := flag.Duration("interval", 100*time.Millisecond, "delay between requests")
	flag.Parse()
	if *count <= 0 || *batch <= 0 || *interval < 0 {
		fmt.Fprintln(os.Stderr, "count and batch must be positive; interval must be nonnegative")
		os.Exit(1)
	}
	if err := produce(*endpoint, *service, *count, *batch, *interval); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func produce(endpoint, service string, count, batch int, interval time.Duration) error {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := logs.NewLogsServiceClient(conn)
	run := time.Now().UTC().Format("20060102T150405.000000000Z")
	for sent := 0; sent < count; {
		records := make([]*logpb.LogRecord, 0, min(batch, count-sent))
		for i := 0; i < cap(records); i++ {
			now := uint64(time.Now().UnixNano())
			records = append(records, &logpb.LogRecord{TimeUnixNano: now, ObservedTimeUnixNano: now, SeverityNumber: logpb.SeverityNumber_SEVERITY_NUMBER_INFO, SeverityText: "INFO", Body: text(fmt.Sprintf("telemetry demo run=%s sequence=%d", run, sent+i+1))})
		}
		request := &logs.ExportLogsServiceRequest{ResourceLogs: []*logpb.ResourceLogs{{Resource: &resource.Resource{Attributes: []*common.KeyValue{{Key: "service.name", Value: text(service)}}}, ScopeLogs: []*logpb.ScopeLogs{{Scope: &common.InstrumentationScope{Name: "telemetry-demo"}, LogRecords: records}}}}}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		response, err := client.Export(ctx, request)
		cancel()
		if err != nil {
			return fmt.Errorf("request after %d accepted records: %w", sent, err)
		}
		if rejected := response.GetPartialSuccess().GetRejectedLogRecords(); rejected > 0 {
			return fmt.Errorf("collector rejected %d records", rejected)
		}
		sent += len(records)
		if sent < count {
			time.Sleep(interval)
		}
	}
	fmt.Printf("accepted %d logs service=%s run=%s (in-memory acknowledgement)\n", count, service, run)
	return nil
}

func text(value string) *common.AnyValue {
	return &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: value}}
}
