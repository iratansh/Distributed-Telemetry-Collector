package collector

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/iratansh/distributed-telemetry-collector/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type exportFunc func(context.Context, *logs.ExportLogsServiceRequest) (int64, error)

func (f exportFunc) Export(ctx context.Context, req *logs.ExportLogsServiceRequest) (int64, error) {
	return f(ctx, req)
}

func request() *logs.ExportLogsServiceRequest {
	return &logs.ExportLogsServiceRequest{ResourceLogs: []*logpb.ResourceLogs{{ScopeLogs: []*logpb.ScopeLogs{{LogRecords: []*logpb.LogRecord{{Body: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "hello"}}}}}}}}}
}

func setup(t *testing.T, adjust func(*config.Config), export Exporter) (*Pipeline, *Metrics) {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Workers = 1
	cfg.QueueCapacity = 1
	cfg.BatchSize = 1
	if adjust != nil {
		adjust(&cfg)
	}
	m := NewMetrics(prometheus.NewRegistry())
	p := New(cfg, export, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		p.Shutdown(ctx)
	})
	return p, m
}

func TestSaturationHasRetryInfoAndDrains(t *testing.T) {
	started, release := make(chan struct{}, 2), make(chan struct{})
	p, m := setup(t, nil, exportFunc(func(ctx context.Context, _ *logs.ExportLogsServiceRequest) (int64, error) {
		started <- struct{}{}
		select {
		case <-release:
			return 0, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}))
	if _, err := p.Export(context.Background(), request()); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := p.Export(context.Background(), request()); err != nil {
		t.Fatal(err)
	}
	_, err := p.Export(context.Background(), request())
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected capacity rejection: %v", err)
	}
	details := status.Convert(err).Details()
	if len(details) != 1 {
		t.Fatalf("missing RetryInfo: %v", details)
	}
	if _, ok := details[0].(*errdetails.RetryInfo); !ok {
		t.Fatalf("unexpected details %T", details[0])
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if testutil.ToFloat64(m.Received) != 2 || testutil.ToFloat64(m.Exported) != 2 || testutil.ToFloat64(m.PendingBytes) != 0 || testutil.ToFloat64(m.QueueDepth) != 0 {
		t.Fatal("drain lost records or leaked admission capacity")
	}
}

func TestByteBudgetIncludesInFlight(t *testing.T) {
	started := make(chan struct{})
	p, _ := setup(t, func(c *config.Config) { c.MaxPendingBytes = proto.Size(request()) }, exportFunc(func(ctx context.Context, _ *logs.ExportLogsServiceRequest) (int64, error) {
		close(started)
		<-ctx.Done()
		return 0, ctx.Err()
	}))
	p.Export(context.Background(), request())
	<-started
	_, err := p.Export(context.Background(), request())
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("in-flight bytes were not reserved: %v", err)
	}
}

func TestShutdownDeadlineAccountsForLostRecords(t *testing.T) {
	started := make(chan struct{})
	p, m := setup(t, nil, exportFunc(func(ctx context.Context, _ *logs.ExportLogsServiceRequest) (int64, error) {
		close(started)
		<-ctx.Done()
		return 0, ctx.Err()
	}))
	p.Export(context.Background(), request())
	<-started
	p.Export(context.Background(), request())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); err == nil {
		t.Fatal("expected drain timeout")
	}
	if testutil.ToFloat64(m.Dropped.WithLabelValues("shutdown")) != 2 || testutil.ToFloat64(m.PendingBytes) != 0 || testutil.ToFloat64(m.InFlight) != 0 || testutil.ToFloat64(m.QueueDepth) != 0 {
		t.Fatal("shutdown loss accounting or capacity release is incorrect")
	}
}

func TestBatchFlushAndPartialSuccess(t *testing.T) {
	p, m := setup(t, func(c *config.Config) { c.BatchSize = 8; c.QueueCapacity = 10; c.BatchWait = time.Hour }, exportFunc(func(_ context.Context, r *logs.ExportLogsServiceRequest) (int64, error) {
		if len(r.ResourceLogs) != 3 {
			t.Errorf("expected three requests batched, got %d", len(r.ResourceLogs))
		}
		return 1, nil
	}))
	for range 3 {
		if _, err := p.Export(context.Background(), request()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if testutil.ToFloat64(m.Exported) != 2 || testutil.ToFloat64(m.Dropped.WithLabelValues("downstream_rejected")) != 1 {
		t.Fatal("partial success accounting is incorrect")
	}
}

func TestConcurrentAdmissionAndShutdown(t *testing.T) {
	p, m := setup(t, func(c *config.Config) { c.Workers = 4; c.QueueCapacity = 100 }, exportFunc(func(context.Context, *logs.ExportLogsServiceRequest) (int64, error) { return 0, nil }))
	var senders sync.WaitGroup
	started := make(chan struct{}, 20)
	for range 20 {
		senders.Add(1)
		go func() {
			defer senders.Done()
			p.Export(context.Background(), request())
			started <- struct{}{}
			for range 50 {
				p.Export(context.Background(), request())
			}
		}()
	}
	<-started
	p.StopAdmission()
	senders.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if testutil.ToFloat64(m.Received) != testutil.ToFloat64(m.Exported) {
		t.Fatal("accepted records were lost during shutdown")
	}
}

func TestGRPCProtocolAndValidation(t *testing.T) {
	p, _ := setup(t, nil, exportFunc(func(context.Context, *logs.ExportLogsServiceRequest) (int64, error) { return 0, nil }))
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer(grpc.MaxRecvMsgSize(1024))
	logs.RegisterLogsServiceServer(s, p)
	go s.Serve(lis)
	defer s.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := logs.NewLogsServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.Export(ctx, request()); err != nil {
		t.Fatal(err)
	}
	bad := request()
	bad.ResourceLogs[0].ScopeLogs[0].LogRecords[0].TraceId = []byte{1}
	if _, err := client.Export(ctx, bad); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid trace id accepted: %v", err)
	}
	large := request()
	large.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body = &common.AnyValue{Value: &common.AnyValue_BytesValue{BytesValue: make([]byte, 2048)}}
	if _, err := client.Export(ctx, large); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("gRPC size limit not enforced: %v", err)
	}
}
