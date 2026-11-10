package exporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestRetryAndPartialSuccess(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Error("missing OTLP content type")
		}
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		data, _ := proto.Marshal(&logs.ExportLogsServiceResponse{PartialSuccess: &logs.ExportLogsPartialSuccess{RejectedLogRecords: 2}})
		w.Write(data)
	}))
	defer server.Close()
	e := &HTTP{Endpoint: server.URL, Client: server.Client(), Attempts: 3, Initial: time.Nanosecond, Max: time.Nanosecond}
	rejected, err := e.Export(context.Background(), &logs.ExportLogsServiceRequest{})
	if err != nil || rejected != 2 || attempts.Load() != 2 {
		t.Fatalf("rejected=%d attempts=%d err=%v", rejected, attempts.Load(), err)
	}
}

func TestLokiNoContentSuccess(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	e := &HTTP{Endpoint: server.URL, Client: server.Client(), Attempts: 3, Initial: time.Nanosecond, Max: time.Nanosecond}
	rejected, err := e.Export(context.Background(), &logs.ExportLogsServiceRequest{})
	if err != nil || rejected != 0 || attempts.Load() != 1 {
		t.Fatalf("rejected=%d attempts=%d err=%v", rejected, attempts.Load(), err)
	}
}

func TestHTTPFailurePolicy(t *testing.T) {
	for _, code := range []int{400, 401, 413, 429, 500, 502, 503, 504} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { attempts++; w.WriteHeader(code) }))
			defer server.Close()
			e := &HTTP{Endpoint: server.URL, Client: server.Client(), Attempts: 3, Initial: time.Nanosecond, Max: time.Nanosecond}
			_, err := e.Export(context.Background(), &logs.ExportLogsServiceRequest{})
			want := 1
			if code == 429 || code == 502 || code == 503 || code == 504 {
				want = 3
			}
			if err == nil || attempts != want {
				t.Fatalf("attempts=%d want=%d err=%v", attempts, want, err)
			}
		})
	}
}

func TestRetryWaitIsCancelable(t *testing.T) {
	called := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(called)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(503)
	}))
	defer server.Close()
	e := &HTTP{Endpoint: server.URL, Client: server.Client(), Attempts: 5, Initial: time.Hour, Max: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := e.Export(ctx, &logs.ExportLogsServiceRequest{}); done <- err }()
	<-called
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("retry did not cancel")
	}
}
