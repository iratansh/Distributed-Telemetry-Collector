package exporter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

type HTTP struct {
	Endpoint           string
	Client             *http.Client
	Attempts           int
	Initial, Max       time.Duration
	OnFailure, OnRetry func()
}

// Export retries only transport failures and OTLP's retryable HTTP status codes.
// A partial success is final: accepted records must not be resent automatically.
func (e *HTTP) Export(ctx context.Context, req *logs.ExportLogsServiceRequest) (int64, error) {
	body, err := proto.Marshal(req)
	if err != nil {
		return 0, err
	}
	delay := e.Initial
	for attempt := 0; attempt < e.Attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		rejected, retry, retryAfter, err := e.once(ctx, body)
		if err == nil {
			return rejected, nil
		}
		if e.OnFailure != nil {
			e.OnFailure()
		}
		if !retry || attempt+1 == e.Attempts {
			return 0, err
		}
		if e.OnRetry != nil {
			e.OnRetry()
		}
		// Add jitter while bounding every sleep, including Retry-After.
		wait := min(delay+time.Duration(rand.Int64N(max(int64(delay/2), 1))), e.Max)
		wait = min(max(wait, retryAfter), e.Max)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, e.Max)
	}
	return 0, fmt.Errorf("export attempts must be positive")
}

func (e *HTTP) once(ctx context.Context, body []byte) (int64, bool, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.Endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, false, 0, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := e.Client.Do(req)
	if err != nil {
		return 0, true, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return 0, true, 0, err
	}
	if len(data) > 1<<20 {
		return 0, false, 0, fmt.Errorf("export response exceeds 1 MiB")
	}
	// Loki's native OTLP endpoint returns 204 for full success. Other OTLP
	// servers return 200 with a protobuf ExportLogsServiceResponse.
	if resp.StatusCode == http.StatusNoContent {
		return 0, false, 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		retry := resp.StatusCode == 429 || resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504
		var wait time.Duration
		if secs, err := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 32); err == nil && secs > 0 {
			wait = time.Duration(secs) * time.Second
		} else if date, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
			wait = max(time.Until(date), 0)
		}
		return 0, retry, wait, fmt.Errorf("export HTTP %d: %.512s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	result := new(logs.ExportLogsServiceResponse)
	if err := proto.Unmarshal(data, result); err != nil {
		return 0, false, 0, fmt.Errorf("invalid OTLP response: %w", err)
	}
	if result.GetPartialSuccess().GetRejectedLogRecords() < 0 {
		return 0, false, 0, fmt.Errorf("invalid negative rejected record count")
	}
	return result.GetPartialSuccess().GetRejectedLogRecords(), false, 0, nil
}
