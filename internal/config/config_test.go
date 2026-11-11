package config

import "testing"

func TestRejectInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct{ key, value string }{{"WORKERS", "0"}, {"BATCH_WAIT", "-1s"}, {"EXPORT_ENDPOINT", "file:///tmp/logs"}, {"MAX_PENDING_BYTES", "1"}, {"RETRY_MAX", "1ns"}, {"GRPC_ADDR", "no-port"}, {"ENABLE_PPROF", "maybe"}, {"WAL_SYNC_INTERVAL", "0s"}, {"WAL_SYNC_INTERVAL", "2s"}, {"WAL_SYNC_MAX_BATCH", "0"}, {"KUBERNETES_ENABLED", "maybe"}, {"KUBERNETES_SCRAPE_INTERVAL", "0s"}, {"KUBERNETES_REQUEST_TIMEOUT", "bad"}} {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}
