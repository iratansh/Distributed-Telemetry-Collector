package config

import "testing"

func TestRejectInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct{ key, value string }{{"WORKERS", "0"}, {"BATCH_WAIT", "-1s"}, {"EXPORT_ENDPOINT", "file:///tmp/logs"}, {"MAX_PENDING_BYTES", "1"}, {"RETRY_MAX", "1ns"}, {"GRPC_ADDR", "no-port"}, {"ENABLE_PPROF", "maybe"}} {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}
