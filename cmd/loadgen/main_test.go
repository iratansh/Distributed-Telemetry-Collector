package main

import (
	"strings"
	"testing"
)

func TestParseMetricSamples(t *testing.T) {
	values, err := parseMetricSamples(strings.NewReader(`# HELP collector_queue_depth waiting requests
# TYPE collector_queue_depth gauge
collector_queue_depth 3
collector_pending_bytes{pool="default"} 512
go_goroutines 17
go_memstats_heap_alloc_bytes 4096
process_resident_memory_bytes 8192
unrelated_metric 123
`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"collector_queue_depth":         3,
		"collector_pending_bytes":       512,
		"go_goroutines":                 17,
		"go_memstats_heap_alloc_bytes":  4096,
		"process_resident_memory_bytes": 8192,
	}
	for name, expected := range want {
		if got := values[name]; got != expected {
			t.Errorf("%s = %v, want %v", name, got, expected)
		}
	}
}

func TestMetricSamplesTrackMaxima(t *testing.T) {
	var samples metricSamples
	samples.record(map[string]float64{"collector_queue_depth": 3})
	samples.record(map[string]float64{"collector_queue_depth": 1, "collector_pending_bytes": 64})
	maxima, errs, count := samples.snapshot()
	if maxima["collector_queue_depth"] != 3 || maxima["collector_pending_bytes"] != 64 || errs != 0 || count != 2 {
		t.Fatalf("unexpected sample summary: maxima=%v errors=%d samples=%d", maxima, errs, count)
	}
}
