package collector

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Received, Exported, Retries, ExportFailures prometheus.Counter
	Rejected, Dropped                           *prometheus.CounterVec
	QueueDepth, PendingBytes, InFlight          prometheus.Gauge
	IngestDuration, ExportDuration              prometheus.Histogram
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
		reg.MustRegister(c)
		return c
	}
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		reg.MustRegister(g)
		return g
	}
	vec := func(name, help string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, []string{"reason"})
		reg.MustRegister(c)
		return c
	}
	hist := func(name, help string) prometheus.Histogram {
		h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: prometheus.DefBuckets})
		reg.MustRegister(h)
		return h
	}
	return &Metrics{
		Received:       counter("collector_received_total", "Log records accepted into memory."),
		Exported:       counter("collector_exported_total", "Log records accepted by the downstream backend."),
		Retries:        counter("collector_export_retries_total", "Scheduled downstream retry attempts."),
		ExportFailures: counter("collector_export_failures_total", "Failed downstream HTTP attempts."),
		Rejected:       vec("collector_rejected_requests_total", "Ingestion requests rejected, by reason."),
		Dropped:        vec("collector_dropped_total", "Accepted records lost without confirmed delivery, by reason."),
		QueueDepth:     gauge("collector_queue_depth", "Requests waiting in the queue, excluding workers."),
		PendingBytes:   gauge("collector_pending_bytes", "Encoded protobuf bytes admitted, including workers."),
		InFlight:       gauge("collector_in_flight_records", "Records held by workers, including batch assembly and retries."),
		IngestDuration: hist("collector_ingest_duration_seconds", "OTLP receiver handler latency."),
		ExportDuration: hist("collector_export_duration_seconds", "Batch export latency including retries."),
	}
}
