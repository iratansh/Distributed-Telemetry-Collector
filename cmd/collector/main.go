package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iratansh/distributed-telemetry-collector/internal/collector"
	"github.com/iratansh/distributed-telemetry-collector/internal/config"
	"github.com/iratansh/distributed-telemetry-collector/internal/exporter"
	"github.com/iratansh/distributed-telemetry-collector/internal/kube"
	"github.com/iratansh/distributed-telemetry-collector/internal/wal"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("collector stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}
	defer grpcListener.Close()
	httpListener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	defer httpListener.Close()
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := collector.NewMetrics(registry)
	kubeMetrics := kube.NewMetrics(registry)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = cfg.Workers
	defer transport.CloseIdleConnections()
	export := &exporter.HTTP{
		Endpoint: cfg.Endpoint,
		Client:   &http.Client{Timeout: cfg.ExportTimeout, Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }},
		Attempts: cfg.MaxAttempts, Initial: cfg.RetryInitial, Max: cfg.RetryMax,
		OnFailure: metrics.ExportFailures.Inc, OnRetry: metrics.Retries.Inc,
	}
	durable, replay, err := wal.OpenWithOptions(cfg.WALDir, wal.Options{Sync: cfg.WALSync, SyncInterval: cfg.WALSyncInterval, MaxSyncBatch: cfg.WALSyncMaxBatch})
	if err != nil {
		return err
	}
	defer durable.Close()
	pipeline := collector.NewWithWAL(cfg, export, metrics, logger, durable, replay)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(cfg.MaxRequestBytes), grpc.MaxConcurrentStreams(64))
	logs.RegisterLogsServiceServer(server, pipeline)
	healthService := health.NewServer()
	healthpb.RegisterHealthServer(server, healthService)
	healthService.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthService.SetServingStatus("opentelemetry.proto.collector.logs.v1.LogsService", healthpb.HealthCheckResponse_SERVING)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !pipeline.Ready() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	if cfg.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	admin := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var kubeDone chan struct{}
	if cfg.KubernetesEnabled {
		client, err := kube.NewClient(cfg.KubernetesKubeconfig)
		if err != nil {
			kubeMetrics.RecordSetupError(err)
			logger.Error("Kubernetes metrics source could not initialize; log ingestion remains available", "error", err)
		} else {
			source := kube.NewSource(client, cfg.KubernetesNamespace, cfg.KubernetesScrapeInterval, cfg.KubernetesRequestTimeout, kubeMetrics, logger)
			kubeDone = make(chan struct{})
			go func() { defer close(kubeDone); source.Run(ctx) }()
		}
	}
	errs := make(chan error, 2)
	go func() { errs <- server.Serve(grpcListener) }()
	go func() { errs <- admin.Serve(httpListener) }()
	logger.Info("collector ready", "grpc", grpcListener.Addr(), "http", httpListener.Addr(), "export_endpoint", cfg.Endpoint, "wal_dir", cfg.WALDir, "wal_sync", cfg.WALSync, "wal_sync_interval", cfg.WALSyncInterval, "wal_sync_max_batch", cfg.WALSyncMaxBatch, "kubernetes_enabled", cfg.KubernetesEnabled, "kubernetes_namespace", cfg.KubernetesNamespace)
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errs:
	}
	stop()
	if kubeDone != nil {
		<-kubeDone
	}
	logger.Info("draining accepted logs", "timeout", cfg.ShutdownTimeout)
	if !pipeline.Ready() {
		logger.Info("WAL recovery is still being queued")
	}
	pipeline.StopAdmission()
	healthService.Shutdown()
	drainCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	grpcDone := make(chan struct{})
	go func() { server.GracefulStop(); close(grpcDone) }()
	select {
	case <-grpcDone:
	case <-drainCtx.Done():
		server.Stop()
		<-grpcDone
	}
	drainErr := pipeline.Shutdown(drainCtx)
	adminCtx, adminCancel := context.WithTimeout(context.Background(), time.Second)
	defer adminCancel()
	if err := admin.Shutdown(adminCtx); err != nil {
		admin.Close()
	}
	if drainErr != nil {
		return fmt.Errorf("shutdown deadline expired; some accepted logs may be lost: %w", drainErr)
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) && !errors.Is(serveErr, grpc.ErrServerStopped) {
		return serveErr
	}
	logger.Info("shutdown complete")
	return nil
}
