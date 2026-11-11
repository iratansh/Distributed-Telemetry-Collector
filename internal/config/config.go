package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	GRPCAddr                 string
	HTTPAddr                 string
	Endpoint                 string
	QueueCapacity            int
	Workers                  int
	BatchSize                int
	MaxRequestBytes          int
	MaxRecords               int
	MaxPendingBytes          int
	MaxAttempts              int
	BatchWait                time.Duration
	ExportTimeout            time.Duration
	RetryInitial             time.Duration
	RetryMax                 time.Duration
	ShutdownTimeout          time.Duration
	Pprof                    bool
	WALDir                   string
	WALSync                  bool
	WALSyncInterval          time.Duration
	WALSyncMaxBatch          int
	KubernetesEnabled        bool
	KubernetesNamespace      string
	KubernetesKubeconfig     string
	KubernetesScrapeInterval time.Duration
	KubernetesRequestTimeout time.Duration
}

func Load() (Config, error) {
	c := Config{
		GRPCAddr: "127.0.0.1:4317", HTTPAddr: "127.0.0.1:8888",
		Endpoint: "http://localhost:3100/otlp/v1/logs", QueueCapacity: 128,
		Workers: 4, BatchSize: 8, MaxRequestBytes: 1 << 20, MaxRecords: 1000,
		MaxPendingBytes: 64 << 20, MaxAttempts: 5, BatchWait: 200 * time.Millisecond,
		ExportTimeout: 5 * time.Second, RetryInitial: 100 * time.Millisecond,
		RetryMax: 2 * time.Second, ShutdownTimeout: 15 * time.Second,
		WALDir: "./data/wal", WALSync: true, WALSyncInterval: 2 * time.Millisecond, WALSyncMaxBatch: 32,
		KubernetesScrapeInterval: 30 * time.Second, KubernetesRequestTimeout: 10 * time.Second,
	}
	for key, dst := range map[string]*string{
		"GRPC_ADDR": &c.GRPCAddr, "HTTP_ADDR": &c.HTTPAddr, "EXPORT_ENDPOINT": &c.Endpoint,
	} {
		if value, ok := os.LookupEnv(key); ok {
			*dst = value
		}
	}
	for key, dst := range map[string]*int{
		"QUEUE_CAPACITY": &c.QueueCapacity, "WORKERS": &c.Workers,
		"BATCH_SIZE": &c.BatchSize, "MAX_REQUEST_BYTES": &c.MaxRequestBytes,
		"MAX_RECORDS_PER_REQUEST": &c.MaxRecords, "MAX_PENDING_BYTES": &c.MaxPendingBytes,
		"MAX_EXPORT_ATTEMPTS": &c.MaxAttempts,
		"WAL_SYNC_MAX_BATCH":  &c.WALSyncMaxBatch,
	} {
		if value, ok := os.LookupEnv(key); ok {
			n, err := strconv.Atoi(value)
			if err != nil || n <= 0 {
				return c, fmt.Errorf("%s must be a positive integer", key)
			}
			*dst = n
		}
	}
	for key, dst := range map[string]*time.Duration{
		"BATCH_WAIT": &c.BatchWait, "EXPORT_TIMEOUT": &c.ExportTimeout,
		"RETRY_INITIAL": &c.RetryInitial, "RETRY_MAX": &c.RetryMax,
		"SHUTDOWN_TIMEOUT":  &c.ShutdownTimeout,
		"WAL_SYNC_INTERVAL": &c.WALSyncInterval,
	} {
		if value, ok := os.LookupEnv(key); ok {
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 {
				return c, fmt.Errorf("%s must be a positive duration", key)
			}
			*dst = d
		}
	}
	if value, ok := os.LookupEnv("ENABLE_PPROF"); ok {
		v, err := strconv.ParseBool(value)
		if err != nil {
			return c, fmt.Errorf("ENABLE_PPROF: %w", err)
		}
		c.Pprof = v
	}
	if value, ok := os.LookupEnv("WAL_DIR"); ok {
		c.WALDir = value
	}
	for key, dst := range map[string]*string{"KUBERNETES_NAMESPACE": &c.KubernetesNamespace, "KUBERNETES_KUBECONFIG": &c.KubernetesKubeconfig} {
		if value, ok := os.LookupEnv(key); ok {
			*dst = value
		}
	}
	for key, dst := range map[string]*time.Duration{"KUBERNETES_SCRAPE_INTERVAL": &c.KubernetesScrapeInterval, "KUBERNETES_REQUEST_TIMEOUT": &c.KubernetesRequestTimeout} {
		if value, ok := os.LookupEnv(key); ok {
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 {
				return c, fmt.Errorf("%s must be a positive duration", key)
			}
			*dst = d
		}
	}
	if value, ok := os.LookupEnv("KUBERNETES_ENABLED"); ok {
		v, err := strconv.ParseBool(value)
		if err != nil {
			return c, fmt.Errorf("KUBERNETES_ENABLED: %w", err)
		}
		c.KubernetesEnabled = v
	}
	if value, ok := os.LookupEnv("WAL_SYNC"); ok {
		v, err := strconv.ParseBool(value)
		if err != nil {
			return c, fmt.Errorf("WAL_SYNC: %w", err)
		}
		c.WALSync = v
	}
	if c.WALDir == "" {
		return c, fmt.Errorf("WAL_DIR must not be empty")
	}
	if c.WALSyncInterval > time.Second {
		return c, fmt.Errorf("WAL_SYNC_INTERVAL must be <= 1s")
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Fragment != "" {
		return c, fmt.Errorf("EXPORT_ENDPOINT must be an absolute HTTP(S) logs URL")
	}
	for _, addr := range []string{c.GRPCAddr, c.HTTPAddr} {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return c, fmt.Errorf("invalid listener address %q: %w", addr, err)
		}
	}
	if c.MaxPendingBytes < c.MaxRequestBytes {
		return c, fmt.Errorf("MAX_PENDING_BYTES must be >= MAX_REQUEST_BYTES")
	}
	if c.RetryMax < c.RetryInitial {
		return c, fmt.Errorf("RETRY_MAX must be >= RETRY_INITIAL")
	}
	return c, nil
}
