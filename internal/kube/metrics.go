package kube

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"
)

// ResourceClient is the small part of the Kubernetes API used by this source.
// Keeping it narrow makes API errors and snapshot conversion easy to exercise.
type ResourceClient interface {
	ListPods(context.Context, string) (*corev1.PodList, error)
	ListDeployments(context.Context, string) (*appsv1.DeploymentList, error)
	ListNodes(context.Context) (*corev1.NodeList, error)
}

type apiClient struct {
	core coreclient.CoreV1Interface
	apps appsclient.AppsV1Interface
}

func (c apiClient) ListPods(ctx context.Context, namespace string) (*corev1.PodList, error) {
	result := &corev1.PodList{}
	opts := metav1.ListOptions{Limit: 500}
	for {
		page, err := c.core.Pods(namespace).List(ctx, opts)
		if err != nil {
			return nil, err
		}
		result.Items = append(result.Items, page.Items...)
		if page.Continue == "" {
			return result, nil
		}
		opts.Continue = page.Continue
	}
}
func (c apiClient) ListDeployments(ctx context.Context, namespace string) (*appsv1.DeploymentList, error) {
	result := &appsv1.DeploymentList{}
	opts := metav1.ListOptions{Limit: 500}
	for {
		page, err := c.apps.Deployments(namespace).List(ctx, opts)
		if err != nil {
			return nil, err
		}
		result.Items = append(result.Items, page.Items...)
		if page.Continue == "" {
			return result, nil
		}
		opts.Continue = page.Continue
	}
}
func (c apiClient) ListNodes(ctx context.Context) (*corev1.NodeList, error) {
	result := &corev1.NodeList{}
	opts := metav1.ListOptions{Limit: 500}
	for {
		page, err := c.core.Nodes().List(ctx, opts)
		if err != nil {
			return nil, err
		}
		result.Items = append(result.Items, page.Items...)
		if page.Continue == "" {
			return result, nil
		}
		opts.Continue = page.Continue
	}
}

// NewClient uses the in-cluster service account when available and otherwise
// uses the supplied kubeconfig path or the standard local kubeconfig rules.
func NewClient(kubeconfig string) (ResourceClient, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	core, err := coreclient.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	apps, err := appsclient.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return apiClient{core: core, apps: apps}, nil
}

type Metrics struct {
	Enabled            prometheus.Gauge
	Success            prometheus.Gauge
	Errors             *prometheus.CounterVec
	Duration           prometheus.Histogram
	LastSuccess        prometheus.Gauge
	Pods               *prometheus.GaugeVec
	Restarts           *prometheus.GaugeVec
	DeploymentReplicas *prometheus.GaugeVec
	Nodes              *prometheus.GaugeVec
	NodeAllocatable    *prometheus.GaugeVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Enabled:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "collector_kubernetes_collection_enabled", Help: "Whether Kubernetes collection is enabled."}),
		Success:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "collector_kubernetes_collection_success", Help: "Whether the most recent Kubernetes collection succeeded."}),
		Errors:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "collector_kubernetes_collection_errors_total", Help: "Kubernetes collection failures by error class."}, []string{"reason"}),
		Duration:           prometheus.NewHistogram(prometheus.HistogramOpts{Name: "collector_kubernetes_collection_duration_seconds", Help: "Duration of a Kubernetes API snapshot collection."}),
		LastSuccess:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "collector_kubernetes_last_success_timestamp_seconds", Help: "Unix timestamp of the last successful Kubernetes collection."}),
		Pods:               prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "collector_kubernetes_pods", Help: "Current pod count by namespace and phase."}, []string{"namespace", "phase"}),
		Restarts:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "collector_kubernetes_container_restarts", Help: "Current cumulative container restart count by namespace, pod, and container."}, []string{"namespace", "pod", "container"}),
		DeploymentReplicas: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "collector_kubernetes_deployment_replicas", Help: "Deployment replica counts by namespace, deployment, and state."}, []string{"namespace", "deployment", "state"}),
		Nodes:              prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "collector_kubernetes_nodes", Help: "Kubernetes node counts by Ready condition status."}, []string{"ready_status"}),
		NodeAllocatable:    prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "collector_kubernetes_node_allocatable", Help: "Node allocatable CPU cores or memory bytes, as reported by Kubernetes."}, []string{"node", "resource"}),
	}
	reg.MustRegister(m.Enabled, m.Success, m.Errors, m.Duration, m.LastSuccess, m.Pods, m.Restarts, m.DeploymentReplicas, m.Nodes, m.NodeAllocatable)
	return m
}

type Source struct {
	client    ResourceClient
	namespace string
	interval  time.Duration
	timeout   time.Duration
	metrics   *Metrics
	logger    *slog.Logger
}

func NewSource(client ResourceClient, namespace string, interval, timeout time.Duration, metrics *Metrics, logger *slog.Logger) *Source {
	return &Source{client: client, namespace: namespace, interval: interval, timeout: timeout, metrics: metrics, logger: logger}
}

func (s *Source) Run(ctx context.Context) {
	s.metrics.Enabled.Set(1)
	if err := s.Collect(ctx); err != nil && ctx.Err() == nil {
		s.logger.Error("Kubernetes collection failed", "error", err)
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Collect(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				s.logger.Error("Kubernetes collection failed", "error", err)
			}
		}
	}
}

func (s *Source) Collect(parent context.Context) error {
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, s.timeout)
	defer cancel()
	pods, err := s.client.ListPods(ctx, s.namespace)
	if err != nil {
		if parent.Err() != nil {
			return err
		}
		return s.collectionError("pods", err, started)
	}
	deployments, err := s.client.ListDeployments(ctx, s.namespace)
	if err != nil {
		if parent.Err() != nil {
			return err
		}
		return s.collectionError("deployments", err, started)
	}
	nodes, err := s.client.ListNodes(ctx)
	if err != nil {
		if parent.Err() != nil {
			return err
		}
		return s.collectionError("nodes", err, started)
	}

	podCounts := make(map[[2]string]float64)
	restarts := make(map[[3]string]float64)
	for i := range pods.Items {
		pod := &pods.Items[i]
		phase := string(pod.Status.Phase)
		if phase == "" {
			phase = "Unknown"
		}
		podCounts[[2]string{pod.Namespace, phase}]++
		for _, status := range pod.Status.ContainerStatuses {
			restarts[[3]string{pod.Namespace, pod.Name, status.Name}] = float64(status.RestartCount)
		}
		for _, status := range pod.Status.InitContainerStatuses {
			restarts[[3]string{pod.Namespace, pod.Name, status.Name}] = float64(status.RestartCount)
		}
	}
	s.metrics.Pods.Reset()
	for labels, value := range podCounts {
		s.metrics.Pods.WithLabelValues(labels[0], labels[1]).Set(value)
	}
	s.metrics.Restarts.Reset()
	for labels, value := range restarts {
		s.metrics.Restarts.WithLabelValues(labels[0], labels[1], labels[2]).Set(value)
	}
	s.metrics.DeploymentReplicas.Reset()
	for i := range deployments.Items {
		deployment := &deployments.Items[i]
		desired := int32(1)
		if deployment.Spec.Replicas != nil {
			desired = *deployment.Spec.Replicas
		}
		values := map[string]float64{"desired": float64(desired), "ready": float64(deployment.Status.ReadyReplicas), "available": float64(deployment.Status.AvailableReplicas), "updated": float64(deployment.Status.UpdatedReplicas)}
		for state, value := range values {
			s.metrics.DeploymentReplicas.WithLabelValues(deployment.Namespace, deployment.Name, state).Set(value)
		}
	}
	nodeStates := make(map[string]float64)
	nodeAllocatable := make(map[[2]string]float64)
	for i := range nodes.Items {
		node := &nodes.Items[i]
		ready := "unknown"
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady {
				switch condition.Status {
				case corev1.ConditionTrue:
					ready = "true"
				case corev1.ConditionFalse:
					ready = "false"
				default:
					ready = "unknown"
				}
				break
			}
		}
		nodeStates[ready]++
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
			if quantity, ok := node.Status.Allocatable[name]; ok {
				nodeAllocatable[[2]string{node.Name, string(name)}] = quantity.AsApproximateFloat64()
			}
		}
	}
	s.metrics.Nodes.Reset()
	for status, count := range nodeStates {
		s.metrics.Nodes.WithLabelValues(status).Set(count)
	}
	s.metrics.NodeAllocatable.Reset()
	for labels, value := range nodeAllocatable {
		s.metrics.NodeAllocatable.WithLabelValues(labels[0], labels[1]).Set(value)
	}
	s.metrics.Duration.Observe(time.Since(started).Seconds())
	s.metrics.LastSuccess.Set(float64(time.Now().Unix()))
	s.metrics.Success.Set(1)
	return nil
}

func (s *Source) collectionError(operation string, err error, started time.Time) error {
	s.metrics.Duration.Observe(time.Since(started).Seconds())
	s.metrics.Success.Set(0)
	reason := "api_error"
	switch {
	case apierrors.IsUnauthorized(err):
		reason = "unauthorized"
	case apierrors.IsForbidden(err):
		reason = "forbidden"
	case errors.Is(err, context.DeadlineExceeded):
		reason = "timeout"
	case errors.Is(err, context.Canceled):
		reason = "canceled"
	}
	s.metrics.Errors.WithLabelValues(reason).Inc()
	return fmt.Errorf("list %s: %w", operation, err)
}

// RecordSetupError exposes client configuration failures without preventing
// the log receiver from starting.
func (m *Metrics) RecordSetupError(err error) {
	m.Enabled.Set(1)
	m.Success.Set(0)
	m.Errors.WithLabelValues("configuration").Inc()
}
