package kube

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type fakeClient struct {
	pods                *corev1.PodList
	deployments         *appsv1.DeploymentList
	podErr              error
	deploymentErr       error
	podNamespace        string
	deploymentNamespace string
}

func (f *fakeClient) ListPods(_ context.Context, namespace string) (*corev1.PodList, error) {
	f.podNamespace = namespace
	return f.pods, f.podErr
}
func (f *fakeClient) ListDeployments(_ context.Context, namespace string) (*appsv1.DeploymentList, error) {
	f.deploymentNamespace = namespace
	return f.deployments, f.deploymentErr
}

func newTestSource(client ResourceClient) (*Source, *Metrics) {
	m := NewMetrics(prometheus.NewRegistry())
	s := NewSource(client, "payments", time.Minute, time.Second, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return s, m
}

func TestCollectPublishesKubernetesSnapshot(t *testing.T) {
	desired := int32(3)
	client := &fakeClient{
		pods: &corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "api-0"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", RestartCount: 2}}, InitContainerStatuses: []corev1.ContainerStatus{{Name: "init", RestartCount: 1}}}},
			{ObjectMeta: metav1.ObjectMeta{Namespace: "payments"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
		}},
		deployments: &appsv1.DeploymentList{Items: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "api"}, Spec: appsv1.DeploymentSpec{Replicas: &desired}, Status: appsv1.DeploymentStatus{ReadyReplicas: 2, AvailableReplicas: 1, UpdatedReplicas: 2}}}},
	}
	source, metrics := newTestSource(client)
	if err := source.Collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if client.podNamespace != "payments" || client.deploymentNamespace != "payments" {
		t.Fatal("namespace selection was not applied to both API calls")
	}
	if got := testutil.ToFloat64(metrics.Pods.WithLabelValues("payments", "Running")); got != 1 {
		t.Fatalf("running pods=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.Pods.WithLabelValues("payments", "Pending")); got != 1 {
		t.Fatalf("pending pods=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.Restarts.WithLabelValues("payments", "api-0", "app")); got != 2 {
		t.Fatalf("app restarts=%v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.Restarts.WithLabelValues("payments", "api-0", "init")); got != 1 {
		t.Fatalf("init restarts=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.DeploymentReplicas.WithLabelValues("payments", "api", "desired")); got != 3 {
		t.Fatalf("desired replicas=%v, want 3", got)
	}
	if got := testutil.ToFloat64(metrics.DeploymentReplicas.WithLabelValues("payments", "api", "available")); got != 1 {
		t.Fatalf("available replicas=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.Success); got != 1 {
		t.Fatalf("collection success=%v, want 1", got)
	}
}

func TestCollectionFailurePreservesLastSnapshotAndIsObservable(t *testing.T) {
	client := &fakeClient{pods: &corev1.PodList{Items: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Namespace: "payments"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}}}, deployments: &appsv1.DeploymentList{}}
	source, metrics := newTestSource(client)
	if err := source.Collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	client.deploymentErr = apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "api", errors.New("denied"))
	if err := source.Collect(context.Background()); err == nil {
		t.Fatal("expected API error")
	}
	if got := testutil.ToFloat64(metrics.Success); got != 0 {
		t.Fatalf("collection success=%v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.Errors.WithLabelValues("forbidden")); got != 1 {
		t.Fatalf("forbidden errors=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.Pods.WithLabelValues("payments", "Running")); got != 1 {
		t.Fatalf("last good pod snapshot was cleared: %v", got)
	}
}

func TestSetupErrorDoesNotRequireAConnectedCluster(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	m.RecordSetupError(errors.New("no kubeconfig"))
	if testutil.ToFloat64(m.Enabled) != 1 || testutil.ToFloat64(m.Success) != 0 || testutil.ToFloat64(m.Errors.WithLabelValues("configuration")) != 1 {
		t.Fatal("Kubernetes setup failure was not exposed in Prometheus metrics")
	}
}
