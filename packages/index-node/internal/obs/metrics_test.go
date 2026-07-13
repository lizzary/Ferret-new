package obs

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetricsRegistersCompleteMetricSet(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	metrics.TasksBacklog.WithLabelValues("pending").Set(4)
	metrics.StageDurationSeconds.WithLabelValues("io").Observe(0.01)
	metrics.StageThroughputTotal.WithLabelValues("io").Inc()
	metrics.RetriesTotal.WithLabelValues("transient").Inc()
	metrics.ReconcileDiffTotal.WithLabelValues("test-root").Inc()
	metrics.BreakerState.WithLabelValues("compute").Set(0)
	metrics.PoolInflight.WithLabelValues("io").Set(1)
	metrics.SearchDurationSeconds.WithLabelValues("hybrid").Observe(0.01)

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	names := make(map[string]bool, len(families))
	for _, family := range families {
		names[family.GetName()] = true
	}
	for _, name := range []string{
		"tasks_backlog",
		"task_oldest_pending_age_seconds",
		"stage_duration_seconds",
		"stage_throughput_total",
		"retries_total",
		"dead_letters_total",
		"dead_letters_size",
		"reconcile_diff_total",
		"watch_overflow_total",
		"breaker_state",
		"pool_inflight",
		"tantivy_commit_duration_seconds",
		"vector_index_size",
		"vector_tombstone_ratio",
		"search_duration_seconds",
		"notes_expired_reaped_total",
	} {
		if !names[name] {
			t.Errorf("metric %q was not registered", name)
		}
	}
}

func TestNewMetricsRollsBackOnRegistrationFailure(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	first, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("first metrics: %v", err)
	}
	if _, err := NewMetrics(registry); err == nil {
		t.Fatal("second registration unexpectedly succeeded")
	}

	first.TasksBacklog.WithLabelValues("pending").Set(1)
	if _, err := registry.Gather(); err != nil {
		t.Fatalf("first metric set was damaged by rollback: %v", err)
	}
}

func TestReconcileDiffRootLabelHonorsPathRedaction(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metrics, err := NewMetricsWithOptions(registry, MetricsOptions{RedactPaths: true})
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	rawRoot := `C:\Users\alice\private`
	metrics.ReconcileDiffTotal.WithLabelValues(rawRoot).Inc()

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "reconcile_diff_total" {
			continue
		}
		got := family.GetMetric()[0].GetLabel()[0].GetValue()
		if got == rawRoot || !strings.HasPrefix(got, "sha256:") {
			t.Fatalf("root label = %q, want a redacted hash", got)
		}
		return
	}
	t.Fatal("reconcile_diff_total was not gathered")
}
