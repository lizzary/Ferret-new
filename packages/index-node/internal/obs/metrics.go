package obs

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns every collector listed in section 6.13 of the specification.
// Instances are explicitly registered, avoiding hidden package globals and
// allowing isolated registries in tests and embedded deployments.
type Metrics struct {
	TasksBacklog                 *prometheus.GaugeVec
	TaskOldestPendingAgeSeconds  prometheus.Gauge
	StageDurationSeconds         *prometheus.HistogramVec
	StageThroughputTotal         *prometheus.CounterVec
	RetriesTotal                 *prometheus.CounterVec
	DeadLettersTotal             prometheus.Counter
	DeadLettersSize              prometheus.Gauge
	ReconcileDiffTotal           *RootCounterVec
	WatchOverflowTotal           prometheus.Counter
	BreakerState                 *prometheus.GaugeVec
	PoolInflight                 *prometheus.GaugeVec
	TantivyCommitDurationSeconds prometheus.Histogram
	VectorIndexSize              prometheus.Gauge
	VectorTombstoneRatio         prometheus.Gauge
	SearchDurationSeconds        *prometheus.HistogramVec
	NotesExpiredReapedTotal      prometheus.Counter
}

// MetricsOptions controls labels that can carry private local information.
type MetricsOptions struct {
	RedactPaths bool
}

// NewMetricsEndpoint creates an isolated registry, registers the complete
// metric set, and returns its HTTP handler. Callers outside obs therefore do
// not depend on Prometheus implementation types.
func NewMetricsEndpoint(options MetricsOptions) (*Metrics, http.Handler, error) {
	registry := prometheus.NewRegistry()
	metrics, err := NewMetricsWithOptions(registry, options)
	if err != nil {
		return nil, nil, err
	}
	return metrics, MetricsHandler(registry), nil
}

// RootCounterVec is a single-label counter vector whose root label is always
// normalized through the configured path privacy policy.
type RootCounterVec struct {
	inner       *prometheus.CounterVec
	redactPaths bool
}

// WithLabelValues returns the counter for one watch root. The method mirrors
// prometheus.CounterVec while ensuring callers cannot bypass redaction.
func (counter *RootCounterVec) WithLabelValues(root string) prometheus.Counter {
	if counter.redactPaths {
		root = RedactPath(root)
	}
	return counter.inner.WithLabelValues(root)
}

// Describe implements prometheus.Collector.
func (counter *RootCounterVec) Describe(ch chan<- *prometheus.Desc) {
	counter.inner.Describe(ch)
}

// Collect implements prometheus.Collector.
func (counter *RootCounterVec) Collect(ch chan<- prometheus.Metric) {
	counter.inner.Collect(ch)
}

// NewMetrics constructs and registers a complete Metrics set. A nil
// registerer uses prometheus.DefaultRegisterer. Registration is transactional:
// collectors already registered by this call are removed if a later one fails.
func NewMetrics(registerer prometheus.Registerer) (*Metrics, error) {
	return NewMetricsWithOptions(registerer, MetricsOptions{})
}

// NewMetricsWithOptions constructs and registers a complete Metrics set with
// an explicit privacy policy for path-bearing labels.
func NewMetricsWithOptions(registerer prometheus.Registerer, options MetricsOptions) (*Metrics, error) {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	metrics := &Metrics{
		TasksBacklog: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tasks_backlog",
			Help: "Number of durable tasks by state.",
		}, []string{"state"}),
		TaskOldestPendingAgeSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "task_oldest_pending_age_seconds",
			Help: "Age in seconds of the oldest pending task.",
		}),
		StageDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "stage_duration_seconds",
			Help:    "Time spent processing an item in a pipeline stage.",
			Buckets: prometheus.DefBuckets,
		}, []string{"stage"}),
		StageThroughputTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "stage_throughput_total",
			Help: "Items completed by each pipeline stage.",
		}, []string{"stage"}),
		RetriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "retries_total",
			Help: "Task retries by error class.",
		}, []string{"class"}),
		DeadLettersTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dead_letters_total",
			Help: "Tasks moved to the dead-letter store.",
		}),
		DeadLettersSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "dead_letters_size",
			Help: "Current number of records in the dead-letter store.",
		}),
		ReconcileDiffTotal: &RootCounterVec{
			inner: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "reconcile_diff_total",
				Help: "Filesystem/catalog differences found while reconciling a root.",
			}, []string{"root"}),
			redactPaths: options.RedactPaths,
		},
		WatchOverflowTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "watch_overflow_total",
			Help: "Watcher overflows that forced a root to be marked dirty.",
		}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "breaker_state",
			Help: "Dependency circuit breaker state (0=closed, 1=half-open, 2=open).",
		}, []string{"dep"}),
		PoolInflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pool_inflight",
			Help: "Work items currently executing in a resource pool.",
		}, []string{"pool"}),
		TantivyCommitDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tantivy_commit_duration_seconds",
			Help:    "Duration of Tantivy batch commits.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
		}),
		VectorIndexSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vector_index_size",
			Help: "Current number of vectors in the ANN index.",
		}),
		VectorTombstoneRatio: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vector_tombstone_ratio",
			Help: "Ratio of vector index entries marked as tombstones.",
		}),
		SearchDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "search_duration_seconds",
			Help:    "End-to-end search latency by search mode.",
			Buckets: prometheus.DefBuckets,
		}, []string{"mode"}),
		NotesExpiredReapedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "notes_expired_reaped_total",
			Help: "Expired notes physically removed by the background reaper.",
		}),
	}

	collectors := []struct {
		name      string
		collector prometheus.Collector
	}{
		{"tasks_backlog", metrics.TasksBacklog},
		{"task_oldest_pending_age_seconds", metrics.TaskOldestPendingAgeSeconds},
		{"stage_duration_seconds", metrics.StageDurationSeconds},
		{"stage_throughput_total", metrics.StageThroughputTotal},
		{"retries_total", metrics.RetriesTotal},
		{"dead_letters_total", metrics.DeadLettersTotal},
		{"dead_letters_size", metrics.DeadLettersSize},
		{"reconcile_diff_total", metrics.ReconcileDiffTotal},
		{"watch_overflow_total", metrics.WatchOverflowTotal},
		{"breaker_state", metrics.BreakerState},
		{"pool_inflight", metrics.PoolInflight},
		{"tantivy_commit_duration_seconds", metrics.TantivyCommitDurationSeconds},
		{"vector_index_size", metrics.VectorIndexSize},
		{"vector_tombstone_ratio", metrics.VectorTombstoneRatio},
		{"search_duration_seconds", metrics.SearchDurationSeconds},
		{"notes_expired_reaped_total", metrics.NotesExpiredReapedTotal},
	}

	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, item := range collectors {
		if err := registerer.Register(item.collector); err != nil {
			for _, collector := range registered {
				registerer.Unregister(collector)
			}
			return nil, fmt.Errorf("register metric %s: %w", item.name, err)
		}
		registered = append(registered, item.collector)
	}

	return metrics, nil
}

// MetricsHandler returns the HTTP handler for the /metrics endpoint. A nil
// gatherer uses prometheus.DefaultGatherer.
func MetricsHandler(gatherer prometheus.Gatherer) http.Handler {
	if gatherer == nil {
		gatherer = prometheus.DefaultGatherer
	}
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
}
