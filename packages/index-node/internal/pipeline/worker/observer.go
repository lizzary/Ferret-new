package worker

import (
	"time"

	"github.com/lizzary/index-node/internal/errclass"
	"github.com/lizzary/index-node/internal/obs"
)

// MetricsObserver adapts the metrics boundary without exposing Prometheus
// implementation types to the processor itself.
type MetricsObserver struct {
	Metrics *obs.Metrics
}

func (observer MetricsObserver) ObserveStage(stage, outcome string, elapsed time.Duration) {
	if observer.Metrics == nil {
		return
	}
	observer.Metrics.StageDurationSeconds.WithLabelValues(stage).Observe(elapsed.Seconds())
	observer.Metrics.StageThroughputTotal.WithLabelValues(stage).Inc()
	if outcome == "relocate" {
		observer.Metrics.StageThroughputTotal.WithLabelValues("move_fast_path").Inc()
	}
}

func (observer MetricsObserver) ObserveRetry(class errclass.Class) {
	if observer.Metrics != nil {
		observer.Metrics.RetriesTotal.WithLabelValues(class.String()).Inc()
	}
}

func (observer MetricsObserver) ObserveDeadLetter() {
	if observer.Metrics != nil {
		observer.Metrics.DeadLettersTotal.Inc()
	}
}
