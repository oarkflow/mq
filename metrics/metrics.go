package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	taskProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tasks_processed_total",
			Help: "Total number of tasks processed.",
		},
		[]string{"status"},
	)
	taskProcessingTime = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "task_processing_time_seconds",
			Help:    "Histogram of task processing times.",
			Buckets: prometheus.DefBuckets,
		},
	)
)

func init() {
	prometheus.MustRegister(taskProcessed)
	prometheus.MustRegister(taskProcessingTime)
}

func RecordTaskProcessed(status string) {
	taskProcessed.WithLabelValues(status).Inc()
}

func RecordTaskProcessingTime(duration float64) {
	taskProcessingTime.Observe(duration)
}

func StartMetricsServer(port string) {
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(port, nil)
}
