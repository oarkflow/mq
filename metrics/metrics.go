package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	TasksProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tasks_processed_total",
			Help: "Total number of processed tasks.",
		},
		[]string{"status"},
	)
	TasksErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tasks_errors_total",
			Help: "Total number of errors encountered while processing tasks.",
		},
		[]string{"node"},
	)
)

func init() {
	prometheus.MustRegister(TasksProcessed)
	prometheus.MustRegister(TasksErrors)
}

func HandleHTTP() {
	http.Handle("/metrics", promhttp.Handler())
}
