package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type CommandInstrumenter struct {
	cmdLatency *prometheus.HistogramVec
}

func NewCommandInstrumenter() *CommandInstrumenter {
	cmdLatency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "redprox",
		Name:      "redis_command_latency",
		Help:      "Duration in seconds to complete operation to upstream Redis cluster",
		Buckets:   prometheus.ExponentialBuckets(0.005, 2, 10),
	}, []string{"command"})
	prometheus.MustRegister(cmdLatency)

	return &CommandInstrumenter{
		cmdLatency: cmdLatency,
	}
}

func (c CommandInstrumenter) Record(cmd string, duration time.Duration) {
	c.cmdLatency.WithLabelValues(cmd).Observe(duration.Seconds())
}

type Metrics struct {
	cmdErrors        *prometheus.CounterVec
	clusterErrors    prometheus.Counter
	clusterRefreshes prometheus.Counter
	connsAccepted    prometheus.Counter
	connsClosed      prometheus.Counter
}

func NewMetrics() *Metrics {
	cmdError := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redprox",
		Name:      "redis_command_errs",
		Help:      "Count of errors returned by Redis by command",
	}, []string{"command"})
	clusterErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redprox",
		Name:      "cluster_state_errs",
		Help:      "Count of errors refreshing or retrieving the cluster state",
	})
	clusterRefreshes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redprox",
		Name:      "cluster_state_refreshes",
		Help:      "Count of cluster state refreshes",
	})
	connsAccepted := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redprox",
		Name:      "conns_accepted",
		Help:      "Count of connections accepted by Redprox",
	})
	connsClosed := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redprox",
		Name:      "conns_closed",
		Help:      "Count of connections closed by Redprox",
	})
	prometheus.MustRegister(cmdError, clusterErrors, clusterRefreshes, connsAccepted, connsClosed)
	return &Metrics{
		cmdErrors:        cmdError,
		clusterErrors:    clusterErrors,
		clusterRefreshes: clusterRefreshes,
		connsAccepted:    connsAccepted,
		connsClosed:      connsClosed,
	}
}

func (m *Metrics) RecordCommandError(cmd string) {
	m.cmdErrors.WithLabelValues(cmd).Inc()
}

func (m *Metrics) RecordClusterError() {
	m.clusterErrors.Inc()
}

func (m *Metrics) RecordClusterRefresh() {
	m.clusterRefreshes.Inc()
}

func (m *Metrics) RecordConnAccepted() {
	m.connsAccepted.Inc()
}

func (m *Metrics) RecordConnClosed() {
	m.connsClosed.Inc()
}
