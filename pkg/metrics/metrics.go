package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Watcher Metrics
	WatcherEventsCapturedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cdc_watcher_events_total",
		Help: "The total number of events captured from MongoDB change stream",
	})
	WatcherPublishErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cdc_watcher_publish_errors_total",
		Help: "The total number of errors occurred while publishing to Kafka",
	})
	WatcherTokenSavesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cdc_watcher_token_saves_total",
		Help: "The total number of resume token saves to storage",
	})

	// Syncer Metrics
	SyncerMessagesConsumedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cdc_syncer_messages_consumed_total",
		Help: "The total number of messages consumed from Kafka",
	})
	SyncerBatchWritesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cdc_syncer_batch_writes_total",
		Help: "The total number of batch write operations to PostgreSQL",
	})
	SyncerWriteErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cdc_syncer_write_errors_total",
		Help: "The total number of errors occurred during PostgreSQL writes",
	})
	SyncerUpsertLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cdc_syncer_upsert_latency_seconds",
		Help:    "Latency of PostgreSQL UPSERT operations",
		Buckets: prometheus.DefBuckets,
	})
)
