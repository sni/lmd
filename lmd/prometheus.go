package main

import (
	"io"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	promInfoCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Name:      "info",
			Help:      "information about LMD",
		},
		[]string{"version"})

	promCompressionLevel = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "compression_level",
			Help:      "CompressionLevel setting from config",
		},
	)
	promCompressionMinimumSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "compression_minimum_size",
			Help:      "CompressionMinimumSize setting from config",
		},
	)
	promSyncIsExecuting = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "sync_is_executing",
			Help:      "SyncIsExecuting setting from config",
		},
	)
	promSaveTempRequests = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "save_temp_requests",
			Help:      "SaveTempRequests setting from config",
		},
	)
	promBackendKeepAlive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "backend_keepalive",
			Help:      "BackendKeepAlive setting from config",
		},
	)

	promFrontendConnections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "frontend",
			Name:      "connections",
			Help:      "Frontend Connection Counter",
		},
		[]string{"listen"},
	)
	promFrontendQueries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "frontend",
			Name:      "queries",
			Help:      "Listener Query Counter",
		},
		[]string{"listen"},
	)
	promFrontendBytesSend = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "frontend",
			Name:      "send_bytes",
			Help:      "Bytes Send to Frontend Clients",
		},
		[]string{"listen"},
	)
	promFrontendBytesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "frontend",
			Name:      "received_bytes",
			Help:      "Bytes Received from Frontend Clients",
		},
		[]string{"listen"},
	)

	promPeerUpdateInterval = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "update_interval",
			Help:      "Peer Backend Update Interval",
		},
	)
	promPeerFullUpdateInterval = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "full_update_interval",
			Help:      "Peer Backend Full Update Interval",
		},
	)
	promPeerConnections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "backend_connections",
			Help:      "Peer Backend Connection Counter",
		},
		[]string{"peer"},
	)
	promPeerFailedConnections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "backend_failed_connections",
			Help:      "Peer Backend Failed Connection Counter",
		},
		[]string{"peer"},
	)
	promPeerQueries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "backend_queries",
			Help:      "Peer Backend Query Counter",
		},
		[]string{"peer"},
	)
	promPeerBytesSend = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "sent_bytes",
			Help:      "Peer Bytes Sent to Backend Sites",
		},
		[]string{"peer"},
	)
	promPeerBytesReceived = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "received_bytes",
			Help:      "Peer Bytes Received from Backend Sites",
		},
		[]string{"peer"},
	)
	promPeerUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "updates",
			Help:      "Peer Update Counter",
		},
		[]string{"peer"},
	)
	promPeerUpdateDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "update_duration_seconds",
			Help:      "Peer Update Duration in Seconds",
		},
		[]string{"peer"},
	)

	promObjectCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "object_count",
			Help:      "Number of objects",
		},
		[]string{"peer", "type"},
	)

	promObjectUpdate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "updated_objects",
			Help:      "Peer Updated Objects Counter",
		},
		[]string{"peer", "type"},
	)

	promStringDedupCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Name:      "string_dedup_count",
			Help:      "total number of deduplicated strings",
		})

	promStringDedupBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Name:      "string_dedup_bytes",
			Help:      "total bytes of all deduplicated strings",
		})

	promStringDedupIndexBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Name:      "string_dedup_index_bytes",
			Help:      "total bytes for storing the index",
		})
)

func initPrometheus(localConfig *Config) (prometheusListener io.Closer) {
	if localConfig.ListenPrometheus != "" {
		l, err := net.Listen("tcp", localConfig.ListenPrometheus)
		prometheusListener = l
		go func() {
			// make sure we log panics properly
			defer logPanicExit()

			if err != nil {
				log.Fatalf("starting prometheus exporter failed: %s", err)
			}
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			http.Serve(l, mux)
		}()
		log.Infof("serving prometheus metrics at %s/metrics", localConfig.ListenPrometheus)
	}
	prometheus.Register(promInfoCount)
	prometheus.Register(promCompressionLevel)
	prometheus.Register(promCompressionMinimumSize)
	prometheus.Register(promSyncIsExecuting)
	prometheus.Register(promSaveTempRequests)
	prometheus.Register(promBackendKeepAlive)
	prometheus.Register(promFrontendConnections)
	prometheus.Register(promFrontendQueries)
	prometheus.Register(promFrontendBytesSend)
	prometheus.Register(promFrontendBytesReceived)
	prometheus.Register(promPeerUpdateInterval)
	prometheus.Register(promPeerFullUpdateInterval)
	prometheus.Register(promPeerConnections)
	prometheus.Register(promPeerFailedConnections)
	prometheus.Register(promPeerQueries)
	prometheus.Register(promPeerBytesSend)
	prometheus.Register(promPeerBytesReceived)
	prometheus.Register(promPeerUpdates)
	prometheus.Register(promPeerUpdateDuration)
	prometheus.Register(promObjectUpdate)
	prometheus.Register(promObjectCount)
	prometheus.Register(promStringDedupCount)
	prometheus.Register(promStringDedupBytes)
	prometheus.Register(promStringDedupIndexBytes)

	promInfoCount.WithLabelValues(VERSION).Set(1)

	return prometheusListener
}
