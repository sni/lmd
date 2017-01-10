package main

import (
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	promFrontendConnections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "frontend",
			Name:      "connections",
			Help:      "Frontend Connection Counter",
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
	promPeerUpdatedHosts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "updated_hosts",
			Help:      "Peer Updated Hosts Counter",
		},
		[]string{"peer"},
	)
	promPeerUpdatedServices = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "updated_services",
			Help:      "Peer Updated Services Counter",
		},
		[]string{"peer"},
	)

	promHostCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "host_num",
			Help:      "Number of hosts",
		},
		[]string{"peer"},
	)
	promServiceCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: NAME,
			Subsystem: "peer",
			Name:      "service_num",
			Help:      "Number of services",
		},
		[]string{"peer"},
	)
)

func initPrometheus() (prometheusListener net.Listener) {
	// TODO: implement TLS support
	if GlobalConfig.ListenPrometheus != "" {
		var err error
		prometheusListener, err = net.Listen("tcp", GlobalConfig.ListenPrometheus)
		go func() {
			// make sure we log panics properly
			defer logPanicExit()

			if err != nil {
				log.Fatalf("starting prometheus exporter failed: %s", err)
			}
			http.Serve(prometheusListener, nil)
		}()
		log.Infof("serving prometheus metrics at %s/metrics", GlobalConfig.ListenPrometheus)
	}
	prometheus.Register(promFrontendConnections)
	prometheus.Register(promFrontendBytesSend)
	prometheus.Register(promFrontendBytesReceived)
	prometheus.Register(promPeerUpdateInterval)
	prometheus.Register(promPeerConnections)
	prometheus.Register(promPeerFailedConnections)
	prometheus.Register(promPeerBytesSend)
	prometheus.Register(promPeerBytesReceived)
	prometheus.Register(promPeerUpdates)
	prometheus.Register(promPeerUpdateDuration)
	prometheus.Register(promPeerUpdatedHosts)
	prometheus.Register(promPeerUpdatedServices)
	prometheus.Register(promHostCount)
	prometheus.Register(promServiceCount)
	return prometheusListener
}
