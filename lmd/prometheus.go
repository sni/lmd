package main

import (
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	promPeerConnections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lmd",
			Subsystem: "peer",
			Name: "backend_connections",
			Help: "Peer Backend Connection Counter",
		},
		[]string{"peer"},
	)
	promPeerFailedConnections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lmd",
			Subsystem: "peer",
			Name: "backend_failed_connections",
			Help: "Peer Backend Failed Connection Counter",
		},
		[]string{"peer"},
	)
	promPeerBytesSend = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lmd",
			Subsystem: "peer",
			Name: "sent_bytes",
			Help: "Peer Bytes Sent to Backend Sites",
		},
		[]string{"peer"},
	)
	promPeerBytesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lmd",
			Subsystem: "peer",
			Name: "received_bytes",
			Help: "Peer Bytes Received from Backend Sites",
		},
		[]string{"peer"},
	)
	promPeerUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lmd",
			Subsystem: "peer",
			Name: "updates",
			Help: "Peer Update Counter",
		},
		[]string{"peer"},
	)
	promPeerUpdateDuration = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lmd",
			Subsystem: "peer",
			Name: "update_duration_seconds",
			Help: "Peer Update Duration in Seconds",
		},
		[]string{"peer"},
	)
)

func InitPrometheus() (prometheusListener net.Listener) {
	if GlobalConfig.ListenPrometheus != "" {
		var err error
		prometheusListener, err = net.Listen("tcp", GlobalConfig.ListenPrometheus)
		go func() {
			if err != nil {
				log.Fatalf("starting prometheus exporter failed: %s", err)
			}
			http.Serve(prometheusListener, nil)
		}()
		log.Infof("serving prometheus metrics at %s/metrics", GlobalConfig.ListenPrometheus)
	}
	prometheus.Register(promPeerConnections)
	prometheus.Register(promPeerFailedConnections)
	prometheus.Register(promPeerBytesSend)
	prometheus.Register(promPeerBytesReceived)
	prometheus.Register(promPeerUpdates)
	prometheus.Register(promPeerUpdateDuration)
	return prometheusListener
}
