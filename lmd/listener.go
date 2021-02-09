package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// HTTPServerRequestTimeout sets the read/write timeout for the HTTP Server
	HTTPServerRequestTimeout = 30 * time.Second

	// RequestReadTimeout sets the read timeout when listening to incoming requests
	RequestReadTimeout = 2 * time.Minute

	// KeepAliveWaitInterval sets the interval at which the listeners checks for new requests in keepalive connections
	KeepAliveWaitInterval = 100 * time.Millisecond

	// PeerCommandTimeout sets the timeout when waiting for peers to process commands
	PeerCommandTimeout = 9500 * time.Millisecond
)

// Listener is the object which handles incoming connections
type Listener struct {
	noCopy           noCopy
	ConnectionString string
	shutdownChannel  chan bool
	connection       net.Listener
	GlobalConfig     *Config
	waitGroupDone    *sync.WaitGroup
	waitGroupInit    *sync.WaitGroup
	openConnections  int64
}

// NewListener creates a new Listener object
func NewListener(localConfig *Config, listen string, waitGroupInit *sync.WaitGroup, waitGroupDone *sync.WaitGroup, shutdownChannel chan bool) *Listener {
	l := Listener{
		ConnectionString: listen,
		shutdownChannel:  shutdownChannel,
		GlobalConfig:     localConfig,
		waitGroupDone:    waitGroupDone,
		waitGroupInit:    waitGroupInit,
	}
	go func() {
		defer logPanicExit()
		l.Listen()
	}()
	return &l
}

// Listen start listening the actual connection
func (l *Listener) Listen() {
	defer func() {
		ListenersLock.Lock()
		delete(Listeners, l.ConnectionString)
		ListenersLock.Unlock()
		l.waitGroupDone.Done()
	}()
	l.waitGroupDone.Add(1)
	listen := l.ConnectionString
	switch {
	case strings.HasPrefix(listen, "https://"):
		listen = strings.TrimPrefix(listen, "https://")
		l.localListenerHTTP("https", listen)
	case strings.HasPrefix(listen, "http://"):
		listen = strings.TrimPrefix(listen, "http://")
		l.localListenerHTTP("http", listen)
	case strings.HasPrefix(listen, "tls://"):
		listen = strings.TrimPrefix(listen, "tls://")
		listen = strings.TrimPrefix(listen, "*") // * means all interfaces
		l.localListenerLivestatus("tls", listen)
	case strings.Contains(listen, ":"):
		listen = strings.TrimPrefix(listen, "*") // * means all interfaces
		l.localListenerLivestatus("tcp", listen)
	default:
		// remove stale sockets on start
		if _, err := os.Stat(listen); err == nil {
			log.Warnf("removing stale socket: %s", listen)
			os.Remove(listen)
		}
		l.localListenerLivestatus("unix", listen)
	}
}

// localListenerLivestatus starts a listening socket with livestatus protocol.
func (l *Listener) localListenerLivestatus(connType string, listen string) {
	var err error
	var c net.Listener
	if connType == "tls" {
		tlsConfig, tErr := GetTLSListenerConfig(l.GlobalConfig)
		if tErr != nil {
			log.Fatalf("failed to initialize tls %s", tErr.Error())
		}
		c, err = tls.Listen("tcp", listen, tlsConfig)
	} else {
		c, err = net.Listen(connType, listen)
	}
	l.connection = c
	if err != nil {
		log.Fatalf("listen error: %s", err.Error())
		return
	}
	defer c.Close()
	if connType == "unix" {
		defer os.Remove(listen)
	}
	defer log.Infof("%s listener %s shutdown complete", connType, listen)
	log.Infof("listening for incoming queries on %s %s", connType, listen)

	// Close connection and log shutdown
	go func() {
		defer logPanicExit()
		<-l.shutdownChannel
		log.Infof("stopping %s listener on %s", connType, listen)
		c.Close()
	}()

	l.waitGroupInit.Done()

	for {
		fd, err := c.Accept()
		if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
			continue
		}
		if err != nil {
			// connection closed by shutdown channel
			return
		}

		atomic.AddInt64(&l.openConnections, 1)
		promFrontendOpenConnections.WithLabelValues(l.ConnectionString).Set(float64(atomic.LoadInt64(&l.openConnections)))
		cl := NewClientConnection(fd, l.GlobalConfig.ListenTimeout, l.GlobalConfig.LogSlowQueryThreshold, l.GlobalConfig.LogHugeQueryThreshold)

		// background waiting for query to finish/timeout
		go func() {
			// make sure we log panics properly
			defer logPanicExit()

			cl.Handle()
			atomic.AddInt64(&l.openConnections, -1)
			promFrontendOpenConnections.WithLabelValues(l.ConnectionString).Set(float64(atomic.LoadInt64(&l.openConnections)))
		}()
	}
}

// localListenerHTTP starts a listening socket with http protocol.
func (l *Listener) localListenerHTTP(httpType string, listen string) {
	// Parse listener address
	listen = strings.TrimPrefix(listen, "*") // * means all interfaces

	// Listener
	var c net.Listener
	if httpType == "https" {
		tlsConfig, err := GetTLSListenerConfig(l.GlobalConfig)
		if err != nil {
			log.Fatalf("failed to initialize https %s", err.Error())
		}
		ln, err := tls.Listen("tcp", listen, tlsConfig)
		if err != nil {
			log.Fatalf("listen error: %s", err.Error())
			return
		}
		c = ln
	} else {
		ln, err := net.Listen("tcp", listen)
		if err != nil {
			log.Fatalf("listen error: %s", err.Error())
			return
		}
		c = ln
	}
	l.connection = c

	// Close connection and log shutdown
	go func() {
		<-l.shutdownChannel
		log.Infof("stopping listener on %s", listen)
		c.Close()
	}()

	// Initialize HTTP router
	router := initializeHTTPRouter()
	log.Infof("listening for rest queries on %s", listen)
	l.waitGroupInit.Done()

	// Wait for and handle http requests
	server := &http.Server{
		Handler:      router,
		ReadTimeout:  HTTPServerRequestTimeout,
		WriteTimeout: HTTPServerRequestTimeout,
	}
	if err := server.Serve(c); err != nil {
		log.Debugf("http listener finished with: %e", err)
	}
}

func GetTLSListenerConfig(localConfig *Config) (config *tls.Config, err error) {
	if localConfig.TLSCertificate == "" || localConfig.TLSKey == "" {
		log.Fatalf("TLSCertificate and TLSKey configuration items are required for tls connections")
	}
	cer, err := tls.LoadX509KeyPair(localConfig.TLSCertificate, localConfig.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("tls.LoadX509KeyPair: %s / %s: %w", localConfig.TLSCertificate, localConfig.TLSKey, err)
	}
	config = getMinimalTLSConfig(localConfig)
	config.Certificates = []tls.Certificate{cer}
	if len(localConfig.TLSClientPems) > 0 {
		caCertPool := x509.NewCertPool()
		for _, file := range localConfig.TLSClientPems {
			caCert, err := ioutil.ReadFile(file)
			if err != nil {
				return nil, fmt.Errorf("ioutil.ReadFile: %w", err)
			}
			caCertPool.AppendCertsFromPEM(caCert)
		}

		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = caCertPool
	}
	return
}
