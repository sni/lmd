package lmd

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// HTTPServerRequestTimeout sets the read/write timeout for the HTTP Server.
	HTTPServerRequestTimeout = 30 * time.Second

	// RequestReadTimeout sets the read timeout when listening to incoming requests.
	RequestReadTimeout = 2 * time.Minute

	// KeepAliveWaitInterval sets the interval at which the listeners checks for new requests in keepalive connections.
	KeepAliveWaitInterval = 100 * time.Millisecond

	// PeerCommandTimeout sets the timeout when waiting for peers to process commands.
	PeerCommandTimeout = 9500 * time.Millisecond
)

// Listener is the object which handles incoming connections.
type Listener struct {
	noCopy           noCopy
	Connection       net.Listener
	Lock             *RWMutex // must be used for when changing config
	lmd              *Daemon
	waitGroupDone    *sync.WaitGroup
	waitGroupInit    *sync.WaitGroup
	queryStats       *QueryStats
	cleanup          func()
	connectionString string
	openConnections  atomic.Int64
}

// NewListener creates a new Listener object.
func NewListener(lmd *Daemon, listen string, qStat *QueryStats) *Listener {
	listener := Listener{
		Lock:             NewRWMutex(),
		lmd:              lmd,
		connectionString: listen,
		waitGroupDone:    lmd.waitGroupListener,
		waitGroupInit:    lmd.waitGroupInit,
		Connection:       nil,
		queryStats:       qStat,
		cleanup:          nil,
	}
	go func() {
		defer lmd.logPanicExit()
		listener.handle()
	}()

	return &listener
}

// handle starts listening on the actual connection.
func (l *Listener) handle() {
	defer func() {
		l.lmd.ListenersLock.Lock()
		delete(l.lmd.Listeners, l.connectionString)
		l.lmd.ListenersLock.Unlock()
		l.waitGroupDone.Done()
	}()
	l.waitGroupDone.Add(1)
	listen := l.connectionString
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
		l.localListenerLivestatus(ConnTypeTLS, listen)
	case strings.Contains(listen, ":"):
		listen = strings.TrimPrefix(listen, "*") // * means all interfaces
		l.localListenerLivestatus(ConnTypeTCP, listen)
	default:
		l.localListenerLivestatus(ConnTypeUnix, listen)
	}
}

// localListenerLivestatus starts a listening socket with livestatus protocol.
func (l *Listener) localListenerLivestatus(connType ConnectionType, listen string) {
	var err error
	var listener net.Listener

	switch connType {
	case ConnTypeTLS:
		l.Lock.RLock()
		tlsConfig, tErr := getTLSListenerConfig(l.lmd.Config)
		l.Lock.RUnlock()
		if tErr != nil {
			l.lmd.cleanFatalf("failed to initialize tls %s", tErr.Error())
		}
		listener, err = tls.Listen("tcp", listen, tlsConfig)
	case ConnTypeTCP:
		listener, err = net.Listen("tcp", listen)
	case ConnTypeUnix:
		// remove stale sockets on start
		_, sErr := os.Stat(listen)
		if sErr == nil {
			log.Warnf("removing stale socket: %s", listen)
			sErr = os.Remove(listen)
			if sErr != nil {
				log.Warnf("removing stale socket failed: %s", sErr)
			}
		}
		listener, err = net.Listen("unix", listen)
	default:
		log.Panicf("not implemented: %#v", connType)
	}

	l.Connection = listener
	if err != nil {
		l.lmd.cleanFatalf("listen error: %s", err.Error())

		return
	}
	defer listener.Close()
	if connType == ConnTypeUnix {
		l.cleanup = func() {
			os.Remove(listen)
		}
	}
	defer log.Infof("%s listener %s shutdown complete", connType, listen)
	log.Infof("listening for incoming queries on %s %s", connType, listen)

	l.waitGroupInit.Done()

	for {
		conn, err := listener.Accept()
		var opErr *net.OpError
		if errors.As(err, &opErr) && opErr.Timeout() {
			continue
		}
		if err != nil {
			log.Infof("stopping %s listener on %s", connType, listen)

			return
		}

		num := l.openConnections.Add(1)
		clConn := NewClientConnection(l.lmd, conn, l.lmd.Config.ListenTimeout, l.lmd.Config.LogSlowQueryThreshold, l.lmd.Config.LogHugeQueryThreshold, l.queryStats)
		promFrontendOpenConnections.WithLabelValues(l.connectionString).Set(float64(num))

		// background waiting for query to finish/timeout
		go func() {
			// make sure we log panics properly
			defer l.lmd.logPanicExit()

			clConn.Handle()
			num := l.openConnections.Add(-1)
			promFrontendOpenConnections.WithLabelValues(l.connectionString).Set(float64(num))
		}()
	}
}

// localListenerHTTP starts a listening socket with http protocol.
func (l *Listener) localListenerHTTP(httpType, listen string) {
	// Parse listener address
	listen = strings.TrimPrefix(listen, "*") // * means all interfaces

	// Listener
	if httpType == "https" {
		l.Lock.RLock()
		tlsConfig, err := getTLSListenerConfig(l.lmd.Config)
		l.Lock.RUnlock()
		if err != nil {
			l.lmd.cleanFatalf("failed to initialize https %s", err.Error())
		}
		listener, err := tls.Listen("tcp", listen, tlsConfig)
		if err != nil {
			l.lmd.cleanFatalf("listen error: %s", err.Error())

			return
		}
		l.Connection = listener
	} else {
		listener, err := net.Listen("tcp", listen)
		if err != nil {
			l.lmd.cleanFatalf("listen error: %s", err.Error())

			return
		}
		l.Connection = listener
	}

	// Initialize HTTP router
	router := initializeHTTPRouter(l.lmd)
	log.Infof("listening for rest queries on %s", listen)
	l.waitGroupInit.Done()

	// Wait for and handle http requests
	server := &http.Server{
		Handler:           router,
		ReadTimeout:       HTTPServerRequestTimeout,
		WriteTimeout:      HTTPServerRequestTimeout,
		ReadHeaderTimeout: HTTPServerRequestTimeout,
	}
	if err := server.Serve(l.Connection); err != nil {
		log.Infof("stopping listener on %s", listen)
		log.Debugf("http listener finished with: %e", err)
	}
}

func (l *Listener) Stop() {
	if l.Connection != nil {
		l.Connection.Close()
		l.Connection = nil
	}
	if l.cleanup != nil {
		l.cleanup()
		l.cleanup = nil
	}
}

func getTLSListenerConfig(localConfig *Config) (config *tls.Config, err error) {
	if localConfig.TLSCertificate == "" || localConfig.TLSKey == "" {
		return nil, fmt.Errorf("TLSCertificate and TLSKey configuration items are required for tls connections")
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
			caCert, err := os.ReadFile(file)
			if err != nil {
				return nil, fmt.Errorf("os.ReadFile: %w", err)
			}
			caCertPool.AppendCertsFromPEM(caCert)
		}

		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = caCertPool
	}

	return
}
