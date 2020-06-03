package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
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

// QueryServer handles a single client connection.
// It returns any error encountered.
func QueryServer(c net.Conn, conf *Config) error {
	localAddr := c.LocalAddr().String()
	keepAlive := false
	remote := c.RemoteAddr().String()
	defer c.Close()
	if remote == "" {
		remote = "unknown"
	}

	for {
		if !keepAlive {
			promFrontendConnections.WithLabelValues(localAddr).Inc()
			log.Debugf("incoming request from: %s to %s", remote, localAddr)
			c.SetDeadline(time.Now().Add(RequestReadTimeout))
		}

		reqs, err := ParseRequests(c)
		if err != nil {
			if err, ok := err.(net.Error); ok {
				if keepAlive {
					log.Debugf("closing keepalive connection from %s", remote)
				} else {
					log.Debugf("network error from %s: %s", remote, err.Error())
				}
				return err
			}
			(&Response{Code: 400, Request: &Request{}, Error: err}).Send(c)
			return err
		}
		switch {
		case len(reqs) > 0:
			promFrontendQueries.WithLabelValues(localAddr).Add(float64(len(reqs)))
			keepAlive, err = ProcessRequests(reqs, c, remote, conf)

			// keep open keepalive request until either the client closes the connection or the deadline timeout is hit
			if keepAlive {
				log.Debugf("keepalive connection from %s, waiting for more requests", remote)
				c.SetDeadline(time.Now().Add(RequestReadTimeout))
				continue
			}
		case keepAlive:
			// wait up to deadline after the last keep alive request
			time.Sleep(KeepAliveWaitInterval)
			continue
		default:
			err = errors.New("bad request: empty request")
			(&Response{Code: 400, Request: &Request{}, Error: err}).Send(c)
			return err
		}

		return err
	}
}

// ProcessRequests creates response for all given requests
func ProcessRequests(reqs []*Request, c net.Conn, remote string, conf *Config) (keepalive bool, err error) {
	if len(reqs) == 0 {
		return
	}
	commandsByPeer := make(map[string][]string)
	for _, req := range reqs {
		t1 := time.Now()
		if req.Command != "" {
			for _, pID := range req.BackendsMap {
				commandsByPeer[pID] = append(commandsByPeer[pID], strings.TrimSpace(req.Command))
			}
			continue
		}

		// send all pending commands so far
		err = SendRemainingCommands(c, remote, &commandsByPeer)
		if err != nil {
			return
		}

		c.SetDeadline(time.Now().Add(time.Duration(conf.ListenTimeout) * time.Second))
		var response *Response
		response, err = req.GetResponse()
		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				(&Response{Code: 502, Request: req, Error: netErr}).Send(c)
				return
			}
			if peerErr, ok := err.(*PeerError); ok && peerErr.kind == ConnectionError {
				(&Response{Code: 502, Request: req, Error: peerErr}).Send(c)
				return
			}
			(&Response{Code: 400, Request: req, Error: err}).Send(c)
			return
		}

		var size int64
		size, err = response.Send(c)
		duration := time.Since(t1)
		log.Infof("incoming %s request from %s to %s finished in %s, response size: %s", req.Table.String(), remote, c.LocalAddr().String(), duration.String(), ByteCountBinary(size))
		if duration > time.Duration(conf.LogSlowQueryThreshold)*time.Second {
			log.Warnf("slow query finished after %s, response size: %s\n%s", duration.String(), ByteCountBinary(size), strings.TrimSpace(req.String()))
		} else if size > int64(conf.LogHugeQueryThreshold*1024*1024) {
			log.Warnf("huge query finished after %s, response size: %s\n%s", duration.String(), ByteCountBinary(size), strings.TrimSpace(req.String()))
		}
		if err != nil || !req.KeepAlive {
			return
		}
	}

	// send all remaining commands
	err = SendRemainingCommands(c, remote, &commandsByPeer)
	if err != nil {
		return
	}

	return reqs[len(reqs)-1].KeepAlive, nil
}

// SendRemainingCommands sends all queued commands
func SendRemainingCommands(c net.Conn, remote string, commandsByPeer *map[string][]string) (err error) {
	if len(*commandsByPeer) == 0 {
		return
	}
	t1 := time.Now()
	code, msg := SendCommands(*commandsByPeer)
	// clear the commands queue
	*commandsByPeer = make(map[string][]string)
	if code != 200 {
		_, err = c.Write([]byte(fmt.Sprintf("%d: %s\n", code, msg)))
		return
	}
	log.Infof("incoming command request from %s to %s finished in %s", remote, c.LocalAddr().String(), time.Since(t1))
	return
}

// SendCommands sends commands for this request to all selected remote sites.
// It returns any error encountered.
func SendCommands(commandsByPeer map[string][]string) (code int, msg string) {
	code = 200
	msg = "OK"
	resultChan := make(chan error, len(commandsByPeer))
	wg := &sync.WaitGroup{}
	for pID := range commandsByPeer {
		PeerMapLock.RLock()
		p := PeerMap[pID]
		PeerMapLock.RUnlock()
		wg.Add(1)
		go func(peer *Peer) {
			defer logPanicExitPeer(peer)
			defer wg.Done()
			resultChan <- peer.SendCommandsWithRetry(commandsByPeer[peer.ID])
		}(p)
	}

	// Wait up to 9.5 seconds for all commands being sent
	if waitTimeout(wg, PeerCommandTimeout) {
		code = 202
		msg = "sending command timed out but will continue in background"
		return
	}

	// collect errors
	for {
		select {
		case err := <-resultChan:
			switch e := err.(type) {
			case *PeerCommandError:
				code = e.code
				msg = e.Error()
			default:
				if err != nil {
					code = 500
					msg = err.Error()
				}
			}
		default:
			return
		}
	}
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
		l.LocalListenerHTTP("https", listen)
	case strings.HasPrefix(listen, "http://"):
		listen = strings.TrimPrefix(listen, "http://")
		l.LocalListenerHTTP("http", listen)
	case strings.HasPrefix(listen, "tls://"):
		listen = strings.TrimPrefix(listen, "tls://")
		listen = strings.TrimPrefix(listen, "*") // * means all interfaces
		l.LocalListenerLivestatus("tls", listen)
	case strings.Contains(listen, ":"):
		listen = strings.TrimPrefix(listen, "*") // * means all interfaces
		l.LocalListenerLivestatus("tcp", listen)
	default:
		// remove stale sockets on start
		if _, err := os.Stat(listen); err == nil {
			log.Warnf("removing stale socket: %s", listen)
			os.Remove(listen)
		}
		l.LocalListenerLivestatus("unix", listen)
	}
}

// LocalListenerLivestatus starts a listening socket with livestatus protocol.
func (l *Listener) LocalListenerLivestatus(connType string, listen string) {
	var err error
	var c net.Listener
	if connType == "tls" {
		tlsConfig, tErr := getTLSListenerConfig(l.GlobalConfig)
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

		// background waiting for query to finish/timeout
		go func() {
			// make sure we log panics properly
			defer logPanicExit()

			handleConnection(fd, l.GlobalConfig)
		}()
	}
}

// LocalListenerHTTP starts a listening socket with http protocol.
func (l *Listener) LocalListenerHTTP(httpType string, listen string) {
	// Parse listener address
	listen = strings.TrimPrefix(listen, "*") // * means all interfaces

	// Listener
	var c net.Listener
	if httpType == "https" {
		tlsConfig, err := getTLSListenerConfig(l.GlobalConfig)
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
	server.Serve(c)
}

func handleConnection(c net.Conn, localConfig *Config) {
	ch := make(chan error, 1)
	go func() {
		// make sure we log panics properly
		defer logPanicExit()

		ch <- QueryServer(c, localConfig)
	}()
	select {
	case err := <-ch:
		if err != nil {
			localAddr := c.LocalAddr().String()
			remote := c.RemoteAddr().String()
			log.Debugf("client request from %s to %s failed with client error: %s", remote, localAddr, err.Error())
		}
	case <-time.After(time.Duration(localConfig.ListenTimeout) * time.Second):
		localAddr := c.LocalAddr().String()
		remote := c.RemoteAddr().String()
		log.Warnf("client request from %s to %s timed out", remote, localAddr)
	}
	c.Close()
}

func getTLSListenerConfig(localConfig *Config) (config *tls.Config, err error) {
	if localConfig.TLSCertificate == "" || localConfig.TLSKey == "" {
		log.Fatalf("TLSCertificate and TLSKey configuration items are required for tls connections")
	}
	cer, err := tls.LoadX509KeyPair(localConfig.TLSCertificate, localConfig.TLSKey)
	if err != nil {
		return nil, err
	}
	config = &tls.Config{Certificates: []tls.Certificate{cer}}
	if len(localConfig.TLSClientPems) > 0 {
		caCertPool := x509.NewCertPool()
		for _, file := range localConfig.TLSClientPems {
			caCert, err := ioutil.ReadFile(file)
			if err != nil {
				return nil, err
			}
			caCertPool.AppendCertsFromPEM(caCert)
		}

		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = caCertPool
	}
	return
}
