package main

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// QueryServer handles a single client connection.
// It returns any error encountered.
func QueryServer(c net.Conn) error {
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
			c.SetDeadline(time.Now().Add(time.Duration(10) * time.Second))
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
		if len(reqs) > 0 {
			keepAlive, err = ProcessRequests(reqs, c, remote)

			// keep open keepalive request until either the client closes the connection or the deadline timeout is hit
			if keepAlive {
				log.Debugf("keepalive connection from %s, waiting for more requests", remote)
				c.SetDeadline(time.Now().Add(time.Duration(10) * time.Second))
				continue
			}
		} else if keepAlive {
			// wait up to deadline after the last keep alive request
			time.Sleep(100 * time.Millisecond)
			continue
		} else {
			err = errors.New("bad request: empty request")
			(&Response{Code: 400, Request: &Request{}, Error: err}).Send(c)
			return err
		}

		return err
	}
}

// ProcessRequests creates response for all given requests
func ProcessRequests(reqs []*Request, c net.Conn, remote string) (bool, error) {
	if len(reqs) == 0 {
		return false, nil
	}
	commandsByPeer := make(map[string][]string)
	for _, req := range reqs {
		t1 := time.Now()
		if req.Command != "" {
			for _, pID := range req.BackendsMap {
				commandsByPeer[pID] = append(commandsByPeer[pID], strings.TrimSpace(req.Command))
			}
		} else {
			// send all pending commands so far
			if len(commandsByPeer) > 0 {
				SendCommands(&commandsByPeer)
				commandsByPeer = make(map[string][]string)
				log.Infof("incoming command request from %s to %s finished in %s", remote, c.LocalAddr().String(), time.Since(t1))
			}
			if req.WaitTrigger != "" {
				c.SetDeadline(time.Now().Add(time.Duration(req.WaitTimeout+1000) * time.Millisecond))
			}
			if req.Table == "log" {
				c.SetDeadline(time.Now().Add(time.Duration(60) * time.Second))
			}
			response, rErr := req.GetResponse()
			if rErr != nil {
				if netErr, ok := rErr.(net.Error); ok {
					(&Response{Code: 500, Request: req, Error: netErr}).Send(c)
					return false, netErr
				}
				(&Response{Code: 400, Request: req, Error: rErr}).Send(c)
				return false, rErr
			}

			size, sErr := response.Send(c)
			duration := time.Since(t1)
			log.Infof("incoming %s request from %s to %s finished in %s, size: %.3f kB", req.Table, remote, c.LocalAddr().String(), duration.String(), float64(size)/1024)
			if sErr != nil {
				return false, sErr
			}
			if !req.KeepAlive {
				return false, nil
			}
		}
	}

	// send all remaining commands
	if len(commandsByPeer) > 0 {
		t1 := time.Now()
		SendCommands(&commandsByPeer)
		log.Infof("incoming command request from %s to %s finished in %s", remote, c.LocalAddr().String(), time.Since(t1))
	}

	return reqs[len(reqs)-1].KeepAlive, nil
}

// SendCommands sends commands for this request to all selected remote sites.
// It returns any error encountered.
func SendCommands(commandsByPeer *map[string][]string) {
	wg := &sync.WaitGroup{}
	for pID := range *commandsByPeer {
		p := DataStore[pID]
		wg.Add(1)
		go func(peer *Peer) {
			// make sure we log panics properly
			defer wg.Done()
			defer logPanicExitPeer(peer)
			commandRequest := &Request{
				Command: strings.Join((*commandsByPeer)[peer.ID], "\n\n"),
			}
			peer.PeerLock.Lock()
			peer.Status["LastQuery"] = time.Now().Unix()
			if peer.Status["Idling"].(bool) {
				peer.Status["Idling"] = false
				log.Infof("[%s] switched back to normal update interval", peer.Name)
			}
			peer.PeerLock.Unlock()
			_, err := peer.Query(commandRequest)
			if err != nil {
				log.Warnf("[%s] sending command failed: %s", peer.Name, err.Error())
				return
			}
			log.Infof("[%s] send %d commands successfully.", peer.Name, len((*commandsByPeer)[peer.ID]))

			// schedule immediate update
			peer.ScheduleImmediateUpdate()
		}(p)
	}
	// Wait up to 10 seconds for all commands being sent
	waitTimeout(wg, 10)
}

// LocalListener starts a listening socket.
func LocalListener(LocalConfig *Config, listen string, waitGroupInit *sync.WaitGroup, waitGroupDone *sync.WaitGroup, shutdownChannel chan bool) {
	defer waitGroupDone.Done()
	waitGroupDone.Add(1)
	if strings.HasPrefix(listen, "https://") {
		listen = strings.TrimPrefix(listen, "https://")
		LocalListenerHTTP(LocalConfig, "https", listen, waitGroupInit, shutdownChannel)
	} else if strings.HasPrefix(listen, "http://") {
		listen = strings.TrimPrefix(listen, "http://")
		LocalListenerHTTP(LocalConfig, "http", listen, waitGroupInit, shutdownChannel)
	} else if strings.Contains(listen, ":") {
		listen = strings.TrimPrefix(listen, "*") // * means all interfaces
		LocalListenerLivestatus(LocalConfig, "tcp", listen, waitGroupInit, shutdownChannel)
	} else {
		// remove stale sockets on start
		if _, err := os.Stat(listen); err == nil {
			log.Warnf("removing stale socket: %s", listen)
			os.Remove(listen)
		}
		LocalListenerLivestatus(LocalConfig, "unix", listen, waitGroupInit, shutdownChannel)
	}
}

// LocalListenerLivestatus starts a listening socket with livestatus protocol.
func LocalListenerLivestatus(LocalConfig *Config, connType string, listen string, waitGroupInit *sync.WaitGroup, shutdownChannel chan bool) {
	l, err := net.Listen(connType, listen)
	if err != nil {
		log.Fatalf("listen error: %s", err.Error())
		return
	}
	defer l.Close()
	if connType == "unix" {
		defer os.Remove(listen)
	}
	defer log.Infof("%s listener %s shutdown complete", connType, listen)
	log.Infof("listening for incoming queries on %s %s", connType, listen)

	// Close connection and log shutdown
	go func() {
		<-shutdownChannel
		log.Infof("stopping %s listener on %s", connType, listen)
		l.Close()
	}()

	waitGroupInit.Done()

	for {
		fd, err := l.Accept()
		if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
			continue
		}
		if err != nil {
			// connection closed by shutdown channel
			return
		}

		// process client request with a timeout
		ch := make(chan error, 1)
		go func() {
			// make sure we log panics properly
			defer logPanicExit()

			ch <- QueryServer(fd)
		}()
		select {
		case <-ch:
		// request finishes normally
		case <-time.After(time.Duration(LocalConfig.ListenTimeout) * time.Second):
			localAddr := fd.LocalAddr().String()
			remote := fd.RemoteAddr().String()
			log.Warnf("client request from %s to %s timed out", remote, localAddr)
		}
		fd.Close()
	}
}

// LocalListenerHTTP starts a listening socket with http protocol.
func LocalListenerHTTP(LocalConfig *Config, httpType string, listen string, waitGroupInit *sync.WaitGroup, shutdownChannel chan bool) {
	// Parse listener address
	listen = strings.TrimPrefix(listen, "*") // * means all interfaces

	// Listener
	var l net.Listener
	if httpType == "https" {
		cer, err := tls.LoadX509KeyPair(LocalConfig.TLSCertificate, LocalConfig.TLSKey)
		if err != nil {
			log.Fatalf("failed to initialize tls %s", err.Error())
		}
		config := &tls.Config{Certificates: []tls.Certificate{cer}}
		ln, err := tls.Listen("tcp", listen, config)
		if err != nil {
			log.Fatalf("listen error: %s", err.Error())
			return
		}
		l = ln
	} else {
		ln, err := net.Listen("tcp", listen)
		if err != nil {
			log.Fatalf("listen error: %s", err.Error())
			return
		}
		l = ln
	}

	// Close connection and log shutdown
	go func() {
		<-shutdownChannel
		log.Infof("stopping listener on %s", listen)
		l.Close()
	}()

	// Initialize HTTP router
	router, err := initializeHTTPRouter()
	if err != nil {
		log.Fatalf("error initializing http server: %s", err.Error())
		return
	}
	log.Infof("listening for rest queries on %s", listen)
	waitGroupInit.Done()

	// Wait for and handle http requests
	server := &http.Server{
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	server.Serve(l)

}
