package main

import (
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var acceptInterval = 500 * time.Millisecond

// QueryServer handles a single client connection.
// It returns any error encountered.
func QueryServer(c net.Conn) error {
	localAddr := c.LocalAddr().String()
	for {
		t1 := time.Now()
		remote := c.RemoteAddr().String()
		if remote == "" {
			remote = "unknown"
		}
		promFrontendConnections.WithLabelValues(localAddr).Inc()
		log.Debugf("incoming request from: %s to %s", remote, localAddr)
		c.SetDeadline(time.Now().Add(time.Duration(10) * time.Second))
		defer c.Close()

		reqs, err := ParseRequests(c)
		if err != nil {
			(&Response{Code: 400, Request: &Request{}, Error: err}).Send(c)
			return err
		}
		if len(reqs) == 0 {
			err = errors.New("bad request: empty request")
			(&Response{Code: 400, Request: &Request{}, Error: err}).Send(c)
			return err
		}
		for _, req := range reqs {
			if req.Command != "" {
				// commands do not send anything back
				err = req.SendPeerCommands()
				duration := time.Since(t1)
				log.Infof("incoming command request from %s to %s finished in %s", remote, c.LocalAddr().String(), duration.String())
				if err != nil {
					return err
				}
			} else {
				if req.WaitTrigger != "" {
					c.SetDeadline(time.Now().Add(time.Duration(req.WaitTimeout+1000) * time.Millisecond))
				}
				if req.Table == "log" {
					c.SetDeadline(time.Now().Add(time.Duration(60) * time.Second))
				}
				response, rErr := req.GetResponse()
				if rErr != nil {
					(&Response{Code: 400, Request: req, Error: rErr}).Send(c)
					return rErr
				}

				size, sErr := response.Send(c)
				duration := time.Since(t1)
				log.Infof("incoming %s request from %s to %s finished in %s, size: %.3f kB", req.Table, remote, c.LocalAddr().String(), duration.String(), float64(size)/1024)
				if sErr != nil {
					return sErr
				}
			}
		}

		return err
	}
}

// SendPeerCommands sends commands for this request to all selected remote sites.
// It returns any error encountered.
func (req *Request) SendPeerCommands() (err error) {
	for _, p := range DataStore {
		_, ok := req.BackendsMap[p.ID]
		if !ok {
			continue
		}
		go func(peer Peer) {
			// make sure we log panics properly
			defer logPanicExit()

			commandRequest := &Request{
				Command: req.Command,
			}
			peer.PeerLock.Lock()
			peer.Status["LastQuery"] = time.Now().Unix()
			if peer.Status["Idling"].(bool) {
				peer.Status["Idling"] = false
				log.Infof("[%s] switched back to normal update interval", peer.Name)
			}
			peer.PeerLock.Unlock()
			peer.Query(commandRequest)
			// schedule immediate update
			peer.StatusSet("LastUpdate", time.Now().Unix()-GlobalConfig.Updateinterval)
		}(p)
	}
	return
}

// LocalListener starts a local tcp/unix listening socket.
func LocalListener(listen string, waitGroup *sync.WaitGroup, shutdownChannel chan bool) {
	defer waitGroup.Done()
	waitGroup.Add(1)
	connType := "unix"
	if strings.Contains(listen, ":") {
		connType = "tcp"
	} else {
		// remove stale sockets on start
		if _, err := os.Stat(listen); err == nil {
			log.Warnf("removing stale socket: %s", listen)
			os.Remove(listen)
		}
	}
	l, err := net.Listen(connType, listen)
	if err != nil {
		log.Fatalf("listen error: %s", err.Error())
		return
	}
	log.Infof("listening for incoming querys on %s %s", connType, listen)
	defer l.Close()

	for {
		if l, ok := l.(*net.TCPListener); ok {
			l.SetDeadline(time.Now().Add(acceptInterval)) // set timeout to 0.5 seconds (10ms during tests for faster shutdowns)
		}
		if l, ok := l.(*net.UnixListener); ok {
			l.SetDeadline(time.Now().Add(acceptInterval))
		}

		select {
		case <-shutdownChannel:
			log.Infof("stopping listening on %s", listen)
			if connType == "unix" {
				os.Remove(listen)
			}
			return
		default:
		}
		fd, err := l.Accept()
		if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
			continue
		}
		if err != nil {
			log.Errorf("accept error: %s", err.Error())
			return
		}

		go func() {
			// make sure we log panics properly
			defer logPanicExit()

			QueryServer(fd)
		}()
	}
}
