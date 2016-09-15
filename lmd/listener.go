package main

import (
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

func queryServer(c net.Conn) error {
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
			return SendResponse(c, &Response{Code: 400, Request: &Request{}, Error: err})
		}
		if len(reqs) == 0 {
			err = errors.New("bad request: empty request")
			return SendResponse(c, &Response{Code: 400, Request: &Request{}, Error: err})
		}
		for _, req := range reqs {
			if req.Command != "" {
				// commands do not send anything back
				err = SendPeerCommands(req)
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
				response, err := BuildResponse(req)
				if err != nil {
					return SendResponse(c, &Response{Code: 400, Request: req, Error: err})
				}

				err = SendResponse(c, response)
				duration := time.Since(t1)
				log.Infof("incoming %s request from %s to %s finished in %s", req.Table, remote, c.LocalAddr().String(), duration.String())
				if err != nil {
					return err
				}
			}
		}

		return err
	}
}

func SendPeerCommands(req *Request) (err error) {
	backendsMap, numBackendsReq, err := ExpandRequestBackends(req)
	if err != nil {
		return
	}
	for _, p := range DataStore {
		if numBackendsReq > 0 {
			_, ok := backendsMap[p.Id]
			if !ok {
				continue
			}
		}
		go func(peer Peer) {
			commandRequest := &Request{
				Command: req.Command,
			}
			peer.PeerLock.Lock()
			peer.Status["LastQuery"] = time.Now()
			peer.PeerLock.Unlock()
			peer.Query(commandRequest)
			// schedule immediate update
			peer.PeerLock.Lock()
			peer.Status["LastUpdate"] = time.Now().Add(-1 * time.Duration(60) * time.Second)
			peer.PeerLock.Unlock()
		}(p)
	}
	return
}

func localListener(listen string, waitGroup *sync.WaitGroup, shutdownChannel chan bool) {
	defer waitGroup.Done()
	waitGroup.Add(1)
	connType := "unix"
	if strings.Contains(listen, ":") {
		connType = "tcp"
	} else {
		// remove socket on exit
		if _, err := os.Stat(listen); err == nil {
			log.Warnf("removing stale socket: %s", listen)
			os.Remove(listen)
		}
		defer func() {
			os.Remove(listen)
		}()
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
			l.SetDeadline(time.Now().Add(5e8)) // set timeout to 0.5 seconds
		}
		if l, ok := l.(*net.UnixListener); ok {
			l.SetDeadline(time.Now().Add(5e8))
		}

		select {
		case <-shutdownChannel:
			log.Infof("stopping listening on %s", listen)
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

		go queryServer(fd)
	}
}
