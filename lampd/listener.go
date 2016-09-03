package main

import (
	"net"
	"os"
	"strings"
	"time"
)

func queryServer(c net.Conn) error {
	for {
		t1 := time.Now()
		remote := c.RemoteAddr().String()
		if remote == "" {
			remote = "unknown"
		}
		log.Debugf("incoming request from: %s to %s", remote, c.LocalAddr().String())
		c.SetDeadline(time.Now().Add(time.Duration(5) * time.Second))
		defer c.Close()

		req, err := ParseRequest(c)
		if err != nil {
			return SendResponse(c, &Response{Code: 400, Request: req, Error: err})
		}

		if req.Command != "" {
			// commands do not send anything back
			err := SendPeerCommands(req)
			return err
		}

		response, err := BuildResponse(req)
		if err != nil {
			return SendResponse(c, &Response{Code: 400, Request: req, Error: err})
		}

		err = SendResponse(c, response)
		duration := time.Since(t1)
		log.Infof("incoming %s request from %s to %s finished in %s", req.Table, remote, c.LocalAddr().String(), duration.String())
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
			_, Ok := backendsMap[p.Id]
			if !Ok {
				continue
			}
		}
		go func() {
			p.Command(&req.Command)
			// TODO: schedule update
		}()
	}
	return
}

func localListener(listen string) {
	connType := "unix"
	if strings.Contains(listen, ":") {
		connType = "tcp"
	} else {
		// remove socket on exit
		// TODO: check if it works
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

	for {
		fd, err := l.Accept()
		if err != nil {
			log.Errorf("accept error", err.Error())
			return
		}

		go queryServer(fd)
	}
}
