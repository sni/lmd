package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// ClientConnection handles a single client connection
type ClientConnection struct {
	noCopy                noCopy
	lmd                   *LMDInstance
	connection            net.Conn
	localAddr             string
	remoteAddr            string
	keepAlive             bool
	listenTimeout         int
	logSlowQueryThreshold int
	logHugeQueryThreshold int
	queryStats            *QueryStats
}

// NewClientConnection creates a new client connection object
func NewClientConnection(lmd *LMDInstance, c net.Conn, listenTimeout int, logSlowQueryThreshold int, logHugeQueryThreshold int, qStat *QueryStats) *ClientConnection {
	cl := &ClientConnection{
		lmd:                   lmd,
		connection:            c,
		localAddr:             c.LocalAddr().String(),
		remoteAddr:            c.RemoteAddr().String(),
		keepAlive:             false,
		listenTimeout:         listenTimeout,
		logSlowQueryThreshold: logSlowQueryThreshold,
		logHugeQueryThreshold: logHugeQueryThreshold,
		queryStats:            qStat,
	}
	if cl.remoteAddr == "" {
		cl.remoteAddr = "unknown"
	}
	return cl
}

func (cl *ClientConnection) Handle() {
	ctx := context.WithValue(context.Background(), CtxClient, fmt.Sprintf("%s->%s", cl.remoteAddr, cl.localAddr))
	ch := make(chan error, 1)
	go func() {
		// make sure we log panics properly
		defer cl.lmd.logPanicExit()

		ch <- cl.answer(ctx)
	}()
	timeout := time.NewTimer(time.Duration(cl.listenTimeout) * time.Second)
	select {
	case err := <-ch:
		timeout.Stop()
		if err != nil {
			logWith(ctx).Debugf("request failed with client error: %s", err.Error())
		}
	case <-timeout.C:
		logWith(ctx).Warnf("request timed out (timeout: %s)", time.Duration(cl.listenTimeout)*time.Second)
	}
	cl.connection.Close()
}

// answer handles a single client connection.
// It returns any error encountered.
func (cl *ClientConnection) answer(ctx context.Context) error {
	defer cl.connection.Close()

	for {
		if !cl.keepAlive {
			promFrontendConnections.WithLabelValues(cl.localAddr).Inc()
			logWith(cl).Debugf("new client connection")
			LogErrors(cl.connection.SetDeadline(time.Now().Add(RequestReadTimeout)))
		}

		reqs, err := ParseRequests(ctx, cl.lmd, cl.connection)
		if err != nil {
			return cl.sendErrorResponse(err)
		}
		switch {
		case len(reqs) > 0:
			promFrontendQueries.WithLabelValues(cl.localAddr).Add(float64(len(reqs)))
			err = cl.processRequests(ctx, reqs)

			// keep open keepalive request until either the client closes the connection or the deadline timeout is hit
			if cl.keepAlive {
				logWith(cl, reqs[len(reqs)-1]).Debugf("connection keepalive, waiting for more requests")
				LogErrors(cl.connection.SetDeadline(time.Now().Add(RequestReadTimeout)))
				continue
			}
		case cl.keepAlive:
			// wait up to deadline after the last keep alive request
			time.Sleep(KeepAliveWaitInterval)
			continue
		default:
			err = errors.New("bad request: empty request")
			LogErrors((&Response{Code: 400, Request: &Request{}, Error: err}).Send(cl.connection))
			return err
		}

		return err
	}
}

// sendErrorResponse creates response for all given requests
func (cl *ClientConnection) sendErrorResponse(err error) error {
	if err, ok := err.(net.Error); ok {
		if cl.keepAlive {
			logWith(cl).Debugf("closing keepalive connection")
		} else {
			logWith(cl).Debugf("network error: %s", err.Error())
		}
		return err
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	LogErrors((&Response{Code: 400, Request: &Request{}, Error: err}).Send(cl.connection))
	return err
}

// processRequests creates response for all given requests
func (cl *ClientConnection) processRequests(ctx context.Context, reqs []*Request) (err error) {
	if len(reqs) == 0 {
		return
	}
	commandsByPeer := make(map[string][]string)
	for _, req := range reqs {
		cl.keepAlive = req.KeepAlive
		reqctx := context.WithValue(ctx, CtxRequest, req.ID())
		t1 := time.Now()
		if req.Command != "" {
			for _, pID := range req.BackendsMap {
				commandsByPeer[pID] = append(commandsByPeer[pID], strings.TrimSpace(req.Command))
			}
			continue
		}

		// send all pending commands so far
		err = cl.sendRemainingCommands(reqctx, &commandsByPeer)
		if err != nil {
			return
		}

		LogErrors(cl.connection.SetDeadline(time.Now().Add(time.Duration(cl.listenTimeout) * time.Second)))
		var response *Response
		response, err = req.GetResponse()
		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				LogErrors((&Response{Code: 502, Request: req, Error: netErr}).Send(cl.connection))
				return
			}
			if peerErr, ok := err.(*PeerError); ok && peerErr.kind == ConnectionError {
				LogErrors((&Response{Code: 502, Request: req, Error: peerErr}).Send(cl.connection))
				return
			}
			LogErrors((&Response{Code: 400, Request: req, Error: err}).Send(cl.connection))
			return
		}

		var size int64
		size, err = response.Send(cl.connection)
		duration := time.Since(t1)
		logWith(reqctx).Infof("%s request finished in %s, response size: %s", req.Table.String(), duration.String(), ByteCountBinary(size))
		if duration-time.Duration(req.WaitTimeout)*time.Millisecond > time.Duration(cl.logSlowQueryThreshold)*time.Second {
			logWith(reqctx).Warnf("slow query finished after %s, response size: %s\n%s", duration.String(), ByteCountBinary(size), strings.TrimSpace(req.String()))
		} else if size > int64(cl.logHugeQueryThreshold*1024*1024) {
			logWith(reqctx).Warnf("huge query finished after %s, response size: %s\n%s", duration.String(), ByteCountBinary(size), strings.TrimSpace(req.String()))
		}
		if cl.queryStats != nil {
			cl.queryStats.In <- QueryStatIn{
				Query:    req.String(),
				Duration: duration,
			}
		}
		promFrontendRequestDuration.Observe(float64(duration / time.Second))
		if err != nil || !req.KeepAlive {
			return
		}
	}

	// send all remaining commands
	err = cl.sendRemainingCommands(ctx, &commandsByPeer)
	if err != nil {
		return
	}

	return nil
}

// sendRemainingCommands sends all queued commands
func (cl *ClientConnection) sendRemainingCommands(ctx context.Context, commandsByPeer *map[string][]string) (err error) {
	if len(*commandsByPeer) == 0 {
		return
	}
	t1 := time.Now()
	code, msg := cl.SendCommands(ctx, *commandsByPeer)
	// clear the commands queue
	*commandsByPeer = make(map[string][]string)
	if code != 200 {
		_, err = fmt.Fprintf(cl.connection, "%d: %s\n", code, msg)
		return
	}
	logWith(ctx).Infof("incoming command request finished in %s", time.Since(t1))
	return
}

// SendCommands sends commands for this request to all selected remote sites.
// It returns any error encountered.
func (cl *ClientConnection) SendCommands(ctx context.Context, commandsByPeer map[string][]string) (code int, msg string) {
	code = 200
	msg = "OK"
	if cl.lmd.flags.flagImport != "" {
		code = 500
		msg = "lmd started with -import from file, cannot send commands without real backend connection."
		return
	}
	resultChan := make(chan error, len(commandsByPeer))
	wg := &sync.WaitGroup{}
	for pID := range commandsByPeer {
		cl.lmd.PeerMapLock.RLock()
		p := cl.lmd.PeerMap[pID]
		cl.lmd.PeerMapLock.RUnlock()
		wg.Add(1)
		go func(peer *Peer) {
			defer logPanicExitPeer(peer)
			defer wg.Done()
			resultChan <- peer.SendCommandsWithRetry(ctx, commandsByPeer[peer.ID])
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
