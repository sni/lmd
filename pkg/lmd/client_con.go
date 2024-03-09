package lmd

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

const (
	ReturnCodeOK              = 200
	ReturnCodeCommandDelayed  = 202
	ReturnCodeBadRequest      = 400
	ReturnCodeInternalError   = 500
	ReturnCodeConnectionError = 502
)

// ClientConnection handles a single client connection.
type ClientConnection struct {
	noCopy                noCopy
	lmd                   *Daemon
	connection            net.Conn
	localAddr             string
	remoteAddr            string
	keepAlive             bool
	keepAliveTimer        *time.Timer
	listenTimeout         int
	logSlowQueryThreshold int
	logHugeQueryThreshold int
	queryStats            *QueryStats
	curRequest            *Request
}

// NewClientConnection creates a new client connection object.
func NewClientConnection(lmd *Daemon, c net.Conn, listenTimeout, logSlowThreshold, logHugeThreshold int, qStat *QueryStats) *ClientConnection {
	clCon := &ClientConnection{
		lmd:                   lmd,
		connection:            c,
		localAddr:             c.LocalAddr().String(),
		remoteAddr:            c.RemoteAddr().String(),
		keepAlive:             false,
		listenTimeout:         listenTimeout,
		logSlowQueryThreshold: logSlowThreshold,
		logHugeQueryThreshold: logHugeThreshold,
		queryStats:            qStat,
	}
	if clCon.remoteAddr == "" {
		clCon.remoteAddr = "unknown"
	}

	return clCon
}

func (cl *ClientConnection) Handle() {
	ctx := context.WithValue(context.Background(), CtxClient, fmt.Sprintf("%s->%s", cl.remoteAddr, cl.localAddr))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cl.keepAliveTimer = time.NewTimer(time.Duration(cl.listenTimeout) * time.Second)
	defer cl.keepAliveTimer.Stop()

	resChan := make(chan error, 1)
	go func() {
		// make sure we log panics properly
		defer cl.lmd.logPanicExit()

		resChan <- cl.answer(ctx)
	}()
	select {
	case err := <-resChan:
		if err != nil {
			logWith(ctx).Debugf("request failed with client error: %s", err.Error())
		}
	case <-cl.keepAliveTimer.C:
		if cl.keepAlive {
			logWith(ctx).Debugf("closing keep alive connection (timeout: %s)", time.Duration(cl.listenTimeout)*time.Second)
		} else {
			logWith(ctx).Warnf("request timed out (timeout: %s)", time.Duration(cl.listenTimeout)*time.Second)
			if cl.curRequest != nil {
				logWith(ctx).Warnf("%s", cl.curRequest.String())
			}
		}
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
				cl.keepAliveTimer.Reset(time.Duration(cl.listenTimeout) * time.Second)

				continue
			}
		case cl.keepAlive:
			// wait up to deadline after the last keep alive request
			time.Sleep(KeepAliveWaitInterval)

			continue
		default:
			err = errors.New("bad request: empty request")
			LogErrors((&Response{Code: ReturnCodeBadRequest, Request: &Request{}, Error: err}).Send(cl.connection))

			return err
		}

		return err
	}
}

// sendErrorResponse creates response for all given requests.
func (cl *ClientConnection) sendErrorResponse(err error) error {
	var netErr net.Error
	if errors.As(err, &netErr) {
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
	LogErrors((&Response{Code: ReturnCodeBadRequest, Request: &Request{}, Error: err}).Send(cl.connection))

	return err
}

// processRequests creates response for all given requests.
func (cl *ClientConnection) processRequests(ctx context.Context, reqs []*Request) (err error) {
	if len(reqs) == 0 {
		return nil
	}
	defer func() {
		cl.curRequest = nil
	}()
	commandsByPeer := make(map[string][]string)
	for _, req := range reqs {
		cl.keepAlive = req.KeepAlive
		cl.curRequest = req
		reqctx := context.WithValue(ctx, CtxRequest, req.ID())
		time1 := time.Now()
		if req.Command != "" {
			for _, pID := range req.BackendsMap {
				commandsByPeer[pID] = append(commandsByPeer[pID], strings.TrimSpace(req.Command))
			}

			continue
		}

		// send all pending commands so far
		err = cl.sendRemainingCommands(reqctx, &commandsByPeer)
		if err != nil {
			return err
		}

		LogErrors(cl.connection.SetDeadline(time.Now().Add(time.Duration(cl.listenTimeout) * time.Second)))

		var size int64
		size, err = cl.processRequest(ctx, req)

		duration := time.Since(time1)
		logWith(reqctx).Infof("%s request finished in %s, response size: %s", req.Table.String(), duration.String(), byteCountBinary(size))
		if duration-time.Duration(req.WaitTimeout)*time.Millisecond > time.Duration(cl.logSlowQueryThreshold)*time.Second {
			logWith(reqctx).Warnf("slow query finished after %s, response size: %s\n%s", duration.String(), byteCountBinary(size), strings.TrimSpace(req.String()))
		} else if size > int64(cl.logHugeQueryThreshold*1024*1024) {
			logWith(reqctx).Warnf("huge query finished after %s, response size: %s\n%s", duration.String(), byteCountBinary(size), strings.TrimSpace(req.String()))
		}
		if cl.queryStats != nil {
			cl.queryStats.In <- QueryStatIn{
				Query:    req.String(),
				Duration: duration,
			}
		}
		promFrontendRequestDuration.Observe(float64(duration / time.Second))
		if err != nil || !req.KeepAlive {
			return err
		}
	}

	// send all remaining commands
	err = cl.sendRemainingCommands(ctx, &commandsByPeer)
	if err != nil {
		return err
	}

	return nil
}

func (cl *ClientConnection) processRequest(ctx context.Context, req *Request) (size int64, err error) {
	cl.curRequest = req
	defer func() {
		cl.curRequest = nil
	}()
	size, err = req.BuildResponseSend(ctx, cl.connection)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) {
			LogErrors((&Response{Code: ReturnCodeConnectionError, Request: req, Error: netErr}).Send(cl.connection))

			return
		}

		var peerErr *PeerError
		if errors.As(err, &peerErr) && peerErr.kind == ConnectionError {
			LogErrors((&Response{Code: ReturnCodeConnectionError, Request: req, Error: peerErr}).Send(cl.connection))

			return
		}
		LogErrors((&Response{Code: ReturnCodeBadRequest, Request: req, Error: err}).Send(cl.connection))

		return
	}

	return
}

// sendRemainingCommands sends all queued commands.
func (cl *ClientConnection) sendRemainingCommands(ctx context.Context, commandsByPeer *map[string][]string) (err error) {
	if len(*commandsByPeer) == 0 {
		return
	}
	time1 := time.Now()
	code, msg := cl.SendCommands(ctx, *commandsByPeer)
	// clear the commands queue
	*commandsByPeer = make(map[string][]string)
	if code != ReturnCodeOK {
		_, err = fmt.Fprintf(cl.connection, "%d: %s\n", code, msg)

		return
	}
	logWith(ctx).Infof("incoming command request finished in %s", time.Since(time1))

	return
}

// SendCommands sends commands for this request to all selected remote sites.
// It returns any error encountered.
func (cl *ClientConnection) SendCommands(ctx context.Context, commandsByPeer map[string][]string) (code int, msg string) {
	code = ReturnCodeOK
	msg = "OK"
	if cl.lmd.flags.flagImport != "" {
		return ReturnCodeInternalError, "lmd started with -import from file, cannot send commands without real backend connection."
	}
	resultChan := make(chan error, len(commandsByPeer))
	wgroup := &sync.WaitGroup{}
	for pID := range commandsByPeer {
		cl.lmd.PeerMapLock.RLock()
		peer := cl.lmd.PeerMap[pID]
		cl.lmd.PeerMapLock.RUnlock()
		wgroup.Add(1)
		go func(peer *Peer) {
			defer logPanicExitPeer(peer)
			defer wgroup.Done()
			resultChan <- peer.SendCommandsWithRetry(ctx, commandsByPeer[peer.ID])
		}(peer)
	}

	// Wait up to 9.5 seconds for all commands being sent
	if waitTimeout(ctx, wgroup, PeerCommandTimeout) {
		return ReturnCodeCommandDelayed, "sending command timed out but will continue in background"
	}

	// collect errors
	for {
		select {
		case err := <-resultChan:
			var commandErr *PeerCommandError
			switch {
			case errors.As(err, &commandErr):
				code = commandErr.code
				msg = commandErr.Error()
			default:
				if err != nil {
					code = ReturnCodeInternalError
					msg = err.Error()
				}
			}
		default:
			return code, msg
		}
	}
}
