package lmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
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
	connection            net.Conn
	lmd                   *Daemon
	keepAliveTimer        *time.Timer
	curRequest            *Request
	localAddr             string
	remoteAddr            string
	listenTimeout         int
	logSlowQueryThreshold int
	logHugeQueryThreshold int
	keepAlive             bool
}

type commandsPerReq struct {
	req      *Request
	commands []string
}

// NewClientConnection creates a new client connection object.
func NewClientConnection(lmd *Daemon, c net.Conn, listenTimeout, logSlowThreshold, logHugeThreshold int) *ClientConnection {
	clCon := &ClientConnection{
		lmd:                   lmd,
		connection:            c,
		localAddr:             c.LocalAddr().String(),
		remoteAddr:            c.RemoteAddr().String(),
		keepAlive:             false,
		listenTimeout:         listenTimeout,
		logSlowQueryThreshold: logSlowThreshold,
		logHugeQueryThreshold: logHugeThreshold,
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
			LogErrors((&Response{code: ReturnCodeBadRequest, request: &Request{}, err: err}).Send(cl))

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
	LogErrors((&Response{code: ReturnCodeBadRequest, request: &Request{}, err: err}).Send(cl))

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
	commandsToSend := make(map[string]commandsPerReq)
	for _, req := range reqs {
		cl.keepAlive = req.KeepAlive
		cl.curRequest = req
		reqCtx := context.WithValue(ctx, CtxRequest, req.ID())
		time1 := time.Now()
		if req.Command != "" {
			for _, pID := range req.BackendsMap {
				entry, ok := commandsToSend[pID]
				if !ok {
					commandsToSend[pID] = commandsPerReq{req: req}
					entry = commandsToSend[pID]
				}
				entry.commands = append(entry.commands, strings.TrimSpace(req.Command))
				commandsToSend[pID] = entry
			}

			continue
		}

		// send all pending commands so far when there is a normal query next
		err = cl.sendRemainingCommands(ctx, commandsToSend)
		if err != nil {
			return err
		}

		LogErrors(cl.connection.SetDeadline(time.Now().Add(time.Duration(cl.listenTimeout) * time.Second)))

		var size int64
		var numRows int
		size, numRows, err = cl.processRequest(ctx, req)

		duration := time.Since(time1)
		logWith(reqCtx).Infof("%13s client request finished: duration %12s | response size: %8s | rows:%8d | backends:%4d",
			req.Table.String(),
			duration.Truncate(time.Millisecond).String(),
			byteCountBinary(size),
			numRows,
			len(req.BackendsMap))
		if duration-time.Duration(req.WaitTimeout)*time.Millisecond > time.Duration(cl.logSlowQueryThreshold)*time.Second {
			logWith(reqCtx).Warnf("slow client query finished after %s, response size: %s\n%s", duration.String(), byteCountBinary(size), strings.TrimSpace(req.String()))
		} else if size > int64(cl.logHugeQueryThreshold*1024*1024) {
			logWith(reqCtx).Warnf("huge client query finished after %s, response size: %s\n%s", duration.String(), byteCountBinary(size), strings.TrimSpace(req.String()))
		}
		if cl.lmd.qStat != nil {
			cl.lmd.qStat.in <- QueryStatIn{
				query:    req.String(),
				duration: duration,
			}
		}
		promFrontendRequestDuration.Observe(float64(duration / time.Second))
		if err != nil || !req.KeepAlive {
			return err
		}
	}

	// send all remaining commands
	err = cl.sendRemainingCommands(ctx, commandsToSend)
	if err != nil {
		return err
	}

	for _, req := range reqs {
		if req.Command != "" && (len(req.BackendErrors) > 0 || req.ResponseFixed16) {
			cl.sendCommandErrors(req)

			if len(req.BackendErrors) > 0 {
				return fmt.Errorf("commands failed to send")
			}
		}
	}

	return nil
}

func (cl *ClientConnection) processRequest(ctx context.Context, req *Request) (size int64, numRows int, err error) {
	cl.curRequest = req
	defer func() {
		cl.curRequest = nil
	}()
	defer cl.lmd.logPanicExitClient(cl)
	size, numRows, err = req.BuildResponseSend(ctx, cl)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) {
			LogErrors((&Response{code: ReturnCodeConnectionError, request: req, err: netErr}).Send(cl))

			return size, numRows, err
		}

		var peerErr *PeerError
		if errors.As(err, &peerErr) && peerErr.kind == ConnectionError {
			LogErrors((&Response{code: ReturnCodeConnectionError, request: req, err: peerErr}).Send(cl))

			return size, numRows, err
		}
		LogErrors((&Response{code: ReturnCodeBadRequest, request: req, err: err}).Send(cl))

		return size, numRows, err
	}

	return size, numRows, err
}

// sendRemainingCommands sends all queued commands.
func (cl *ClientConnection) sendRemainingCommands(ctx context.Context, commands map[string]commandsPerReq) (err error) {
	if len(commands) == 0 {
		return err
	}
	time1 := time.Now()
	err = cl.sendCommandsDo(ctx, commands)

	// clear the commands queue
	for k := range commands {
		delete(commands, k)
	}

	if err != nil {
		logWith(cl).Infof("incoming command request failed in %s: %s", time.Since(time1), err.Error())

		return err
	}

	logWith(cl).Infof("incoming command request finished in %s", time.Since(time1))

	return nil
}

// sendCommandsDo sends commands for this request to all selected remote sites.
// It returns any error encountered.
func (cl *ClientConnection) sendCommandsDo(ctx context.Context, commands map[string]commandsPerReq) error {
	if cl.lmd.flags.flagImport != "" {
		return &PeerCommandError{
			code: ReturnCodeInternalError,
			err:  fmt.Errorf("lmd started with -import from file, cannot send commands without real backend connection"),
		}
	}

	resultChan := make(chan error, len(commands))
	wgroup := &sync.WaitGroup{}

	for pID, command := range commands {
		req := command.req
		reqCtx := context.WithValue(ctx, CtxRequest, req.ID())
		peer := cl.lmd.peerMap.Get(pID)
		wgroup.Add(1)
		go func(peer *Peer) {
			defer wgroup.Done()
			defer logPanicExitPeer(peer)
			resultChan <- peer.sendCommandsWithRetry(reqCtx, command.commands)
		}(peer)
	}

	// Wait up to 9.5 seconds for all commands being sent
	if waitTimeout(ctx, wgroup, PeerCommandTimeout) {
		return &PeerCommandError{
			code: ReturnCodeCommandDelayed,
			err:  fmt.Errorf("sending command timed out but will continue in background"),
		}
	}

	// collect errors
	for {
		select {
		case err := <-resultChan:
			if err != nil {
				logWith(ctx).Debugf("command failed: %s", err.Error())

				// sort back into the request
				var peerCmdErr *PeerCommandError
				if errors.As(err, &peerCmdErr) {
					for pID, command := range commands {
						req := command.req
						if peerCmdErr.peer != nil && peerCmdErr.peer.ID == pID {
							req.BackendErrors[pID] = peerCmdErr
						}
					}
				} else {
					return err
				}
			}
		default:
			// no more results
			return nil
		}
	}
}

func (cl *ClientConnection) sendCommandErrors(req *Request) {
	switch req.OutputFormat {
	case OutputFormatWrappedJSON:
		buf := new(bytes.Buffer)
		json := jsoniter.ConfigCompatibleWithStandardLibrary.BorrowStream(buf)
		defer jsoniter.ConfigCompatibleWithStandardLibrary.ReturnStream(json)

		json.WriteRaw("{\"failed\": {")
		num := 0
		for k, v := range req.BackendErrors {
			if num > 0 {
				json.WriteMore()
			}
			json.WriteObjectField(k)
			json.WriteString(strings.TrimSpace(v.Error()))
			num++
		}
		json.WriteObjectEnd()
		json.WriteObjectEnd()
		json.WriteRaw("\n")

		err := json.Flush()
		if err != nil {
			log.Debugf("json error: %s", err.Error())

			return
		}
		json.Reset(nil)

		if req.ResponseFixed16 {
			result := buf.Bytes()
			headerFixed16 := fmt.Sprintf("%d %11d\n", ReturnCodeOK, len(result))
			buf = new(bytes.Buffer)
			buf.WriteString(headerFixed16)
			buf.Write(result)
		}

		_, err = buf.WriteTo(cl.connection)
		if err != nil {
			log.Debugf("failed to write error string back to client, probably disconnected already (%s)", err.Error())

			return
		}

		return

	default:
		for _, err := range req.BackendErrors {
			_, cErr := fmt.Fprintf(cl.connection, "%s\n", err.Error())
			if cErr != nil {
				log.Debugf("failed to write error string back to client, probably disconnected already (%s)", cErr.Error())

				return
			}
		}
	}
}
