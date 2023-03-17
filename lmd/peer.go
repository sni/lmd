package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sasha-s/go-deadlock"
)

var reResponseHeader = regexp.MustCompile(`^(\d+)\s+(\d+)$`)
var reHTTPTooOld = regexp.MustCompile(`Can.t locate object method`)
var reHTTPOMDError = regexp.MustCompile(`<h1>(OMD:.*?)</h1>`)
var reHTTPThrukError = regexp.MustCompile(`(?sm)<!--error:(.*?):error-->`)
var reShinkenVersion = regexp.MustCompile(`\-shinken$`)
var reIcinga2Version = regexp.MustCompile(`^(r[\d.-]+|.*\-icinga2)$`)
var reNaemonVersion = regexp.MustCompile(`\-naemon$`)
var reThrukVersion = regexp.MustCompile(`^(\d+\.\d+|\d+).*?$`)

const (
	// MinFullScanInterval is the minimum interval between two full scans
	MinFullScanInterval = 60

	// UpdateLoopTickerInterval sets the interval for the peer to check if updates should be fetched
	UpdateLoopTickerInterval = 500 * time.Millisecond

	// WaitTimeoutDefault sets the default timeout if nothing specified (1 minute in milliseconds)
	WaitTimeoutDefault = 60000

	// WaitTimeoutCheckInterval set interval in which wait condition is checked
	WaitTimeoutCheckInterval = 200 * time.Millisecond

	// ErrorContentPreviewSize sets the number of bytes from the response to include in the error message
	ErrorContentPreviewSize = 50

	// TemporaryNetworkErrorRetryDelay sets the sleep time for temporary network issue retries
	TemporaryNetworkErrorRetryDelay = 500 * time.Millisecond

	// TemporaryNetworkErrorMaxRetries is the number of retries
	TemporaryNetworkErrorMaxRetries = 3
)

// Peer is the object which handles collecting and updating data and connections.
type Peer struct {
	noCopy          noCopy
	Name            string                        // Name of this peer, aka peer_name
	ID              string                        // ID for this peer, aka peer_key
	ParentID        string                        // ID of parent Peer
	Flags           uint32                        // optional flags, like LMD, Icinga2, etc...
	Source          []string                      // reference to all connection strings
	Lock            *deadlock.RWMutex             // must be used for Peer.* access
	data            *DataStoreSet                 // the cached remote data tables
	Status          map[PeerStatusKey]interface{} // map with all kind of status information
	ErrorCount      int                           // count times this backend has failed
	ErrorLogged     bool                          // flag wether last error has been logged already
	waitGroup       *sync.WaitGroup               // wait group used to wait on shutdowns
	shutdownChannel chan bool                     // channel used to wait to finish shutdown
	stopChannel     chan bool                     // channel to stop this peer
	Config          *Connection                   // reference to the peer configuration from the config file
	lmd             *LMDInstance                  // reference to main lmd instance
	last            struct {
		Request  *Request // reference to last query (used in error reports)
		Response []byte   // reference to last response
	}
	cache struct {
		HTTPClient             *http.Client  // cached http client for http backends
		connectionPool         chan net.Conn // tcp connection get stored here for reuse
		maxParallelConnections chan bool     // limit max parallel connections
	}
}

// PeerStatus contains the different states a peer can have
type PeerStatus uint8

// A peer can be up, warning, down and pending.
// It is pending right after start and warning when the connection fails
// but the stale timeout is not yet hit.
const (
	PeerStatusUp PeerStatus = iota
	PeerStatusWarning
	PeerStatusDown
	PeerStatusBroken // broken flags clients which cannot be used, lmd will check program_start and only retry them if start time changed
	PeerStatusPending
)

// String converts a PeerStatus into a string
func (ps *PeerStatus) String() string {
	switch *ps {
	case PeerStatusUp:
		return "up"
	case PeerStatusWarning:
		return "warning"
	case PeerStatusDown:
		return "down"
	case PeerStatusBroken:
		return "broken"
	case PeerStatusPending:
		return "pending"
	default:
		log.Panicf("not implemented")
	}
	return ""
}

// PeerErrorType is used to distinguish between connection and response errors.
type PeerErrorType uint8

const (
	// ConnectionError is used when the connection to a remote site failed
	ConnectionError PeerErrorType = iota

	// ResponseError is used when the remote site is available but returns an unusable result.
	ResponseError

	// RestartRequiredError is used when the remote site needs to be reinitialized
	RestartRequiredError
)

// PeerStatusKey contains the different keys for the Peer.Status map
type PeerStatusKey int32

// available keys for the peer status map
const (
	_ PeerStatusKey = iota
	PeerKey
	PeerName
	PeerState
	PeerAddr
	CurPeerAddrNum
	LastUpdate
	LastFullUpdate
	LastFullHostUpdate
	LastFullServiceUpdate
	LastTimeperiodUpdateMinute
	LastQuery
	LastError
	LastOnline
	LastPid
	ProgramStart
	BytesSend
	BytesReceived
	Queries
	ResponseTime
	Idling
	Paused
	Section
	PeerParent
	ThrukVersion
	SubKey
	SubName
	SubAddr
	SubType
	SubPeerStatus
	ConfigTool
	ThrukExtras
	ForceFull
	LastHTTPRequestSuccessful
)

// HTTPResult contains the livestatus result as long with some meta data.
type HTTPResult struct {
	Rc      int
	Version string
	Branch  string
	Output  json.RawMessage
	Raw     []byte
	Code    int
	Message string
}

// PeerError is a custom error to distinguish between connection and response errors.
type PeerError struct {
	msg      string
	kind     PeerErrorType
	req      *Request
	res      [][]interface{}
	resBytes []byte
	code     int
	srcErr   error
}

// Error returns the error message as string.
func (e *PeerError) Error() string {
	msg := e.msg
	if e.req != nil {
		msg += fmt.Sprintf("\nRequest: %s", e.req.String())
	}
	if e.res != nil {
		msg += fmt.Sprintf("\nResponse: %#v", e.res)
	}
	if e.resBytes != nil {
		msg += fmt.Sprintf("\nResponse: %s", string(e.resBytes))
	}
	return msg
}

// Type returns the error type.
func (e *PeerError) Type() PeerErrorType { return e.kind }

// PeerCommandError is a custom error when remote site returns something after sending a command
type PeerCommandError struct {
	err  error
	code int
	peer *Peer
}

// Error returns the error message as string.
func (e *PeerCommandError) Error() string {
	return e.err.Error()
}

// NewPeer creates a new peer object.
// It returns the created peer.
func NewPeer(lmd *LMDInstance, config *Connection) *Peer {
	p := Peer{
		Name:            config.Name,
		ID:              config.ID,
		Source:          config.Source,
		Status:          make(map[PeerStatusKey]interface{}),
		waitGroup:       lmd.waitGroupPeers,
		shutdownChannel: lmd.shutdownChannel,
		stopChannel:     make(chan bool),
		Lock:            new(deadlock.RWMutex),
		Config:          config,
		lmd:             lmd,
		Flags:           uint32(NoFlags),
	}
	p.cache.connectionPool = make(chan net.Conn, lmd.Config.MaxParallelPeerConnections)
	p.cache.maxParallelConnections = make(chan bool, lmd.Config.MaxParallelPeerConnections)
	if len(p.Source) == 0 {
		logWith(&p).Fatalf("peer requires at least one source")
	}
	p.Status[PeerKey] = p.ID
	p.Status[PeerName] = p.Name
	p.Status[CurPeerAddrNum] = 0
	p.Status[PeerAddr] = p.Source[p.Status[CurPeerAddrNum].(int)]
	p.Status[PeerState] = PeerStatusPending
	p.Status[LastUpdate] = float64(0)
	p.Status[LastFullUpdate] = float64(0)
	p.Status[LastFullHostUpdate] = float64(0)
	p.Status[LastFullServiceUpdate] = float64(0)
	p.Status[LastQuery] = float64(0)
	p.Status[LastError] = "connecting..."
	p.Status[LastOnline] = float64(0)
	p.Status[LastTimeperiodUpdateMinute] = 0
	p.Status[LastHTTPRequestSuccessful] = false
	p.Status[ForceFull] = false
	p.Status[ProgramStart] = int64(0)
	p.Status[LastPid] = 0
	p.Status[BytesSend] = int64(0)
	p.Status[BytesReceived] = int64(0)
	p.Status[Queries] = int64(0)
	p.Status[ResponseTime] = float64(0)
	p.Status[Idling] = false
	p.Status[Paused] = true
	p.Status[Section] = config.Section
	p.Status[PeerParent] = ""
	p.Status[ThrukVersion] = float64(-1)
	p.Status[SubKey] = []string{}
	p.Status[SubName] = []string{}
	p.Status[SubAddr] = []string{}
	p.Status[SubType] = []string{}

	/* initialize http client if there are any http(s) connections */
	p.SetHTTPClient()

	p.ResetFlags()

	return &p
}

// Start creates the initial objects and starts the update loop in a separate goroutine.
func (p *Peer) Start() {
	if !p.StatusGet(Paused).(bool) {
		logWith(p).Panicf("tried to start updateLoop twice")
	}
	waitgroup := p.waitGroup
	waitgroup.Add(1)
	p.StatusSet(Paused, false)
	logWith(p).Infof("starting connection")
	go func(peer *Peer, wg *sync.WaitGroup) {
		// make sure we log panics properly
		defer logPanicExitPeer(peer)
		peer.updateLoop()
		peer.StatusSet(Paused, true)
		wg.Done()
	}(p, waitgroup)
}

// Stop stops this peer. Restart with Start.
func (p *Peer) Stop() {
	if !p.StatusGet(Paused).(bool) {
		logWith(p).Infof("stopping connection")
		p.stopChannel <- true
	}
}

// SetHTTPClient creates the cached http client (if backend uses HTTP)
func (p *Peer) SetHTTPClient() {
	hasHTTP := false
	for _, addr := range p.Source {
		if strings.HasPrefix(addr, "http") {
			hasHTTP = true
			break
		}
	}
	if !hasHTTP {
		return
	}

	tlsConfig, err := p.getTLSClientConfig()
	if err != nil {
		logWith(p).Fatalf("failed to initialize peer: %s", err.Error())
	}
	client := NewLMDHTTPClient(tlsConfig, p.Config.Proxy)
	client.Timeout = time.Duration(p.lmd.Config.NetTimeout) * time.Second

	logWith(p).Debugf("set new http client cache")
	p.cache.HTTPClient = client
}

func (p *Peer) countFromServer(name string, queryCondition string) (count int) {
	count = -1
	res, _, err := p.QueryString("GET " + name + "\nOutputFormat: json\nStats: " + queryCondition + "\n\n")
	if err == nil && len(res) > 0 && len(res[0]) > 0 {
		count = int(interface2float64(res[0][0]))
	}
	return
}

// updateLoop is the main loop updating this peer.
// It does not return till triggered by the shutdownChannel or by the internal stopChannel.
func (p *Peer) updateLoop() {
	err := p.InitAllTables()
	if err != nil {
		logWith(p).Warnf("initializing objects failed: %s", err.Error())
		p.ErrorLogged = true
	}

	shutdownStop := func(peer *Peer, ticker *time.Ticker) {
		logWith(peer).Debugf("stopping...")
		ticker.Stop()
		peer.clearLastRequest()
	}

	ticker := time.NewTicker(UpdateLoopTickerInterval)
	for {
		err = nil
		t1 := time.Now()
		select {
		case <-p.shutdownChannel:
			shutdownStop(p, ticker)
			return
		case <-p.stopChannel:
			shutdownStop(p, ticker)
			return
		case <-ticker.C:
			switch {
			case p.HasFlag(MultiBackend):
				err = p.periodicUpdateMultiBackends(nil, false)
			default:
				err = p.periodicUpdate()
			}
		}
		duration := time.Since(t1)
		err = p.initTablesIfRestartRequiredError(err)
		if err != nil {
			if !p.ErrorLogged {
				logWith(p).Infof("updating objects failed after: %s: %s", duration.String(), err.Error())
				p.ErrorLogged = true
			} else {
				logWith(p).Debugf("updating objects failed after: %s: %s", duration.String(), err.Error())
			}
		}
		p.clearLastRequest()
	}
}

// periodicUpdate runs the periodic updates from the update loop
func (p *Peer) periodicUpdate() (err error) {
	p.Lock.RLock()
	lastUpdate := p.Status[LastUpdate].(float64)
	lastTimeperiodUpdateMinute := p.Status[LastTimeperiodUpdateMinute].(int)
	lastFullUpdate := p.Status[LastFullUpdate].(float64)
	lastStatus := p.Status[PeerState].(PeerStatus)
	lastQuery := p.Status[LastQuery].(float64)
	idling := p.Status[Idling].(bool)
	forceFull := p.Status[ForceFull].(bool)
	data := p.data
	p.Lock.RUnlock()

	idling = p.updateIdleStatus(idling, lastQuery)
	now := currentUnixTime()
	currentMinute, _ := strconv.Atoi(time.Now().Format("4"))

	// update timeperiods every full minute except when idling
	if !idling && lastTimeperiodUpdateMinute != currentMinute && data != nil {
		p.StatusSet(LastTimeperiodUpdateMinute, currentMinute)
		err = p.periodicTimeperiodsUpdate(data)
		if err != nil {
			return
		}
	}

	nextUpdate := float64(0)
	if idling {
		nextUpdate = lastUpdate + float64(p.lmd.Config.IdleInterval)
	} else {
		nextUpdate = lastUpdate + float64(p.lmd.Config.Updateinterval)
	}
	if now < nextUpdate {
		return
	}

	// set last update timestamp, otherwise we would retry the connection every 500ms instead
	// of the update interval
	p.StatusSet(LastUpdate, now)

	switch lastStatus {
	case PeerStatusBroken:
		return p.handleBrokenPeer()
	case PeerStatusDown, PeerStatusPending:
		return p.InitAllTables()
	case PeerStatusWarning:
		if data == nil {
			logWith(p).Warnf("inconsistent state, no data with state: %s", lastStatus)
			return p.InitAllTables()
		}
		// run update if it was just a short outage
		return data.UpdateFull(Objects.UpdateTables)
	case PeerStatusUp:
		if data == nil {
			logWith(p).Warnf("inconsistent state, no data with state: %s", lastStatus)
			return p.InitAllTables()
		}
		// full update interval
		if !idling && p.lmd.Config.FullUpdateInterval > 0 && now > lastFullUpdate+float64(p.lmd.Config.FullUpdateInterval) {
			return data.UpdateFull(Objects.UpdateTables)
		}
		if forceFull {
			lastUpdate = 0
			p.StatusSet(ForceFull, false)
		}
		return data.UpdateDelta(lastUpdate, now)
	}
	logWith(p).Panicf("unhandled status case: %s", lastStatus)
	return
}

// it fetches the sites table and creates and updates LMDSub backends for them
func (p *Peer) handleBrokenPeer() (err error) {
	var res ResultSet
	res, _, err = p.QueryString("GET status\nOutputFormat: json\nColumns: program_start nagios_pid\n\n")
	if err != nil {
		logWith(p).Debugf("waiting for reload")
		return
	}
	if len(res) > 0 && len(res[0]) == 2 {
		programStart := interface2int64(res[0][0])
		corePid := interface2int(res[0][1])
		if p.StatusGet(ProgramStart) != programStart || p.StatusGet(LastPid) != corePid {
			logWith(p).Debugf("broken peer has reloaded, trying again.")
			return p.InitAllTables()
		}
		return fmt.Errorf("waiting for peer to recover: program_start: %s (%d)  - pid: %d", time.Unix(programStart, 0).String(), programStart, corePid)
	}
	return fmt.Errorf("unknown result while waiting for peer to recover: %v", res)
}

// periodicUpdateLMD runs the periodic updates from the update loop for LMD backends
// it fetches the sites table and creates and updates LMDSub backends for them
func (p *Peer) periodicUpdateLMD(data *DataStoreSet, force bool) (err error) {
	p.Lock.RLock()
	lastUpdate := p.Status[LastUpdate].(float64)
	p.Lock.RUnlock()

	if data == nil {
		data, err = p.GetDataStoreSet()
		if err != nil {
			return
		}
	}

	now := currentUnixTime()
	if !force && now < lastUpdate+float64(p.lmd.Config.Updateinterval) {
		return
	}

	// check main connection and update status table
	err = data.UpdateFull(Objects.StatusTables)
	if err != nil {
		return
	}

	// set last update timestamp, otherwise we would retry the connection every 500ms instead
	// of the update interval
	p.StatusSet(LastUpdate, now)

	columns := []string{"key", "name", "status", "addr", "last_error", "last_update", "last_online", "last_query", "idling"}
	req := &Request{
		Table:   TableSites,
		Columns: columns,
	}
	p.setQueryOptions(req)
	res, _, err := p.query(req)
	if err != nil {
		logWith(p, req).Infof("failed to fetch sites information: %s", err.Error())
		return
	}
	resHash := res.Result2Hash(columns)

	// check if we need to start/stop peers
	logWith(p).Debugf("checking for changed remote lmd backends")
	existing := make(map[string]bool)
	p.lmd.PeerMapLock.Lock()
	defer p.lmd.PeerMapLock.Unlock()
	for _, rowHash := range resHash {
		subID := p.addSubPeer(LMDSub, rowHash["key"].(string), p.Name+"/"+rowHash["name"].(string), rowHash)
		existing[subID] = true
	}

	// remove exceeding peers
	for id := range p.lmd.PeerMap {
		peer := p.lmd.PeerMap[id]
		if peer.ParentID == p.ID {
			if _, ok := existing[id]; !ok {
				logWith(peer, req).Debugf("removing sub peer")
				peer.Stop()
				peer.ClearData(true)
				p.lmd.PeerMapRemove(id)
			}
		}
	}
	return
}

// periodicUpdateMultiBackends runs the periodic updates from the update loop for multi backends
// it fetches the all sites and creates and updates HTTPSub backends for them
func (p *Peer) periodicUpdateMultiBackends(data *DataStoreSet, force bool) (err error) {
	if p.HasFlag(LMD) {
		return p.periodicUpdateLMD(data, force)
	}
	p.Lock.RLock()
	lastUpdate := p.Status[LastUpdate].(float64)
	p.Lock.RUnlock()

	now := currentUnixTime()
	if !force && now < lastUpdate+float64(p.lmd.Config.Updateinterval) {
		return
	}

	if data == nil {
		data, err = p.GetDataStoreSet()
		if err != nil {
			return
		}
	}

	// check main connection and update status table
	err = data.UpdateFull(Objects.StatusTables)
	if err != nil {
		return
	}

	sites, err := p.fetchRemotePeers(data)
	if err != nil {
		logWith(p).Infof("failed to fetch sites information: %s", err.Error())
		p.ErrorLogged = true
		return
	}

	// set last update timestamp, otherwise we would retry the connection every 500ms instead
	// of the update interval
	p.StatusSet(LastUpdate, currentUnixTime())

	// check if we need to start/stop peers
	logWith(p).Debugf("checking for changed remote multi backends")
	existing := make(map[string]bool)
	p.lmd.PeerMapLock.Lock()
	defer p.lmd.PeerMapLock.Unlock()
	for _, siteRow := range sites {
		var site map[string]interface{}
		if s, ok := siteRow.(map[string]interface{}); ok {
			site = s
		} else {
			continue
		}
		subID := p.addSubPeer(HTTPSub, site["id"].(string), site["name"].(string), site)
		existing[subID] = true
	}

	// remove exceeding peers
	for id := range p.lmd.PeerMap {
		peer := p.lmd.PeerMap[id]
		if peer.ParentID == p.ID {
			if _, ok := existing[id]; !ok {
				logWith(peer).Debugf("removing sub peer")
				peer.Stop()
				peer.ClearData(true)
				p.lmd.PeerMapRemove(id)
			}
		}
	}
	return
}

func (p *Peer) updateIdleStatus(idling bool, lastQuery float64) bool {
	now := currentUnixTime()
	shouldIdle := false
	if lastQuery == 0 && p.lmd.lastMainRestart < now-float64(p.lmd.Config.IdleTimeout) {
		shouldIdle = true
	} else if lastQuery > 0 && lastQuery < now-float64(p.lmd.Config.IdleTimeout) {
		shouldIdle = true
	}
	if !idling && shouldIdle {
		logWith(p).Infof("switched to idle interval, last query: %s (idle timeout: %d)", timeOrNever(lastQuery), p.lmd.Config.IdleTimeout)
		p.StatusSet(Idling, true)
		idling = true
	}
	return idling
}

func (p *Peer) periodicTimeperiodsUpdate(data *DataStoreSet) (err error) {
	t1 := time.Now()
	err = data.UpdateFullTablesList([]TableName{TableTimeperiods, TableHostgroups, TableServicegroups})
	duration := time.Since(t1).Truncate(time.Millisecond)
	logWith(p).Debugf("updating timeperiods and host/servicegroup statistics completed (%s)", duration)
	if err != nil {
		return
	}
	if err = p.requestLocaltime(); err != nil {
		return
	}
	// this also sets the thruk version and checks the clock, so it should be called first
	if _, _, err = p.fetchThrukExtras(); err != nil {
		return
	}
	return
}

func (p *Peer) initTablesIfRestartRequiredError(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*PeerError); ok {
		if e.kind == RestartRequiredError {
			return p.InitAllTables()
		}
	}
	return err
}

func (p *Peer) scheduleUpdateIfRestartRequiredError(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*PeerError); ok {
		if e.kind == RestartRequiredError {
			p.ScheduleImmediateUpdate()
			return true
		}
	}
	return false
}

// StatusSet updates a status map and takes care about the logging.
func (p *Peer) StatusSet(key PeerStatusKey, value interface{}) {
	p.Lock.Lock()
	p.Status[key] = value
	p.Lock.Unlock()
}

// StatusGet returns a status map entry and takes care about the logging.
func (p *Peer) StatusGet(key PeerStatusKey) interface{} {
	p.Lock.RLock()
	value := p.Status[key]
	p.Lock.RUnlock()
	return value
}

// ScheduleImmediateUpdate resets all update timer so the next updateloop iteration
// will performan an update.
func (p *Peer) ScheduleImmediateUpdate() {
	p.Lock.Lock()
	p.Status[LastUpdate] = float64(0)
	p.Status[LastFullServiceUpdate] = float64(0)
	p.Status[LastFullHostUpdate] = float64(0)
	p.Lock.Unlock()
}

// InitAllTables creates all tables for this peer.
// It returns true if the import was successful or false otherwise.
func (p *Peer) InitAllTables() (err error) {
	p.Lock.Lock()
	now := currentUnixTime()
	p.Status[LastUpdate] = now
	p.Status[LastFullUpdate] = now
	p.Status[LastFullServiceUpdate] = now
	p.Status[LastFullHostUpdate] = now
	p.Lock.Unlock()
	data := NewDataStoreSet(p)
	t1 := time.Now()

	if p.lmd.Config.MaxParallelPeerConnections <= 1 {
		err = p.initAllTablesSerial(data)
	} else {
		err = p.initAllTablesParallel(data)
	}
	if err != nil {
		return
	}

	if !p.HasFlag(MultiBackend) {
		err = data.SetReferences()
		if err != nil {
			return
		}

		err = data.RebuildCommentsList()
		if err != nil {
			return
		}

		err = data.RebuildDowntimesList()
		if err != nil {
			return
		}
	}

	err = p.requestLocaltime()
	if err != nil {
		return
	}

	duration := time.Since(t1)
	p.Lock.Lock()
	p.SetDataStoreSet(data, false)
	p.Status[ResponseTime] = duration.Seconds()
	peerStatus := p.Status[PeerState].(PeerStatus)
	logWith(p).Infof("objects created in: %s", duration.String())
	if peerStatus != PeerStatusUp {
		// Reset errors
		if peerStatus == PeerStatusDown {
			logWith(p).Infof("site is back online")
		}
		p.resetErrors()
	}
	p.Lock.Unlock()

	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())

	p.clearLastRequest()
	return
}

// fetches all objects one at a time
func (p *Peer) initAllTablesSerial(data *DataStoreSet) (err error) {
	t1 := time.Now()

	// fetch one at a time
	for _, n := range Objects.UpdateTables {
		t := Objects.Tables[n]
		err = p.initTable(data, t)
		if err != nil {
			logWith(p).Debugf("fetching %s objects failed: %s", t.Name.String(), err.Error())
			return
		}
	}

	logWith(p).Debugf("objects fetched serially in %s", time.Since(t1).String())
	return
}

// fetches all objects at once
func (p *Peer) initAllTablesParallel(data *DataStoreSet) (err error) {
	t1 := time.Now()

	// go with status table first
	err = p.initTable(data, Objects.Tables[TableStatus])
	if err != nil {
		return
	}

	// then fetch all others in parallel
	results := make(chan error, len(Objects.UpdateTables)-1)
	wait := &sync.WaitGroup{}
	for _, n := range Objects.UpdateTables {
		if n == TableStatus {
			continue
		}
		t := Objects.Tables[n]
		wait.Add(1)
		go func(data *DataStoreSet, table *Table) {
			// make sure we log panics properly
			defer logPanicExitPeer(p)
			defer func() {
				wait.Done()
			}()

			err := p.initTable(data, table)
			results <- err
			if err != nil {
				logWith(p).Debugf("fetching %s objects failed: %s", table.Name.String(), err.Error())
				return
			}
		}(data, t)
	}

	// wait till fetching all tables finished
	go func() {
		wait.Wait()
		close(results)
	}()

	// read results till channel is closed
	for e := range results {
		if e != nil {
			err = e
			return
		}
	}
	logWith(p).Debugf("objects fetched parallel in %s", time.Since(t1).String())
	return
}

// resetErrors reset the error counter after the site has recovered
func (p *Peer) initTable(data *DataStoreSet, table *Table) (err error) {
	if p.HasFlag(MultiBackend) && table.Name != TableStatus {
		// just create empty data pools
		// real data is handled by separate peers
		return
	}
	var store *DataStore
	store, err = data.CreateObjectByType(table)
	if err != nil {
		logWith(p).Debugf("creating initial objects failed in table %s: %s", table.Name.String(), err.Error())
		return
	}
	data.Set(table.Name, store)
	switch table.Name {
	case TableStatus:
		err = p.updateInitialStatus(store)
		if err != nil {
			return
		}
		// got an answer, remove last error and let clients know we are reconnecting
		if p.StatusGet(PeerState).(PeerStatus) != PeerStatusPending {
			p.StatusSet(LastError, "reconnecting...")
		}
	case TableTimeperiods:
		lastTimeperiodUpdateMinute, _ := strconv.Atoi(time.Now().Format("4"))
		p.StatusSet(LastTimeperiodUpdateMinute, lastTimeperiodUpdateMinute)
	}
	return
}

// updateInitialStatus updates peer meta data from last status request
func (p *Peer) updateInitialStatus(store *DataStore) (err error) {
	statusData := store.Data
	hasStatus := len(statusData) > 0
	// this may happen if we query another lmd daemon which has no backends ready yet
	if !hasStatus {
		p.Lock.Lock()
		p.Status[PeerState] = PeerStatusDown
		p.Status[LastError] = "peered partner not ready yet"
		p.ClearData(false)
		p.Lock.Unlock()
		return fmt.Errorf("peered partner not ready yet")
	}

	// if its http and a status request, try a processinfo query to fetch all backends
	configtool, thrukextras, cerr := p.fetchThrukExtras() // this also sets the thruk version and checks the clock, so it should be called first
	if cerr != nil {
		err = cerr
		return
	}
	p.LogErrors(p.fetchRemotePeers(store.DataSet))
	p.LogErrors(p.checkStatusFlags(store.DataSet))

	err = p.checkAvailableTables() // must be done after checkStatusFlags, because it does not work on Icinga2
	if err != nil {
		return
	}

	programStart := statusData[0].GetInt64ByName("program_start")
	corePid := statusData[0].GetIntByName("nagios_pid")

	// check thruk config tool settings and other extra data
	p.Lock.Lock()
	delete(p.Status, ConfigTool)
	delete(p.Status, ThrukExtras)
	p.Status[ProgramStart] = programStart
	p.Status[LastPid] = corePid
	p.Lock.Unlock()
	if !p.HasFlag(MultiBackend) {
		// store as string, we simply passthrough it anyway
		if configtool != nil {
			p.StatusSet(ConfigTool, interface2jsonstring(configtool))
		}
		if thrukextras != nil {
			p.StatusSet(ThrukExtras, interface2jsonstring(thrukextras))
		}
	}
	return
}

// resetErrors reset the error counter after the site has recovered
func (p *Peer) resetErrors() {
	p.Status[LastError] = ""
	p.Status[LastOnline] = currentUnixTime()
	p.ErrorCount = 0
	p.ErrorLogged = false
	p.Status[PeerState] = PeerStatusUp
}

// query sends the request to a remote livestatus.
// It returns the unmarshaled result and any error encountered.
func (p *Peer) query(req *Request) (ResultSet, *ResultMetaData, error) {
	p.cache.maxParallelConnections <- true // wait/reserve one connection slot, channel will block if full
	var conn net.Conn
	var connType ConnectionType
	var err error
	defer func() {
		switch {
		case conn == nil:
		case req.KeepAlive:
			// give back connection
			if err == nil {
				logWith(p, req).Tracef("put connection back into pool")
				p.cache.connectionPool <- conn
			} else {
				p.cache.connectionPool <- nil
			}
		default:
			conn.Close()
		}
		<-p.cache.maxParallelConnections // free one connection slot
	}()

	// add backends filter for lmd sub peers
	if p.HasFlag(LMDSub) {
		req.Backends = []string{p.ID}
	}

	logWith(p, req).Tracef("connection #%02d of max. %02d", len(p.cache.maxParallelConnections), p.lmd.Config.MaxParallelPeerConnections)

	conn, connType, err = p.GetConnection(req)
	if err != nil {
		logWith(p, req).Tracef("query: %s", req.String())
		logWith(p, req).Debugf("connection failed: %s", err)
		return nil, nil, err
	}
	if connType == ConnTypeHTTP {
		req.KeepAlive = false
	}
	query := req.String()
	if log.IsV(LogVerbosityTrace) {
		logWith(p, req).Tracef("query: %s", query)
	}

	p.Lock.Lock()
	if p.lmd.Config.SaveTempRequests {
		p.last.Request = req
		p.last.Response = nil
	}
	p.Status[Queries] = p.Status[Queries].(int64) + 1
	totalBytesSend := p.Status[BytesSend].(int64) + int64(len(query))
	p.Status[BytesSend] = totalBytesSend
	peerAddr := p.Status[PeerAddr].(string)
	p.Lock.Unlock()
	promPeerBytesSend.WithLabelValues(p.Name).Set(float64(totalBytesSend))
	promPeerQueries.WithLabelValues(p.Name).Inc()

	t1 := time.Now()
	resBytes, newConn, err := p.getQueryResponse(req, query, peerAddr, conn, connType)
	duration := time.Since(t1)
	if err != nil {
		logWith(p, req).Debugf("backend query failed: %w", err)
		return nil, nil, err
	}
	if newConn != nil {
		conn = newConn
	}
	if req.Command != "" {
		resBytes = bytes.TrimSpace(resBytes)
		if len(resBytes) > 0 {
			tmp := strings.SplitN(strings.TrimSpace(string(resBytes)), ":", 2)
			if len(tmp) == 2 {
				code, _ := strconv.Atoi(tmp[0])
				return nil, nil, &PeerCommandError{err: fmt.Errorf("%s", strings.TrimSpace(tmp[1])), code: code, peer: p}
			}
			return nil, nil, fmt.Errorf("%s", tmp[0])
		}
		return nil, nil, nil
	}

	if log.IsV(LogVerbosityTrace) {
		logWith(p, req).Tracef("result: %s", string(resBytes))
	}
	p.Lock.Lock()
	if p.lmd.Config.SaveTempRequests {
		p.last.Response = resBytes
	}
	totalBytesReceived := p.Status[BytesReceived].(int64) + int64(len(resBytes))
	p.Status[BytesReceived] = totalBytesReceived
	p.Lock.Unlock()
	promPeerBytesReceived.WithLabelValues(p.Name).Set(float64(totalBytesReceived))

	data, meta, err := req.parseResult(resBytes)

	if err != nil {
		logWith(p, req).Debugf("fetched table %20s time: %s, size: %d kB", req.Table.String(), duration, len(resBytes)/1024)
		logWith(p, req).Errorf("result json string: %s", string(resBytes))
		logWith(p, req).Errorf("result parse error: %s", err.Error())
		return nil, nil, &PeerError{msg: err.Error(), kind: ResponseError, srcErr: err}
	}

	meta.Duration = duration
	meta.Size = len(resBytes)

	logWith(p, req).Tracef("fetched table: %15s - time: %8s - count: %8d - size: %8d kB", req.Table.String(), duration, len(data), len(resBytes)/1024)

	if duration > time.Duration(p.lmd.Config.LogSlowQueryThreshold)*time.Second {
		logWith(p, req).Warnf("slow query finished after %s, response size: %s\n%s", duration, ByteCountBinary(int64(len(resBytes))), strings.TrimSpace(req.String()))
	}
	return data, meta, nil
}

func (p *Peer) getQueryResponse(req *Request, query string, peerAddr string, conn net.Conn, connType ConnectionType) ([]byte, net.Conn, error) {
	// http connections
	if connType == ConnTypeHTTP {
		return p.getHTTPQueryResponse(req, query, peerAddr)
	}
	return p.getSocketQueryResponseWithTemporaryRetries(req, query, conn)
}

func (p *Peer) getHTTPQueryResponse(req *Request, query string, peerAddr string) ([]byte, net.Conn, error) {
	res, err := p.HTTPQueryWithRetries(req, peerAddr, query, 2)
	if err != nil {
		return nil, nil, err
	}
	if req.ResponseFixed16 {
		code, expSize, err := p.parseResponseHeader(&res)
		if err != nil {
			logWith(p, req).Debugf("LastQuery:")
			logWith(p, req).Debugf("%s", req.String())
			return nil, nil, err
		}
		res = res[16:]

		err = p.validateResponseHeader(res, req, code, expSize)
		if err != nil {
			logWith(p, req).Debugf("LastQuery:")
			logWith(p, req).Debugf("%s", req.String())
			return nil, nil, err
		}
	}
	return res, nil, nil
}

func (p *Peer) getSocketQueryResponse(req *Request, query string, conn net.Conn) ([]byte, error) {
	// tcp/unix connections
	n, err := p.socketSendQuery(query, conn)
	if err != nil {
		return nil, fmt.Errorf("connection error, send %d of %d bytes: %w", n, len(query), err)
	}

	// close write part of connection
	// but only on commands, it'll breaks larger responses with stunnel / xinetd constructs
	if req.Command != "" && !req.KeepAlive {
		p.LogErrors(CloseWrite(conn))
	}

	b, err := p.parseResponse(req, conn)
	return b, err
}

func (p *Peer) getSocketQueryResponseWithTemporaryRetries(req *Request, query string, conn net.Conn) ([]byte, net.Conn, error) {
	// catch temporary errors
	retries := 0
	for {
		b, err := p.getSocketQueryResponse(req, query, conn)
		if err == nil {
			return b, conn, nil
		}
		if !isTemporary(err) {
			return b, conn, err
		}

		retries++
		if retries > TemporaryNetworkErrorMaxRetries {
			return nil, nil, err
		}
		if retries > 1 {
			time.Sleep(TemporaryNetworkErrorRetryDelay * time.Duration(retries-1))
		}
		peerAddr, connType := extractConnType(p.StatusGet(PeerAddr).(string))
		conn.Close()
		var oErr error
		conn, oErr = p.OpenConnection(peerAddr, connType)
		if oErr != nil {
			// return both errors
			return nil, conn, fmt.Errorf("connection failed: %w, retry failed as well: %s", err, oErr.Error())
		}
	}
}

func (p *Peer) parseResponse(req *Request, conn net.Conn) (b []byte, err error) {
	// read result with fixed result size
	if req.ResponseFixed16 {
		b, err = p.parseResponseFixedSize(req, conn)
		return
	}

	// read result with unknown result size
	b, err = p.parseResponseUndefinedSize(conn)
	if err != nil && req.Command != "" {
		// ignore errors for commands, might close connection immediately (and sending did work already...)
		logWith(p, req).Tracef("ignoring error while reading command response: %s", err.Error())
		err = nil
	}
	return
}

func isTemporary(err error) bool {
	switch {
	case errors.Is(err, syscall.EAGAIN):
		// might be temporary filled send buffer, wait a couple of milliseconds and try again
		return true
	case errors.Is(err, syscall.ECONNRESET):
		// might be closed cached connection, wait and try again
		return true
	case errors.Is(err, syscall.EPIPE):
		return true
	}
	if e, ok := err.(*PeerError); ok {
		if e.srcErr != nil {
			return isTemporary((e.srcErr))
		}
	}
	return false
}

func CloseWrite(conn net.Conn) (err error) {
	switch c := conn.(type) {
	case *net.TCPConn:
		err = c.CloseWrite()
	case *net.UnixConn:
		err = c.CloseWrite()
	}
	if err != nil {
		err = fmt.Errorf("net.Conn.CloseWrite: %w", err)
	}
	return err
}

func (p *Peer) socketSendQuery(query string, conn net.Conn) (int, error) {
	// set read timeout
	err := conn.SetDeadline(time.Now().Add(time.Duration(p.lmd.Config.NetTimeout) * time.Second))
	if err != nil {
		return 0, fmt.Errorf("conn.SetDeadline: %w", err)
	}

	if strings.HasSuffix(query, "\n\n") {
		query = strings.TrimSuffix(query, "\n\n")
		n, err := fmt.Fprintf(conn, "%s\n", query)
		if err != nil {
			return n, fmt.Errorf("socket error: %w", err)
		}
		// send an extra newline to finish the query but ignore errors because the connection might have closed right after the query
		n2, err2 := fmt.Fprintf(conn, "\n")
		if err2 != nil {
			log.Tracef("sending final newline failed: %s", err2.Error())
		}
		n += n2
		return n, nil
	}

	n, err := fmt.Fprintf(conn, "%s", query)
	if err != nil {
		err = fmt.Errorf("socket error: %w", err)
	}
	return n, err
}

func (p *Peer) parseResponseUndefinedSize(conn io.Reader) ([]byte, error) {
	// read result from connection into result buffer with undefined result size
	body := new(bytes.Buffer)
	for {
		_, err := io.CopyN(body, conn, 65536)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("io.CopyN: %w", err)
			}
			break
		}
	}
	res := body.Bytes()
	return res, nil
}

func (p *Peer) parseResponseFixedSize(req *Request, conn io.ReadCloser) ([]byte, error) {
	header := new(bytes.Buffer)
	_, err := io.CopyN(header, conn, 16)
	resBytes := header.Bytes()
	if err != nil {
		return nil, &PeerError{msg: err.Error(), kind: ResponseError, req: req, resBytes: resBytes, srcErr: err}
	}
	if bytes.Contains(resBytes, []byte("No UNIX socket /")) {
		p.LogErrors(io.CopyN(header, conn, ErrorContentPreviewSize))
		resBytes = bytes.TrimSpace(header.Bytes())
		return nil, &PeerError{msg: string(resBytes), kind: ConnectionError}
	}
	code, expSize, err := p.parseResponseHeader(&resBytes)
	if err != nil {
		logWith(p, req).Debugf("LastQuery:")
		logWith(p, req).Debugf("%s", req.String())
		return nil, err
	}
	body := new(bytes.Buffer)
	_, err = io.CopyN(body, conn, expSize)
	if err != nil && errors.Is(err, io.EOF) {
		err = nil
	}

	if err != nil {
		return nil, fmt.Errorf("io.CopyN: %w", err)
	}

	res := body.Bytes()
	err = p.validateResponseHeader(res, req, code, expSize)
	if err != nil {
		logWith(p, req).Debugf("LastQuery:")
		logWith(p, req).Debugf("%s", req.String())
		return nil, err
	}
	return res, nil
}

// Query sends a livestatus request from a request object.
// It calls query and logs all errors except connection errors which are logged in GetConnection.
// It returns the livestatus result and any error encountered.
func (p *Peer) Query(req *Request) (result ResultSet, meta *ResultMetaData, err error) {
	result, meta, err = p.query(req)
	if err != nil {
		p.setNextAddrFromErr(err, req)
	}
	return
}

// QueryString sends a livestatus request from a given string.
// It returns the livestatus result and any error encountered.
func (p *Peer) QueryString(str string) (ResultSet, *ResultMetaData, error) {
	ctx := context.WithValue(context.Background(), CtxPeer, p.Name)
	req, _, err := NewRequest(ctx, p.lmd, bufio.NewReader(bytes.NewBufferString(str)), ParseDefault)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.New("bad request: empty request")
			return nil, nil, err
		}
		return nil, nil, err
	}
	return p.Query(req)
}

// parseResponseHeader parses the return code and content length from the first line of livestatus answer.
// It returns the body size or an error if parsing fails.
func (p *Peer) parseResponseHeader(resBytes *[]byte) (code int, expSize int64, err error) {
	resSize := len(*resBytes)
	if resSize == 0 {
		err = fmt.Errorf("empty response, got 0 bytes")
		return
	}
	if resSize < 16 {
		err = fmt.Errorf("incomplete response header: '%s'", string(*resBytes))
		return
	}
	header := string((*resBytes)[0:15])
	matched := reResponseHeader.FindStringSubmatch(header)
	if len(matched) != 3 {
		if len(*resBytes) > ErrorContentPreviewSize {
			*resBytes = (*resBytes)[:ErrorContentPreviewSize]
		}
		err = fmt.Errorf("incorrect response header: '%s'", string((*resBytes)))
		return
	}
	code, err = strconv.Atoi(matched[1])
	if err != nil {
		err = fmt.Errorf("header parse error - %s: %s", err.Error(), string(*resBytes))
		return
	}
	expSize, err = strconv.ParseInt(matched[2], 10, 64)
	if err != nil {
		err = fmt.Errorf("header parse error - %s: %s", err.Error(), string(*resBytes))
		return
	}

	return
}

// validateResponseHeader checks if the response header returned a valid size and return code
func (p *Peer) validateResponseHeader(resBytes []byte, req *Request, code int, expSize int64) (err error) {
	switch code {
	case 200:
		// everything fine
	default:
		if expSize > 0 && expSize < 300 && int64(len(resBytes)) == expSize {
			msg := fmt.Sprintf("bad response code: %d - %s", code, string(resBytes))
			return &PeerError{msg: msg, kind: ResponseError, req: req, resBytes: resBytes, code: code}
		}
		msg := fmt.Sprintf("bad response code: %d", code)
		return &PeerError{msg: msg, kind: ResponseError, req: req, resBytes: resBytes, code: code}
	}
	if int64(len(resBytes)) != expSize {
		err = fmt.Errorf("bad response size, expected %d, got %d", expSize, len(resBytes))
		return &PeerError{msg: err.Error(), kind: ResponseError, req: req, resBytes: resBytes, code: code}
	}
	return
}

// GetConnection returns the next net.Conn object which answers to a connect.
// In case of a http connection, it just tries a tcp connect, but does not
// return anything.
// It returns the connection object and any error encountered.
func (p *Peer) GetConnection(req *Request) (conn net.Conn, connType ConnectionType, err error) {
	numSources := len(p.Source)

	for x := 0; x < numSources; x++ {
		var peerAddr string
		peerAddr, connType = extractConnType(p.StatusGet(PeerAddr).(string))
		if connType == ConnTypeHTTP {
			// return ok if status is ok, don't create a new connection every time
			if p.StatusGet(LastHTTPRequestSuccessful).(bool) {
				return
			}
		}
		conn = p.GetCachedConnection(req)
		if conn != nil {
			return
		}

		conn, err = p.OpenConnection(peerAddr, connType)

		// connection successful
		if err == nil {
			promPeerConnections.WithLabelValues(p.Name).Inc()
			if x > 0 {
				logWith(p).Infof("active source changed to %s", peerAddr)
				p.ResetFlags()
			}
			return
		}

		// connection error
		p.setNextAddrFromErr(err, req)
	}

	return nil, ConnTypeUnix, &PeerError{msg: err.Error(), kind: ConnectionError, srcErr: err}
}

func (p *Peer) OpenConnection(peerAddr string, connType ConnectionType) (conn net.Conn, err error) {
	switch connType {
	case ConnTypeTCP:
		logWith(p).Tracef("doing tcp connection test: %s", peerAddr)
		conn, err = net.DialTimeout("tcp", peerAddr, time.Duration(p.lmd.Config.ConnectTimeout)*time.Second)
	case ConnTypeUnix:
		logWith(p).Tracef("doing socket connection test: %s", peerAddr)
		conn, err = net.DialTimeout("unix", peerAddr, time.Duration(p.lmd.Config.ConnectTimeout)*time.Second)
	case ConnTypeTLS:
		tlsConfig, cErr := p.getTLSClientConfig()
		if cErr != nil {
			err = cErr
		} else {
			dialer := new(net.Dialer)
			dialer.Timeout = time.Duration(p.lmd.Config.ConnectTimeout) * time.Second
			logWith(p).Tracef("doing tls connection test: %s", peerAddr)
			conn, err = tls.DialWithDialer(dialer, "tcp", peerAddr, tlsConfig)
		}
	case ConnTypeHTTP:
		// test at least basic tcp connect
		uri, uErr := url.Parse(peerAddr)
		if uErr != nil {
			err = uErr
			return
		}
		host := uri.Host
		if !strings.Contains(host, ":") {
			switch uri.Scheme {
			case "http":
				host += ":80"
			case "https":
				host += ":443"
			default:
				err = &PeerError{msg: fmt.Sprintf("unknown scheme: %s", uri.Scheme), kind: ConnectionError}
				return
			}
		}
		logWith(p).Tracef("doing http connection test: %s", host)
		conn, err = net.DialTimeout("tcp", host, time.Duration(p.lmd.Config.ConnectTimeout)*time.Second)
		if conn != nil {
			conn.Close()
		}
		conn = nil
	}
	if err != nil {
		logWith(p).Tracef("connection test failed: %s", err.Error())
	} else {
		logWith(p).Tracef("connection ok")
	}
	return
}

// GetCachedConnection returns the next free cached connection or nil of none found
func (p *Peer) GetCachedConnection(req *Request) (conn net.Conn) {
	select {
	case conn = <-p.cache.connectionPool:
		if conn != nil {
			logWith(p, req).Tracef("using cached connection")
		} else {
			logWith(p, req).Tracef("using new connection")
		}
		return
	default:
		logWith(p, req).Tracef("no cached connection found")
		return nil
	}
}

func extractConnType(rawAddr string) (string, ConnectionType) {
	connType := ConnTypeUnix
	switch {
	case strings.HasPrefix(rawAddr, "http"):
		connType = ConnTypeHTTP
	case strings.HasPrefix(rawAddr, "tls://"):
		connType = ConnTypeTLS
		rawAddr = strings.TrimPrefix(rawAddr, "tls://")
	case strings.Contains(rawAddr, ":"):
		connType = ConnTypeTCP
	}
	return rawAddr, connType
}

func (p *Peer) setNextAddrFromErr(err error, req *Request) {
	if _, ok := err.(*PeerCommandError); ok {
		// client errors do not affect remote site status
		return
	}
	promPeerFailedConnections.WithLabelValues(p.Name).Inc()

	p.Lock.Lock()
	defer p.Lock.Unlock()

	peerAddr := p.Status[PeerAddr].(string)
	logContext := []interface{}{p}
	if req != nil {
		logContext = append(logContext, req)
	}
	logWith(logContext...).Debugf("connection error %s: %s", peerAddr, err)
	p.Status[LastError] = strings.TrimSpace(err.Error())
	p.ErrorCount++

	numSources := len(p.Source)

	// try next node if there are multiple
	curNum := p.Status[CurPeerAddrNum].(int)
	nextNum := curNum
	nextNum++
	if nextNum >= numSources {
		nextNum = 0
	}
	p.Status[CurPeerAddrNum] = nextNum
	p.Status[PeerAddr] = p.Source[nextNum]
	peerAddr = p.Source[nextNum]

	// invalidate connection cache
	p.closeConnectionPool()
	p.cache.connectionPool = make(chan net.Conn, p.lmd.Config.MaxParallelPeerConnections)

	if p.Status[PeerState].(PeerStatus) == PeerStatusUp || p.Status[PeerState].(PeerStatus) == PeerStatusPending {
		p.Status[PeerState] = PeerStatusWarning
	}
	now := currentUnixTime()
	lastOnline := p.Status[LastOnline].(float64)
	logWith(logContext...).Debugf("last online: %s", timeOrNever(lastOnline))
	if lastOnline < now-float64(p.lmd.Config.StaleBackendTimeout) || (p.ErrorCount > numSources && lastOnline <= 0) {
		if p.Status[PeerState].(PeerStatus) != PeerStatusDown {
			logWith(logContext...).Infof("site went offline: %s", err.Error())
		}
		// clear existing data from memory
		p.Status[PeerState] = PeerStatusDown
		p.ClearData(false)
	}

	if numSources > 1 {
		logWith(logContext...).Debugf("trying next one: %s", peerAddr)
	}
}

func (p *Peer) closeConnectionPool() {
	for {
		select {
		case conn := <-p.cache.connectionPool:
			if conn != nil {
				conn.Close()
			}
		default:
			return
		}
	}
}

func (p *Peer) checkStatusFlags(store *DataStoreSet) (err error) {
	data := store.Get(TableStatus).Data
	if len(data) == 0 {
		return
	}
	p.Lock.Lock()
	row := data[0]
	livestatusVersion := row.GetStringByName("livestatus_version")
	switch {
	case len(reShinkenVersion.FindStringSubmatch(livestatusVersion)) > 0:
		if !p.HasFlag(Shinken) {
			logWith(p).Debugf("remote connection Shinken flag set")
			p.SetFlag(Shinken)
		}
	case len(data) > 1:
		// getting more than one status sets the multibackend flag
		if !p.HasFlag(MultiBackend) {
			logWith(p).Infof("remote connection MultiBackend flag set, got %d sites", len(data))
			p.SetFlag(MultiBackend)
			// if its no http connection, then it must be LMD
			if !strings.HasPrefix(p.Status[PeerAddr].(string), "http") {
				p.SetFlag(LMD)
			}
			// force immediate update to fetch all sites
			p.Status[LastUpdate] = currentUnixTime() - float64(p.lmd.Config.Updateinterval)
			p.Lock.Unlock()

			err = p.periodicUpdateMultiBackends(store, true)
			if err != nil {
				return
			}
			return
		}
	case len(reIcinga2Version.FindStringSubmatch(livestatusVersion)) > 0:
		if !p.HasFlag(Icinga2) {
			logWith(p).Debugf("remote connection Icinga2 flag set")
			p.SetFlag(Icinga2)
		}
	case len(reNaemonVersion.FindStringSubmatch(livestatusVersion)) > 0:
		if !p.HasFlag(Naemon) {
			logWith(p).Debugf("remote connection Naemon flag set")
			p.SetFlag(Naemon)
		}
	}
	p.Lock.Unlock()
	return
}

func (p *Peer) checkAvailableTables() (err error) {
	columnFlags := []struct {
		Table  TableName
		Column string
		Flag   OptionalFlags
	}{
		{TableStatus, "localtime", HasLocaltimeColumn},
		{TableHosts, "depends_exec", HasDependencyColumn},
		{TableHosts, "lmd_last_cache_update", HasLMDLastCacheUpdateColumn},
		{TableHosts, "last_update", HasLastUpdateColumn},
		{TableHosts, "event_handler", HasEventHandlerColumn},
		{TableHosts, "staleness", HasStalenessColumn},
		{TableServices, "check_freshness", HasCheckFreshnessColumn},
		{TableServices, "parents", HasServiceParentsColumn},
		{TableContacts, "groups", HasContactsGroupColumn},
	}

	if p.HasFlag(Icinga2) {
		logWith(p).Debugf("Icinga2 does not support checking tables and columns")
		return
	}

	availableTables, err := p.GetSupportedColumns()
	if err != nil {
		return
	}

	for _, optFlag := range columnFlags {
		if _, ok := availableTables[optFlag.Table]; !ok {
			continue
		}
		if !p.HasFlag(optFlag.Flag) {
			if _, ok := availableTables[optFlag.Table][optFlag.Column]; ok {
				logWith(p).Debugf("remote connection supports %s.%s column", optFlag.Table.String(), optFlag.Column)
				p.SetFlag(optFlag.Flag)
			}
		}
	}
	return
}

func (p *Peer) fetchThrukExtras() (conf map[string]interface{}, thrukextras map[string]interface{}, err error) {
	// no http client is a sure sign for no http connection
	if p.cache.HTTPClient == nil {
		return
	}
	// try all http connections and return first config tool config
	for _, addr := range p.Config.Source {
		if strings.HasPrefix(addr, "http") {
			c, t, e := p.fetchThrukExtrasFromAddr(addr)
			err = e
			if t != nil {
				thrukextras = t
			}
			if c != nil {
				conf = c
				return
			}
		}
	}
	return
}

func (p *Peer) fetchThrukExtrasFromAddr(peerAddr string) (conf map[string]interface{}, thrukextras map[string]interface{}, err error) {
	if !strings.HasPrefix(peerAddr, "http") {
		return
	}
	options := make(map[string]interface{})
	options["action"] = "raw"
	options["sub"] = "get_processinfo"
	if p.Config.RemoteName != "" {
		options["remote_name"] = p.Config.RemoteName
	}
	optionStr, err := json.Marshal(options)
	if err != nil {
		return
	}
	output, _, err := p.HTTPPostQuery(nil, peerAddr, url.Values{
		"data": {fmt.Sprintf("{\"credential\": %q, \"options\": %s}", p.Config.Auth, optionStr)},
	}, nil)
	if err != nil {
		return
	}
	conf, thrukextras, err = p.extractThrukExtrasFromResult(output)
	if err != nil {
		return
	}
	return
}

func (p *Peer) extractThrukExtrasFromResult(output []interface{}) (map[string]interface{}, map[string]interface{}, error) {
	if len(output) < 3 {
		return nil, nil, nil
	}
	data, ok := output[2].(map[string]interface{})
	if !ok {
		return nil, nil, nil
	}
	for k := range data {
		processinfo, ok := data[k].(map[string]interface{})
		if !ok {
			continue
		}
		if ts, ok2 := processinfo["localtime"]; ok2 {
			err := p.CheckLocaltime(interface2float64(ts))
			if err != nil {
				p.setNextAddrFromErr(err, nil)
				return nil, nil, err
			}
		}
		var thrukextras map[string]interface{}
		if c, ok2 := processinfo["thruk"]; ok2 {
			if v, ok3 := c.(map[string]interface{}); ok3 {
				thrukextras = v
			}
		}
		if c, ok2 := processinfo["configtool"]; ok2 {
			if v, ok3 := c.(map[string]interface{}); ok3 {
				return v, thrukextras, nil
			}
		}
		if thrukextras != nil {
			return nil, thrukextras, nil
		}
	}
	return nil, nil, nil
}

func (p *Peer) fetchRemotePeers(store *DataStoreSet) (sites []interface{}, err error) {
	// no http client is a sure sign for no http connection
	if p.cache.HTTPClient == nil {
		return
	}
	// we only fetch remote peers if not explicitly requested a single backend
	if p.Config.RemoteName != "" {
		return
	}
	if p.StatusGet(ThrukVersion).(float64) < ThrukMultiBackendMinVersion {
		logWith(p).Warnf("remote thruk version too old (%.2f < %.2f) cannot fetch all sites.", p.StatusGet(ThrukVersion).(float64), ThrukMultiBackendMinVersion)
		return
	}
	// try all http connections and use first working connection
	for _, addr := range p.Config.Source {
		if !strings.HasPrefix(addr, "http") {
			continue
		}

		sites, err = p.fetchRemotePeersFromAddr(addr)
		if err != nil {
			continue
		}

		if len(sites) <= 1 {
			continue
		}

		if !p.HasFlag(MultiBackend) {
			p.Lock.Lock()
			logWith(p).Infof("remote connection MultiBackend flag set, got %d sites", len(sites))
			p.SetFlag(MultiBackend)
			p.Lock.Unlock()
			err = p.periodicUpdateMultiBackends(store, true)
			if err != nil {
				return
			}
		}
		return
	}
	return
}

func (p *Peer) fetchRemotePeersFromAddr(peerAddr string) (sites []interface{}, err error) {
	if !strings.HasPrefix(peerAddr, "http") {
		return
	}
	data, res, err := p.HTTPRestQuery(peerAddr, "/thruk/r/v1/sites")
	if err != nil {
		logWith(p).Warnf("rest query failed, cannot fetch all sites: %s", err.Error())
		return
	}
	if s, ok := data.([]interface{}); ok {
		sites = s
	} else {
		err = &PeerError{msg: fmt.Sprintf("unknown site error, got: %v", res), kind: ResponseError, srcErr: err}
	}
	return
}

// WaitCondition waits for a given condition.
// It returns when the condition matches successfully or after a timeout
func (p *Peer) WaitCondition(req *Request) {
	// wait up to one minute if nothing specified
	if req.WaitTimeout <= 0 {
		req.WaitTimeout = WaitTimeoutDefault
	}
	c := make(chan struct{})
	go func(p *Peer, c chan struct{}, req *Request) {
		// make sure we log panics properly
		defer logPanicExitPeer(p)

		p.LogErrors(p.waitcondition(c, req))
	}(p, c, req)
	timeout := time.NewTimer(time.Duration(req.WaitTimeout) * time.Millisecond)
	select {
	case <-c:
		// finished with condition met
		timeout.Stop()
	case <-timeout.C:
		// timed out
	}

	safeCloseWaitChannel(c)
}

func (p *Peer) waitcondition(c chan struct{}, req *Request) (err error) {
	var lastUpdate float64
	for {
		select {
		case <-c:
			// canceled
			return
		default:
		}

		// waiting for final update to complete
		if lastUpdate > 0 {
			curUpdate := p.StatusGet(LastUpdate).(float64)
			// wait up to WaitTimeout till the update is complete
			if curUpdate > 0 {
				safeCloseWaitChannel(c)
				return nil
			}
			time.Sleep(WaitTimeoutCheckInterval)
			continue
		}

		data, err := p.GetDataStoreSet()
		if err != nil {
			time.Sleep(WaitTimeoutCheckInterval)
			continue
		}

		store, err := p.GetDataStore(req.Table)
		if err != nil {
			time.Sleep(WaitTimeoutCheckInterval)
			continue
		}

		// get object to watch
		var found = false
		if req.WaitObject != "" {
			obj, ok := store.GetWaitObject(req)
			if !ok {
				logWith(p, req).Warnf("WaitObject did not match any object: %s", req.WaitObject)
				safeCloseWaitChannel(c)
				return nil
			}

			found = true
			for i := range req.WaitCondition {
				if !obj.MatchFilter(req.WaitCondition[i], false) {
					found = false
				}
			}
		} else if p.waitConditionTableMatches(store, req.WaitCondition) {
			found = true
		}

		// invert wait condition logic
		if req.WaitConditionNegate {
			found = !found
		}

		if found {
			// trigger update for all, wait conditions are run against the last object
			// but multiple commands may have been sent
			lastUpdate = p.StatusGet(LastUpdate).(float64)
			p.ScheduleImmediateUpdate()
			time.Sleep(WaitTimeoutCheckInterval)
			continue
		}

		// nothing matched, update tables
		time.Sleep(WaitTimeoutCheckInterval)
		switch req.Table {
		case TableHosts:
			err = data.UpdateDeltaHosts(fmt.Sprintf("Filter: name = %s\n", req.WaitObject), false)
		case TableServices:
			tmp := strings.SplitN(req.WaitObject, ";", 2)
			if len(tmp) < 2 {
				logWith(p, req).Errorf("unsupported service wait object: %s", req.WaitObject)
				safeCloseWaitChannel(c)
				return nil
			}
			err = data.UpdateDeltaServices(fmt.Sprintf("Filter: host_name = %s\nFilter: description = %s\n", tmp[0], tmp[1]), false)
		default:
			err = data.UpdateFullTable(req.Table)
		}
		if err != nil {
			if p.scheduleUpdateIfRestartRequiredError(err) {
				// backend is going to restart, wait a bit and try again
				time.Sleep(WaitTimeoutCheckInterval)
			} else {
				safeCloseWaitChannel(c)
				return err
			}
		}
	}
}

// close channel and catch errors of already close channels
func safeCloseWaitChannel(ch chan struct{}) {
	defer func() {
		if recover() != nil {
			log.Debug("close of closed channel")
		}
	}()

	close(ch)
}

// HTTPQueryWithRetries calls HTTPQuery with given amount of retries.
func (p *Peer) HTTPQueryWithRetries(req *Request, peerAddr string, query string, retries int) (res []byte, err error) {
	res, err = p.HTTPQuery(req, peerAddr, query)

	// retry on broken pipe errors
	for retry := 1; retry <= retries && err != nil; retry++ {
		logWith(p, req).Debugf("errored: %s", err.Error())
		if strings.HasPrefix(err.Error(), "remote site returned rc: 0 - ERROR: broken pipe.") {
			time.Sleep(1 * time.Second)
			res, err = p.HTTPQuery(req, peerAddr, query)
			if err == nil {
				logWith(p, req).Debugf("site returned successful result after %d retries", retry)
			}
		}
	}
	return
}

// HTTPQuery sends a query over http to a Thruk backend.
// It returns the livestatus answers and any encountered error.
func (p *Peer) HTTPQuery(req *Request, peerAddr string, query string) (res []byte, err error) {
	options := make(map[string]interface{})
	if p.Config.RemoteName != "" {
		options["backends"] = []string{p.Config.RemoteName}
	}
	options["action"] = "raw"
	options["sub"] = "_raw_query"
	if p.Config.RemoteName != "" {
		options["remote_name"] = p.Config.RemoteName
	}
	options["args"] = []string{strings.TrimSpace(query) + "\n"}
	optionStr, err := json.Marshal(options)
	if err != nil {
		return
	}

	headers := make(map[string]string)
	if p.StatusGet(ThrukVersion).(float64) >= ThrukMultiBackendMinVersion {
		headers["Accept"] = "application/livestatus"
	}

	output, result, err := p.HTTPPostQuery(req, peerAddr, url.Values{
		"data": {fmt.Sprintf("{\"credential\": %q, \"options\": %s}", p.Config.Auth, optionStr)},
	}, headers)
	if err != nil {
		return
	}
	if result.Raw != nil {
		res = result.Raw
		return
	}
	if len(output) <= 2 {
		err = &PeerError{msg: fmt.Sprintf("unknown site error, got: %#v", result), kind: ResponseError}
		return
	}
	if v, ok := output[2].(string); ok {
		res = []byte(v)
	} else {
		err = &PeerError{msg: fmt.Sprintf("unknown site error, got: %#v", result), kind: ResponseError}
	}
	return
}

// HTTPPostQueryResult returns response array from thruk api
func (p *Peer) HTTPPostQueryResult(query *Request, peerAddr string, postData url.Values, headers map[string]string) (result *HTTPResult, err error) {
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, peerAddr, strings.NewReader(postData.Encode()))
	if err != nil {
		logWith(p, query).Debugf("http(s) error: %s", fmtHTTPerr(req, err))
		return nil, fmt.Errorf("http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	p.logHTTPRequest(query, req)
	response, err := p.cache.HTTPClient.Do(req)
	if err != nil {
		p.StatusSet(LastHTTPRequestSuccessful, false)
		logWith(p, query).Debugf("http(s) error: %s", fmtHTTPerr(req, err))
		p.logHTTPResponse(query, response, []byte{})
		return
	}
	p.StatusSet(LastHTTPRequestSuccessful, true)
	contents, err := ExtractHTTPResponse(response)
	p.logHTTPResponse(query, response, contents)
	if err != nil {
		logWith(p, query).Debugf("http(s) error: %s", fmtHTTPerr(req, err))
		return
	}

	if query != nil && query.Command != "" {
		if len(contents) == 0 || contents[0] != '{' {
			result = &HTTPResult{Raw: contents}
			return
		}
	}
	if len(contents) > 10 && bytes.HasPrefix(contents, []byte("200 ")) {
		result = &HTTPResult{Raw: contents}
		return
	}
	if len(contents) > 1 && contents[0] == '[' {
		result = &HTTPResult{Raw: contents}
		return
	}
	if len(contents) < 1 || contents[0] != '{' {
		if len(contents) > ErrorContentPreviewSize {
			contents = contents[:ErrorContentPreviewSize]
		}
		err = &PeerError{msg: fmt.Sprintf("site did not return a proper response: %s", contents), kind: ResponseError}
		logWith(p, query).Debugf("http(s) error: %s", fmtHTTPerr(req, err))
		return
	}
	err = json.Unmarshal(contents, &result)
	if err != nil {
		logWith(p, query).Debugf("http(s) error: %s", fmtHTTPerr(req, err))
		return
	}
	if result.Rc != 0 {
		err = &PeerError{msg: fmt.Sprintf("remote site returned rc: %d - %s", result.Rc, result.Output), kind: ResponseError}
		logWith(p, query).Debugf("http(s) error: %s", fmtHTTPerr(req, err))
	}
	return
}

// HTTPPostQuery returns response array from thruk api
func (p *Peer) HTTPPostQuery(req *Request, peerAddr string, postData url.Values, headers map[string]string) (output []interface{}, result *HTTPResult, err error) {
	result, err = p.HTTPPostQueryResult(req, peerAddr, postData, headers)
	if err != nil {
		return
	}
	if result.Version != "" {
		currentVersion := p.StatusGet(ThrukVersion).(float64)
		newVersion := reThrukVersion.ReplaceAllString(result.Version, `$1`)
		thrukVersion, e := strconv.ParseFloat(newVersion, 64)
		if e == nil && currentVersion != thrukVersion {
			logWith(p, req).Debugf("remote site uses thruk version: %s", result.Version)
			p.StatusSet(ThrukVersion, thrukVersion)
		}
	}
	if result.Raw != nil {
		return
	}
	err = json.Unmarshal(result.Output, &output)
	if err != nil {
		logWith(p, req).Errorf("%s", err.Error())
		return
	}
	if log.IsV(LogVerbosityTrace) {
		logWith(p, req).Tracef("response: %s", result.Output)
	}
	if len(output) >= 4 {
		if v, ok := output[3].(string); ok {
			remoteError := strings.TrimSpace(v)
			matched := reHTTPTooOld.FindStringSubmatch(remoteError)
			if len(matched) > 0 {
				err = &PeerError{msg: fmt.Sprintf("remote site too old: v%s - %s", result.Version, result.Branch), kind: ResponseError}
			} else {
				err = &PeerError{msg: fmt.Sprintf("remote site returned rc: %d - %s", result.Rc, remoteError), kind: ResponseError}
			}
			return
		}
	}
	return
}

// HTTPRestQuery returns rest response from thruk api
func (p *Peer) HTTPRestQuery(peerAddr string, uri string) (output interface{}, result *HTTPResult, err error) {
	options := make(map[string]interface{})
	options["action"] = "url"
	options["commandoptions"] = []string{uri}
	optionStr, err := json.Marshal(options)
	if err != nil {
		return
	}
	result, err = p.HTTPPostQueryResult(nil, peerAddr, url.Values{
		"data": {fmt.Sprintf("{\"credential\": %q, \"options\": %s}", p.Config.Auth, optionStr)},
	}, map[string]string{"Accept": "application/json"})
	if err != nil {
		return
	}
	if result.Code >= 400 {
		err = fmt.Errorf("%d: %s", result.Code, result.Message)
		return
	}
	var str string
	if result.Raw != nil {
		err = json.Unmarshal(result.Raw, &output)
		return
	}
	err = json.Unmarshal(result.Output, &str)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(str), &output)
	if err != nil {
		return
	}
	return
}

// ExtractHTTPResponse returns the content of a HTTP request.
func ExtractHTTPResponse(response *http.Response) (contents []byte, err error) {
	contents, err = io.ReadAll(response.Body)
	if err != nil {
		return
	}

	_, err = io.Copy(io.Discard, response.Body)
	if err != nil {
		return
	}

	err = response.Body.Close()
	if err != nil {
		return
	}

	if response.StatusCode == http.StatusOK {
		return
	}

	matched := reHTTPOMDError.FindStringSubmatch(string(contents))
	if len(matched) > 1 {
		err = &PeerError{msg: fmt.Sprintf("http request failed: %s - %s", response.Status, matched[1]), kind: ResponseError}
		return
	}
	matched = reHTTPThrukError.FindStringSubmatch(string(contents))
	if len(matched) > 1 {
		err = &PeerError{msg: fmt.Sprintf("http request failed: %s - %s", response.Status, matched[1]), kind: ResponseError}
		return
	}
	err = &PeerError{msg: fmt.Sprintf("http request failed: %s", response.Status), kind: ResponseError}
	return
}

// PassThroughQuery runs a passthrough query on a single peer and appends the result
func (p *Peer) PassThroughQuery(res *Response, passthroughRequest *Request, virtualColumns []*Column, columnsIndex map[*Column]int) {
	req := res.Request
	// do not use Query here, might be a log query with log
	result, _, queryErr := p.query(passthroughRequest)
	logWith(p, req).Tracef("req done")
	if queryErr != nil {
		if peerErr, ok := queryErr.(*PeerError); ok && peerErr.kind == ResponseError {
			// no connection issue, no need to reset current connection
		} else {
			p.setNextAddrFromErr(queryErr, passthroughRequest)
		}
		logWith(p, req).Tracef("passthrough req errored %s", queryErr.Error())
		res.Lock.Lock()
		res.Failed[p.ID] = queryErr.Error()
		res.Lock.Unlock()
		return
	}
	// insert virtual values, like peer_addr or name
	if len(virtualColumns) > 0 {
		table := Objects.Tables[res.Request.Table]
		store := NewDataStore(table, p)
		tmpRow, _ := NewDataRow(store, nil, nil, 0, true)
		for rowNum := range result {
			row := &(result[rowNum])
			for j := range virtualColumns {
				col := virtualColumns[j]
				i := columnsIndex[col]
				*row = append(*row, 0)
				copy((*row)[i+1:], (*row)[i:])
				(*row)[i] = tmpRow.GetValueByColumn(col)
			}
			result[rowNum] = *row
		}
	}
	logWith(p, req).Tracef("result ready")
	res.Lock.Lock()
	if len(req.Stats) == 0 {
		res.Result = append(res.Result, result...)
	} else {
		if res.Request.StatsResult == nil {
			res.Request.StatsResult = NewResultSetStats()
			res.Request.StatsResult.Stats[""] = createLocalStatsCopy(res.Request.Stats)
		}
		// apply stats queries
		if len(result) > 0 {
			for i := range result[0] {
				val := result[0][i].(float64)
				res.Request.StatsResult.Stats[""][i].ApplyValue(val, int(val))
			}
		}
	}
	res.Lock.Unlock()
}

// isOnline returns true if this peer is online
func (p *Peer) isOnline() bool {
	return (p.hasPeerState([]PeerStatus{PeerStatusUp, PeerStatusWarning}))
}

// hasPeerState returns true if this peer has given state
func (p *Peer) hasPeerState(states []PeerStatus) bool {
	status := p.StatusGet(PeerState).(PeerStatus)
	if p.HasFlag(LMDSub) {
		realStatus := p.StatusGet(SubPeerStatus).(map[string]interface{})
		num, ok := realStatus["status"]
		if !ok {
			return false
		}
		status = PeerStatus(num.(float64))
	}
	for _, s := range states {
		if status == s {
			return true
		}
	}
	return false
}

func (p *Peer) getError() string {
	if p.HasFlag(LMDSub) {
		realStatus := p.StatusGet(SubPeerStatus).(map[string]interface{})
		errString, ok := realStatus["last_error"]
		if ok && errString.(string) != "" {
			return errString.(string)
		}
	}
	return fmt.Sprintf("%v", p.StatusGet(LastError))
}

func (p *Peer) waitConditionTableMatches(store *DataStore, filter []*Filter) bool {
Rows:
	for j := range store.Data {
		row := store.Data[j]
		// does our filter match?
		for _, f := range filter {
			if !row.MatchFilter(f, false) {
				continue Rows
			}
		}
		return true
	}
	return false
}

func createLocalStatsCopy(stats []*Filter) []*Filter {
	localStats := make([]*Filter, len(stats))
	for i, s := range stats {
		localStats[i] = &Filter{}
		localStats[i].StatsType = s.StatsType
		if s.StatsType == Min {
			localStats[i].Stats = -1
		}
	}
	return localStats
}

func (p *Peer) clearLastRequest() {
	if !p.lmd.Config.SaveTempRequests {
		return
	}
	p.Lock.Lock()
	p.last.Request = nil
	p.last.Response = nil
	p.Lock.Unlock()
}

func (p *Peer) setBroken(details string) {
	details = strings.TrimSpace(details)
	logWith(p).Warnf("%s", details)
	p.Lock.Lock()
	p.Status[PeerState] = PeerStatusBroken
	p.Status[LastError] = "broken: " + details
	p.Status[ThrukVersion] = float64(-1)
	p.ClearData(false)
	p.Lock.Unlock()
}

func logPanicExitPeer(p *Peer) {
	r := recover()
	if r == nil {
		return
	}

	log := logWith(p, p.last.Request)
	log.Errorf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	log.Errorf("Panic:                 %s", r)
	log.Errorf("LMD Version:           %s", Version())
	p.logPeerStatus(log.Errorf)
	log.Errorf("Stacktrace:\n%s", debug.Stack())
	if p.last.Request != nil {
		log.Errorf("LastQuery:")
		log.Errorf("%s", p.last.Request.String())
		log.Errorf("LastResponse:")
		log.Errorf("%s", string(p.last.Response))
	}
	logThreaddump()
	deletePidFile(p.lmd.flags.flagPidfile)
	log.Errorf("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	os.Exit(1)
}

func (p *Peer) logPeerStatus(logger func(string, ...interface{})) {
	status := p.Status[PeerState].(PeerStatus)
	peerflags := OptionalFlags(atomic.LoadUint32(&p.Flags))
	logger("PeerAddr:              %v", p.Status[PeerAddr])
	logger("Idling:                %v", p.Status[Idling])
	logger("Paused:                %v", p.Status[Paused])
	logger("ResponseTime:          %vs", p.Status[ResponseTime])
	logger("LastUpdate:            %.3f", p.Status[LastUpdate].(float64))
	logger("LastFullUpdate:        %.3f", p.Status[LastFullUpdate].(float64))
	logger("LastFullHostUpdate:    %.3f", p.Status[LastFullHostUpdate].(float64))
	logger("LastFullServiceUpdate: %.3f", p.Status[LastFullServiceUpdate].(float64))
	logger("LastQuery:             %.3f", p.Status[LastQuery].(float64))
	logger("Peerstatus:            %s", status.String())
	logger("Flags:                 %s", peerflags.String())
	logger("LastError:             %s", p.Status[LastError].(string))
	logger("ErrorCount:            %d", p.ErrorCount)
}

func (p *Peer) getTLSClientConfig() (*tls.Config, error) {
	config := getMinimalTLSConfig(p.lmd.Config)
	if p.Config.TLSCertificate != "" && p.Config.TLSKey != "" {
		cer, err := tls.LoadX509KeyPair(p.Config.TLSCertificate, p.Config.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("tls.LoadX509KeyPair %s / %s: %w", p.Config.TLSCertificate, p.Config.TLSKey, err)
		}
		config.Certificates = []tls.Certificate{cer}
	}

	if p.Config.TLSSkipVerify > 0 || p.lmd.Config.SkipSSLCheck > 0 {
		config.InsecureSkipVerify = true
	}

	if p.Config.TLSCA != "" {
		caCert, err := os.ReadFile(p.Config.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("readfile %s: %w", p.Config.TLSCA, err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		config.RootCAs = caCertPool
	}

	return config, nil
}

// SendCommandsWithRetry sends list of commands and retries until the peer is completely down
func (p *Peer) SendCommandsWithRetry(ctx context.Context, commands []string) (err error) {
	ctx = context.WithValue(ctx, CtxPeer, p.Name)
	p.Lock.Lock()
	p.Status[LastQuery] = currentUnixTime()
	if p.Status[Idling].(bool) {
		p.Status[Idling] = false
		logWith(ctx).Infof("switched back to normal update interval")
	}
	p.Lock.Unlock()

	// check status of backend
	retries := 0
	for {
		status := p.StatusGet(PeerState).(PeerStatus)
		switch status {
		case PeerStatusDown:
			logWith(ctx).Debugf("cannot send command, peer is down")
			return fmt.Errorf("%s", p.StatusGet(LastError))
		case PeerStatusBroken:
			logWith(ctx).Debugf("cannot send command, peer is broken")
			return fmt.Errorf("%s", p.StatusGet(LastError))
		case PeerStatusWarning, PeerStatusPending:
			// wait till we get either a up or down
			time.Sleep(1 * time.Second)
		case PeerStatusUp:
			err = p.SendCommands(ctx, commands)
			if err == nil {
				return
			}
			switch err := err.(type) {
			case *PeerError:
				// connection error, try again
				if err.kind == ConnectionError {
					if retries > 0 {
						/* this indicates a problem with the command itself:
						   the peer is up, we send a command -> peer is down
						   then peer comes up again, we send the command again
						   and the peer is immediately down again. This means
						   the command probably worked, but something else failed,
						   so don't repeat the command in an endless loop
						*/
						return fmt.Errorf("sending command failed, number of retries exceeded")
					}
					retries++
					time.Sleep(1 * time.Second)
					continue
				}
			case *PeerCommandError:
				return err
			}
			return fmt.Errorf("%s", p.StatusGet(LastError))
		default:
			logWith(ctx).Panicf("PeerStatus %v not implemented", status)
		}
	}
}

// SendCommands sends list of commands
func (p *Peer) SendCommands(ctx context.Context, commands []string) (err error) {
	commandRequest := &Request{
		Command: strings.Join(commands, "\n\n"),
	}
	ctx = context.WithValue(ctx, CtxRequest, commandRequest.ID())
	p.setQueryOptions(commandRequest)
	_, _, err = p.Query(commandRequest)
	if err != nil {
		switch err := err.(type) {
		case *PeerCommandError:
			logWith(ctx).Debugf("sending command failed (invalid query) - %d: %s", err.code, err.Error())
		default:
			logWith(ctx).Warnf("sending command failed: %s", err.Error())
		}
		return
	}
	logWith(ctx).Infof("send %d commands successfully.", len(commands))

	// schedule immediate update
	p.ScheduleImmediateUpdate()

	if !p.HasFlag(HasLastUpdateColumn) {
		p.StatusSet(ForceFull, true)
	}

	return
}

// setFederationInfo updates federation information for /site request
func (p *Peer) setFederationInfo(data map[string]interface{}, statuskey PeerStatusKey, datakey string) {
	if _, ok := data["federation_"+datakey]; ok {
		if v, ok := data["federation_"+datakey].([]interface{}); ok {
			list := []string{}
			for _, d := range v {
				s := d.(string)
				if statuskey == SubAddr {
					s = strings.TrimSuffix(s, "/thruk/cgi-bin/remote.cgi")
				}
				list = append(list, s)
			}
			p.Status[statuskey] = list
			return
		}
	}
	if v, ok := data[datakey].(string); ok {
		p.Status[statuskey] = []string{v}
		return
	}
	p.Status[statuskey] = []string{}
}

// HasFlag returns true if flags are present
func (p *Peer) HasFlag(flag OptionalFlags) bool {
	if flag == 0 {
		return true
	}
	f := OptionalFlags(atomic.LoadUint32(&p.Flags))
	return f.HasFlag(flag)
}

// SetFlag set a flag
func (p *Peer) SetFlag(flag OptionalFlags) {
	f := OptionalFlags(atomic.LoadUint32(&p.Flags))
	f |= flag
	atomic.StoreUint32(&p.Flags, uint32(f))
}

// ResetFlags removes all flags and sets the initial flags from config
func (p *Peer) ResetFlags() {
	atomic.StoreUint32(&p.Flags, uint32(NoFlags))

	// add default flags
	for _, flag := range p.Config.Flags {
		switch strings.ToLower(flag) {
		case "icinga2":
			logWith(p).Debugf("remote connection Icinga2 flag set")
			p.SetFlag(Icinga2)
		default:
			if p.lmd.flags.flagImport == "" {
				logWith(p).Warnf("unknown flag: %s", flag)
			}
		}
	}
}

// GetDataStore returns store for given name or error if peer is offline
func (p *Peer) GetDataStore(tableName TableName) (store *DataStore, err error) {
	table := Objects.Tables[tableName]
	if table.Virtual != nil {
		store = table.Virtual(table, p)
		if store == nil {
			err = fmt.Errorf("peer is down: %s", p.getError())
			return
		}
		return
	}
	data, err := p.GetDataStoreSet()
	if err != nil {
		return
	}

	store = data.Get(tableName)
	if store != nil {
		return
	}
	err = fmt.Errorf("peer is down: %s", p.getError())
	return
}

// GetSupportedColumns returns a list of supported columns
func (p *Peer) GetSupportedColumns() (tables map[TableName]map[string]bool, err error) {
	req := &Request{
		Table:   TableColumns,
		Columns: []string{"table", "name"},
	}
	p.setQueryOptions(req)
	res, _, err := p.query(req) // skip default error handling here
	if err != nil {
		if strings.Contains(err.Error(), "Table 'columns' does not exist") {
			return tables, nil
		}
		// not a fatal error, we can assume safe defaults
		logWith(p, req).Warnf("fetching available columns failed: %w", err)
		return tables, nil
	}
	tables = make(map[TableName]map[string]bool)
	for _, row := range res {
		table := interface2stringNoDedup(row[0])
		tableName, e := NewTableName(table)
		if e != nil {
			continue
		}
		column := interface2stringNoDedup(row[1])
		if _, ok := tables[tableName]; !ok {
			tables[tableName] = make(map[string]bool)
		}
		tables[tableName][column] = true
	}
	return
}

// setQueryOptions sets common required query options
func (p *Peer) setQueryOptions(req *Request) {
	if req.Command == "" {
		req.KeepAlive = p.lmd.Config.BackendKeepAlive
		req.ResponseFixed16 = true
		req.OutputFormat = OutputFormatJSON
	}
	if p.ParentID != "" && p.HasFlag(LMDSub) {
		req.Backends = []string{p.ID}
	}
}

// GetDataStoreSet returns table data or error
func (p *Peer) GetDataStoreSet() (data *DataStoreSet, err error) {
	p.Lock.RLock()
	data = p.data
	p.Lock.RUnlock()
	if data == nil {
		err = fmt.Errorf("peer is down: %s", p.getError())
	}
	return
}

// SetDataStoreSet resets the data table.
func (p *Peer) SetDataStoreSet(data *DataStoreSet, lock bool) {
	if lock {
		p.Lock.Lock()
		defer p.Lock.Unlock()
	}
	p.data = data
}

// ClearData resets the data table.
func (p *Peer) ClearData(lock bool) {
	if lock {
		p.Lock.Lock()
		defer p.Lock.Unlock()
	}
	p.data = nil
}

func (p *Peer) ResumeFromIdle() (err error) {
	p.Lock.RLock()
	data := p.data
	p.Lock.RUnlock()
	p.StatusSet(Idling, false)
	logWith(p).Infof("switched back to normal update interval")
	if p.StatusGet(PeerState).(PeerStatus) == PeerStatusUp && data != nil {
		logWith(p).Debugf("spin up update")
		err = data.UpdateFullTablesList([]TableName{TableTimeperiods})
		if err != nil {
			return
		}
		err = data.UpdateDelta(p.StatusGet(LastUpdate).(float64), currentUnixTime())
		if err != nil {
			return
		}
		logWith(p).Debugf("spin up update done")
	} else {
		// force new update sooner
		p.StatusSet(LastUpdate, currentUnixTime()-float64(p.lmd.Config.Updateinterval))
	}
	return
}

func (p *Peer) requestLocaltime() (err error) {
	if !p.HasFlag(HasLocaltimeColumn) {
		return nil
	}
	req := &Request{
		Table:   TableStatus,
		Columns: []string{"localtime"},
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
		return
	}
	if len(res) == 0 || len(res[0]) == 0 {
		return
	}
	unix := interface2float64(res[0][0])
	err = p.CheckLocaltime(unix)
	if err != nil {
		p.setNextAddrFromErr(err, req)
	}
	return
}

func (p *Peer) CheckLocaltime(unix float64) (err error) {
	if unix == 0 {
		return nil
	}

	nanoseconds := int64((unix - float64(int64(unix))) * float64(time.Second))
	ts := time.Unix(int64(unix), nanoseconds)
	diff := time.Since(ts)
	logWith(p).Debugf("clock difference: %s", diff.Truncate(time.Millisecond).String())
	if p.lmd.Config.MaxClockDelta > 0 && math.Abs(diff.Seconds()) > p.lmd.Config.MaxClockDelta {
		return fmt.Errorf("clock error, peer is off by %s (threshold: %vs)", diff.Truncate(time.Millisecond).String(), p.lmd.Config.MaxClockDelta)
	}
	return
}

// LogErrors i a generic error logger with peer prefix
func (p *Peer) LogErrors(v ...interface{}) {
	if !log.IsV(LogVerbosityDebug) {
		return
	}
	logWith(p).LogErrors(v...)
}

func (p *Peer) CheckBackendRestarted(primaryKeysLen int, res ResultSet, columns ColumnList) (err error) {
	if p.HasFlag(MultiBackend) {
		return
	}
	if len(res) != 1 {
		return
	}

	programStart := p.StatusGet(ProgramStart).(int64)
	corePid := p.StatusGet(LastPid).(int)

	// not yet started completely
	if programStart == 0 || corePid == 0 {
		return
	}

	if len(res[0]) != len(columns)+primaryKeysLen {
		return
	}

	newProgramStart := int64(0)
	newCorePid := 0
	for i, col := range columns {
		switch col.Name {
		case "program_start":
			newProgramStart = interface2int64(res[0][i+primaryKeysLen])
		case "nagios_pid":
			newCorePid = interface2int(res[0][i+primaryKeysLen])
		}
	}

	if newProgramStart != programStart || newCorePid != corePid {
		err = fmt.Errorf("site has been restarted, recreating objects (program_start: %d, pid: %d)", newProgramStart, newCorePid)
		if !p.ErrorLogged {
			logWith(p).Infof("%s", err.Error())
			p.ErrorLogged = true
		}
		return &PeerError{msg: err.Error(), kind: RestartRequiredError}
	}
	return
}

// addSubPeer adds new/existing lmd/http sub federated peer
func (p *Peer) addSubPeer(subFlag OptionalFlags, key string, subName string, data map[string]interface{}) (subID string) {
	subID = key
	subPeer, ok := p.lmd.PeerMap[subID]
	duplicate := ""
	if ok {
		logWith(p).Tracef("already got a sub peer for id %s", subPeer.ID)
		if subPeer.HasFlag(subFlag) {
			// update flags for existing sub peers
			subPeer.Lock.Lock()
			subPeer.Status[SubPeerStatus] = data
			subPeer.Lock.Unlock()
			return
		}

		// create dummy peer which is disabled and only shows this error
		duplicate = fmt.Sprintf("federate site %s/%s id clash %s already taken", p.Name, subName, subID)
		subID += "dup"
		_, ok = p.lmd.PeerMap[subID]
		if ok {
			return
		}
	}

	logWith(p).Debugf("starting sub peer for %s, id: %s", subName, subID)
	c := Connection{
		ID:             subID,
		Name:           subName,
		Source:         p.Source,
		RemoteName:     subName,
		TLSCertificate: p.Config.TLSCertificate,
		TLSKey:         p.Config.TLSKey,
		TLSCA:          p.Config.TLSCA,
		TLSSkipVerify:  p.Config.TLSSkipVerify,
		Auth:           p.Config.Auth,
	}
	subPeer = NewPeer(p.lmd, &c)
	subPeer.ParentID = p.ID
	subPeer.SetFlag(subFlag)
	subPeer.Status[PeerParent] = p.ID
	subPeer.Status[SubPeerStatus] = data
	section := ""

	switch subFlag {
	case HTTPSub:
		section = interface2stringNoDedup(data["section"])
		subPeer.setFederationInfo(data, SubKey, "key")
		subPeer.setFederationInfo(data, SubName, "name")
		subPeer.setFederationInfo(data, SubAddr, "addr")
		subPeer.setFederationInfo(data, SubType, "type")

	case LMDSub:
		// try to fetch section information
		// may fail for older lmd versions
		req := &Request{
			Table:   TableSites,
			Columns: []string{"section"},
		}
		subPeer.setQueryOptions(req)
		res, _, err := subPeer.query(req)
		if err == nil {
			section = interface2stringNoDedup(res[0][0])
		}
	default:
		log.Panicf("sub flag %#v not supported", subFlag)
	}

	section = strings.TrimPrefix(section, "Default")
	section = strings.TrimPrefix(section, "/")
	subPeer.Status[Section] = section

	p.lmd.PeerMap[subID] = subPeer
	p.lmd.PeerMapOrder = append(p.lmd.PeerMapOrder, c.ID)
	p.lmd.nodeAccessor.assignedBackends = append(p.lmd.nodeAccessor.assignedBackends, subID)

	if !p.StatusGet(Paused).(bool) {
		subPeer.Start()
	}
	if duplicate != "" {
		subPeer.Stop()
		subPeer.setBroken(duplicate)
	}
	return
}

// trace log http request
func (p *Peer) logHTTPRequest(query *Request, req *http.Request) {
	if log.IsV(LogVerbosityTrace) {
		requestBytes, err := httputil.DumpRequest(req, true)
		if err != nil {
			logWith(p, query).Debugf("failed to dump http request: %s", fmtHTTPerr(req, err))
		}
		logWith(p, query).Tracef("***************** HTTP Request *****************")
		logWith(p, query).Tracef("%s", string(requestBytes))
	}
}

// trace log http response
func (p *Peer) logHTTPResponse(query *Request, res *http.Response, contents []byte) {
	if log.IsV(LogVerbosityTrace) {
		responseBytes, err := httputil.DumpResponse(res, len(contents) == 0)
		if err != nil {
			logWith(p, query).Debugf("failed to dump http response: %s", err)
		}
		logWith(p, query).Tracef("***************** HTTP Response *****************")
		if len(contents) > 0 {
			responseBytes = append(responseBytes, contents...)
		}
		logWith(p, query).Tracef("%s", string(responseBytes))
	}
}
