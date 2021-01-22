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
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sasha-s/go-deadlock"
)

var reResponseHeader = regexp.MustCompile(`^(\d+)\s+(\d+)$`)
var reHTTPTooOld = regexp.MustCompile(`Can.t locate object method`)
var reHTTPOMDError = regexp.MustCompile(`<h1>(OMD:.*?)</h1>`)
var reShinkenVersion = regexp.MustCompile(`\-shinken$`)
var reIcinga2Version = regexp.MustCompile(`^(r[\d.-]+|.*\-icinga2)$`)
var reNaemonVersion = regexp.MustCompile(`\-naemon$`)

const (
	// MinFullScanInterval is the minimum interval between two full scans
	MinFullScanInterval = 60

	// ConnectionPoolCacheSize sets the number of cached connections per peer
	ConnectionPoolCacheSize = 5

	// UpdateLoopTickerInterval sets the interval for the peer to check if updates should be fetched
	UpdateLoopTickerInterval = 500 * time.Millisecond

	// WaitTimeoutDefault sets the default timeout if nothing specified (1 minute in milliseconds)
	WaitTimeoutDefault = 60000

	// Interval in which wait condition is checked
	WaitTimeoutCheckInterval = 200 * time.Millisecond

	// ErrorContentPreviewSize sets the number of bytes from the response to include in the error message
	ErrorContentPreviewSize = 50
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
	GlobalConfig    *Config                       // reference to global config object
	last            struct {
		Request  *Request // reference to last query (used in error reports)
		Response *[]byte  // reference to last response
	}
	cache struct {
		HTTPClient *http.Client  // cached http client for http backends
		connection chan net.Conn // tcp connection get stored here for reuse
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
)

// PeerConnType contains the different connection types
type PeerConnType uint8

// A peer can be up, warning, down and pending.
// It is pending right after start and warning when the connection fails
// but the stale timeout is not yet hit.
const (
	ConnTypeTCP PeerConnType = iota
	ConnTypeUnix
	ConnTypeTLS
	ConnTypeHTTP
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
	resBytes *[]byte
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
		msg += fmt.Sprintf("\nResponse: %s", string(*(e.resBytes)))
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
func NewPeer(globalConfig *Config, config *Connection, waitGroup *sync.WaitGroup, shutdownChannel chan bool) *Peer {
	p := Peer{
		Name:            config.Name,
		ID:              config.ID,
		Source:          config.Source,
		Status:          make(map[PeerStatusKey]interface{}),
		waitGroup:       waitGroup,
		shutdownChannel: shutdownChannel,
		stopChannel:     make(chan bool),
		Lock:            new(deadlock.RWMutex),
		Config:          config,
		GlobalConfig:    globalConfig,
		Flags:           uint32(NoFlags),
	}
	p.cache.connection = make(chan net.Conn, ConnectionPoolCacheSize)
	if len(p.Source) == 0 {
		log.Fatalf("[%s] peer requires at least one source", p.Name)
	}
	p.Status[PeerKey] = p.ID
	p.Status[PeerName] = p.Name
	p.Status[CurPeerAddrNum] = 0
	p.Status[PeerAddr] = p.Source[p.Status[CurPeerAddrNum].(int)]
	p.Status[PeerState] = PeerStatusPending
	p.Status[LastUpdate] = int64(0)
	p.Status[LastFullUpdate] = int64(0)
	p.Status[LastFullHostUpdate] = int64(0)
	p.Status[LastFullServiceUpdate] = int64(0)
	p.Status[LastQuery] = int64(0)
	p.Status[LastError] = "connecting..."
	p.Status[LastOnline] = int64(0)
	p.Status[LastTimeperiodUpdateMinute] = 0
	p.Status[ProgramStart] = 0
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
	hasHTTP := false
	for _, addr := range config.Source {
		if strings.HasPrefix(addr, "http") {
			hasHTTP = true
			break
		}
	}
	if hasHTTP {
		tlsConfig, err := p.getTLSClientConfig()
		if err != nil {
			log.Fatalf("failed to initialize peer: %s", err.Error())
		}
		p.cache.HTTPClient = NewLMDHTTPClient(tlsConfig, config.Proxy)
	}

	p.ResetFlags()

	return &p
}

// Start creates the initial objects and starts the update loop in a separate goroutine.
func (p *Peer) Start() {
	if !p.StatusGet(Paused).(bool) {
		log.Panicf("[%s] tried to start updateLoop twice", p.Name)
	}
	waitgroup := p.waitGroup
	waitgroup.Add(1)
	p.StatusSet(Paused, false)
	log.Infof("[%s] starting connection", p.Name)
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
		log.Infof("[%s] stopping connection", p.Name)
		p.stopChannel <- true
	}
}

func (p *Peer) countFromServer(name string, queryCondition string) (count int) {
	count = -1
	res, _, err := p.QueryString("GET " + name + "\nOutputFormat: json\nStats: " + queryCondition + "\n\n")
	if err == nil && len(*res) > 0 && len((*res)[0]) > 0 {
		count = int(interface2float64((*res)[0][0]))
	}
	return
}

// updateLoop is the main loop updating this peer.
// It does not return till triggered by the shutdownChannel or by the internal stopChannel.
func (p *Peer) updateLoop() {
	err := p.InitAllTables()
	if err != nil {
		log.Infof("[%s] initializing objects failed: %s", p.Name, err.Error())
		p.ErrorLogged = true
	}

	shutdownStop := func(peer *Peer, ticker *time.Ticker) {
		log.Debugf("[%s] stopping...", peer.Name)
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
				err = p.periodicUpdateMultiBackends(false)
			default:
				err = p.periodicUpdate()
			}
		}
		duration := time.Since(t1)
		err = p.checkRestartRequired(err)
		if err != nil {
			if !p.ErrorLogged {
				log.Infof("[%s] updating objects failed after: %s: %s", p.Name, duration.String(), err.Error())
				p.ErrorLogged = true
			} else {
				log.Debugf("[%s] updating objects failed after: %s: %s", p.Name, duration.String(), err.Error())
			}
		}
		p.clearLastRequest()
	}
}

// periodicUpdate runs the periodic updates from the update loop
func (p *Peer) periodicUpdate() (err error) {
	p.Lock.RLock()
	lastUpdate := p.Status[LastUpdate].(int64)
	lastTimeperiodUpdateMinute := p.Status[LastTimeperiodUpdateMinute].(int)
	lastFullUpdate := p.Status[LastFullUpdate].(int64)
	lastStatus := p.Status[PeerState].(PeerStatus)
	lastQuery := p.Status[LastQuery].(int64)
	idling := p.Status[Idling].(bool)
	data := p.data
	p.Lock.RUnlock()

	idling = p.updateIdleStatus(idling, lastQuery)
	now := time.Now().Unix()
	currentMinute, _ := strconv.Atoi(time.Now().Format("4"))

	// update timeperiods every full minute except when idling
	if !idling && lastTimeperiodUpdateMinute != currentMinute && data != nil {
		p.StatusSet(LastTimeperiodUpdateMinute, currentMinute)
		err = p.periodicTimeperiodsUpdate(data)
		if err != nil {
			return
		}
	}

	nextUpdate := int64(0)
	if idling {
		nextUpdate = lastUpdate + p.GlobalConfig.IdleInterval
	} else {
		nextUpdate = lastUpdate + p.GlobalConfig.Updateinterval
	}
	if now < nextUpdate {
		return
	}

	// set last update timestamp, otherwise we would retry the connection every 500ms instead
	// of the update interval
	p.StatusSet(LastUpdate, now)

	switch lastStatus {
	case PeerStatusBroken:
		var res *ResultSet
		res, _, err = p.QueryString("GET status\nOutputFormat: json\nColumns: program_start nagios_pid\n\n")
		if err != nil {
			log.Debugf("[%s] waiting for reload", p.Name)
			return
		}
		if len(*res) > 0 && len((*res)[0]) == 2 {
			programStart := interface2int64((*res)[0][0])
			corePid := interface2int((*res)[0][1])
			if p.StatusGet(ProgramStart) != programStart || p.StatusGet(LastPid) != corePid {
				log.Debugf("[%s] broken peer has reloaded, trying again.", p.Name)
				return p.InitAllTables()
			}
		}
		return fmt.Errorf("unknown result while waiting for peer to recover: %v", res)
	case PeerStatusWarning:
		// run update if it was just a short outage
		return data.UpdateFull(Objects.UpdateTables)
	case PeerStatusDown:
		return p.InitAllTables()
	case PeerStatusUp:
		// full update interval
		if !idling && p.GlobalConfig.FullUpdateInterval > 0 && now > lastFullUpdate+p.GlobalConfig.FullUpdateInterval {
			return data.UpdateFull(Objects.UpdateTables)
		}
		return data.UpdateDelta(lastUpdate, now)
	case PeerStatusPending:
		// since we called InitAllTables before, status should not be pending
		log.Panicf("peer should not be in status pending here")
	}
	log.Panicf("unhandled status case: %s", lastStatus)
	return
}

// periodicUpdateLMD runs the periodic updates from the update loop for LMD backends
// it fetches the sites table and creates and updates LMDSub backends for them
func (p *Peer) periodicUpdateLMD(force bool) (err error) {
	p.Lock.RLock()
	lastUpdate := p.Status[LastUpdate].(int64)
	p.Lock.RUnlock()

	data, err := p.GetData()
	if err != nil {
		return
	}

	now := time.Now().Unix()
	if !force && now < lastUpdate+p.GlobalConfig.Updateinterval {
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
		log.Infof("[%s] failed to fetch sites information: %s", p.Name, err.Error())
		return
	}
	resHash := res.Result2Hash(columns)

	// check if we need to start/stop peers
	log.Debugf("[%s] checking for changed remote lmd backends", p.Name)
	existing := make(map[string]bool)
	PeerMapLock.Lock()
	for _, rowHash := range resHash {
		subID := rowHash["key"].(string)
		existing[subID] = true
		subName := p.Name + "/" + rowHash["name"].(string)
		subPeer, ok := PeerMap[subID]
		if ok {
			log.Tracef("[%s] already got a sub peer for id %s", p.Name, subID)
		} else {
			log.Debugf("[%s] starting sub peer for %s, id: %s", p.Name, subName, subID)
			c := Connection{ID: subID, Name: subName, Source: p.Source, RemoteName: subName}
			subPeer = NewPeer(p.GlobalConfig, &c, p.waitGroup, p.shutdownChannel)
			subPeer.ParentID = p.ID
			subPeer.SetFlag(LMDSub)
			subPeer.StatusSet(PeerParent, p.ID)
			PeerMap[subID] = subPeer
			PeerMapOrder = append(PeerMapOrder, c.ID)

			// try to fetch section information
			// may fail for older lmd versions
			req := &Request{
				Table:   TableSites,
				Columns: []string{"section"},
			}
			subPeer.setQueryOptions(req)
			res, _, err := subPeer.query(req)
			if err == nil {
				section := *(interface2stringNoDedup((*res)[0][0]))
				section = strings.TrimPrefix(section, "Default")
				section = strings.TrimPrefix(section, "/")
				subPeer.StatusSet(Section, section)
			}

			nodeAccessor.assignedBackends = append(nodeAccessor.assignedBackends, subID)
			subPeer.Start()
		}

		// update flags for existing sub peers
		subPeer.Lock.Lock()
		subPeer.Status[SubPeerStatus] = rowHash
		subPeer.Lock.Unlock()
	}
	PeerMapLock.Unlock()

	// remove exceeding peers
	removed := 0
	PeerMapLock.Lock()
	for id, peer := range PeerMap {
		if peer.ParentID == p.ID {
			if _, ok := existing[id]; !ok {
				log.Debugf("[%s] removing sub peer", peer.Name)
				peer.Stop()
				peer.ClearData(true)
				PeerMapRemove(id)
				removed++
			}
		}
	}
	PeerMapLock.Unlock()
	return
}

// periodicUpdateMultiBackends runs the periodic updates from the update loop for multi backends
// it fetches the all sites and creates and updates HTTPSub backends for them
func (p *Peer) periodicUpdateMultiBackends(force bool) (err error) {
	if p.HasFlag(LMD) {
		return p.periodicUpdateLMD(force)
	}
	p.Lock.RLock()
	lastUpdate := p.Status[LastUpdate].(int64)
	p.Lock.RUnlock()

	now := time.Now().Unix()
	if !force && now < lastUpdate+p.GlobalConfig.Updateinterval {
		return
	}

	data, err := p.GetData()
	if err != nil {
		return
	}

	// check main connection and update status table
	err = data.UpdateFull(Objects.StatusTables)
	if err != nil {
		return
	}

	sites, err := p.fetchRemotePeers()
	if err != nil {
		log.Infof("[%s] failed to fetch sites information: %s", p.Name, err.Error())
		p.ErrorLogged = true
		return
	}

	// set last update timestamp, otherwise we would retry the connection every 500ms instead
	// of the update interval
	p.StatusSet(LastUpdate, time.Now().Unix())

	// check if we need to start/stop peers
	log.Debugf("[%s] checking for changed remote multi backends", p.Name)
	existing := make(map[string]bool)
	PeerMapLock.Lock()
	for _, siteRow := range sites {
		var site map[string]interface{}
		if s, ok := siteRow.(map[string]interface{}); ok {
			site = s
		} else {
			continue
		}
		subID := site["id"].(string)
		existing[subID] = true
		subName := site["name"].(string)
		subPeer, ok := PeerMap[subID]
		if ok {
			log.Tracef("[%s] already got a sub peer for id %s", p.Name, subPeer.ID)
		} else {
			log.Debugf("[%s] starting sub peer for %s, id: %s", p.Name, subName, subID)
			c := Connection{
				ID:             subID,
				Name:           subName,
				Source:         p.Source,
				RemoteName:     site["name"].(string),
				TLSCertificate: p.Config.TLSCertificate,
				TLSKey:         p.Config.TLSKey,
				TLSCA:          p.Config.TLSCA,
				TLSSkipVerify:  p.Config.TLSSkipVerify,
				Auth:           p.Config.Auth,
			}
			subPeer = NewPeer(p.GlobalConfig, &c, p.waitGroup, p.shutdownChannel)
			subPeer.ParentID = p.ID
			subPeer.SetFlag(HTTPSub)
			subPeer.Status[PeerParent] = p.ID
			section := site["section"].(string)
			section = strings.TrimPrefix(section, "Default")
			section = strings.TrimPrefix(section, "/")
			subPeer.Status[Section] = section
			subPeer.setFederationInfo(site, SubKey, "key")
			subPeer.setFederationInfo(site, SubName, "name")
			subPeer.setFederationInfo(site, SubAddr, "addr")
			subPeer.setFederationInfo(site, SubType, "type")
			PeerMap[subID] = subPeer
			PeerMapOrder = append(PeerMapOrder, c.ID)
			nodeAccessor.assignedBackends = append(nodeAccessor.assignedBackends, subID)
			subPeer.Start()
		}
	}

	// remove exceeding peers
	removed := 0
	for id, peer := range PeerMap {
		if peer.ParentID == p.ID {
			if _, ok := existing[id]; !ok {
				log.Debugf("[%s] removing sub peer", peer.Name)
				peer.Stop()
				peer.ClearData(true)
				PeerMapRemove(id)
				removed++
			}
		}
	}
	PeerMapLock.Unlock()
	return
}

func (p *Peer) updateIdleStatus(idling bool, lastQuery int64) bool {
	now := time.Now().Unix()
	shouldIdle := false
	if lastQuery == 0 && lastMainRestart < now-p.GlobalConfig.IdleTimeout {
		shouldIdle = true
	} else if lastQuery > 0 && lastQuery < now-p.GlobalConfig.IdleTimeout {
		shouldIdle = true
	}
	if !idling && shouldIdle {
		log.Infof("[%s] switched to idle interval, last query: %s (idle timeout: %d)", p.Name, timeOrNever(lastQuery), p.GlobalConfig.IdleTimeout)
		p.StatusSet(Idling, true)
		idling = true
	}
	return idling
}

func (p *Peer) periodicTimeperiodsUpdate(data *DataStoreSet) (err error) {
	t1 := time.Now()
	err = data.UpdateFullTablesList([]TableName{TableTimeperiods, TableHostgroups, TableServicegroups})
	duration := time.Since(t1).Truncate(time.Millisecond)
	log.Debugf("[%s] updating timeperiods and host/servicegroup statistics completed (%s)", p.Name, duration)
	if err != nil {
		return
	}
	err = p.requestLocaltime()
	if err != nil {
		return
	}
	_, cerr := p.fetchConfigTool() // this also sets the thruk version and checks the clock, so it should be called first
	if cerr != nil {
		err = cerr
		return
	}
	return
}

func (p *Peer) checkRestartRequired(err error) error {
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
	p.Status[LastUpdate] = time.Now().Unix() - p.GlobalConfig.Updateinterval - 1
	p.Status[LastFullServiceUpdate] = time.Now().Unix() - MinFullScanInterval - 1
	p.Status[LastFullHostUpdate] = time.Now().Unix() - MinFullScanInterval - 1
	p.Lock.Unlock()
}

// InitAllTables creates all tables for this peer.
// It returns true if the import was successful or false otherwise.
func (p *Peer) InitAllTables() (err error) {
	p.Lock.Lock()
	p.Status[LastUpdate] = time.Now().Unix()
	p.Status[LastFullUpdate] = time.Now().Unix()
	p.Status[LastFullServiceUpdate] = time.Now().Unix()
	p.Status[LastFullHostUpdate] = time.Now().Unix()
	p.Lock.Unlock()
	data := NewDataStoreSet(p)
	t1 := time.Now()
	for _, n := range Objects.UpdateTables {
		t := Objects.Tables[n]
		if p.HasFlag(MultiBackend) && t.Name != TableStatus {
			// just create empty data pools
			// real data is handled by separate peers
			continue
		}
		var store *DataStore
		store, err = data.CreateObjectByType(t)
		if err != nil {
			log.Debugf("[%s] creating initial objects failed in table %s: %s", p.Name, t.Name.String(), err.Error())
			return
		}
		data.Set(t.Name, store)
		switch t.Name {
		case TableStatus:
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
			configtool, cerr := p.fetchConfigTool() // this also sets the thruk version and checks the clock, so it should be called first
			if cerr != nil {
				err = cerr
				return
			}
			logDebugError2(p.fetchRemotePeers())
			logDebugError(p.checkStatusFlags(statusData))

			err = p.checkAvailableTables() // must be done after checkStatusFlags, because it does not work on Icinga2
			if err != nil {
				return
			}

			programStart := statusData[0].GetInt64ByName("program_start")
			corePid := statusData[0].GetIntByName("nagios_pid")

			// check thruk config tool settings
			p.Lock.Lock()
			delete(p.Status, ConfigTool)
			p.Status[ProgramStart] = programStart
			p.Status[LastPid] = corePid
			p.Lock.Unlock()
			if !p.HasFlag(MultiBackend) {
				if configtool != nil {
					p.StatusSet(ConfigTool, configtool)
				}
			}
		case TableComments:
			err = data.RebuildCommentsCache()
			if err != nil {
				return
			}
		case TableDowntimes:
			err = data.RebuildDowntimesCache()
			if err != nil {
				return
			}
		case TableTimeperiods:
			lastTimeperiodUpdateMinute, _ := strconv.Atoi(time.Now().Format("4"))
			p.StatusSet(LastTimeperiodUpdateMinute, lastTimeperiodUpdateMinute)
		}
	}

	err = p.requestLocaltime()
	if err != nil {
		return
	}

	duration := time.Since(t1)
	p.Lock.Lock()
	p.SetData(data, false)
	p.Status[ResponseTime] = duration.Seconds()
	peerStatus := p.Status[PeerState].(PeerStatus)
	log.Infof("[%s] objects created in: %s", p.Name, duration.String())
	if peerStatus != PeerStatusUp {
		// Reset errors
		if peerStatus == PeerStatusDown {
			log.Infof("[%s] site is back online", p.Name)
		}
		p.resetErrors()
	}
	p.Lock.Unlock()

	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())

	p.clearLastRequest()
	return
}

// resetErrors reset the error counter after the site has recovered
func (p *Peer) resetErrors() {
	p.Status[LastError] = ""
	p.Status[LastOnline] = time.Now().Unix()
	p.ErrorCount = 0
	p.ErrorLogged = false
	p.Status[PeerState] = PeerStatusUp
}

// query sends the request to a remote livestatus.
// It returns the unmarshaled result and any error encountered.
func (p *Peer) query(req *Request) (*ResultSet, *ResultMetaData, error) {
	conn, connType, err := p.GetConnection()
	if err != nil {
		log.Debugf("[%s] connection failed: %s", p.Name, err)
		return nil, nil, err
	}
	if connType == ConnTypeHTTP {
		req.KeepAlive = false
	}
	defer func() {
		if req.KeepAlive && err != nil && connType != ConnTypeHTTP {
			// give back connection
			log.Tracef("[%s] put cached connection back", p.Name)
			p.cache.connection <- conn
		} else if conn != nil {
			conn.Close()
		}
	}()

	if p.HasFlag(LMDSub) {
		// add backends filter for lmd sub peers
		req.Backends = []string{p.ID}
	}

	query := req.String()
	if log.IsV(LogVerbosityTrace) {
		log.Tracef("[%s] query: %s", p.Name, query)
	}

	p.Lock.Lock()
	if p.GlobalConfig.SaveTempRequests {
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
	resBytes, err := p.getQueryResponse(req, query, peerAddr, conn, connType)
	duration := time.Since(t1).Truncate(time.Millisecond)
	if err != nil {
		log.Debugf("[%s] sending data/query failed: %s", p.Name, err)
		return nil, nil, err
	}
	if req.Command != "" {
		*resBytes = bytes.TrimSpace(*resBytes)
		if len(*resBytes) > 0 {
			tmp := strings.SplitN(strings.TrimSpace(string(*resBytes)), ":", 2)
			if len(tmp) == 2 {
				code, _ := strconv.Atoi(tmp[0])
				return nil, nil, &PeerCommandError{err: fmt.Errorf(strings.TrimSpace(tmp[1])), code: code, peer: p}
			}
			return nil, nil, fmt.Errorf(tmp[0])
		}
		return nil, nil, nil
	}

	if log.IsV(LogVerbosityTrace) {
		log.Tracef("[%s] result: %s", p.Name, string(*resBytes))
	}
	p.Lock.Lock()
	if p.GlobalConfig.SaveTempRequests {
		p.last.Response = resBytes
	}
	totalBytesReceived := p.Status[BytesReceived].(int64) + int64(len(*resBytes))
	p.Status[BytesReceived] = totalBytesReceived
	p.Lock.Unlock()
	promPeerBytesReceived.WithLabelValues(p.Name).Set(float64(totalBytesReceived))

	data, meta, err := req.parseResult(resBytes)
	meta.Duration = duration
	meta.Size = len(*resBytes)

	if err != nil {
		log.Debugf("[%s] fetched table %20s time: %s, size: %d kB", p.Name, req.Table.String(), duration, len(*resBytes)/1024)
		log.Errorf("[%s] result json string: %s", p.Name, string(*resBytes))
		log.Errorf("[%s] result parse error: %s", p.Name, err.Error())
		return nil, nil, &PeerError{msg: err.Error(), kind: ResponseError}
	}

	log.Tracef("[%s] fetched table: %15s - time: %8s - count: %8d - size: %8d kB", p.Name, req.Table.String(), duration, len(*data), len(*resBytes)/1024)
	return data, meta, nil
}

func (p *Peer) getQueryResponse(req *Request, query string, peerAddr string, conn net.Conn, connType PeerConnType) (*[]byte, error) {
	// http connections
	if connType == ConnTypeHTTP {
		return p.getHTTPQueryResponse(req, query, peerAddr)
	}
	return p.getSocketQueryResponse(req, query, conn)
}

func (p *Peer) getHTTPQueryResponse(req *Request, query string, peerAddr string) (*[]byte, error) {
	res, err := p.HTTPQueryWithRetries(req, peerAddr, query, 2)
	if err != nil {
		return nil, err
	}
	if req.ResponseFixed16 {
		code, expSize, err := p.parseResponseHeader(&res)
		if err != nil {
			log.Debugf("[%s] LastQuery:", p.Name)
			log.Debugf("[%s] %s", p.Name, req.String())
			return nil, err
		}
		res = res[16:]

		err = p.validateResponseHeader(&res, req, code, expSize)
		if err != nil {
			log.Debugf("[%s] LastQuery:", p.Name)
			log.Debugf("[%s] %s", p.Name, req.String())
			return nil, err
		}
	}
	return &res, nil
}

func (p *Peer) getSocketQueryResponse(req *Request, query string, conn net.Conn) (*[]byte, error) {
	// tcp/unix connections
	// set read timeout
	err := conn.SetDeadline(time.Now().Add(time.Duration(p.GlobalConfig.NetTimeout) * time.Second))
	if err != nil {
		return nil, fmt.Errorf("conn.SetDeadline: %w", err)
	}
	_, err = fmt.Fprintf(conn, "%s", query)
	if err != nil {
		return nil, fmt.Errorf("conn write failed: %w", err)
	}

	// close write part of connection
	// but only on commands, it'll breaks larger responses with stunnel / xinetd constructs
	if req.Command != "" && !req.KeepAlive {
		switch c := conn.(type) {
		case *net.TCPConn:
			logDebugError(c.CloseWrite())
		case *net.UnixConn:
			logDebugError(c.CloseWrite())
		}
	}

	// read result with fixed result size
	if req.ResponseFixed16 {
		return p.parseResponseFixedSize(req, conn)
	}

	return p.parseResponseUndefinedSize(conn)
}

func (p *Peer) parseResponseUndefinedSize(conn io.Reader) (*[]byte, error) {
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
	return &res, nil
}

func (p *Peer) parseResponseFixedSize(req *Request, conn io.ReadCloser) (*[]byte, error) {
	header := new(bytes.Buffer)
	_, err := io.CopyN(header, conn, 16)
	resBytes := header.Bytes()
	if err != nil {
		return nil, &PeerError{msg: err.Error(), kind: ResponseError, req: req, resBytes: &resBytes}
	}
	if bytes.Contains(resBytes, []byte("No UNIX socket /")) {
		logDebugError2(io.CopyN(header, conn, ErrorContentPreviewSize))
		resBytes = bytes.TrimSpace(header.Bytes())
		return nil, &PeerError{msg: fmt.Sprintf("%s", resBytes), kind: ConnectionError}
	}
	code, expSize, err := p.parseResponseHeader(&resBytes)
	if err != nil {
		log.Debugf("[%s] LastQuery:", p.Name)
		log.Debugf("[%s] %s", p.Name, req.String())
		return nil, err
	}
	body := new(bytes.Buffer)
	_, err = io.CopyN(body, conn, expSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("io.CopyN: %w", err)
	}
	if !req.KeepAlive {
		conn.Close()
	}

	res := body.Bytes()
	err = p.validateResponseHeader(&res, req, code, expSize)
	if err != nil {
		log.Debugf("[%s] LastQuery:", p.Name)
		log.Debugf("[%s] %s", p.Name, req.String())
		return nil, err
	}
	return &res, nil
}

// Query sends a livestatus request from a request object.
// It calls query and logs all errors except connection errors which are logged in GetConnection.
// It returns the livestatus result and any error encountered.
func (p *Peer) Query(req *Request) (result *ResultSet, meta *ResultMetaData, err error) {
	result, meta, err = p.query(req)
	if err != nil {
		p.setNextAddrFromErr(err)
	}
	return
}

// QueryString sends a livestatus request from a given string.
// It returns the livestatus result and any error encountered.
func (p *Peer) QueryString(str string) (*ResultSet, *ResultMetaData, error) {
	req, _, err := NewRequest(bufio.NewReader(bytes.NewBufferString(str)), ParseDefault)
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
		err = fmt.Errorf("[%s] empty response, got 0 bytes", p.Name)
		return
	}
	if resSize < 16 {
		err = fmt.Errorf("[%s] incomplete response header: '%s'", p.Name, string(*resBytes))
		return
	}
	header := string((*resBytes)[0:15])
	matched := reResponseHeader.FindStringSubmatch(header)
	if len(matched) != 3 {
		if len(*resBytes) > ErrorContentPreviewSize {
			*resBytes = (*resBytes)[:ErrorContentPreviewSize]
		}
		err = fmt.Errorf("[%s] incorrect response header: '%s'", p.Name, string((*resBytes)))
		return
	}
	code, err = strconv.Atoi(matched[1])
	if err != nil {
		err = fmt.Errorf("[%s] header parse error - %s: %s", p.Name, err.Error(), string(*resBytes))
		return
	}
	expSize, err = strconv.ParseInt(matched[2], 10, 64)
	if err != nil {
		err = fmt.Errorf("[%s] header parse error - %s: %s", p.Name, err.Error(), string(*resBytes))
		return
	}

	return
}

// validateResponseHeader checks if the response header returned a valid size and return code
func (p *Peer) validateResponseHeader(resBytes *[]byte, req *Request, code int, expSize int64) (err error) {
	switch code {
	case 200:
		// everything fine
	default:
		if expSize > 0 && expSize < 200 && int64(len(*resBytes)) == expSize {
			err = fmt.Errorf("[%s] bad response code: %d - %s", p.Name, code, string(*resBytes))
		} else {
			err = fmt.Errorf("[%s] bad response code: %d", p.Name, code)
			err = &PeerError{msg: err.Error(), kind: ResponseError, req: req, resBytes: resBytes}
		}
		return
	}
	if int64(len(*resBytes)) != expSize {
		err = fmt.Errorf("[%s] bad response size, expected %d, got %d", p.Name, expSize, len(*resBytes))
		return &PeerError{msg: err.Error(), kind: ResponseError, req: req, resBytes: resBytes}
	}
	return
}

// GetConnection returns the next net.Conn object which answers to a connect.
// In case of a http connection, it just tries a tcp connect, but does not
// return anything.
// It returns the connection object and any error encountered.
func (p *Peer) GetConnection() (conn net.Conn, connType PeerConnType, err error) {
	numSources := len(p.Source)

	for x := 0; x < numSources; x++ {
		var peerAddr string
		peerAddr, connType = extractConnType(p.StatusGet(PeerAddr).(string))
		conn = p.GetCachedConnection()
		if conn != nil {
			// make sure it is still useable
			_, err = conn.Read(make([]byte, 0))
			if err == nil {
				return
			}
			conn.Close()
		}
		switch connType {
		case ConnTypeTCP:
			conn, err = net.DialTimeout("tcp", peerAddr, time.Duration(p.GlobalConfig.ConnectTimeout)*time.Second)
		case ConnTypeUnix:
			conn, err = net.DialTimeout("unix", peerAddr, time.Duration(p.GlobalConfig.ConnectTimeout)*time.Second)
		case ConnTypeTLS:
			tlsConfig, cErr := p.getTLSClientConfig()
			if cErr != nil {
				err = cErr
			} else {
				dialer := new(net.Dialer)
				dialer.Timeout = time.Duration(p.GlobalConfig.ConnectTimeout) * time.Second
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
				}
			}
			conn, err = net.DialTimeout("tcp", host, time.Duration(p.GlobalConfig.ConnectTimeout)*time.Second)
			if conn != nil {
				conn.Close()
			}
			conn = nil
		}
		// connection successful
		if err == nil {
			promPeerConnections.WithLabelValues(p.Name).Inc()
			if x > 0 {
				log.Infof("[%s] active source changed to %s", p.Name, peerAddr)
				p.ResetFlags()
			}
			return
		}

		// connection error
		p.setNextAddrFromErr(err)
	}

	return nil, ConnTypeUnix, &PeerError{msg: err.Error(), kind: ConnectionError}
}

// GetCachedConnection returns the next free cached connection or nil of none found
func (p *Peer) GetCachedConnection() (conn net.Conn) {
	select {
	case conn = <-p.cache.connection:
		log.Tracef("[%s] using cached connection", p.Name)
		return
	default:
		log.Tracef("[%s] no cached connection found", p.Name)
		return nil
	}
}

func extractConnType(rawAddr string) (string, PeerConnType) {
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

func (p *Peer) setNextAddrFromErr(err error) {
	if _, ok := err.(*PeerCommandError); ok {
		// client errors do not affect remote site status
		return
	}
	promPeerFailedConnections.WithLabelValues(p.Name).Inc()

	p.Lock.Lock()
	defer p.Lock.Unlock()

	peerAddr := p.Status[PeerAddr].(string)
	log.Debugf("[%s] connection error %s: %s", p.Name, peerAddr, err)
	p.Status[LastError] = err.Error()
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
cache:
	for {
		select {
		case conn := <-p.cache.connection:
			conn.Close()
		default:
			break cache
		}
	}
	p.cache.connection = make(chan net.Conn, ConnectionPoolCacheSize)

	if p.Status[PeerState].(PeerStatus) == PeerStatusUp || p.Status[PeerState].(PeerStatus) == PeerStatusPending {
		p.Status[PeerState] = PeerStatusWarning
	}
	now := time.Now().Unix()
	lastOnline := p.Status[LastOnline].(int64)
	log.Debugf("[%s] last online: %s", p.Name, timeOrNever(lastOnline))
	if lastOnline < now-int64(p.GlobalConfig.StaleBackendTimeout) || (p.ErrorCount > numSources && lastOnline <= 0) {
		if p.Status[PeerState].(PeerStatus) != PeerStatusDown {
			log.Infof("[%s] site went offline: %s", p.Name, err.Error())
		}
		// clear existing data from memory
		p.Status[PeerState] = PeerStatusDown
		p.ClearData(false)
	}

	if numSources > 1 {
		log.Debugf("[%s] trying next one: %s", p.Name, peerAddr)
	}
}

func (p *Peer) checkStatusFlags(data []*DataRow) (err error) {
	if len(data) == 0 {
		return
	}
	p.Lock.Lock()
	row := data[0]
	livestatusVersion := row.GetStringByName("livestatus_version")
	switch {
	case len(reShinkenVersion.FindStringSubmatch(*livestatusVersion)) > 0:
		if !p.HasFlag(Shinken) {
			log.Debugf("[%s] remote connection Shinken flag set", p.Name)
			p.SetFlag(Shinken)
		}
	case len(data) > 1:
		// getting more than one status sets the multibackend flag
		if !p.HasFlag(MultiBackend) {
			log.Infof("[%s] remote connection MultiBackend flag set, got %d sites", p.Name, len(data))
			p.SetFlag(MultiBackend)
			// if its no http connection, then it must be LMD
			if !strings.HasPrefix(p.Status[PeerAddr].(string), "http") {
				p.SetFlag(LMD)
			}
			// force immediate update to fetch all sites
			p.Status[LastUpdate] = time.Now().Unix() - p.GlobalConfig.Updateinterval
			p.Lock.Unlock()

			err = p.periodicUpdateMultiBackends(true)
			if err != nil {
				return
			}
			return
		}
	case len(reIcinga2Version.FindStringSubmatch(*livestatusVersion)) > 0:
		if !p.HasFlag(Icinga2) {
			log.Debugf("[%s] remote connection Icinga2 flag set", p.Name)
			p.SetFlag(Icinga2)
		}
	case len(reNaemonVersion.FindStringSubmatch(*livestatusVersion)) > 0:
		if !p.HasFlag(Naemon) {
			log.Debugf("[%s] remote connection Naemon flag set", p.Name)
			p.SetFlag(Naemon)
		}
	}
	p.Lock.Unlock()
	return
}

func (p *Peer) checkAvailableTables() (err error) {
	if p.HasFlag(Icinga2) {
		log.Debugf("[%s] Icinga2 does not support checking tables and columns", p.Name)
		return
	}
	availableTables, err := p.GetSupportedColumns()
	if err != nil {
		return
	}
	if _, ok := availableTables[TableHosts]; !ok {
		return
	}
	if _, ok := availableTables[TableServices]; !ok {
		return
	}
	if !p.HasFlag(HasDependencyColumn) {
		if _, ok := availableTables[TableHosts]["depends_exec"]; ok {
			log.Debugf("[%s] remote connection supports dependency columns", p.Name)
			p.SetFlag(HasDependencyColumn)
		}
	}
	if !p.HasFlag(HasLMDLastCacheUpdateColumn) {
		if _, ok := availableTables[TableHosts]["lmd_last_cache_update"]; ok {
			log.Debugf("[%s] remote connection supports lmd_last_cache_update columns", p.Name)
			p.SetFlag(HasLMDLastCacheUpdateColumn)
		}
	}
	if !p.HasFlag(HasLastUpdateColumn) {
		if _, ok := availableTables[TableHosts]["last_update"]; ok {
			log.Debugf("[%s] remote connection supports last_update columns", p.Name)
			p.SetFlag(HasLastUpdateColumn)
		}
	}
	if !p.HasFlag(HasLocaltimeColumn) {
		if _, ok := availableTables[TableStatus]; ok {
			if _, ok := availableTables[TableStatus]["localtime"]; ok {
				log.Debugf("[%s] remote connection supports localtime columns", p.Name)
				p.SetFlag(HasLocaltimeColumn)
			}
		}
	}
	return
}

func (p *Peer) fetchConfigTool() (conf map[string]interface{}, err error) {
	// no http client is a sure sign for no http connection
	if p.cache.HTTPClient == nil {
		return
	}
	// try all http connections and return first config tool config
	for _, addr := range p.Config.Source {
		if strings.HasPrefix(addr, "http") {
			c, e := p.fetchConfigToolFromAddr(addr)
			err = e
			if c != nil {
				conf = c
				return
			}
		}
	}
	return
}

func (p *Peer) fetchConfigToolFromAddr(peerAddr string) (conf map[string]interface{}, err error) {
	if !strings.HasPrefix(peerAddr, "http") {
		return
	}
	options := make(map[string]interface{})
	options["action"] = "raw"
	options["sub"] = "get_processinfo"
	if p.Config.RemoteName != "" {
		options["remote_name"] = p.Config.RemoteName
	}
	optionStr, _ := json.Marshal(options)
	output, _, err := p.HTTPPostQuery(nil, peerAddr, url.Values{
		"data": {fmt.Sprintf("{\"credential\": \"%s\", \"options\": %s}", p.Config.Auth, optionStr)},
	}, nil)
	if err != nil {
		return
	}
	conf, err = p.extractConfigToolResult(output)
	if err != nil {
		return
	}
	return
}

func (p *Peer) extractConfigToolResult(output []interface{}) (map[string]interface{}, error) {
	if len(output) < 3 {
		return nil, nil
	}
	data, ok := output[2].(map[string]interface{})
	if !ok {
		return nil, nil
	}
	for k := range data {
		processinfo, ok := data[k].(map[string]interface{})
		if !ok {
			continue
		}
		if ts, ok2 := processinfo["localtime"]; ok2 {
			err := p.CheckLocaltime(interface2float64(ts))
			if err != nil {
				return nil, err
			}
		}
		if c, ok2 := processinfo["configtool"]; ok2 {
			if v, ok3 := c.(map[string]interface{}); ok3 {
				return v, nil
			}
		}
	}
	return nil, nil
}

func (p *Peer) fetchRemotePeers() (sites []interface{}, err error) {
	// no http client is a sure sign for no http connection
	if p.cache.HTTPClient == nil {
		return
	}
	// we only fetch remote peers if not explicitly requested a single backend
	if p.Config.RemoteName != "" {
		return
	}
	if p.StatusGet(ThrukVersion).(float64) < 2.23 {
		log.Warnf("[%s] remote thruk version too old (%.2f < 2.23) cannot fetch all sites.", p.Name, p.StatusGet(ThrukVersion).(float64))
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
			log.Infof("[%s] remote connection MultiBackend flag set, got %d sites", p.Name, len(sites))
			p.SetFlag(MultiBackend)
			p.Lock.Unlock()
			err = p.periodicUpdateMultiBackends(true)
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
		log.Warnf("[%s] rest query failed, cannot fetch all sites: %s", p.Name, err.Error())
		return
	}
	if s, ok := data.([]interface{}); ok {
		sites = s
	} else {
		err = &PeerError{msg: fmt.Sprintf("unknown site error, got: %v", res), kind: ResponseError}
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

		logDebugError(p.waitcondition(c, req))
	}(p, c, req)
	select {
	case <-c:
		// finished with condition met
		return
	case <-time.After(time.Duration(req.WaitTimeout) * time.Millisecond):
		// timed out
		close(c)
		return
	}
}

func (p *Peer) waitcondition(c chan struct{}, req *Request) (err error) {
	var lastUpdate int64
	for {
		select {
		case <-c:
			// canceled
			return
		default:
		}

		// waiting for final update to complete
		if lastUpdate > 0 {
			curUpdate := p.StatusGet(LastUpdate).(int64)
			// wait up to WaitTimeout till the update is complete
			if curUpdate > lastUpdate {
				close(c)
				return nil
			}
			time.Sleep(WaitTimeoutCheckInterval)
			continue
		}

		data, err := p.GetData()
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
				log.Warnf("WaitObject did not match any object: %s", req.WaitObject)
				close(c)
				return nil
			}

			found = true
			for i := range req.WaitCondition {
				if !obj.MatchFilter(req.WaitCondition[i]) {
					found = false
				}
			}
		} else if p.waitConditionTableMatches(store, &req.WaitCondition) {
			found = true
		}

		// invert wait condition logic
		if req.WaitConditionNegate {
			found = !found
		}

		if found {
			// trigger update for all, wait conditions are run against the last object
			// but multiple commands may have been sent
			p.ScheduleImmediateUpdate()
			lastUpdate = p.StatusGet(LastUpdate).(int64)
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
				log.Errorf("unsupported service wait object: %s", req.WaitObject)
				close(c)
				return nil
			}
			err = data.UpdateDeltaServices(fmt.Sprintf("Filter: host_name = %s\nFilter: description = %s\n", tmp[0], tmp[1]), false)
		default:
			err = data.UpdateFullTable(req.Table)
		}
		if err != nil {
			return err
		}
	}
}

// HTTPQueryWithRetries calls HTTPQuery with given amount of retries.
func (p *Peer) HTTPQueryWithRetries(req *Request, peerAddr string, query string, retries int) (res []byte, err error) {
	res, err = p.HTTPQuery(req, peerAddr, query)

	// retry on broken pipe errors
	for retry := 1; retry <= retries && err != nil; retry++ {
		log.Debugf("[%s] errored: %s", p.Name, err.Error())
		if strings.HasPrefix(err.Error(), "remote site returned rc: 0 - ERROR: broken pipe.") {
			time.Sleep(1 * time.Second)
			res, err = p.HTTPQuery(req, peerAddr, query)
			if err == nil {
				log.Debugf("[%s] site returned successful result after %d retries", p.Name, retry)
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
	optionStr, _ := json.Marshal(options)

	headers := make(map[string]string)
	if p.StatusGet(ThrukVersion).(float64) >= 2.23 {
		headers["Accept"] = "application/livestatus"
	}

	output, result, err := p.HTTPPostQuery(req, peerAddr, url.Values{
		"data": {fmt.Sprintf("{\"credential\": \"%s\", \"options\": %s}", p.Config.Auth, optionStr)},
	}, headers)
	if err != nil {
		return
	}
	if result.Raw != nil {
		res = result.Raw
		return
	}
	if v, ok := output[2].(string); ok {
		res = []byte(v)
	} else {
		err = &PeerError{msg: fmt.Sprintf("unknown site error, got: %v", result), kind: ResponseError}
	}
	return
}

// HTTPPostQueryResult returns response array from thruk api
func (p *Peer) HTTPPostQueryResult(query *Request, peerAddr string, postData url.Values, headers map[string]string) (result *HTTPResult, err error) {
	p.cache.HTTPClient.Timeout = time.Duration(p.GlobalConfig.NetTimeout) * time.Second
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, "POST", peerAddr, strings.NewReader(postData.Encode()))
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	response, err := p.cache.HTTPClient.Do(req)
	if err != nil {
		return
	}
	contents, err := ExtractHTTPResponse(response)
	if err != nil {
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
		return
	}
	err = json.Unmarshal(contents, &result)
	if err != nil {
		return
	}
	if result.Rc != 0 {
		err = &PeerError{msg: fmt.Sprintf("remote site returned rc: %d - %s", result.Rc, result.Output), kind: ResponseError}
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
		thrukVersion, e := strconv.ParseFloat(result.Version, 64)
		if e == nil && currentVersion != thrukVersion {
			log.Debugf("[%s] remote site uses thruk version: %s", p.Name, result.Version)
			p.StatusSet(ThrukVersion, thrukVersion)
		}
	}
	if result.Raw != nil {
		return
	}
	err = json.Unmarshal(result.Output, &output)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	remoteError := ""
	if log.IsV(LogVerbosityTrace) {
		log.Tracef("[%s] response: %s", p.Name, result.Output)
	}
	if len(output) >= 4 {
		if v, ok := output[3].(string); ok {
			remoteError = strings.TrimSpace(v)
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
	optionStr, _ := json.Marshal(options)
	result, err = p.HTTPPostQueryResult(nil, peerAddr, url.Values{
		"data": {fmt.Sprintf("{\"credential\": \"%s\", \"options\": %s}", p.Config.Auth, optionStr)},
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
	contents, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}

	_, err = io.Copy(ioutil.Discard, response.Body)
	if err != nil {
		return
	}

	err = response.Body.Close()
	if err != nil {
		return
	}

	if response.StatusCode != 200 {
		matched := reHTTPOMDError.FindStringSubmatch(string(contents))
		if len(matched) > 1 {
			err = &PeerError{msg: fmt.Sprintf("http request failed: %s - %s", response.Status, matched[1]), kind: ResponseError}
		} else {
			err = &PeerError{msg: fmt.Sprintf("http request failed: %s", response.Status), kind: ResponseError}
		}
	}
	return
}

// PassThroughQuery runs a passthrough query on a single peer and appends the result
func (p *Peer) PassThroughQuery(res *Response, passthroughRequest *Request, virtualColumns []*Column, columnsIndex map[*Column]int) {
	req := res.Request
	result, _, queryErr := p.Query(passthroughRequest)
	log.Tracef("[%s] req done", p.Name)
	if queryErr != nil {
		log.Tracef("[%s] req errored", queryErr.Error())
		res.Lock.Lock()
		res.Failed[p.ID] = queryErr.Error()
		res.Lock.Unlock()
		return
	}
	// insert virtual values, like peer_addr or name
	if len(virtualColumns) > 0 {
		table := Objects.Tables[res.Request.Table]
		store := NewDataStore(table, p)
		tmpRow, _ := NewDataRow(store, nil, nil, 0)
		for rowNum := range *result {
			row := &((*result)[rowNum])
			for j := range virtualColumns {
				col := virtualColumns[j]
				i := columnsIndex[col]
				*row = append(*row, 0)
				copy((*row)[i+1:], (*row)[i:])
				(*row)[i] = tmpRow.GetValueByColumn(col)
			}
			(*result)[rowNum] = *row
		}
	}
	log.Tracef("[%s] result ready", p.Name)
	res.Lock.Lock()
	if len(req.Stats) == 0 {
		res.Result = append(res.Result, *result...)
	} else {
		if res.Request.StatsResult == nil {
			res.Request.StatsResult = make(ResultSetStats)
			res.Request.StatsResult[""] = createLocalStatsCopy(&res.Request.Stats)
		}
		// apply stats queries
		if len(*result) > 0 {
			for i := range (*result)[0] {
				val := (*result)[0][i].(float64)
				res.Request.StatsResult[""][i].ApplyValue(val, int(val))
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

func (p *Peer) waitConditionTableMatches(store *DataStore, filter *[]*Filter) bool {
Rows:
	for j := range store.Data {
		row := store.Data[j]
		// does our filter match?
		for i := range *filter {
			if !row.MatchFilter((*filter)[i]) {
				continue Rows
			}
		}
		return true
	}
	return false
}

func createLocalStatsCopy(stats *[]*Filter) []*Filter {
	localStats := make([]*Filter, len(*stats))
	for i := range *stats {
		s := (*stats)[i]
		localStats[i] = &Filter{}
		localStats[i].StatsType = s.StatsType
		if s.StatsType == Min {
			localStats[i].Stats = -1
		}
	}
	return localStats
}

func (p *Peer) clearLastRequest() {
	if !p.GlobalConfig.SaveTempRequests {
		return
	}
	p.Lock.Lock()
	p.last.Request = nil
	p.last.Response = nil
	p.Lock.Unlock()
}

func (p *Peer) setBroken(details string) {
	log.Warnf("[%s] %s", p.Name, details)
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

	log.Errorf("[%s] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", p.Name)
	log.Errorf("[%s] Panic:                 %s", p.Name, r)
	log.Errorf("[%s] LMD Version:           %s", p.Name, Version())
	p.logPeerStatus(log.Errorf)
	log.Errorf("[%s] Stacktrace:\n%s", p.Name, debug.Stack())
	if p.last.Request != nil {
		log.Errorf("[%s] LastQuery:", p.Name)
		log.Errorf("[%s] %s", p.Name, p.last.Request.String())
		log.Errorf("[%s] LastResponse:", p.Name)
		log.Errorf("[%s] %s", p.Name, string(*(p.last.Response)))
	}
	logThreaddump()
	deletePidFile(flagPidfile)
	log.Errorf("[%s] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<", p.Name)
	os.Exit(1)
}

func (p *Peer) logPeerStatus(logger func(string, ...interface{})) {
	name := p.Name
	status := p.Status[PeerState].(PeerStatus)
	peerflags := OptionalFlags(atomic.LoadUint32(&p.Flags))
	logger("[%s] PeerAddr:              %v", name, p.Status[PeerAddr])
	logger("[%s] Idling:                %v", name, p.Status[Idling])
	logger("[%s] Paused:                %v", name, p.Status[Paused])
	logger("[%s] ResponseTime:          %vs", name, p.Status[ResponseTime])
	logger("[%s] LastUpdate:            %v", name, p.Status[LastUpdate])
	logger("[%s] LastFullUpdate:        %v", name, p.Status[LastFullUpdate])
	logger("[%s] LastFullHostUpdate:    %v", name, p.Status[LastFullHostUpdate])
	logger("[%s] LastFullServiceUpdate: %v", name, p.Status[LastFullServiceUpdate])
	logger("[%s] LastQuery:             %v", name, p.Status[LastQuery])
	logger("[%s] Peerstatus:            %s", name, status.String())
	logger("[%s] Flags:                 %s", name, peerflags.String())
	logger("[%s] LastError:             %s", name, p.Status[LastError].(string))
}

func (p *Peer) getTLSClientConfig() (*tls.Config, error) {
	config := getMinimalTLSConfig()
	if p.Config.TLSCertificate != "" && p.Config.TLSKey != "" {
		cer, err := tls.LoadX509KeyPair(p.Config.TLSCertificate, p.Config.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("tls.LoadX509KeyPair %s / %s: %w", p.Config.TLSCertificate, p.Config.TLSKey, err)
		}
		config.Certificates = []tls.Certificate{cer}
	}

	if p.Config.TLSSkipVerify > 0 || p.GlobalConfig.SkipSSLCheck > 0 {
		config.InsecureSkipVerify = true
	}

	if p.Config.TLSCA != "" {
		caCert, err := ioutil.ReadFile(p.Config.TLSCA)
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
func (p *Peer) SendCommandsWithRetry(commands []string) (err error) {
	p.Lock.Lock()
	p.Status[LastQuery] = time.Now().Unix()
	if p.Status[Idling].(bool) {
		p.Status[Idling] = false
		log.Infof("[%s] switched back to normal update interval", p.Name)
	}
	p.Lock.Unlock()

	// check status of backend
	retries := 0
	for {
		status := p.StatusGet(PeerState).(PeerStatus)
		switch status {
		case PeerStatusDown:
			log.Debugf("[%s] cannot send command, peer is down", p.Name)
			return fmt.Errorf("%s", p.StatusGet(LastError))
		case PeerStatusWarning, PeerStatusPending:
			// wait till we get either a up or down
			time.Sleep(1 * time.Second)
		case PeerStatusUp:
			err = p.SendCommands(commands)
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
			log.Panicf("[%s] PeerStatus %v not implemented", p.Name, status)
		}
	}
}

// SendCommands sends list of commands
func (p *Peer) SendCommands(commands []string) (err error) {
	commandRequest := &Request{
		Command: strings.Join(commands, "\n\n"),
	}
	p.setQueryOptions(commandRequest)
	_, _, err = p.Query(commandRequest)
	if err != nil {
		switch err := err.(type) {
		case *PeerCommandError:
			log.Debugf("[%s] sending command failed (invalid query) - %d: %s", p.Name, err.code, err.Error())
		default:
			log.Warnf("[%s] sending command failed: %s", p.Name, err.Error())
		}
		return
	}
	log.Infof("[%s] send %d commands successfully.", p.Name, len(commands))

	// schedule immediate update
	p.ScheduleImmediateUpdate()

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
	return f&flag != 0
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
			log.Debugf("[%s] remote connection Icinga2 flag set", p.Name)
			p.SetFlag(Icinga2)
		default:
			log.Warnf("[%s] unknown flag: %s", p.Name, flag)
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
	data, err := p.GetData()
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
		return nil, err
	}
	tables = make(map[TableName]map[string]bool)
	for i := range *res {
		row := (*res)[i]
		table := interface2stringNoDedup(row[0])
		tableName, e := NewTableName(*table)
		if e != nil {
			continue
		}
		column := interface2stringNoDedup(row[1])
		if _, ok := tables[tableName]; !ok {
			tables[tableName] = make(map[string]bool)
		}
		tables[tableName][*column] = true
	}
	return
}

// setQueryOptions sets common required query options
func (p *Peer) setQueryOptions(req *Request) {
	if req.Command == "" {
		req.KeepAlive = p.GlobalConfig.BackendKeepAlive
		req.ResponseFixed16 = true
		req.OutputFormat = OutputFormatJSON
	}
	if p.ParentID != "" && p.HasFlag(LMDSub) {
		req.Backends = []string{p.ID}
	}
}

// GetData returns table data or error
func (p *Peer) GetData() (data *DataStoreSet, err error) {
	p.Lock.RLock()
	data = p.data
	p.Lock.RUnlock()
	if data == nil {
		err = fmt.Errorf("peer is down: %s", p.getError())
	}
	return
}

// SetData resets the data table.
func (p *Peer) SetData(data *DataStoreSet, lock bool) {
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
	log.Infof("[%s] switched back to normal update interval", p.Name)
	if p.StatusGet(PeerState).(PeerStatus) == PeerStatusUp && data != nil {
		log.Debugf("[%s] spin up update", p.Name)
		err = data.UpdateFullTablesList([]TableName{TableTimeperiods})
		if err != nil {
			return
		}
		err = data.UpdateDelta(p.StatusGet(LastUpdate).(int64), time.Now().Unix())
		if err != nil {
			return
		}
		log.Debugf("[%s] spin up update done", p.Name)
	} else {
		// force new update sooner
		p.StatusSet(LastUpdate, time.Now().Unix()-p.GlobalConfig.Updateinterval)
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
	unix := interface2float64((*res)[0][0])
	return p.CheckLocaltime(unix)
}

func (p *Peer) CheckLocaltime(unix float64) (err error) {
	if unix == 0 {
		return nil
	}

	nanoseconds := int64((unix - float64(int64(unix))) * float64(time.Second))
	ts := time.Unix(int64(unix), nanoseconds)
	diff := time.Since(ts)
	log.Debugf("[%s] clock difference: %s", p.Name, diff.Truncate(time.Millisecond).String())
	if p.GlobalConfig.MaxClockDelta > 0 && math.Abs(diff.Seconds()) > p.GlobalConfig.MaxClockDelta {
		return fmt.Errorf("clock error, peer is off by %s (threshold: %vs)", diff.Truncate(time.Millisecond).String(), p.GlobalConfig.MaxClockDelta)
	}
	return
}
