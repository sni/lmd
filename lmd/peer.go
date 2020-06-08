package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
var reIcinga2Version = regexp.MustCompile(`^(r[\d\.-]+|.*\-icinga2)$`)
var reNaemonVersion = regexp.MustCompile(`\-naemon$`)

const (
	// UpdateAdditionalDelta is the number of seconds to add to the last_check filter on delta updates
	UpdateAdditionalDelta = 3

	// MinFullScanInterval is the minimum interval between two full scans
	MinFullScanInterval = 30

	// ConnectionPoolCacheSize sets the number of cached connections per peer
	ConnectionPoolCacheSize = 5

	// UpdateLoopTickerInterval sets the interval for the peer to check if updates should be fetched
	UpdateLoopTickerInterval = 500 * time.Millisecond

	// SpinUpPeersTimeout sets timeout to wait for peers after spinup
	SpinUpPeersTimeout = 5 * time.Second

	// WaitTimeoutDefault sets the default timeout if nothing specified (1 minute in milliseconds)
	WaitTimeoutDefault = 60000

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
	PeerLock        *deadlock.RWMutex             // must be used for Peer.* access
	DataLock        *deadlock.RWMutex             // must be used for Peer.Table access
	Tables          map[TableName]*DataStore      // the cached remote data tables
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
		HTTPClient *http.Client         // cached http client for http backends
		connection chan net.Conn        // tcp connection get stored here for reuse
		comments   map[*DataRow][]int64 // caches hosts/services datarows to list of comments
		downtimes  map[*DataRow][]int64 // caches hosts/services datarows to list of downtimes
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

// PeerErrorType is used to distinguish between connection and response errors.
type PeerErrorType uint8

const (
	// ConnectionError is used when the connection to a remote site failed
	ConnectionError PeerErrorType = iota

	// ResponseError is used when the remote site is available but returns an unusable result.
	ResponseError
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
	LastUpdateOK
	LastQuery
	LastError
	LastOnline
	LastPid
	ProgramStart
	BytesSend
	BytesReceived
	Querys
	ReponseTime
	Idling
	Updating
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
		Tables:          make(map[TableName]*DataStore),
		Status:          make(map[PeerStatusKey]interface{}),
		waitGroup:       waitGroup,
		shutdownChannel: shutdownChannel,
		stopChannel:     make(chan bool),
		PeerLock:        new(deadlock.RWMutex),
		DataLock:        new(deadlock.RWMutex),
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
	p.Status[ProgramStart] = 0
	p.Status[LastPid] = 0
	p.Status[BytesSend] = int64(0)
	p.Status[BytesReceived] = int64(0)
	p.Status[Querys] = int64(0)
	p.Status[ReponseTime] = float64(0)
	p.Status[Idling] = false
	p.Status[Updating] = false
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

	return &p
}

// Start creates the initial objects and starts the update loop in a separate goroutine.
func (p *Peer) Start() {
	if p.StatusGet(Updating).(bool) {
		log.Panicf("[%s] tried to start updateLoop twice", p.Name)
	}
	p.waitGroup.Add(1)
	p.StatusSet(Updating, true)
	log.Infof("[%s] starting connection", p.Name)
	go func() {
		// make sure we log panics properly
		defer logPanicExitPeer(p)
		p.updateLoop()
		p.StatusSet(Updating, false)
		p.waitGroup.Done()
	}()
}

// Stop stops this peer. Restart with Start.
func (p *Peer) Stop() {
	if p.StatusGet(Updating).(bool) {
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

func (p *Peer) hasChanged() (changed bool) {
	changed = false
	tablenames := []TableName{TableCommands, TableContactgroups, TableContacts, TableHostgroups, TableHosts, TableServicegroups, TableTimeperiods}
	for _, name := range tablenames {
		counter := p.countFromServer(name.String(), "name !=")
		p.DataLock.RLock()
		changed = changed || (counter != len(p.Tables[name].Data))
		p.DataLock.RUnlock()
	}
	counter := p.countFromServer("services", "host_name !=")
	p.DataLock.RLock()
	changed = changed || (counter != len(p.Tables[TableServices].Data))
	p.DataLock.RUnlock()
	p.clearLastRequest(true)

	return
}

// Clear resets the data table.
func (p *Peer) Clear(lock bool) {
	if lock {
		p.DataLock.Lock()
		defer p.DataLock.Unlock()
	}
	p.Tables = make(map[TableName]*DataStore)
}

// updateLoop is the main loop updating this peer.
// It does not return till triggered by the shutdownChannel or by the internal stopChannel.
func (p *Peer) updateLoop() {
	var ok bool
	var lastTimeperiodUpdateMinute int
	p.PeerLock.RLock()
	firstRun := true
	if value, gotLastValue := p.Status[LastUpdateOK]; gotLastValue {
		ok = value.(bool)
		lastTimeperiodUpdateMinute = p.Status[LastTimeperiodUpdateMinute].(int)
		firstRun = false
	}
	p.PeerLock.RUnlock()

	// First run, initialize tables
	if firstRun {
		ok = p.InitAllTables()
		lastTimeperiodUpdateMinute, _ = strconv.Atoi(time.Now().Format("4"))
		p.PeerLock.Lock()
		p.Status[LastUpdateOK] = ok
		p.Status[LastTimeperiodUpdateMinute] = lastTimeperiodUpdateMinute
		p.clearLastRequest(false)
		p.PeerLock.Unlock()
	}

	ticker := time.NewTicker(UpdateLoopTickerInterval)
	for {
		select {
		case <-p.shutdownChannel:
			log.Debugf("[%s] stopping...", p.Name)
			ticker.Stop()
			return
		case <-p.stopChannel:
			log.Debugf("[%s] stopping...", p.Name)
			ticker.Stop()
			p.PeerLock.Lock()
			p.Status[LastUpdateOK] = ok
			p.Status[LastTimeperiodUpdateMinute] = lastTimeperiodUpdateMinute
			p.clearLastRequest(false)
			p.PeerLock.Unlock()
			return
		case <-ticker.C:
			switch {
			case p.HasFlag(LMD):
				p.periodicUpdateLMD(&ok, false)
			case p.HasFlag(MultiBackend):
				p.periodicUpdateMultiBackends(&ok, false)
			default:
				p.periodicUpdate(&ok, &lastTimeperiodUpdateMinute)
			}
			p.clearLastRequest(true)
		}
	}
}

// periodicUpdate runs the periodic updates from the update loop
func (p *Peer) periodicUpdate(ok *bool, lastTimeperiodUpdateMinute *int) {
	p.PeerLock.RLock()
	lastUpdate := p.Status[LastUpdate].(int64)
	lastFullUpdate := p.Status[LastFullUpdate].(int64)
	lastStatus := p.Status[PeerState].(PeerStatus)
	p.PeerLock.RUnlock()

	idling := p.updateIdleStatus()
	now := time.Now().Unix()
	currentMinute, _ := strconv.Atoi(time.Now().Format("4"))

	// update timeperiods every full minute except when idling
	if *ok && !idling && *lastTimeperiodUpdateMinute != currentMinute && lastStatus != PeerStatusBroken {
		log.Debugf("[%s] updating timeperiods and host/servicegroup statistics", p.Name)
		p.UpdateFullTable(TableTimeperiods)
		p.UpdateFullTable(TableHostgroups)
		p.UpdateFullTable(TableServicegroups)
		*lastTimeperiodUpdateMinute = currentMinute

		if p.HasFlag(Icinga2) {
			*ok = p.reloadIfNumberOfObjectsChanged()
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

	if lastStatus == PeerStatusBroken {
		restartRequired, _ := p.UpdateFullTable(TableStatus)
		if restartRequired {
			log.Debugf("[%s] broken peer has reloaded, trying again.", p.Name)
			*ok = p.InitAllTables()
			*lastTimeperiodUpdateMinute = currentMinute
		} else {
			log.Debugf("[%s] waiting for reload", p.Name)
		}
		return
	}

	// run full update if the site was down.
	// run update if it was just a short outage
	if !*ok && lastStatus != PeerStatusWarning {
		*ok = p.InitAllTables()
		*lastTimeperiodUpdateMinute = currentMinute
		return
	}

	// full update interval
	if !idling && p.GlobalConfig.FullUpdateInterval > 0 && now > lastFullUpdate+p.GlobalConfig.FullUpdateInterval {
		*ok = p.UpdateFull()
		return
	}

	*ok = p.UpdateDelta(lastUpdate, now)
}

// periodicUpdateLMD runs the periodic updates from the update loop for LMD backends
// it fetches the sites table and creates and updates LMDSub backends for them
func (p *Peer) periodicUpdateLMD(ok *bool, force bool) {
	p.PeerLock.RLock()
	lastUpdate := p.Status[LastUpdate].(int64)
	p.PeerLock.RUnlock()

	now := time.Now().Unix()
	if !force && now < lastUpdate+p.GlobalConfig.Updateinterval {
		return
	}

	// check main connection and update status table
	*ok = p.UpdateFull()

	// set last update timestamp, otherwise we would retry the connection every 500ms instead
	// of the update interval
	p.StatusSet(LastUpdate, time.Now().Unix())

	columns := []string{"key", "name", "status", "addr", "last_error", "last_update", "last_online", "last_query", "idling"}
	req := &Request{
		Table:   TableSites,
		Columns: columns,
	}
	p.setQueryOptions(req)
	res, _, err := p.query(req)
	if err != nil {
		log.Infof("[%s] failed to fetch sites information: %s", p.Name, err.Error())
		*ok = false
		return
	}
	resHash := Result2Hash(res, columns)

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
		subPeer.PeerLock.Lock()
		subPeer.Status[SubPeerStatus] = rowHash
		subPeer.PeerLock.Unlock()
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
				peer.Clear(true)
				PeerMapRemove(id)
				removed++
			}
		}
	}
	PeerMapLock.Unlock()
}

// periodicUpdateMultiBackends runs the periodic updates from the update loop for multi backends
// it fetches the all sites and creates and updates HTTPSub backends for them
func (p *Peer) periodicUpdateMultiBackends(ok *bool, force bool) {
	p.PeerLock.RLock()
	lastUpdate := p.Status[LastUpdate].(int64)
	p.PeerLock.RUnlock()

	now := time.Now().Unix()
	if !force && now < lastUpdate+p.GlobalConfig.Updateinterval {
		return
	}

	// check main connection and update status table
	*ok = p.UpdateFull()

	sites, err := p.fetchRemotePeers()
	if err != nil {
		log.Infof("[%s] failed to fetch sites information: %s", p.Name, err.Error())
		*ok = false
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
				peer.Clear(true)
				PeerMapRemove(id)
				removed++
			}
		}
	}
	PeerMapLock.Unlock()
}

func (p *Peer) updateIdleStatus() bool {
	now := time.Now().Unix()
	shouldIdle := false
	p.PeerLock.RLock()
	lastQuery := p.Status[LastQuery].(int64)
	idling := p.Status[Idling].(bool)
	p.PeerLock.RUnlock()
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

// StatusSet updates a status map and takes care about the logging.
func (p *Peer) StatusSet(key PeerStatusKey, value interface{}) {
	p.PeerLock.Lock()
	p.Status[key] = value
	p.PeerLock.Unlock()
}

// StatusGet returns a status map entry and takes care about the logging.
func (p *Peer) StatusGet(key PeerStatusKey) interface{} {
	p.PeerLock.RLock()
	value := p.Status[key]
	p.PeerLock.RUnlock()
	return value
}

// ScheduleImmediateUpdate resets all update timer so the next updateloop iteration
// will performan an update.
func (p *Peer) ScheduleImmediateUpdate() {
	p.PeerLock.Lock()
	p.Status[LastUpdate] = time.Now().Unix() - p.GlobalConfig.Updateinterval - 1
	p.Status[LastFullServiceUpdate] = time.Now().Unix() - MinFullScanInterval - 1
	p.Status[LastFullHostUpdate] = time.Now().Unix() - MinFullScanInterval - 1
	p.PeerLock.Unlock()
}

// InitAllTables creates all tables for this peer.
// It returns true if the import was successful or false otherwise.
func (p *Peer) InitAllTables() bool {
	var err error
	p.PeerLock.Lock()
	p.Status[LastUpdate] = time.Now().Unix()
	p.Status[LastFullUpdate] = time.Now().Unix()
	p.PeerLock.Unlock()
	t1 := time.Now()
	for _, n := range Objects.Order {
		t := Objects.Tables[n]
		if p.HasFlag(MultiBackend) && t.Name != TableStatus {
			// just create empty data pools
			// real data is handled by separate peers
			continue
		}
		err = p.CreateObjectByType(t)
		if err != nil {
			log.Debugf("[%s] creating initial objects failed in table %s: %s", p.Name, t.Name.String(), err.Error())
			return false
		}
		switch t.Name {
		case TableStatus:
			p.DataLock.RLock()
			hasStatus := len(p.Tables[TableStatus].Data) > 0
			p.DataLock.RUnlock()
			// this may happen if we query another lmd daemon which has no backends ready yet
			if !hasStatus {
				p.PeerLock.Lock()
				p.Status[PeerState] = PeerStatusDown
				p.Status[LastError] = "peered partner not ready yet"
				p.Clear(false)
				p.PeerLock.Unlock()
				return false
			}

			// if its http and a status request, try a processinfo query to fetch all backends
			configtool, _ := p.fetchConfigTool() // this also sets the thruk version so it should be called first
			p.fetchRemotePeers()
			p.checkStatusFlags()

			err = p.checkAvailableTables() // must be done after checkStatusFlags, because it does not work on Icinga2
			if err != nil {
				return false
			}

			// check thruk config tool settings
			p.PeerLock.Lock()
			delete(p.Status, ConfigTool)
			p.PeerLock.Unlock()
			if !p.HasFlag(MultiBackend) {
				if configtool != nil {
					p.StatusSet(ConfigTool, configtool)
				}
			}
		case TableComments:
			p.RebuildCommentsCache()
		case TableDowntimes:
			p.RebuildDowntimesCache()
		}
	}

	p.DataLock.RLock()
	if _, ok := p.Tables[TableStatus]; ok && len(p.Tables[TableStatus].Data) == 0 {
		// not ready yet
		p.DataLock.RUnlock()
		return false
	}
	programStart := p.Tables[TableStatus].Data[0].GetInt64ByName("program_start")
	corePid := p.Tables[TableStatus].Data[0].GetIntByName("nagios_pid")
	p.DataLock.RUnlock()

	duration := time.Since(t1)
	p.PeerLock.Lock()
	p.Status[ProgramStart] = programStart
	p.Status[LastPid] = corePid
	p.Status[ReponseTime] = duration.Seconds()
	peerStatus := p.Status[PeerState].(PeerStatus)
	log.Infof("[%s] objects created in: %s", p.Name, duration.String())
	if peerStatus != PeerStatusUp {
		// Reset errors
		if peerStatus == PeerStatusDown {
			log.Infof("[%s] site is back online", p.Name)
		}
		p.resetErrors()
	}
	p.PeerLock.Unlock()

	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())

	return true
}

// resetErrors reset the error counter after the site has recovered
func (p *Peer) resetErrors() {
	p.Status[LastError] = ""
	p.Status[LastOnline] = time.Now().Unix()
	p.ErrorCount = 0
	p.ErrorLogged = false
	p.Status[PeerState] = PeerStatusUp
}

// UpdateFull runs a full update on all dynamic values for all tables which have dynamic updated columns.
// It returns true if the update was successful or false otherwise.
func (p *Peer) UpdateFull() bool {
	t1 := time.Now()
	var err error
	restartRequired := false
	for _, n := range Objects.Order {
		t := Objects.Tables[n]
		restartRequired, err = p.UpdateFullTable(t.Name)
		if err != nil {
			log.Debugf("[%s] update failed: %s", p.Name, err.Error())
			return false
		}
		if restartRequired {
			break
		}
	}
	if err != nil {
		log.Debugf("[%s] update failed: %s", p.Name, err.Error())
		return false
	}
	if restartRequired {
		return p.InitAllTables()
	}
	duration := time.Since(t1)
	peerStatus := p.StatusGet(PeerState).(PeerStatus)
	if peerStatus != PeerStatusUp && peerStatus != PeerStatusPending {
		log.Infof("[%s] site soft recovered from short outage", p.Name)
	}
	p.PeerLock.Lock()
	p.resetErrors()
	p.Status[ReponseTime] = duration.Seconds()
	p.Status[LastUpdate] = time.Now().Unix()
	p.Status[LastFullUpdate] = time.Now().Unix()
	p.PeerLock.Unlock()
	log.Debugf("[%s] full update complete in: %s", p.Name, duration.String())
	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())
	return true
}

// UpdateDelta runs a delta update on all status, hosts, services, comments and downtimes table.
// It returns true if the update was successful or false otherwise.
func (p *Peer) UpdateDelta(from, to int64) bool {
	t1 := time.Now()

	restartRequired, err := p.UpdateFullTable(TableStatus)
	if restartRequired {
		return p.InitAllTables()
	}
	filterStr := ""
	if from > 0 {
		if p.HasFlag(HasLMDLastCacheUpdateColumn) {
			filterStr = fmt.Sprintf("Filter: lmd_last_cache_update >= %v\nFilter: lmd_last_cache_update < %v\nAnd: 2\n", from, to)
		} else if p.HasFlag(HasLastUpdateColumn) {
			filterStr = fmt.Sprintf("Filter: last_update >= %v\nFilter: last_update < %v\nAnd: 2\n", from, to)
		}
	}
	if err == nil {
		err = p.UpdateDeltaHosts(filterStr)
	}
	if err == nil {
		err = p.UpdateDeltaServices(filterStr)
	}
	if err == nil {
		err = p.UpdateDeltaCommentsOrDowntimes(TableComments)
	}
	if err == nil {
		err = p.UpdateDeltaCommentsOrDowntimes(TableDowntimes)
	}

	duration := time.Since(t1)
	if err != nil {
		if !p.ErrorLogged {
			log.Infof("[%s] updating objects failed after: %s: %s", p.Name, duration.String(), err.Error())
			p.ErrorLogged = true
		} else {
			log.Debugf("[%s] updating objects failed after: %s: %s", p.Name, duration.String(), err.Error())
		}
		return false
	}
	log.Debugf("[%s] delta update complete in: %s", p.Name, duration.String())

	p.PeerLock.Lock()
	peerStatus := p.Status[PeerState].(PeerStatus)
	p.resetErrors()
	p.Status[LastUpdate] = to
	p.Status[ReponseTime] = duration.Seconds()
	p.PeerLock.Unlock()

	if peerStatus != PeerStatusUp && peerStatus != PeerStatusPending {
		log.Infof("[%s] site soft recovered from short outage", p.Name)
	}

	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())
	return true
}

// UpdateDeltaHosts update hosts by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional UpdateAdditionalDelta seconds.
// It returns any error encountered.
func (p *Peer) UpdateDeltaHosts(filterStr string) (err error) {
	// update changed hosts
	p.PeerLock.RLock()
	table := p.Tables[TableHosts]
	p.PeerLock.RUnlock()
	if table == nil {
		err = fmt.Errorf("peer not ready, either not initialized or went offline recently")
		return
	}
	if filterStr == "" {
		filterStr = fmt.Sprintf("Filter: last_check >= %v\n", (p.StatusGet(LastUpdate).(int64) - UpdateAdditionalDelta))
		if p.GlobalConfig.SyncIsExecuting {
			filterStr += "\nFilter: is_executing = 1\nOr: 2\n"
		}
		// no filter means regular delta update, so lets check if all last_check dates match
		ok, uErr := p.UpdateDeltaFullScan(table, filterStr)
		if ok || uErr != nil {
			return uErr
		}
	}
	req := &Request{
		Table:     table.Table.Name,
		Columns:   table.DynamicColumnNamesCache,
		FilterStr: filterStr,
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
		return
	}
	hostDataOffset := 1
	return p.insertDeltaHostResult(hostDataOffset, res, table)
}

func (p *Peer) insertDeltaHostResult(hostDataOffset int, res *ResultSet, table *DataStore) (err error) {
	updateSet, err := p.prepareHostUpdateSet(hostDataOffset, res, table)
	if err != nil {
		return
	}
	now := time.Now().Unix()

	p.DataLock.Lock()
	defer p.DataLock.Unlock()
	for i := range updateSet {
		update := updateSet[i]
		if update.FullUpdate {
			err = update.DataRow.UpdateValues(hostDataOffset, update.ResultRow, &table.DynamicColumnCache, now)
		} else {
			err = update.DataRow.UpdateValuesNumberOnly(hostDataOffset, update.ResultRow, &table.DynamicColumnCache, now)
		}
		if err != nil {
			return
		}
	}
	promObjectUpdate.WithLabelValues(p.Name, "hosts").Add(float64(len(*res)))
	log.Debugf("[%s] updated %d hosts", p.Name, len(*res))

	return
}

func (p *Peer) prepareHostUpdateSet(hostDataOffset int, res *ResultSet, table *DataStore) (updateSet []ResultPrepared, err error) {
	updateSet = make([]ResultPrepared, len(*res))

	// prepare list of large strings
	stringlistIndexes := make([]int, 0)
	for i := range table.DynamicColumnCache {
		col := table.DynamicColumnCache[i]
		if col.DataType == StringLargeCol {
			stringlistIndexes = append(stringlistIndexes, i+hostDataOffset)
		}
	}

	// compare last check date and only update large strings if the last check date has changed
	lastCheckCol := table.GetColumn("last_check")
	lastCheckIndex := table.DynamicColumnCache.GetColumnIndex("last_check") + hostDataOffset

	// prepare update
	p.DataLock.RLock()
	defer p.DataLock.RUnlock()
	nameIndex := table.Index
	for i := range *res {
		resRow := &(*res)[i]
		prepared := ResultPrepared{
			ResultRow:  resRow,
			FullUpdate: false,
		}
		if hostDataOffset == 0 {
			prepared.DataRow = table.Data[i]
		} else {
			hostName := interface2stringNoDedup((*resRow)[0])
			dataRow := nameIndex[*hostName]
			if dataRow == nil {
				return updateSet, fmt.Errorf("cannot update host, no host named '%s' found", *hostName)
			}
			prepared.DataRow = dataRow
		}

		// compare last check date and prepare large strings if the last check date has changed
		if interface2int64((*resRow)[lastCheckIndex]) != prepared.DataRow.GetInt64(lastCheckCol) {
			prepared.FullUpdate = true
			for j := range stringlistIndexes {
				(*res)[i][j] = interface2stringlarge((*res)[i][j])
			}
		}
		updateSet[i] = prepared
	}
	return updateSet, nil
}

// UpdateDeltaServices update services by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional UpdateAdditionalDelta seconds.
// It returns any error encountered.
func (p *Peer) UpdateDeltaServices(filterStr string) (err error) {
	// update changed services
	p.PeerLock.RLock()
	table := p.Tables[TableServices]
	p.PeerLock.RUnlock()
	if table == nil {
		err = fmt.Errorf("peer not ready, either not initialized or went offline recently")
		return
	}
	if filterStr == "" {
		filterStr = fmt.Sprintf("Filter: last_check >= %v\n", (p.StatusGet(LastUpdate).(int64) - UpdateAdditionalDelta))
		if p.GlobalConfig.SyncIsExecuting {
			filterStr += "\nFilter: is_executing = 1\nOr: 2\n"
		}
		// no filter means regular delta update, so lets check if all last_check dates match
		ok, uErr := p.UpdateDeltaFullScan(table, filterStr)
		if ok || uErr != nil {
			return uErr
		}
	}
	req := &Request{
		Table:     table.Table.Name,
		Columns:   table.DynamicColumnNamesCache,
		FilterStr: filterStr,
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
		return
	}
	serviceDataOffset := 2
	return p.insertDeltaServiceResult(serviceDataOffset, res, table)
}

func (p *Peer) insertDeltaServiceResult(serviceDataOffset int, res *ResultSet, table *DataStore) (err error) {
	updateSet, err := p.prepareServiceUpdateSet(serviceDataOffset, res, table)
	if err != nil {
		return
	}
	now := time.Now().Unix()

	p.DataLock.Lock()
	defer p.DataLock.Unlock()
	for i := range updateSet {
		update := updateSet[i]
		if update.FullUpdate {
			err = update.DataRow.UpdateValues(serviceDataOffset, update.ResultRow, &table.DynamicColumnCache, now)
		} else {
			err = update.DataRow.UpdateValuesNumberOnly(serviceDataOffset, update.ResultRow, &table.DynamicColumnCache, now)
		}
		if err != nil {
			return
		}
	}

	promObjectUpdate.WithLabelValues(p.Name, "services").Add(float64(len(*res)))
	log.Debugf("[%s] updated %d services", p.Name, len(*res))

	return
}

func (p *Peer) prepareServiceUpdateSet(serviceDataOffset int, res *ResultSet, table *DataStore) (updateSet []ResultPrepared, err error) {
	updateSet = make([]ResultPrepared, len(*res))

	// prepare list of large strings
	stringlistIndexes := make([]int, 0)
	for i := range table.DynamicColumnCache {
		col := table.DynamicColumnCache[i]
		if col.DataType == StringLargeCol {
			stringlistIndexes = append(stringlistIndexes, i+serviceDataOffset)
		}
	}

	// compare last check date and only update large strings if the last check date has changed
	lastCheckCol := table.GetColumn("last_check")
	lastCheckIndex := table.DynamicColumnCache.GetColumnIndex("last_check") + serviceDataOffset

	// prepare update
	p.DataLock.RLock()
	defer p.DataLock.RUnlock()
	nameIndex := table.Index2
	for i := range *res {
		resRow := &(*res)[i]
		prepared := ResultPrepared{
			ResultRow:  resRow,
			FullUpdate: false,
		}
		if serviceDataOffset == 0 {
			prepared.DataRow = table.Data[i]
		} else {
			dataRow := nameIndex[*(interface2stringNoDedup((*resRow)[0]))][*(interface2stringNoDedup((*resRow)[1]))]
			if dataRow == nil {
				return updateSet, fmt.Errorf("cannot update service, no service named '%s' - '%s' found", *(interface2stringNoDedup((*resRow)[0])), *(interface2stringNoDedup((*resRow)[1])))
			}
			prepared.DataRow = dataRow
		}

		// compare last check date and prepare large strings if the last check date has changed
		if interface2int64((*resRow)[lastCheckIndex]) != prepared.DataRow.GetInt64(lastCheckCol) {
			prepared.FullUpdate = true
			for j := range stringlistIndexes {
				(*res)[i][j] = interface2stringlarge((*res)[i][j])
			}
		}
		updateSet[i] = prepared
	}
	return updateSet, nil
}

// UpdateDeltaFullScan updates hosts and services tables by fetching some key indicator fields like last_check
// downtimes or acknowledged status. If an update is required, the last_check timestamp is used as filter for a
// delta update.
// The full scan just returns false without any update if the last update was less then MinFullScanInterval seconds
// ago.
// It returns true if an update was done and any error encountered.
func (p *Peer) UpdateDeltaFullScan(store *DataStore, filterStr string) (updated bool, err error) {
	updated = false
	p.PeerLock.RLock()
	var lastUpdate int64
	switch store.Table.Name {
	case TableServices:
		lastUpdate = p.Status[LastFullServiceUpdate].(int64)
	case TableHosts:
		lastUpdate = p.Status[LastFullHostUpdate].(int64)
	default:
		log.Panicf("not implemented for: " + store.Table.Name.String())
	}
	p.PeerLock.RUnlock()

	// do not do a full scan more often than every 30 seconds
	if lastUpdate > time.Now().Unix()-MinFullScanInterval {
		return
	}

	scanColumns := []string{"last_check",
		"scheduled_downtime_depth",
		"acknowledged",
		"active_checks_enabled",
		"notifications_enabled",
	}
	req := &Request{
		Table:   store.Table.Name,
		Columns: scanColumns,
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
		return
	}

	columns := make(ColumnList, len(scanColumns))
	for i, name := range scanColumns {
		columns[i] = store.Table.ColumnsIndex[name]
	}

	missing, err := p.getMissingTimestamps(store, res, &columns)
	if err != nil {
		return
	}

	if len(missing) > 0 {
		filter := []string{filterStr}
		for lastCheck := range missing {
			filter = append(filter, fmt.Sprintf("Filter: last_check = %d\n", lastCheck))
		}
		filter = append(filter, fmt.Sprintf("Or: %d\n", len(filter)))
		if store.Table.Name == TableServices {
			err = p.UpdateDeltaServices(strings.Join(filter, ""))
		} else if store.Table.Name == TableHosts {
			err = p.UpdateDeltaHosts(strings.Join(filter, ""))
		}
	}

	if store.Table.Name == TableServices {
		p.StatusSet(LastFullServiceUpdate, time.Now().Unix())
	} else if store.Table.Name == TableHosts {
		p.StatusSet(LastFullHostUpdate, time.Now().Unix())
	}
	updated = true
	return
}

// getMissingTimestamps returns list of last_check dates which can be used to delta update
func (p *Peer) getMissingTimestamps(store *DataStore, res *ResultSet, columns *ColumnList) (missing map[int64]bool, err error) {
	missing = make(map[int64]bool)
	p.DataLock.RLock()
	data := store.Data
	if len(data) < len(*res) {
		p.DataLock.RUnlock()
		// sometimes
		if p.HasFlag(Icinga2) || len(data) == 0 {
			p.reloadIfNumberOfObjectsChanged()
			return
		}
		err = &PeerError{msg: fmt.Sprintf("%s cache not ready, got %d entries but only have %d in cache", store.Table.Name.String(), len(*res), len(data)), kind: ResponseError}
		log.Warnf("[%s] %s", p.Name, err.Error())
		p.setBroken(fmt.Sprintf("got more %s than expected. Hint: check clients 'max_response_size' setting.", store.Table.Name.String()))
		return
	}
	for i := range *res {
		row := (*res)[i]
		if data[i].CheckChangedIntValues(&row, columns) {
			missing[interface2int64(row[0])] = true
		}
	}
	p.DataLock.RUnlock()
	return
}

// UpdateDeltaCommentsOrDowntimes update the comments or downtimes table. It fetches the number and highest id of
// the remote comments/downtimes. If an update is required, it then fetches all ids to check which are new and
// which have to be removed.
// It returns any error encountered.
func (p *Peer) UpdateDeltaCommentsOrDowntimes(name TableName) (err error) {
	// add new comments / downtimes
	store := p.Tables[name]

	// get number of entrys and max id
	req := &Request{
		Table:     store.Table.Name,
		FilterStr: "Stats: id != -1\nStats: max id\n",
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
		return
	}
	var maxID int64
	p.DataLock.RLock()
	entries := len(store.Data)
	if entries > 0 {
		maxID = store.Data[entries-1].GetInt64ByName("id")
	}
	p.DataLock.RUnlock()

	if len(*res) == 0 || float64(entries) == interface2float64((*res)[0][0]) && (entries == 0 || interface2float64((*res)[0][1]) == float64(maxID)) {
		log.Debugf("[%s] %s did not change", p.Name, name.String())
		return
	}

	// fetch all ids to see which ones are missing or to be removed
	req = &Request{
		Table:   store.Table.Name,
		Columns: []string{"id"},
	}
	p.setQueryOptions(req)
	res, _, err = p.Query(req)
	if err != nil {
		return
	}
	p.DataLock.Lock()
	idIndex := store.Index
	missingIds := []int64{}
	resIndex := make(map[string]bool)
	for i := range *res {
		resRow := &(*res)[i]
		id := fmt.Sprintf("%d", interface2int64((*resRow)[0]))
		_, ok := idIndex[id]
		if !ok {
			log.Debugf("adding %s with id %s", name.String(), id)
			id64 := int64((*resRow)[0].(float64))
			missingIds = append(missingIds, id64)
		}
		resIndex[id] = true
	}

	// remove old comments / downtimes
	for id := range idIndex {
		_, ok := resIndex[id]
		if !ok {
			log.Debugf("removing %s with id %s", name.String(), id)
			tmp := idIndex[id]
			store.RemoveItem(tmp)
		}
	}
	p.DataLock.Unlock()

	if len(missingIds) > 0 {
		keys, columns := store.GetInitialColumns()
		req := &Request{
			Table:     store.Table.Name,
			Columns:   keys,
			FilterStr: "",
		}
		for _, id := range missingIds {
			req.FilterStr += fmt.Sprintf("Filter: id = %d\n", id)
		}
		req.FilterStr += fmt.Sprintf("Or: %d\n", len(missingIds))
		p.setQueryOptions(req)
		res, _, err = p.Query(req)
		if err != nil {
			return
		}

		nErr := store.AppendData(res, columns)
		if nErr != nil {
			return nErr
		}
	}

	// reset cache
	switch store.Table.Name {
	case TableComments:
		p.RebuildCommentsCache()
	case TableDowntimes:
		p.RebuildDowntimesCache()
	}

	log.Debugf("[%s] updated %s", p.Name, name.String())
	return
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

	p.PeerLock.Lock()
	if p.GlobalConfig.SaveTempRequests {
		p.last.Request = req
		p.last.Response = nil
	}
	p.Status[Querys] = p.Status[Querys].(int64) + 1
	totalBytesSend := p.Status[BytesSend].(int64) + int64(len(query))
	p.Status[BytesSend] = totalBytesSend
	peerAddr := p.Status[PeerAddr].(string)
	p.PeerLock.Unlock()
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
	p.PeerLock.Lock()
	if p.GlobalConfig.SaveTempRequests {
		p.last.Response = resBytes
	}
	totalBytesReceived := p.Status[BytesReceived].(int64) + int64(len(*resBytes))
	p.Status[BytesReceived] = totalBytesReceived
	p.PeerLock.Unlock()
	promPeerBytesReceived.WithLabelValues(p.Name).Set(float64(totalBytesReceived))

	data, meta, err := req.parseResult(resBytes)

	if err != nil {
		log.Debugf("[%s] fetched table %20s time: %s, size: %d kB", p.Name, req.Table.String(), duration, len(*resBytes)/1024)
		log.Errorf("[%s] result json string: %s", p.Name, string(*resBytes))
		log.Errorf("[%s] result parse error: %s", p.Name, err.Error())
		return nil, nil, &PeerError{msg: err.Error(), kind: ResponseError}
	}

	log.Debugf("[%s] fetched table: %15s - time: %8s - count: %8d - size: %8d kB", p.Name, req.Table.String(), duration, len(*data), len(*resBytes)/1024)
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
	res, err := p.HTTPQueryWithRetrys(req, peerAddr, query, 2)
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
		return nil, err
	}
	_, err = fmt.Fprintf(conn, "%s", query)
	if err != nil {
		return nil, err
	}

	// close write part of connection
	// but only on commands, it'll breaks larger responses with stunnel / xinetd constructs
	if req.Command != "" && !req.KeepAlive {
		switch c := conn.(type) {
		case *net.TCPConn:
			c.CloseWrite()
		case *net.UnixConn:
			c.CloseWrite()
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
			if err != io.EOF {
				return nil, err
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
		io.CopyN(header, conn, ErrorContentPreviewSize)
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
	if err != nil && err != io.EOF {
		return nil, err
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
		return nil, nil, err
	}
	if req == nil {
		err = errors.New("bad request: empty request")
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
// In case of a http connection, it just trys a tcp connect, but does not
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
				p.ClearFlags()
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

	// both locks are required, because we might clear the datastore
	p.PeerLock.Lock()
	defer p.PeerLock.Unlock()
	p.DataLock.Lock()
	defer p.DataLock.Unlock()

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
			// clear existing data from memory
			p.Clear(false)
		}
		p.Status[PeerState] = PeerStatusDown
	}

	if numSources > 1 {
		log.Debugf("[%s] trying next one: %s", p.Name, peerAddr)
	}
}

// CreateObjectByType fetches all static and dynamic data from the remote site and creates the initial table.
// It returns any error encountered.
func (p *Peer) CreateObjectByType(table *Table) (err error) {
	// log table does not create objects
	if table.PassthroughOnly || table.Virtual != nil {
		return
	}

	store := NewDataStore(table, p)
	keys, columns := store.GetInitialColumns()

	// fetch remote objects
	req := &Request{
		Table:   store.Table.Name,
		Columns: keys,
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)

	if err != nil {
		return
	}

	// icinga2 returns hosts and services in random order but we assume ordered results later
	if p.HasFlag(Icinga2) {
		res = res.SortByPrimaryKey(table, req)
	}

	now := time.Now().Unix()
	err = store.InsertData(res, columns)
	if err != nil {
		return
	}

	p.DataLock.Lock()
	p.Tables[store.Table.Name] = store
	p.DataLock.Unlock()
	p.PeerLock.Lock()
	p.Status[LastUpdate] = now
	p.Status[LastFullUpdate] = now
	p.PeerLock.Unlock()
	promObjectCount.WithLabelValues(p.Name, table.Name.String()).Set(float64(len((*res))))

	return
}

func (p *Peer) checkStatusFlags() {
	// set backend specific flags
	p.DataLock.RLock()
	store := p.Tables[TableStatus]
	if store == nil {
		return
	}
	data := store.Data
	p.DataLock.RUnlock()
	if len(data) == 0 {
		return
	}
	p.PeerLock.Lock()
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
			p.PeerLock.Unlock()

			ok := true
			if p.HasFlag(LMD) {
				p.periodicUpdateLMD(&ok, true)
			} else {
				p.periodicUpdateMultiBackends(&ok, true)
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
	p.PeerLock.Unlock()
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
	output, result, err := p.HTTPPostQuery(nil, peerAddr, url.Values{
		"data": {fmt.Sprintf("{\"credential\": \"%s\", \"options\": %s}", p.Config.Auth, optionStr)},
	}, nil)
	if err != nil {
		return
	}
	conf, ok := extractConfigToolResult(output)
	if !ok {
		err = &PeerError{msg: fmt.Sprintf("unknown site error, got: %v", result), kind: ResponseError}
		return
	}
	return
}

func extractConfigToolResult(output []interface{}) (map[string]interface{}, bool) {
	if len(output) < 3 {
		return nil, false
	}
	data, ok := output[2].(map[string]interface{})
	if !ok {
		return nil, false
	}
	for k := range data {
		if processinfo, ok2 := data[k].(map[string]interface{}); ok2 {
			if c, ok2 := processinfo["configtool"]; ok2 {
				if v, ok3 := c.(map[string]interface{}); ok3 {
					return v, true
				}
			}
		}
	}
	return nil, false
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
		if strings.HasPrefix(addr, "http") {
			sites, err = p.fetchRemotePeersFromAddr(addr)
			if err == nil && len(sites) > 1 {
				if !p.HasFlag(MultiBackend) {
					p.PeerLock.Lock()
					log.Infof("[%s] remote connection MultiBackend flag set, got %d sites", p.Name, len(sites))
					p.SetFlag(MultiBackend)
					p.PeerLock.Unlock()
					ok := true
					p.periodicUpdateMultiBackends(&ok, true)
				}
				return
			}
		}
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

// UpdateFullTable updates a given table by requesting all dynamic columns from the remote peer.
// Assuming we get the objects always in the same order, we can just iterate over the index and update the fields.
// It returns a boolean flag whether the remote site has been restarted and any error encountered.
func (p *Peer) UpdateFullTable(tableName TableName) (restartRequired bool, err error) {
	store := p.Tables[tableName]
	if p.skipTableUpdate(store) {
		return
	}

	columns := store.DynamicColumnNamesCache
	// primary keys are not required, we fetch everything anyway
	primaryKeysLen := len(store.Table.PrimaryKey)
	if primaryKeysLen > 0 {
		columns = columns[primaryKeysLen:]
	}

	req := &Request{
		Table:   store.Table.Name,
		Columns: columns,
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
		return
	}

	// icinga2 returns hosts and services in random order but we assume ordered results later
	if p.HasFlag(Icinga2) {
		res = res.SortByPrimaryKey(store.Table, req)
	}

	p.DataLock.RLock()
	data := store.Data
	p.DataLock.RUnlock()
	if len(*res) != len(data) {
		log.Debugf("[%s] site returned different number of objects, assuming backend has been restarted, table: %s, expected: %d, received: %d", p.Name, store.Table.Name, len(data), len(*res))
		restartRequired = true
		return
	}

	promObjectUpdate.WithLabelValues(p.Name, tableName.String()).Add(float64(len(*res)))

	switch tableName {
	case TableTimeperiods:
		// check for changed timeperiods, because we have to update the linked hosts and services as well
		err = p.updateTimeperiodsData(store, res, &store.DynamicColumnCache)
	case TableHosts:
		err = p.insertDeltaHostResult(0, res, store)
	case TableServices:
		err = p.insertDeltaServiceResult(0, res, store)
	default:
		p.DataLock.Lock()
		now := time.Now().Unix()
		for i := range *res {
			row := (*res)[i]
			err = data[i].UpdateValues(0, &row, &store.DynamicColumnCache, now)
			if err != nil {
				p.DataLock.Unlock()
				return
			}
		}
		p.DataLock.Unlock()
	}

	if tableName == TableStatus {
		p.checkStatusFlags()
		if !p.HasFlag(MultiBackend) && len(data) >= 1 && (p.StatusGet(ProgramStart) != data[0].GetInt64ByName("program_start") || p.StatusGet(LastPid) != data[0].GetIntByName("nagios_pid")) {
			log.Infof("[%s] site has been restarted, recreating objects", p.Name)
			restartRequired = true
		}
	}

	return
}

func (p *Peer) skipTableUpdate(store *DataStore) bool {
	if store == nil {
		return true
	}
	if store.Table.Virtual != nil {
		return true
	}
	// no updates for passthrough tables, ex.: log
	if store.Table.PassthroughOnly {
		return true
	}
	if p.HasFlag(MultiBackend) && store.Table.Name != TableStatus {
		return true
	}
	if len(store.DynamicColumnNamesCache) == 0 {
		return true
	}
	return false
}

func (p *Peer) updateTimeperiodsData(store *DataStore, res *ResultSet, columns *ColumnList) (err error) {
	changedTimeperiods := make(map[string]bool)
	p.DataLock.Lock()
	data := store.Data
	now := time.Now().Unix()
	nameCol := store.GetColumn("name")
	for i := range *res {
		row := (*res)[i]
		if data[i].CheckChangedIntValues(&row, columns) {
			changedTimeperiods[*(data[i].GetString(nameCol))] = true
		}
		data[i].UpdateValues(0, &row, columns, now)
	}
	p.DataLock.Unlock()
	// Update hosts and services with those changed timeperiods
	for name, state := range changedTimeperiods {
		log.Debugf("[%s] timeperiod %s has changed to %v, need to update affected hosts/services", p.Name, name, state)
		err = p.UpdateDeltaHosts("Filter: check_period = " + name + "\nFilter: notification_period = " + name + "\nOr: 2\n")
		if err != nil {
			return
		}
		err = p.UpdateDeltaServices("Filter: check_period = " + name + "\nFilter: notification_period = " + name + "\nOr: 2\n")
		if err != nil {
			return
		}
	}
	return
}

// WaitCondition waits for a given condition.
// It returns true if the wait timed out or false if the condition matched successfully.
func (p *Peer) WaitCondition(req *Request) bool {
	// wait up to one minute if nothing specified
	if req.WaitTimeout <= 0 {
		req.WaitTimeout = WaitTimeoutDefault
	}
	c := make(chan struct{})
	go func(p *Peer, c chan struct{}, req *Request) {
		p.waitcondition(c, req)
	}(p, c, req)
	select {
	case <-c:
		return false // completed normally
	case <-time.After(time.Duration(req.WaitTimeout) * time.Millisecond):
		close(c)
		return true // timed out
	}
}

func (p *Peer) waitcondition(c chan struct{}, req *Request) {
	// make sure we log panics properly
	defer logPanicExitPeer(p)

	var lastUpdate int64
	for {
		select {
		case <-c:
			// canceled
			return
		default:
		}

		store, err := p.GetDataStore(req.Table)
		if err != nil {
			time.Sleep(time.Millisecond * 200)
			continue
		}

		// waiting for final update to complete
		if lastUpdate > 0 {
			curUpdate := p.StatusGet(LastUpdate).(int64)
			// wait up to WaitTimeout till the update is complete
			if curUpdate > lastUpdate {
				close(c)
				return
			}
			time.Sleep(time.Millisecond * 200)
			continue
		}

		// get object to watch
		var found = false
		if req.WaitObject != "" {
			obj, ok := p.getWaitObject(store, req)
			if !ok {
				log.Errorf("WaitObject did not match any object: %s", req.WaitObject)
				close(c)
				return
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
		time.Sleep(time.Millisecond * 200)
		switch req.Table {
		case TableHosts:
			p.UpdateDeltaHosts("Filter: name = " + req.WaitObject + "\n")
		case TableServices:
			tmp := strings.SplitN(req.WaitObject, ";", 2)
			if len(tmp) < 2 {
				log.Errorf("unsupported service wait object: %s", req.WaitObject)
				close(c)
				return
			}
			p.UpdateDeltaServices("Filter: host_name = " + tmp[0] + "\nFilter: description = " + tmp[1] + "\n")
		default:
			p.UpdateFullTable(store.Table.Name)
		}
	}
}

// HTTPQueryWithRetrys calls HTTPQuery with given amount of retries.
func (p *Peer) HTTPQueryWithRetrys(req *Request, peerAddr string, query string, retries int) (res []byte, err error) {
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
	req, err := http.NewRequest("POST", peerAddr, strings.NewReader(postData.Encode()))
	if err != nil {
		return nil, err
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
	contents, hErr := ioutil.ReadAll(response.Body)
	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	if hErr != nil {
		err = hErr
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

// SpinUpPeers starts an immediate parallel delta update for all supplied peer ids.
func SpinUpPeers(peers []*Peer) {
	waitgroup := &sync.WaitGroup{}
	for i := range peers {
		p := peers[i]
		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)
			defer wg.Done()

			peer.StatusSet(Idling, false)
			log.Infof("[%s] switched back to normal update interval", peer.Name)
			if peer.StatusGet(PeerState).(PeerStatus) == PeerStatusUp {
				log.Debugf("[%s] spin up update", peer.Name)
				peer.UpdateFullTable(TableTimeperiods)
				peer.UpdateDelta(p.StatusGet(LastUpdate).(int64), time.Now().Unix())
				log.Debugf("[%s] spin up update done", peer.Name)
			} else {
				// force new update sooner
				peer.StatusSet(LastUpdate, time.Now().Unix()-peer.GlobalConfig.Updateinterval)
			}
		}(p, waitgroup)
	}
	waitTimeout(waitgroup, SpinUpPeersTimeout)
	log.Debugf("spin up completed")
}

// BuildLocalResponseData returns the result data for a given request
func (p *Peer) BuildLocalResponseData(res *Response, store *DataStore, resultcollector chan *PeerResponse) {
	req := res.Request
	log.Tracef("BuildLocalResponseData: %s", p.Name)

	// if a WaitTrigger is supplied, wait max ms till the condition is true
	if req.WaitTrigger != "" {
		p.WaitCondition(req)

		// peer might have gone down meanwhile, ex. after waiting for a waittrigger, so check again
		s, err := p.GetDataStore(req.Table)
		if err != nil {
			res.Lock.Lock()
			res.Failed[p.ID] = err.Error()
			res.Lock.Unlock()
			return
		}
		store = s
	}

	// for some tables its faster to lock the table only once
	if store.PeerLockMode == PeerLockModeFull {
		p.PeerLock.RLock()
		defer p.PeerLock.RUnlock()
	}

	if !store.Table.WorksUnlocked {
		p.DataLock.RLock()
		defer p.DataLock.RUnlock()
	}

	if len(store.Data) == 0 {
		return
	}

	if len(res.Request.Stats) > 0 {
		// stats queries
		res.MergeStats(p.gatherStatsResult(res, store))
	} else {
		// data queries
		p.gatherResultRows(res, store, resultcollector)
	}
}

// PassThrougQuery runs a passthrough query on a single peer and appends the result
func (p *Peer) PassThrougQuery(res *Response, passthroughRequest *Request, virtColumns []*Column, columnsIndex map[*Column]int) {
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
	if len(virtColumns) > 0 {
		table := Objects.Tables[res.Request.Table]
		store := NewDataStore(table, p)
		tmpRow, _ := NewDataRow(store, nil, nil, 0)
		for rowNum := range *result {
			row := &((*result)[rowNum])
			for j := range virtColumns {
				col := virtColumns[j]
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
		// apply stats querys
		if len(*result) > 0 {
			for i := range (*result)[0] {
				val := (*result)[0][i].(float64)
				res.Request.StatsResult[""][i].ApplyValue(val, int(val))
			}
		}
	}
	res.Lock.Unlock()
}

// isOnline returns true if this peer is online and has data
func (p *Peer) isOnline() bool {
	status := p.StatusGet(PeerState).(PeerStatus)
	if p.HasFlag(LMDSub) {
		realStatus := p.StatusGet(SubPeerStatus).(map[string]interface{})
		num, ok := realStatus["status"]
		if !ok {
			return false
		}
		status = PeerStatus(num.(float64))
	}
	if status == PeerStatusUp || status == PeerStatusWarning {
		return true
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

func (p *Peer) gatherResultRows(res *Response, store *DataStore, resultcollector chan *PeerResponse) {
	result := &PeerResponse{}
	defer func() {
		resultcollector <- result
	}()
	req := res.Request

	// if there is no sort header or sort by name only,
	// we can drastically reduce the result set by applying the limit here already
	limit := optimizeResultLimit(req)
	if limit <= 0 {
		limit = len(store.Data) + 1
	}

	// no need to count all the way to the end unless the total number is required in wrapped_json output
	breakOnLimit := res.Request.OutputFormat != OutputFormatWrappedJSON

Rows:
	for _, row := range store.Data {
		// does our filter match?
		for _, f := range req.Filter {
			if !row.MatchFilter(f) {
				continue Rows
			}
		}

		if !row.checkAuth(req.AuthUser) {
			continue Rows
		}

		result.Total++

		// check if we have enough result rows already
		// we still need to count how many result we would have...
		if result.Total > limit {
			if breakOnLimit {
				return
			}
			continue Rows
		}
		result.Rows = append(result.Rows, row)
	}
}

func (p *Peer) gatherStatsResult(res *Response, store *DataStore) *ResultSetStats {
	localStats := make(ResultSetStats)
	req := res.Request

Rows:
	for _, row := range store.Data {
		// does our filter match?
		for _, f := range req.Filter {
			if !row.MatchFilter(f) {
				continue Rows
			}
		}

		if !row.checkAuth(req.AuthUser) {
			continue Rows
		}

		key := row.getStatsKey(res)
		stat := localStats[key]
		if stat == nil {
			stat = createLocalStatsCopy(&req.Stats)
			localStats[key] = stat
		}

		// count stats
		for i := range req.Stats {
			s := req.Stats[i]
			// avg/sum/min/max are passed through, they dont have filter
			// counter must match their filter
			if s.StatsType == Counter {
				if row.MatchFilter(s) {
					stat[i].Stats++
					stat[i].StatsCount++
				}
			} else {
				stat[i].ApplyValue(row.GetFloat(s.Column), 1)
			}
		}
	}

	return &localStats
}

func (p *Peer) waitConditionTableMatches(store *DataStore, filter *[]*Filter) bool {
	p.DataLock.RLock()
	defer p.DataLock.RUnlock()

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
func optimizeResultLimit(req *Request) (limit int) {
	if req.Limit != nil && req.IsDefaultSortOrder() {
		limit = *req.Limit
		if req.Offset > 0 {
			limit += req.Offset
		}
	} else {
		limit = -1
	}
	return
}

func (p *Peer) clearLastRequest(lock bool) {
	if !p.GlobalConfig.SaveTempRequests {
		return
	}
	if lock {
		p.PeerLock.Lock()
		defer p.PeerLock.Unlock()
	}
	p.last.Request = nil
	p.last.Response = nil
}

func (p *Peer) reloadIfNumberOfObjectsChanged() bool {
	if p.hasChanged() {
		return (p.InitAllTables())
	}
	return (true)
}

func (p *Peer) setBroken(details string) {
	log.Warnf("[%s] %s", p.Name, details)
	p.PeerLock.Lock()
	p.Status[PeerState] = PeerStatusBroken
	p.Status[LastError] = "broken: " + details
	p.Status[ThrukVersion] = float64(-1)
	p.Clear(false)
	p.PeerLock.Unlock()
}

func logPanicExitPeer(p *Peer) {
	if r := recover(); r != nil {
		log.Errorf("[%s] Panic: %s", p.Name, r)
		log.Errorf("[%s] Version: %s", p.Name, Version())
		log.Errorf("[%s] %s", p.Name, debug.Stack())
		if p.last.Request != nil {
			log.Errorf("[%s] LastQuery:", p.Name)
			log.Errorf("[%s] %s", p.Name, p.last.Request.String())
			log.Errorf("[%s] LastResponse:", p.Name)
			log.Errorf("[%s] %s", p.Name, string(*(p.last.Response)))
		}
		deletePidFile(flagPidfile)
		os.Exit(1)
	}
}

// Result2Hash converts list result into hashes
func Result2Hash(data *ResultSet, columns []string) []map[string]interface{} {
	hash := make([]map[string]interface{}, 0)
	for _, row := range *data {
		rowHash := make(map[string]interface{})
		for x, key := range columns {
			rowHash[key] = row[x]
		}
		hash = append(hash, rowHash)
	}
	return hash
}

func (p *Peer) getTLSClientConfig() (*tls.Config, error) {
	config := &tls.Config{}
	if p.Config.TLSCertificate != "" && p.Config.TLSKey != "" {
		cer, err := tls.LoadX509KeyPair(p.Config.TLSCertificate, p.Config.TLSKey)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{cer}
	}

	if p.Config.TLSSkipVerify > 0 || p.GlobalConfig.SkipSSLCheck > 0 {
		config.InsecureSkipVerify = true
	}

	if p.Config.TLSCA != "" {
		caCert, err := ioutil.ReadFile(p.Config.TLSCA)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		config.RootCAs = caCertPool
	}

	return config, nil
}

// SendCommandsWithRetry sends list of commands and retries until the peer is completly down
func (p *Peer) SendCommandsWithRetry(commands []string) (err error) {
	p.PeerLock.Lock()
	p.Status[LastQuery] = time.Now().Unix()
	if p.Status[Idling].(bool) {
		p.Status[Idling] = false
		log.Infof("[%s] switched back to normal update interval", p.Name)
	}
	p.PeerLock.Unlock()

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

// RebuildCommentsCache updates the comment cache
func (p *Peer) RebuildCommentsCache() {
	cache := p.buildDowntimeCommentsCache(TableComments)
	p.PeerLock.Lock()
	p.cache.comments = cache
	p.PeerLock.Unlock()
	log.Debugf("comments cache rebuild")
}

// RebuildDowntimesCache updates the comment cache
func (p *Peer) RebuildDowntimesCache() {
	cache := p.buildDowntimeCommentsCache(TableDowntimes)
	p.PeerLock.Lock()
	p.cache.downtimes = cache
	p.PeerLock.Unlock()
	log.Debugf("downtimes cache rebuild")
}

// buildDowntimesCache returns the downtimes/comments cache
func (p *Peer) buildDowntimeCommentsCache(name TableName) map[*DataRow][]int64 {
	p.DataLock.RLock()
	cache := make(map[*DataRow][]int64)
	store := p.Tables[name]
	idIndex := store.Table.GetColumn("id").Index
	hostNameIndex := store.Table.GetColumn("host_name").Index
	serviceDescIndex := store.Table.GetColumn("service_description").Index
	hostIndex := p.Tables[TableHosts].Index
	serviceIndex := p.Tables[TableServices].Index2
	for i := range store.Data {
		row := store.Data[i]
		key := row.dataString[hostNameIndex]
		serviceName := row.dataString[serviceDescIndex]
		var obj *DataRow
		if serviceName != "" {
			obj = serviceIndex[key][serviceName]
		} else {
			obj = hostIndex[key]
		}
		id := row.dataInt64[idIndex]
		cache[obj] = append(cache[obj], id)
	}
	p.DataLock.RUnlock()
	promObjectCount.WithLabelValues(p.Name, name.String()).Set(float64(len(store.Data)))

	return cache
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

// ClearFlags removes all flags
func (p *Peer) ClearFlags() {
	atomic.StoreUint32(&p.Flags, uint32(NoFlags))
}

// GetDataStore returns store for given name or error if peer is offline
func (p *Peer) GetDataStore(tableName TableName) (store *DataStore, err error) {
	table := Objects.Tables[tableName]
	if table.Virtual != nil {
		store = table.Virtual(table, p)
		if store == nil {
			err = fmt.Errorf("%s", p.getError())
			return
		}
		return
	}
	p.DataLock.RLock()
	store = p.Tables[tableName]
	p.DataLock.RUnlock()
	if store == nil || !p.isOnline() {
		store = nil
		err = fmt.Errorf("%s", p.getError())
		return
	}
	return
}

// GetSupportedColumns returns a list of supported columns
func (p *Peer) GetSupportedColumns() (tables map[TableName]map[string]bool, err error) {
	req := &Request{
		Table:   TableColumns,
		Columns: []string{"table", "name"},
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
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

func (p *Peer) getWaitObject(store *DataStore, req *Request) (*DataRow, bool) {
	p.DataLock.RLock()
	defer p.DataLock.RUnlock()
	if req.Table == TableServices {
		parts := strings.SplitN(req.WaitObject, ";", 2)
		obj, ok := store.Index2[parts[0]][parts[1]]
		return obj, ok
	}
	obj, ok := store.Index[req.WaitObject]
	return obj, ok
}
