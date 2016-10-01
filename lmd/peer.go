package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var reResponseHeader = regexp.MustCompile(`^(\d+)\s+(\d+)$`)
var reHTTPTooOld = regexp.MustCompile(`Can.t locate object method`)
var reShinkenVersion = regexp.MustCompile(`-shinken$`)

const (
	// UpdateAdditionalDelta is the number of seconds to add to the last_check filter on delta updates
	UpdateAdditionalDelta = 3

	// MinFullScanInterval is the minimum interval between two full scans
	MinFullScanInterval = 30
)

// DataTable contains the actual data with a reference to the table.
type DataTable struct {
	Table *Table
	Data  [][]interface{}
	Refs  map[string][][]interface{}
	Index map[string][]interface{}
}

// Peer is the object which handles collecting and updating data and connections.
type Peer struct {
	Name            string
	ID              string
	Source          []string
	PeerLock        *sync.RWMutex
	DataLock        *sync.RWMutex
	Tables          map[string]DataTable
	Status          map[string]interface{}
	ErrorCount      int
	waitGroup       *sync.WaitGroup
	shutdownChannel chan bool
	Config          Connection
	Flags           OptionalFlags
}

// PeerStatus contains the different states a peer can have
type PeerStatus int

// A peer can be up, warning, down and pending.
// It is pending right after start and warning when the connection fails
// but the stale timeout is not yet hit.
const (
	PeerStatusUp PeerStatus = iota
	PeerStatusWarning
	PeerStatusDown
	_
	PeerStatusPending
)

// PeerErrorType is used to distinguish between connection and response errors.
type PeerErrorType int

const (
	// ConnectionError is used when the connection to a remote site failed
	ConnectionError PeerErrorType = iota

	// ResponseError is used when the remote site is available but returns an unusable result.
	ResponseError
)

// HTTPResult contains the livestatus result as long with some meta data.
type HTTPResult struct {
	Rc      int
	Version string
	Branch  string
	Output  json.RawMessage
}

// PeerError is a custom error to distinguish between connection and response errors.
type PeerError struct {
	msg  string
	kind PeerErrorType
}

// Error returns the error message as string.
func (e *PeerError) Error() string { return e.msg }

// Type returns the error type.
func (e *PeerError) Type() PeerErrorType { return e.kind }

// AddItem adds an new entry to a datatable.
func (d *DataTable) AddItem(row *[]interface{}) {
	d.Data = append(d.Data, *row)
	return
}

// RemoveItem removes an entry from a datatable.
func (d *DataTable) RemoveItem(row []interface{}) {
	for i, r := range d.Data {
		if fmt.Sprintf("%p", r) == fmt.Sprintf("%p", row) {
			d.Data = append(d.Data[:i], d.Data[i+1:]...)
			delete(d.Index, fmt.Sprintf("%v", r[d.Table.GetColumn("id").Index]))
			return
		}
	}
	panic("element not found")
}

// NewPeer creates a new peer object.
// It returns the created peer.
func NewPeer(config Connection, waitGroup *sync.WaitGroup, shutdownChannel chan bool) *Peer {
	p := Peer{
		Name:            config.Name,
		ID:              config.ID,
		Source:          config.Source,
		Tables:          make(map[string]DataTable),
		Status:          make(map[string]interface{}),
		ErrorCount:      0,
		waitGroup:       waitGroup,
		shutdownChannel: shutdownChannel,
		PeerLock:        new(sync.RWMutex),
		DataLock:        new(sync.RWMutex),
		Config:          config,
	}
	p.Status["PeerKey"] = p.ID
	p.Status["PeerName"] = p.Name
	p.Status["CurPeerAddrNum"] = 0
	p.Status["PeerAddr"] = p.Source[p.Status["CurPeerAddrNum"].(int)]
	p.Status["PeerStatus"] = PeerStatusPending
	p.Status["LastUpdate"] = time.Now()
	p.Status["LastFullUpdate"] = time.Now()
	p.Status["LastFullHostUpdate"] = time.Now()
	p.Status["LastFullServiceUpdate"] = time.Now()
	p.Status["LastQuery"] = time.Now()
	p.Status["LastError"] = "connecting..."
	p.Status["LastOnline"] = time.Time{}
	p.Status["ProgramStart"] = 0
	p.Status["BytesSend"] = 0
	p.Status["BytesReceived"] = 0
	p.Status["Querys"] = 0
	p.Status["ReponseTime"] = 0
	p.Status["Idling"] = false
	return &p
}

// Start creates the initial objects and starts the update loop in a separate goroutine.
func (p *Peer) Start() {
	go func() {
		defer p.waitGroup.Done()
		p.waitGroup.Add(1)
		log.Infof("[%s] starting connection", p.Name)
		p.UpdateLoop()
	}()

	return
}

// UpdateLoop is the main loop updating this peer.
// It does not return till triggered by the shutdownChannel.
func (p *Peer) UpdateLoop() {
	for {
		ok := p.InitAllTables()
		lastFullUpdateMinute, _ := strconv.Atoi(time.Now().Format("4"))

		c := time.Tick(500 * time.Millisecond)
		for {
			select {
			case <-p.shutdownChannel:
				log.Debugf("[%s] stopping...", p.Name)
				return
			case <-c:
				p.PeerLock.RLock()
				lastUpdate := p.Status["LastUpdate"].(time.Time).Unix()
				lastQuery := p.Status["LastQuery"].(time.Time).Unix()
				lastFullUpdate := p.Status["LastFullUpdate"].(time.Time).Unix()
				idling := p.Status["Idling"].(bool)
				p.PeerLock.RUnlock()
				now := time.Now().Unix()

				if !idling && lastQuery < now-GlobalConfig.IdleTimeout {
					log.Infof("[%s] switched to idle interval, last query: %s", p.Name, time.Unix(lastQuery, 0))
					p.PeerLock.Lock()
					p.Status["Idling"] = true
					p.PeerLock.Unlock()
					idling = true
				}

				currentMinute, _ := strconv.Atoi(time.Now().Format("4"))
				if idling {
					// idle update interval
					if now > lastUpdate+GlobalConfig.IdleInterval {
						if !ok {
							ok = p.InitAllTables()
							lastFullUpdateMinute = currentMinute
						} else {
							p.UpdateObjectByType(Objects.Tables["timeperiods"])
							ok = p.UpdateDeltaTables()
						}
					}
				} else {
					// full update interval
					if GlobalConfig.FullUpdateInterval > 0 && now > lastFullUpdate+GlobalConfig.FullUpdateInterval {
					} else if now > lastUpdate+GlobalConfig.Updateinterval {
						// normal update interval
						if !ok {
							ok = p.InitAllTables()
							lastFullUpdateMinute = currentMinute
						} else {
							if lastFullUpdateMinute != currentMinute {
								//ok = p.UpdateAllTables()
								p.UpdateObjectByType(Objects.Tables["timeperiods"])
								lastFullUpdateMinute = currentMinute
							}
							ok = p.UpdateDeltaTables()
						}
					}
				}
				break
			}
		}
	}
}

// InitAllTables creates all tables for this peer.
// It returns true if the import was succesful or false otherwise.
func (p *Peer) InitAllTables() bool {
	var err error
	p.PeerLock.Lock()
	p.Status["LastUpdate"] = time.Now()
	p.Status["LastFullUpdate"] = time.Now()
	p.PeerLock.Unlock()
	t1 := time.Now()
	for _, n := range Objects.Order {
		t := Objects.Tables[n]
		_, err = p.CreateObjectByType(&t)
		if err != nil {
			return false
		}
	}
	p.PeerLock.Lock()
	// this may happen if we query another lmd daemon which has no backends ready yet
	if len(p.Tables["status"].Data) == 0 {
		p.Status["PeerStatus"] = PeerStatusWarning
		p.Status["LastError"] = "peered partner not ready yet"
		p.PeerLock.Unlock()
		return false
	}
	p.DataLock.RLock()
	p.Status["ProgramStart"] = p.Tables["status"].Data[0][p.Tables["status"].Table.ColumnsIndex["program_start"]]
	p.DataLock.RUnlock()
	duration := time.Since(t1)
	p.Status["ReponseTime"] = duration.Seconds()
	p.PeerLock.Unlock()
	log.Infof("[%s] objects created in: %s", p.Name, duration.String())

	if p.Status["PeerStatus"].(PeerStatus) != PeerStatusUp {
		// Reset errors
		p.PeerLock.Lock()
		if p.Status["PeerStatus"].(PeerStatus) == PeerStatusDown {
			log.Infof("[%s] site is back online", p.Name)
		}
		p.Status["LastError"] = ""
		p.Status["LastOnline"] = time.Now()
		p.ErrorCount = 0
		p.Status["PeerStatus"] = PeerStatusUp
		p.PeerLock.Unlock()
	}
	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())

	return true
}

// UpdateAllTables runs a full update on all dynamic values for all tables which have dynamic updated columns.
// It returns true if the update was succesful or false otherwise.
func (p *Peer) UpdateAllTables() bool {
	t1 := time.Now()
	var err error
	restartRequired := false
	for _, n := range Objects.Order {
		t := Objects.Tables[n]
		restartRequired, err = p.UpdateObjectByType(t)
		if err != nil {
			return false
		}
		if restartRequired {
			break
		}
	}
	if err != nil {
		return false
	}
	if restartRequired {
		return p.InitAllTables()
	}
	duration := time.Since(t1)
	p.PeerLock.Lock()
	p.Status["ReponseTime"] = duration.Seconds()
	p.Status["LastUpdate"] = time.Now()
	p.Status["LastFullUpdate"] = time.Now()
	p.Status["LastOnline"] = time.Now()
	p.Status["PeerStatus"] = PeerStatusUp
	p.Status["LastError"] = ""
	p.PeerLock.Unlock()
	log.Infof("[%s] update complete in: %s", p.Name, duration.String())
	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())
	return true
}

// UpdateDeltaTables runs a delta update on all status, hosts, services, comments and downtimes table.
// It returns true if the update was succesful or false otherwise.
func (p *Peer) UpdateDeltaTables() bool {
	t1 := time.Now()

	restartRequired, err := p.UpdateObjectByType(Objects.Tables["status"])
	if restartRequired {
		return p.InitAllTables()
	}
	if err == nil {
		err = p.UpdateDeltaTableHosts("")
	}
	if err == nil {
		err = p.UpdateDeltaTableServices("")
	}
	if err == nil {
		err = p.UpdateDeltaCommentsOrDowntimes("comments")
	}
	if err == nil {
		err = p.UpdateDeltaCommentsOrDowntimes("downtimes")
	}

	duration := time.Since(t1)
	if err != nil {
		log.Infof("[%s] updating objects failed after: %s: %s", p.Name, duration.String(), err.Error())
		return false
	}
	log.Debugf("[%s] delta update complete in: %s", p.Name, duration.String())
	p.PeerLock.Lock()
	p.Status["LastUpdate"] = time.Now()
	p.Status["LastOnline"] = time.Now()
	p.Status["ReponseTime"] = duration.Seconds()
	p.PeerLock.Unlock()
	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())
	return true
}

// UpdateDeltaTableHosts update services by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional UpdateAdditionalDelta seconds.
// It returns any error encountered.
func (p *Peer) UpdateDeltaTableHosts(filterStr string) (err error) {
	// update changed hosts
	table := Objects.Tables["hosts"]
	keys, indexes := table.GetDynamicColumns(p.Flags)
	keys = append(keys, "name")
	if filterStr == "" {
		filterStr = fmt.Sprintf("Filter: last_check >= %v\nFilter: is_executing = 1\nOr: 2\n", (p.Status["LastUpdate"].(time.Time).Unix() - UpdateAdditionalDelta))
		// no filter means regular delta update, so lets check if all last_check dates match
		ok, err := p.UpdateDeltaTableFullScan(&table, filterStr)
		if ok || err != nil {
			return err
		}
	}
	req := &Request{
		Table:           table.Name,
		Columns:         keys,
		ResponseFixed16: true,
		OutputFormat:    "json",
		FilterStr:       filterStr,
	}
	res, err := p.Query(req)
	if err != nil {
		return
	}
	p.DataLock.Lock()
	nameindex := p.Tables[table.Name].Index
	fieldIndex := len(keys) - 1
	for _, resRow := range res {
		dataRow := nameindex[resRow[fieldIndex].(string)]
		for j, k := range indexes {
			dataRow[k] = resRow[j]
		}
	}
	p.DataLock.Unlock()
	promPeerUpdatedHosts.WithLabelValues(p.Name).Add(float64(len(res)))
	log.Debugf("[%s] updated %d hosts", p.Name, len(res))

	return
}

// UpdateDeltaTableServices update services by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional UpdateAdditionalDelta seconds.
// It returns any error encountered.
func (p *Peer) UpdateDeltaTableServices(filterStr string) (err error) {
	// update changed services
	table := Objects.Tables["services"]
	keys, indexes := table.GetDynamicColumns(p.Flags)
	keys = append(keys, []string{"host_name", "description"}...)
	if filterStr == "" {
		filterStr = fmt.Sprintf("Filter: last_check >= %v\nFilter: is_executing = 1\nOr: 2\n", (p.Status["LastUpdate"].(time.Time).Unix() - UpdateAdditionalDelta))
		// no filter means regular delta update, so lets check if all last_check dates match
		ok, err := p.UpdateDeltaTableFullScan(&table, filterStr)
		if ok || err != nil {
			return err
		}
	}
	req := &Request{
		Table:           table.Name,
		Columns:         keys,
		ResponseFixed16: true,
		OutputFormat:    "json",
		FilterStr:       filterStr,
	}
	res, err := p.Query(req)
	if err != nil {
		return
	}
	p.DataLock.Lock()
	nameindex := p.Tables[table.Name].Index
	fieldIndex1 := len(keys) - 2
	fieldIndex2 := len(keys) - 1
	for _, resRow := range res {
		dataRow := nameindex[resRow[fieldIndex1].(string)+";"+resRow[fieldIndex2].(string)]
		for j, k := range indexes {
			dataRow[k] = resRow[j]
		}
	}
	p.DataLock.Unlock()
	promPeerUpdatedServices.WithLabelValues(p.Name).Add(float64(len(res)))
	log.Debugf("[%s] updated %d services", p.Name, len(res))

	return
}

// UpdateDeltaTableFullScan updates hosts and services tables by fetching some key indicator fields like last_check
// downtimes or acknowledged status. If an update is required, the last_check timestamp is used as filter for a
// delta update.
// The full scan just returns false without any update if the last update was less then MinFullScanInterval seconds
// ago.
// It returns true if an update was done and any error encountered.
func (p *Peer) UpdateDeltaTableFullScan(table *Table, filterStr string) (updated bool, err error) {
	updated = false
	p.PeerLock.RLock()
	var lastUpdate int64
	if table.Name == "services" {
		lastUpdate = p.Status["LastFullServiceUpdate"].(time.Time).Unix()
	} else if table.Name == "hosts" {
		lastUpdate = p.Status["LastFullHostUpdate"].(time.Time).Unix()
	} else {
		log.Panicf("not implemented for: " + table.Name)
	}
	p.PeerLock.RUnlock()

	// do not do a full scan more often than every 30 seconds
	if lastUpdate > time.Now().Unix()-MinFullScanInterval {
		return
	}

	req := &Request{
		Table:           table.Name,
		Columns:         []string{"last_check", "scheduled_downtime_depth", "acknowledged"},
		ResponseFixed16: true,
		OutputFormat:    "json",
	}
	res, err := p.Query(req)
	if err != nil {
		return
	}
	p.DataLock.RLock()
	index1 := table.GetColumn("last_check").Index
	index2 := table.GetColumn("scheduled_downtime_depth").Index
	index3 := table.GetColumn("acknowledged").Index
	data := p.Tables[table.Name].Data
	missing := make(map[float64]bool)
	for i, row := range res {
		if row[0].(float64) != data[i][index1].(float64) {
			missing[row[0].(float64)] = true
		} else if row[1].(float64) != data[i][index2].(float64) {
			missing[row[0].(float64)] = true
		} else if row[2].(float64) != data[i][index3].(float64) {
			missing[row[0].(float64)] = true
		}
	}
	p.DataLock.RUnlock()
	if len(missing) > 0 {
		filter := []string{filterStr}
		for lastCheck := range missing {
			filter = append(filter, fmt.Sprintf("Filter: last_check = %d\n", int(lastCheck)))
		}
		filter = append(filter, fmt.Sprintf("Or: %d\n", len(filter)))
		if table.Name == "services" {
			p.UpdateDeltaTableServices(strings.Join(filter, ""))
		} else if table.Name == "hosts" {
			p.UpdateDeltaTableHosts(strings.Join(filter, ""))
		}
	}

	p.PeerLock.Lock()
	if table.Name == "services" {
		p.Status["LastFullServiceUpdate"] = time.Now()
	} else if table.Name == "hosts" {
		p.Status["LastFullHostUpdate"] = time.Now()
	}
	p.PeerLock.Unlock()
	updated = true
	return
}

// UpdateDeltaCommentsOrDowntimes update the comments or downtimes table. It fetches the number and highest id of
// the remote comments/downtimes. If an update is required, it then fetches all ids to check which are new and
// which have to be removed.
// It returns any error encountered.
func (p *Peer) UpdateDeltaCommentsOrDowntimes(name string) (err error) {
	// add new comments / downtimes
	table := Objects.Tables[name]

	// get number of entrys and max id
	req := &Request{
		Table:           table.Name,
		ResponseFixed16: true,
		OutputFormat:    "json",
		FilterStr:       "Stats: id !=\nStats: max id\n",
	}
	res, err := p.Query(req)
	if err != nil {
		return
	}
	var last []interface{}
	p.DataLock.RLock()
	entries := len(p.Tables[table.Name].Data)
	if entries > 0 {
		last = p.Tables[table.Name].Data[entries-1]
	}
	p.DataLock.RUnlock()
	fieldIndex := table.ColumnsIndex["id"]

	if float64(entries) == res[0][0].(float64) && (entries == 0 || last[fieldIndex].(float64) == res[0][1].(float64)) {
		log.Debugf("[%s] %s did not change", p.Name, name)
		return
	}

	// fetch all ids to see which ones are missing or to be removed
	req = &Request{
		Table:           table.Name,
		Columns:         []string{"id"},
		ResponseFixed16: true,
		OutputFormat:    "json",
	}
	res, err = p.Query(req)
	if err != nil {
		return
	}
	p.DataLock.Lock()
	idIndex := p.Tables[table.Name].Index
	missingIds := []string{}
	resIndex := make(map[string]bool)
	for _, resRow := range res {
		id := fmt.Sprintf("%v", resRow[0])
		_, ok := idIndex[id]
		if !ok {
			log.Debugf("adding %s with id %s", name, id)
			missingIds = append(missingIds, id)
		}
		resIndex[id] = true
	}

	// remove old comments / downtimes
	data := p.Tables[table.Name]
	for id := range idIndex {
		_, ok := resIndex[id]
		if !ok {
			log.Debugf("removing %s with id %s", name, id)
			tmp := idIndex[id]
			data.RemoveItem(tmp)
			delete(idIndex, id)
		}
	}
	p.Tables[table.Name] = data
	p.DataLock.Unlock()

	if len(missingIds) > 0 {
		keys := table.GetInitialKeys(p.Flags)
		req := &Request{
			Table:           table.Name,
			Columns:         keys,
			ResponseFixed16: true,
			OutputFormat:    "json",
			FilterStr:       fmt.Sprintf("Filter: id = %s\nOr: %d\n", strings.Join(missingIds, "\nFilter: id = "), len(missingIds)),
		}
		res, err = p.Query(req)
		if err != nil {
			return
		}
		p.DataLock.Lock()
		data := p.Tables[table.Name]
		for _, resRow := range res {
			id := fmt.Sprintf("%v", resRow[fieldIndex])
			idIndex[id] = resRow
			data.AddItem(&resRow)
		}
		p.Tables[table.Name] = data
		p.DataLock.Unlock()
	}

	log.Debugf("[%s] updated %s", p.Name, name)
	return
}

// query sends the request to a remote livestatus.
// It returns the unmarshaled result and any error encountered.
func (p *Peer) query(req *Request) (result [][]interface{}, err error) {
	conn, connType, err := p.GetConnection()
	if err != nil {
		return nil, err
	}
	if conn != nil {
		defer conn.Close()
	}

	query := fmt.Sprintf(req.String())
	log.Tracef("[%s] query: %s", p.Name, query)

	p.PeerLock.Lock()
	p.Status["Querys"] = p.Status["Querys"].(int) + 1
	p.Status["BytesSend"] = p.Status["BytesSend"].(int) + len(query)
	promPeerBytesSend.WithLabelValues(p.Name).Set(float64(p.Status["BytesSend"].(int)))
	peerAddr := p.Status["PeerAddr"].(string)
	p.PeerLock.Unlock()

	var buf bytes.Buffer
	if connType == "http" {
		res, hErr := p.HTTPQuery(peerAddr, query)
		if hErr != nil {
			return nil, hErr
		}
		// commands do not send anything back
		if req.Command != "" {
			return nil, err
		}
		buf = *(bytes.NewBuffer(res))
	} else {
		conn.SetDeadline(time.Now().Add(time.Duration(GlobalConfig.NetTimeout) * time.Second))
		fmt.Fprintf(conn, "%s", query)
		switch c := conn.(type) {
		case *net.TCPConn:
			c.CloseWrite()
			break
		case *net.UnixConn:
			c.CloseWrite()
			break
		}
		// commands do not send anything back
		if req.Command != "" {
			return nil, err
		}
		io.Copy(&buf, conn)
	}

	log.Tracef("[%s] result: %s", p.Name, string(buf.Bytes()))

	resBytes := buf.Bytes()
	if req.ResponseFixed16 {
		err = p.CheckResponseHeader(&resBytes)
		if err != nil {
			return nil, &PeerError{msg: err.Error(), kind: ResponseError}
		}
		resBytes = resBytes[16:]
	}

	p.PeerLock.Lock()
	p.Status["BytesReceived"] = p.Status["BytesReceived"].(int) + len(resBytes)
	log.Debugf("[%s] got %s answer: size: %d kB", p.Name, req.Table, len(resBytes)/1024)
	promPeerBytesReceived.WithLabelValues(p.Name).Set(float64(p.Status["BytesReceived"].(int)))
	p.PeerLock.Unlock()

	if req.OutputFormat == "wrapped_json" {
		if len(resBytes) == 0 || string(resBytes[0]) != "{" {
			err = errors.New(strings.TrimSpace(string(resBytes)))
			return nil, &PeerError{msg: err.Error(), kind: ResponseError}
		}
		wrappedResult := make(map[string]json.RawMessage)
		err = json.Unmarshal(resBytes, &wrappedResult)
		if err != nil {
			return nil, &PeerError{msg: err.Error(), kind: ResponseError}
		}
		err = json.Unmarshal(wrappedResult["data"], &result)
	} else {
		if len(resBytes) == 0 || string(resBytes[0]) != "[" {
			err = errors.New(strings.TrimSpace(string(resBytes)))
			return nil, &PeerError{msg: err.Error(), kind: ResponseError}
		}
		err = json.Unmarshal(resBytes, &result)
	}

	if err != nil {
		log.Errorf("[%s] json string: %s", p.Name, string(buf.Bytes()))
		log.Errorf("[%s] json error: %s", p.Name, err.Error())
		return nil, &PeerError{msg: err.Error(), kind: ResponseError}
	}

	return
}

// Query sends a livestatus request from a request object.
// It calls query and logs all errors except connection errors which are logged in GetConnection.
// It returns the livestatus result and any error encountered.
func (p *Peer) Query(req *Request) (result [][]interface{}, err error) {
	result, err = p.query(req)
	if err != nil {
		p.setNextAddrFromErr(err)
	}
	return
}

// QueryString sends a livestatus request from a given string.
// It returns the livestatus result and any error encountered.
func (p *Peer) QueryString(str string) ([][]interface{}, error) {
	req, _, err := ParseRequestFromBuffer(bufio.NewReader(bytes.NewBufferString(str)))
	if err != nil {
		return nil, err
	}
	if req == nil {
		err = errors.New("bad request: empty request")
		return nil, err
	}
	return p.Query(req)
}

// CheckResponseHeader verifies the return code and content length of livestatus answer.
// It returns an error if something is wrong with the header.
func (p *Peer) CheckResponseHeader(resBytes *[]byte) (err error) {
	resSize := len(*resBytes)
	if resSize < 16 {
		err = fmt.Errorf("uncomplete response header: " + string(*resBytes))
		return
	}
	header := (*resBytes)[0:15]
	resSize = resSize - 16

	matched := reResponseHeader.FindStringSubmatch(string(header))
	if len(matched) != 3 {
		err = fmt.Errorf("[%s] uncomplete response header: %s", p.Name, string(header))
		return
	}
	resCode, _ := strconv.Atoi(matched[1])
	expSize, _ := strconv.Atoi(matched[2])

	if resCode != 200 {
		err = fmt.Errorf("[%s] bad response: %s", p.Name, string(*resBytes))
		return
	}
	if expSize != resSize {
		err = fmt.Errorf("[%s] bad response size, expected %d, got %d", p.Name, expSize, resSize)
		return
	}
	return
}

// GetConnection returns the next net.Conn object which answers to a connect.
// In case of a http connection, it just trys a tcp connect, but does not
// return anything.
// It returns the connection object and any error encountered.
func (p *Peer) GetConnection() (conn net.Conn, connType string, err error) {
	numSources := len(p.Source)

	for x := 0; x < numSources; x++ {
		p.PeerLock.RLock()
		peerAddr := p.Status["PeerAddr"].(string)
		p.PeerLock.RUnlock()
		connType = "unix"
		if strings.HasPrefix(peerAddr, "http") {
			connType = "http"
		} else if strings.Contains(peerAddr, ":") {
			connType = "tcp"
		}
		switch connType {
		case "tcp":
			fallthrough
		case "unix":
			conn, err = net.DialTimeout(connType, peerAddr, time.Duration(GlobalConfig.NetTimeout)*time.Second)
			break
		case "http":
			// test at least basic tcp connect
			uri, uErr := url.Parse(peerAddr)
			if uErr != nil {
				err = uErr
			}
			host := uri.Host
			if !strings.Contains(host, ":") {
				switch uri.Scheme {
				case "http":
					host = host + ":80"
					break
				case "https":
					host = host + ":443"
					break
				default:
					err = &PeerError{msg: fmt.Sprintf("unknown scheme: %s", uri.Scheme), kind: ConnectionError}
					break
				}
			}
			conn, err = net.DialTimeout("tcp", host, time.Duration(GlobalConfig.NetTimeout)*time.Second)
			if conn != nil {
				conn.Close()
			}
			conn = nil
			break
		}
		// connection succesfull
		if err == nil {
			promPeerConnections.WithLabelValues(p.Name).Inc()
			if x > 0 {
				log.Infof("[%s] active source changed to %s", p.Name, peerAddr)
				p.Flags = NoFlags
			}
			return
		}

		// connection error
		p.setNextAddrFromErr(err)
	}

	return nil, "", &PeerError{msg: err.Error(), kind: ConnectionError}
}

func (p *Peer) setNextAddrFromErr(err error) {
	promPeerFailedConnections.WithLabelValues(p.Name).Inc()
	p.PeerLock.Lock()
	peerAddr := p.Status["PeerAddr"].(string)
	log.Debugf("[%s] connection error %s: %s", peerAddr, p.Name, err)
	defer p.PeerLock.Unlock()
	p.Status["LastError"] = err.Error()
	p.ErrorCount++

	numSources := len(p.Source)

	// try next node if there are multiple
	curNum := p.Status["CurPeerAddrNum"].(int)
	nextNum := curNum
	nextNum++
	if nextNum >= numSources {
		nextNum = 0
	}
	p.Status["CurPeerAddrNum"] = nextNum
	p.Status["PeerAddr"] = p.Source[nextNum]

	if p.Status["PeerStatus"].(PeerStatus) == PeerStatusUp || p.Status["PeerStatus"].(PeerStatus) == PeerStatusPending {
		p.Status["PeerStatus"] = PeerStatusWarning
	}
	now := time.Now().Unix()
	lastOnline := p.Status["LastOnline"].(time.Time).Unix()
	log.Debugf("[%s] last online: %s", p.Name, time.Unix(lastOnline, 0))
	if lastOnline < now-int64(GlobalConfig.StaleBackendTimeout) || (p.ErrorCount > numSources && lastOnline <= 0) {
		if p.Status["PeerStatus"].(PeerStatus) != PeerStatusDown {
			log.Warnf("[%s] site went offline: %s", p.Name, err.Error())
			// clear existing data from memory
			p.DataLock.Lock()
			for name := range p.Tables {
				table := p.Tables[name]
				table.Data = make([][]interface{}, 0)
				table.Refs = make(map[string][][]interface{}, 0)
				table.Index = make(map[string][]interface{}, 0)
			}
			p.DataLock.Unlock()
		}
		p.Status["PeerStatus"] = PeerStatusDown
	}

	if numSources > 1 {
		log.Debugf("[%s] trying next one: %s", p.Name, peerAddr)
	}
	return
}

// CreateObjectByType fetches all static and dynamic data from the remote site and creates the initial table.
// It returns any error encountered.
func (p *Peer) CreateObjectByType(table *Table) (_, err error) {
	// log table does not create objects
	if table.PassthroughOnly {
		return
	}

	keys := table.GetInitialKeys(p.Flags)
	refs := make(map[string][][]interface{})
	index := make(map[string][]interface{})

	// complete virtual table ends here
	if len(keys) == 0 || table.Virtual {
		p.DataLock.Lock()
		p.Tables[table.Name] = DataTable{Table: table, Data: make([][]interface{}, 1), Refs: refs, Index: index}
		p.DataLock.Unlock()
		return
	}

	// fetch remote objects
	req := &Request{
		Table:           table.Name,
		Columns:         keys,
		ResponseFixed16: true,
		OutputFormat:    "json",
	}
	res, err := p.Query(req)
	if err != nil {
		return
	}

	// expand references, create a hash entry for each reference type, ex.: hosts
	// and put an array containing the references (using the same index as the original row)
	for _, refNum := range table.RefColCacheIndexes {
		refCol := table.Columns[refNum]
		fieldName := refCol.Name
		refs[fieldName] = make([][]interface{}, len(res))
		RefByName := p.Tables[fieldName].Index
		for i, row := range res {
			refs[fieldName][i] = RefByName[row[refCol.RefIndex].(string)]
			if RefByName[row[refCol.RefIndex].(string)] == nil {
				panic("ref not found")
			}
		}
	}

	// create host lookup indexes
	if table.Name == "hosts" {
		indexField := table.ColumnsIndex["name"]
		for _, row := range res {
			index[row[indexField].(string)] = row
		}
		promHostCount.WithLabelValues(p.Name).Set(float64(len(res)))
	}
	// create service lookup indexes
	if table.Name == "services" {
		indexField1 := table.ColumnsIndex["host_name"]
		indexField2 := table.ColumnsIndex["description"]
		for _, row := range res {
			index[row[indexField1].(string)+";"+row[indexField2].(string)] = row
		}
		promServiceCount.WithLabelValues(p.Name).Set(float64(len(res)))
	}
	// create downtime / comment id lookup indexes
	if table.Name == "comments" || table.Name == "downtimes" {
		indexField := table.ColumnsIndex["id"]
		for _, row := range res {
			index[fmt.Sprintf("%v", row[indexField])] = row
		}
	}
	// is this a shinken backend?
	if table.Name == "status" && len(res) > 0 {
		row := res[0]
		matched := reShinkenVersion.FindStringSubmatch(row[table.GetColumn("livestatus_version").Index].(string))
		if len(matched) > 0 {
			p.PeerLock.Lock()
			p.Flags |= ShinkenOnly
			p.PeerLock.Unlock()
		}
	}
	p.DataLock.Lock()
	p.Tables[table.Name] = DataTable{Table: table, Data: res, Refs: refs, Index: index}
	p.DataLock.Unlock()
	p.PeerLock.Lock()
	p.Status["LastUpdate"] = time.Now()
	p.Status["LastFullUpdate"] = time.Now()
	p.PeerLock.Unlock()
	return
}

// UpdateObjectByType updates a given table by requesting all dynamic columns from the remote peer.
// Assuming we get the objects always in the same order, we can just iterate over the index and update the fields.
// It returns a boolean flag wheter the remote site has been restarted and any error encountered.
func (p *Peer) UpdateObjectByType(table Table) (restartRequired bool, err error) {
	if len(table.DynamicColCacheNames) == 0 {
		return
	}
	if table.Virtual {
		return
	}
	// no updates for passthrough tables, ex.: log
	if table.PassthroughOnly {
		return
	}
	keys, indexes := table.GetDynamicColumns(p.Flags)
	req := &Request{
		Table:           table.Name,
		Columns:         keys,
		ResponseFixed16: true,
		OutputFormat:    "json",
	}
	res, err := p.Query(req)
	if err != nil {
		return
	}
	data := p.Tables[table.Name].Data
	if table.Name == "timeperiods" {
		// check for changed timeperiods, because we have to update the linked hosts and services as well
		p.updateTimeperiodsData(&table, res, indexes)
	} else {
		p.DataLock.Lock()
		for i, row := range res {
			for j, k := range indexes {
				data[i][k] = row[j]
			}
		}
		p.DataLock.Unlock()
	}

	switch table.Name {
	case "hosts":
		promPeerUpdatedHosts.WithLabelValues(p.Name).Add(float64(len(res)))
		break
	case "services":
		promPeerUpdatedServices.WithLabelValues(p.Name).Add(float64(len(res)))
		break
	case "status":
		p.PeerLock.RLock()
		if p.Status["ProgramStart"] != data[0][table.ColumnsIndex["program_start"]] {
			log.Infof("[%s] site has been restarted, recreating objects", p.Name)
			restartRequired = true
		}
		p.PeerLock.RUnlock()
		return
	}
	return
}

func (p *Peer) updateTimeperiodsData(table *Table, res [][]interface{}, indexes []int) {
	changedTimeperiods := make(map[string]float64)
	nameIndex := table.ColumnsIndex["name"]
	p.DataLock.Lock()
	data := p.Tables[table.Name].Data
	for i, row := range res {
		for j, k := range indexes {
			if data[i][k] != row[j] {
				changedTimeperiods[data[i][nameIndex].(string)] = data[i][k].(float64)
			}
			data[i][k] = row[j]
		}
	}
	p.DataLock.Unlock()
	// Update hosts and services with those changed timeperiods
	for name, state := range changedTimeperiods {
		log.Debugf("[%s] timeperiod %s has changed to %v, need to update affected hosts/services", p.Name, state)
		p.UpdateDeltaTableHosts("Filter: check_period = " + name + "\nFilter: notification_period = " + name + "\nOr: 2\n")
		p.UpdateDeltaTableServices("Filter: check_period = " + name + "\nFilter: notification_period = " + name + "\nOr: 2\n")
	}
}

// GetRowValue returns the value for a given index in a data row and resolves
// any virtual or reference column.
// The result is returned as interface.
func (p *Peer) GetRowValue(index int, row *[]interface{}, rowNum int, table *Table, refs *map[string][][]interface{}, inputRowLen int) interface{} {
	if index >= inputRowLen {
		col := table.Columns[index]
		if col.Type == VirtCol {
			p.PeerLock.RLock()
			value, ok := p.Status[VirtKeyMap[col.Name].Key]
			p.PeerLock.RUnlock()
			if !ok {
				switch col.Name {
				case "last_state_change_order":
					// return last_state_change or program_start
					lastStateChange := numberToFloat((*row)[table.ColumnsIndex["last_state_change"]])
					if lastStateChange == 0 {
						value = p.Status["ProgramStart"]
					} else {
						value = lastStateChange
					}
					break
				case "host_last_state_change_order":
					// return last_state_change or program_start
					lastStateChange := numberToFloat(p.GetRowValue(table.GetColumn("host_last_state_change").Index, row, rowNum, table, refs, inputRowLen))
					if lastStateChange == 0 {
						value = p.Status["ProgramStart"]
					} else {
						value = lastStateChange
					}
					break
				case "state_order":
					// return 4 instead of 2, which makes critical come first
					// this way we can use this column to sort by state
					state := numberToFloat((*row)[table.ColumnsIndex["state"]])
					if state == 2 {
						value = 4
					} else {
						value = state
					}
					break
				default:
					log.Panicf("cannot handle virtual column: %s", col.Name)
					break
				}
			}
			switch VirtKeyMap[col.Name].Type {
			case IntCol:
				fallthrough
			case FloatCol:
				fallthrough
			case StringCol:
				return value
			case TimeCol:
				val := value.(time.Time).Unix()
				if val < 0 {
					val = 0
				}
				return val
			default:
				log.Panicf("not implemented")
			}
		}
		// this happens if we are requesting an optional column from the wrong backend
		// ex.: shinken specific columns from a non-shinken backend
		if col.RefIndex == 0 {
			if _, ok := (*refs)[table.Columns[col.RefIndex].Name]; !ok {
				// return empty placeholder matching the column type
				switch col.Type {
				case IntListCol:
					fallthrough
				case StringListCol:
					return (make([]interface{}, 0))
				default:
					return ("")
				}
			}
		}
		refObj := (*refs)[table.Columns[col.RefIndex].Name][rowNum]
		if refObj == nil {
			panic("should not happen, ref not found")
		}
		if len(refObj) > col.RefColIndex {
			return refObj[col.RefColIndex]
		}
		// this happens if we are requesting an optional column from the wrong backend
		// ex.: shinken specific columns from a non-shinken backend
		// -> return empty placeholder matching the column type
		switch col.Type {
		case IntListCol:
			fallthrough
		case StringListCol:
			return (make([]interface{}, 0))
		default:
			return ("")
		}
	}
	return (*row)[index]
}

// WaitCondition waits for a given condition.
// It returns true if the wait timed out or false if the condition matched succesfully.
func (p *Peer) WaitCondition(req *Request) bool {
	c := make(chan struct{})
	go func() {
		table := p.Tables[req.Table].Table
		refs := p.Tables[req.Table].Refs
		for {
			select {
			case <-c:
				// canceled
				return
			default:
			}
			// get object to watch
			var obj []interface{}
			if req.Table == "hosts" || req.Table == "services" {
				obj = p.Tables[req.Table].Index[req.WaitObject]
			} else {
				log.Errorf("unsupported wait table: %s", req.Table)
				close(c)
				return
			}
			if p.MatchFilter(table, &refs, len(obj), &req.WaitCondition[0], &obj, 0) {
				// trigger update for all, wait conditions are run against the last object
				// but multiple commands may have been sent
				if req.Table == "hosts" {
					go p.UpdateDeltaTableHosts("")
				} else if req.Table == "services" {
					go p.UpdateDeltaTableServices("")
				}
				close(c)
				return
			}
			time.Sleep(time.Millisecond * 200)
			if req.Table == "hosts" {
				p.UpdateDeltaTableHosts("Filter: name = " + req.WaitObject + "\n")
			} else if req.Table == "services" {
				tmp := strings.SplitN(req.WaitObject, ";", 2)
				if len(tmp) < 2 {
					log.Errorf("unsupported service wait object: %s", req.WaitObject)
					close(c)
					return
				}
				p.UpdateDeltaTableServices("Filter: host_name = " + tmp[0] + "\nFilter: description = " + tmp[1] + "\n")
			}
		}
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(time.Duration(req.WaitTimeout) * time.Millisecond):
		close(c)
		return true // timed out
	}
}

// HTTPQuery sends a query over http to a Thruk backend.
// It returns the livestatus answers and any encountered error.
func (p *Peer) HTTPQuery(peerAddr string, query string) (res []byte, err error) {
	options := make(map[string]interface{})
	if p.Config.RemoteName != "" {
		options["backends"] = []string{p.Config.RemoteName}
	}
	options["action"] = "raw"
	options["sub"] = "_raw_query"
	options["args"] = []string{query}
	optionStr, _ := json.Marshal(options)
	response, err := netClient.PostForm(peerAddr+"/thruk/cgi-bin/remote.cgi", url.Values{
		"data": {fmt.Sprintf("{\"credential\": \"%s\", \"options\": %s}", p.Config.Auth, optionStr)},
	})
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		err = &PeerError{msg: fmt.Sprintf("http request failed: %s", response.Status), kind: ResponseError}
		io.Copy(ioutil.Discard, response.Body)
		response.Body.Close()
		return
	}
	contents, hErr := ioutil.ReadAll(response.Body)
	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	if hErr != nil {
		err = hErr
		return
	}
	if len(contents) < 1 || contents[0] != '{' {
		if len(contents) > 50 {
			contents = contents[:50]
		}
		err = &PeerError{msg: fmt.Sprintf("site did not return a proper response: %s", contents), kind: ResponseError}
		return
	}
	var result HTTPResult
	err = json.Unmarshal(contents, &result)
	if err != nil {
		return
	}
	if result.Rc != 0 {
		err = &PeerError{msg: fmt.Sprintf("remote site returned rc: %d - %s", result.Rc, result.Output), kind: ResponseError}
		return
	}

	var output []interface{}
	err = json.Unmarshal(result.Output, &output)
	if err != nil {
		return
	}
	remoteError := ""
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
	if v, ok := output[2].(string); ok {
		res = []byte(v)
	} else {
		err = &PeerError{msg: fmt.Sprintf("unknown site error, got: %v", result), kind: ResponseError}
	}
	return
}

// SpinUpPeers starts an immediate parallel delta update for all supplied peer ids.
func SpinUpPeers(peers []string) {
	waitgroup := &sync.WaitGroup{}
	for _, id := range peers {
		p := DataStore[id]
		waitgroup.Add(1)
		go func(peer Peer, wg *sync.WaitGroup) {
			defer wg.Done()
			peer.PeerLock.Lock()
			peer.Status["Idling"] = false
			log.Infof("[%s] switched back to normal update interval", peer.Name)
			peer.PeerLock.Unlock()
			log.Debugf("[%s] spin up update", peer.Name)
			peer.UpdateObjectByType(Objects.Tables["timeperiods"])
			peer.UpdateDeltaTables()
			log.Debugf("[%s] spin up update done", peer.Name)
		}(p, waitgroup)
	}
	waitTimeout(waitgroup, 5)
	log.Debugf("spin up completed")
}

// BuildLocalResponseData returnss the result data for a given request
func (p *Peer) BuildLocalResponseData(res *Response, req *Request, numPerRow int, indexes *[]int) {
	log.Tracef("BuildLocalResponseData: %s", p.Name)
	p.DataLock.RLock()
	defer p.DataLock.RUnlock()
	table := p.Tables[req.Table].Table
	p.PeerLock.Lock()
	p.Status["LastQuery"] = time.Now()
	if table == nil || (p.Status["PeerStatus"].(PeerStatus) == PeerStatusDown && !table.Virtual) {
		res.Failed[p.ID] = fmt.Sprintf("%v", p.Status["LastError"])
		p.PeerLock.Unlock()
		return
	}
	p.PeerLock.Unlock()

	// if a WaitTrigger is supplied, wait max ms till the condition is true
	if req.WaitTrigger != "" {
		p.WaitCondition(req)
	}

	data := p.Tables[req.Table].Data
	refs := p.Tables[req.Table].Refs

	if table.Name == "tables" || table.Name == "columns" {
		data = Objects.GetTableColumnsData()
	}

	if len(data) == 0 {
		return
	}
	inputRowLen := len(data[0])
	statsLen := len(res.Request.Stats)

	// if there is no sort header or sort by name only,
	// we can drastically reduce the result set by applying the limit here already
	limit := 0
	if req.Limit > 0 && table.IsDefaultSortOrder(&req.Sort) {
		limit = req.Limit
		if req.Offset > 0 {
			limit += req.Offset
		}
	}

	found := 0
	for j, row := range data {
		// does our filter match?
		filterMatched := true
		for _, f := range res.Request.Filter {
			if !p.MatchFilter(table, &refs, inputRowLen, &f, &row, j) {
				filterMatched = false
				break
			}
		}
		if !filterMatched {
			continue
		}

		// count stats
		if statsLen > 0 {
			for i := range res.Request.Stats {
				s := &(res.Request.Stats[i])
				// avg/sum/min/max are passed through, they dont have filter
				// counter must match their filter
				if s.StatsType != Counter || p.MatchFilter(table, &refs, inputRowLen, s, &row, j) {
					val := p.GetRowValue(s.Column.Index, &row, j, table, &refs, inputRowLen)
					switch s.StatsType {
					case Counter:
						s.Stats++
						break
					case Average:
						fallthrough
					case Sum:
						s.Stats += numberToFloat(val)
						break
					case Min:
						value := numberToFloat(val)
						if s.Stats > value || s.Stats == -1 {
							s.Stats = value
						}
						break
					case Max:
						value := numberToFloat(val)
						if s.Stats < value {
							s.Stats = value
						}
						break
					}
					s.StatsCount++
				}
			}
			continue
		}

		// build result row
		resRow := make([]interface{}, numPerRow)
		for k, i := range *(indexes) {
			if i < 0 {
				// virtual columns
				resRow[k] = p.GetRowValue(res.Columns[k].RefIndex, &row, j, table, &refs, inputRowLen)
			} else {
				// check if this is a reference column
				// reference columns come after the non-ref columns
				if i >= inputRowLen {
					resRow[k] = p.GetRowValue(table.Columns[i].Index, &row, j, table, &refs, inputRowLen)
				} else {
					resRow[k] = row[i]
				}
			}
			// fill null values with something useful
			if resRow[k] == nil {
				switch table.Columns[i].Type {
				case IntListCol:
					fallthrough
				case StringListCol:
					resRow[k] = make([]interface{}, 0)
					break
				default:
					resRow[k] = ""
					break
				}
			}
		}
		found++
		// check if we have enough result rows already
		if limit == 0 || found <= limit {
			res.Result = append(res.Result, resRow)
		}
	}
	res.ResultTotal += found

	return
}
