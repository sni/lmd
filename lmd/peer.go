package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ReResponseHeader = regexp.MustCompile(`^(\d+)\s+(\d+)$`)

type DataTable struct {
	Table *Table
	Data  [][]interface{}
	Refs  map[string][][]interface{}
	Index map[string][]interface{}
}

type Peer struct {
	Name            string
	Id              string
	Source          []string
	Lock            *sync.RWMutex
	Tables          map[string]DataTable
	Status          map[string]interface{}
	ErrorCount      int
	waitGroup       *sync.WaitGroup
	shutdownChannel chan bool
}

type PeerStatus int

const (
	PeerStatusUp PeerStatus = iota
	PeerStatusWarning
	PeerStatusDown
	PeerStatusPending
)

type PeerErrorType int

const (
	ConnectionError PeerErrorType = iota
	RequestError
	ResponseError
)

type PeerError struct {
	msg  string
	kind PeerErrorType
}

func (e *PeerError) Error() string       { return e.msg }
func (e *PeerError) Type() PeerErrorType { return e.kind }

func (d *DataTable) AddItem(row *[]interface{}) {
	d.Data = append(d.Data, *row)
	return
}

func (d *DataTable) RemoveItem(row []interface{}) {
	for i, r := range d.Data {
		if fmt.Sprintf("%p", r) == fmt.Sprintf("%p", row) {
			d.Data = append(d.Data[:i], d.Data[i+1:]...)
			return
		}
	}
	panic("element not found")
}

// send query to remote livestatus and returns unmarshaled result
func NewPeer(config *Connection, waitGroup *sync.WaitGroup, shutdownChannel chan bool) *Peer {
	p := Peer{
		Name:            config.Name,
		Id:              config.Id,
		Source:          config.Source,
		Tables:          make(map[string]DataTable),
		Status:          make(map[string]interface{}),
		ErrorCount:      0,
		waitGroup:       waitGroup,
		shutdownChannel: shutdownChannel,
		Lock:            new(sync.RWMutex),
	}
	p.Status["PeerKey"] = p.Id
	p.Status["PeerName"] = p.Name
	p.Status["CurPeerAddrNum"] = 0
	p.Status["PeerAddr"] = p.Source[p.Status["CurPeerAddrNum"].(int)]
	p.Status["PeerStatus"] = PeerStatusPending
	p.Status["LastUpdate"] = time.Now()
	p.Status["LastQuery"] = time.Now()
	p.Status["LastError"] = "connecting..."
	p.Status["LastOnline"] = time.Time{}
	p.Status["ProgramStart"] = 0
	p.Status["BytesSend"] = 0
	p.Status["BytesReceived"] = 0
	p.Status["Querys"] = 0
	p.Status["ReponseTime"] = 0
	return &p
}

// create initial objects
func (p *Peer) Start() (_, err error) {
	go func() {
		defer p.waitGroup.Done()
		p.waitGroup.Add(1)
		log.Infof("[%s] starting connection", p.Name)
		// TODO: check if locking each peer to a os thread changes anything
		//runtime.LockOSThread()
		p.UpdateLoop()
	}()

	return
}

func (p *Peer) UpdateLoop() {
	for {
		ok := p.InitAllTables()
		lastFullUpdateMinute, _ := strconv.Atoi(time.Now().Format("4"))

		// TODO: implement idle_interval update (maybe one minute) and the normal update interval
		c := time.Tick(500 * time.Millisecond)
		for {
			select {
			case <-p.shutdownChannel:
				log.Infof("stopping peer %s", p.Name)
				return
			case <-c:
				p.Lock.RLock()
				lastUpdate := p.Status["LastUpdate"].(time.Time)
				p.Lock.RUnlock()
				currentMinute, _ := strconv.Atoi(time.Now().Format("4"))
				if time.Now().Add(-1 * time.Duration(GlobalConfig.Updateinterval) * time.Second).After(lastUpdate) {
					if !ok {
						ok = p.InitAllTables()
						lastFullUpdateMinute = currentMinute
					} else {
						if lastFullUpdateMinute != currentMinute {
							ok = p.UpdateAllTables()
						} else {
							ok = p.UpdateDeltaTables()
						}
						lastFullUpdateMinute = currentMinute
					}
				}
				break
			}
		}
	}
}

func (p *Peer) InitAllTables() bool {
	var err error
	p.Lock.Lock()
	p.Status["LastUpdate"] = time.Now()
	p.Lock.Unlock()
	t1 := time.Now()
	for _, n := range Objects.Order {
		t := Objects.Tables[n]
		_, err = p.CreateObjectByType(&t)
		if err != nil {
			return false
		}
	}
	p.Lock.Lock()
	// this may happen if we query another lmd daemon which has no backends ready yet
	if len(p.Tables["status"].Data) == 0 {
		p.Status["PeerStatus"] = PeerStatusWarning
		p.Status["LastError"] = "peered partner not ready yet"
		p.Lock.Unlock()
		return false
	}
	p.Status["ProgramStart"] = p.Tables["status"].Data[0][p.Tables["status"].Table.ColumnsIndex["program_start"]]
	duration := time.Since(t1)
	p.Status["ReponseTime"] = duration.Seconds()
	p.Lock.Unlock()
	log.Infof("[%s] objects created in: %s", p.Name, duration.String())

	if p.Status["PeerStatus"].(PeerStatus) != PeerStatusUp {
		// Reset errors
		p.Lock.Lock()
		if p.Status["PeerStatus"].(PeerStatus) == PeerStatusDown {
			log.Infof("[%s] site is back online", p.Name)
		}
		p.Status["LastError"] = ""
		p.Status["LastOnline"] = time.Now()
		p.ErrorCount = 0
		p.Status["PeerStatus"] = PeerStatusUp
		p.Lock.Unlock()
	}
	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())

	return true
}

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
	p.Lock.Lock()
	p.Status["ReponseTime"] = duration.Seconds()
	p.Status["LastUpdate"] = time.Now()
	p.Status["PeerStatus"] = PeerStatusUp
	p.Status["LastError"] = ""
	p.Lock.Unlock()
	log.Infof("[%s] update complete in: %s", p.Name, duration.String())
	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())
	return true
}

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
	p.Lock.Lock()
	p.Status["LastUpdate"] = time.Now()
	p.Status["ReponseTime"] = duration.Seconds()
	p.Lock.Unlock()
	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())
	return true
}

func (p *Peer) UpdateDeltaTableHosts(filterStr string) (err error) {
	// update changed hosts
	table := Objects.Tables["hosts"]
	keys := append(table.DynamicColCacheNames, "name")
	if filterStr == "" {
		filterStr = fmt.Sprintf("Filter: last_check >= %v\nFilter: is_executing = 1\nOr: 2\n", p.Status["LastUpdate"].(time.Time).Unix())
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
	p.Lock.Lock()
	nameindex := p.Tables[table.Name].Index
	fieldIndex := len(keys) - 1
	for _, resRow := range res {
		dataRow := nameindex[resRow[fieldIndex].(string)]
		for j, k := range table.DynamicColCacheIndexes {
			dataRow[k] = resRow[j]
		}
	}
	p.Lock.Unlock()
	promPeerUpdatedHosts.WithLabelValues(p.Name).Add(float64(len(res)))
	log.Debugf("[%s] updated %d hosts", p.Name, len(res))
	return
}

func (p *Peer) UpdateDeltaTableServices(filterStr string) (err error) {
	// update changed services
	table := Objects.Tables["services"]
	keys := append(table.DynamicColCacheNames, []string{"host_name", "description"}...)
	if filterStr == "" {
		filterStr = fmt.Sprintf("Filter: last_check >= %v\nFilter: is_executing = 1\nOr: 2\n", p.Status["LastUpdate"].(time.Time).Unix())
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
	p.Lock.Lock()
	nameindex := p.Tables[table.Name].Index
	fieldIndex1 := len(keys) - 2
	fieldIndex2 := len(keys) - 1
	for _, resRow := range res {
		dataRow := nameindex[resRow[fieldIndex1].(string)+";"+resRow[fieldIndex2].(string)]
		for j, k := range table.DynamicColCacheIndexes {
			dataRow[k] = resRow[j]
		}
	}
	p.Lock.Unlock()
	promPeerUpdatedServices.WithLabelValues(p.Name).Add(float64(len(res)))
	log.Debugf("[%s] updated %d services", p.Name, len(res))
	return
}

func (p *Peer) UpdateDeltaCommentsOrDowntimes(name string) (err error) {
	// add new comments / downtimes
	table := Objects.Tables[name]
	req := &Request{
		Table:           table.Name,
		Columns:         []string{"id"},
		ResponseFixed16: true,
		OutputFormat:    "json",
	}
	res, err := p.Query(req)
	if err != nil {
		return
	}
	p.Lock.Lock()
	idIndex := p.Tables[table.Name].Index
	fieldIndex := 0
	missingIds := []string{}
	resIndex := make(map[string]bool)
	for _, resRow := range res {
		id := fmt.Sprintf("%v", resRow[fieldIndex])
		_, ok := idIndex[id]
		if !ok {
			log.Debugf("adding %s with id %s", name, id)
			missingIds = append(missingIds, id)
		}
		resIndex[id] = true
	}

	// remove old comments / downtimes
	data := p.Tables[table.Name]
	for id, _ := range idIndex {
		_, ok := resIndex[id]
		if !ok {
			log.Debugf("removing %s with id %s", name, id)
			tmp := idIndex[id]
			data.RemoveItem(tmp)
			delete(idIndex, id)
		}
	}
	p.Tables[table.Name] = data
	p.Lock.Unlock()

	if len(missingIds) > 0 {
		keys := table.GetInitialKeys()
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
		fieldIndex = table.ColumnsIndex["id"]
		p.Lock.Lock()
		data := p.Tables[table.Name]
		for _, resRow := range res {
			id := fmt.Sprintf("%v", resRow[fieldIndex])
			idIndex[id] = resRow
			data.AddItem(&resRow)
		}
		p.Tables[table.Name] = data
		p.Lock.Unlock()
	}

	log.Debugf("[%s] updated %s", p.Name, name)
	return
}

// send query to remote livestatus and returns unmarshaled result
func (p *Peer) query(req *Request) (result [][]interface{}, err error) {
	conn, err := p.GetConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	query := fmt.Sprintf(req.String())
	log.Tracef("[%s] query: %s", p.Name, query)

	p.Lock.Lock()
	p.Status["Querys"] = p.Status["Querys"].(int) + 1
	p.Status["BytesSend"] = p.Status["BytesSend"].(int) + len(query)
	promPeerBytesSend.WithLabelValues(p.Name).Set(float64(p.Status["BytesSend"].(int)))
	p.Lock.Unlock()

	conn.SetDeadline(time.Now().Add(time.Duration(GlobalConfig.NetTimeout) * time.Second))
	fmt.Fprintf(conn, "%s", query)

	// commands do not send anything back
	if req.Command != "" {
		return nil, err
	}

	var buf bytes.Buffer
	io.Copy(&buf, conn)

	log.Tracef("[%s] result: %s", p.Name, string(buf.Bytes()))

	resBytes := buf.Bytes()
	if req.ResponseFixed16 {
		err = p.CheckResponseHeader(&resBytes)
		if err != nil {
			return nil, &PeerError{msg: err.Error(), kind: ResponseError}
		}
		resBytes = resBytes[16:]
	}

	p.Lock.Lock()
	p.Status["BytesReceived"] = p.Status["BytesReceived"].(int) + len(resBytes)
	promPeerBytesReceived.WithLabelValues(p.Name).Set(float64(p.Status["BytesReceived"].(int)))
	p.Lock.Unlock()

	if req.OutputFormat == "wrapped_json" {
		if string(resBytes[0]) != "{" {
			err = errors.New(strings.TrimSpace(string(resBytes)))
			return nil, &PeerError{msg: err.Error(), kind: ResponseError}
		}
		wrapped_result := make(map[string]json.RawMessage)
		err = json.Unmarshal(resBytes, &wrapped_result)
		if err != nil {
			return nil, &PeerError{msg: err.Error(), kind: ResponseError}
		}
		err = json.Unmarshal(wrapped_result["data"], &result)
	} else {
		if string(resBytes[0]) != "[" {
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

// call query and log all errors except connection errors which are logged in GetConnection
func (p *Peer) Query(req *Request) (result [][]interface{}, err error) {
	result, err = p.query(req)
	if err != nil {
		if cerr, ok := err.(*PeerError); ok {
			if cerr.Type() != ConnectionError {
				log.Errorf("[%s] site error: %s", p.Name, cerr.Error())
			}
		} else {
			log.Errorf("[%s] site error: %s", p.Name, cerr.Error())
		}
		p.setNextAddrFromErr(err)
	}
	return
}

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

func (p *Peer) CheckResponseHeader(resBytes *[]byte) (err error) {
	resSize := len(*resBytes)
	if resSize < 16 {
		err = errors.New(fmt.Sprintf("uncomplete response header: " + string(*resBytes)))
		return
	}
	header := (*resBytes)[0:15]
	resSize = resSize - 16

	matched := ReResponseHeader.FindStringSubmatch(string(header))
	if len(matched) != 3 {
		err = errors.New(fmt.Sprintf("[%s] uncomplete response header: %s", p.Name, string(header)))
		return
	}
	resCode, _ := strconv.Atoi(matched[1])
	expSize, _ := strconv.Atoi(matched[2])

	if resCode != 200 {
		err = errors.New(fmt.Sprintf("[%s] bad response: %s", p.Name, string(*resBytes)))
		return
	}
	if expSize != resSize {
		err = errors.New(fmt.Sprintf("[%s] bad response size, expected %d, got %d", p.Name, expSize, resSize))
		return
	}
	return
}

func (p *Peer) GetConnection() (conn net.Conn, err error) {
	numSources := len(p.Source)

	for x := 0; x < numSources; x++ {
		p.Lock.RLock()
		peerAddr := p.Status["PeerAddr"].(string)
		p.Lock.RUnlock()
		connType := "unix"
		if strings.Contains(peerAddr, ":") {
			connType = "tcp"
		}
		conn, err = net.DialTimeout(connType, peerAddr, time.Duration(GlobalConfig.NetTimeout)*time.Second)
		// connection succesful
		if err == nil {
			promPeerConnections.WithLabelValues(p.Name).Inc()
			if x > 0 {
				log.Infof("[%s] active source changed to %s", p.Name, peerAddr)
			}
			return
		}

		promPeerFailedConnections.WithLabelValues(p.Name).Inc()
		// connection error
		log.Debugf("[%s] connection error %s: %s", peerAddr, p.Name, err)
		p.setNextAddrFromErr(err)
	}

	p.Lock.Lock()
	if p.Status["PeerStatus"].(PeerStatus) != PeerStatusDown {
		log.Warnf("[%s] site went offline: %s", p.Name, err.Error())
	}
	p.Status["PeerStatus"] = PeerStatusDown
	p.Lock.Unlock()
	return nil, &PeerError{msg: err.Error(), kind: ConnectionError}
}

func (p *Peer) setNextAddrFromErr(err error) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	p.Status["LastError"] = err.Error()
	p.ErrorCount++

	numSources := len(p.Source)
	if numSources == 1 {
		return
	}

	// try next node if there are multiple
	curNum := p.Status["CurPeerAddrNum"].(int)
	nextNum := curNum
	nextNum++
	if nextNum >= numSources {
		nextNum = 0
	}
	p.Status["CurPeerAddrNum"] = nextNum
	p.Status["PeerAddr"] = p.Source[nextNum]
	peerAddr := p.Status["PeerAddr"].(string)

	if p.Status["PeerStatus"].(PeerStatus) == PeerStatusUp {
		p.Status["PeerStatus"] = PeerStatusWarning
	}
	if p.ErrorCount > numSources*2 {
		p.Status["PeerStatus"] = PeerStatusDown
	}

	log.Debugf("[%s] trying next one: %s", p.Name, peerAddr)
	return
}

// create initial objects
func (p *Peer) CreateObjectByType(table *Table) (_, err error) {
	// log table does not create objects
	if table.PassthroughOnly {
		return
	}
	keys := table.GetInitialKeys()
	refs := make(map[string][][]interface{})
	index := make(map[string][]interface{})

	// complete virtual table ends here
	if len(keys) == 0 {
		p.Lock.Lock()
		p.Tables[table.Name] = DataTable{Table: table, Data: make([][]interface{}, 1), Refs: refs, Index: index}
		p.Status["LastUpdate"] = time.Now()
		p.Lock.Unlock()
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
	// create comment id lookup indexes
	if table.Name == "comments" {
		indexField := table.ColumnsIndex["id"]
		for _, row := range res {
			index[fmt.Sprintf("%v", row[indexField])] = row
		}
	}
	// create downtime id lookup indexes
	if table.Name == "downtimes" {
		indexField := table.ColumnsIndex["id"]
		for _, row := range res {
			index[fmt.Sprintf("%v", row[indexField])] = row
		}
	}
	p.Lock.Lock()
	p.Tables[table.Name] = DataTable{Table: table, Data: res, Refs: refs, Index: index}
	p.Status["LastUpdate"] = time.Now()
	p.Lock.Unlock()
	return
}

// update objects
// assuming we get the objects always in the same order, we can just iterate over the index and update the fields
func (p *Peer) UpdateObjectByType(table Table) (restartRequired bool, err error) {
	if len(table.DynamicColCacheNames) == 0 {
		return
	}
	// no updates for passthrough tables, ex.: log
	if table.PassthroughOnly {
		return
	}
	req := &Request{
		Table:           table.Name,
		Columns:         table.DynamicColCacheNames,
		ResponseFixed16: true,
		OutputFormat:    "json",
	}
	res, err := p.Query(req)
	if err != nil {
		return
	}
	p.Lock.Lock()
	data := p.Tables[table.Name].Data
	for i, row := range res {
		for j, k := range table.DynamicColCacheIndexes {
			data[i][k] = row[j]
		}
	}
	p.Lock.Unlock()

	switch table.Name {
	case "hosts":
		promPeerUpdatedHosts.WithLabelValues(p.Name).Add(float64(len(res)))
		break
	case "services":
		promPeerUpdatedServices.WithLabelValues(p.Name).Add(float64(len(res)))
		break
	case "stats":
		if p.Status["ProgramStart"] != data[0][table.ColumnsIndex["program_start"]] {
			log.Infof("[%s] site has been restarted, recreating objects", p.Name)
			restartRequired = true
			return
		}
		break
	}
	return
}

func (peer *Peer) getRowValue(index int, row *[]interface{}, rowNum int, table *Table, refs *map[string][][]interface{}, inputRowLen int) interface{} {
	if index >= inputRowLen {
		col := table.Columns[index]
		if col.Type == VirtCol {
			value, ok := peer.Status[VirtKeyMap[col.Name].Key]
			if !ok {
				switch col.Name {
				case "last_state_change_order":
					// return last_state_change or program_start
					last_state_change := NumberToFloat((*row)[table.ColumnsIndex["last_state_change"]])
					if last_state_change == 0 {
						value = peer.Status["ProgramStart"]
					} else {
						value = last_state_change
					}
					break
				case "state_order":
					// return 4 instead of 2, which makes critical come first
					// this way we can use this column to sort by state
					state := NumberToFloat((*row)[table.ColumnsIndex["state"]])
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
				return value.(time.Time).Unix()
			default:
				log.Panicf("not implemented")
			}
		}
		refObj := (*refs)[table.Columns[col.RefIndex].Name][rowNum]
		if refObj == nil {
			panic("should not happen, ref not found")
		}
		return refObj[table.Columns[index].RefColIndex]
	}
	return (*row)[index]
}

func (peer *Peer) waitCondition(req *Request) bool {
	c := make(chan struct{})
	go func() {
		table := peer.Tables[req.Table].Table
		refs := peer.Tables[req.Table].Refs
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
				obj = peer.Tables[req.Table].Index[req.WaitObject]
			} else {
				log.Errorf("unsupported wait table: %s", req.Table)
				close(c)
				return
			}
			if peer.matchFilter(table, &refs, len(obj), req.WaitCondition[0], &obj, 0) {
				// trigger update for all, wait conditions are run against the last object
				// but multiple commands may have been sent
				if req.Table == "hosts" {
					go peer.UpdateDeltaTableHosts("")
				} else if req.Table == "services" {
					go peer.UpdateDeltaTableServices("")
				}
				close(c)
				return
			}
			time.Sleep(time.Millisecond * 200)
			if req.Table == "hosts" {
				peer.UpdateDeltaTableHosts("Filter: name = " + req.WaitObject + "\n")
			} else if req.Table == "services" {
				tmp := strings.SplitN(req.WaitObject, ";", 2)
				if len(tmp) < 2 {
					log.Errorf("unsupported service wait object: %s", req.WaitObject)
					close(c)
					return
				}
				peer.UpdateDeltaTableServices("Filter: host_name = " + tmp[0] + "\nFilter: description = " + tmp[1] + "\n")
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
