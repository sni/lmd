package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Response contains the livestatus response data as long with some meta data
type Response struct {
	noCopy      noCopy
	Lock        *LoggingLock // must be used for Result and Failed access
	Code        int
	Result      ResultSet
	ResultTotal int
	Request     *Request
	Error       error
	Failed      map[string]string
}

// ResultSet is a list of result rows
type ResultSet [][]interface{}

// NewResponse creates a new response object for a given request
// It returns the Response object and any error encountered.
func NewResponse(req *Request) (res *Response, err error) {
	res = &Response{
		Code:    200,
		Failed:  req.BackendErrors,
		Request: req,
		Lock:    NewLoggingLock("ResponseLock"),
	}
	if res.Failed == nil {
		res.Failed = make(map[string]string)
	}

	table := Objects.Tables[req.Table]

	// check if we have to spin up updates, if so, do it parallel
	selectedPeers := []string{}
	spinUpPeers := []string{}
	// iterate over PeerMap instead of BackendsMap to retain backend order
	PeerMapLock.RLock()
	for _, id := range PeerMapOrder {
		p := PeerMap[id]
		if _, ok := req.BackendsMap[p.ID]; !ok {
			continue
		}
		if !nodeAccessor.IsOurBackend(p.ID) {
			continue
		}
		selectedPeers = append(selectedPeers, p.ID)

		// spin up required?
		if p.StatusGet("Idling").(bool) && table.Virtual == nil {
			p.StatusSet("LastQuery", time.Now().Unix())
			spinUpPeers = append(spinUpPeers, id)
		}
	}
	PeerMapLock.RUnlock()

	if len(selectedPeers) == 0 {
		res.Result = make(ResultSet, 0)
		res.PostProcessing()
		return
	}

	// only use the first backend when requesting table or columns table
	if table.Name == "tables" || table.Name == "columns" {
		res.AppendPeerResult(PeerMap[PeerMapOrder[0]])
	} else {
		if !table.PassthroughOnly && len(spinUpPeers) > 0 {
			SpinUpPeers(spinUpPeers)
		}

		if table.PassthroughOnly {
			// passthrough requests, ex.: log table
			res.BuildPassThroughResult(selectedPeers, table)
		} else {
			res.BuildLocalResponse(selectedPeers)
		}
	}

	// if all backends are down, send an error instead of an empty result
	if res.Request.OutputFormat != OutputFormatWrappedJSON && len(res.Failed) > 0 && len(res.Failed) == len(req.Backends) {
		res.Code = 502
		err = &PeerError{msg: res.Failed[req.Backends[0]], kind: ConnectionError}
		return
	}

	if res.Result == nil {
		res.Result = make(ResultSet, 0)
	}
	res.PostProcessing()
	return
}

// Len returns the result length used for sorting results.
func (res *Response) Len() int {
	return len(res.Result)
}

// Less returns the sort result of two data rows
func (res *Response) Less(i, j int) bool {
	for k := range res.Request.Sort {
		s := &(res.Request.Sort[k])
		var sortType DataType
		if s.Group {
			sortType = StringCol
		} else {
			sortType = res.Request.RequestColumns[s.Index].OutputType
		}
		switch sortType {
		case IntCol:
			fallthrough
		case FloatCol:
			valueA := res.Result[i][s.Index].(float64)
			valueB := res.Result[j][s.Index].(float64)
			if valueA == valueB {
				continue
			}
			if s.Direction == Asc {
				return valueA < valueB
			}
			return valueA > valueB
		case StringCol:
			index := s.Index
			if s.Group {
				index = 0
			}
			s1 := res.Result[i][index].(string)
			s2 := res.Result[j][index].(string)
			if s1 == s2 {
				continue
			}
			if s.Direction == Asc {
				return s1 < s2
			}
			return s1 > s2
		case StringListCol:
			// not implemented
			return s.Direction == Asc
		case IntListCol:
			// not implemented
			return s.Direction == Asc
		case HashMapCol:
			s1 := (res.Result[i][s.Index]).(map[string]string)[s.Args]
			s2 := (res.Result[j][s.Index]).(map[string]string)[s.Args]
			if s1 == s2 {
				continue
			}
			if s.Direction == Asc {
				return s1 < s2
			}
			return s1 > s2
		}
		panic(fmt.Sprintf("sorting not implemented for type %d", sortType))
	}
	return true
}

// Swap replaces two data rows while sorting.
func (res *Response) Swap(i, j int) {
	res.Result[i], res.Result[j] = res.Result[j], res.Result[i]
}

// ExpandRequestedBackends fills the requests backends map
func (req *Request) ExpandRequestedBackends() (err error) {
	req.BackendsMap = make(map[string]string)
	req.BackendErrors = make(map[string]string)

	// no backends selected means all backends
	PeerMapLock.RLock()
	defer PeerMapLock.RUnlock()
	if len(req.Backends) == 0 {
		for _, p := range PeerMap {
			req.BackendsMap[p.ID] = p.ID
		}
		return
	}

	for _, b := range req.Backends {
		_, Ok := PeerMap[b]
		if !Ok {
			req.BackendErrors[b] = fmt.Sprintf("bad request: backend %s does not exist", b)
			continue
		}
		req.BackendsMap[b] = b
	}
	return
}

// PostProcessing does all the post processing required for a request like sorting
// and cutting of limits, applying offsets and calculating final stats.
func (res *Response) PostProcessing() {
	log.Tracef("PostProcessing")
	// sort our result
	if len(res.Request.Sort) > 0 {
		// skip sorting if there is only one backend requested and we want the default sort order
		if len(res.Request.BackendsMap) >= 1 || !res.Request.IsDefaultSortOrder(&res.Request.Sort) {
			t1 := time.Now()
			sort.Sort(res)
			duration := time.Since(t1)
			log.Debugf("sorting result took %s", duration.String())
		}

		// remove hidden columns from result, for ex. columns used for sorting only
		firstHiddenColumnIndex := -1
		for i := range res.Request.RequestColumns {
			// first hidden column breaks the loop, because hidden columns are appended at the end of all columns
			if res.Request.RequestColumns[i].Hidden {
				firstHiddenColumnIndex = i
			}
		}
		if firstHiddenColumnIndex != -1 {
			for i := range res.Result {
				res.Result[i] = (res.Result[i])[:firstHiddenColumnIndex]
			}
		}
	}

	if res.ResultTotal == 0 {
		res.ResultTotal = len(res.Result)
	}

	// apply request offset
	if res.Request.Offset > 0 {
		if res.Request.Offset > res.ResultTotal {
			res.Result = make(ResultSet, 0)
		} else {
			res.Result = res.Result[res.Request.Offset:]
		}
	}

	// apply request limit
	if res.Request.Limit != nil && *res.Request.Limit >= 0 && *res.Request.Limit < len(res.Result) {
		res.Result = res.Result[0:*res.Request.Limit]
	}

	// final calculation of stats querys
	res.CalculateFinalStats()
}

// CalculateFinalStats calculates final averages and sums from stats queries
func (res *Response) CalculateFinalStats() {
	if len(res.Request.Stats) == 0 {
		return
	}
	hasColumns := len(res.Request.Columns)
	if hasColumns == 0 && len(res.Request.StatsResult) == 0 {
		if res.Request.StatsResult == nil {
			res.Request.StatsResult = make(map[string][]*Filter)
		}
		res.Request.StatsResult[""] = createLocalStatsCopy(&res.Request.Stats)
	}
	res.Result = make(ResultSet, len(res.Request.StatsResult))

	j := 0
	for key, stats := range res.Request.StatsResult {
		rowSize := len(stats)
		rowSize += hasColumns
		res.Result[j] = make([]interface{}, rowSize)
		if hasColumns > 0 {
			for i, keyPart := range strings.Split(key, ";") {
				res.Result[j][i] = keyPart
			}
		}
		for i := range stats {
			s := stats[i]
			i += hasColumns

			res.Result[j][i] = finalStatsApply(s)

			if res.Request.SendStatsData {
				res.Result[j][i] = []interface{}{s.Stats, s.StatsCount}
				continue
			}

		}
		j++
	}

	/* sort by columns for grouped stats */
	if hasColumns > 0 {
		t1 := time.Now()
		// fake sort column
		if hasColumns > 0 {
			res.Request.Sort = []SortField{}
			for x := 0; x < hasColumns; x++ {
				res.Request.Sort = append(res.Request.Sort, SortField{Name: "name", Group: true, Direction: Asc})
			}
		}
		sort.Sort(res)
		duration := time.Since(t1)
		log.Debugf("sorting result took %s", duration.String())
		res.Request.Sort = []SortField{}
	}
}

func finalStatsApply(s *Filter) (res float64) {
	switch s.StatsType {
	case Counter:
		res = s.Stats
	case Min:
		res = s.Stats
	case Max:
		res = s.Stats
	case Sum:
		res = s.Stats
	case Average:
		if s.StatsCount > 0 {
			res = s.Stats / float64(s.StatsCount)
		} else {
			res = 0
		}
	default:
		log.Panicf("not implemented")
	}
	if s.StatsCount == 0 {
		res = 0
	}
	return
}

// Send writes converts the result object to a livestatus answer and writes the resulting bytes back to the client.
func (res *Response) Send(c net.Conn) (size int, err error) {
	resBytes, err := res.Bytes()
	if err != nil {
		return
	}
	size = len(resBytes) + 1
	if res.Request.ResponseFixed16 {
		if log.IsV(3) {
			log.Tracef("write: %s", fmt.Sprintf("%d %11d", res.Code, size))
		}
		_, err = c.Write([]byte(fmt.Sprintf("%d %11d\n", res.Code, size)))
		if err != nil {
			log.Warnf("write error: %s", err.Error())
			return
		}
	}
	if log.IsV(3) {
		log.Tracef("write: %s", resBytes)
	}
	written, err := c.Write(resBytes)
	if err != nil {
		log.Warnf("write error: %s", err.Error())
		return
	}
	if written != size-1 {
		log.Warnf("write error: written %d, size: %d", written, size)
		return
	}
	localAddr := c.LocalAddr().String()
	promFrontendBytesSend.WithLabelValues(localAddr).Add(float64(len(resBytes)))
	_, err = c.Write([]byte("\n"))
	return
}

// Bytes converts the response into a bytes array
func (res *Response) Bytes() ([]byte, error) {
	if res.Error != nil {
		log.Warnf("sending error response: %d - %s", res.Code, res.Error.Error())
		return []byte(res.Error.Error()), nil
	}

	if res.Request.OutputFormat == OutputFormatWrappedJSON {
		return res.WrappedJSON()
	}
	return res.JSON()
}

// JSON converts the response into a json structure
func (res *Response) JSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)

	// enable header row for regular requests, not for stats requests
	isStatsRequest := len(res.Request.Stats) != 0
	sendColumnsHeader := res.SendColumnsHeader()

	buf.Write([]byte("["))
	// add optional columns header as first row
	cols := make([]interface{}, len(res.Request.RequestColumns)+len(res.Request.Stats))
	if sendColumnsHeader {
		for k := 0; k < len(res.Request.RequestColumns); k++ {
			if k < len(res.Request.Columns) {
				cols[k] = res.Request.Columns[k]
			} else {
				cols[k] = res.Request.RequestColumns[k].Name
			}
		}
		if isStatsRequest {
			for i := range res.Request.Stats {
				index := i + len(res.Request.RequestColumns)
				var buffer bytes.Buffer
				buffer.WriteString("stats_")
				buffer.WriteString(strconv.Itoa(i + 1))
				cols[index] = buffer.String()
			}
		}
		err := enc.Encode(cols)
		if err != nil {
			log.Errorf("json error: %s in column header: %v", err.Error(), cols)
			return nil, err
		}
	}
	// append result row by row
	for i := range res.Result {
		if i == 0 {
			if sendColumnsHeader {
				buf.Write([]byte(",\n"))
			}
		} else {
			buf.Write([]byte(","))
		}
		err := enc.Encode(res.Result[i])
		if err != nil {
			log.Errorf("json error: %s in row: %v", err.Error(), res.Result[i])
			return nil, err
		}
	}
	buf.Write([]byte("]"))
	return buf.Bytes(), nil
}

// WrappedJSON converts the response into a wrapped json structure
func (res *Response) WrappedJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)

	buf.Write([]byte("{\"data\":"))

	// enable header row for regular requests, not for stats requests
	isStatsRequest := len(res.Request.Stats) != 0
	sendColumnsHeader := res.SendColumnsHeader()

	buf.Write([]byte("["))
	// add optional columns header as first row
	cols := make([]interface{}, len(res.Request.RequestColumns)+len(res.Request.Stats))
	if sendColumnsHeader {
		for k := 0; k < len(res.Request.RequestColumns); k++ {
			if k < len(res.Request.Columns) {
				cols[k] = res.Request.Columns[k]
			} else {
				cols[k] = res.Request.RequestColumns[k].Name
			}
		}

		if isStatsRequest {
			for i := range res.Request.Stats {
				index := i + len(res.Request.Columns)
				var buffer bytes.Buffer
				buffer.WriteString("stats_")
				buffer.WriteString(strconv.Itoa(i + 1))
				cols[index] = buffer.String()
			}
		}
	}
	// append result row by row
	for i := range res.Result {
		if i > 0 {
			buf.Write([]byte(","))
		}
		err := enc.Encode(res.Result[i])
		if err != nil {
			log.Errorf("json error: %s in row: %v", err.Error(), res.Result[i])
			return nil, err
		}
	}
	buf.Write([]byte("]"))
	buf.Write([]byte("\n,\"failed\":"))
	enc.Encode(res.Failed)
	if sendColumnsHeader {
		buf.Write([]byte("\n,\"columns\":"))
		enc.Encode(cols)
	}
	buf.Write([]byte(fmt.Sprintf("\n,\"total_count\":%d}", res.ResultTotal)))
	return buf.Bytes(), nil
}

// BuildLocalResponse builds local data table result for all selected peers
func (res *Response) BuildLocalResponse(peers []string) {
	res.Result = make(ResultSet, 0)

	waitgroup := &sync.WaitGroup{}

	for _, id := range peers {
		PeerMapLock.RLock()
		p := PeerMap[id]
		PeerMapLock.RUnlock()
		if p == nil {
			log.Errorf("got no backend for id: %s", id)
			continue
		}
		if p.Flags&MultiBackend == MultiBackend {
			continue
		}
		p.DataLock.RLock()
		store := p.Tables[res.Request.Table]
		p.DataLock.RUnlock()

		p.StatusSet("LastQuery", time.Now().Unix())

		if store != nil && store.Table.Virtual != nil {
			// append results serially for local calculations
			// no need to create goroutines for simple sites queries
			res.AppendPeerResult(p)
			continue
		}

		if store == nil || !p.isOnline() {
			res.Lock.Lock()
			res.Failed[p.ID] = p.getError()
			res.Lock.Unlock()
			continue
		}

		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)

			log.Tracef("[%s] starting local data computation", peer.Name)
			defer wg.Done()

			res.AppendPeerResult(peer)
		}(p, waitgroup)
	}
	log.Tracef("waiting...")
	waitgroup.Wait()
	log.Tracef("waiting for all local data computations done")
}

// AppendPeerResult appends result for given peer
func (res *Response) AppendPeerResult(peer *Peer) {
	total, result, statsResult := peer.BuildLocalResponseData(res)
	log.Tracef("[%s] result ready", peer.Name)

	if result != nil {
		if total == 0 {
			return
		}

		// data results rows
		res.Lock.Lock()
		res.ResultTotal += total
		res.Result = append(res.Result, *result...)
		res.Lock.Unlock()
	} else if statsResult != nil {
		res.Lock.Lock()
		res.ResultTotal += total
		if res.Request.StatsResult == nil {
			res.Request.StatsResult = make(map[string][]*Filter)
		}
		// apply stats querys
		for key, stats := range statsResult {
			if _, ok := res.Request.StatsResult[key]; !ok {
				res.Request.StatsResult[key] = stats
			} else {
				for i := range stats {
					s := stats[i]
					res.Request.StatsResult[key][i].ApplyValue(s.Stats, s.StatsCount)
				}
			}
		}
		res.Lock.Unlock()
	}
}

// BuildPassThroughResult passes a query transparently to one or more remote sites and builds the response
// from that.
func (res *Response) BuildPassThroughResult(peers []string, table *Table) {
	res.Result = make(ResultSet, 0)

	// build columns list
	backendColumns := []string{}
	virtColumns := []*RequestColumn{}
	for i := range res.Request.RequestColumns {
		col := &(res.Request.RequestColumns[i])
		if col.Column.StorageType == VirtStore {
			virtColumns = append(virtColumns, col)
		} else {
			backendColumns = append(backendColumns, col.Name)
		}
	}

	waitgroup := &sync.WaitGroup{}

	for _, id := range peers {
		PeerMapLock.RLock()
		p := PeerMap[id]
		PeerMapLock.RUnlock()

		peerStatus := p.StatusGet("PeerStatus").(PeerStatus)
		if peerStatus == PeerStatusDown || peerStatus == PeerStatusBroken {
			res.Lock.Lock()
			res.Failed[p.ID] = fmt.Sprintf("%v", p.StatusGet("LastError"))
			res.Lock.Unlock()
			continue
		}

		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)

			log.Debugf("[%s] starting passthrough request", peer.Name)
			defer wg.Done()

			res.PassThrougQuery(peer, table, virtColumns, backendColumns)
		}(p, waitgroup)
	}
	log.Tracef("waiting...")
	waitgroup.Wait()
	log.Debugf("waiting for passed through requests done")
}

// PassThrougQuery runs a passthrough query on a single peer and appends the result
func (res *Response) PassThrougQuery(peer *Peer, table *Table, virtColumns []*RequestColumn, backendColumns []string) {
	req := res.Request
	passthroughRequest := &Request{
		Table:           req.Table,
		Filter:          req.Filter,
		Stats:           req.Stats,
		Columns:         backendColumns,
		Limit:           req.Limit,
		OutputFormat:    OutputFormatJSON,
		ResponseFixed16: true,
	}
	result, _, queryErr := peer.Query(passthroughRequest)
	log.Tracef("[%s] req done", peer.Name)
	if queryErr != nil {
		log.Tracef("[%s] req errored", queryErr.Error())
		res.Lock.Lock()
		res.Failed[peer.ID] = queryErr.Error()
		res.Lock.Unlock()
		return
	}
	// insert virtual values, like peer_addr or name
	if len(virtColumns) > 0 {
		store := NewDataStore(table, peer)
		data := make([]interface{}, 0)
		columns := make(ColumnList, 0)
		tmpRow, _ := NewDataRow(store, &data, &columns, 0)
		for rowNum := range *result {
			row := &((*result)[rowNum])
			for j := range virtColumns {
				col := virtColumns[j]
				i := col.Index
				*row = append(*row, 0)
				copy((*row)[i+1:], (*row)[i:])
				(*row)[i] = tmpRow.GetValueByColumn(col.Column)
			}
			(*result)[rowNum] = *row
		}
	}
	log.Tracef("[%s] result ready", peer.Name)
	res.Lock.Lock()
	if len(req.Stats) == 0 {
		res.Result = append(res.Result, *result...)
	} else {
		if res.Request.StatsResult == nil {
			res.Request.StatsResult = make(map[string][]*Filter)
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

// determines if the response should contain the columns header
func (res *Response) SendColumnsHeader() bool {
	if len(res.Request.Stats) > 0 {
		return false
	}
	if res.Request.ColumnsHeaders || len(res.Request.Columns) == 0 {
		return true
	}
	return false
}
