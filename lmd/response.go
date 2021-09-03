package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/sasha-s/go-deadlock"
)

const (
	// SpinUpPeersTimeout sets timeout to wait for peers after spin up
	SpinUpPeersTimeout = 5 * time.Second
)

// Response contains the livestatus response data as long with some meta data
type Response struct {
	noCopy        noCopy
	Lock          *deadlock.RWMutex // must be used for Result and Failed access
	Request       *Request          // the initial request
	Result        ResultSet         // final processed result table
	Code          int               // 200 if the query was successful
	Error         error             // error object if the query was not successful
	RawResults    *RawResultSet     // collected results from peers
	ResultTotal   int
	RowsScanned   int // total number of data rows scanned for this result
	Failed        map[string]string
	SelectedPeers []*Peer
}

// PeerResponse is the sub result from a peer before merged into the end result
type PeerResponse struct {
	Rows        []*DataRow // set of datarows
	Total       int        // total number of matched rows regardless of any limits or offsets
	RowsScanned int        // total number of rows scanned to create result
}

// NewResponse creates a new response object for a given request
// It returns the Response object and any error encountered.
func NewResponse(req *Request) (res *Response, err error) {
	res = &Response{
		Code:    200,
		Failed:  req.BackendErrors,
		Request: req,
		Lock:    new(deadlock.RWMutex),
	}
	if res.Failed == nil {
		res.Failed = make(map[string]string)
	}

	table := Objects.Tables[req.Table]

	// check if we have to spin up updates, if so, do it parallel
	res.SelectedPeers = make([]*Peer, 0)
	spinUpPeers := make([]*Peer, 0)
	// iterate over PeerMap instead of BackendsMap to retain backend order
	req.lmd.PeerMapLock.RLock()
	for _, id := range req.lmd.PeerMapOrder {
		p := req.lmd.PeerMap[id]
		if _, ok := req.BackendsMap[p.ID]; !ok {
			continue
		}
		if req.lmd.nodeAccessor == nil || !req.lmd.nodeAccessor.IsOurBackend(p.ID) {
			continue
		}
		if p.HasFlag(MultiBackend) {
			continue
		}
		res.SelectedPeers = append(res.SelectedPeers, p)

		// spin up required?
		if p.StatusGet(Idling).(bool) && table.Virtual == nil {
			p.StatusSet(LastQuery, time.Now().Unix())
			spinUpPeers = append(spinUpPeers, p)
		}
	}
	req.lmd.PeerMapLock.RUnlock()

	// only use the first backend when requesting table or columns table
	if table.Name == TableTables || table.Name == TableColumns {
		res.SelectedPeers = []*Peer{req.lmd.PeerMap[req.lmd.PeerMapOrder[0]]}
	}

	if !table.PassthroughOnly && len(spinUpPeers) > 0 {
		SpinUpPeers(spinUpPeers)
	}

	// if all backends are down, send an error instead of an empty result
	if res.Request.OutputFormat != OutputFormatWrappedJSON && len(res.Failed) > 0 && len(res.Failed) == len(req.Backends) {
		res.Code = 502
		err = &PeerError{msg: res.Failed[req.Backends[0]], kind: ConnectionError}
		return
	}

	switch {
	case len(res.SelectedPeers) == 0:
		// no backends selected, return empty result
		res.Result = make(ResultSet, 0)
		return
	case table.PassthroughOnly:
		// passthrough requests, ex.: log table
		res.BuildPassThroughResult()
		res.PostProcessing()
	default:
		// normal requests
		res.RawResults = &RawResultSet{}
		res.RawResults.Sort = req.Sort
		res.BuildLocalResponse()
		res.RawResults.PostProcessing(res)
	}

	res.CalculateFinalStats()
	return
}

// Len returns the result length used for sorting results.
func (res *Response) Len() int {
	return len(res.Result)
}

// Less returns the sort result of two data rows
func (res *Response) Less(i, j int) bool {
	for k := range res.Request.Sort {
		s := res.Request.Sort[k]
		var sortType DataType
		if s.Group {
			sortType = StringCol
		} else {
			sortType = res.Request.RequestColumns[s.Index].DataType
		}
		switch sortType {
		case IntCol:
			fallthrough
		case Int64Col:
			fallthrough
		case FloatCol:
			valueA := interface2float64(res.Result[i][s.Index])
			valueB := interface2float64(res.Result[j][s.Index])
			if valueA == valueB {
				continue
			}
			if s.Direction == Asc {
				return valueA < valueB
			}
			return valueA > valueB
		case JSONCol:
			fallthrough
		case StringCol:
			index := s.Index
			if s.Group {
				index = 0
			}
			s1 := interface2stringNoDedup(res.Result[i][index])
			s2 := interface2stringNoDedup(res.Result[j][index])
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
		case Int64ListCol:
			// not implemented
			return s.Direction == Asc
		}
		panic(fmt.Sprintf("sorting not implemented for type %s", sortType))
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
	req.lmd.PeerMapLock.RLock()
	defer req.lmd.PeerMapLock.RUnlock()
	if len(req.Backends) == 0 {
		for id := range req.lmd.PeerMap {
			p := req.lmd.PeerMap[id]
			req.BackendsMap[p.ID] = p.ID
		}
		return
	}

	for _, b := range req.Backends {
		_, Ok := req.lmd.PeerMap[b]
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
	if len(res.Request.Stats) > 0 {
		return
	}
	logWith(res).Tracef("PostProcessing")
	if res.Result == nil {
		res.Result = make(ResultSet, 0)
	}
	// sort our result
	if len(res.Request.Sort) > 0 {
		// skip sorting if there is only one backend requested and we want the default sort order
		if len(res.Request.BackendsMap) >= 1 || !res.Request.IsDefaultSortOrder() {
			t1 := time.Now()
			sort.Sort(res)
			duration := time.Since(t1)
			logWith(res).Debugf("sorting result took %s", duration.String())
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
}

// CalculateFinalStats calculates final averages and sums from stats queries
func (res *Response) CalculateFinalStats() {
	if len(res.Request.Stats) == 0 {
		return
	}
	if res.Request.StatsResult == nil {
		res.Request.StatsResult = NewResultSetStats()
	}
	hasColumns := len(res.Request.Columns)
	if hasColumns == 0 && len(res.Request.StatsResult.Stats) == 0 {
		res.Request.StatsResult.Stats[""] = createLocalStatsCopy(res.Request.Stats)
	}
	res.Result = make(ResultSet, len(res.Request.StatsResult.Stats))

	j := 0
	for key, stats := range res.Request.StatsResult.Stats {
		rowSize := len(stats)
		rowSize += hasColumns
		res.Result[j] = make([]interface{}, rowSize)
		if hasColumns > 0 {
			parts := strings.Split(key, ListSepChar1)
			for i := range parts {
				res.Result[j][i] = &parts[i]
				if i >= hasColumns {
					break
				}
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
		res.RowsScanned += res.Request.StatsResult.RowsScanned
	}

	if hasColumns > 0 {
		// Sort by stats key
		res.Request.Sort = make([]*SortField, 0, hasColumns)
		for i := range res.Request.Columns {
			res.Request.Sort = append(res.Request.Sort, &SortField{Index: i, Group: true, Direction: Asc})
		}
		sort.Sort(res)
	}
	res.ResultTotal += len(res.Result)
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

// Send converts the result object to a livestatus answer and writes the resulting bytes back to the client.
func (res *Response) Send(c net.Conn) (size int64, err error) {
	if res.Request.ResponseFixed16 {
		return res.SendFixed16(c)
	}
	return res.SendUnbuffered(c)
}

// SendFixed16 converts the result object to a livestatus answer and writes the resulting bytes back to the client.
func (res *Response) SendFixed16(c net.Conn) (size int64, err error) {
	resBuffer, err := res.Buffer()
	if err != nil {
		return
	}
	size = int64(resBuffer.Len()) + 1
	headerFixed16 := fmt.Sprintf("%d %11d", res.Code, size)
	logWith(res).Tracef("write: %s", headerFixed16)
	_, err = fmt.Fprintf(c, "%s\n", headerFixed16)
	if err != nil {
		logWith(res).Warnf("write error: %s", err.Error())
		return
	}
	if log.IsV(LogVerbosityTrace) {
		logWith(res).Tracef("write: %s", resBuffer.Bytes())
	}
	written, err := resBuffer.WriteTo(c)
	if err != nil {
		logWith(res).Warnf("write error: %s", err.Error())
		return
	}
	localAddr := c.LocalAddr().String()
	promFrontendBytesSend.WithLabelValues(localAddr).Add(float64(written + 1))
	if written != size-1 {
		logWith(res).Warnf("write error: written %d, size: %d", written, size)
		return
	}
	_, err = c.Write([]byte("\n"))
	return
}

// SendUnbuffered directly prints the result to the client connection
func (res *Response) SendUnbuffered(c io.Writer) (size int64, err error) {
	countingWriter := NewWriteCounter(c)
	if res.Error != nil {
		logWith(res).Warnf("sending error response: %d - %s", res.Code, res.Error.Error())
		_, err = countingWriter.Write([]byte(res.Error.Error()))
		if err != nil {
			return
		}
		_, err = countingWriter.Write([]byte("\n"))
		size = countingWriter.Count
		return
	}
	if res.Request.OutputFormat == OutputFormatWrappedJSON {
		err = res.WrappedJSON(countingWriter)
	} else {
		err = res.JSON(countingWriter)
	}
	if err != nil {
		logWith(res).Warnf("write error: %s", err.Error())
		return
	}
	_, err = countingWriter.Write([]byte("\n"))
	size = countingWriter.Count
	return
}

// Buffer fills buffer with the response as bytes array
func (res *Response) Buffer() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	if res.Error != nil {
		logWith(res).Warnf("sending error response: %d - %s", res.Code, res.Error.Error())
		buf.WriteString(res.Error.Error())
		return buf, nil
	}

	if res.Request.OutputFormat == OutputFormatWrappedJSON {
		return buf, res.WrappedJSON(buf)
	}
	return buf, res.JSON(buf)
}

// JSON converts the response into a json structure
func (res *Response) JSON(buf io.Writer) error {
	json := jsoniter.ConfigCompatibleWithStandardLibrary.BorrowStream(buf)
	defer jsoniter.ConfigCompatibleWithStandardLibrary.ReturnStream(json)

	json.WriteRaw("[")

	// add optional columns header as first row
	sendColumnsHeader := res.SendColumnsHeader()
	if sendColumnsHeader {
		res.WriteColumnsResponse(json)
		if (res.Result != nil && len(res.Result) > 0) || (res.RawResults != nil && len(res.RawResults.DataResult) > 0) {
			json.WriteRaw(",")
		}
	}

	res.WriteDataResponse(json)

	json.WriteRaw("]")
	err := json.Flush()
	if err != nil {
		return fmt.Errorf("JSON: %w", err)
	}
	json.Reset(nil)
	return nil
}

// WrappedJSON converts the response into a wrapped json structure
func (res *Response) WrappedJSON(buf io.Writer) error {
	json := jsoniter.ConfigCompatibleWithStandardLibrary.BorrowStream(buf)
	defer jsoniter.ConfigCompatibleWithStandardLibrary.ReturnStream(json)

	json.WriteRaw("{\"data\":\n[")
	res.WriteDataResponse(json)
	json.WriteRaw("]\n,\"failed\": {")
	num := 0
	for k, v := range res.Failed {
		if num > 0 {
			json.WriteMore()
		}
		json.WriteObjectField(k)
		json.WriteString(strings.TrimSpace(v))
		num++
	}
	json.WriteObjectEnd()

	// add optional columns header as first row
	if res.SendColumnsHeader() {
		json.WriteRaw("\n,\"columns\":")
		res.WriteColumnsResponse(json)
	}

	json.WriteRaw(fmt.Sprintf("\n,\"rows_scanned\":%d", res.RowsScanned))
	json.WriteRaw(fmt.Sprintf("\n,\"total_count\":%d}", res.ResultTotal))
	err := json.Flush()
	if err != nil {
		return fmt.Errorf("WrappedJSON: %w", err)
	}
	json.Reset(nil)
	return nil
}

// WriteDataResponse writes the data part of the result
func (res *Response) WriteDataResponse(json *jsoniter.Stream) {
	switch {
	case res.Result != nil:
		// append result row by row
		for i := range res.Result {
			if i > 0 {
				json.WriteRaw(",\n")
				json.Flush()
			}
			json.WriteArrayStart()
			for k := range res.Result[i] {
				if k > 0 {
					json.WriteMore()
				}
				json.WriteVal(res.Result[i][k])
			}
			json.WriteArrayEnd()
		}
	case res.RawResults != nil:
		// unprocessed result?
		res.ResultTotal = res.RawResults.Total
		res.RowsScanned = res.RawResults.RowsScanned

		// PeerLockModeFull means we have to lock all peers before creating the result
		if len(res.RawResults.DataResult) > 0 && res.RawResults.DataResult[0].DataStore.PeerLockMode == PeerLockModeFull {
			res.WriteDataResponseRowLocked(json)
			return
		}

		for i := range res.RawResults.DataResult {
			if i > 0 {
				json.WriteRaw(",\n")
				json.Flush()
			}
			res.RawResults.DataResult[i].WriteJSON(json, res.Request.RequestColumns)
		}
	default:
		logWith(res).Errorf("response contains no result at all")
	}
}

// WriteDataResponseRowLocked appends each row but locks the peer before doing so. We don't have to lock for each column then
func (res *Response) WriteDataResponseRowLocked(json *jsoniter.Stream) {
	for i := range res.RawResults.DataResult {
		if i > 0 {
			json.WriteRaw(",\n")
		}
		row := res.RawResults.DataResult[i]
		row.DataStore.Peer.Lock.RLock()
		row.WriteJSON(json, res.Request.RequestColumns)
		row.DataStore.Peer.Lock.RUnlock()
	}
}

// WriteColumnsResponse writes the columns header
func (res *Response) WriteColumnsResponse(json *jsoniter.Stream) {
	cols := make([]string, len(res.Request.RequestColumns)+len(res.Request.Stats))
	for k := 0; k < len(res.Request.RequestColumns); k++ {
		if k < len(res.Request.Columns) {
			cols[k] = res.Request.Columns[k]
		} else {
			cols[k] = res.Request.RequestColumns[k].Name
		}
	}
	for i := range res.Request.Stats {
		index := i + len(res.Request.RequestColumns)
		var buffer bytes.Buffer
		buffer.WriteString("stats_")
		buffer.WriteString(strconv.Itoa(i + 1))
		cols[index] = buffer.String()
	}
	json.WriteArrayStart()
	for i, s := range cols {
		if i > 0 {
			json.WriteMore()
		}
		json.WriteString(s)
	}
	json.WriteArrayEnd()
	json.WriteRaw("\n")
}

// BuildLocalResponse builds local data table result for all selected peers
func (res *Response) BuildLocalResponse() {
	var resultcollector chan *PeerResponse
	var waitChan chan bool
	if len(res.Request.Stats) == 0 {
		waitChan = make(chan bool)
		resultcollector = make(chan *PeerResponse, len(res.SelectedPeers))
		go func() {
			result := res.RawResults
			for subRes := range resultcollector {
				result.Total += subRes.Total
				result.RowsScanned += subRes.RowsScanned
				result.DataResult = append(result.DataResult, subRes.Rows...)
			}
			waitChan <- true
		}()
	}

	waitgroup := &sync.WaitGroup{}
	for i := range res.SelectedPeers {
		p := res.SelectedPeers[i]
		p.StatusSet(LastQuery, time.Now().Unix())

		store, err := p.GetDataStore(res.Request.Table)
		if err != nil {
			res.Lock.Lock()
			res.Failed[p.ID] = err.Error()
			res.Lock.Unlock()
			continue
		}

		// process virtual tables serially without go routines to maintain the correct order, ex.: from the sites table
		if store.Table.Virtual != nil {
			res.BuildLocalResponseData(store, resultcollector)
			continue
		}

		// if a WaitTrigger is supplied, wait max ms till the condition is true
		if res.Request.WaitTrigger != "" {
			p.WaitCondition(res.Request)

			// peer might have gone down meanwhile, ex. after waiting for a waittrigger, so check again
			s, err := p.GetDataStore(res.Request.Table)
			if err != nil {
				res.Lock.Lock()
				res.Failed[p.ID] = err.Error()
				res.Lock.Unlock()
				return
			}
			store = s
		}

		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)

			defer wg.Done()

			res.BuildLocalResponseData(store, resultcollector)
		}(p, waitgroup)
	}
	logWith(res).Tracef("waiting...")

	waitgroup.Wait()
	if resultcollector != nil {
		close(resultcollector)
		<-waitChan
	}

	logWith(res).Tracef("waiting for all local data computations done")
}

// MergeStats merges stats result into final result set
func (res *Response) MergeStats(stats *ResultSetStats) {
	res.Lock.Lock()
	defer res.Lock.Unlock()
	if res.Request.StatsResult == nil {
		res.Request.StatsResult = NewResultSetStats()
	}
	// apply stats queries
	for key, stats := range stats.Stats {
		if _, ok := res.Request.StatsResult.Stats[key]; !ok {
			res.Request.StatsResult.Stats[key] = stats
		} else {
			for i := range stats {
				s := stats[i]
				res.Request.StatsResult.Stats[key][i].ApplyValue(s.Stats, s.StatsCount)
			}
		}
	}
	res.Request.StatsResult.Total += stats.Total
	res.Request.StatsResult.RowsScanned += stats.RowsScanned
}

// BuildPassThroughResult passes a query transparently to one or more remote sites and builds the response
// from that.
func (res *Response) BuildPassThroughResult() {
	res.Result = make(ResultSet, 0)

	// build columns list
	backendColumns := []string{}
	virtualColumns := []*Column{}
	columnsIndex := make(map[*Column]int)
	for i := range res.Request.RequestColumns {
		col := res.Request.RequestColumns[i]
		if col.StorageType == VirtualStore {
			virtualColumns = append(virtualColumns, col)
		} else {
			backendColumns = append(backendColumns, col.Name)
		}
		columnsIndex[col] = i
	}
	for i := range res.Request.Sort {
		s := res.Request.Sort[i]
		if j, ok := columnsIndex[s.Column]; ok {
			// sort column does exist in the request columns
			s.Index = j
		} else {
			s.Index = len(backendColumns) + len(virtualColumns)
			if s.Column.StorageType == VirtualStore {
				virtualColumns = append(virtualColumns, s.Column)
			} else {
				backendColumns = append(backendColumns, s.Column.Name)
			}
		}
	}
	req := res.Request
	passthroughRequest := &Request{
		Table:           req.Table,
		Filter:          req.Filter,
		Stats:           req.Stats,
		Columns:         backendColumns,
		Limit:           req.Limit,
		OutputFormat:    OutputFormatJSON,
		ResponseFixed16: true,
		AuthUser:        req.AuthUser,
	}

	waitgroup := &sync.WaitGroup{}

	for i := range res.SelectedPeers {
		p := res.SelectedPeers[i]

		if !p.isOnline() {
			res.Lock.Lock()
			res.Failed[p.ID] = fmt.Sprintf("%v", p.StatusGet(LastError))
			res.Lock.Unlock()
			continue
		}

		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)

			logWith(peer, passthroughRequest).Debugf("starting passthrough request")
			defer wg.Done()

			peer.PassThroughQuery(res, passthroughRequest, virtualColumns, columnsIndex)
		}(p, waitgroup)
	}
	logWith(passthroughRequest).Tracef("waiting...")
	waitgroup.Wait()
	logWith(passthroughRequest).Debugf("waiting for passed through requests done")
}

// SendColumnsHeader determines if the response should contain the columns header
func (res *Response) SendColumnsHeader() bool {
	if len(res.Request.Stats) > 0 {
		return false
	}
	if res.Request.ColumnsHeaders || len(res.Request.Columns) == 0 {
		return true
	}
	return false
}

// SetResultData populates Result table with data from the RawResultSet
func (res *Response) SetResultData() {
	res.Result = make(ResultSet, 0, len(res.RawResults.DataResult))
	rowSize := len(res.Request.RequestColumns)
	for i := range res.RawResults.DataResult {
		datarow := res.RawResults.DataResult[i]
		row := make([]interface{}, rowSize)
		for j := range res.Request.RequestColumns {
			row[j] = datarow.GetValueByColumn(res.Request.RequestColumns[j])
		}
		res.Result = append(res.Result, row)
	}
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
			LogErrors(peer.ResumeFromIdle())
		}(p, waitgroup)
	}
	waitTimeout(waitgroup, SpinUpPeersTimeout)
	log.Debugf("spin up completed")
}

// BuildLocalResponseData returns the result data for a given request
func (res *Response) BuildLocalResponseData(store *DataStore, resultcollector chan *PeerResponse) {
	ds := store.DataSet
	logWith(store.PeerName, res).Tracef("BuildLocalResponseData")

	// for some tables its faster to lock the table only once
	if store.PeerLockMode == PeerLockModeFull && ds != nil && ds.peer != nil {
		ds.peer.Lock.RLock()
		defer ds.peer.Lock.RUnlock()
	}

	if !store.Table.WorksUnlocked {
		ds.Lock.RLock()
		defer ds.Lock.RUnlock()
	}

	if len(store.Data) == 0 {
		return
	}

	if len(res.Request.Stats) > 0 {
		// stats queries
		res.MergeStats(res.gatherStatsResult(store))
	} else {
		// data queries
		res.gatherResultRows(store, resultcollector)
	}
}

func (res *Response) gatherResultRows(store *DataStore, resultcollector chan *PeerResponse) {
	result := &PeerResponse{}
	defer func() {
		resultcollector <- result
	}()
	req := res.Request

	// if there is no sort header or sort by name only,
	// we can drastically reduce the result set by applying the limit here already
	limit := req.optimizeResultLimit()
	if limit <= 0 {
		limit = len(store.Data) + 1
	}

	// no need to count all the way to the end unless the total number is required in wrapped_json output
	breakOnLimit := res.Request.OutputFormat != OutputFormatWrappedJSON

Rows:
	for _, row := range store.GetPreFilteredData(req.Filter) {
		result.RowsScanned++

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

func (res *Response) gatherStatsResult(store *DataStore) *ResultSetStats {
	result := NewResultSetStats()
	req := res.Request
	localStats := result.Stats

Rows:
	for _, row := range store.GetPreFilteredData(req.Filter) {
		result.RowsScanned++
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

		key := row.getStatsKey(res)
		stat := localStats[key]
		if stat == nil {
			stat = createLocalStatsCopy(req.Stats)
			localStats[key] = stat
		}

		// count stats
		if req.StatsGrouped == nil {
			row.CountStats(req.Stats, stat)
		} else {
			row.CountStats(req.StatsGrouped, stat)
		}
	}

	return result
}
