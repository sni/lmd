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
	Failed        map[string]string
	SelectedPeers []*Peer
}

// PeerResponse is the sub result from a peer before merged into the end result
type PeerResponse struct {
	Rows  []*DataRow // set of datarows
	Total int        // total number of matched rows regardless of any limits or offsets
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
	PeerMapLock.RLock()
	for _, id := range PeerMapOrder {
		p := PeerMap[id]
		if _, ok := req.BackendsMap[p.ID]; !ok {
			continue
		}
		if !nodeAccessor.IsOurBackend(p.ID) {
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
	PeerMapLock.RUnlock()

	// only use the first backend when requesting table or columns table
	if table.Name == TableTables || table.Name == TableColumns {
		res.SelectedPeers = []*Peer{PeerMap[PeerMapOrder[0]]}
	}

	if !table.PassthroughOnly && len(spinUpPeers) > 0 {
		logDebugError(SpinUpPeers(spinUpPeers))
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
		res.RawResults.Sort = &req.Sort
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
		case StringCol:
			index := s.Index
			if s.Group {
				index = 0
			}
			s1 := *interface2stringNoDedup(res.Result[i][index])
			s2 := *interface2stringNoDedup(res.Result[j][index])
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
		case HashMapCol:
			s1 := interface2hashmap(res.Result[i][s.Index])[s.Args]
			s2 := interface2hashmap(res.Result[j][s.Index])[s.Args]
			if s1 == s2 {
				continue
			}
			if s.Direction == Asc {
				return s1 < s2
			}
			return s1 > s2
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
	if len(res.Request.Stats) > 0 {
		return
	}
	log.Tracef("PostProcessing")
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
			log.Debugf("sorting result took %s", duration.String())
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
	hasColumns := len(res.Request.Columns)
	if hasColumns == 0 && len(res.Request.StatsResult) == 0 {
		if res.Request.StatsResult == nil {
			res.Request.StatsResult = make(ResultSetStats)
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
	}

	if hasColumns > 0 {
		// Sort by stats key
		res.Request.Sort = make([]*SortField, 0, hasColumns)
		for i := range res.Request.Columns {
			res.Request.Sort = append(res.Request.Sort, &SortField{Index: i, Group: true, Direction: Asc})
		}
		sort.Sort(res)
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
func (res *Response) Send(c net.Conn) (size int64, err error) {
	resBuffer, err := res.Buffer()
	if err != nil {
		return
	}
	size = int64(resBuffer.Len()) + 1
	if res.Request.ResponseFixed16 {
		if log.IsV(LogVerbosityTrace) {
			log.Tracef("write: %s", fmt.Sprintf("%d %11d", res.Code, size))
		}
		_, err = fmt.Fprintf(c, "%d %11d\n", res.Code, size)
		if err != nil {
			log.Warnf("write error: %s", err.Error())
			return
		}
	}
	if log.IsV(LogVerbosityTrace) {
		log.Tracef("write: %s", resBuffer.Bytes())
	}
	written, err := resBuffer.WriteTo(c)
	if err != nil {
		log.Warnf("write error: %s", err.Error())
		return
	}
	if written != size-1 {
		log.Warnf("write error: written %d, size: %d", written, size)
		return
	}
	localAddr := c.LocalAddr().String()
	promFrontendBytesSend.WithLabelValues(localAddr).Add(float64(size))
	_, err = c.Write([]byte("\n"))
	return
}

// Buffer fills buffer with the response as bytes array
func (res *Response) Buffer() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	if res.Error != nil {
		log.Warnf("sending error response: %d - %s", res.Code, res.Error.Error())
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
	return err
}

// WrappedJSON converts the response into a wrapped json structure
func (res *Response) WrappedJSON(buf io.Writer) error {
	json := jsoniter.ConfigCompatibleWithStandardLibrary.BorrowStream(buf)
	defer jsoniter.ConfigCompatibleWithStandardLibrary.ReturnStream(json)

	json.WriteRaw("{\"data\":\n[")
	res.WriteDataResponse(json)
	json.WriteRaw("]\n,\"failed\":")
	json.WriteVal(res.Failed)

	// add optional columns header as first row
	if res.SendColumnsHeader() {
		json.WriteRaw("\n,\"columns\":")
		res.WriteColumnsResponse(json)
	}

	json.WriteRaw(fmt.Sprintf("\n,\"total_count\":%d}", res.ResultTotal))
	err := json.Flush()
	return err
}

// WriteDataResponse writes the data part of the result
func (res *Response) WriteDataResponse(json *jsoniter.Stream) {
	if res.Result != nil {
		// append result row by row
		for i := range res.Result {
			if i > 0 {
				json.WriteRaw(",")
			}
			json.WriteVal(res.Result[i])
		}
	} else {
		// unprocessed result?
		res.ResultTotal = res.RawResults.Total

		// PeerLockModeFull means we have to lock all peers before creating the result
		if len(res.RawResults.DataResult) > 0 && res.RawResults.DataResult[0].DataStore.PeerLockMode == PeerLockModeFull {
			res.WriteDataResponseRowLocked(json)
			return
		}

		for i := range res.RawResults.DataResult {
			if i > 0 {
				json.WriteRaw(",")
			}
			res.RawResults.DataResult[i].WriteJSON(json, &res.Request.RequestColumns)
		}
	}
}

// WriteDataResponseRowLocked appends each row but locks the peer befor doing so. We don't have to lock for each column then
func (res *Response) WriteDataResponseRowLocked(json *jsoniter.Stream) {
	for i := range res.RawResults.DataResult {
		if i > 0 {
			json.WriteRaw(",")
		}
		row := res.RawResults.DataResult[i]
		row.DataStore.Peer.PeerLock.RLock()
		row.WriteJSON(json, &res.Request.RequestColumns)
		row.DataStore.Peer.PeerLock.RUnlock()
	}
}

// WriteColumnsResponse writes the columns header
func (res *Response) WriteColumnsResponse(json *jsoniter.Stream) {
	cols := make([]interface{}, len(res.Request.RequestColumns)+len(res.Request.Stats))
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
	json.WriteVal(cols)
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
			p.BuildLocalResponseData(res, store, resultcollector)
			continue
		}

		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)

			log.Tracef("[%s] starting local data computation", peer.Name)
			defer wg.Done()

			peer.BuildLocalResponseData(res, store, resultcollector)
		}(p, waitgroup)
	}
	log.Tracef("waiting...")

	waitgroup.Wait()
	if resultcollector != nil {
		close(resultcollector)
		<-waitChan
	}

	log.Tracef("waiting for all local data computations done")
}

// MergeStats merges stats result into final result set
func (res *Response) MergeStats(stats *ResultSetStats) {
	res.Lock.Lock()
	defer res.Lock.Unlock()
	if res.Request.StatsResult == nil {
		res.Request.StatsResult = make(ResultSetStats)
	}
	// apply stats querys
	for key, stats := range *stats {
		if _, ok := res.Request.StatsResult[key]; !ok {
			res.Request.StatsResult[key] = stats
		} else {
			for i := range stats {
				s := stats[i]
				res.Request.StatsResult[key][i].ApplyValue(s.Stats, s.StatsCount)
			}
		}
	}
}

// BuildPassThroughResult passes a query transparently to one or more remote sites and builds the response
// from that.
func (res *Response) BuildPassThroughResult() {
	res.Result = make(ResultSet, 0)

	// build columns list
	backendColumns := []string{}
	virtColumns := []*Column{}
	columnsIndex := make(map[*Column]int)
	for i := range res.Request.RequestColumns {
		col := res.Request.RequestColumns[i]
		if col.StorageType == VirtStore {
			virtColumns = append(virtColumns, col)
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
			s.Index = len(backendColumns) + len(virtColumns)
			if s.Column.StorageType == VirtStore {
				virtColumns = append(virtColumns, s.Column)
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

			log.Debugf("[%s] starting passthrough request", peer.Name)
			defer wg.Done()

			peer.PassThrougQuery(res, passthroughRequest, virtColumns, columnsIndex)
		}(p, waitgroup)
	}
	log.Tracef("waiting...")
	waitgroup.Wait()
	log.Debugf("waiting for passed through requests done")
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
