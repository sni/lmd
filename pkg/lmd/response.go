package lmd

import (
	"bytes"
	"context"
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
	// SpinUpPeersTimeout sets timeout to wait for peers after spin up.
	SpinUpPeersTimeout = 5 * time.Second

	// Number of processes rows after which the context is checked again.
	RowContextCheck = 10000
)

// Response contains the livestatus response data as long with some meta data.
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

// PeerResponse is the sub result from a peer before merged into the end result.
type PeerResponse struct {
	Rows        []*DataRow // set of datarows
	Total       int        // total number of matched rows regardless of any limits or offsets
	RowsScanned int        // total number of rows scanned to create result
}

// NewResponse creates a new response object for a given request
// It returns the Response object and any error encountered.
// unlockFn must be called whenever there is no error returned.
func NewResponse(ctx context.Context, req *Request, conn net.Conn) (res *Response, size int64, err error) {
	res = &Response{
		Code:    200,
		Failed:  req.BackendErrors,
		Request: req,
		Lock:    new(deadlock.RWMutex),
	}
	res.prepareResponse(ctx, req)

	// if all backends are down, send an error instead of an empty result
	if res.Request.OutputFormat != OutputFormatWrappedJSON && len(res.Failed) > 0 && len(res.Failed) == len(req.Backends) {
		res.Code = 502
		err = &PeerError{msg: res.Failed[req.Backends[0]], kind: ConnectionError}

		return res, 0, err
	}

	table := Objects.Tables[req.Table]

	switch {
	case len(res.SelectedPeers) == 0:
		// no backends selected, return empty result
		res.Result = make(ResultSet, 0)
	case table.PassthroughOnly:
		// passthrough requests, ex.: log table
		res.BuildPassThroughResult(ctx)
		res.PostProcessing()
	default:
		// normal requests

		if res.Request.WaitTrigger != "" {
			for i := range res.SelectedPeers {
				p := res.SelectedPeers[i]
				res.waitTrigger(ctx, p)
			}
		}

		// set locks for required stores
		stores := make(map[*Peer]*DataStore)
		for i := range res.SelectedPeers {
			peer := res.SelectedPeers[i]
			store, err2 := peer.GetDataStore(table.Name)
			if err2 != nil {
				res.Lock.Lock()
				res.Failed[peer.ID] = err2.Error()
				res.Lock.Unlock()

				continue
			}
			if !table.WorksUnlocked {
				store.DataSet.Lock.RLock()
			}
			stores[peer] = store
		}
		if !table.WorksUnlocked {
			defer func() {
				for _, s := range stores {
					s.DataSet.Lock.RUnlock()
				}
			}()
		}

		res.RawResults = &RawResultSet{}
		res.RawResults.Sort = req.Sort
		res.buildLocalResponse(ctx, stores)
		res.RawResults.PostProcessing(res)
	}

	res.CalculateFinalStats()

	if conn != nil {
		size, err = res.Send(conn)

		return nil, size, err
	}

	return res, 0, err
}

func (res *Response) prepareResponse(ctx context.Context, req *Request) {
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
		peer := req.lmd.PeerMap[id]
		if _, ok := req.BackendsMap[peer.ID]; !ok {
			continue
		}
		if req.lmd.nodeAccessor == nil || !req.lmd.nodeAccessor.IsOurBackend(peer.ID) {
			continue
		}
		if peer.HasFlag(MultiBackend) {
			continue
		}
		res.SelectedPeers = append(res.SelectedPeers, peer)

		// spin up required?
		if table.Virtual == nil {
			if idling, ok := peer.statusGetLocked(Idling).(bool); ok && idling {
				peer.statusSetLocked(LastQuery, currentUnixTime())
				spinUpPeers = append(spinUpPeers, peer)
			}
		}
	}
	req.lmd.PeerMapLock.RUnlock()

	// only use the first backend when requesting table or columns table
	if table.Name == TableTables || table.Name == TableColumns {
		res.SelectedPeers = []*Peer{req.lmd.PeerMap[req.lmd.PeerMapOrder[0]]}
	}

	if !table.PassthroughOnly && len(spinUpPeers) > 0 {
		SpinUpPeers(ctx, spinUpPeers)
	}
}

// Len returns the result length used for sorting results.
func (res *Response) Len() int {
	return len(res.Result)
}

// Less returns the sort result of two data rows.
func (res *Response) Less(idx1, idx2 int) bool {
	for k := range res.Request.Sort {
		field := res.Request.Sort[k]
		var sortType DataType
		if field.Group {
			sortType = StringCol
		} else {
			sortType = res.Request.RequestColumns[field.Index].DataType
		}
		switch sortType {
		case IntCol, Int64Col, FloatCol:
			valueA := interface2float64(res.Result[idx1][field.Index])
			valueB := interface2float64(res.Result[idx2][field.Index])
			if valueA == valueB {
				continue
			}
			if field.Direction == Asc {
				return valueA < valueB
			}

			return valueA > valueB
		case JSONCol, StringCol:
			index := field.Index
			if field.Group {
				index = 0
			}
			str1 := interface2stringNoDedup(res.Result[idx1][index])
			str2 := interface2stringNoDedup(res.Result[idx2][index])
			if str1 == str2 {
				continue
			}
			if field.Direction == Asc {
				return str1 < str2
			}

			return str1 > str2
		case StringListCol:
			// not implemented
			return field.Direction == Asc
		case Int64ListCol:
			// not implemented
			return field.Direction == Asc
		default:
			panic(fmt.Sprintf("sorting not implemented for type %s", sortType))
		}
	}

	return true
}

// Swap replaces two data rows while sorting.
func (res *Response) Swap(i, j int) {
	res.Result[i], res.Result[j] = res.Result[j], res.Result[i]
}

// ExpandRequestedBackends fills the requests backends map.
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

	for _, peerKey := range req.Backends {
		_, Ok := req.lmd.PeerMap[peerKey]
		if !Ok {
			req.BackendErrors[peerKey] = fmt.Sprintf("bad request: backend %s does not exist", peerKey)

			continue
		}
		req.BackendsMap[peerKey] = peerKey
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

// CalculateFinalStats calculates final averages and sums from stats queries.
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

	rowNum := 0
	for key, stats := range res.Request.StatsResult.Stats {
		rowSize := len(stats)
		rowSize += hasColumns
		res.Result[rowNum] = make([]interface{}, rowSize)
		if hasColumns > 0 {
			parts := strings.Split(key, ListSepChar1)
			for i := range parts {
				res.Result[rowNum][i] = &parts[i]
				if i >= hasColumns {
					break
				}
			}
		}
		for colNum := range stats {
			stat := stats[colNum]
			colNum += hasColumns

			res.Result[rowNum][colNum] = finalStatsApply(stat)

			if res.Request.SendStatsData {
				res.Result[rowNum][colNum] = []interface{}{stat.Stats, stat.StatsCount}

				continue
			}
		}
		rowNum++
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

func finalStatsApply(stat *Filter) (res float64) {
	switch stat.StatsType {
	case Counter:
		res = stat.Stats
	case Min:
		res = stat.Stats
	case Max:
		res = stat.Stats
	case Sum:
		res = stat.Stats
	case Average:
		if stat.StatsCount > 0 {
			res = stat.Stats / float64(stat.StatsCount)
		} else {
			res = 0
		}
	default:
		log.Panicf("not implemented")
	}
	if stat.StatsCount == 0 {
		res = 0
	}

	return
}

// Send converts the result object to a livestatus answer and writes the resulting bytes back to the client.
func (res *Response) Send(conn net.Conn) (size int64, err error) {
	if res.Request.ResponseFixed16 {
		size, err = res.SendFixed16(conn)
	} else {
		size, err = res.SendUnbuffered(conn)
	}

	localAddr := conn.LocalAddr().String()
	promFrontendBytesSend.WithLabelValues(localAddr).Add(float64(size + 1))

	return
}

// SendFixed16 converts the result object to a livestatus answer and writes the resulting bytes back to the client.
func (res *Response) SendFixed16(conn io.Writer) (size int64, err error) {
	resBuffer, err := res.Buffer()
	if err != nil {
		return 0, err
	}
	size = int64(resBuffer.Len())
	headerFixed16 := fmt.Sprintf("%d %11d", res.Code, size+1)
	logWith(res).Tracef("write: %s", headerFixed16)
	_, err = fmt.Fprintf(conn, "%s\n", headerFixed16)
	if err != nil {
		logWith(res).Warnf("write error: %s", err.Error())

		return 0, fmt.Errorf("write: %s", err.Error())
	}
	if log.IsV(LogVerbosityTrace) {
		logWith(res).Tracef("write: %s", resBuffer.Bytes())
	}
	written, err := resBuffer.WriteTo(conn)
	if err != nil {
		logWith(res).Warnf("write error: %s", err.Error())

		return 0, fmt.Errorf("writeTo: %s", err.Error())
	}
	if written != size {
		logWith(res).Warnf("write error: written %d, size: %d", written, size)

		return written, nil
	}

	_, err = conn.Write([]byte("\n"))
	if err != nil {
		logWith(res).Warnf("write error: %s", err.Error())

		return 0, fmt.Errorf("writeTo: %s", err.Error())
	}

	return written, nil
}

// SendUnbuffered directly prints the result to the client connection.
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

// Buffer fills buffer with the response as bytes array.
func (res *Response) Buffer() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	if res.Error != nil {
		logWith(res).Warnf("sending error response: %d - %s", res.Code, res.Error.Error())
		buf.WriteString(res.Error.Error())

		return buf, nil //nolint:nilerr // error has been forwared to the client but sending it worked
	}

	if res.Request.OutputFormat == OutputFormatWrappedJSON {
		return buf, res.WrappedJSON(buf)
	}

	return buf, res.JSON(buf)
}

// JSON converts the response into a json structure.
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
		return fmt.Errorf("json flush failed: %s", err.Error())
	}
	json.Reset(nil)

	return nil
}

// WrappedJSON converts the response into a wrapped json structure.
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

// WriteDataResponse writes the data part of the result.
func (res *Response) WriteDataResponse(json *jsoniter.Stream) {
	switch {
	case res.Result != nil:
		// append result row by row
		for rowNum := range res.Result {
			if rowNum > 0 {
				json.WriteRaw(",\n")
				json.Flush()
			}
			json.WriteArrayStart()
			for k := range res.Result[rowNum] {
				if k > 0 {
					json.WriteMore()
				}
				json.WriteVal(res.Result[rowNum][k])
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

// WriteDataResponseRowLocked appends each row but locks the peer before doing so. We don't have to lock for each column then.
func (res *Response) WriteDataResponseRowLocked(json *jsoniter.Stream) {
	for i := range res.RawResults.DataResult {
		if i > 0 {
			json.WriteRaw(",\n")
		}
		row := res.RawResults.DataResult[i]
		row.DataStore.Peer.lock.RLock()
		row.WriteJSON(json, res.Request.RequestColumns)
		row.DataStore.Peer.lock.RUnlock()
	}
}

// WriteColumnsResponse writes the columns header.
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

// buildLocalResponse builds local data table result for all selected peers.
func (res *Response) buildLocalResponse(ctx context.Context, stores map[*Peer]*DataStore) {
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
		peer := res.SelectedPeers[i]
		peer.statusSetLocked(LastQuery, currentUnixTime())

		store, ok := stores[peer]
		if !ok {
			continue
		}

		// process virtual tables serially without go routines to maintain the correct order, ex.: from the sites table
		if store.Table.Virtual != nil {
			res.buildLocalResponseData(ctx, store, resultcollector)

			continue
		}

		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)

			defer wg.Done()

			res.buildLocalResponseData(ctx, store, resultcollector)
		}(peer, waitgroup)
	}
	logWith(res).Tracef("waiting...")

	waitgroup.Wait()
	if resultcollector != nil {
		close(resultcollector)
		select {
		case <-waitChan:
		case <-ctx.Done():
		}
	}

	logWith(res).Tracef("waiting for all local data computations done")
}

// waitTrigger waits till all trigger are fulfilled.
func (res *Response) waitTrigger(ctx context.Context, peer *Peer) {
	// if a WaitTrigger is supplied, wait max ms till the condition is true
	if res.Request.WaitTrigger == "" {
		return
	}

	peer.WaitCondition(ctx, res.Request)

	// peer might have gone down meanwhile, ex. after waiting for a waittrigger, so check again
	_, err := peer.GetDataStore(res.Request.Table)
	if err != nil {
		res.Lock.Lock()
		res.Failed[peer.ID] = err.Error()
		res.Lock.Unlock()

		return
	}
}

// MergeStats merges stats result into final result set.
func (res *Response) MergeStats(stats *ResultSetStats) {
	if stats == nil {
		return
	}
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
func (res *Response) BuildPassThroughResult(ctx context.Context) {
	res.Result = make(ResultSet, 0)

	// build columns list
	backendColumns := []string{}
	virtualColumns := []*Column{}
	columnsIndex := make(map[*Column]int)
	for colNum := range res.Request.RequestColumns {
		col := res.Request.RequestColumns[colNum]
		if col.StorageType == VirtualStore {
			virtualColumns = append(virtualColumns, col)
		} else {
			backendColumns = append(backendColumns, col.Name)
		}
		columnsIndex[col] = colNum
	}
	for i := range res.Request.Sort {
		field := res.Request.Sort[i]
		if j, ok := columnsIndex[field.Column]; ok {
			// sort column does exist in the request columns
			field.Index = j
		} else {
			field.Index = len(backendColumns) + len(virtualColumns)
			if field.Column.StorageType == VirtualStore {
				virtualColumns = append(virtualColumns, field.Column)
			} else {
				backendColumns = append(backendColumns, field.Column.Name)
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
		peer := res.SelectedPeers[i]

		if !peer.isOnline() {
			res.Lock.Lock()
			res.Failed[peer.ID] = fmt.Sprintf("%v", peer.statusGetLocked(LastError))
			res.Lock.Unlock()

			continue
		}

		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)

			logWith(peer, passthroughRequest).Debugf("starting passthrough request")
			defer wg.Done()

			peer.PassThroughQuery(ctx, res, passthroughRequest, virtualColumns, columnsIndex)
		}(peer, waitgroup)
	}
	logWith(passthroughRequest).Tracef("waiting...")
	waitgroup.Wait()
	logWith(passthroughRequest).Debugf("waiting for passed through requests done")
}

// SendColumnsHeader determines if the response should contain the columns header.
func (res *Response) SendColumnsHeader() bool {
	if len(res.Request.Stats) > 0 {
		return false
	}
	if res.Request.ColumnsHeaders || len(res.Request.Columns) == 0 {
		return true
	}

	return false
}

// SetResultData populates Result table with data from the RawResultSet.
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
func SpinUpPeers(ctx context.Context, peers []*Peer) {
	waitgroup := &sync.WaitGroup{}
	for i := range peers {
		peer := peers[i]
		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)
			defer wg.Done()
			LogErrors(peer.ResumeFromIdle(ctx))
		}(peer, waitgroup)
	}
	waitTimeout(ctx, waitgroup, SpinUpPeersTimeout)
	log.Debugf("spin up completed")
}

// buildLocalResponseData returns the result data for a given request.
func (res *Response) buildLocalResponseData(ctx context.Context, store *DataStore, resultcollector chan *PeerResponse) {
	logWith(store.PeerName, res).Tracef("BuildLocalResponseData")

	if len(store.Data) == 0 {
		return
	}

	// for some tables its faster to lock the table only once
	ds := store.DataSet
	if store.PeerLockMode == PeerLockModeFull && ds != nil && ds.peer != nil {
		ds.peer.lock.RLock()
		defer ds.peer.lock.RUnlock()
	}

	if len(res.Request.Stats) > 0 {
		// stats queries
		res.MergeStats(res.gatherStatsResult(ctx, store))
	} else {
		// data queries
		res.gatherResultRows(ctx, store, resultcollector)
	}
}

func (res *Response) gatherResultRows(ctx context.Context, store *DataStore, resultcollector chan *PeerResponse) {
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

	done := ctx.Done()
Rows:
	for i, row := range store.GetPreFilteredData(req.Filter) {
		// only check every couple of rows
		if i%RowContextCheck == 0 {
			select {
			case <-done:
				// request canceled
				return
			default:
			}
		}

		result.RowsScanned++

		// does our filter match?
		for _, f := range req.Filter {
			if !row.MatchFilter(f, false) {
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

func (res *Response) gatherStatsResult(ctx context.Context, store *DataStore) *ResultSetStats {
	result := NewResultSetStats()
	req := res.Request
	localStats := result.Stats

	done := ctx.Done()
Rows:
	for i, row := range store.GetPreFilteredData(req.Filter) {
		// only check every couple of rows
		if i%RowContextCheck == 0 {
			select {
			case <-done:
				// request canceled
				return nil
			default:
			}
		}
		result.RowsScanned++
		// does our filter match?
		for _, f := range req.Filter {
			if !row.MatchFilter(f, false) {
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
