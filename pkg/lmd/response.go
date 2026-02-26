package lmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	// SpinUpPeersTimeout sets timeout to wait for peers after spin up.
	SpinUpPeersTimeout = 5 * time.Second

	// Number of processes rows after which the context is checked again.
	RowContextCheck = 10000
)

// Response contains the livestatus response data as long with some meta data.
type Response struct {
	noCopy         noCopy
	err            error             // error object if the query was not successful
	lock           *RWMutex          // must be used for Result and Failed access
	request        *Request          // the initial request
	rawResults     *RawResultSet     // collected results from peers
	lockedStores   []*DataStore      // list of locked stores
	affectedTables []TableName       // list of affected tables
	failed         map[string]string // map of failed backends by key
	result         ResultSet         // final processed result table
	selectedPeers  []*Peer           // peers used for this response
	code           int               // 200 if the query was successful
	resultTotal    int
	rowsScanned    int // total number of data rows scanned for this result
}

// PeerResponse is the sub result from a peer before merged into the end result.
type PeerResponse struct {
	rows        []*DataRow // set of data rows
	total       int        // total number of matched rows regardless of any limits or offsets
	rowsScanned int        // total number of rows scanned to create result
}

// NewResponse creates a new response object for a given request
// It returns the Response object and any error encountered.
func NewResponse(ctx context.Context, req *Request, client *ClientConnection) (res *Response, size int64, err error) {
	name := "unknown"
	if client != nil {
		name = client.remoteAddr
	}
	res = &Response{
		code:    200,
		failed:  req.BackendErrors,
		request: req,
		lock:    NewRWMutex("response" + name),
	}
	res.prepareResponse(ctx, req)

	// if all backends are down, send an error instead of an empty result
	if res.request.OutputFormat != OutputFormatWrappedJSON && len(res.failed) > 0 && len(res.failed) == len(req.Backends) {
		res.code = 502
		err = &PeerError{msg: res.failed[req.Backends[0]], kind: ConnectionError}

		return res, 0, err
	}

	table := Objects.Tables[req.Table]

	switch {
	case len(res.selectedPeers) == 0:
		// no backends selected, return empty result
		res.result = make(ResultSet, 0)
	case table.passthroughOnly:
		// passthrough requests, ex.: log table
		res.BuildPassThroughResult(ctx)
		res.PostProcessing()
	default:
		// normal requests
		if res.request.WaitTrigger != "" {
			for i := range res.selectedPeers {
				p := res.selectedPeers[i]
				res.waitTrigger(ctx, p)
			}
		}

		res.affectedTables = res.getAffectedTables(table)

		// set locks for required stores
		res.lockedStores = make([]*DataStore, 0, len(res.selectedPeers)*len(res.affectedTables))
		stores := make(map[*Peer]*DataStore)
		for i := range res.selectedPeers {
			peer := res.selectedPeers[i]
			store, err2 := peer.getDataStore(table.name)
			if err2 != nil {
				res.lock.Lock()
				res.failed[peer.ID] = err2.Error()
				res.lock.Unlock()

				continue
			}
			if !table.worksUnlocked {
				res.lockStores(store.dataSet)
			}
			stores[peer] = store
		}

		defer res.unlockStores()

		res.rawResults = &RawResultSet{}
		res.rawResults.Sort = req.Sort
		res.buildLocalResponse(ctx, stores)
		res.rawResults.PostProcessing(res)
	}

	res.CalculateFinalStats()

	if client != nil {
		size, err = res.Send(client)

		return nil, size, err
	}

	return res, 0, err
}

func (res *Response) lockStores(data *DataStoreSet) {
	for _, tableName := range res.affectedTables {
		table := Objects.Tables[tableName]
		if table.virtual != nil {
			continue
		}
		store := data.get(tableName)
		if store != nil {
			store.lock.RLock()
			res.lockedStores = append(res.lockedStores, store)
		}
	}
}

func (res *Response) unlockStores() {
	if res.lockedStores == nil {
		return
	}

	for _, s := range res.lockedStores {
		s.lock.RUnlock()
	}

	res.lockedStores = nil
}

func (res *Response) getAffectedTables(reqTable *Table) []TableName {
	if len(reqTable.refTables) == 0 {
		return ([]TableName{reqTable.name})
	}

	uniq := map[TableName]bool{
		res.request.Table: true,
	}

	for _, col := range res.request.RequestColumns {
		if col.StorageType == RefStore {
			uniq[col.RefCol.Table.name] = true
		}
	}

	tables := slices.Collect(maps.Keys(uniq))

	// sort tables by id, otherwise race condition will detect random ordered locks
	slices.Sort(tables)

	return tables
}

func (res *Response) prepareResponse(ctx context.Context, req *Request) {
	if res.failed == nil {
		res.failed = make(map[string]string)
	}

	table := Objects.Tables[req.Table]

	// check if we have to spin up updates, if so, do it parallel
	res.selectedPeers = make([]*Peer, 0)
	spinUpPeers := make([]*Peer, 0)
	// iterate over PeerMap instead of BackendsMap to retain backend order
	for _, peer := range req.lmd.peerMap.Peers() {
		if _, ok := req.BackendsMap[peer.ID]; !ok {
			continue
		}
		if req.lmd.nodeAccessor == nil || !req.lmd.nodeAccessor.IsOurBackend(peer.ID) {
			continue
		}
		if peer.hasFlag(MultiBackend) {
			continue
		}
		res.selectedPeers = append(res.selectedPeers, peer)

		// spin up required?
		if table.virtual == nil {
			if peer.idling.Load() {
				peer.lastQuery.Set(currentUnixTime())
				spinUpPeers = append(spinUpPeers, peer)
			}
		}
	}

	// only use the first backend when requesting table or columns table
	if table.name == TableTables || table.name == TableColumns {
		res.selectedPeers = []*Peer{req.lmd.peerMap.Peers()[0]}
	}

	if !table.passthroughOnly && len(spinUpPeers) > 0 {
		SpinUpPeers(ctx, spinUpPeers)
	}
}

// Len returns the result length used for sorting results.
func (res *Response) Len() int {
	return len(res.result)
}

// Less returns the sort result of two data rows.
func (res *Response) Less(idx1, idx2 int) bool {
	for k := range res.request.Sort {
		field := res.request.Sort[k]
		var sortType DataType
		if field.Group {
			sortType = StringCol
		} else {
			sortType = res.request.RequestColumns[field.Index].DataType
		}
		switch sortType {
		case IntCol, Int64Col, FloatCol:
			valueA := interface2float64(res.result[idx1][field.Index])
			valueB := interface2float64(res.result[idx2][field.Index])
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
			str1 := interface2stringNoDedup(res.result[idx1][index])
			str2 := interface2stringNoDedup(res.result[idx2][index])
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
	res.result[i], res.result[j] = res.result[j], res.result[i]
}

// ExpandRequestedBackends fills the requests backends map.
func (req *Request) ExpandRequestedBackends() {
	req.BackendsMap = make(map[string]string)
	req.BackendErrors = make(map[string]string)

	// no backends selected means all backends
	if len(req.Backends) == 0 {
		for _, peer := range req.lmd.peerMap.Peers() {
			req.BackendsMap[peer.ID] = peer.ID
		}

		return
	}

	peerMap := req.lmd.peerMap.Refs()
	for _, peerKey := range req.Backends {
		_, ok := peerMap[peerKey]
		if !ok {
			req.BackendErrors[peerKey] = fmt.Sprintf("bad request: backend %s does not exist", peerKey)

			continue
		}
		req.BackendsMap[peerKey] = peerKey
	}
}

// PostProcessing does all the post processing required for a request like sorting
// and cutting of limits, applying offsets and calculating final stats.
func (res *Response) PostProcessing() {
	if len(res.request.Stats) > 0 {
		return
	}
	logWith(res).Tracef("PostProcessing")
	if res.result == nil {
		res.result = make(ResultSet, 0)
	}
	// sort our result
	if len(res.request.Sort) > 0 {
		// skip sorting if there is only one backend requested and we want the default sort order
		if len(res.request.BackendsMap) >= 1 || !res.request.IsDefaultSortOrder() {
			t1 := time.Now()
			sort.Sort(res)
			duration := time.Since(t1)
			logWith(res).Debugf("sorting result took %s", duration.String())
		}
	}

	if res.resultTotal == 0 {
		res.resultTotal = len(res.result)
	}

	// apply request offset
	if res.request.Offset > 0 {
		if res.request.Offset > res.resultTotal {
			res.result = make(ResultSet, 0)
		} else {
			res.result = res.result[res.request.Offset:]
		}
	}

	// apply request limit
	if res.request.Limit != nil && *res.request.Limit >= 0 && *res.request.Limit < len(res.result) {
		res.result = res.result[0:*res.request.Limit]
	}
}

// CalculateFinalStats calculates final averages and sums from stats queries.
func (res *Response) CalculateFinalStats() {
	if len(res.request.Stats) == 0 {
		return
	}
	if res.request.StatsResult == nil {
		res.request.StatsResult = NewResultSetStats()
	}
	hasColumns := len(res.request.Columns)
	if hasColumns == 0 && len(res.request.StatsResult.Stats) == 0 {
		res.request.StatsResult.Stats[""] = createLocalStatsCopy(res.request.Stats)
	}
	res.result = make(ResultSet, len(res.request.StatsResult.Stats))

	rowNum := 0
	for key, stats := range res.request.StatsResult.Stats {
		rowSize := len(stats)
		rowSize += hasColumns
		res.result[rowNum] = make([]any, rowSize)
		if hasColumns > 0 {
			parts := strings.Split(key, ListSepChar1)
			for i := range parts {
				res.result[rowNum][i] = &parts[i]
				if i >= hasColumns {
					break
				}
			}
		}
		for colNum := range stats {
			stat := stats[colNum]
			colNum += hasColumns

			res.result[rowNum][colNum] = finalStatsApply(stat)

			if res.request.SendStatsData {
				res.result[rowNum][colNum] = []any{stat.stats, stat.statsCount}

				continue
			}
		}
		rowNum++
		res.rowsScanned += res.request.StatsResult.RowsScanned
	}

	if hasColumns > 0 {
		// Sort by stats key
		res.request.Sort = make([]*SortField, 0, hasColumns)
		for i := range res.request.Columns {
			res.request.Sort = append(res.request.Sort, &SortField{Index: i, Group: true, Direction: Asc})
		}
		sort.Sort(res)
	}
	res.resultTotal += len(res.result)
}

func finalStatsApply(stat *Filter) (res float64) {
	switch stat.statsType {
	case Counter:
		res = stat.stats
	case Min:
		res = stat.stats
	case Max:
		res = stat.stats
	case Sum:
		res = stat.stats
	case Average:
		if stat.statsCount > 0 {
			res = stat.stats / float64(stat.statsCount)
		} else {
			res = 0
		}
	default:
		log.Panicf("not implemented")
	}
	if stat.statsCount == 0 {
		res = 0
	}

	return res
}

// Send converts the result object to a livestatus answer and writes the resulting bytes back to the client.
func (res *Response) Send(client *ClientConnection) (size int64, err error) {
	size, err = res.send(client.connection)

	localAddr := client.connection.LocalAddr().String()
	promFrontendBytesSend.WithLabelValues(localAddr).Add(float64(size + 1))

	return size, err
}

// send converts the result object to a livestatus answer and writes the resulting bytes back to the client.
func (res *Response) send(conn io.Writer) (size int64, err error) {
	resBuffer, err := res.Buffer()
	if err != nil {
		return 0, err
	}
	size = int64(resBuffer.Len())
	if res.request.ResponseFixed16 {
		headerFixed16 := fmt.Sprintf("%d %11d", res.code, size+1)
		logWith(res).Tracef("write: %s", headerFixed16)
		_, err = fmt.Fprintf(conn, "%s\n", headerFixed16)
		if err != nil {
			logWith(res).Warnf("write error: %s", err.Error())

			return 0, fmt.Errorf("write: %s", err.Error())
		}
	}
	if log.IsV(LogVerbosityTrace) {
		logWith(res).Tracef("write: %s", resBuffer.Bytes())
	}
	written, err := resBuffer.WriteTo(conn)
	if err != nil {
		logWith(res).Warnf("write error: %s", err.Error())

		return written, fmt.Errorf("writeTo: %s", err.Error())
	}
	if written != size {
		logWith(res).Warnf("write error: written %d, size: %d", written, size)

		return written, nil
	}

	_, err = conn.Write([]byte("\n"))
	if err != nil {
		logWith(res).Warnf("write error: %s", err.Error())

		return written, fmt.Errorf("writeTo: %s", err.Error())
	}

	return written, nil
}

// Buffer fills buffer with the response as bytes array.
func (res *Response) Buffer() (*bytes.Buffer, error) {
	// we can unlock all stores as soon this function is over
	defer res.unlockStores()

	buf := new(bytes.Buffer)
	if res.err != nil {
		logWith(res).Warnf("sending error response: %d - %s", res.code, res.err.Error())
		buf.WriteString(res.err.Error())

		return buf, nil
	}

	if res.request.OutputFormat == OutputFormatWrappedJSON {
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
		if len(res.result) > 0 || (res.rawResults != nil && len(res.rawResults.DataResult) > 0) {
			json.WriteRaw(",")
		}
	}

	if err := res.WriteDataResponse(json); err != nil {
		return err
	}

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
	if err := res.WriteDataResponse(json); err != nil {
		return err
	}
	json.WriteRaw("]\n,\"failed\": {")
	num := 0
	for k, v := range res.failed {
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

	json.WriteRaw(fmt.Sprintf("\n,\"rows_scanned\":%d", res.rowsScanned))
	json.WriteRaw(fmt.Sprintf("\n,\"total_count\":%d}", res.resultTotal))
	err := json.Flush()
	if err != nil {
		return fmt.Errorf("WrappedJSON: %w", err)
	}
	json.Reset(nil)

	return nil
}

// WriteDataResponse writes the data part of the result.
func (res *Response) WriteDataResponse(json *jsoniter.Stream) error {
	switch {
	case res.result != nil:
		// append result row by row
		for rowNum := range res.result {
			if rowNum > 0 {
				json.WriteRaw(",\n")
				if err := json.Flush(); err != nil {
					return fmt.Errorf("json flush failed: %s", err.Error())
				}
			}
			json.WriteArrayStart()
			for k := range res.result[rowNum] {
				if k > 0 {
					json.WriteMore()
				}
				json.WriteVal(res.result[rowNum][k])
			}
			json.WriteArrayEnd()
		}
	case res.rawResults != nil:
		// unprocessed result?
		res.resultTotal = res.rawResults.Total
		res.rowsScanned = res.rawResults.RowsScanned

		for i, row := range res.rawResults.DataResult {
			if i > 0 {
				json.WriteRaw(",\n")
				if err := json.Flush(); err != nil {
					return fmt.Errorf("json flush failed: %s", err.Error())
				}
			}
			row.WriteJSON(json, res.request.RequestColumns)
		}
	default:
		logWith(res).Errorf("response contains no result at all")
	}

	return nil
}

// WriteColumnsResponse writes the columns header.
func (res *Response) WriteColumnsResponse(json *jsoniter.Stream) {
	cols := make([]string, len(res.request.RequestColumns)+len(res.request.Stats))
	for k := range len(res.request.RequestColumns) {
		if k < len(res.request.Columns) {
			cols[k] = res.request.Columns[k]
		} else {
			cols[k] = res.request.RequestColumns[k].Name
		}
	}
	for i := range res.request.Stats {
		index := i + len(res.request.RequestColumns)
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
	var resultCollector chan *PeerResponse
	waitChan := make(chan bool)
	defer func() {
		close(waitChan)
	}()
	if len(res.request.Stats) == 0 {
		resultCollector = make(chan *PeerResponse, len(res.selectedPeers))
		go func() {
			result := res.rawResults
			for subRes := range resultCollector {
				result.Total += subRes.total
				result.RowsScanned += subRes.rowsScanned
				result.DataResult = append(result.DataResult, subRes.rows...)
			}
			waitChan <- true
		}()
	}

	waitgroup := &sync.WaitGroup{}
	for i := range res.selectedPeers {
		peer := res.selectedPeers[i]
		peer.lastQuery.Set(currentUnixTime())

		store, ok := stores[peer]
		if !ok {
			continue
		}

		// process virtual tables serially without go routines to maintain the correct order, ex.: from the sites table
		if store.table.virtual != nil {
			res.buildLocalResponseData(ctx, store, resultCollector)

			continue
		}

		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)

			defer wg.Done()

			res.buildLocalResponseData(ctx, store, resultCollector)
		}(peer, waitgroup)
	}
	logWith(res).Tracef("waiting...")

	waitgroup.Wait()
	if resultCollector != nil {
		close(resultCollector)
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
	if res.request.WaitTrigger == "" {
		return
	}

	peer.WaitCondition(ctx, res.request)

	// peer might have gone down meanwhile, ex. after waiting for a waittrigger, so check again
	_, err := peer.getDataStore(res.request.Table)
	if err != nil {
		res.lock.Lock()
		res.failed[peer.ID] = err.Error()
		res.lock.Unlock()

		return
	}
}

// MergeStats merges stats result into final result set.
func (res *Response) MergeStats(stats *ResultSetStats) {
	if stats == nil {
		return
	}
	res.lock.Lock()
	defer res.lock.Unlock()
	if res.request.StatsResult == nil {
		res.request.StatsResult = NewResultSetStats()
	}
	// apply stats queries
	for key, stats := range stats.Stats {
		if _, ok := res.request.StatsResult.Stats[key]; !ok {
			res.request.StatsResult.Stats[key] = stats
		} else {
			for i := range stats {
				s := stats[i]
				res.request.StatsResult.Stats[key][i].ApplyValue(s.stats, s.statsCount)
			}
		}
	}
	res.request.StatsResult.Total += stats.Total
	res.request.StatsResult.RowsScanned += stats.RowsScanned
}

// BuildPassThroughResult passes a query transparently to one or more remote sites and builds the response
// from that.
func (res *Response) BuildPassThroughResult(ctx context.Context) {
	res.result = make(ResultSet, 0)

	// build columns list
	backendColumns := []string{}
	virtualColumns := []*Column{}
	columnsIndex := make(map[*Column]int)
	for colNum := range res.request.RequestColumns {
		col := res.request.RequestColumns[colNum]
		if col.StorageType == VirtualStore {
			virtualColumns = append(virtualColumns, col)
		} else {
			backendColumns = append(backendColumns, col.Name)
		}
		columnsIndex[col] = colNum
	}
	for i := range res.request.Sort {
		field := res.request.Sort[i]
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
	req := res.request
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

	for i := range res.selectedPeers {
		peer := res.selectedPeers[i]

		if !peer.isOnline() {
			res.lock.Lock()
			res.failed[peer.ID] = peer.lastError.Get()
			res.lock.Unlock()

			continue
		}

		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)

			logWith(peer, passthroughRequest).Debugf("starting passthrough request")
			defer wg.Done()

			peer.passThroughQuery(ctx, res, passthroughRequest, virtualColumns, columnsIndex)
		}(peer, waitgroup)
	}
	logWith(passthroughRequest).Tracef("waiting...")
	waitgroup.Wait()
	logWith(passthroughRequest).Debugf("waiting for passed through requests done")
}

// SendColumnsHeader determines if the response should contain the columns header.
func (res *Response) SendColumnsHeader() bool {
	if len(res.request.Stats) > 0 {
		return false
	}
	if res.request.ColumnsHeaders || len(res.request.Columns) == 0 {
		return true
	}

	return false
}

// SetResultData populates Result table with data from the RawResultSet.
func (res *Response) SetResultData() {
	res.result = make(ResultSet, 0, len(res.rawResults.DataResult))
	rowSize := len(res.request.RequestColumns)
	for i := range res.rawResults.DataResult {
		dataRow := res.rawResults.DataResult[i]
		row := make([]any, rowSize)
		for j := range res.request.RequestColumns {
			row[j] = dataRow.GetValueByColumn(res.request.RequestColumns[j])
		}
		res.result = append(res.result, row)
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
			LogErrors(peer.resumeFromIdle(ctx))
		}(peer, waitgroup)
	}
	waitTimeout(ctx, waitgroup, SpinUpPeersTimeout)
	log.Debugf("spin up completed")
}

// buildLocalResponseData returns the result data for a given request.
func (res *Response) buildLocalResponseData(ctx context.Context, store *DataStore, resultCollector chan *PeerResponse) {
	logWith(store.peer, res).Tracef("BuildLocalResponseData")

	if len(store.data) == 0 {
		return
	}

	if len(res.request.Stats) > 0 {
		// stats queries
		res.MergeStats(res.gatherStatsResult(ctx, store))
	} else {
		// data queries
		res.gatherResultRows(ctx, store, resultCollector)
	}
}

func (res *Response) gatherResultRows(ctx context.Context, store *DataStore, resultCollector chan *PeerResponse) {
	result := &PeerResponse{}
	defer func() {
		resultCollector <- result
	}()
	req := res.request

	// if there is no sort header or sort by name only,
	// we can drastically reduce the result set by applying the limit here already
	limit := req.optimizeResultLimit()
	if limit <= 0 {
		limit = len(store.data) + 1
	}

	// no need to count all the way to the end unless the total number is required in wrapped_json output
	breakOnLimit := res.request.OutputFormat != OutputFormatWrappedJSON

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

		result.rowsScanned++

		// does our filter match?
		for _, f := range req.Filter {
			if !row.MatchFilter(f, false) {
				continue Rows
			}
		}

		if !row.checkAuth(req.AuthUser) {
			continue Rows
		}

		result.total++

		// check if we have enough result rows already
		// we still need to count how many result we would have...
		if result.total > limit {
			if breakOnLimit {
				return
			}

			continue Rows
		}
		result.rows = append(result.rows, row)
	}
}

func (res *Response) gatherStatsResult(ctx context.Context, store *DataStore) *ResultSetStats {
	result := NewResultSetStats()
	req := res.request
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
