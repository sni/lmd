package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ResultColumn is a column container for results
type ResultColumn struct {
	Name   string
	Type   ColumnType
	Column *Column // reference to the real column
	Index  int     // index in the request
	Hidden bool    // true if column is not used in the output and ex. just for sorting
}

// VirtKeyMapTupel is used to define the virtual key mapping in the VirtKeyMap
type VirtKeyMapTupel struct {
	Index int
	Key   string
	Type  ColumnType
}

// VirtKeyMap maps the virtual columns with the peer status map entry.
// If the entry is empty, then there must be a corresponding resolve function in the GetRowValue() function.
var VirtKeyMap = map[string]VirtKeyMapTupel{
	"key":                     {Index: -1, Key: "PeerKey", Type: StringCol},
	"name":                    {Index: -2, Key: "PeerName", Type: StringCol},
	"addr":                    {Index: -4, Key: "PeerAddr", Type: StringCol},
	"status":                  {Index: -5, Key: "PeerStatus", Type: IntCol},
	"bytes_send":              {Index: -6, Key: "BytesSend", Type: IntCol},
	"bytes_received":          {Index: -7, Key: "BytesReceived", Type: IntCol},
	"queries":                 {Index: -8, Key: "Querys", Type: IntCol},
	"last_error":              {Index: -9, Key: "LastError", Type: StringCol},
	"last_online":             {Index: -10, Key: "LastOnline", Type: TimeCol},
	"last_update":             {Index: -11, Key: "LastUpdate", Type: TimeCol},
	"response_time":           {Index: -12, Key: "ReponseTime", Type: FloatCol},
	"state_order":             {Index: -13, Key: "", Type: IntCol},
	"last_state_change_order": {Index: -14, Key: "", Type: IntCol},
	"has_long_plugin_output":  {Index: -15, Key: "", Type: IntCol},
	"idling":                  {Index: -16, Key: "Idling", Type: IntCol},
	"last_query":              {Index: -17, Key: "LastQuery", Type: TimeCol},
	"lmd_last_cache_update":   {Index: -18, Key: "", Type: IntCol},
	"lmd_version":             {Index: -19, Key: "", Type: StringCol},
	"section":                 {Index: -20, Key: "Section", Type: StringCol},
	"parent":                  {Index: -21, Key: "PeerParent", Type: StringCol},
	"configtool":              {Index: -22, Key: "", Type: HashMapCol},
	"federation_key":          {Index: -23, Key: "", Type: StringListCol},
	"federation_name":         {Index: -24, Key: "", Type: StringListCol},
	"federation_addr":         {Index: -25, Key: "", Type: StringListCol},
	"federation_type":         {Index: -26, Key: "", Type: StringListCol},
	"services_with_state":     {Index: -27, Key: "", Type: StringListCol},
	"services_with_info":      {Index: -28, Key: "", Type: StringListCol},
	"empty":                   {Index: -29, Key: "", Type: StringCol},
}

// Response contains the livestatus response data as long with some meta data
type Response struct {
	noCopy      noCopy
	Lock        *LoggingLock // must be used for Result and Failed access
	Code        int
	Result      [][]interface{}
	ResultTotal int
	Request     *Request
	Error       error
	Failed      map[string]string
	Columns     []ResultColumn
}

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

	indexes, columns, err := req.BuildResponseIndexes(table)
	if err != nil {
		return
	}
	res.Columns = columns

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
		if p.StatusGet("Idling").(bool) && len(table.DynamicColCacheIndexes) > 0 {
			p.StatusSet("LastQuery", time.Now().Unix())
			spinUpPeers = append(spinUpPeers, id)
		}
	}
	PeerMapLock.RUnlock()

	if len(selectedPeers) == 0 {
		res.Result = make([][]interface{}, 0)
		res.PostProcessing()
		return
	}

	// only use the first backend when requesting table or columns table
	if table.Name == "tables" || table.Name == "columns" {
		selectedPeers = []string{PeerMapOrder[0]}
	} else if !table.PassthroughOnly && len(spinUpPeers) > 0 {
		SpinUpPeers(spinUpPeers)
	}

	if table.PassthroughOnly {
		// passthrough requests, ex.: log table
		res.BuildPassThroughResult(selectedPeers, table, &columns)
	} else {
		res.BuildLocalResponse(selectedPeers, &indexes)
	}
	// if all backends are down, send an error instead of an empty result
	if res.Request.OutputFormat != "wrapped_json" && len(res.Failed) > 0 && len(res.Failed) == len(req.Backends) {
		res.Code = 502
		err = &PeerError{msg: res.Failed[req.Backends[0]], kind: ConnectionError}
		return
	}

	if res.Result == nil {
		res.Result = make([][]interface{}, 0)
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
	for _, s := range res.Request.Sort {
		Type := StringFakeSortCol
		if s.Index != -1 {
			Type = res.Columns[s.Index].Column.Type
		}
		if Type == VirtCol {
			Type = res.Columns[s.Index].Column.VirtType
		}
		switch Type {
		case TimeCol:
			fallthrough
		case IntCol:
			fallthrough
		case FloatCol:
			valueA := numberToFloat(&(res.Result[i][s.Index]))
			valueB := numberToFloat(&(res.Result[j][s.Index]))
			if valueA == valueB {
				continue
			}
			if s.Direction == Asc {
				return valueA < valueB
			}
			return valueA > valueB
		case StringCol:
			if s1, ok := res.Result[i][s.Index].(string); ok {
				if s2, ok := res.Result[j][s.Index].(string); ok {
					if s1 == s2 {
						continue
					}
					if s.Direction == Asc {
						return s1 < s2
					}
					return s1 > s2
				}
			}
			// not implemented
			return s.Direction == Asc
		case StringListCol:
			// not implemented
			return s.Direction == Asc
		case IntListCol:
			// not implemented
			return s.Direction == Asc
		case CustomVarCol:
			s1, _ := ((*(res.Result[i][s.Index]).(*map[string]interface{}))[s.Args]).(string)
			s2, _ := ((*(res.Result[j][s.Index]).(*map[string]interface{}))[s.Args]).(string)
			if s1 == s2 {
				continue
			}
			if s.Direction == Asc {
				return s1 < s2
			}
			return s1 > s2
		case StringFakeSortCol:
			if s1, ok := res.Result[i][0].(string); ok {
				if s2, ok := res.Result[j][0].(string); ok {
					if s1 == s2 {
						continue
					}
					if s.Direction == Asc {
						return s1 < s2
					}
					return s1 > s2
				}
			}
			// not implemented
			return s.Direction == Asc
		}
		panic(fmt.Sprintf("sorting not implemented for type %d", Type))
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
		table := Objects.Tables[res.Request.Table]
		if len(res.Request.BackendsMap) >= 1 || !table.IsDefaultSortOrder(&res.Request.Sort) {
			t1 := time.Now()
			sort.Sort(res)
			duration := time.Since(t1)
			log.Debugf("sorting result took %s", duration.String())
		}

		// remove hidden columns from result, for ex. columns used for sorting only
		firstHiddenColumnIndex := -1
		for i, col := range res.Columns {
			// first hidden column breaks the loop, because hidden columns are appended at the end of all columns
			if col.Hidden {
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
			res.Result = make([][]interface{}, 0)
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
	res.Result = make([][]interface{}, len(res.Request.StatsResult))

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
		for i, s := range stats {
			i += hasColumns

			finalStatsApply(s, &res.Result[j][i])

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
			res.Request.Sort = []*SortField{}
			for x := 0; x < hasColumns; x++ {
				res.Request.Sort = append(res.Request.Sort, &SortField{Name: "name", Index: -1, Direction: Asc})
			}
		}
		sort.Sort(res)
		duration := time.Since(t1)
		log.Debugf("sorting result took %s", duration.String())
		res.Request.Sort = []*SortField{}
	}
}

func finalStatsApply(s *Filter, res *interface{}) {
	switch s.StatsType {
	case Counter:
		*res = s.Stats
	case Min:
		*res = s.Stats
	case Max:
		*res = s.Stats
	case Sum:
		*res = s.Stats
	case Average:
		if s.StatsCount > 0 {
			*res = s.Stats / float64(s.StatsCount)
		} else {
			*res = 0
		}
	default:
		log.Panicf("not implemented")
	}
	if s.StatsCount == 0 {
		*res = 0
	}
}

// BuildResponseIndexes returns a list of used indexes and columns for this request.
func (req *Request) BuildResponseIndexes(table *Table) (indexes []int, columns []ResultColumn, err error) {
	log.Tracef("BuildResponseIndexes")
	requestColumnsMap := make(map[string]int)
	// if no column header was given, return all columns
	// but only if this is no stats query
	if len(req.Columns) == 0 && len(req.Stats) == 0 {
		req.SendColumnsHeader = true
		for _, col := range table.Columns {
			if col.Update != RefUpdate && col.Name != "empty" {
				req.Columns = append(req.Columns, col.Name)
			}
		}
	}
	// build array of requested columns as ResultColumn objects list
	for j, colName := range req.Columns {
		indexes, columns = addBuildResponseIndexColumn(table, colName, j, requestColumnsMap, indexes, columns, false)
	}

	// check wether our sort columns do exist in the output
	for _, s := range req.Sort {
		_, Ok := table.ColumnsIndex[s.Name]
		if !Ok {
			err = errors.New("bad request: table " + req.Table + " has no column " + s.Name + " to sort")
			return
		}
		i, Ok := requestColumnsMap[s.Name]
		if !Ok {
			i = len(columns)
			indexes, columns = addBuildResponseIndexColumn(table, s.Name, i, requestColumnsMap, indexes, columns, true)
		}
		s.Index = i
	}

	return
}
func addBuildResponseIndexColumn(table *Table, colName string, requestIndex int, requestColumnsMap map[string]int, indexes []int, columns []ResultColumn, hidden bool) ([]int, []ResultColumn) {
	colName = strings.ToLower(colName)
	i, ok := table.ColumnsIndex[colName]
	if !ok {
		if table.PassthroughOnly {
			indexes = append(indexes, -1)
			columns = append(columns, ResultColumn{Name: colName, Type: StringCol, Index: requestIndex, Hidden: hidden})
			requestColumnsMap[colName] = requestIndex
			return indexes, columns
		}
		if !fixBrokenClientsRequestColumn(&colName, table.Name) {
			colName = "empty"
		}
		i = table.ColumnsIndex[colName]
	}
	col := table.Columns[i]
	if table.Columns[i].Type == VirtCol {
		indexes = append(indexes, col.VirtMap.Index)
		columns = append(columns, ResultColumn{Name: colName, Type: VirtCol, Index: requestIndex, Column: col, Hidden: hidden})
		requestColumnsMap[colName] = requestIndex
		return indexes, columns
	}
	indexes = append(indexes, i)
	columns = append(columns, ResultColumn{Name: colName, Type: col.Type, Index: requestIndex, Column: col, Hidden: hidden})
	requestColumnsMap[colName] = requestIndex
	return indexes, columns
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

	if res.Request.OutputFormat == "wrapped_json" {
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
	sendColumnsHeader := res.Request.SendColumnsHeader

	buf.Write([]byte("["))
	// add optional columns header as first row
	cols := make([]interface{}, len(res.Request.Columns)+len(res.Request.Stats))
	if sendColumnsHeader {
		for i, v := range res.Request.Columns {
			cols[i] = v
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
	sendColumnsHeader := res.Request.SendColumnsHeader

	buf.Write([]byte("["))
	// add optional columns header as first row
	cols := make([]interface{}, len(res.Request.Columns)+len(res.Request.Stats))
	if sendColumnsHeader {
		for i, v := range res.Request.Columns {
			cols[i] = v
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
func (res *Response) BuildLocalResponse(peers []string, indexes *[]int) {
	res.Result = make([][]interface{}, 0)

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
		table := p.Tables[res.Request.Table].Table
		p.DataLock.RUnlock()

		p.StatusSet("LastQuery", time.Now().Unix())

		if table != nil && table.Virtual {
			// append results serially for local calculations
			// no need to create goroutines for simple sites queries
			res.AppendPeerResult(p, indexes)
			continue
		}

		if table == nil || !p.isOnline() {
			res.Lock.Lock()
			res.Failed[p.ID] = p.getError()
			res.Lock.Unlock()
			continue
		}

		if table.Name == "status" {
			// append results serially for simple calculations
			// no need to create goroutines for simple status queries
			res.AppendPeerResult(p, indexes)
			continue
		}

		waitgroup.Add(1)
		go func(peer *Peer, wg *sync.WaitGroup) {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)

			log.Tracef("[%s] starting local data computation", peer.Name)
			defer wg.Done()

			res.AppendPeerResult(peer, indexes)
		}(p, waitgroup)
	}
	log.Tracef("waiting...")
	waitgroup.Wait()
	log.Tracef("waiting for all local data computations done")
}

// AppendPeerResult appends result for given peer
func (res *Response) AppendPeerResult(peer *Peer, indexes *[]int) {
	total, result, statsResult := peer.BuildLocalResponseData(res, indexes)
	log.Tracef("[%s] result ready", peer.Name)

	if result != nil {
		if total == 0 {
			return
		}

		// data results rows
		res.Lock.Lock()
		res.ResultTotal += total
		res.Result = append(res.Result, (*result)...)
		res.Lock.Unlock()
	} else if statsResult != nil {
		res.Lock.Lock()
		res.ResultTotal += total
		if res.Request.StatsResult == nil {
			res.Request.StatsResult = make(map[string][]*Filter)
		}
		// apply stats querys
		for key, stats := range *statsResult {
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
func (res *Response) BuildPassThroughResult(peers []string, table *Table, columns *[]ResultColumn) {
	res.Result = make([][]interface{}, 0)

	// build columns list
	backendColumns := []string{}
	virtColumns := []*ResultColumn{}
	for i, col := range *columns {
		if col.Type == VirtCol {
			colref := (*columns)[i]
			virtColumns = append(virtColumns, &colref)
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
func (res *Response) PassThrougQuery(peer *Peer, table *Table, virtColumns []*ResultColumn, backendColumns []string) {
	req := res.Request
	passthroughRequest := &Request{
		Table:           req.Table,
		Filter:          req.Filter,
		Stats:           req.Stats,
		Columns:         backendColumns,
		Limit:           req.Limit,
		OutputFormat:    "json",
		ResponseFixed16: true,
	}
	var result [][]interface{}
	result, queryErr := peer.Query(passthroughRequest)
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
		for rowNum, row := range result {
			for _, col := range virtColumns {
				i := col.Index
				row = append(row, 0)
				copy(row[i+1:], row[i:])
				row[i] = peer.GetRowValue(col, &row, rowNum, table, nil)
			}
			result[rowNum] = row
		}
	}
	log.Tracef("[%s] result ready", peer.Name)
	res.Lock.Lock()
	if len(req.Stats) == 0 {
		res.Result = append(res.Result, result...)
	} else {
		if res.Request.StatsResult == nil {
			res.Request.StatsResult = make(map[string][]*Filter)
			res.Request.StatsResult[""] = createLocalStatsCopy(&res.Request.Stats)
		}
		// apply stats querys
		if len(result) > 0 {
			for i := range result[0] {
				val := result[0][i].(float64)
				res.Request.StatsResult[""][i].ApplyValue(val, int(val))
			}
		}
	}
	res.Lock.Unlock()
}
