package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

type ResultData [][]interface{}

type Response struct {
	Code        int
	Result      ResultData
	ResultTotal int
	Request     *Request
	Error       error
	Failed      map[string]string
	Columns     []Column
}

type VirtKeyMapTupel struct {
	Index int
	Key   string
	Type  ColumnType
}

var VirtKeyMap = map[string]VirtKeyMapTupel{
	"peer_key":                {Index: -1, Key: "PeerKey", Type: StringCol},
	"key":                     {Index: -1, Key: "PeerKey", Type: StringCol},
	"peer_name":               {Index: -2, Key: "PeerName", Type: StringCol},
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
}

// result sorter
func (res Response) Len() int {
	return len(res.Result)
}

func (res Response) Less(i, j int) bool {
	for _, s := range res.Request.Sort {
		Type := res.Columns[s.Index].Type
		switch Type {
		case IntCol:
			fallthrough
		case FloatCol:
			valueA := NumberToFloat(res.Result[i][s.Index])
			valueB := NumberToFloat(res.Result[j][s.Index])
			if valueA == valueB {
				continue
			}
			if s.Direction == Asc {
				return valueA < valueB
			}
			return valueA > valueB
		case StringCol:
			if res.Result[i][s.Index].(string) == res.Result[j][s.Index].(string) {
				continue
			}
			if s.Direction == Asc {
				return res.Result[i][s.Index].(string) < res.Result[j][s.Index].(string)
			}
			return res.Result[i][s.Index].(string) > res.Result[j][s.Index].(string)
		case StringListCol:
			// not implemented
			return s.Direction == Asc
		}
		panic(fmt.Sprintf("sorting not implemented for type %d", Type))
	}
	return true
}

func (res Response) Swap(i, j int) {
	res.Result[i], res.Result[j] = res.Result[j], res.Result[i]
}

func BuildResponse(req *Request) (res *Response, err error) {
	log.Tracef("BuildResponse")
	res = &Response{
		Code:    200,
		Failed:  make(map[string]string),
		Request: req,
	}

	table, _ := Objects.Tables[req.Table]

	indexes, columns, err := BuildResponseIndexes(req, &table)
	if err != nil {
		return
	}
	res.Columns = columns
	numPerRow := len(indexes)

	backendsMap, numBackendsReq, err := ExpandRequestBackends(req)
	if err != nil {
		return
	}

	// check if we have to spin up updates, if so, do it parallel
	selectedPeers := []string{}
	spinUpPeers := []string{}
	for _, id := range DataStoreOrder {
		p := DataStore[id]
		if numBackendsReq > 0 {
			_, Ok := backendsMap[p.Id]
			if !Ok {
				continue
			}
		}
		selectedPeers = append(selectedPeers, id)

		// spin up required?
		if !table.PassthroughOnly && p.Status["Idling"].(bool) && len(table.DynamicColCacheIndexes) > 0 {
			spinUpPeers = append(spinUpPeers, id)
		}
	}

	if len(spinUpPeers) > 0 {
		SpinUpPeers(spinUpPeers)
	}

	if table.PassthroughOnly {
		// passthrough requests, ex.: log table
		BuildPassThroughResult(selectedPeers, res, &table, &columns, numPerRow)
		if err != nil {
			return
		}
	} else {
		for _, id := range selectedPeers {
			p := DataStore[id]
			BuildLocalResponseDataForPeer(res, req, &p, numPerRow, &indexes)
			log.Tracef("BuildLocalResponseDataForPeer done: %s", p.Name)
		}
	}
	if res.Result == nil {
		res.Result = make([][]interface{}, 0)
	}
	BuildResponsePostProcessing(res)
	return
}

func ExpandRequestBackends(req *Request) (backendsMap map[string]string, numBackendsReq int, err error) {
	numBackendsReq = len(req.Backends)
	if numBackendsReq > 0 {
		backendsMap = make(map[string]string)
		for _, b := range req.Backends {
			_, Ok := DataStore[b]
			if !Ok {
				err = errors.New("bad request: backend " + b + " does not exist")
				return
			}
			backendsMap[b] = b
		}
	}
	return
}

func BuildResponsePostProcessing(res *Response) {
	log.Tracef("BuildResponsePostProcessing")
	// sort our result
	if len(res.Request.Sort) > 0 {
		t1 := time.Now()
		sort.Sort(res)
		duration := time.Since(t1)
		log.Debugf("sorting result took %s", duration.String())
	}

	res.ResultTotal = len(res.Result)

	// apply request offset
	if res.Request.Offset > 0 {
		if res.Request.Offset > res.ResultTotal {
			res.Result = make([][]interface{}, 0)
		} else {
			res.Result = res.Result[res.Request.Offset:]
		}
	}

	// apply request limit
	if res.Request.Limit > 0 {
		if res.Request.Limit < res.ResultTotal {
			res.Result = res.Result[0:res.Request.Limit]
		}
	}

	// final calculation of stats querys
	if len(res.Request.Stats) > 0 {
		res.Result = make([][]interface{}, 1)
		res.Result[0] = make([]interface{}, len(res.Request.Stats))
		for i, s := range res.Request.Stats {
			switch s.StatsType {
			case Counter:
				res.Result[0][i] = s.Stats
				break
			case Min:
				res.Result[0][i] = s.Stats
				break
			case Max:
				res.Result[0][i] = s.Stats
				break
			case Sum:
				res.Result[0][i] = s.Stats
				break
			case Average:
				if s.StatsCount > 0 {
					res.Result[0][i] = float64(s.Stats) / float64(s.StatsCount)
				} else {
					res.Result[0][i] = 0
				}
				break
			default:
				log.Panicf("not implemented")
				break
			}
			if s.StatsCount == 0 {
				res.Result[0][i] = 0
			}
		}
	}
	return
}

func BuildResponseIndexes(req *Request, table *Table) (indexes []int, columns []Column, err error) {
	log.Tracef("BuildResponseIndexes")
	requestColumnsMap := make(map[string]int)
	// if no column header was given, return all columns
	// but only if this is no stats query
	if len(req.Columns) == 0 && len(req.Stats) == 0 {
		req.SendColumnsHeader = true
		for _, col := range table.Columns {
			if col.Update == StaticUpdate || col.Update == DynamicUpdate || col.Type == VirtCol {
				req.Columns = append(req.Columns, col.Name)
			}
		}
	}
	// build array of requested columns as Column objects list
	for j, col := range req.Columns {
		col = strings.ToLower(col)
		i, ok := table.ColumnsIndex[col]
		if !ok {
			err = errors.New("bad request: table " + req.Table + " has no column " + col)
			return
		}
		if table.Columns[i].Type == VirtCol {
			indexes = append(indexes, VirtKeyMap[col].Index)
			columns = append(columns, Column{Name: col, Type: VirtKeyMap[col].Type, Index: j, RefIndex: i})
			requestColumnsMap[col] = j
			continue
		}
		indexes = append(indexes, i)
		columns = append(columns, Column{Name: col, Type: table.Columns[i].Type, Index: j})
		requestColumnsMap[col] = j
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
			err = errors.New("bad request: sort column " + s.Name + " not in result set")
			return
		}
		s.Index = i
	}

	return
}

func BuildLocalResponseDataForPeer(res *Response, req *Request, peer *Peer, numPerRow int, indexes *[]int) {
	log.Tracef("BuildLocalResponseDataForPeer: %s", peer.Name)
	peer.PeerLock.Lock()
	peer.Status["LastQuery"] = time.Now()
	if peer.Status["PeerStatus"].(PeerStatus) == PeerStatusDown && req.Table != "backends" {
		res.Failed[peer.Id] = fmt.Sprintf("%v", peer.Status["LastError"])
		peer.PeerLock.Unlock()
		return
	}
	peer.PeerLock.Unlock()

	// if a WaitTrigger is supplied, wait max ms till the condition is true
	if req.WaitTrigger != "" {
		peer.waitCondition(req)
	}

	peer.DataLock.RLock()
	defer peer.DataLock.RUnlock()

	table := peer.Tables[req.Table].Table
	data := peer.Tables[req.Table].Data
	refs := peer.Tables[req.Table].Refs

	if len(data) == 0 {
		return
	}
	inputRowLen := len(data[0])
	statsLen := len(res.Request.Stats)
	for j, row := range data {
		// does our filter match?
		filterMatched := true
		for _, f := range res.Request.Filter {
			if !peer.matchFilter(table, &refs, inputRowLen, &f, &row, j) {
				filterMatched = false
				break
			}
		}
		if !filterMatched {
			continue
		}

		// count stats
		if statsLen > 0 {
			for i, _ := range res.Request.Stats {
				s := &(res.Request.Stats[i])
				// avg/sum/min/max are passed through, they dont have filter
				// counter must match their filter
				if s.StatsType != Counter || peer.matchFilter(table, &refs, inputRowLen, s, &row, j) {
					val := peer.getRowValue(s.Column.Index, &row, j, table, &refs, inputRowLen)
					switch s.StatsType {
					case Counter:
						s.Stats++
						break
					case Average:
						fallthrough
					case Sum:
						s.Stats += NumberToFloat(val)
						break
					case Min:
						value := NumberToFloat(val)
						if s.Stats > value || s.Stats == -1 {
							s.Stats = value
						}
						break
					case Max:
						value := NumberToFloat(val)
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
				resRow[k] = peer.getRowValue(res.Columns[k].RefIndex, &row, j, table, nil, inputRowLen)
			} else {
				// check if this is a reference column
				// reference columns come after the non-ref columns
				if i >= inputRowLen {
					refObj := refs[table.Columns[table.Columns[i].RefIndex].Name][j]
					resRow[k] = refObj[table.Columns[i].RefColIndex]
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
		res.Result = append(res.Result, resRow)
	}

	return
}

func SendResponse(c net.Conn, res *Response) (size int, err error) {
	resBytes := []byte{}
	if res.Request.SendColumnsHeader {
		var result ResultData = make([][]interface{}, 0)
		cols := make([]interface{}, len(res.Request.Columns)+len(res.Request.Stats))
		for i, v := range res.Request.Columns {
			cols[i] = v
		}
		result = append(result, cols)
		result = append(result, res.Result...)
		res.Result = result
	}
	if res.Error != nil {
		log.Warnf("client error: %s", res.Error.Error())
		resBytes = []byte(res.Error.Error())
	} else if res.Result != nil {
		if res.Request.OutputFormat == "wrapped_json" {
			resBytes = append(resBytes, []byte("{\"data\":[")...)
		}
		if res.Request.OutputFormat == "json" || res.Request.OutputFormat == "" {
			resBytes = append(resBytes, []byte("[")...)
		}
		// append result row by row
		if res.Request.OutputFormat == "wrapped_json" || res.Request.OutputFormat == "json" || res.Request.OutputFormat == "" {
			for i, row := range res.Result {
				rowBytes, jerr := json.Marshal(row)
				if jerr != nil {
					log.Errorf("json error: %s in row: %v", jerr.Error(), row)
					err = jerr
					return
				}
				if i > 0 {
					resBytes = append(resBytes, []byte(",\n")...)
				}
				resBytes = append(resBytes, rowBytes...)
			}
			resBytes = append(resBytes, []byte("]")...)
		}
		if res.Request.OutputFormat == "wrapped_json" {
			resBytes = append(resBytes, []byte("\n,\"failed\":")...)
			failBytes, _ := json.Marshal(res.Failed)
			resBytes = append(resBytes, failBytes...)
			resBytes = append(resBytes, []byte(fmt.Sprintf("\n,\"total\":%d}", res.ResultTotal))...)
		}
	}

	size = len(resBytes) + 1
	if res.Request.ResponseFixed16 {
		log.Debugf("write: %s", fmt.Sprintf("%d %11d", res.Code, size))
		_, err = c.Write([]byte(fmt.Sprintf("%d %11d\n", res.Code, size)))
		if err != nil {
			log.Warnf("write error: %s", err.Error())
		}
	}
	log.Debugf("write: %s", resBytes)
	written, err := c.Write(resBytes)
	if err != nil {
		log.Warnf("write error: %s", err.Error())
	}
	if written != size-1 {
		log.Warnf("write error: written %d, size: %d", written, size)
	}
	localAddr := c.LocalAddr().String()
	promFrontendBytesSend.WithLabelValues(localAddr).Add(float64(len(resBytes)))
	_, err = c.Write([]byte("\n"))
	return
}

func BuildPassThroughResult(peers []string, res *Response, table *Table, columns *[]Column, numPerRow int) (err error) {
	req := res.Request
	res.Result = make([][]interface{}, 0)

	// build columns list
	backendColumns := []string{}
	virtColumns := []Column{}
	for _, col := range *columns {
		if col.RefIndex > 0 {
			virtColumns = append(virtColumns, col)
		} else {
			backendColumns = append(backendColumns, col.Name)
		}
	}

	waitgroup := &sync.WaitGroup{}

	for _, id := range peers {
		p := DataStore[id]
		m := sync.Mutex{}

		p.PeerLock.RLock()
		if p.Status["PeerStatus"].(PeerStatus) == PeerStatusDown {
			m.Lock()
			res.Failed[p.Id] = fmt.Sprintf("%v", p.Status["LastError"])
			m.Unlock()
			p.PeerLock.RUnlock()
			continue
		}
		p.PeerLock.RUnlock()

		waitgroup.Add(1)
		go func(peer Peer, wg *sync.WaitGroup) {
			log.Debugf("[%s] starting passthrough request", p.Name)
			defer wg.Done()
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
			result, err = peer.Query(passthroughRequest)
			log.Tracef("[%s] req done", p.Name)
			if err != nil {
				log.Tracef("[%s] req errored", err.Error())
				m.Lock()
				res.Failed[p.Id] = err.Error()
				m.Unlock()
				return
			}
			// insert virtual values
			if len(virtColumns) > 0 {
				for j, row := range result {
					for _, col := range virtColumns {
						i := col.Index
						row = append(row, 0)
						copy(row[i+1:], row[i:])
						row[i] = peer.getRowValue(col.RefIndex, &row, j, table, nil, numPerRow)
					}
					result[j] = row
				}
			}
			log.Tracef("[%s] result ready", p.Name)
			m.Lock()
			res.Result = append(res.Result, result...)
			m.Unlock()
		}(p, waitgroup)
	}
	log.Tracef("waiting...")
	waitgroup.Wait()
	log.Debugf("waiting for passed through requests done")
	return
}
