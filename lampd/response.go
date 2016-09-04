package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
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

const (
	PEER_KEY_INDEX = -1
)

// result sorter
func (res Response) Len() int {
	return len(res.Result)
}

func (res Response) Less(i, j int) bool {
	for _, sort := range res.Request.Sort {
		Type := res.Columns[sort.Index].Type
		switch Type {
		case IntCol:
			fallthrough
		case FloatCol:
			if res.Result[i][sort.Index].(float64) == res.Result[j][sort.Index].(float64) {
				continue
			}
			if sort.Direction == Asc {
				return res.Result[i][sort.Index].(float64) < res.Result[j][sort.Index].(float64)
			} else {
				return res.Result[i][sort.Index].(float64) > res.Result[j][sort.Index].(float64)
			}
			break
		case StringCol:
			if res.Result[i][sort.Index].(string) == res.Result[j][sort.Index].(string) {
				continue
			}
			if sort.Direction == Asc {
				return res.Result[i][sort.Index].(string) < res.Result[j][sort.Index].(string)
			} else {
				return res.Result[i][sort.Index].(string) > res.Result[j][sort.Index].(string)
			}
			break
		case StringListCol:
			// not implemented
			return sort.Direction == Asc
		default:
			panic(fmt.Sprintf("sorting not implemented for type %d", Type))
		}
		if sort.Direction == Asc {
			return false
		}
		return true
	}
	return true
}

func (res Response) Swap(i, j int) {
	res.Result[i], res.Result[j] = res.Result[j], res.Result[i]
}

func BuildResponse(req *Request) (res *Response, err error) {
	res = &Response{
		Code:    200,
		Failed:  make(map[string]string),
		Request: req,
	}

	table, _ := Objects.Tables[req.Table]

	indexes, columns, err := BuildResponseIndexes(req, table)
	if err != nil {
		return
	}
	res.Columns = columns
	numPerRow := len(indexes)

	backendsMap, numBackendsReq, err := ExpandRequestBackends(req)
	if err != nil {
		return
	}

	for _, p := range DataStore {
		if numBackendsReq > 0 {
			_, Ok := backendsMap[p.Id]
			if !Ok {
				continue
			}
		}
		BuildResponseDataForPeer(res, req, &p, numPerRow, &indexes)
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
				res.Result[0][i] = s.StatsCount
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
			}
		}
	}
	return
}

func BuildResponseIndexes(req *Request, table *Table) (indexes []int, columns []Column, err error) {
	requestColumnsMap := make(map[string]int)
	// if no column header was given, return all columns
	// but only if this is no stats query
	if len(req.Columns) == 0 && len(req.Stats) == 0 {
		req.SendColumnsHeader = true
		for _, col := range table.Columns {
			if col.Update == StaticUpdate || col.Update == DynamicUpdate {
				req.Columns = append(req.Columns, col.Name)
			}
		}
	}
	// build array of requested columns as Column objects list
	for j, col := range req.Columns {
		col = strings.ToLower(col)
		if col == "peer_key" {
			indexes = append(indexes, PEER_KEY_INDEX)
			columns = append(columns, Column{Name: col, Type: StringCol, Index: j})
			continue
		}
		i, Ok := table.ColumnsIndex[col]
		if !Ok {
			err = errors.New("bad request: table " + req.Table + " has no column " + col)
			return
		}
		indexes = append(indexes, i)
		columns = append(columns, Column{Name: col, Type: table.Columns[i].Type, Index: j})
		requestColumnsMap[col] = j
	}

	// check wether our sort columns do exist in the output
	for _, sort := range req.Sort {
		_, Ok := table.ColumnsIndex[sort.Name]
		if !Ok {
			err = errors.New("bad request: table " + req.Table + " has no column " + sort.Name + " to sort")
			return
		}
		i, Ok := requestColumnsMap[sort.Name]
		if !Ok {
			err = errors.New("bad request: sort column " + sort.Name + " not in result set")
			return
		}
		sort.Index = i
	}

	return
}

func BuildResponseDataForPeer(res *Response, req *Request, peer *Peer, numPerRow int, indexes *[]int) (err error) {
	peer.Lock.RLock()
	defer peer.Lock.RUnlock()
	peer.Status["LastQuery"] = time.Now()
	if peer.Status["LastError"] != "" {
		res.Failed[peer.Id] = fmt.Sprintf("%v", peer.Status["LastError"])
		return
	}

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
			if !matchFilter(table, &refs, inputRowLen, f, &row, j) {
				filterMatched = false
				break
			}
		}
		if !filterMatched {
			continue
		}

		// count stats
		if statsLen > 0 {
			for i, s := range res.Request.Stats {
				if s.StatsType != Counter || matchFilter(table, &refs, inputRowLen, s, &row, j) {
					val := getRowValue(s.Column.Index, &row, j, table, &refs, inputRowLen)
					switch s.StatsType {
					case Counter:
						s.Stats++
						break
					case Average:
						value := val.(float64)
						s.Stats += value
						break
					case Sum:
						value := val.(float64)
						s.Stats += value
						break
					case Min:
						value := val.(float64)
						if s.Stats > value || s.Stats == -1 {
							s.Stats = value
						}
						break
					case Max:
						value := val.(float64)
						if s.Stats < value {
							s.Stats = value
						}
						break
					}
					s.StatsCount++
					res.Request.Stats[i] = s
				}
			}
			continue
		}

		// build result row
		resRow := make([]interface{}, numPerRow)
		for k, i := range *(indexes) {
			if i == PEER_KEY_INDEX {
				resRow[k] = peer.Id
			} else {
				// check if this is a reference column
				// reference columns come after the non-ref columns
				if i >= inputRowLen {
					refObj := refs[table.Columns[table.Columns[i].RefIndex].Name][j]
					if len(refObj) == 0 {
						res.Failed[peer.Id] = "peer not ready yet"
						return
					}
					resRow[k] = refObj[table.Columns[i].RefColIndex]
				} else {
					resRow[k] = row[i]
				}
			}
		}
		res.Result = append(res.Result, resRow)
	}

	return
}

func SendResponse(c net.Conn, res *Response) (err error) {
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
	if res.Result != nil {
		if res.Request.OutputFormat == "" || res.Request.OutputFormat == "wrapped_json" {
			wrappedResult := make(map[string]interface{})
			wrappedResult["data"] = res.Result
			wrappedResult["total"] = res.ResultTotal
			wrappedResult["failed"] = res.Failed
			resBytes, err = json.Marshal(wrappedResult)
		} else {
			resBytes, err = json.Marshal(res.Result)
		}
		if err != nil {
			log.Errorf("json error: %s", err.Error())
		}
	}
	if res.Error != nil {
		log.Warnf("client error: %s", res.Error.Error())
		resBytes = []byte(res.Error.Error())
	}

	if res.Request.ResponseFixed16 {
		size := len(resBytes) + 1
		log.Debugf("write: %s", fmt.Sprintf("%d %11d", res.Code, size))
		_, err = c.Write([]byte(fmt.Sprintf("%d %11d\n", res.Code, size)))
		if err != nil {
			log.Errorf("write error: %s", err.Error())
		}
	}
	// TODO: send array line by line to avoid to long lines
	log.Debugf("write: %s", resBytes)
	_, err = c.Write(resBytes)
	if err != nil {
		log.Errorf("write error: %s", err.Error())
	}
	_, err = c.Write([]byte("\n"))
	return
}

func getRowValue(index int, row *[]interface{}, rowNum int, table *Table, refs *map[string][][]interface{}, inputRowLen int) interface{} {
	if index >= inputRowLen {
		refObj := (*refs)[table.Columns[table.Columns[index].RefIndex].Name][rowNum]
		if len(refObj) == 0 {
			return nil
		}
		return refObj[table.Columns[index].RefColIndex]
	}
	return (*row)[index]
}
