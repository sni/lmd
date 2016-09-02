package main

import (
	"bytes"
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

	backendsMap := make(map[string]string)
	numBackendsReq := len(req.Backends)
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
		// TODO: should not happen on stats requests
		res.Result = make([][]interface{}, 0)
	}
	BuildResponsePostProcessing(res)
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

	// apply request offset
	if res.Request.Offset > 0 {
		if res.Request.Offset > len(res.Result) {
			res.Result = make([][]interface{}, 0)
		} else {
			res.Result = res.Result[res.Request.Offset:]
		}
	}

	// apply request limit
	if res.Request.Limit > 0 {
		if res.Request.Limit < len(res.Result) {
			res.Result = res.Result[0:res.Request.Limit]
		}
	}

	return
}

func BuildResponseIndexes(req *Request, table *Table) (indexes []int, columns []Column, err error) {
	requestColumnsMap := make(map[string]int)
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
	if peer.Status["LastError"] != "" {
		res.Failed[peer.Id] = fmt.Sprintf("%v", peer.Status["LastError"])
		return
	}

	table := peer.Tables[req.Table].Table
	data := peer.Tables[req.Table].Data
	refs := peer.Tables[req.Table].Refs

	inputRowLen := len(data[0])
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
		var out bytes.Buffer
		// TODO: remove pretty json or make extra header for it
		json.Indent(&out, resBytes, "", "  ")
		resBytes = out.Bytes()
	}
	if res.Error != nil {
		log.Warnf("client error: %s", res.Error.Error())
		resBytes = []byte(res.Error.Error())
	}

	// TODO: check wether we have to send column headers
	if res.Request.ResponseFixed16 {
		size := len(resBytes) + 1
		_, err = c.Write([]byte(fmt.Sprintf("%d %12d\n", res.Code, size)))
		if err != nil {
			log.Errorf("write error: %s", err.Error())
		}
	}
	// TODO: send array line by line to avoid to long lines
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

func matchFilter(table *Table, refs *map[string][][]interface{}, inputRowLen int, filter Filter, row *[]interface{}, rowNum int) bool {
	// recursive group filter
	if len(filter.Filter) > 0 {
		for _, f := range filter.Filter {
			subresult := matchFilter(table, refs, inputRowLen, f, row, rowNum)
			if subresult == false && filter.GroupOperator == And {
				return false
			}
			if subresult == true && filter.GroupOperator == Or {
				return true
			}
		}
		// if we did not return yet, this means all AND filter have matched
		if filter.GroupOperator == And {
			return true
		}
		// if we did not return yet, this means no OR filter have matched
		return false
	}

	// normal field filter
	value := getRowValue(filter.Column.Index, row, rowNum, table, refs, inputRowLen)
	if value == nil {
		return false
	}
	switch filter.Column.Type {
	case StringCol:
		switch filter.Operator {
		case Equal:
			if value.(string) == filter.Value.(string) {
				return true
			}
		case Unequal:
			if value.(string) != filter.Value.(string) {
				return true
			}
		default:
			log.Errorf("not implemented op: %v", filter.Operator)
			return false
		}
		break
	case IntCol:
		fallthrough
	case FloatCol:
		valueA := value.(float64)
		var valueB float64
		if filter.Column.Type == IntCol {
			valueB = float64(filter.Value.(int))
		} else {
			valueB = filter.Value.(float64)
		}
		switch filter.Operator {
		case Equal:
			if valueA == valueB {
				return true
			}
		case Unequal:
			if valueA != valueB {
				return true
			}
		case Less:
			if valueA < valueB {
				return true
			}
		case LessThan:
			if valueA <= valueB {
				return true
			}
		case Greater:
			if valueA > valueB {
				return true
			}
		case GreaterThan:
			if valueA >= valueB {
				return true
			}
		default:
			log.Errorf("not implemented op: %v", filter.Operator)
			return false
		}
		break
	default:
		log.Errorf("not implemented type: %v", filter.Column.Type)
		return false
	}
	return false
}
