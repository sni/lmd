package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
)

type Response struct {
	Code        int
	Result      [][]interface{}
	ResultTotal int
	Request     *Request
	Error       error
	Failed      map[string]string
}

const (
	PEER_KEY_INDEX = -1
)

// TODO: split into sub func
func BuildResponse(req *Request) (res *Response, err error) {
	res = &Response{
		Code:    200,
		Failed:  make(map[string]string),
		Request: req,
	}

	table, ok := Objects.Tables[req.Table]
	if !ok {
		err = errors.New("table " + req.Table + " does not exist")
		res.Code = 404
		return
	}

	// TODO: define size of result
	//Result: make([][]interface{}),

	indexes := []int{}
	for _, col := range req.Columns {
		if col == "peer_key" {
			indexes = append(indexes, PEER_KEY_INDEX)
			continue
		}
		i, Ok := table.ColumnsIndex[col]
		if !Ok {
			err = errors.New("bad request: table " + req.Table + " has no column " + col)
			return
			continue
		}
		indexes = append(indexes, i)
	}
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

	// TODO: sort here

	if req.Offset > 0 {
		if req.Offset > len(res.Result) {
			res.Result = make([][]interface{}, 0)
		} else {
			res.Result = res.Result[req.Offset:]
		}
	}

	if req.Limit > 0 {
		if req.Limit < len(res.Result) {
			res.Result = res.Result[0:req.Limit]
		}
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
		// TODO: filter here
		resRow := make([]interface{}, numPerRow)
		for k, i := range *(indexes) {
			if i == PEER_KEY_INDEX {
				resRow[k] = peer.Id
			} else {
				// this means this is a reference column
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
		if res.Request.OutputFormat == "wrapped_json" {
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
		resBytes = []byte(res.Error.Error())
	}

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
