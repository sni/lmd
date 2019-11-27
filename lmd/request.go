package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/a8m/djson"
	"github.com/buger/jsonparser"
)

// Request defines a livestatus request object.
type Request struct {
	noCopy              noCopy
	Table               TableName
	Command             string
	Columns             []string  // parsed columns field
	RequestColumns      []*Column // calculated/expanded columns list
	Filter              []*Filter
	FilterStr           string
	Stats               []*Filter
	StatsResult         ResultSetStats
	Limit               *int
	Offset              int
	Sort                []*SortField
	ResponseFixed16     bool
	OutputFormat        OutputFormat
	Backends            []string
	BackendsMap         map[string]string
	BackendErrors       map[string]string
	ColumnsHeaders      bool
	SendStatsData       bool
	WaitTimeout         int
	WaitTrigger         string
	WaitCondition       []*Filter
	WaitObject          string
	WaitConditionNegate bool
	KeepAlive           bool
	AuthUser            string
}

// SortDirection can be either Asc or Desc
type SortDirection uint8

// The only possible SortDirection are "Asc" and "Desc" for
// sorting ascending or descending.
const (
	_ SortDirection = iota
	Asc
	Desc
)

// String converts a SortDirection back to the original string.
func (s *SortDirection) String() string {
	switch *s {
	case Asc:
		return "asc"
	case Desc:
		return "desc"
	}
	log.Panicf("not implemented")
	return ""
}

// OutputFormat defines the format used to return the result
type OutputFormat uint8

const (
	OutputFormatDefault OutputFormat = iota
	OutputFormatJSON
	OutputFormatWrappedJSON
	OutputFormatPython
)

// String converts a SortDirection back to the original string.
func (o *OutputFormat) String() string {
	switch *o {
	case OutputFormatDefault:
		return "json"
	case OutputFormatWrappedJSON:
		return "wrapped_json"
	case OutputFormatJSON:
		return "json"
	case OutputFormatPython:
		return "python"
	}
	log.Panicf("not implemented")
	return ""
}

// SortField defines a single sort entry
type SortField struct {
	noCopy    noCopy
	Name      string
	Direction SortDirection
	Index     int
	Group     bool
	Args      string
	Column    *Column
}

// GroupOperator is the operator used to combine multiple filter or stats header.
type GroupOperator uint8

// The only possible GroupOperator are "And" and "Or"
const (
	_ GroupOperator = iota
	And
	Or
)

// String converts a GroupOperator back to the original string.
func (op *GroupOperator) String() string {
	switch *op {
	case And:
		return ("And")
	case Or:
		return ("Or")
	}
	log.Panicf("not implemented")
	return ""
}

// ResultMetaData contains meta from the response data
type ResultMetaData struct {
	Total   int64
	Columns []string
}

var reRequestAction = regexp.MustCompile(`^GET +([a-z]+)$`)
var reRequestCommand = regexp.MustCompile(`^COMMAND +(\[\d+\].*)$`)

// ParseRequest reads from a connection and returns a single requests.
// It returns a the requests and any errors encountered.
func ParseRequest(c net.Conn) (req *Request, err error) {
	b := bufio.NewReader(c)
	localAddr := c.LocalAddr().String()
	req, size, err := NewRequest(b)
	promFrontendBytesReceived.WithLabelValues(localAddr).Add(float64(size))
	return
}

// ParseRequests reads from a connection and returns all requests read.
// It returns a list of requests and any errors encountered.
func ParseRequests(c net.Conn) (reqs []*Request, err error) {
	b := bufio.NewReader(c)
	localAddr := c.LocalAddr().String()
	for {
		req, size, err := NewRequest(b)
		promFrontendBytesReceived.WithLabelValues(localAddr).Add(float64(size))
		if err != nil {
			return nil, err
		}
		if req == nil {
			break
		}
		err = req.ExpandRequestedBackends()
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, req)
		// only multiple commands are allowed
		if req.Command == "" {
			break
		}
	}
	return
}

// String returns the request object as livestatus query string.
func (req *Request) String() (str string) {
	// Commands are easy passthrough
	if req.Command != "" {
		str = req.Command + "\n\n"
		return
	}
	str = "GET " + req.Table.String() + "\n"
	if req.ResponseFixed16 {
		str += "ResponseHeader: fixed16\n"
	}
	if req.OutputFormat != OutputFormatDefault {
		str += fmt.Sprintf("OutputFormat: %s\n", req.OutputFormat.String())
	}
	if len(req.Columns) > 0 {
		str += "Columns: " + strings.Join(req.Columns, " ") + "\n"
	}
	if len(req.Backends) > 0 {
		str += "Backends: " + strings.Join(req.Backends, " ") + "\n"
	}
	if req.Limit != nil {
		str += fmt.Sprintf("Limit: %d\n", *req.Limit)
	}
	if req.Offset > 0 {
		str += fmt.Sprintf("Offset: %d\n", req.Offset)
	}
	if req.ColumnsHeaders {
		str += fmt.Sprintf("ColumnHeaders: on\n")
	}
	if req.KeepAlive {
		str += fmt.Sprintf("KeepAlive: on\n")
	}
	for i := range req.Filter {
		str += req.Filter[i].String("")
	}
	if req.FilterStr != "" {
		str += req.FilterStr
	}
	for i := range req.Stats {
		str += req.Stats[i].String("Stats")
	}
	if req.WaitTrigger != "" {
		str += fmt.Sprintf("WaitTrigger: %s\n", req.WaitTrigger)
	}
	if req.WaitObject != "" {
		str += fmt.Sprintf("WaitObject: %s\n", req.WaitObject)
	}
	if req.WaitTimeout > 0 {
		str += fmt.Sprintf("WaitTimeout: %d\n", req.WaitTimeout)
	}
	if req.WaitConditionNegate {
		str += fmt.Sprintf("WaitConditionNegate\n")
	}
	if req.AuthUser != "" {
		str += fmt.Sprintf("AuthUser: %s\n", req.AuthUser)
	}
	for i := range req.WaitCondition {
		str += req.WaitCondition[i].String("WaitCondition")
	}
	for i := range req.Sort {
		str += fmt.Sprintf("Sort: %s %s\n", req.Sort[i].Name, req.Sort[i].Direction.String())
	}
	str += "\n"
	return
}

// NewRequest reads a buffer and creates a new request object.
// It returns the request as long with the number of bytes read and any error.
func NewRequest(b *bufio.Reader) (req *Request, size int, err error) {
	req = &Request{ColumnsHeaders: false, KeepAlive: false}
	firstLine, err := b.ReadString('\n')
	// Network errors will be logged in the listener
	if _, ok := err.(net.Error); ok {
		req = nil
		return
	}
	size += len(firstLine)
	firstLine = strings.TrimSpace(firstLine)
	// probably a open connection without new data from a keepalive request
	if log.IsV(LogVerbosityDebug) && firstLine != "" {
		log.Debugf("request: %s", firstLine)
	}

	ok, err := req.ParseRequestAction(&firstLine)
	if err != nil || !ok {
		req = nil
		return
	}

	for {
		line, berr := b.ReadBytes('\n')
		if berr != nil && berr != io.EOF {
			err = berr
			return
		}
		size += len(line)
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			break
		}

		if log.IsV(LogVerbosityDebug) {
			log.Debugf("request: %s", line)
		}
		perr := req.ParseRequestHeaderLine(line)
		if perr != nil {
			err = fmt.Errorf("bad request: %s in: %s", perr.Error(), line)
			return
		}
		if berr == io.EOF {
			break
		}
	}

	req.SetRequestColumns()
	err = req.SetSortColumns()
	return
}

// ParseRequestAction parses the first line from a request which
// may start with GET or COMMAND
func (req *Request) ParseRequestAction(firstLine *string) (valid bool, err error) {
	valid = false

	// normal get request?
	if strings.HasPrefix(*firstLine, "GET ") {
		matched := reRequestAction.FindStringSubmatch(*firstLine)
		if len(matched) != 2 {
			err = fmt.Errorf("bad request: %s", *firstLine)
			return
		}

		tableName, tErr := NewTableName(matched[1])
		if tErr != nil {
			err = fmt.Errorf("bad request: %s", tErr.Error())
		}
		req.Table = tableName
		valid = true
		return
	}

	// or a command
	if strings.HasPrefix(*firstLine, "COMMAND ") {
		matched := reRequestCommand.FindStringSubmatch(*firstLine)
		if len(matched) < 1 {
			err = fmt.Errorf("bad request: %s", *firstLine)
			return
		}
		req.Command = matched[0]
		valid = true
		return
	}

	// empty request
	if *firstLine == "" {
		return
	}

	err = fmt.Errorf("bad request: %s", *firstLine)
	return
}

// GetResponse builds the response for a given request.
// It returns the Response object and any error encountered.
func (req *Request) GetResponse() (*Response, error) {
	// Run single request if possible
	if nodeAccessor == nil || !nodeAccessor.IsClustered() {
		// Single mode (send request and return response)
		return NewResponse(req)
	}

	// Determine if request for this node only (if backends specified)
	allBackendsRequested := len(req.Backends) == 0
	isForOurBackends := false // request for our own backends only
	if !allBackendsRequested {
		isForOurBackends = true
		for _, backend := range req.Backends {
			isOurs := nodeAccessor.IsOurBackend(backend)
			isForOurBackends = isForOurBackends && isOurs
		}
	}

	// Return local result if its not distributed at all
	if isForOurBackends {
		return NewResponse(req)
	}

	// Distribute request
	return req.getDistributedResponse()
}

// getDistributedResponse builds the response from a distributed setup
func (req *Request) getDistributedResponse() (*Response, error) {
	// Type of request
	allBackendsRequested := len(req.Backends) == 0

	// Cluster mode (don't send this request; send sub-requests, build response)
	var wg sync.WaitGroup
	collectedDatasets := make(chan ResultSet, len(nodeAccessor.nodeBackends))
	collectedFailedHashes := make(chan map[string]string, len(nodeAccessor.nodeBackends))
	for nodeID, nodeBackends := range nodeAccessor.nodeBackends {
		node := nodeAccessor.Node(nodeID)
		// limit to requested backends if necessary
		// nodeBackends: all backends handled by current node
		subBackends := req.getSubBackends(allBackendsRequested, nodeBackends)

		// skip node if it doesn't have relevant backends
		if len(subBackends) == 0 {
			collectedDatasets <- ResultSet{}
			collectedFailedHashes <- map[string]string{}
			continue
		}

		if node.isMe {
			// answer locally
			req.SendStatsData = true
			res, err := NewResponse(req)
			if err != nil {
				return nil, err
			}
			req.SendStatsData = false
			if res.Result == nil {
				res.SetResultData()
			}
			collectedDatasets <- res.Result
			collectedFailedHashes <- res.Failed
			continue
		}

		requestData := req.buildDistributedRequestData(subBackends)
		wg.Add(1)
		// Send query to remote node
		err := nodeAccessor.SendQuery(node, "table", requestData, func(responseData interface{}) {
			defer wg.Done()

			// Hash containing metadata in addition to rows
			hash, ok := responseData.(map[string]interface{})
			if !ok {
				return
			}

			// Hash containing error messages
			failedHash, ok := hash["failed"].(map[string]interface{})
			if !ok {
				return
			}
			failedHashStrings := make(map[string]string, len(failedHash))
			for key, val := range failedHash {
				failedHashStrings[key] = fmt.Sprintf("%v", val)
			}

			// Parse data (table rows)
			rowsVariants, ok := hash["data"].([]interface{})
			if !ok {
				return
			}
			rows := make(ResultSet, len(rowsVariants))
			for i, rowVariant := range rowsVariants {
				rowVariants, ok := rowVariant.([]interface{})
				if !ok {
					return
				}
				rows[i] = rowVariants
			}

			// Collect data
			collectedDatasets <- rows
			collectedFailedHashes <- failedHashStrings
		})
		if err != nil {
			return nil, err
		}
	}

	// Wait for all requests
	timeout := 10
	if waitTimeout(&wg, time.Duration(timeout)*time.Second) {
		err := fmt.Errorf("timeout waiting for partner nodes")
		return nil, err
	}
	close(collectedDatasets)

	// Double-check that we have the right number of datasets
	if len(collectedDatasets) != len(nodeAccessor.nodeBackends) {
		err := fmt.Errorf("got %d instead of %d datasets", len(collectedDatasets), len(nodeAccessor.nodeBackends))
		return nil, err
	}

	res := req.mergeDistributedResponse(collectedDatasets, collectedFailedHashes)

	// Process results
	// This also applies sort/offset/limit settings
	if len(res.Request.Stats) > 0 {
		res.CalculateFinalStats()
	} else {
		res.PostProcessing()
	}

	return res, nil
}

func (req *Request) getSubBackends(allBackendsRequested bool, nodeBackends []string) (subBackends []string) {
	// nodeBackends: all backends handled by current node
	for _, nodeBackend := range nodeBackends {
		if nodeBackend == "" {
			continue
		}
		isRequested := allBackendsRequested
		for _, requestedBackend := range req.Backends {
			if requestedBackend == nodeBackend {
				isRequested = true
			}
		}
		if isRequested {
			subBackends = append(subBackends, nodeBackend)
		}
	}
	return
}

func (req *Request) buildDistributedRequestData(subBackends []string) (requestData map[string]interface{}) {
	requestData = make(map[string]interface{})
	if req.Table != TableNone {
		requestData["table"] = req.Table.String()
	}

	// avoid recursion
	requestData["distributed"] = true

	// Set backends for this sub-request
	requestData["backends"] = subBackends

	// No header row
	requestData["sendcolumnsheader"] = false

	// Columns
	// Columns need to be defined or else response will add them
	isStatsRequest := len(req.Stats) != 0
	if len(req.Columns) != 0 {
		requestData["columns"] = req.Columns
	} else if !isStatsRequest {
		panic("columns undefined for dispatched request")
	}

	// Filter
	if len(req.Filter) != 0 || req.FilterStr != "" {
		var str string
		for i := range req.Filter {
			str += req.Filter[i].String("")
		}
		if req.FilterStr != "" {
			str += req.FilterStr
		}
		requestData["filter"] = str
	}

	// Stats
	if isStatsRequest {
		var str string
		for i := range req.Stats {
			str += req.Stats[i].String("Stats")
		}
		requestData["stats"] = str
	}

	// Limit
	// An upper limit is used to make sorting possible
	// Offset is 0 for sub-request (sorting)
	if req.Limit != nil && *req.Limit != 0 {
		requestData["limit"] = *req.Limit + req.Offset
	}

	// Sort order
	if len(req.Sort) != 0 {
		var sort []string
		for i := range req.Sort {
			sortField := req.Sort[i]
			var line string
			var direction string
			switch sortField.Direction {
			case Desc:
				direction = "desc"
			case Asc:
				direction = "asc"
			}
			line = sortField.Name + " " + direction
			sort = append(sort, line)
		}
		requestData["sort"] = sort
	}

	// Get hash with metadata in addition to table rows
	requestData["outputformat"] = OutputFormatWrappedJSON

	return
}

// mergeDistributedResponse returns response object with merged result from distributed requests
func (req *Request) mergeDistributedResponse(collectedDatasets chan ResultSet, collectedFailedHashes chan map[string]string) *Response {
	// Build response object
	res := &Response{
		Code:    200,
		Failed:  make(map[string]string),
		Request: req,
	}

	// Merge data
	isStatsRequest := len(req.Stats) != 0
	req.StatsResult = make(ResultSetStats)
	for currentRows := range collectedDatasets {
		if isStatsRequest {
			// Stats request
			// Value (sum), count (number of elements)
			hasColumns := len(req.Columns)
			for _, row := range currentRows {
				// apply stats querys
				key := ""
				if hasColumns > 0 {
					keys := []string{}
					for x := 0; x < hasColumns; x++ {
						keys = append(keys, *(row[x].(*string)))
					}
					key = strings.Join(keys, ListSepChar1)
				}
				if _, ok := req.StatsResult[key]; !ok {
					req.StatsResult[key] = createLocalStatsCopy(&req.Stats)
				}
				if hasColumns > 0 {
					row = row[hasColumns:]
				}
				for i := range row {
					data := reflect.ValueOf(row[i])
					value := data.Index(0).Interface()
					count := data.Index(1).Interface()
					req.StatsResult[key][i].ApplyValue(interface2float64(value), int(interface2float64(count)))
				}
			}
		} else {
			// Regular request
			res.Result = append(res.Result, currentRows...)
			currentFailedHash := <-collectedFailedHashes
			for id, val := range currentFailedHash {
				res.Failed[id] = val
			}
		}
	}
	return res
}

// ParseRequestHeaderLine parses a single request line
// It returns any error encountered.
func (req *Request) ParseRequestHeaderLine(line []byte) (err error) {
	matched := bytes.SplitN(line, []byte(":"), 2)

	if len(matched) != 2 {
		err = fmt.Errorf("syntax error")
		return
	}
	args := bytes.TrimLeft(matched[1], " ")

	switch string(bytes.ToLower(matched[0])) {
	case "filter":
		err = ParseFilter(args, req.Table, &req.Filter)
		return
	case "and":
		err = ParseFilterOp(And, args, &req.Filter)
		return
	case "or":
		err = ParseFilterOp(Or, args, &req.Filter)
		return
	case "stats":
		err = ParseStats(args, req.Table, &req.Stats)
		return
	case "statsand":
		err = parseStatsOp(And, args, req.Table, &req.Stats)
		return
	case "statsor":
		err = parseStatsOp(Or, args, req.Table, &req.Stats)
		return
	case "sort":
		err = parseSortHeader(&req.Sort, args)
		return
	case "limit":
		req.Limit = new(int)
		err = parseIntHeader(req.Limit, args, 0)
		return
	case "offset":
		err = parseIntHeader(&req.Offset, args, 0)
		return
	case "backends":
		req.Backends = strings.Fields(string(args))
		return
	case "columns":
		req.Columns = strings.Fields(string(args))
		return
	case "responseheader":
		err = parseResponseHeader(&req.ResponseFixed16, args)
		return
	case "outputformat":
		err = parseOutputFormat(&req.OutputFormat, args)
		return
	case "waittimeout":
		err = parseIntHeader(&req.WaitTimeout, args, 1)
		return
	case "waittrigger":
		req.WaitTrigger = string(args)
		return
	case "waitobject":
		req.WaitObject = string(args)
		return
	case "waitcondition":
		err = ParseFilter(args, req.Table, &req.WaitCondition)
		return
	case "waitconditionand":
		err = parseStatsOp(And, args, req.Table, &req.WaitCondition)
		return
	case "waitconditionor":
		err = parseStatsOp(Or, args, req.Table, &req.WaitCondition)
		return
	case "waitconditionnegate":
		req.WaitConditionNegate = true
		return
	case "negate":
		err = ParseFilterNegate(&req.Filter)
		return
	case "keepalive":
		err = parseOnOff(&req.KeepAlive, args)
		return
	case "columnheaders":
		err = parseOnOff(&req.ColumnsHeaders, args)
		return
	case "localtime":
		if log.IsV(LogVerbosityDebug) {
			log.Debugf("Ignoring %s as LMD works on unix timestamps only.", matched[0])
		}
		return
	case "authuser":
		err = parseAuthUser(&req.AuthUser, args)
		return
	}
	err = fmt.Errorf("unrecognized header")
	return
}

func parseResponseHeader(field *bool, value []byte) (err error) {
	if !bytes.Equal(value, []byte("fixed16")) {
		err = errors.New("unrecognized responseformat, only fixed16 is supported")
		return
	}
	*field = true
	return
}

func parseIntHeader(field *int, value []byte, minValue int) (err error) {
	intVal, err := strconv.Atoi(string(value))
	if err != nil || intVal < minValue {
		err = fmt.Errorf("expecting a positive number")
		return
	}
	*field = intVal
	return
}

func parseSortHeader(field *[]*SortField, value []byte) (err error) {
	if len(value) == 0 {
		err = errors.New("invalid sort header, must be 'Sort: <field> <asc|desc>' or 'Sort: custom_variables <name> <asc|desc>'")
		return
	}
	tmp := bytes.SplitN(value, []byte(" "), 3)
	args := ""
	if len(tmp) == 3 {
		if !bytes.Equal(tmp[0], []byte("custom_variables")) && !bytes.Equal(tmp[0], []byte("host_custom_variables")) {
			err = errors.New("invalid sort header, must be 'Sort: <field> <asc|desc>' or 'Sort: custom_variables <name> <asc|desc>'")
			return
		}
		args = string(bytes.ToUpper(tmp[1]))
		tmp[1] = tmp[2]
	}
	var direction SortDirection
	switch {
	case len(tmp) <= 1 || len(tmp[1]) == 0:
		direction = Asc
	case bytes.EqualFold(tmp[1], []byte("asc")):
		direction = Asc
	case bytes.EqualFold(tmp[1], []byte("desc")):
		direction = Desc
	default:
		err = errors.New("unrecognized sort direction, must be asc or desc")
		return
	}
	sortfield := &SortField{
		Name:      string(bytes.ToLower(tmp[0])),
		Direction: direction,
		Args:      args,
	}
	*field = append(*field, sortfield)
	return
}

func parseStatsOp(op GroupOperator, value []byte, table TableName, stats *[]*Filter) (err error) {
	num, cerr := strconv.Atoi(string(value))
	if cerr == nil && num == 0 {
		err = ParseStats([]byte("state != 9999"), table, stats)
		return
	}
	err = ParseFilterOp(op, value, stats)
	if err != nil {
		return
	}
	(*stats)[len(*stats)-1].StatsType = Counter
	return
}

func parseOutputFormat(field *OutputFormat, value []byte) (err error) {
	switch string(value) {
	case "wrapped_json":
		*field = OutputFormatWrappedJSON
	case "json":
		*field = OutputFormatJSON
	case "python":
		*field = OutputFormatPython
	default:
		err = errors.New("unrecognized outputformat, choose from json, wrapped_json and python")
		return
	}
	return
}

// parseOnOff parses a on/off header
// It returns any error encountered.
func parseOnOff(field *bool, value []byte) (err error) {
	switch string(value) {
	case "on":
		*field = true
	case "off":
		*field = false
	default:
		err = fmt.Errorf("must be 'on' or 'off'")
	}
	return
}

func parseAuthUser(field *string, value []byte) (err error) {
	if string(value) != "" {
		*field = string(value)
	} else {
		err = fmt.Errorf("bad request: AuthUser should not be empty")
	}
	return
}

// SetRequestColumns sets  list of used indexes and columns for this request.
func (req *Request) SetRequestColumns() {
	log.Tracef("SetRequestColumns")
	if req.Command != "" {
		return
	}
	table := Objects.Tables[req.Table]
	numColumns := len(req.Columns) + len(req.Stats)
	if numColumns == 0 {
		numColumns = len(table.Columns)
	}
	columns := make([]*Column, 0, numColumns)

	// if no column header was given, return all columns
	// but only if this is no stats query
	if len(req.Columns) == 0 && len(req.Stats) == 0 {
		for j := range table.Columns {
			col := table.Columns[j]
			columns = append(columns, col)
		}
	}

	// build array of requested columns as ResultColumn objects list
	for j := range req.Columns {
		col := table.GetColumnWithFallback(req.Columns[j])
		columns = append(columns, col)
	}
	req.RequestColumns = columns
}

// SetSortColumns set the requestcolumn for the sortfields
func (req *Request) SetSortColumns() (err error) {
	log.Tracef("SetSortColumns")
	if req.Command != "" {
		return
	}
	table := Objects.Tables[req.Table]

	// build array of requested columns as ResultColumn objects list
	for j := range req.Sort {
		col := table.GetColumn(req.Sort[j].Name)
		if col == nil {
			err = fmt.Errorf("unknown sort column %s", req.Sort[j].Name)
		}
		req.Sort[j].Column = col
	}

	return
}

// parseResult parses the result bytes and returns the data table and optional meta data for wrapped_json requests
func (req *Request) parseResult(resBytes *[]byte) (*ResultSet, *ResultMetaData, error) {
	var err error
	meta := &ResultMetaData{}
	if len(*resBytes) == 0 || (string((*resBytes)[0]) != "{" && string((*resBytes)[0]) != "[") {
		err = errors.New(strings.TrimSpace(string(*resBytes)))
		return nil, nil, &PeerError{msg: fmt.Sprintf("response does not look like a json result: %s", err.Error()), kind: ResponseError, req: req, resBytes: resBytes}
	}
	if req.OutputFormat == OutputFormatWrappedJSON {
		dataBytes, dataType, _, jErr := jsonparser.Get(*resBytes, "columns")
		if jErr != nil {
			log.Debugf("column header parse error: %s", jErr.Error())
		} else if dataType == jsonparser.Array {
			var columns []string
			err = json.Unmarshal(dataBytes, &columns)
			if err != nil {
				return nil, nil, &PeerError{msg: fmt.Sprintf("column header parse error: %s", err.Error()), kind: ResponseError, req: req, resBytes: resBytes}
			}
			meta.Columns = columns
		}
		totalCount, jErr := jsonparser.GetInt(*resBytes, "total_count")
		if jErr != nil {
			return nil, nil, &PeerError{msg: fmt.Sprintf("total header parse error: %s", jErr.Error()), kind: ResponseError, req: req, resBytes: resBytes}
		}
		meta.Total = totalCount

		dataBytes, _, _, jErr = jsonparser.Get(*resBytes, "data")
		if jErr != nil {
			return nil, nil, &PeerError{msg: fmt.Sprintf("data header parse error: %s", jErr.Error()), kind: ResponseError, req: req, resBytes: resBytes}
		}
		resBytes = &dataBytes
	}
	res := make(ResultSet, 0)
	offset, jErr := jsonparser.ArrayEach(*resBytes, func(rowBytes []byte, _ jsonparser.ValueType, _ int, aErr error) {
		if aErr != nil {
			err = aErr
			return
		}
		row, dErr := djson.DecodeArray(rowBytes)
		if dErr != nil {
			// try to fix invalid escape sequences and unknown utf8 characters
			if strings.Contains(dErr.Error(), "invalid character") {
				rowBytes = bytesToValidUTF8(rowBytes, []byte("\uFFFD"))
				row, dErr = djson.DecodeArray(rowBytes)
			}
			// still failing
			if dErr != nil {
				err = dErr
				return
			}
			log.Debugf("fixed invalid characters in json data")
		}
		res = append(res, row)
	})
	// trailing comma error will be ignored
	if jErr != nil && offset < len(*resBytes)-3 {
		return nil, nil, jErr
	}
	if err != nil {
		return nil, nil, err
	}

	return &res, meta, err
}

// IsDefaultSortOrder returns true if the sortfields are the default for the given table.
func (req *Request) IsDefaultSortOrder() bool {
	if len(req.Sort) == 0 {
		return true
	}
	switch req.Table {
	case TableServices:
		if len(req.Sort) == 2 && req.Sort[0].Name == "host_name" && req.Sort[0].Direction == Asc && req.Sort[1].Name == "description" && req.Sort[1].Direction == Asc {
			return true
		}
	case TableHosts:
		if len(req.Sort) == 1 && req.Sort[0].Name == "name" && req.Sort[0].Direction == Asc {
			return true
		}
	}
	return false
}
