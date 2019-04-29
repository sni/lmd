package main

import (
	"bufio"
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
)

// Request defines a livestatus request object.
type Request struct {
	noCopy              noCopy
	Table               string
	Command             string
	Columns             []string        // parsed columns field
	RequestColumns      []RequestColumn // calculated/expanded columns list
	ReqColIndex         []int
	Filter              []*Filter
	FilterStr           string
	Stats               []*Filter
	StatsResult         map[string][]*Filter
	Limit               *int
	Offset              int
	Sort                []*SortField
	ResponseFixed16     bool
	OutputFormat        string
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
}

// RequestColumn is a column container for results
type RequestColumn struct {
	Name   string
	Type   ColumnType
	Column *Column // reference to the real column
	Index  int     // index in the request and the position in the result row
	Hidden bool    // true if column is not used in the output and ex. just for sorting
}

// SortDirection can be either Asc or Desc
type SortDirection int

// The only possible SortDirection are "Asc" and "Desc" for
// sorting ascending or descending.
const (
	_ SortDirection = iota
	Asc
	Desc
)

// outputformat types
const (
	// WRAPPEDJSON is json data along with meta data
	WRAPPEDJSON = "wrapped_json"
)

// String converts a SortDirection back to the original string.
func (s *SortDirection) String() string {
	switch *s {
	case Asc:
		return ("asc")
	case Desc:
		return ("desc")
	}
	log.Panicf("not implemented")
	return ""
}

// SortField defines a single sort entry
type SortField struct {
	Name      string
	Direction SortDirection
	Index     int
	Args      string
}

// GroupOperator is the operator used to combine multiple filter or stats header.
type GroupOperator int

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
	str = "GET " + req.Table + "\n"
	if req.ResponseFixed16 {
		str += "ResponseHeader: fixed16\n"
	}
	if req.OutputFormat != "" {
		str += "OutputFormat: " + req.OutputFormat + "\n"
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
	for _, f := range req.Filter {
		str += f.String("")
	}
	if req.FilterStr != "" {
		str += req.FilterStr
	}
	for _, s := range req.Stats {
		str += s.String("Stats")
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
	for _, f := range req.WaitCondition {
		str += f.String("WaitCondition")
	}
	for _, s := range req.Sort {
		str += fmt.Sprintf("Sort: %s %s\n", s.Name, s.Direction.String())
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
	if log.IsV(2) && firstLine != "" {
		log.Debugf("request: %s", firstLine)
	}

	ok, err := req.ParseRequestAction(&firstLine)
	if err != nil || !ok {
		req = nil
		return
	}

	for {
		line, berr := b.ReadString('\n')
		if berr != nil && berr != io.EOF {
			err = berr
			return
		}
		size += len(line)
		line = strings.TrimSpace(line)
		if line == "" {
			break
		}

		if log.IsV(2) {
			log.Debugf("request: %s", line)
		}
		perr := req.ParseRequestHeaderLine(&line)
		if perr != nil {
			err = perr
			return
		}
		if berr == io.EOF {
			break
		}
	}

	err = req.SetColumnsIndex()
	if err != nil {
		return
	}

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

		req.Table = matched[1]
		_, ok := Objects.Tables[req.Table]
		if !ok {
			err = fmt.Errorf("bad request: table %s does not exist", req.Table)
		}
		valid = true
		return
	}

	// or a command
	if strings.HasPrefix(*firstLine, "COMMAND ") {
		matched := reRequestCommand.FindStringSubmatch(*firstLine)
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
	collectedDatasets := make(chan [][]interface{}, len(nodeAccessor.nodeBackends))
	collectedFailedHashes := make(chan map[string]string, len(nodeAccessor.nodeBackends))
	for nodeID, nodeBackends := range nodeAccessor.nodeBackends {
		node := nodeAccessor.Node(nodeID)
		// limit to requested backends if necessary
		// nodeBackends: all backends handled by current node
		subBackends := req.getSubBackends(allBackendsRequested, nodeBackends)

		// skip node if it doesn't have relevant backends
		if len(subBackends) == 0 {
			collectedDatasets <- [][]interface{}{}
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
			rows := make([][]interface{}, len(rowsVariants))
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
	res.PostProcessing()

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
	if req.Table != "" {
		requestData["table"] = req.Table
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
		for _, f := range req.Filter {
			str += f.String("")
		}
		if req.FilterStr != "" {
			str += req.FilterStr
		}
		requestData["filter"] = str
	}

	// Stats
	if isStatsRequest {
		var str string
		for _, f := range req.Stats {
			str += f.String("Stats")
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
		for _, sortField := range req.Sort {
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
	requestData["outputformat"] = WRAPPEDJSON

	return
}

// mergeDistributedResponse returns response object with merged result from distributed requests
func (req *Request) mergeDistributedResponse(collectedDatasets chan [][]interface{}, collectedFailedHashes chan map[string]string) *Response {
	// Build response object
	res := &Response{
		Code:    200,
		Failed:  make(map[string]string),
		Request: req,
	}

	// Merge data
	isStatsRequest := len(req.Stats) != 0
	req.StatsResult = make(map[string][]*Filter)
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
						keys = append(keys, row[x].(string))
					}
					key = strings.Join(keys, ";")
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
					req.StatsResult[key][i].ApplyValue(numberToFloat(&value), int(numberToFloat(&count)))
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
func (req *Request) ParseRequestHeaderLine(line *string) (err error) {
	matched := strings.SplitN(*line, ":", 2)
	if len(matched) != 2 {
		err = fmt.Errorf("bad request header: %s", *line)
		return
	}
	matched[0] = strings.ToLower(matched[0])
	matched[1] = strings.TrimLeft(matched[1], " ")

	switch matched[0] {
	case "filter":
		err = ParseFilter(matched[1], line, req.Table, &req.Filter)
		return
	case "and":
		fallthrough
	case "or":
		err = ParseFilterOp(matched[0], matched[1], line, &req.Filter)
		return
	case "stats":
		err = ParseStats(matched[1], line, req.Table, &req.Stats)
		return
	case "statsand":
		err = parseStatsOp("and", matched[1], line, req.Table, &req.Stats)
		return
	case "statsor":
		err = parseStatsOp("or", matched[1], line, req.Table, &req.Stats)
		return
	case "sort":
		err = parseSortHeader(&req.Sort, matched[1])
		return
	case "limit":
		req.Limit = new(int)
		err = parseIntHeader(req.Limit, matched[0], matched[1], 0)
		return
	case "offset":
		err = parseIntHeader(&req.Offset, matched[0], matched[1], 0)
		return
	case "backends":
		req.Backends = strings.Split(matched[1], " ")
		return
	case "columns":
		req.Columns = strings.Split(matched[1], " ")
		return
	case "responseheader":
		err = parseResponseHeader(&req.ResponseFixed16, matched[1])
		return
	case "outputformat":
		err = parseOutputFormat(&req.OutputFormat, matched[1])
		return
	case "waittimeout":
		err = parseIntHeader(&req.WaitTimeout, matched[0], matched[1], 1)
		return
	case "waittrigger":
		req.WaitTrigger = matched[1]
		return
	case "waitobject":
		req.WaitObject = matched[1]
		return
	case "waitcondition":
		err = ParseFilter(matched[1], line, req.Table, &req.WaitCondition)
		return
	case "waitconditionand":
		err = parseStatsOp("and", matched[1], line, req.Table, &req.WaitCondition)
		return
	case "waitconditionor":
		err = parseStatsOp("or", matched[1], line, req.Table, &req.WaitCondition)
		return
	case "waitconditionnegate":
		req.WaitConditionNegate = true
		return
	case "keepalive":
		err = parseOnOff(&req.KeepAlive, line, matched[1])
		return
	case "columnheaders":
		err = parseOnOff(&req.ColumnsHeaders, line, matched[1])
		return
	case "localtime":
		if log.IsV(2) {
			log.Debugf("Ignoring %s as LMD works on unix timestamps only.", *line)
		}
		return
	default:
		err = fmt.Errorf("bad request: unrecognized header %s", *line)
		return
	}
}

func parseResponseHeader(field *bool, value string) (err error) {
	if value != "fixed16" {
		err = errors.New("bad request: unrecognized responseformat, only fixed16 is supported")
		return
	}
	*field = true
	return
}

func parseIntHeader(field *int, header string, value string, minValue int) (err error) {
	intVal, err := strconv.Atoi(value)
	if err != nil || intVal < minValue {
		err = fmt.Errorf("bad request: %s must be a positive number", header)
		return
	}
	*field = intVal
	return
}

func parseSortHeader(field *[]*SortField, value string) (err error) {
	args := ""
	tmp := strings.SplitN(value, " ", 3)
	if len(tmp) < 1 {
		err = errors.New("bad request: invalid sort header, must be 'Sort: <field> <asc|desc>' or 'Sort: custom_variables <name> <asc|desc>'")
		return
	}
	if len(tmp) == 1 {
		// Add default sorting option
		tmp = append(tmp, "asc")
	}
	if len(tmp) == 3 {
		if tmp[0] != "custom_variables" && tmp[0] != "host_custom_variables" {
			err = errors.New("bad request: invalid sort header, must be 'Sort: <field> <asc|desc>' or 'Sort: custom_variables <name> <asc|desc>'")
			return
		}
		args = strings.ToUpper(tmp[1])
		tmp[1] = tmp[2]
	}
	var direction SortDirection
	switch strings.ToLower(tmp[1]) {
	case "asc":
		direction = Asc
	case "desc":
		direction = Desc
	default:
		err = errors.New("bad request: unrecognized sort direction, must be asc or desc")
		return
	}
	*field = append(*field, &SortField{Name: strings.ToLower(tmp[0]), Direction: direction, Args: args})
	return
}

func parseStatsOp(op string, value string, line *string, table string, stats *[]*Filter) (err error) {
	num, cerr := strconv.Atoi(value)
	if cerr == nil && num == 0 {
		newline := "Stats: state != 9999"
		err = ParseStats("state != 9999", &newline, table, stats)
		return
	}
	err = ParseFilterOp(op, value, line, stats)
	if err != nil {
		return
	}
	(*stats)[len(*stats)-1].StatsType = Counter
	return
}

func parseOutputFormat(field *string, value string) (err error) {
	switch value {
	case WRAPPEDJSON:
		*field = value
	case "json":
		*field = value
	case "python":
		*field = value
	default:
		err = errors.New("bad request: unrecognized outputformat, only json and wrapped_json is supported")
		return
	}
	return
}

// parseOnOff parses a on/off header
// It returns any error encountered.
func parseOnOff(field *bool, line *string, value string) (err error) {
	switch value {
	case "on":
		*field = true
	case "off":
		*field = false
	default:
		err = fmt.Errorf("bad request: must be 'on' or 'off' in %s", *line)
	}
	return
}

// SetColumnsIndex returns a list of used indexes and columns for this request.
func (req *Request) SetColumnsIndex() (err error) {
	log.Tracef("SetColumnsIndex")
	if req.Command != "" {
		return
	}
	indexes := make([]int, 0)
	columns := make([]RequestColumn, 0)
	requestColumnsMap := make(map[string]int)
	table := Objects.Tables[req.Table]

	// if no column header was given, return all columns
	// but only if this is no stats query
	if len(req.Columns) == 0 && len(req.Stats) == 0 {
		for j, col := range table.Columns {
			if col.Update != RefUpdate && col.Name != EMPTY {
				indexes, columns = addRequestColumn(table, col.Name, j, requestColumnsMap, indexes, columns, false)
			}
		}
	}

	// build array of requested columns as ResultColumn objects list
	for j, colName := range req.Columns {
		indexes, columns = addRequestColumn(table, colName, j, requestColumnsMap, indexes, columns, false)
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
			indexes, columns = addRequestColumn(table, s.Name, i, requestColumnsMap, indexes, columns, true)
		}
		s.Index = i
	}

	req.RequestColumns = columns
	req.ReqColIndex = indexes

	return
}

func addRequestColumn(table *Table, colName string, requestIndex int, requestColumnsMap map[string]int, indexes []int, columns []RequestColumn, hidden bool) ([]int, []RequestColumn) {
	colName = strings.ToLower(colName)
	i, ok := table.ColumnsIndex[colName]
	if !ok {
		if table.PassthroughOnly {
			indexes = append(indexes, -1)
			columns = append(columns, RequestColumn{Name: colName, Type: StringCol, Index: requestIndex, Hidden: hidden})
			requestColumnsMap[colName] = requestIndex
			return indexes, columns
		}
		if !fixBrokenClientsRequestColumn(&colName, table.Name) {
			colName = EMPTY
		}
		i = table.ColumnsIndex[colName]
	}
	col := table.Columns[i]
	if col.Type == VirtCol {
		indexes = append(indexes, col.VirtMap.Index)
	} else {
		indexes = append(indexes, i)
	}
	columns = append(columns, RequestColumn{Name: colName, Type: col.Type, Index: requestIndex, Column: col, Hidden: hidden})
	requestColumnsMap[colName] = requestIndex
	return indexes, columns
}
