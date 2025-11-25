package lmd

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"net"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/willabides/rjson"
)

// Request defines a livestatus request object.
type Request struct {
	noCopy              noCopy
	StatsResult         *ResultSetStats
	lmd                 *Daemon
	BackendsMap         map[string]string
	BackendErrors       map[string]string
	Limit               *int
	id                  string
	AuthUser            string
	Command             string
	WaitTrigger         string
	FilterStr           string
	WaitObject          string
	Stats               []*Filter
	StatsGrouped        []*Filter // optimized stats groups
	Filter              []*Filter
	Sort                []*SortField
	WaitCondition       []*Filter
	RequestColumns      []*Column // calculated/expanded columns list
	Backends            []string
	Columns             []string // parsed columns field
	Offset              int
	WaitTimeout         int // milliseconds
	NumFilter           int
	Table               TableName
	ColumnsHeaders      bool
	SendStatsData       bool
	OutputFormat        OutputFormat
	ResponseFixed16     bool
	WaitConditionNegate bool
	KeepAlive           bool
}

// SortDirection can be either Asc or Desc.
type SortDirection uint8

// The only possible SortDirection are "Asc" and "Desc" for
// sorting ascending or descending.
const (
	_ SortDirection = iota
	Asc
	Desc
)

// ParseOptions can be used to customize the request parser.
type ParseOptions int

// The only possible SortDirection are "Asc" and "Desc" for
// sorting ascending or descending.
const (
	// ParseDefault parses the request as is.
	ParseDefault ParseOptions = 0

	// ParseOptimize trys to use lower case columns and string matches instead of regular expressions.
	ParseOptimize ParseOptions = 1 << iota
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

// OutputFormat defines the format used to return the result.
type OutputFormat uint8

// available output formats.
const (
	OutputFormatDefault OutputFormat = iota
	OutputFormatJSON
	OutputFormatWrappedJSON
	OutputFormatPython
	OutputFormatPython3
)

// String converts a SortDirection back to the original string.
func (o *OutputFormat) String() string {
	switch *o {
	case OutputFormatJSON, OutputFormatDefault:
		return "json"
	case OutputFormatWrappedJSON:
		return "wrapped_json"
	case OutputFormatPython:
		return "python"
	case OutputFormatPython3:
		return "python3"
	}
	log.Panicf("not implemented")

	return ""
}

// SortField defines a single sort entry.
type SortField struct {
	noCopy    noCopy
	Column    *Column
	Name      string
	Args      string
	Index     int
	Direction SortDirection
	Group     bool
}

// GroupOperator is the operator used to combine multiple filter or stats header.
type GroupOperator uint8

// The only possible GroupOperator are "And" and "Or".
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
	log.Panicf("not implemented: %#v", op)

	return ""
}

// ResultMetaData contains meta from the response data.
type ResultMetaData struct {
	Request     *Request      // the request itself
	Res         ResultSet     `json:"data"`         // result data temp store
	Columns     []string      `json:"columns"`      // list of requested columns
	Total       int64         `json:"total_count"`  // total number of result rows
	RowsScanned int64         `json:"rows_scanned"` // total number of scanned rows for this result
	Duration    time.Duration // response time in seconds
	Size        int           // result size in bytes
}

var (
	reRequestAction  = regexp.MustCompile(`^GET +([a-z]+)$`)
	reRequestCommand = regexp.MustCompile(`^COMMAND +(\[\d+\].*)$`)
)

// ParseRequest reads from a connection and returns a single requests.
// It returns a the requests and any errors encountered.
func ParseRequest(ctx context.Context, lmd *Daemon, c net.Conn) (req *Request, err error) {
	b := bufio.NewReader(c)
	localAddr := c.LocalAddr().String()
	req, size, err := NewRequest(ctx, lmd, b, lmd.defaultRequestParseOption)
	promFrontendBytesReceived.WithLabelValues(localAddr).Add(float64(size))

	return req, err
}

// ParseRequests reads from a connection and returns all requests read.
// It returns a list of requests and any errors encountered.
func ParseRequests(ctx context.Context, lmd *Daemon, c net.Conn) (reqs []*Request, err error) {
	b := bufio.NewReader(c)
	localAddr := c.LocalAddr().String()
	eof := false
	for {
		req, size, err := NewRequest(ctx, lmd, b, lmd.defaultRequestParseOption)
		promFrontendBytesReceived.WithLabelValues(localAddr).Add(float64(size))
		if err != nil {
			if errors.Is(err, io.EOF) {
				eof = true
			} else {
				return nil, err
			}
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
	if eof {
		if len(reqs) == 0 {
			return nil, io.EOF
		}
		reqs[len(reqs)-1].KeepAlive = false
	}

	return reqs, nil
}

// String returns the request object as livestatus query string.
func (req *Request) String() (str string) {
	// Commands are easy passthrough
	if req.Command != "" {
		return req.Command + "\n\n"
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
		str += "ColumnHeaders: on\n"
	}
	if req.KeepAlive {
		str += "KeepAlive: on\n"
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
		str += "WaitConditionNegate\n"
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

	return str
}

// NewRequest reads a buffer and creates a new request object.
// It returns the request along with the number of bytes read and any error.
func NewRequest(ctx context.Context, lmd *Daemon, buf *bufio.Reader, options ParseOptions) (req *Request, size int, err error) {
	firstLine, err := buf.ReadString('\n')
	if errors.Is(err, io.EOF) {
		if firstLine == "" {
			return nil, 0, fmt.Errorf("read: %w %s", err, err.Error())
		}

		// ignore eof error, continue with this request
		err = nil
	}

	// Network errors will be logged in the listener
	var netErr net.Error
	//nolint:nilnesserr // false positive, doesn't work without nil check
	if err != nil && errors.Is(err, netErr) {
		return nil, 0, err
	}

	req = &Request{lmd: lmd, ColumnsHeaders: false, KeepAlive: false}
	ctx = context.WithValue(ctx, CtxRequest, req.ID())
	size += len(firstLine)
	firstLine = strings.TrimSpace(firstLine)
	// probably a open connection without new data from a keepalive request
	if firstLine != "" {
		logWith(ctx).Debugf("request: %s", firstLine)
	}

	ok, err := req.ParseRequestAction(&firstLine)
	if err != nil || !ok {
		return nil, 0, err
	}

	for {
		line, berr := buf.ReadBytes('\n')
		if berr != nil && berr != io.EOF {
			return nil, 0, fmt.Errorf("read: %s", berr.Error())
		}
		size += len(line)
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			break
		}

		logWith(ctx).Debugf("request: %s", line)
		perr := req.ParseRequestHeaderLine(line, options)
		if perr != nil {
			return nil, 0, fmt.Errorf("bad request: %s in: %s", perr.Error(), line)
		}
		if lmd.Config.MaxQueryFilter > 0 && req.NumFilter > lmd.Config.MaxQueryFilter {
			return nil, 0, fmt.Errorf("bad request: maximum number of query filter reached")
		}
		if errors.Is(berr, io.EOF) {
			req.KeepAlive = false

			break
		}
	}

	// remove unnecessary filter indentation
	if options&ParseOptimize != 0 {
		req.optimizeFilter()
		req.StatsGrouped = req.optimizeStatsGroups(req.Stats, true)
	}

	req.SetRequestColumns()
	err = req.SetSortColumns()

	return req, size, err
}

// NewRequestFromString creates a new request object from query string.
// It returns the request along with the number of bytes read and any error.
func NewRequestFromString(ctx context.Context, lmd *Daemon, query *string, options ParseOptions) (req *Request, size int, err error) {
	buf := bufio.NewReader(bytes.NewBufferString(*query))

	return (NewRequest(ctx, lmd, buf, options))
}

// ID returns the uniq request id.
func (req *Request) ID() string {
	if req.id != "" {
		return req.id
	}
	req.id = fmt.Sprintf("r:%x", sha256.Sum256([]byte(fmt.Sprintf("%d-%d", time.Now().Nanosecond(), rand.Int()))))[0:8]

	return req.id
}

// ParseRequestAction parses the first line from a request which
// may start with GET or COMMAND.
func (req *Request) ParseRequestAction(firstLine *string) (valid bool, err error) {
	// normal get request?
	if strings.HasPrefix(*firstLine, "GET ") {
		matched := reRequestAction.FindStringSubmatch(*firstLine)
		if len(matched) != 2 {
			return false, fmt.Errorf("bad request: %s", *firstLine)
		}

		tableName, tErr := NewTableName(matched[1])
		if tErr != nil {
			return false, fmt.Errorf("bad request: %s", tErr.Error())
		}
		req.Table = tableName

		return true, nil
	}

	// or a command
	if strings.HasPrefix(*firstLine, "COMMAND ") {
		matched := reRequestCommand.FindStringSubmatch(*firstLine)
		if len(matched) < 1 {
			return false, fmt.Errorf("bad request: %s", *firstLine)
		}
		req.Command = matched[0]

		return true, nil
	}

	// empty request
	if *firstLine == "" {
		return false, nil
	}

	return false, fmt.Errorf("bad request: %s", *firstLine)
}

// BuildResponse builds the response for a given request.
// It returns the Response object and any error encountered.
func (req *Request) BuildResponse(ctx context.Context) (*Response, error) {
	// Run single request if possible
	if req.lmd.nodeAccessor == nil || !req.lmd.nodeAccessor.IsClustered() {
		// Single mode (send request and return response)
		res, _, err := NewResponse(ctx, req, nil)

		return res, err
	}

	// Determine if request for this node only (if backends specified)
	allBackendsRequested := len(req.Backends) == 0
	isForOurBackends := false // request for our own backends only
	if !allBackendsRequested {
		isForOurBackends = true
		for _, backend := range req.Backends {
			isOurs := req.lmd.nodeAccessor.IsOurBackend(backend)
			isForOurBackends = isForOurBackends && isOurs
		}
	}

	// Return local result if its not distributed at all
	if isForOurBackends {
		res, _, err := NewResponse(ctx, req, nil)

		return res, err
	}

	// Distribute request
	return req.getDistributedResponse(ctx)
}

// BuildResponseSend builds the response and sends to the given connection.
// It returns the transferred size or an error.
func (req *Request) BuildResponseSend(ctx context.Context, client *ClientConnection) (int64, error) {
	// Run single request if possible
	if req.lmd.nodeAccessor == nil || !req.lmd.nodeAccessor.IsClustered() {
		// Single mode (send request)
		_, size, err := NewResponse(ctx, req, client)

		return size, err
	}

	res, err := req.BuildResponse(ctx)
	if err != nil {
		return 0, err
	}

	return res.Send(client)
}

// getDistributedResponse builds the response from a distributed setup.
func (req *Request) getDistributedResponse(ctx context.Context) (*Response, error) {
	// Type of request
	allBackendsRequested := len(req.Backends) == 0

	// Cluster mode (don't send this request; send sub-requests, build response)
	var waitGroup sync.WaitGroup
	collectedDatasets := make(chan ResultSet, len(req.lmd.nodeAccessor.nodeBackends))
	collectedFailedHashes := make(chan map[string]string, len(req.lmd.nodeAccessor.nodeBackends))
	for nodeID, nodeBackends := range req.lmd.nodeAccessor.nodeBackends {
		node := req.lmd.nodeAccessor.Node(nodeID)
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
			res, _, err := NewResponse(ctx, req, nil)
			if err != nil {
				return nil, err
			}
			req.SendStatsData = false
			if res.result == nil {
				res.SetResultData()
			}
			collectedDatasets <- res.result
			collectedFailedHashes <- res.failed

			continue
		}

		requestData := req.buildDistributedRequestData(subBackends)
		waitGroup.Add(1)
		// Send query to remote node
		err := req.lmd.nodeAccessor.SendQuery(ctx, node, "table", requestData, func(responseData any) {
			defer waitGroup.Done()

			// Hash containing metadata in addition to rows
			hash, ok := responseData.(map[string]any)
			if !ok {
				return
			}

			// Hash containing error messages
			failedHash, ok := hash["failed"].(map[string]any)
			if !ok {
				return
			}
			failedHashStrings := make(map[string]string, len(failedHash))
			for key, val := range failedHash {
				failedHashStrings[key] = fmt.Sprintf("%v", val)
			}

			// Parse data (table rows)
			rowsVariants, ok := hash["data"].([]any)
			if !ok {
				return
			}
			rows := make(ResultSet, len(rowsVariants))
			for i, rowVariant := range rowsVariants {
				rowVariants, ok := rowVariant.([]any)
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
	if waitTimeout(ctx, &waitGroup, time.Duration(timeout)*time.Second) {
		err := fmt.Errorf("timeout waiting for partner nodes")

		return nil, err
	}
	close(collectedDatasets)

	// Double-check that we have the right number of datasets
	if len(collectedDatasets) != len(req.lmd.nodeAccessor.nodeBackends) {
		err := fmt.Errorf("got %d instead of %d datasets", len(collectedDatasets), len(req.lmd.nodeAccessor.nodeBackends))

		return nil, err
	}

	res := req.mergeDistributedResponse(collectedDatasets, collectedFailedHashes)

	// Process results
	// This also applies sort/offset/limit settings
	if len(res.request.Stats) > 0 {
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

	return subBackends
}

func (req *Request) buildDistributedRequestData(subBackends []string) (requestData map[string]any) {
	requestData = make(map[string]any)
	if req.Table != TableNone {
		requestData["table"] = req.Table.String()
	}

	// avoid recursion
	requestData["distributed"] = true

	// Set backends for this sub-request
	requestData["backends"] = subBackends

	// No header row
	requestData["sendcolumnsheader"] = false

	// Columns need to be defined or else response will add them
	isStatsRequest := len(req.Stats) != 0
	if len(req.Columns) != 0 {
		requestData["columns"] = req.Columns
	} else if !isStatsRequest {
		panic("columns undefined for dispatched request")
	}

	// Filter
	if len(req.Filter) != 0 || req.FilterStr != "" {
		str := strings.Builder{}
		for i := range req.Filter {
			str.WriteString(req.Filter[i].String(""))
		}
		if req.FilterStr != "" {
			str.WriteString(req.FilterStr)
		}
		requestData["filter"] = str.String()
	}

	// Stats
	if isStatsRequest {
		str := strings.Builder{}
		for i := range req.Stats {
			str.WriteString(req.Stats[i].String("Stats"))
		}
		requestData["stats"] = str.String()
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

	return requestData
}

// mergeDistributedResponse returns response object with merged result from distributed requests.
func (req *Request) mergeDistributedResponse(collectedDatasets chan ResultSet, collectedFailedHashes chan map[string]string) *Response {
	// Build response object
	res := &Response{
		code:    200,
		failed:  make(map[string]string),
		request: req,
	}

	// Merge data
	isStatsRequest := len(req.Stats) != 0
	req.StatsResult = NewResultSetStats()
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
					for x := range hasColumns {
						keys = append(keys, interface2stringNoDedup(row[x]))
					}
					key = strings.Join(keys, ListSepChar1)
				}
				if _, ok := req.StatsResult.Stats[key]; !ok {
					req.StatsResult.Stats[key] = createLocalStatsCopy(req.Stats)
				}
				if hasColumns > 0 {
					row = row[hasColumns:]
				}
				for i := range row {
					data := reflect.ValueOf(row[i])
					value := data.Index(0).Interface()
					count := data.Index(1).Interface()
					req.StatsResult.Stats[key][i].ApplyValue(interface2float64(value), int(interface2float64(count)))
				}
			}
		} else {
			// Regular request
			res.result = append(res.result, currentRows...)
			currentFailedHash := <-collectedFailedHashes
			maps.Copy(res.failed, currentFailedHash)
		}
	}

	return res
}

// ParseRequestHeaderLine parses a single request line
// It returns any error encountered.
func (req *Request) ParseRequestHeaderLine(line []byte, options ParseOptions) (err error) {
	matched := bytes.SplitN(line, []byte(":"), 2)

	if len(matched) != 2 {
		return fmt.Errorf("syntax error")
	}
	args := bytes.TrimLeft(matched[1], " ")

	switch string(bytes.ToLower(matched[0])) {
	case "filter":
		err = ParseFilter(args, req.Table, &req.Filter, options)
		req.NumFilter++

		return err
	case "and":
		return parseFilterGroupOp(And, args, &req.Filter)
	case "or":
		return parseFilterGroupOp(Or, args, &req.Filter)
	case "stats":
		err = ParseStats(args, req.Table, &req.Stats, options)
		req.NumFilter++

		return err
	case "statsand":
		return parseStatsGroupOp(And, args, req.Table, &req.Stats, options)
	case "statsor":
		return parseStatsGroupOp(Or, args, req.Table, &req.Stats, options)
	case "sort":
		return parseSortHeader(&req.Sort, args)
	case "limit":
		req.Limit = new(int)

		return parseIntHeader(req.Limit, args, 0)
	case "offset":
		return parseIntHeader(&req.Offset, args, 0)
	case "backends":
		req.Backends = strings.Fields(string(args))

		return nil
	case "columns":
		req.Columns = append(req.Columns, strings.Fields(string(args))...)

		return nil
	case "responseheader":
		return parseResponseHeader(&req.ResponseFixed16, args)
	case "outputformat":
		return parseOutputFormat(&req.OutputFormat, args)
	case "waittimeout":
		return parseIntHeader(&req.WaitTimeout, args, 1)
	case "waittrigger":
		req.WaitTrigger = string(args)

		return nil
	case "waitobject":
		req.WaitObject = string(args)

		return nil
	case "waitcondition":
		req.NumFilter++

		return ParseFilter(args, req.Table, &req.WaitCondition, options)
	case "waitconditionand":
		return parseStatsGroupOp(And, args, req.Table, &req.WaitCondition, options)
	case "waitconditionor":
		return parseStatsGroupOp(Or, args, req.Table, &req.WaitCondition, options)
	case "waitconditionnegate":
		req.WaitConditionNegate = true

		return nil
	case "negate":
		return ParseFilterNegate(req.Filter)
	case "keepalive":
		return parseOnOff(&req.KeepAlive, args)
	case "columnheaders":
		return parseOnOff(&req.ColumnsHeaders, args)
	case "localtime":
		if log.IsV(LogVerbosityDebug) {
			logWith(req).Debugf("Ignoring %s as LMD works on unix timestamps only.", matched[0])
		}

		return nil
	case "authuser":
		return parseAuthUser(&req.AuthUser, args)
	case "statsnegate":
		return ParseFilterNegate(req.Stats)
	}

	return fmt.Errorf("unrecognized header")
}

func parseResponseHeader(field *bool, value []byte) (err error) {
	if !bytes.Equal(value, []byte("fixed16")) {
		return errors.New("unrecognized responseformat, only fixed16 is supported")
	}
	*field = true

	return err
}

func parseIntHeader(field *int, value []byte, minValue int) (err error) {
	intVal, err := strconv.Atoi(string(value))
	if err != nil || intVal < minValue {
		return fmt.Errorf("expecting a positive number")
	}
	*field = intVal

	return err
}

func parseSortHeader(field *[]*SortField, value []byte) (err error) {
	if len(value) == 0 {
		return errors.New("invalid sort header, must be 'Sort: <field> <asc|desc>' or 'Sort: custom_variables <name> <asc|desc>'")
	}
	tmp := bytes.SplitN(value, []byte(" "), 3)
	args := ""
	if len(tmp) == 3 {
		if !bytes.Equal(tmp[0], []byte("custom_variables")) && !bytes.Equal(tmp[0], []byte("host_custom_variables")) {
			return errors.New("invalid sort header, must be 'Sort: <field> <asc|desc>' or 'Sort: custom_variables <name> <asc|desc>'")
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
		return errors.New("unrecognized sort direction, must be asc or desc")
	}
	sortfield := &SortField{
		Name:      string(bytes.ToLower(tmp[0])),
		Direction: direction,
		Args:      args,
	}
	*field = append(*field, sortfield)

	return nil
}

func parseStatsGroupOp(op GroupOperator, value []byte, table TableName, stats *[]*Filter, options ParseOptions) (err error) {
	num, cerr := strconv.Atoi(string(value))
	if cerr == nil && num == 0 {
		return ParseStats([]byte("state != 9999"), table, stats, options)
	}
	err = parseFilterGroupOp(op, value, stats)
	if err != nil {
		return err
	}
	(*stats)[len(*stats)-1].statsType = Counter

	return err
}

func parseOutputFormat(field *OutputFormat, value []byte) (err error) {
	switch string(value) {
	case "wrapped_json":
		*field = OutputFormatWrappedJSON
	case "json":
		*field = OutputFormatJSON
	case "python":
		*field = OutputFormatPython
	case "python3":
		*field = OutputFormatPython3
	default:
		return errors.New("unrecognized outputformat, choose from json, wrapped_json, python and python3")
	}

	return err
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

	return err
}

func parseAuthUser(field *string, value []byte) (err error) {
	user := string(value)
	if user != "" {
		*field = user
	} else {
		err = fmt.Errorf("bad request: AuthUser should not be empty")
	}

	return err
}

// SetRequestColumns sets  list of used indexes and columns for this request.
func (req *Request) SetRequestColumns() {
	logWith(req).Tracef("SetRequestColumns")
	if req.Command != "" {
		return
	}
	table := Objects.Tables[req.Table]
	numColumns := len(req.Columns) + len(req.Stats)
	if numColumns == 0 {
		numColumns = len(table.columns)
	}
	columns := make([]*Column, 0, numColumns)

	// if no column header was given, return all columns
	// but only if this is no stats query
	if len(req.Columns) == 0 && len(req.Stats) == 0 {
		for j := range table.columns {
			col := table.columns[j]
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

// SetSortColumns set the requestcolumn for the sortfields.
func (req *Request) SetSortColumns() (err error) {
	logWith(req).Tracef("SetSortColumns")
	if req.Command != "" {
		return err
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

	return err
}

// parseResult parses the result bytes and returns the data table and optional meta data for wrapped_json requests.
func (req *Request) parseResult(resBytes []byte) (ResultSet, *ResultMetaData, error) {
	var err error
	meta := &ResultMetaData{Request: req}
	if len(resBytes) == 0 || (string(resBytes[0]) != "{" && string(resBytes[0]) != "[") {
		err = errors.New(strings.TrimSpace(string(resBytes)))

		return nil, nil, &PeerError{msg: fmt.Sprintf("response does not look like a json result: %s", err.Error()), kind: ResponseError, req: req, resBytes: resBytes}
	}
	if req.OutputFormat == OutputFormatWrappedJSON {
		res, err2 := req.parseWrappedJSONMeta(resBytes, meta)
		if err2 != nil {
			return nil, nil, fmt.Errorf("parserResult: %w", err2)
		}

		return res, meta, err
	}
	res, err := NewResultSet(resBytes)

	return res, meta, err
}

func parseJSONResult(data []byte) (res ResultSet, remaining []byte, err error) {
	res = make(ResultSet, 0)
	rowNum := 0

	finalPos := 0
	data, trim := trimLeftTracking(data)
	finalPos += trim

	if len(data) < 1 {
		return nil, nil, fmt.Errorf("empty json data")
	}
	if data[0] != '[' {
		return nil, nil, fmt.Errorf("json data should start with '['")
	}

	// remove leading '['
	data, trim = trimLeftTracking(data[1:])
	finalPos += trim + 1
	var linePos int

	json := &rjson.ValueReader{}
	for {
		if len(data) >= 1 && data[0] == ']' {
			data, trim = trimLeftTracking(data[1:])
			finalPos += trim + 1

			break
		}

		rowNum++
		row, pos, jErr := json.ReadArray(data)
		linePos = pos
		if jErr != nil {
			var newPos int
			errPos := pos
			for {
				tryRecoverJSON(data[errPos:], jErr)
				row, newPos, jErr = json.ReadArray(data)
				if jErr == nil {
					err = nil

					break
				}
				err = jErr
				// position of error did not advance
				if newPos == errPos {
					break
				}
			}
			pos = newPos
			linePos = pos
			finalPos += pos
			if err != nil {
				break
			}
		}
		finalPos += pos
		res = append(res, row)

		data, trim = trimLeftTracking(data[pos:])
		finalPos += trim

		if len(data) >= 1 && data[0] == ',' {
			data, trim = trimLeftTracking(data[1:])
			finalPos += trim + 1
		}
	}

	if err != nil {
		return nil,
			nil,
			fmt.Errorf("json parse error at row %d pos %d (byte offset %d): %s",
				rowNum,
				linePos,
				finalPos+1,
				err.Error(),
			)
	}

	return res, data, nil
}

func (req *Request) parseWrappedJSONMeta(resBytes []byte, meta *ResultMetaData) (res ResultSet, err error) {
	resBytes, _ = trimLeftTracking(resBytes)
	if len(resBytes) == 0 || resBytes[0] != '{' {
		return nil, &PeerError{msg: "json parse error: expected {", kind: ResponseError, req: req, resBytes: resBytes}
	}

	idx := bytes.Index(resBytes, []byte("\"data\":"))
	if idx < 0 {
		return nil, &PeerError{msg: "json parse error: expected \"data\":", kind: ResponseError, req: req, resBytes: resBytes}
	}
	pre := resBytes[:idx]

	res, post, err := parseJSONResult(resBytes[idx+7:])
	if err != nil {
		return nil, &PeerError{msg: fmt.Sprintf("json parse error: %s", err.Error()), kind: ResponseError, req: req, resBytes: resBytes}
	}
	post, _ = trimLeftTracking(post)
	if (len(pre) < 3 && len(post) > 1) && post[0] == ',' {
		post = post[1:]
		post, _ = trimLeftTracking(post)
	}

	json := &rjson.ValueReader{}
	remaining := []byte{}
	remaining = append(remaining, pre...)
	remaining = append(remaining, post...)
	wrapped, _, err := json.ReadObject(remaining)
	if err != nil {
		return nil, &PeerError{msg: fmt.Sprintf("json parse error: %s", err.Error()), kind: ResponseError, req: req, resBytes: resBytes}
	}

	for key, value := range wrapped {
		switch key {
		case "total_count":
			meta.Total = interface2int64(value)
		case "columns":
			meta.Columns = interface2stringListNoDedup(value)
		case "rows_scanned":
			meta.RowsScanned = interface2int64(value)
		case "failed":
			// ignored, contains backend ids which failed
		}
	}

	return res, nil
}

// trim leading whitespace bytes and return the number of trimmed bytes.
func trimLeftTracking(data []byte) (dat []byte, numTrimmed int) {
	whiteSpace := "\t\n\v\f\r "
	len1 := len(data)
	data = bytes.TrimLeft(data, whiteSpace)
	len2 := len(data)

	return data, len1 - len2
}

// replace invalid json characters inline, does not change length of the byte array.
func tryRecoverJSON(data []byte, err error) {
	// try to fix invalid escape sequences and unknown utf8 characters
	if strings.Contains(err.Error(), "invalid json string") {
		replaceInvalidUTF8(data)
	}

	// try to fix invalid nan for numbers (simply replace with 0)
	if strings.Contains(err.Error(), "not null") {
		for idx := 0; idx != -1; idx = bytes.Index(data, []byte("nan,")) {
			data[idx] = 32
			data[idx+1] = 48
			data[idx+2] = 32
		}
		for idx := 0; idx != -1; idx = bytes.Index(data, []byte("nan]")) {
			data[idx] = 32
			data[idx+1] = 48
			data[idx+2] = 32
		}
	}
}

// bytes.ToValidUTF8 replaces invalid utf8 characters
// taken from go 1.13 beta
// https://github.com/golang/go/commit/3259bc441957bf74f069cf7df961367a3472afb2
// https://github.com/golang/go/issues/25805
// enhanced with removing ctrl characters like in
// https://rosettacode.org/wiki/Strip_control_codes_and_extended_characters_from_a_string#Go
// replaces characters inline with spaces so length is not changed.
func replaceInvalidUTF8(src []byte) {
	invalid := false // previous byte was from an invalid UTF-8 sequence

	for idx := 0; idx < len(src); {
		chr := src[idx]

		// stop at the first newline
		if chr == '\n' {
			return
		}

		// the byte range from 32 to 126 contains valid one byte size characters
		// excluding control characters (0-31)
		// and the del character (127)
		if chr >= 32 && chr <= 126 {
			idx++
			invalid = false

			continue
		}

		// try to read a utf8 character, returns (err, 1) if invalid
		_, wid := utf8.DecodeRune(src[idx:])
		if wid == 1 {
			if !invalid {
				invalid = true
				src[idx] = '.'
			}

			idx++

			continue
		}

		// read valid utf8 character
		invalid = false
		idx += wid
	}
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
	default:
		return false
	}

	return false
}

func (req *Request) optimizeResultLimit() (limit int) {
	if req.Limit != nil && req.IsDefaultSortOrder() {
		limit = *req.Limit
		if req.Offset > 0 {
			limit += req.Offset
		}
	} else {
		limit = -1
	}

	return limit
}

// optimizeFilter recursively optimizes the filter.
func (req *Request) optimizeFilter() {
	for i, f := range req.Filter {
		req.Filter[i] = f.optimize()
	}

	// reorder by complexity
	slices.SortStableFunc(req.Filter, func(a, b *Filter) int {
		return a.complexity() - b.complexity()
	})

	for {
		if len(req.Filter) == 1 && len(req.Filter[0].filter) > 0 &&
			req.Filter[0].groupOperator == And && !req.Filter[0].negate {
			req.Filter = req.Filter[0].filter
		} else {
			break
		}
	}
}

/*
	optimizeStatsGroups combines similar StatsAnd: to nested stats
	  for example with a query like:

```

	Stats: has_been_checked = 1
	Stats: state = 0
	StatsAnd: 2
	Stats: has_been_checked = 1
	Stats: state = 1
	StatsAnd: 2

```

	those two counters can be combined, so the has_been_checked has only to be checked once
*/
func (req *Request) optimizeStatsGroups(stats []*Filter, renumber bool) []*Filter {
	if len(stats) <= 1 {
		return nil
	}
	groupedStats := make([]*Filter, 0)
	var lastGroup *Filter
	for idx := range stats {
		stat := stats[idx]
		if renumber {
			stat.statsPos = idx
		}
		if stat.statsType != Counter || stat.column != nil || len(stat.filter) < 2 {
			groupedStats = append(groupedStats, stat)

			continue
		}

		// append to previous group?
		if idx >= 1 && lastGroup != nil {
			firstFilter := stat.filter[0]
			switch {
			case lastGroup.column != firstFilter.column:
			case lastGroup.operator != firstFilter.operator:
			case lastGroup.stringVal != firstFilter.stringVal:
			case lastGroup.negate != firstFilter.negate:
			case len(firstFilter.filter) != 0:
			default:
				lastGroup.filter = append(lastGroup.filter, removeFirstStatsFilter(stat))

				continue
			}
		}

		// build sub stats groups recursively
		req.optimizeStatsGroupsRecurse(lastGroup)

		// start a new group if the current first stats filter matches the next first stats filter
		if len(stats) > idx+1 {
			next := stats[idx+1]
			if next.statsType != Counter || next.column != nil || len(next.filter) < 2 || stat.filter[0].groupOperator == Or {
				groupedStats = append(groupedStats, stat)

				continue
			}
			if !next.filter[0].Equals(stat.filter[0]) {
				groupedStats = append(groupedStats, stat)

				continue
			}

			group := stat.filter[0]
			group.statsType = StatsGroup
			group.filter = []*Filter{removeFirstStatsFilter(stat)}

			groupedStats = append(groupedStats, group)
			lastGroup = group

			continue
		}

		groupedStats = append(groupedStats, stat)
	}
	// build sub stats groups recursively
	req.optimizeStatsGroupsRecurse(lastGroup)

	return groupedStats
}

func (req *Request) optimizeStatsGroupsRecurse(lastGroup *Filter) {
	if lastGroup == nil {
		return
	}

	subgroup := req.optimizeStatsGroups(lastGroup.filter, false)
	if subgroup != nil {
		lastGroup.filter = subgroup
	}
}

func removeFirstStatsFilter(stat *Filter) *Filter {
	// strip first filter, this one is handled in the parent group
	stat.filter = stat.filter[1:]

	// still multiple filters, keep list
	if len(stat.filter) > 1 {
		return stat
	}

	// remove indentation lvl if only one remaining
	stat.filter[0].statsPos = stat.statsPos
	stat = stat.filter[0]
	stat.statsType = Counter

	return stat
}
