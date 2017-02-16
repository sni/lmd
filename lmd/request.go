package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// Request defines a livestatus request object.
type Request struct {
	Table             string
	Command           string
	Columns           []string
	Filter            []Filter
	FilterStr         string
	Stats             []Filter
	Limit             int
	Offset            int
	Sort              []*SortField
	ResponseFixed16   bool
	OutputFormat      string
	Backends          []string
	BackendsMap       map[string]string
	SendColumnsHeader bool
	WaitTimeout       int
	WaitTrigger       string
	WaitCondition     []Filter
	WaitObject        string
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

var reRequestAction = regexp.MustCompile(`^GET ([a-z]+)$`)
var reRequestCommand = regexp.MustCompile(`^COMMAND (\[\d+\].*)$`)
var reRequestHeader = regexp.MustCompile(`^(\w+):\s*(.*)$`)

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
	if req.Limit > 0 {
		str += fmt.Sprintf("Limit: %d\n", req.Limit)
	}
	if req.Offset > 0 {
		str += fmt.Sprintf("Offset: %d\n", req.Offset)
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
		str += fmt.Sprintf("WaitObject: %s\n", req.WaitObject)
		str += fmt.Sprintf("WaitTimeout: %d\n", req.WaitTimeout)
		for _, f := range req.WaitCondition {
			str += f.String("WaitCondition")
		}
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
	req = &Request{SendColumnsHeader: false}
	firstLine, err := b.ReadString('\n')
	if err != nil && err != io.EOF {
		err = fmt.Errorf("bad request: %s", err.Error())
		return
	}
	size += len(firstLine)
	firstLine = strings.TrimSpace(firstLine)
	if log.IsV(2) {
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

	err = req.VerifyRequestIntegrity()
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
	if len(*firstLine) == 0 {
		return
	}

	err = fmt.Errorf("bad request: %s", *firstLine)
	return
}

// VerifyRequestIntegrity checks for logical errors in the request
// It returns any error encountered.
func (req *Request) VerifyRequestIntegrity() (err error) {
	if len(req.Columns) > 0 && len(req.Stats) > 0 {
		err = errors.New("bad request: stats and columns cannot be mixed")
		return
	}
	if req.WaitTrigger != "" {
		if req.WaitObject == "" {
			err = errors.New("bad request: WaitTrigger without WaitObject")
		}
		if req.WaitTimeout == 0 {
			err = errors.New("bad request: WaitTrigger without WaitTimeout")
		}
		if len(req.WaitCondition) == 0 {
			err = errors.New("bad request: WaitTrigger without WaitCondition")
		}
		return
	}
	return
}

// GetResponse builds the response for a given request.
// It returns the Response object and any error encountered.
func (req *Request) GetResponse() (*Response, error) {
	log.Tracef("GetResponse")

	// Determine if request for this node only (if backends specified)
	allBackendsRequested := len(req.Backends) == 0
	isForOurBackends := false // request for our own backends only
	if !allBackendsRequested {
		isForOurBackends = true
		for _, backend := range req.Backends {
			isOurs := NodeAccessor.IsOurBackend(backend)
			isForOurBackends = isForOurBackends && isOurs
		}
	}

	// Distribute request if necessary
	if NodeAccessor.IsClustered() && !isForOurBackends {
		// Columns for sub-requests
		// Define request columns if not specified
		table, _ := Objects.Tables[req.Table]
		_, resultColumns, err := req.BuildResponseIndexes(&table)
		if err != nil {
			return nil, err
		}

		// Cluster mode (don't send request, send sub-requests, build response)
		var wg sync.WaitGroup
		nodeBackends := NodeAccessor.nodeBackends
		datasets := make(chan [][]interface{}, len(nodeBackends))
		for node, nodeBackends := range nodeBackends {
			// Limit to requested backends if necessary
			// nodeBackends: all backends handled by current node
			var subBackends []string // backends for current sub-request
			for _, nodeBackend := range nodeBackends {
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

			// Skip node if it doesn't have relevant backends
			if len(subBackends) == 0 {
				datasets <- [][]interface{}{}
				continue
			}

			// Create sub-request
			requestData := make(map[string]interface{})
			if req.Table != "" {
				requestData["table"] = req.Table
			}

			// Set backends for this sub-request
			requestData["backends"] = subBackends

			// No header row
			requestData["sendcolumnsheader"] = false

			// Columns
			// Columns need to be defined or else response will add them
			if len(req.Columns) != 0 {
				requestData["columns"] = req.Columns
			} else {
				panic("columns undefined for dispatched request")
			}

			// Filter
			if len(req.Filter) != 0 {
				requestData["filter"] = req.Filter
			}
			if req.FilterStr != "" {
				requestData["filterstr"] = req.FilterStr
			}

			// TODO stats

			// Limit
			// An upper limit is used to make sorting possible TODO test
			// Offset is 0 for sub-request (sorting) TODO test
			if req.Limit != 0 {
				requestData["limit"] = req.Limit + req.Offset
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

			// Callback
			callback := func(responseData interface{}) {
				defer wg.Done()

				// Parse data (rows)
				rowsVariants, ok := responseData.([]interface{})
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

				// Keep rows
				datasets <- rows
			}

			// Send query to node
			wg.Add(1)
			err := NodeAccessor.SendQuery(node, "table", requestData, callback)
			if err != nil {
				return nil, err
			}

		}

		// Wait for all requests
		// TODO timeout
		wg.Wait()
		close(datasets)

		// Double-check that we have the right number of datasets
		if len(datasets) != len(nodeBackends) {
			err := fmt.Errorf("got %d instead of %d datasets", len(datasets), len(nodeBackends))
			return nil, err
		}

		// Build response object
		res := &Response{Request: req}
		res.Columns = resultColumns

		// Merge data
		for currentRows := range datasets {
			res.Result = append(res.Result, currentRows...)
		}

		// Process results
		// This also applies sort/offset/limit settings
		res.PostProcessing()

		return res, nil
	} else {
		// Single mode (send request and return response)
		// Or sub-request in cluster mode
		// Or request for our backends only
		return (NewResponse(req))
	}
}

// ParseRequestHeaderLine parses a single request line
// It returns any error encountered.
func (req *Request) ParseRequestHeaderLine(line *string) (err error) {
	matched := reRequestHeader.FindStringSubmatch(*line)
	if len(matched) != 3 {
		err = fmt.Errorf("bad request header: %s", *line)
		return
	}
	header := strings.ToLower(matched[1])
	value := matched[2]

	switch header {
	case "filter":
		err = ParseFilter(value, line, req.Table, &req.Filter)
		return
	case "and":
		fallthrough
	case "or":
		err = ParseFilterOp(header, value, line, &req.Filter)
		return
	case "stats":
		err = ParseStats(value, line, req.Table, &req.Stats)
		return
	case "statsand":
		err = parseStatsOp("and", value, line, &req.Stats)
		return
	case "statsor":
		err = parseStatsOp("or", value, line, &req.Stats)
		return
	case "sort":
		err = parseSortHeader(&req.Sort, value)
		return
	case "limit":
		err = parseIntHeader(&req.Limit, header, value, 1)
		return
	case "offset":
		err = parseIntHeader(&req.Offset, header, value, 0)
		return
	case "backends":
		req.Backends = strings.Split(value, " ")
		return
	case "columns":
		req.Columns = strings.Split(value, " ")
		return
	case "responseheader":
		err = parseResponseHeader(&req.ResponseFixed16, value)
		return
	case "outputformat":
		err = parseOutputFormat(&req.OutputFormat, value)
		return
	case "waittimeout":
		err = parseIntHeader(&req.WaitTimeout, header, value, 1)
		return
	case "waittrigger":
		req.WaitTrigger = value
		return
	case "waitobject":
		req.WaitObject = value
		return
	case "waitcondition":
		err = ParseFilter(value, line, req.Table, &req.WaitCondition)
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
	if len(tmp) < 2 {
		err = errors.New("bad request: invalid sort header, must be 'Sort: <field> <asc|desc>' or 'Sort: custom_variables <name> <asc|desc>'")
		return
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
		break
	case "desc":
		direction = Desc
		break
	default:
		err = errors.New("bad request: unrecognized sort direction, must be asc or desc")
		return
	}
	*field = append(*field, &SortField{Name: strings.ToLower(tmp[0]), Direction: direction, Args: args})
	return
}

func parseStatsOp(op string, value string, line *string, stats *[]Filter) (err error) {
	err = ParseFilterOp(op, value, line, stats)
	if err != nil {
		return
	}
	(*stats)[len(*stats)-1].StatsType = Counter
	return
}

func parseOutputFormat(field *string, value string) (err error) {
	switch value {
	case "wrapped_json":
		*field = value
		break
	case "json":
		*field = value
		break
	default:
		err = errors.New("bad request: unrecognized outputformat, only json and wrapped_json is supported")
		return
	}
	return
}
