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

// SortField defines a single sort entry
type SortField struct {
	Name      string
	Direction SortDirection
	Index     int
}

// GroupOperator is the operator used to combine multiple filter or stats header.
type GroupOperator int

// The only possible GroupOperator are "And" and "Or"
const (
	_ GroupOperator = iota
	And
	Or
)

var reRequestAction = regexp.MustCompile(`^GET ([a-z]+)$`)
var reRequestCommand = regexp.MustCompile(`^COMMAND (\[\d+\].*)$`)
var reRequestHeader = regexp.MustCompile(`^(\w+):\s*(.*)$`)
var reRequestEmpty = regexp.MustCompile(`^\s*$`)

// ParseRequest reads from a connection and returns a single requests.
// It returns a the requests and any errors encountered.
func ParseRequest(c net.Conn) (req *Request, err error) {
	b := bufio.NewReader(c)
	localAddr := c.LocalAddr().String()
	req, size, err := ParseRequestFromBuffer(b)
	promFrontendBytesReceived.WithLabelValues(localAddr).Add(float64(size))
	return
}

// ParseRequests reads from a connection and returns all requests read.
// It returns a list of requests and any errors encountered.
func ParseRequests(c net.Conn) (reqs []*Request, err error) {
	b := bufio.NewReader(c)
	localAddr := c.LocalAddr().String()
	for {
		req, size, err := ParseRequestFromBuffer(b)
		promFrontendBytesReceived.WithLabelValues(localAddr).Add(float64(size))
		if err != nil {
			return nil, err
		}
		if req == nil {
			break
		}
		reqs = append(reqs, req)
		// only multiple commands are allowed
		if req.Command == "" {
			break
		}
	}
	return
}

// OperatorString converts a Operator back to the original string.
func OperatorString(op Operator) string {
	switch op {
	case Equal:
		return ("=")
	case Unequal:
		return ("!=")
	case EqualNocase:
		return ("=~")
	case UnequalNocase:
		return ("!=~")
	case RegexMatch:
		return ("~")
	case RegexMatchNot:
		return ("!~")
	case RegexNoCaseMatch:
		return ("~~")
	case RegexNoCaseMatchNot:
		return ("!~~")
	case Less:
		return ("<")
	case LessThan:
		return ("<=")
	case Greater:
		return (">")
	case GreaterThan:
		return (">=")
	case GroupContainsNot:
		return ("!>=")
	}
	log.Panicf("not implemented")
	return ""
}

// StatsTypeString converts a StatsType back to the original string.
func StatsTypeString(op StatsType) string {
	switch op {
	case Average:
		return ("avg")
	case Sum:
		return ("sum")
	case Min:
		return ("min")
	case Max:
		return ("Max")
	}
	log.Panicf("not implemented")
	return ""
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
	for _, s := range req.Sort {
		direction := "asc"
		if s.Direction == Desc {
			direction = "desc"
		}
		str += fmt.Sprintf("Sort: %s %s\n", s.Name, direction)
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
	for _, f := range req.WaitCondition {
		str += f.String("WaitCondition")
	}
	str += "\n"
	return
}

// ParseRequestFromBuffer reads a buffer and creates a request object.
// It returns the request as long with the number of bytes read and any error.
func ParseRequestFromBuffer(b *bufio.Reader) (req *Request, size int, err error) {
	req = &Request{SendColumnsHeader: false}
	firstLine, err := b.ReadString('\n')
	if err != nil && err != io.EOF {
		err = fmt.Errorf("bad request: %s", err.Error())
		return
	}
	size += len(firstLine)
	firstLine = strings.TrimSpace(firstLine)
	// check for commands
	log.Debugf("request: %s", firstLine)
	matched := reRequestCommand.FindStringSubmatch(firstLine)
	if len(matched) == 2 {
		req.Command = matched[0]
	} else {
		matched = reRequestAction.FindStringSubmatch(firstLine)
		if len(matched) != 2 {
			if len(firstLine) == 0 {
				return nil, size, nil
			}
			err = fmt.Errorf("bad request: %s", firstLine)
			return
		}

		req.Table = matched[1]
		_, ok := Objects.Tables[req.Table]
		if !ok {
			err = fmt.Errorf("bad request: table %s does not exist", req.Table)
			return
		}
	}

	for {
		line, berr := b.ReadString('\n')
		if berr != nil && berr != io.EOF {
			err = berr
			return
		}
		size += len(line)
		line = strings.TrimSpace(line)
		if reRequestEmpty.MatchString(line) {
			break
		}

		log.Debugf("request: %s", line)
		perr := req.ParseRequestHeaderLine(&line)
		if perr != nil {
			err = perr
			return
		}
		if berr == io.EOF {
			break
		}
	}

	if len(req.Columns) > 0 && len(req.Stats) > 0 {
		err = errors.New("bad request: stats and columns cannot be mixed")
		return
	}

	err = nil
	return
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
		err = parseFilterOp(header, value, line, &req.Filter)
		return
	case "stats":
		err = parseStats(value, line, req.Table, &req.Stats)
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
		if value != "fixed16" {
			err = errors.New("bad request: unrecognized responseformat, only fixed16 is supported")
			return
		}
		req.ResponseFixed16 = true
		return
	case "outputformat":
		switch value {
		case "wrapped_json":
			req.OutputFormat = value
			break
		case "json":
			req.OutputFormat = value
			break
		default:
			err = errors.New("bad request: unrecognized outputformat, only json and wrapped_json is supported")
			return
		}
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
	tmp := strings.SplitN(value, " ", 2)
	if len(tmp) < 2 {
		err = errors.New("bad request: invalid sort header, must be Sort: <field> <asc|desc>")
		return
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
	*field = append(*field, &SortField{Name: strings.ToLower(tmp[0]), Direction: direction})
	return
}

func parseStatsOp(op string, value string, line *string, stats *[]Filter) (err error) {
	err = parseFilterOp(op, value, line, stats)
	if err != nil {
		return
	}
	(*stats)[len(*stats)-1].StatsType = Counter
	return
}
