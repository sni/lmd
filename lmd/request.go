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
}

type SortDirection int

const (
	UnknownSortDirection SortDirection = iota
	Asc
	Desc
)

type SortField struct {
	Name      string
	Direction SortDirection
	Index     int
}

type GroupOperator int

const (
	UnknownGroupOperator GroupOperator = iota
	And
	Or
)

var ReRequestAction = regexp.MustCompile(`^GET ([a-z]+)$`)
var ReRequestCommand = regexp.MustCompile(`^COMMAND (\[\d+\].*)$`)
var ReRequestHeader = regexp.MustCompile(`^(\w+):\s*(.*)$`)
var ReRequestEmpty = regexp.MustCompile(`^\s*$`)

func ParseRequest(c net.Conn) (req *Request, err error) {
	b := bufio.NewReader(c)
	localAddr := c.LocalAddr().String()
	req, size, err := ParseRequestFromBuffer(b)
	promFrontendBytesReceived.WithLabelValues(localAddr).Add(float64(size))
	return
}

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
	if len(req.Sort) > 0 {
		for _, s := range req.Sort {
			direction := "asc"
			if s.Direction == Desc {
				direction = "desc"
			}
			str += fmt.Sprintf("Sort: %s %s\n", s.Name, direction)
		}
	}
	if len(req.Filter) > 0 {
		for _, f := range req.Filter {
			str += f.String("")
		}
	}
	if req.FilterStr != "" {
		str += req.FilterStr
	}
	if len(req.Stats) > 0 {
		for _, s := range req.Stats {
			str += s.String("Stats")
		}
	}
	str += "\n"
	return
}

func ParseRequestFromBuffer(b *bufio.Reader) (req *Request, size int, err error) {
	req = &Request{SendColumnsHeader: false}
	firstLine, err := b.ReadString('\n')
	if err != nil && err != io.EOF {
		err = errors.New("bad request: " + err.Error())
		return
	}
	size += len(firstLine)
	firstLine = strings.TrimSpace(firstLine)
	// check for commands
	log.Debugf("request: %s", firstLine)
	matched := ReRequestCommand.FindStringSubmatch(firstLine)
	if len(matched) == 2 {
		req.Command = matched[0]
	} else {
		matched = ReRequestAction.FindStringSubmatch(firstLine)
		if len(matched) != 2 {
			if len(firstLine) == 0 {
				err = errors.New("bad request: empty request")
			} else {
				err = errors.New("bad request: " + firstLine)
			}
			return
		}

		req.Table = matched[1]
		_, ok := Objects.Tables[req.Table]
		if !ok {
			err = errors.New("bad request: table " + req.Table + " does not exist")
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
		if ReRequestEmpty.MatchString(line) {
			break
		}

		log.Debugf("request: %s", line)
		perr := ParseRequestHeaderLine(req, &line)
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

	return
}

func ParseRequestHeaderLine(req *Request, line *string) (err error) {
	matched := ReRequestHeader.FindStringSubmatch(*line)
	if len(matched) != 3 {
		err = errors.New("bad request header: " + *line)
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
		err = ParseFilterOp("and", value, line, &req.Stats)
		req.Stats[len(req.Stats)-1].StatsType = Counter
		return
	case "statsor":
		err = ParseFilterOp("or", value, line, &req.Stats)
		req.Stats[len(req.Stats)-1].StatsType = Counter
		return
	case "sort":
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
		req.Sort = append(req.Sort, &SortField{Name: strings.ToLower(tmp[0]), Direction: direction})
		return
	case "limit":
		limit, cerr := strconv.Atoi(value)
		if cerr != nil || limit < 1 {
			err = errors.New("bad request: limit must be a positive number")
			return
		}
		req.Limit = limit
		return
	case "offset":
		offset, cerr := strconv.Atoi(value)
		if cerr != nil || offset < 0 {
			err = errors.New("bad request: offset must be a positive number")
			return
		}
		req.Offset = offset
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
	default:
		err = errors.New("bad request: unrecognized header " + *line)
		return
	}
	return
}
