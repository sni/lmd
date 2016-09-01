package main

import (
	"bufio"
	"errors"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
)

type Request struct {
	Table           string
	Columns         []string
	Filter          []Filter
	Limit           int
	Offset          int
	Sort            []*SortField
	ResponseFixed16 bool
	OutputFormat    string
	Backends        []string
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

type Filter struct {
	// filter can either be a single filter
	Column   Column
	Operator Operator
	Value    interface{}

	// or a group of filters
	Filter        []Filter
	GroupOperator GroupOperator
}

type Operator int

const (
	UnknownOperator Operator = iota
	Equal                    // =
	Unequal                  // !=
	LessThan                 // <=
	Less                     // <
	Greater                  // >
	GreaterThan              // >=
	Match                    // ~~
	MatchNot                 // !~~
)

var ReRequestAction = regexp.MustCompile(`^GET ([a-z]+)\n`)
var ReRequestHeader = regexp.MustCompile(`^(\w+):\s*(.*)$`)
var ReRequestEmpty = regexp.MustCompile(`^\s*$`)

func ParseRequest(c net.Conn) (req *Request, err error) {
	b := bufio.NewReader(c)
	return ParseRequestFromBuffer(b)
}

func ParseRequestFromBuffer(b *bufio.Reader) (req *Request, err error) {
	req = &Request{}
	firstLine, err := b.ReadString('\n')
	if err != nil {
		err = errors.New("bad request: " + err.Error())
		return
	}
	log.Debugf("request: %s", firstLine)
	matched := ReRequestAction.FindStringSubmatch(firstLine)
	if len(matched) != 2 {
		err = errors.New("bad request in " + firstLine)
		return
	}

	req.Table = matched[1]
	_, ok := Objects.Tables[req.Table]
	if !ok {
		err = errors.New("bad request: table " + req.Table + " does not exist")
		return
	}

	for {
		line, berr := b.ReadString('\n')
		if berr == io.EOF {
			break
		}
		if berr != nil {
			err = berr
			return
		}
		line = strings.TrimSpace(line)
		log.Debugf("request: %s", line)
		if ReRequestEmpty.MatchString(line) {
			break
		}

		perr := ParseRequestHeaderLine(req, &line)
		if perr != nil {
			err = perr
			return
		}
	}

	return
}

func ParseRequestHeaderLine(req *Request, line *string) (err error) {
	matched := ReRequestHeader.FindStringSubmatch(*line)
	if len(matched) != 3 {
		err = errors.New("bad request in " + *line)
		return
	}
	header := strings.ToLower(matched[1])
	value := matched[2]

	switch header {
	case "filter":
		tmp := strings.SplitN(value, " ", 3)
		if len(tmp) < 3 {
			err = errors.New("bad request: filter header, must be Filter: <field> <operator> <value>")
			return
		}
		op := UnknownOperator
		switch tmp[1] {
		case "=":
			op = Equal
			break
		case "!=":
			op = Unequal
			break
		case "<":
			op = Less
			break
		case "<=":
			op = LessThan
			break
		case ">":
			op = Greater
			break
		case ">=":
			op = GreaterThan
			break
		case "~~":
			op = Match
			break
		case "!~~":
			op = MatchNot
			break
		default:
			err = errors.New("bad request: unrecognized filter operator: " + tmp[1] + " in " + *line)
			return
		}
		// convert value to type of column
		i, Ok := Objects.Tables[req.Table].ColumnsIndex[tmp[0]]
		if !Ok {
			err = errors.New("bad request: unrecognized column from filter: " + tmp[0] + " in " + *line)
			return
		}
		var filtervalue interface{}
		col := Objects.Tables[req.Table].Columns[i]
		switch col.Type {
		case IntCol:
			var cerr error
			filtervalue, cerr = strconv.Atoi(tmp[2])
			if cerr != nil {
				err = errors.New("bad request: could not convert " + tmp[2] + " to integer from filter: " + *line)
				return
			}
			break
		case FloatCol:
			var cerr error
			filtervalue, cerr = strconv.ParseFloat(tmp[2], 64)
			if cerr != nil {
				err = errors.New("bad request: could not convert " + tmp[2] + " to float from filter: " + *line)
				return
			}
			break
		default:
			filtervalue = tmp[2]
		}
		filter := Filter{Operator: op, Value: filtervalue, Column: col}
		req.Filter = append(req.Filter, filter)
		return
	case "and":
		fallthrough
	case "or":
		and, cerr := strconv.Atoi(value)
		if cerr != nil || and < 1 {
			err = errors.New("bad request: " + header + " must be a positive number")
		}
		err = errors.New("bad request: not implemented")
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
			err = errors.New("unrecognized outputformat, only json and wrapped_json is supported")
			return
		}
	default:
		err = errors.New("unrecognized header " + *line)
		return
	}
	return
}
