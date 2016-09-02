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

var ReRequestAction = regexp.MustCompile(`^GET ([a-z]+)$`)
var ReRequestHeader = regexp.MustCompile(`^(\w+):\s*(.*)$`)
var ReRequestEmpty = regexp.MustCompile(`^\s*$`)

func ParseRequest(c net.Conn) (req *Request, err error) {
	b := bufio.NewReader(c)
	return ParseRequestFromBuffer(b)
}

func ParseRequestFromBuffer(b *bufio.Reader) (req *Request, err error) {
	req = &Request{}
	firstLine, err := b.ReadString('\n')
	firstLine = strings.TrimSpace(firstLine)
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
		if berr != nil && berr != io.EOF {
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
		if berr == io.EOF {
			break
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
		err = ParseFilter(value, line, req.Table, &req.Filter)
		return
	case "and":
		fallthrough
	case "or":
		err = ParseFilterOp(header, value, line, &req.Filter)
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
			err = errors.New("unrecognized outputformat, only json and wrapped_json is supported")
			return
		}
	default:
		err = errors.New("unrecognized header " + *line)
		return
	}
	return
}
