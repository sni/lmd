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
	Filter          []FilterGroup
	Limit           int
	Offset          int
	Sort            string // TODO: ... change to something useful
	ResponseFixed16 bool
	OutputFormat    string
	Backends        []string
}

type GroupOperator int

const (
	And GroupOperator = iota
	Or
)

type FilterGroup struct {
	Operator    GroupOperator
	Filter      []Filter
	FilterGroup []FilterGroup
}

type Operator int

const (
	Equal    Operator = iota // ==
	Unequal                  // !=
	Match                    // ~~
	MatchNot                 // !~~
)

type Filter struct {
	Column   string
	Operator Operator
	Value    interface{}
}

var ReRequestAction = regexp.MustCompile(`^GET ([a-z]+)\n`)
var ReRequestHeader = regexp.MustCompile(`^(\w+):\s*(.*)\n`)
var ReRequestEmpty = regexp.MustCompile(`^\s*\n`)

func ParseRequest(c net.Conn) (req *Request, err error) {
	req = &Request{}
	b := bufio.NewReader(c)
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

	for {
		line, berr := b.ReadString('\n')
		if berr == io.EOF {
			break
		}
		if berr != nil {
			err = berr
			return
		}
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
		// TODO: implement
		return
	case "and":
		// TODO: implement
		return
	case "or":
		// TODO: implement
		return
	case "sort":
		// TODO: implement
		return
	case "limit":
		limit, cerr := strconv.Atoi(value)
		if cerr != nil || limit < 1 {
			err = errors.New("limit must be a positive number")
			return
		}
		req.Limit = limit
		return
	case "offset":
		offset, cerr := strconv.Atoi(value)
		if cerr != nil || offset < 0 {
			err = errors.New("offset must be a positive number")
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
			err = errors.New("unrecognized responseformat, only fixed16 is supported")
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
