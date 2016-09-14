package main

import (
	"bufio"
	"bytes"
	"testing"
)

func init() {
	InitLogging(&Config{LogLevel: "Panic", LogFile: "stderr"})
	InitObjects()
}

func TestRequestHeader(t *testing.T) {
	testRequestStrings := []string{
		"GET hosts\n\n",
		"GET hosts\nColumns: name state\n\n",
		"GET hosts\nColumns: name state\nFilter: state != 1\n\n",
		"GET hosts\nOutputFormat: wrapped_json\n\n",
		"GET hosts\nResponseHeader: fixed16\n\n",
		"GET hosts\nColumns: name state\nFilter: state != 1\nFilter: is_executing = 1\nOr: 2\n\n",
		"GET hosts\nColumns: name state\nFilter: state != 1\nFilter: is_executing = 1\nAnd: 2\nFilter: state = 1\nOr: 2\nFilter: name = test\n\n",
		"GET hosts\nBackends: a b cde\n\n",
		"GET hosts\nLimit: 25\nOffset: 5\n\n",
		"GET hosts\nSort: name asc\nSort: state desc\n\n",
		"GET hosts\nStats: state = 1\nStats: avg latency\nStats: state = 3\nStats: state != 1\nStatsAnd: 2\n\n",
		"GET hosts\nColumns: name\nFilter: name ~~ test\n\n",
		"GET hosts\nColumns: name\nFilter: name ~~ Test\n\n",
		"GET hosts\nColumns: name\nFilter: name !~~ test\n\n",
		"GET hosts\nColumns: name\nFilter: custom_variables ~~ TAGS test\n\n",
		"GET hosts\nColumns: name\nFilter: name != \n\n",
		"COMMAND [123456] TEST\n\n",
		"GET hosts\nColumns: name\nFilter: name = test\nWaitTrigger: all\nWaitObject: test\nWaitTimeout: 10000\nWaitCondition: last_check > 1473760401\n\n",
	}
	for _, str := range testRequestStrings {
		buf := bufio.NewReader(bytes.NewBufferString(str))
		req, _, err := ParseRequestFromBuffer(buf)
		if err != nil {
			t.Fatal(err)
		}
		if err = assertEq(str, req.String()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRequestHeaderTable(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\n"))
	req, _, _ := ParseRequestFromBuffer(buf)
	if err := assertEq("hosts", req.Table); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderLimit(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nLimit: 10\n"))
	req, _, _ := ParseRequestFromBuffer(buf)
	if err := assertEq(10, req.Limit); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderOffset(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nOffset: 3\n"))
	req, _, _ := ParseRequestFromBuffer(buf)
	if err := assertEq(3, req.Offset); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderColumns(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name state\n"))
	req, _, _ := ParseRequestFromBuffer(buf)
	if err := assertEq([]string{"name", "state"}, req.Columns); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderSort(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: latency state name\nSort: name desc\nSort: state asc\n"))
	req, _, _ := ParseRequestFromBuffer(buf)
	table, _ := Objects.Tables[req.Table]
	BuildResponseIndexes(req, &table)
	if err := assertEq(SortField{Name: "name", Direction: Desc, Index: 2}, *req.Sort[0]); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(SortField{Name: "state", Direction: Asc, Index: 1}, *req.Sort[1]); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderFilter1(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: name != test\n"))
	req, _, _ := ParseRequestFromBuffer(buf)
	if err := assertEq([]Filter{Filter{Column: Column{Name: "name", Type: StringCol, Index: 56, RefIndex: 0, RefColIndex: 0, Update: StaticUpdate}, Operator: Unequal, Value: "test", Filter: []Filter(nil), GroupOperator: 0, Stats: 0, StatsCount: 0, StatsType: 0}}, req.Filter); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderFilter2(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: state != 1\nFilter: name = with spaces \n"))
	req, _, _ := ParseRequestFromBuffer(buf)
	expect := []Filter{Filter{Column: Column{Name: "state", Type: IntCol, Index: 80, RefIndex: 0, RefColIndex: 0, Update: DynamicUpdate}, Operator: Unequal, Value: 1, Filter: []Filter(nil), GroupOperator: 0},
		Filter{Column: Column{Name: "name", Type: StringCol, Index: 56, RefIndex: 0, RefColIndex: 0, Update: StaticUpdate}, Operator: Equal, Value: "with spaces", Filter: []Filter(nil), GroupOperator: 0, Stats: 0, StatsCount: 0, StatsType: 0}}
	if err := assertEq(expect, req.Filter); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderFilter3(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: state != 1\nFilter: name = with spaces\nOr: 2"))
	req, _, _ := ParseRequestFromBuffer(buf)
	expect := []Filter{Filter{Column: Column{Name: "", Type: 0, Index: 0, RefIndex: 0, RefColIndex: 0, Update: 0}, Operator: 0, Value: interface{}(nil),
		Filter: []Filter{Filter{Column: Column{Name: "state", Type: 3, Index: 80, RefIndex: 0, RefColIndex: 0, Update: 2}, Operator: 2, Value: 1, Filter: []Filter(nil), GroupOperator: 0, Stats: 0, StatsCount: 0, StatsType: 0},
			Filter{Column: Column{Name: "name", Type: 1, Index: 56, RefIndex: 0, RefColIndex: 0, Update: 1}, Operator: 1, Value: "with spaces", Filter: []Filter(nil), GroupOperator: 0, Stats: 0, StatsCount: 0, StatsType: 0}},
		GroupOperator: Or}}
	if err := assertEq(expect, req.Filter); err != nil {
		t.Fatal(err)
	}
}

func TestRequestListFilter(t *testing.T) {
	peer := SetupTestPeer()

	res, _ := peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
	if err := assertEq("gearman", res[0][0]); err != nil {
		t.Fatal(err)
	}

	StopTestPeer()
}

func TestRequestHeaderMultipleCommands(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString(`COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web1;1473627610
Backends: id2

COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web2;1473627610`))
	req, size, err := ParseRequestFromBuffer(buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(size, 83); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(req.Command, "COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web1;1473627610"); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(req.Backends[0], "id2"); err != nil {
		t.Fatal(err)
	}
	req, size, err = ParseRequestFromBuffer(buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(size, 67); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(req.Command, "COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web2;1473627610"); err != nil {
		t.Fatal(err)
	}
}

type ErrorRequest struct {
	Request string
	Error   string
}

func TestResponseErrorsFunc(t *testing.T) {
	peer := SetupTestPeer()

	testRequestStrings := []ErrorRequest{
		ErrorRequest{"", "bad request: empty request"},
		ErrorRequest{"NOE", "bad request: NOE"},
		ErrorRequest{"GET none\nColumns: none", "bad request: table none does not exist"},
		ErrorRequest{"GET backends\nColumns: status none", "bad request: table backends has no column none"},
		ErrorRequest{"GET hosts\nColumns: name\nFilter: none = 1", "bad request: unrecognized column from filter: none in Filter: none = 1"},
		ErrorRequest{"GET hosts\nBackends: none", "bad request: backend none does not exist"},
		ErrorRequest{"GET hosts\nnone", "bad request header: none"},
		ErrorRequest{"GET hosts\nNone: blah", "bad request: unrecognized header None: blah"},
		ErrorRequest{"GET hosts\nLimit: x", "bad request: limit must be a positive number"},
		ErrorRequest{"GET hosts\nLimit: -1", "bad request: limit must be a positive number"},
		ErrorRequest{"GET hosts\nOffset: x", "bad request: offset must be a positive number"},
		ErrorRequest{"GET hosts\nOffset: -1", "bad request: offset must be a positive number"},
		ErrorRequest{"GET hosts\nSort: 1", "bad request: invalid sort header, must be Sort: <field> <asc|desc>"},
		ErrorRequest{"GET hosts\nSort: name none", "bad request: unrecognized sort direction, must be asc or desc"},
		ErrorRequest{"GET hosts\nSort: name", "bad request: invalid sort header, must be Sort: <field> <asc|desc>"},
		ErrorRequest{"GET hosts\nColumns: name\nSort: state asc", "bad request: sort column state not in result set"},
		ErrorRequest{"GET hosts\nResponseheader: none", "bad request: unrecognized responseformat, only fixed16 is supported"},
		ErrorRequest{"GET hosts\nOutputFormat: csv: none", "bad request: unrecognized outputformat, only json and wrapped_json is supported"},
	}

	for _, er := range testRequestStrings {
		_, err := peer.QueryString(er.Request)
		if err = assertEq(er.Error, err.Error()); err != nil {
			t.Error("Request: " + er.Request)
			t.Error(err)
		}
	}

	StopTestPeer()
}

func TestRequestStats(t *testing.T) {
	peer := SetupTestPeer()

	res, err := peer.QueryString("GET hosts\nStats: sum latency\nStats: avg latency\nStats: min has_been_checked\nStats: max execution_time\n")

	if err = assertEq(6.8264913559332, res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq(0.142218569915275, res[0][1]); err != nil {
		t.Error(err)
	}
	if err = assertEq(float64(1), res[0][2]); err != nil {
		t.Error(err)
	}
	if err = assertEq(4.031151, res[0][3]); err != nil {
		t.Error(err)
	}

	StopTestPeer()
}

func TestRequestStatsBroken(t *testing.T) {
	peer := SetupTestPeer()

	res, err := peer.QueryString("GET hosts\nStats: sum name\nStats: avg contacts\nStats: min plugin_output\n")
	if err = assertEq(float64(0), res[0][0]); err != nil {
		t.Error(err)
	}

	StopTestPeer()
}
