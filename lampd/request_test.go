package main

import (
	"bufio"
	"bytes"
	"testing"
)

func init() {
	InitLogging(&Config{LogLevel: "Error", LogFile: "stderr"})
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
	}
	for _, str := range testRequestStrings {
		buf := bufio.NewReader(bytes.NewBufferString(str))
		req, err := ParseRequestFromBuffer(buf)
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
	req, _ := ParseRequestFromBuffer(buf)
	if err := assertEq("hosts", req.Table); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderLimit(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nLimit: 10\n"))
	req, _ := ParseRequestFromBuffer(buf)
	if err := assertEq(10, req.Limit); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderOffset(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nOffset: 3\n"))
	req, _ := ParseRequestFromBuffer(buf)
	if err := assertEq(3, req.Offset); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderColumns(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name state\n"))
	req, _ := ParseRequestFromBuffer(buf)
	if err := assertEq([]string{"name", "state"}, req.Columns); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderSort(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: latency state name\nSort: name desc\nSort: state asc\n"))
	req, _ := ParseRequestFromBuffer(buf)
	table, _ := Objects.Tables[req.Table]
	BuildResponseIndexes(req, table)
	if err := assertEq(SortField{Name: "name", Direction: Desc, Index: 2}, *req.Sort[0]); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(SortField{Name: "state", Direction: Asc, Index: 1}, *req.Sort[1]); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderFilter1(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: name != test\n"))
	req, _ := ParseRequestFromBuffer(buf)
	if err := assertEq([]Filter{Filter{Column: Column{Name: "name", Type: StringCol, Index: 56, RefIndex: 0, RefColIndex: 0, Update: StaticUpdate}, Operator: Unequal, Value: "test", Filter: []Filter(nil), GroupOperator: 0, Stats: 0, StatsCount: 0, StatsType: 0}}, req.Filter); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderFilter2(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: state != 1\nFilter: name = with spaces \n"))
	req, _ := ParseRequestFromBuffer(buf)
	expect := []Filter{Filter{Column: Column{Name: "state", Type: IntCol, Index: 80, RefIndex: 0, RefColIndex: 0, Update: DynamicUpdate}, Operator: Unequal, Value: 1, Filter: []Filter(nil), GroupOperator: 0},
		Filter{Column: Column{Name: "name", Type: StringCol, Index: 56, RefIndex: 0, RefColIndex: 0, Update: StaticUpdate}, Operator: Equal, Value: "with spaces", Filter: []Filter(nil), GroupOperator: 0, Stats: 0, StatsCount: 0, StatsType: 0}}
	if err := assertEq(expect, req.Filter); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderFilter3(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: state != 1\nFilter: name = with spaces\nOr: 2"))
	req, _ := ParseRequestFromBuffer(buf)
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
