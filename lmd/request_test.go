package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestRequestHeader(t *testing.T) {
	lmd := createTestLMDInstance()
	testRequestStrings := []string{
		"GET hosts\n\n",
		"GET hosts\nColumns: name state\n\n",
		"GET hosts\nColumns: name state\nFilter: state != 1\n\n",
		"GET hosts\nOutputFormat: wrapped_json\nColumnHeaders: on\n\n",
		"GET hosts\nResponseHeader: fixed16\n\n",
		"GET hosts\nColumns: name state\nFilter: state != 1\nFilter: is_executing = 1\nOr: 2\n\n",
		"GET hosts\nColumns: name state\nFilter: state != 1\nFilter: is_executing = 1\nAnd: 2\nFilter: state = 1\nOr: 2\nFilter: name = test\n\n",
		"GET hosts\nBackends: mockid0\n\n",
		"GET hosts\nLimit: 25\nOffset: 5\n\n",
		"GET hosts\nSort: name asc\nSort: state desc\n\n",
		"GET hosts\nStats: state = 1\nStats: avg latency\nStats: state = 3\nStats: state != 1\nStatsAnd: 2\n\n",
		"GET hosts\nColumns: name\nFilter: notes ~~ test\n\n",
		"GET hosts\nColumns: name\nFilter: notes !~ Test\n\n",
		"GET hosts\nColumns: name\nFilter: notes !~~ test\n\n",
		"GET hosts\nColumns: name\nFilter: custom_variables ~~ TAGS test\n\n",
		"GET hosts\nColumns: name\nFilter: custom_variables = TAGS\n\n",
		"GET hosts\nColumns: name\nFilter: name !=\n\n",
		"COMMAND [123456] TEST\n\n",
		"GET hosts\nColumns: name\nFilter: name = test\nWaitTrigger: all\nWaitObject: test\nWaitTimeout: 10000\nWaitCondition: last_check > 1473760401\n\n",
		"GET hosts\nColumns: name\nFilter: latency != 1.23456789012345\n\n",
		"GET hosts\nColumns: name comments\nFilter: comments >= 1\n\n",
		"GET hosts\nColumns: name contact_groups\nFilter: contact_groups >= test\n\n",
		"GET hosts\nColumns: name\nFilter: last_check >= 123456789\n\n",
		"GET hosts\nColumns: name\nFilter: last_check =\n\n",
		"GET hosts\nAuthUser: testUser\n\n",
		"GET hosts\nColumns: name\nFilter: contact_groups >= test\nNegate:\n\n",
		"GET hosts\nColumns: name\nFilter: state ~~ 0|1|2\n\n",
		"GET hosts\nStats: contact_groups >= test\nStatsNegate:\n\n",
	}
	for _, str := range testRequestStrings {
		buf := bufio.NewReader(bytes.NewBufferString(str))
		req, _, err := NewRequest(context.TODO(), lmd, buf, ParseDefault)
		if err != nil {
			t.Fatal(err)
		}
		if err = assertEq(str, req.String()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRequestHeaderTable(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err := assertEq("hosts", req.Table.String()); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderLimit(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nLimit: 10\n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err := assertEq(10, *req.Limit); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderOffset(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nOffset: 3\n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err := assertEq(3, req.Offset); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderColumns(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name state\n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err := assertEq([]string{"name", "state"}, req.Columns); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderSort(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, _ := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: latency state name\nSort: name desc\nSort: state asc\n")), ParseOptimize)
	if err := assertEq(&SortField{Name: "name", Direction: Desc, Index: 0, Column: Objects.Tables[TableHosts].GetColumn("name")}, req.Sort[0]); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(&SortField{Name: "state", Direction: Asc, Index: 0, Column: Objects.Tables[TableHosts].GetColumn("state")}, req.Sort[1]); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderSortCust(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, _ := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name custom_variables\nSort: custom_variables TEST asc\n")), ParseOptimize)
	if err := assertEq(&SortField{Name: "custom_variables", Direction: Asc, Index: 0, Args: "TEST", Column: Objects.Tables[TableHosts].GetColumn("custom_variables")}, req.Sort[0]); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderFilter1(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: name != test\n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err := assertEq(len(req.Filter), 1); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(req.Filter[0].Column.Name, "name"); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderFilter2(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: state != 1\nFilter: name = with spaces \n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err := assertEq(len(req.Filter), 2); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(req.Filter[0].Column.Name, "state"); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(req.Filter[1].Column.Name, "name"); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(req.Filter[1].StrValue, "with spaces"); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderFilter3(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: state != 1\nFilter: name = with spaces\nOr: 2"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err := assertEq(len(req.Filter), 1); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(len(req.Filter[0].Filter), 2); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(req.Filter[0].GroupOperator, Or); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderFilter4(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: state != 1\nFilter: name = with spaces\nAnd: 2"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err := assertEq(len(req.Filter), 2); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(len(req.Filter[0].Filter), 0); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(req.Filter[0].Column.Name, "state"); err != nil {
		t.Fatal(err)
	}
}

func TestRequestListFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= example\nSort: name asc")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq("testhost_1", res[0][0]); err != nil {
		t.Fatal(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestRegexIntFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name state\nFilter: state ~~ 0|1|2")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(10, len(res)); err != nil {
		t.Fatal(err)
	}

	res, _, err = peer.QueryString("GET hosts\nColumns: name state\nFilter: state ~~ 8|9")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(0, len(res)); err != nil {
		t.Fatal(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestHeaderMultipleCommands(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString(`COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web1;1473627610
Backends: mockid0

COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web2;1473627610`))
	req, size, err := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(size, 87); err != nil {
		t.Fatal(err)
	}
	if err = assertEq(req.Command, "COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web1;1473627610"); err != nil {
		t.Fatal(err)
	}
	if err = assertEq(req.Backends[0], "mockid0"); err != nil {
		t.Fatal(err)
	}
	req, size, err = NewRequest(context.TODO(), lmd, buf, ParseOptimize)
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
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	testRequestStrings := []ErrorRequest{
		{"", "bad request: empty request"},
		{"NOE", "bad request: NOE"},
		{"GET none\nColumns: none", "bad request: table none does not exist"},
		{"GET hosts\nnone", "bad request: syntax error in: none"},
		{"GET hosts\nNone: blah", "bad request: unrecognized header in: None: blah"},
		{"GET hosts\nLimit: x", "bad request: expecting a positive number in: Limit: x"},
		{"GET hosts\nLimit: -1", "bad request: expecting a positive number in: Limit: -1"},
		{"GET hosts\nOffset: x", "bad request: expecting a positive number in: Offset: x"},
		{"GET hosts\nOffset: -1", "bad request: expecting a positive number in: Offset: -1"},
		{"GET hosts\nSort: name none", "bad request: unrecognized sort direction, must be asc or desc in: Sort: name none"},
		{"GET hosts\nResponseheader: none", "bad request: unrecognized responseformat, only fixed16 is supported in: Responseheader: none"},
		{"GET hosts\nOutputFormat: csv: none", "bad request: unrecognized outputformat, choose from json, wrapped_json and python in: OutputFormat: csv: none"},
		{"GET hosts\nStatsAnd: 1", "bad request: not enough filter on stack in: StatsAnd: 1"},
		{"GET hosts\nStatsOr: 1", "bad request: not enough filter on stack in: StatsOr: 1"},
		{"GET hosts\nFilter: name", "bad request: filter header must be Filter: <field> <operator> <value> in: Filter: name"},
		{"GET hosts\nFilter: name ~~ *^", "bad request: invalid regular expression: error parsing regexp: missing argument to repetition operator: `*` in: Filter: name ~~ *^"},
		{"GET hosts\nStats: name", "bad request: stats header, must be Stats: <field> <operator> <value> OR Stats: <sum|avg|min|max> <field> in: Stats: name"},
		{"GET hosts\nFilter: name !=\nAnd: x", "bad request: And must be a positive number in: And: x"},
		{"GET hosts\nColumns: name\nFilter: custom_variables =", "bad request: custom variable filter must have form \"Filter: custom_variables <op> <variable> [<value>]\" in: Filter: custom_variables ="},
		{"GET hosts\nKeepalive: broke", "bad request: must be 'on' or 'off' in: Keepalive: broke"},
	}

	for _, er := range testRequestStrings {
		_, _, err := peer.QueryString(er.Request)
		if err == nil {
			t.Fatalf("No Error in Request: " + er.Request)
		}
		if err = assertEq(er.Error, err.Error()); err != nil {
			t.Error("Request: " + er.Request)
			t.Fatalf(err.Error())
		}
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestNestedFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	query := `GET services
Columns: host_name description state peer_key
Filter: description ~~ testsvc_1
Filter: display_name ~~ testsvc_1
Or: 2
Filter: host_name !~~ testhost_1
Filter: host_name !~~ testhost_[2-6]
And: 2
And: 2
Limit: 100
Offset: 0
Sort: host_name asc
Sort: description asc
OutputFormat: wrapped_json
ResponseHeader: fixed16
`
	res, _, err := peer.QueryString(query)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(3, len(res)); err != nil {
		t.Fatal(err)
	}

	if err = assertEq("testhost_7", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq("testsvc_1", res[0][1]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestStats(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(4, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name latency\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(40, len(res)); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET hosts\nStats: sum latency\nStats: avg latency\nStats: min has_been_checked\nStats: max execution_time\nStats: name !=\n")
	if err != nil {
		t.Fatal(err)
	}

	if err = assertEq(3.346320092680001, res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq(0.08365800231700002, res[0][1]); err != nil {
		t.Error(err)
	}
	if err = assertEq(float64(1), res[0][2]); err != nil {
		t.Error(err)
	}
	if err = assertEq(0.005645, res[0][3]); err != nil {
		t.Error(err)
	}
	if err = assertEq(float64(40), res[0][4]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestStatsGroupBy(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(4, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name\nStats: avg latency\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(10, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq(0.083658002317, res[1][1]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET hosts\nColumns: name alias\nStats: avg latency\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(10, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_1_ALIAS", res[0][1]); err != nil {
		t.Error(err)
	}
	if err = assertEq(0.083658002317, res[1][2]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestStatsEmpty(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(2, 0, 0)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nFilter: check_type = 15\nStats: sum percent_state_change\nStats: min percent_state_change\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Fatal(err)
	}
	if err = assertEq(float64(0), res[0][0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestStatsBroken(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 0, 0)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nStats: sum name\nStats: avg contacts\nStats: min plugin_output\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(float64(0), res[0][0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestStatsFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(4, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name latency\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(40, len(res)); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString(`GET hosts
Stats: has_been_checked = 0
Stats: has_been_checked = 1
Stats: state = 0
StatsAnd: 2
Stats: has_been_checked = 1
Stats: state = 1
StatsAnd: 2
Stats: has_been_checked = 1
Stats: state = 2
StatsAnd: 2
Stats: avg latency
`)
	if err != nil {
		t.Fatal(err)
	}

	if err = assertEq(float64(0), res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq(float64(40), res[0][1]); err != nil {
		t.Error(err)
	}
	if err = assertEq(float64(0), res[0][2]); err != nil {
		t.Error(err)
	}
	if err = assertEq(float64(0), res[0][3]); err != nil {
		t.Error(err)
	}
	if err = assertEq(0.08365800231700002, res[0][4]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestRefs(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res1, _, err := peer.QueryString("GET hosts\nColumns: name latency check_command\nLimit: 1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res1)); err != nil {
		t.Error(err)
	}

	res2, _, err := peer.QueryString("GET services\nColumns: host_name host_latency host_check_command\nFilter: host_name = " + res1[0][0].(string) + "\nLimit: 1\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err = assertEq(res1[0], res2[0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestBrokenColumns(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: host_name alias\nFilter: host_name = testhost_1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_1_ALIAS", res[0][1]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestGroupByTable(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET servicesbyhostgroup\nColumns: host_name description host_groups groups host_alias host_address\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(10, len(res)); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq("127.0.0.1", res[0][5]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestBlocking(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	start := time.Now()

	// start long running query in background
	errs := make(chan error, 1)
	go func() {
		_, _, err := peer.QueryString("GET hosts\nColumns: name latency check_command\nLimit: 1\nWaitTrigger: all\nWaitTimeout: 5000\nWaitCondition: state = 99\n\n")
		errs <- err
	}()

	// test how long next query will take
	_, _, err1 := peer.QueryString("GET hosts\nColumns: name latency check_command\nLimit: 1\n\n")
	if err1 != nil {
		t.Fatal(err1)
	}

	elapsed := time.Since(start)
	if elapsed.Seconds() > 3 {
		t.Error("query2 should return immediately")
	}

	// check non-blocking if there were any errors in the long running query so far
	select {
	case err2 := <-errs:
		if err2 != nil {
			t.Fatal(err2)
		}
	default:
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestSort(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name latency\nSort: latency asc\nSort: name asc\nLimit: 5\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(5, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestSort2(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 20, 20)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(20, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_10", res[1][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_11", res[2][0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestSortColumnNotRequested(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name state alias\nSort: latency asc\nSort: name asc\nLimit: 5\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(5, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(3, len(res[0])); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestNoColumns(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET status\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(2, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(51, len(res[0])); err != nil {
		t.Error(err)
	}
	if err = assertEq("program_start", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq("mockid0", res[1][36]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestUnknownOptionalColumns(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name is_impact\nLimit: 1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq(float64(-1), res[0][1]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestUnknownOptionalRefsColumns(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET services\nColumns: host_name host_is_impact\nLimit: 1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq(float64(-1), res[0][1]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET services\nColumns: host_name\nFilter: host_is_impact != -1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(0, len(res)); err != nil {
		t.Fatal(err)
	}

	res, _, err = peer.QueryString("GET services\nColumns: host_name\nFilter: host_is_impact = -1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(10, len(res)); err != nil {
		t.Fatal(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestColumnsWrappedJson(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name state alias\nOutputFormat: wrapped_json\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(10, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_1", (res)[0][0]); err != nil {
		t.Error(err)
	}

	peer.lmd.Config.SaveTempRequests = true
	res, meta, err := peer.QueryString("GET hosts\nColumns: name state alias\nOutputFormat: wrapped_json\nColumnHeaders: on\nLimit: 5\n\n")
	if err != nil {
		t.Fatal(err)
	}
	var jsonTest interface{}
	jErr := json.Unmarshal(peer.last.Response, &jsonTest)
	if jErr != nil {
		t.Fatal(jErr)
	}
	if err = assertEq(5, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(10), meta.Total); err != nil {
		t.Error(err)
	}
	if err = assertEq("name", meta.Columns[0]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET hosts\nColumns: name state alias\nOutputFormat: json\n\n")
	if err != nil {
		t.Fatal(err)
	}
	jErr = json.Unmarshal(peer.last.Response, &jsonTest)
	if jErr != nil {
		t.Fatal(jErr)
	}
	if err = assertEq(10, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestCommands(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("COMMAND [0] test_ok\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Errorf("result for successful command should be empty")
	}

	res, _, err = peer.QueryString("COMMAND [0] test_broken\n\n")
	if err == nil {
		t.Fatal("expected error for broken command")
	}
	if res != nil {
		t.Errorf("result for unsuccessful command should be empty")
	}
	if err2 := assertEq(err.Error(), "command broken"); err2 != nil {
		t.Error(err2)
	}
	if err2 := assertEq(err.(*PeerCommandError).code, 400); err2 != nil {
		t.Error(err2)
	}

	_, _, err = peer.QueryString("COMMAND [123.456] test_broken\n\n")
	if err == nil {
		t.Fatal("expected error for broken command")
	}
	if err2 := assertEq(err.Error(), "bad request: COMMAND [123.456] test_broken"); err2 != nil {
		t.Error(err2)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestHTTPCommands(t *testing.T) {
	lmd := createTestLMDInstance()
	peer, cleanup := GetHTTPMockServerPeer(t, lmd)
	defer cleanup()

	res, _, err := peer.QueryString("COMMAND [0] test_ok")
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Errorf("result for successful command should be empty")
	}

	res, _, err = peer.QueryString("COMMAND [0] test_broken")
	if err == nil {
		t.Fatal("expected error for broken command")
	}
	if res != nil {
		t.Errorf("result for unsuccessful command should be empty")
	}
	if err2 := assertEq("command broken", err.Error()); err2 != nil {
		t.Error(err2)
	}
	if err2 := assertEq(400, err.(*PeerCommandError).code); err2 != nil {
		t.Error(err2)
	}
	if err2 := assertEq(2.20, peer.StatusGet(ThrukVersion)); err2 != nil {
		t.Errorf("version set correctly: %s", err2.Error())
	}

	// newer thruk versions return result directly
	thrukVersion := 2.26
	peer.StatusSet(ThrukVersion, thrukVersion)

	res, _, err = peer.QueryString("COMMAND [0] test_ok")
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Errorf("result for successful command should be empty")
	}

	res, _, err = peer.QueryString("COMMAND [0] test_broken")
	if err == nil {
		t.Fatal("expected error for broken command")
	}
	if res != nil {
		t.Errorf("result for unsuccessful command should be empty")
	}
	if err2 := assertEq("command broken", err.Error()); err2 != nil {
		t.Error(err2)
	}
	if err2 := assertEq(400, err.(*PeerCommandError).code); err2 != nil {
		t.Error(err2)
	}
	if err2 := assertEq(thrukVersion, peer.StatusGet(ThrukVersion)); err2 != nil {
		t.Errorf("version unchanged: %s", err2.Error())
	}
}

func TestHTTPPeer(t *testing.T) {
	lmd := createTestLMDInstance()
	peer, cleanup := GetHTTPMockServerPeer(t, lmd)
	defer cleanup()

	if err := peer.InitAllTables(); err != nil {
		t.Error(err)
	}
}

func TestRequestPassthrough(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(5, 10, 10)
	PauseTestPeers(peer)

	// query without virtual columns
	res, _, err := peer.QueryString("GET log\nColumns: time type message\nLimit: 3\nSort: time asc\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(3, len(res)); err != nil {
		t.Fatal(err)
	}
	if err = assertEq(float64(1630307788), res[0][0]); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("SERVICE ALERT", res[0][1]); err != nil {
		t.Fatal(err)
	}

	// query with extra virtual columns
	res, _, err = peer.QueryString("GET log\nColumns: time peer_key type message\nLimit: 3\nSort: peer_key asc\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(3, len(res)); err != nil {
		t.Fatal(err)
	}
	if err = assertEq(float64(1630307788), res[0][0]); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("mockid0", res[0][1]); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("SERVICE ALERT", res[0][2]); err != nil {
		t.Fatal(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestSites(t *testing.T) {
	extraConfig := `
    Listen = ["test.sock"]

    [[Connections]]
    name   = 'offline1'
    id     = 'offline1'
    source = ['/does/not/exist.sock']

    [[Connections]]
    name   = 'offline2'
    id     = 'offline2'
    source = ['/does/not/exist.sock']
    `
	peer, cleanup, _ := StartTestPeerExtra(4, 10, 10, extraConfig)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET sites\nColumns: name status last_error\nSort: name")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(6, len(res)); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("offline2", res[5][0]); err != nil {
		t.Fatal(err)
	}
	if err = assertEq(float64(2), res[5][1]); err != nil {
		t.Fatal(err)
	}
	if err = assertLike("connect: no such file or directory", res[5][2].(string)); err != nil {
		t.Fatal(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

/* Tests that getting columns based on <table>_<colum-name> works */
func TestTableNameColName(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: host_name\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET hostgroups\nColumns: hostgroup_name\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("Everything", res[0][0]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET hostgroups\nColumns: hostgroup_name\nFilter: hostgroup_name = host_1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("host_1", res[0][0]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET hostsbygroup\nColumns: host_name\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testhost_1", res[0][0]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET servicesbygroup\nColumns: service_description\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testsvc_1", res[0][0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestMembersWithState(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hostgroups\nColumns: members_with_state\nFilter: name = host_1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	value := res[0][0].([]interface{})[0].([]interface{})
	if err = assertEq("testhost_1", value[0]); err != nil {
		t.Error(err)
	}
	if err = assertEq(0., value[1]); err != nil {
		t.Error(err)
	}
	if err = assertEq(1., value[2]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET servicegroups\nColumns: members_with_state\nFilter: name = svc4\n\n")
	if err != nil {
		t.Fatal(err)
	}
	value = res[0][0].([]interface{})[0].([]interface{})
	if err := assertEq("testhost_2", value[0]); err != nil {
		t.Error(err)
	}
	if err := assertEq("testsvc_1", value[1]); err != nil {
		t.Error(err)
	}
	if err := assertEq(0., value[2]); err != nil {
		t.Error(err)
	}
	if err := assertEq(1., value[3]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestCustomVarCol(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: custom_variables custom_variable_names custom_variable_values\n\n")
	if err != nil {
		t.Fatal(err)
	}
	val := res[0][1].([]interface{})
	if err = assertEq("TEST", val[0].(string)); err != nil {
		t.Error(err)
	}
	val = res[0][2].([]interface{})
	if err = assertEq("1", val[0].(string)); err != nil {
		t.Error(err)
	}
	hash := res[0][0].(map[string]interface{})
	if err = assertEq("1", hash["TEST"].(string)); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestCustomVarColContacts(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET contacts\nColumns: custom_variables custom_variable_names custom_variable_values\n\n")
	if err != nil {
		t.Fatal(err)
	}

	val := res[0][1].([]interface{})
	// custom_variable_names
	if err = assertEq("FOO", val[0].(string)); err != nil {
		t.Error(err)
	}
	// custom_variable_values
	val = res[0][2].([]interface{})
	if err = assertEq("12345", val[0].(string)); err != nil {
		t.Error(err)
	}
	// custom_variables
	hash := res[0][0].(map[string]interface{})
	if err = assertEq("12345", hash["FOO"].(string)); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestServiceHardStateColumns(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	testQueries := []struct {
		query    string
		expected int
	}{
		{"GET hostgroups\nColumns: num_services_hard_crit\n\n", 1},
		{"GET hostgroups\nColumns: num_services_hard_ok\n\n", 5},
		{"GET hostgroups\nColumns: num_services_hard_unknown\n\n", 0},
		{"GET hostgroups\nColumns: num_services_hard_warn\n\n", 2},
		{"GET hostgroups\nColumns: worst_service_hard_state\n\n", 2},
		{"GET hosts\nColumns: num_services_hard_crit\n\n", 1},
		{"GET hosts\nColumns: num_services_hard_ok\n\n", 5},
		{"GET hosts\nColumns: num_services_hard_unknown\n\n", 0},
		{"GET hosts\nColumns: num_services_hard_warn\n\n", 2},
		{"GET hosts\nColumns: worst_service_hard_state\n\n", 1},
		{"GET servicegroups\nColumns: num_services_hard_crit\n\n", 0},
		{"GET servicegroups\nColumns: num_services_hard_ok\n\n", 0},
		{"GET servicegroups\nColumns: num_services_hard_unknown\n\n", 0},
		{"GET servicegroups\nColumns: num_services_hard_warn\n\n", 0},
	}

	for _, req := range testQueries {
		res, _, err := peer.QueryString(req.query)
		if err != nil {
			t.Fatal(err)
		}
		if err = assertEq(float64(req.expected), res[0][0]); err != nil {
			t.Errorf("Query failed: %q\n%s", req.query, err)
		}
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestWorstServiceState(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	testQueries := []struct {
		query    string
		expected int
	}{
		{"GET hostgroups\nColumns: worst_service_state\n\n", 2},
		{"GET hosts\nColumns: worst_service_state\n\n", 2},
		{"GET servicegroups\nColumns: worst_service_state\n\n", 1},
	}

	for _, req := range testQueries {
		res, _, err := peer.QueryString(req.query)
		if err != nil {
			t.Fatal(err)
		}
		if err = assertEq(float64(req.expected), res[0][0]); err != nil {
			t.Errorf("Query failed: %q\n%s", req.query, err)
		}
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestShouldBeScheduled(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: should_be_scheduled\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err = assertEq(1., res[0][0]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET services\nColumns: should_be_scheduled\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err := assertEq(1., res[0][0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestNegate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name\nFilter: host_name = testhost_1\nNegate:\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(9, len(res)); err != nil {
		t.Error(err)
	}

	for _, host := range res {
		if err = assertNeq("testhost_1", host[0]); err != nil {
			t.Error(err)
		}
	}

	res, _, err = peer.QueryString("GET services\nColumns: host_name state\nFilter: host_name = testhost_1\nFilter: state = 0\nNegate:\nAnd: 2\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err = assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	for _, host := range res {
		if err = assertEq("testhost_1", host[0]); err != nil {
			t.Error(err)
		}

		if err = assertNeq("0", host[1]); err != nil {
			t.Error(err)
		}
	}

	res, _, err = peer.QueryString("GET services\nColumns: host_name state\nFilter: host_name != testhost_1\nFilter: state != 10\nAnd: 2\nNegate:\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err = assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	for _, host := range res {
		if err = assertEq("testhost_1", host[0]); err != nil {
			t.Error(err)
		}

		if err = assertNeq("0", host[1]); err != nil {
			t.Error(err)
		}
	}

	res, _, err = peer.QueryString("GET services\nColumns: host_name state\nFilter: host_name != testhost_1\nFilter: state != 1\nOr: 2\nNegate:\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err = assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	for _, host := range res {
		if err = assertEq("testhost_1", host[0]); err != nil {
			t.Error(err)
		}

		if err = assertNeq("0", host[1]); err != nil {
			t.Error(err)
		}
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestStatsNegate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nStats: host_name = testhost_1\nStatsNegate:\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(float64(9), res[0][0]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET services\nStats: host_name ~ testhost_1\nStats: state = 0\nStatsNegate:\nStatsAnd: 2\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err = assertEq(float64(1), res[0][0]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET services\nStats: host_name != testhost_1\nStats: state != 10\nStatsAnd: 2\nStatsNegate:\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err = assertEq(float64(1), res[0][0]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET services\nStats: host_name != testhost_1\nStats: state != 1\nStatsOr: 2\nStatsNegate:\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err = assertEq(float64(1), res[0][0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestComments(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET comments\nColumns: id host_name service_description\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(2, len(res)); err != nil {
		t.Fatal(err)
	}
	if err = assertEq(float64(1), res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq(float64(2), res[1][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_2", res[1][1]); err != nil {
		t.Error(err)
	}

	res, _, err = peer.QueryString("GET hosts\nColumns: comments\nFilter: name = testhost_2\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Fatal(err)
	}
	if err = assertEq([]interface{}{interface{}(float64(2))}, res[0][0]); err != nil {
		t.Error(err)
	}

	if err = cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestServicesWithInfo(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: services_with_info\n\n")
	if err != nil {
		t.Fatal(err)
	}
	value := res[0][0].([]interface{})[0].([]interface{})

	// service description
	if err = assertEq("testsvc_1", value[0]); err != nil {
		t.Error(err)
	}

	// state
	if err = assertEq(1.0, value[1]); err != nil {
		t.Error(err)
	}

	// has_been_checked
	if err = assertEq(1.0, value[2]); err != nil {
		t.Error(err)
	}

	// plugin output
	expectedPluginOutput := "HTTP WARNING: HTTP/1.1 403 Forbidden - 5215 bytes in 0.001 second response time"
	if err = assertEq(expectedPluginOutput, value[3]); err != nil {
		t.Error(err)
	}

	if err = cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestLowercaseFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	query := `GET hosts
Columns: name state peer_key
Filter: name !~~ testHOST_2
OutputFormat: wrapped_json
ResponseHeader: fixed16
`

	req, _, err := NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("name_lc", req.Filter[0].Column.Name); err != nil {
		t.Error(err)
	}

	res, meta, err := peer.QueryString(query)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(int64(9), meta.Total); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testhost_9", res[8][0]); err != nil {
		t.Error(err)
	}

	query = `GET services
	Columns: host_name host_alias description state peer_key
	Filter: host_name ~~ testHOST_2
	OutputFormat: wrapped_json
	ResponseHeader: fixed16
	`
	req, _, err = NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("host_name_lc", req.Filter[0].Column.Name); err != nil {
		t.Error(err)
	}
	res, meta, err = peer.QueryString(query)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(int64(1), meta.Total); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testhost_2", res[0][0]); err != nil {
		t.Error(err)
	}

	query = `GET services
	Columns: host_name host_alias host_name_lc host_alias_lc description state peer_key
	Filter: host_alias ~~ testHOST_2_alias
	OutputFormat: wrapped_json
	ResponseHeader: fixed16
	`
	req, _, err = NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("host_alias_lc", req.Filter[0].Column.Name); err != nil {
		t.Error(err)
	}
	res, meta, err = peer.QueryString(query)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(int64(1), meta.Total); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testhost_2", res[0][0]); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_2", res[0][2]); err != nil {
		t.Error(err)
	}
	if err = assertEq("testhost_2_alias", res[0][3]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestLowercaseHostFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)
	query := `GET hosts
	Columns: alias
	Filter: alias ~~ .*host_1_ALIAS
	OutputFormat: wrapped_json
	ResponseHeader: fixed16
	`
	req, _, err := NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("alias_lc", req.Filter[0].Column.Name); err != nil {
		t.Error(err)
	}
	res, meta, err := peer.QueryString(query)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(int64(1), meta.Total); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("testhost_1_ALIAS", res[0][0]); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestKeepalive(t *testing.T) {
	extraConfig := `
        ListenPrometheus = "127.0.0.1:50999"
	`
	peer, cleanup, mocklmd := StartTestPeerExtra(1, 10, 10, extraConfig)
	PauseTestPeers(peer)
	peer.closeConnectionPool()

	getOpenListeners := func() int64 {
		time.Sleep(KeepAliveWaitInterval)
		l := mocklmd.Listeners["test.sock"]
		l.Lock.RLock()
		num := l.openConnections
		l.Lock.RUnlock()
		return num
	}

	// open connections should be zero
	if err := assertEq(int64(0), getOpenListeners()); err != nil {
		t.Error(err)
	}

	// open first connection
	conn, err := net.DialTimeout("unix", "test.sock", 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// there should be one connection now
	if err := assertEq(int64(1), getOpenListeners()); err != nil {
		t.Error(err)
	}

	req, _, _ := NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name\n")), ParseOptimize)
	req.ResponseFixed16 = true

	// send request without keepalive
	_, err = fmt.Fprintf(conn, "%s", req.String())
	if err != nil {
		t.Fatal(err)
	}

	_, err = peer.parseResponseFixedSize(req, conn)
	if err != nil {
		t.Fatal(err)
	}

	// open connections should be zero, we are done
	if err := assertEq(int64(0), getOpenListeners()); err != nil {
		t.Error(err)
	}

	// send seconds request without keepalive, should fail
	_, err = fmt.Fprintf(conn, "%s", req.String())
	if err == nil {
		t.Fatal(fmt.Errorf("request should fail because connection is closed on server side"))
	}
	conn.Close()

	// open connections should be zero, we are done
	time.Sleep(KeepAliveWaitInterval)
	if err := assertEq(int64(0), getOpenListeners()); err != nil {
		t.Error(err)
	}

	// open connection again
	conn, err = net.DialTimeout("unix", "test.sock", 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	req.KeepAlive = true
	_, err = fmt.Fprintf(conn, "%s", req.String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = peer.parseResponseFixedSize(req, conn)
	if err != nil {
		t.Fatal(err)
	}

	// open connections should be one
	if err := assertEq(int64(1), getOpenListeners()); err != nil {
		t.Error(err)
	}

	// second request should pass as well
	_, err = fmt.Fprintf(conn, "%s", req.String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = peer.parseResponseFixedSize(req, conn)
	if err != nil {
		t.Fatal(err)
	}

	// open connections should be one
	if err := assertEq(int64(1), getOpenListeners()); err != nil {
		t.Error(err)
	}
	conn.Close()

	// open connections should be zero, we are done
	time.Sleep(KeepAliveWaitInterval)
	if err := assertEq(int64(0), getOpenListeners()); err != nil {
		t.Error(err)
	}

	// open connection again
	conn, err = net.DialTimeout("unix", "test.sock", 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	req.KeepAlive = true
	_, err = fmt.Fprintf(conn, "%s", req.String())
	if err != nil {
		t.Fatal(err)
	}
	err = conn.(*net.UnixConn).CloseWrite()
	if err != nil {
		t.Error(err)
	}

	// open connections should be one
	time.Sleep(KeepAliveWaitInterval)
	if err := assertEq(int64(0), getOpenListeners()); err != nil {
		t.Error(err)
	}

	_, err = peer.parseResponseFixedSize(req, conn)
	if err != nil {
		t.Fatal(err)
	}

	// check if connection is really closed
	conn.Close()

	// open connections should zero again
	time.Sleep(KeepAliveWaitInterval)
	if err := assertEq(int64(0), getOpenListeners()); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestIndexedHost(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET hosts\nColumns: name state alias\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: name = testhost_1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.Total); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.RowsScanned); err != nil {
		t.Error(err)
	}
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedService(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description state\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: host_name = testhost_1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.Total); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.RowsScanned); err != nil {
		t.Error(err)
	}
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedServiceByHostRegex(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description state\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: host_name ~~ Test.*1$\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.Total); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.RowsScanned); err != nil {
		t.Error(err)
	}
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedHostByHostgroup(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET hosts\nColumns: name state\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: groups >= Everything\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.Total); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.RowsScanned); err != nil {
		t.Error(err)
	}
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedServiceByHostgroup(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description state\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: host_groups >= Everything\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.Total); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.RowsScanned); err != nil {
		t.Error(err)
	}
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedServiceByServicegroup(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description state\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: groups >= Http Check\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.Total); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(1), meta.RowsScanned); err != nil {
		t.Error(err)
	}
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedServiceByNestedHosts(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString(`GET services
Columns: host_name description state
OutputFormat: wrapped_json
ColumnHeaders: on
Filter: host_name = testhost_1
Filter: description = testsvc_1
And: 2
Filter: host_name = testhost_2
Filter: description = testsvc_1
And: 2
Or: 2

`)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(2, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(2), meta.Total); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(2), meta.RowsScanned); err != nil {
		t.Error(err)
	}
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestStatsQueryOptimizer(t *testing.T) {
	query := `GET services
OutputFormat: wrapped_json
ColumnHeaders: on
Stats: has_been_checked = 1
Stats: has_been_checked = 1
Stats: state = 0
StatsAnd: 2
Stats: has_been_checked = 1
Stats: state = 1
StatsAnd: 2
Stats: has_been_checked = 1
Stats: state = 2
StatsAnd: 2
`
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString(query))
	req, _, err := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(2, len(req.StatsGrouped)); err != nil {
		t.Error(err)
	}
	if err = assertEq(Counter, req.StatsGrouped[0].StatsType); err != nil {
		t.Error(err)
	}
	if err = assertEq(StatsGroup, req.StatsGrouped[1].StatsType); err != nil {
		t.Error(err)
	}
	if err = assertEq(3, len(req.StatsGrouped[1].Filter)); err != nil {
		t.Error(err)
	}
	if err = assertEq(3, req.StatsGrouped[1].Filter[2].StatsPos); err != nil {
		t.Error(err)
	}
}

func TestStatsQueryOptimizer2(t *testing.T) {
	query := `GET services
	Filter: host_name = localhost
	Stats: state_type = 0
	Stats: state = 0
	StatsOr: 2
	Stats: has_been_checked = 1
	StatsAnd: 2
	Stats: state_type = 0
	Stats: state = 0
	StatsOr: 2
	Stats: has_been_checked = 1
	Stats: scheduled_downtime_depth = 0
	Stats: acknowledged = 0
	StatsAnd: 4
	OutputFormat: json
	ResponseHeader: fixed16
`
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString(query))
	req, _, err := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(2, len(req.StatsGrouped)); err != nil {
		t.Error(err)
	}
	if err = assertEq(`Stats: state_type = 0
Stats: state = 0
StatsOr: 2
Stats: has_been_checked = 1
StatsAnd: 2
`, req.StatsGrouped[0].String("Stats")); err != nil {
		t.Error(err)
	}
	if err = assertEq(`Stats: state_type = 0
Stats: state = 0
StatsOr: 2
Stats: has_been_checked = 1
Stats: scheduled_downtime_depth = 0
Stats: acknowledged = 0
StatsAnd: 4
`, req.StatsGrouped[1].String("Stats")); err != nil {
		t.Error(err)
	}
}

func TestQueryWrappedJSON(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString(`GET services
OutputFormat: wrapped_json
ColumnHeaders: on
`)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(int64(10), meta.RowsScanned); err != nil {
		t.Error(err)
	}
	if err = assertEq(10, len(res)); err != nil {
		t.Error(err)
	}
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestServiceCustVarFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description custom_variables\nOutputFormat: wrapped_json\nFilter: custom_variables = TEST 1\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(3, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(3), meta.Total); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(10), meta.RowsScanned); err != nil {
		t.Error(err)
	}
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestServiceCustVarRegexFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description custom_variables\nOutputFormat: wrapped_json\nFilter: custom_variables ~~ TEST2 ^cust\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(3, len(res)); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(3), meta.Total); err != nil {
		t.Error(err)
	}
	if err = assertEq(int64(10), meta.RowsScanned); err != nil {
		t.Error(err)
	}
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}
