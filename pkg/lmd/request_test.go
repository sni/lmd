package lmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		require.NoError(t, err)
		assert.Equal(t, str, req.String())
	}
}

func TestRequestHeaderTable(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	assert.Equal(t, "hosts", req.Table.String())
}

func TestRequestHeaderLimit(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nLimit: 10\n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	assert.Equal(t, 10, *req.Limit)
}

func TestRequestHeaderOffset(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nOffset: 3\n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	assert.Equal(t, 3, req.Offset)
}

func TestRequestHeaderColumns(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name state\n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	assert.Equal(t, []string{"name", "state"}, req.Columns)
}

func TestRequestHeaderSort(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, _ := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: latency state name\nSort: name desc\nSort: state asc\n")), ParseOptimize)
	assert.Equal(t, &SortField{Name: "name", Direction: Desc, Index: 0, Column: Objects.Tables[TableHosts].GetColumn("name")}, req.Sort[0])
	assert.Equal(t, &SortField{Name: "state", Direction: Asc, Index: 0, Column: Objects.Tables[TableHosts].GetColumn("state")}, req.Sort[1])
}

func TestRequestHeaderSortCust(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, _ := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name custom_variables\nSort: custom_variables TEST asc\n")), ParseOptimize)
	assert.Equal(t, &SortField{Name: "custom_variables", Direction: Asc, Index: 0, Args: "TEST", Column: Objects.Tables[TableHosts].GetColumn("custom_variables")}, req.Sort[0])
}

func TestRequestHeaderFilter1(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: name != test\n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	assert.Len(t, req.Filter, 1)
	assert.Equal(t, "name", req.Filter[0].Column.Name)
}

func TestRequestHeaderFilter2(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: state != 1\nFilter: name = with spaces \n"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	assert.Len(t, req.Filter, 2)
	assert.Equal(t, "state", req.Filter[0].Column.Name)
	assert.Equal(t, "name", req.Filter[1].Column.Name)
	assert.Equal(t, "with spaces", req.Filter[1].StrValue)
}

func TestRequestHeaderFilter3(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: state != 1\nFilter: name = with spaces\nOr: 2"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	assert.Len(t, req.Filter, 1)
	assert.Len(t, req.Filter[0].Filter, 2)
	assert.Equal(t, Or, req.Filter[0].GroupOperator)
}

func TestRequestHeaderFilter4(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nFilter: state != 1\nFilter: name = with spaces\nAnd: 2"))
	req, _, _ := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	assert.Len(t, req.Filter, 2)
	assert.Empty(t, req.Filter[0].Filter)
	assert.Equal(t, "state", req.Filter[0].Column.Name)
}

func TestRequestListFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= example\nSort: name asc")
	require.NoError(t, err)
	assert.Equal(t, "testhost_1", res[0][0])

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestRegexIntFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name state\nFilter: state ~~ 0|1|2")
	require.NoError(t, err)
	assert.Len(t, res, 10)

	res, _, err = peer.QueryString("GET hosts\nColumns: name state\nFilter: state ~~ 8|9")
	require.NoError(t, err)
	assert.Empty(t, res)

	if err = cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestRequestHeaderMultipleCommands(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString(`COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web1;1473627610
Backends: mockid0

COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web2;1473627610`))
	req, size, err := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	require.NoError(t, err)
	require.Equal(t, 87, size)
	require.Equal(t, "COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web1;1473627610", req.Command)
	require.Equal(t, "mockid0", req.Backends[0])
	req, size, err = NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	require.NoError(t, err)
	assert.Equal(t, 67, size)
	assert.Equal(t, "COMMAND [1473627610] SCHEDULE_FORCED_SVC_CHECK;demo;Web2;1473627610", req.Command)
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
		{"GET hosts\nOutputFormat: csv: none", "bad request: unrecognized outputformat, choose from json, wrapped_json, python and python3 in: OutputFormat: csv: none"},
		{"GET hosts\nStatsAnd: 1", "bad request: not enough filter on stack in: StatsAnd: 1"},
		{"GET hosts\nStatsOr: 1", "bad request: not enough filter on stack in: StatsOr: 1"},
		{"GET hosts\nFilter: name", "bad request: filter header must be Filter: <field> <operator> <value> in: Filter: name"},
		{"GET hosts\nFilter: name ~~ *^", "bad request: invalid regular expression: error parsing regexp: missing argument to repetition operator: `*` in: Filter: name ~~ *^"},
		{"GET hosts\nStats: name", "bad request: stats header, must be Stats: <field> <operator> <value> OR Stats: <sum|avg|min|max> <field> in: Stats: name"},
		{"GET hosts\nFilter: name !=\nAnd: x", "bad request: And must be a positive number in: And: x"},
		{"GET hosts\nColumns: name\nFilter: custom_variables =", "bad request: custom variable filter must have form \"Filter: custom_variables <op> <variable> [<value>]\" in: Filter: custom_variables ="},
		{"GET hosts\nKeepalive: broke", "bad request: must be 'on' or 'off' in: Keepalive: broke"},
	}

	for _, req := range testRequestStrings {
		_, _, err := peer.QueryString(req.Request)
		require.Errorf(t, err, "Error in Request: "+req.Request)
		require.Equalf(t, req.Error, err.Error(), "Request: "+req.Request)
	}

	err := cleanup()
	require.NoError(t, err)
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
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 4, "result length")

	assert.Equalf(t, "UPPER_3", res[0][0], "hostname matches")
	assert.Equalf(t, "testsvc_1", res[0][1], "service matches")

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestStats(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(4, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name latency\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 40)

	res, _, err = peer.QueryString("GET hosts\nStats: sum latency\nStats: avg latency\nStats: min has_been_checked\nStats: max execution_time\nStats: name !=\n")
	require.NoError(t, err)

	assert.InDelta(t, 3.346320092680001, res[0][0], 0)
	assert.InDelta(t, 0.08365800231700002, res[0][1], 0)
	assert.InDelta(t, float64(1), res[0][2], 0)
	assert.InDelta(t, 0.005645, res[0][3], 0)
	assert.InDelta(t, float64(40), res[0][4], 0)

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestStatsGroupBy(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(4, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name\nStats: avg latency\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 10, "result length")

	assert.Equalf(t, "UPPER_3", res[0][0], "hostname matches")
	assert.InDeltaf(t, 0.083658002317, res[1][1], 0.00001, "latency matches")

	res, _, err = peer.QueryString("GET hosts\nColumns: name alias\nStats: avg latency\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 10, "result length")

	assert.Equalf(t, "UPPER_3", res[0][0], "hostname matches")
	assert.Equalf(t, "UPPER_3_ALIAS", res[0][1], "hostalias matches")
	assert.InDeltaf(t, 0.083658002317, res[1][2], 0.00001, "latency matches")

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestStatsEmpty(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(2, 0, 0)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nFilter: check_type = 15\nStats: sum percent_state_change\nStats: min percent_state_change\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)
	assert.InDelta(t, float64(0), res[0][0], 0)

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestStatsBroken(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 0, 0)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nStats: sum name\nStats: avg contacts\nStats: min plugin_output\n")
	require.NoError(t, err)
	assert.InDelta(t, float64(0), res[0][0], 0)

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestStatsFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(4, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name latency\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 40)

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
	require.NoError(t, err)

	assert.InDelta(t, float64(0), res[0][0], 0)
	assert.InDelta(t, float64(40), res[0][1], 0)
	assert.InDelta(t, float64(0), res[0][2], 0)
	assert.InDelta(t, float64(0), res[0][3], 0)
	assert.InDelta(t, 0.08365800231700002, res[0][4], 0)

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestRefs(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res1, _, err := peer.QueryString("GET hosts\nColumns: name latency check_command\nLimit: 1\n\n")
	require.NoError(t, err)
	assert.Len(t, res1, 1)

	hostName, ok := res1[0][0].(string)
	require.True(t, ok)
	res2, _, err := peer.QueryString("GET services\nColumns: host_name host_latency host_check_command\nFilter: host_name = " + hostName + "\nLimit: 1\n\n")
	require.NoError(t, err)

	assert.Equal(t, res1[0], res2[0])

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestBrokenColumns(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: host_name alias\nFilter: host_name = testhost_1\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, "testhost_1", res[0][0])
	assert.Equal(t, "testhost_1_ALIAS", res[0][1])

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestGroupByTable(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET servicesbyhostgroup\nColumns: host_name description host_groups groups host_alias host_address\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 10, "result length")

	assert.Equalf(t, "UPPER_3", res[0][0], "hostname matches")
	assert.Equalf(t, "127.0.0.1", res[0][5], "address matches")

	err = cleanup()
	require.NoError(t, err)
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

	err := cleanup()
	require.NoError(t, err)
}

func TestRequestSort(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name latency\nSort: latency asc\nSort: name asc\nLimit: 5\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 5, "result length")

	assert.Equalf(t, "UPPER_3", res[0][0], "hostname matches")

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestSort2(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 20, 20)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 20, "result length")

	assert.Equalf(t, "UPPER_3", (res)[0][0], "hostname matches")
	assert.Equalf(t, "testhost_1", (res)[1][0], "hostname matches")
	assert.Equalf(t, "testhost_10", (res)[2][0], "hostname matches")
	assert.Equalf(t, "testhost_11", (res)[3][0], "hostname matches")

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestSortColumnNotRequested(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name state alias\nSort: latency asc\nSort: name asc\nLimit: 5\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 5, "result length")

	require.Lenf(t, res[0], 3, "result length")
	assert.Equalf(t, "UPPER_3", res[0][0], "hostname matches")

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestNoColumns(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET status\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 2)
	assert.Len(t, res[0], 51)
	assert.Equal(t, "program_start", res[0][0])
	assert.Equal(t, "mockid0", res[1][36])

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestUnknownOptionalColumns(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name is_impact\nLimit: 1\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 1, "result length")

	assert.Equalf(t, "UPPER_3", (res)[0][0], "hostname matches")
	assert.InDeltaf(t, -1, (res)[0][1], 0.00001, "is_impact matches")

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestUnknownOptionalRefsColumns(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET services\nColumns: host_name host_is_impact\nLimit: 1\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 1, "result length")

	assert.Equalf(t, "UPPER_3", (res)[0][0], "hostname matches")
	assert.InDeltaf(t, -1, (res)[0][1], 0.00001, "is_impact matches")

	res, _, err = peer.QueryString("GET services\nColumns: host_name\nFilter: host_is_impact != -1\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Emptyf(t, res, "result length")

	res, _, err = peer.QueryString("GET services\nColumns: host_name\nFilter: host_is_impact = -1\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 10, "result length")

	err = cleanup()
	require.NoError(t, err)
}

func TestRequestColumnsWrappedJson(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name state alias\nOutputFormat: wrapped_json\n\n")
	require.NoErrorf(t, err, "query successful")

	require.Lenf(t, res, 10, "result length")
	assert.Equalf(t, "UPPER_3", (res)[0][0], "hostname matches")

	peer.lmd.Config.SaveTempRequests = true
	res, meta, err := peer.QueryString("GET hosts\nColumns: name state alias\nOutputFormat: wrapped_json\nColumnHeaders: on\nLimit: 5\n\n")
	require.NoErrorf(t, err, "query successful")

	var jsonTest interface{}
	jErr := json.Unmarshal(peer.last.Response, &jsonTest)
	require.NoErrorf(t, jErr, "json ok")

	require.Lenf(t, res, 5, "result length")
	assert.Equalf(t, "UPPER_3", (res)[0][0], "hostname matches")
	assert.Equalf(t, int64(10), meta.Total, "meta.total matches")
	assert.Equalf(t, "name", meta.Columns[0], "meta columns matches")

	res, _, err = peer.QueryString("GET hosts\nColumns: name state alias\nOutputFormat: json\n\n")
	require.NoErrorf(t, err, "query successful")

	jErr = json.Unmarshal(peer.last.Response, &jsonTest)
	require.NoErrorf(t, jErr, "json ok")

	require.Lenf(t, res, 10, "result length")
	assert.Equalf(t, "UPPER_3", (res)[0][0], "hostname matches")

	err = cleanup()
	require.NoError(t, err)
}

func TestCommands(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("COMMAND [0] test_ok\n\n")
	require.NoError(t, err)
	assert.Nilf(t, res, "result for successful command should be empty")

	res, _, err = peer.QueryString("COMMAND [0] test_broken\n\n")
	require.Errorf(t, err, "expected error for broken command")
	require.Nilf(t, res, "result for unsuccessful command should be empty")

	assert.Equal(t, "command broken", err.Error())
	assert.Equal(t, 400, err.(*PeerCommandError).code)

	_, _, err = peer.QueryString("COMMAND [123.456] test_broken\n\n")
	require.Errorf(t, err, "expected error for broken command")
	assert.Equal(t, "bad request: COMMAND [123.456] test_broken", err.Error())

	err = cleanup()
	require.NoError(t, err)
}

func TestHTTPCommands(t *testing.T) {
	lmd := createTestLMDInstance()
	peer, cleanup := GetHTTPMockServerPeer(t, lmd)
	defer cleanup()

	res, _, err := peer.QueryString("COMMAND [0] test_ok")
	require.NoError(t, err)
	assert.Nilf(t, res, "result for successful command should be empty")

	res, _, err = peer.QueryString("COMMAND [0] test_broken")
	require.Errorf(t, err, "expected error for broken command")
	assert.Nilf(t, res, "result for successful command should be empty")

	assert.Equal(t, "command broken", err.Error())
	assert.Equal(t, 400, err.(*PeerCommandError).code)
	assert.InDeltaf(t, 2.20, peer.statusGetLocked(ThrukVersion), 0, "version set correctly")

	// newer thruk versions return result directly
	thrukVersion := 2.26
	peer.statusSetLocked(ThrukVersion, thrukVersion)

	res, _, err = peer.QueryString("COMMAND [0] test_ok")
	require.NoError(t, err)
	assert.Nilf(t, res, "result for successful command should be empty")

	res, _, err = peer.QueryString("COMMAND [0] test_broken")
	require.Errorf(t, err, "expected error for broken command")
	assert.Nilf(t, res, "result for successful command should be empty")

	assert.Equal(t, "command broken", err.Error())
	assert.Equal(t, 400, err.(*PeerCommandError).code)
	assert.InDeltaf(t, thrukVersion, peer.statusGetLocked(ThrukVersion), 0, "version set correctly")
}

func TestHTTPPeer(t *testing.T) {
	lmd := createTestLMDInstance()
	peer, cleanup := GetHTTPMockServerPeer(t, lmd)
	defer cleanup()

	if err := peer.InitAllTables(context.TODO()); err != nil {
		t.Error(err)
	}
}

func TestRequestPassthrough(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(5, 10, 10)
	PauseTestPeers(peer)

	// query without virtual columns
	res, _, err := peer.QueryString("GET log\nColumns: time type message\nLimit: 3\nSort: time asc\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 3)
	require.InDelta(t, float64(1630307788), res[0][0], 0)
	require.Equal(t, "SERVICE ALERT", res[0][1])

	// query with extra virtual columns
	res, _, err = peer.QueryString("GET log\nColumns: time peer_key type message\nLimit: 3\nSort: peer_key asc\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 3)
	require.InDelta(t, float64(1630307788), res[0][0], 0)
	require.Equal(t, "mockid0", res[0][1])
	require.Equal(t, "SERVICE ALERT", res[0][2])

	err = cleanup()
	require.NoError(t, err)
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
	require.NoError(t, err)
	assert.Len(t, res, 6)
	require.Equal(t, "offline2", res[5][0])
	require.InDelta(t, float64(2), res[5][1], 0)
	require.Contains(t, res[5][2].(string), "connect: no such file or directory")

	err = cleanup()
	require.NoError(t, err)
}

// Tests that getting columns based on <table>_<colum-name> works.
func TestTableNameColName(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: host_name\n\n")
	require.NoError(t, err)
	assert.Equal(t, "testhost_1", res[0][0])

	res, _, err = peer.QueryString("GET hostgroups\nColumns: hostgroup_name\n\n")
	require.NoError(t, err)
	assert.Equal(t, "Everything", res[0][0])

	res, _, err = peer.QueryString("GET hostgroups\nColumns: hostgroup_name\nFilter: hostgroup_name = host_1\n\n")
	require.NoError(t, err)
	assert.Equal(t, "host_1", res[0][0])

	res, _, err = peer.QueryString("GET hostsbygroup\nColumns: host_name\n\n")
	require.NoError(t, err)
	assert.Equal(t, "testhost_1", res[0][0])

	res, _, err = peer.QueryString("GET servicesbygroup\nColumns: service_description\n\n")
	require.NoError(t, err)
	assert.Equal(t, "testsvc_1", res[0][0])

	err = cleanup()
	require.NoError(t, err)
}

func TestMembersWithState(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hostgroups\nColumns: members_with_state\nFilter: name = host_1\n\n")
	require.NoError(t, err)
	value := res[0][0].([]interface{})[0].([]interface{})
	assert.Equal(t, "testhost_1", value[0])
	assert.InDelta(t, 0., value[1], 0)
	assert.InDelta(t, 1., value[2], 0)

	res, _, err = peer.QueryString("GET servicegroups\nColumns: members_with_state\nFilter: name = svc4\n\n")
	require.NoError(t, err)
	value = res[0][0].([]interface{})[0].([]interface{})
	assert.Equal(t, "testhost_2", value[0])
	assert.Equal(t, "testsvc_1", value[1])
	assert.InDelta(t, 0., value[2], 0)
	assert.InDelta(t, 1., value[3], 0)

	err = cleanup()
	require.NoError(t, err)
}

func TestCustomVarCol(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: custom_variables custom_variable_names custom_variable_values\n\n")
	require.NoError(t, err)
	val := res[0][1].([]interface{})
	assert.Equal(t, "TEST", val[0].(string))
	val = res[0][2].([]interface{})
	assert.Equal(t, "1", val[0].(string))
	hash := res[0][0].(map[string]interface{})
	assert.Equal(t, "1", hash["TEST"].(string))

	err = cleanup()
	require.NoError(t, err)
}

func TestCustomVarColContacts(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET contacts\nColumns: custom_variables custom_variable_names custom_variable_values\n\n")
	require.NoError(t, err)

	val := res[0][1].([]interface{})
	// custom_variable_names
	assert.Equal(t, "FOO", val[0].(string))
	// custom_variable_values
	val = res[0][2].([]interface{})
	assert.Equal(t, "12345", val[0].(string))
	// custom_variables
	hash := res[0][0].(map[string]interface{})
	assert.Equal(t, "12345", hash["FOO"].(string))

	err = cleanup()
	require.NoError(t, err)
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
		require.NoError(t, err)
		assert.InDeltaf(t, float64(req.expected), res[0][0], 0, "Query failed: %q", req.query)
	}

	err := cleanup()
	require.NoError(t, err)
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
		require.NoError(t, err)
		assert.InDeltaf(t, float64(req.expected), res[0][0], 0, "Query failed: %q", req.query)
	}

	err := cleanup()
	require.NoError(t, err)
}

func TestShouldBeScheduled(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: should_be_scheduled\n\n")
	require.NoError(t, err)

	assert.InDelta(t, 1., res[0][0], 0)

	res, _, err = peer.QueryString("GET services\nColumns: should_be_scheduled\n\n")
	require.NoError(t, err)

	assert.InDelta(t, 1., res[0][0], 0)

	err = cleanup()
	require.NoError(t, err)
}

func TestNegate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: name\nFilter: host_name = testhost_1\nNegate:\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 9)

	for _, host := range res {
		assert.NotEqual(t, "testhost_1", host[0])
	}

	res, _, err = peer.QueryString("GET services\nColumns: host_name state\nFilter: host_name = testhost_1\nFilter: state = 0\nNegate:\nAnd: 2\n\n")
	require.NoError(t, err)

	assert.Len(t, res, 1)

	for _, host := range res {
		assert.Equal(t, "testhost_1", host[0])
		assert.NotEqual(t, 0, host[1])
	}

	res, _, err = peer.QueryString("GET services\nColumns: host_name state\nFilter: host_name != testhost_1\nFilter: state != 10\nAnd: 2\nNegate:\n\n")
	require.NoError(t, err)

	assert.Len(t, res, 1)

	for _, host := range res {
		assert.Equal(t, "testhost_1", host[0])
		assert.NotEqual(t, 0, host[1])
	}

	res, _, err = peer.QueryString("GET services\nColumns: host_name state\nFilter: host_name != testhost_1\nFilter: state != 1\nOr: 2\nNegate:\n\n")
	require.NoError(t, err)

	assert.Len(t, res, 1)

	for _, host := range res {
		assert.Equal(t, "testhost_1", host[0])
		assert.NotEqual(t, 0, host[1])
	}

	err = cleanup()
	require.NoError(t, err)
}

func TestStatsNegate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nStats: host_name = testhost_1\nStatsNegate:\n\n")
	require.NoError(t, err)
	assert.InDelta(t, float64(9), res[0][0], 0)

	res, _, err = peer.QueryString("GET services\nStats: host_name ~ testhost_1\nStats: state = 0\nStatsNegate:\nStatsAnd: 2\n\n")
	require.NoError(t, err)

	assert.InDelta(t, float64(1), res[0][0], 0)

	res, _, err = peer.QueryString("GET services\nStats: host_name != testhost_1\nStats: state != 10\nStatsAnd: 2\nStatsNegate:\n\n")
	require.NoError(t, err)

	assert.InDelta(t, float64(1), res[0][0], 0)

	res, _, err = peer.QueryString("GET services\nStats: host_name != testhost_1\nStats: state != 1\nStatsOr: 2\nStatsNegate:\n\n")
	require.NoError(t, err)

	assert.InDelta(t, float64(1), res[0][0], 0)

	err = cleanup()
	require.NoError(t, err)
}

func TestComments(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET comments\nColumns: id host_name service_description\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 2)
	assert.InDelta(t, float64(1), res[0][0], 0)
	assert.InDelta(t, float64(2), res[1][0], 0)
	assert.Equal(t, "testhost_2", res[1][1])

	res, _, err = peer.QueryString("GET hosts\nColumns: comments\nFilter: name = testhost_2\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, []interface{}{interface{}(float64(2))}, res[0][0])

	if err = cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestServicesWithInfo(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nColumns: services_with_info\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 10, "result length")

	value := res[0][0].([]interface{})[0].([]interface{})

	// service description
	assert.Equalf(t, "testsvc_1", value[0], "service matches")

	// state
	assert.InDeltaf(t, 1, value[1], 0.00001, "state matches")

	// has_been_checked
	assert.InDeltaf(t, 1, value[2], 0.00001, "has_been_checked matches")

	// plugin output
	assert.Equalf(t, "HTTP WARNING: HTTP/1.1 403 Forbidden - 5215 bytes in 0.004 second response time", value[3], "plugin output matches")

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
	require.NoErrorf(t, err, "query successful")
	assert.Equalf(t, "name_lc", req.Filter[0].Column.Name, "column name is correct")

	res, meta, err := peer.QueryString(query)
	require.NoErrorf(t, err, "query string successful")
	require.Lenf(t, res, 9, "result length")
	assert.Equalf(t, int64(9), meta.Total, "meta.Total is correct")
	assert.Equalf(t, int64(10), meta.RowsScanned, "meta.RowsScanned is correct")
	assert.Equalf(t, "testhost_9", res[8][0], "hostname is correct")

	query = `GET services
	Columns: host_name host_alias description state peer_key
	Filter: host_name ~~ testHOST_2
	OutputFormat: wrapped_json
	ResponseHeader: fixed16
	`
	req, _, err = NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	require.NoErrorf(t, err, "request successful")
	assert.Equalf(t, "host_name_lc", req.Filter[0].Column.Name, "column name is correct")

	res, meta, err = peer.QueryString(query)
	require.NoErrorf(t, err, "request successful")
	require.Lenf(t, res, 1, "result length")
	assert.Equalf(t, int64(1), meta.Total, "meta.Total is correct")
	assert.Equalf(t, int64(1), meta.RowsScanned, "meta.RowsScanned is correct")
	assert.Equalf(t, "testhost_2", res[0][0], "hostname is correct")

	query = `GET services
	Columns: host_name host_alias host_name_lc host_alias_lc description state peer_key
	Filter: host_alias ~~ testHOST_2_alias
	OutputFormat: wrapped_json
	ResponseHeader: fixed16
	`
	req, _, err = NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	require.NoErrorf(t, err, "request successful")
	assert.Equalf(t, "host_alias_lc", req.Filter[0].Column.Name, "column name is correct")

	res, meta, err = peer.QueryString(query)
	require.NoErrorf(t, err, "query string successful")
	require.Lenf(t, res, 1, "result length")
	assert.Equalf(t, int64(1), meta.Total, "meta.Total is correct")
	assert.Equalf(t, int64(10), meta.RowsScanned, "meta.RowsScanned is correct")
	assert.Equalf(t, "testhost_2", res[0][0], "hostname is correct")
	assert.Equalf(t, "testhost_2", res[0][2], "hostname is correct")
	assert.Equalf(t, "testhost_2_alias", res[0][3], "alias is correct")

	// case insensitive search on upper case name
	query = `GET services
	Columns: host_name description
	Filter: host_name ~~ UPPER
	OutputFormat: wrapped_json
	ResponseHeader: fixed16
	`
	req, _, err = NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	require.NoErrorf(t, err, "request successful")
	assert.Equalf(t, "host_name_lc", req.Filter[0].Column.Name, "column name is correct")

	res, meta, err = peer.QueryString(query)
	require.NoErrorf(t, err, "query string successful")
	require.Lenf(t, res, 1, "result length")
	assert.Equalf(t, int64(1), meta.Total, "meta.Total is correct")
	assert.Equalf(t, int64(1), meta.RowsScanned, "meta.RowsScanned is correct")
	assert.Equalf(t, "UPPER_3", res[0][0], "hostname is correct")
	assert.Equalf(t, "testsvc_1", res[0][1], "service is correct")

	// case sensitive search on upper case name
	query = `GET services
	Columns: host_name description
	Filter: host_name ~ UPPER
	OutputFormat: wrapped_json
	ResponseHeader: fixed16
	`
	req, _, err = NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	require.NoErrorf(t, err, "request successful")
	assert.Equalf(t, "host_name", req.Filter[0].Column.Name, "column name is correct")

	res, meta, err = peer.QueryString(query)
	require.NoErrorf(t, err, "query string successful")
	require.Lenf(t, res, 1, "result length")
	assert.Equalf(t, int64(1), meta.Total, "meta.Total is correct")
	assert.Equalf(t, int64(1), meta.RowsScanned, "meta.RowsScanned is correct")
	assert.Equalf(t, "UPPER_3", res[0][0], "hostname is correct")
	assert.Equalf(t, "testsvc_1", res[0][1], "service is correct")

	err = cleanup()
	require.NoError(t, err)
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
	require.NoErrorf(t, err, "request successful")
	assert.Equalf(t, "alias_lc", req.Filter[0].Column.Name, "column name is correct")

	res, meta, err := peer.QueryString(query)
	require.NoErrorf(t, err, "query string successful")
	require.Lenf(t, res, 1, "result length")
	assert.Equalf(t, int64(1), meta.Total, "meta.Total is correct")
	assert.Equalf(t, int64(10), meta.RowsScanned, "meta.RowsScanned is correct")
	assert.Equalf(t, "testhost_1_ALIAS", res[0][0], "hostname is correct")

	// upper case request and case sensitive
	query = `GET hosts
	Columns: name
	Filter: name ~ UPPER
	OutputFormat: wrapped_json
	ResponseHeader: fixed16
	`
	req, _, err = NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	require.NoErrorf(t, err, "request successful")
	assert.Equalf(t, "name", req.Filter[0].Column.Name, "column name is correct")

	res, meta, err = peer.QueryString(query)
	require.NoErrorf(t, err, "query string successful")
	require.Lenf(t, res, 1, "result length")

	assert.Equalf(t, int64(1), meta.Total, "meta.Total is correct")
	assert.Equalf(t, int64(10), meta.RowsScanned, "meta.RowsScanned is correct")
	assert.Equalf(t, "UPPER_3", res[0][0], "hostname is correct")

	// lower case request but case insensitive
	query = `GET hosts
	Columns: name
	Filter: name ~~ upper
	OutputFormat: wrapped_json
	ResponseHeader: fixed16
	`
	req, _, err = NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	require.NoErrorf(t, err, "request successful")
	assert.Equalf(t, "name_lc", req.Filter[0].Column.Name, "column name is correct")

	res, meta, err = peer.QueryString(query)
	require.NoErrorf(t, err, "query string successful")
	require.Lenf(t, res, 1, "result length")

	assert.Equalf(t, int64(1), meta.Total, "meta.Total is correct")
	assert.Equalf(t, int64(10), meta.RowsScanned, "meta.RowsScanned is correct")
	assert.Equalf(t, "UPPER_3", res[0][0], "hostname is correct")

	// not actually a regex
	query = `GET hosts
	Columns: name
	Filter: name ~ ^UPPER_3$
	OutputFormat: wrapped_json
	ResponseHeader: fixed16
	`
	req, _, err = NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	require.NoErrorf(t, err, "request successful")
	assert.Equalf(t, "name", req.Filter[0].Column.Name, "column name is correct")

	res, meta, err = peer.QueryString(query)
	require.NoErrorf(t, err, "query string successful")
	require.Lenf(t, res, 1, "result length")

	assert.Equalf(t, int64(1), meta.Total, "meta.Total is correct")
	assert.Equalf(t, int64(1), meta.RowsScanned, "meta.RowsScanned is correct")
	assert.Equalf(t, "UPPER_3", res[0][0], "hostname is correct")

	err = cleanup()
	require.NoError(t, err)
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
	assert.Equal(t, int64(0), getOpenListeners())

	// open first connection
	conn, err := net.DialTimeout("unix", "test.sock", 60*time.Second)
	require.NoError(t, err)

	// there should be one connection now
	assert.Equal(t, int64(1), getOpenListeners())

	req, _, _ := NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name\n")), ParseOptimize)
	req.ResponseFixed16 = true

	// send request without keepalive
	_, err = fmt.Fprintf(conn, "%s", req.String())
	require.NoError(t, err)

	_, err = peer.parseResponseFixedSize(req, conn)
	require.NoError(t, err)

	// open connections should be zero, we are done
	assert.Equal(t, int64(0), getOpenListeners())

	// send seconds request without keepalive, should fail
	_, err = fmt.Fprintf(conn, "%s", req.String())
	if err == nil {
		t.Fatal(fmt.Errorf("request should fail because connection is closed on server side"))
	}
	conn.Close()

	// open connections should be zero, we are done
	time.Sleep(KeepAliveWaitInterval)
	assert.Equal(t, int64(0), getOpenListeners())

	// open connection again
	conn, err = net.DialTimeout("unix", "test.sock", 60*time.Second)
	require.NoError(t, err)

	req.KeepAlive = true
	_, err = fmt.Fprintf(conn, "%s", req.String())
	require.NoError(t, err)
	_, err = peer.parseResponseFixedSize(req, conn)
	require.NoError(t, err)

	// open connections should be one
	assert.Equal(t, int64(1), getOpenListeners())

	// second request should pass as well
	_, err = fmt.Fprintf(conn, "%s", req.String())
	require.NoError(t, err)
	_, err = peer.parseResponseFixedSize(req, conn)
	require.NoError(t, err)

	// open connections should be one
	assert.Equal(t, int64(1), getOpenListeners())
	conn.Close()

	// open connections should be zero, we are done
	time.Sleep(KeepAliveWaitInterval)
	assert.Equal(t, int64(0), getOpenListeners())

	// open connection again
	conn, err = net.DialTimeout("unix", "test.sock", 60*time.Second)
	require.NoError(t, err)

	req.KeepAlive = true
	_, err = fmt.Fprintf(conn, "%s", req.String())
	require.NoError(t, err)
	err = conn.(*net.UnixConn).CloseWrite()
	require.NoError(t, err)

	// open connections should be one
	time.Sleep(KeepAliveWaitInterval)
	assert.Equal(t, int64(0), getOpenListeners())

	_, err = peer.parseResponseFixedSize(req, conn)
	require.NoError(t, err)

	// check if connection is really closed
	conn.Close()

	// open connections should zero again
	time.Sleep(KeepAliveWaitInterval)
	assert.Equal(t, int64(0), getOpenListeners())

	err = cleanup()
	require.NoError(t, err)
}

func TestIndexedHost(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET hosts\nColumns: name state alias\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: name = testhost_1\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, int64(1), meta.Total)
	assert.Equal(t, int64(1), meta.RowsScanned)
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedService(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description state\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: host_name = testhost_1\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, int64(1), meta.Total)
	assert.Equal(t, int64(1), meta.RowsScanned)
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedServiceByHostRegex(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description state\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: host_name ~~ Test.*1$\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 1, "result length")
	assert.Equalf(t, int64(1), meta.Total, "meta.Total is correct")
	assert.Equalf(t, int64(1), meta.RowsScanned, "meta.RowsScanned is correct")

	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedHostByHostgroup(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET hosts\nColumns: name state\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: groups >= Everything\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, int64(1), meta.Total)
	assert.Equal(t, int64(1), meta.RowsScanned)
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedServiceByHostgroup(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description state\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: host_groups >= Everything\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, int64(1), meta.Total)
	assert.Equal(t, int64(1), meta.RowsScanned)
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestIndexedServiceByServicegroup(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description state\nOutputFormat: wrapped_json\nColumnHeaders: on\nFilter: groups >= Http Check\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, int64(1), meta.Total)
	assert.Equal(t, int64(1), meta.RowsScanned)
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
	require.NoError(t, err)
	assert.Len(t, res, 2)
	assert.Equal(t, int64(2), meta.Total)
	assert.Equal(t, int64(2), meta.RowsScanned)
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
	require.NoError(t, err)
	assert.Len(t, req.StatsGrouped, 2)
	assert.Equal(t, Counter, req.StatsGrouped[0].StatsType)
	assert.Equal(t, StatsGroup, req.StatsGrouped[1].StatsType)
	assert.Len(t, req.StatsGrouped[1].Filter, 3)
	assert.Equal(t, 3, req.StatsGrouped[1].Filter[2].StatsPos)
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
	require.NoError(t, err)
	assert.Len(t, req.StatsGrouped, 2)
	exp := `Stats: state_type = 0
Stats: state = 0
StatsOr: 2
Stats: has_been_checked = 1
StatsAnd: 2
`
	assert.Equal(t, exp, req.StatsGrouped[0].String("Stats"))

	exp = `Stats: state_type = 0
Stats: state = 0
StatsOr: 2
Stats: has_been_checked = 1
Stats: scheduled_downtime_depth = 0
Stats: acknowledged = 0
StatsAnd: 4
`
	assert.Equal(t, exp, req.StatsGrouped[1].String("Stats"))
}

func TestQueryWrappedJSON(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString(`GET services
OutputFormat: wrapped_json
ColumnHeaders: on
`)
	require.NoError(t, err)
	assert.Equal(t, int64(10), meta.RowsScanned)
	assert.Len(t, res, 10)
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestServiceCustVarFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description custom_variables\nOutputFormat: wrapped_json\nFilter: custom_variables = TEST 1\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assert.Equal(t, int64(3), meta.Total)
	assert.Equal(t, int64(10), meta.RowsScanned)
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestServiceCustVarRegexFilter(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, meta, err := peer.QueryString("GET services\nColumns: host_name description custom_variables\nOutputFormat: wrapped_json\nFilter: custom_variables ~~ TEST2 ^cust\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assert.Equal(t, int64(3), meta.Total)
	assert.Equal(t, int64(10), meta.RowsScanned)
	if err = cleanup(); err != nil {
		t.Error(err)
	}
}

func TestStatusJSONColumns(t *testing.T) {
	peer, cleanup, mockLmd := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	jsonStr := `{"core_type":null,"obj_check_cmd":1,"obj_readonly":null,"obj_reload_cmd":1}`
	peerKey := mockLmd.PeerMapOrder[0]
	mockLmd.PeerMap[peerKey].statusSetLocked(ThrukExtras, jsonStr)

	res, _, err := peer.QueryString("GET status\nColumns: peer_key configtool thruk\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)
	assert.IsTypef(t, map[string]interface{}{}, res[0][2], "thruk extras type check")

	if err = cleanup(); err != nil {
		t.Error(err)
	}
}
