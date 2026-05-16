package lmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseResultJSON(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[
	 ["host1", "desc1", 0, [1,2], {"a": 1}] , [
	 "host2", "desc2", 1, [1,2], {"a": 1}] , ]
	`)

	res, _, err := req.parseResult(t.Context(), data)
	require.NoError(t, err)

	assert.Len(t, res, 2)
	assert.Len(t, res[0], 5)
	assert.Equal(t, "host2", res[1][0])
	assert.InDelta(t, float64(1), res[1][2], 0)
}

func TestParseResultJSON2(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[["host1", "desc1", 0, [1,2], {"a": 1}],
["host2", "desc2", 1, [1,2], {"a": 1}],
]
	`)

	res, _, err := req.parseResult(t.Context(), data)
	require.NoError(t, err)

	assert.Len(t, res, 2)
	assert.Len(t, res[0], 5)
	assert.Equal(t, "host2", res[1][0])
	assert.InDelta(t, float64(1), res[1][2], 0)
}

func TestParseResultJSONEmpty(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[]`)

	res, _, err := req.parseResult(t.Context(), data)
	require.NoError(t, err)

	assert.Empty(t, res)
}

func TestParseResultWrappedJSON(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: wrapped_json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`{"data": [
	 ["host1", "desc1", 0, [1,2], {"a": 1}],
	 ["host2", "desc2", 1, [1,2], {"a": 1}],
	],
	"total_count": 2}`)

	res, meta, err := req.parseResult(t.Context(), data)
	require.NoError(t, err)
	assert.Len(t, res, 2)
	assert.Len(t, res[0], 5)
	assert.Equal(t, "host2", res[1][0])
	assert.InDelta(t, 1, res[1][2], 0)
	assert.Equal(t, int64(2), meta.Total)
}

func TestParseResultJSONBroken(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[
	 ["host1", "desc1", 0, [1,2], {"a": 1}],
	 ["host2", "desc2", 1, [1
	]`)

	InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
	res, _, err := req.parseResult(t.Context(), data)
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

	if err == nil {
		t.Errorf("got no error from broken json")
	}

	if res != nil {
		t.Errorf("got result for broken json")
	}
}

func TestParseResultJSONBroken2(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[
	 ["host1", "desc1", 0, [1,2], {"a": 1}],
	 ["host2", "desc2", 1, [1,2], {"a" 1}],
	]`)

	InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
	res, _, err := req.parseResult(t.Context(), data)
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

	if err == nil {
		t.Errorf("got no error from broken json")
	}

	if res != nil {
		t.Errorf("got result for broken json")
	}
}

func TestParseResultJSONEscapeSequences(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	for _, s := range []string{"\x00", "\x01", "\x02", "\x02", "\x06", "a\xc5z"} {
		data := []byte(fmt.Sprintf(`[["null%s"] ,
			["xy%cz"],
			[null],
		]`, s, 0))

		InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
		res, _, err := req.parseResult(t.Context(), data)
		InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

		require.NoError(t, err)
		require.Len(t, res, 3)
		assert.Contains(t, res[0][0], "null")
	}
}

func TestParseResultJSONBrokenNaN(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name hourly_value\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[
	 ["host1", 1],
	 ["host2", nan],
	]`)

	InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
	res, _, err := req.parseResult(t.Context(), data)
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

	require.NoError(t, err)
	assert.Len(t, res, 2)
}

func TestParseResultJSONBrokenError(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name hourly_value\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[
	 ["host1", 1],
	 ["host2", broken],
	]`)

	InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
	res, _, err := req.parseResult(t.Context(), data)
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

	require.Errorf(t, err, "got no error but expected broken peer")
	require.Nilf(t, res, "did not expect result for broken json")
	assert.ErrorContainsf(t, err, "json parse error at row 2 pos 10 (byte offset 31): invalid json array", "got error %v", err)
}

func TestParseResultJSONStreamFixedSize(t *testing.T) {
	peer, cleanup, lmd := StartTestPeer(t, 1, 10, 10)
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name hourly_value downtimes\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[
	 ["host1", 1, [1,2,3]],
	 ["host2", 2, []],
	]`)
	header := []byte(`200          49`)

	results := ResultSet{}
	numRead := 0
	clb := func(row []any, numBytes int) error {
		results = append(results, row)
		numRead += numBytes

		return nil
	}
	response := header
	response = append(response, '\n')
	response = append(response, data...)

	res, err := peer.parseResponseFixedSize(req, io.NopCloser(bytes.NewReader(response)), clb)

	require.NoErrorf(t, err, "parser worked")
	require.Nilf(t, res, "did not expect result for callback reading")

	assert.Lenf(t, results, 2, "expect 2 result rows")
	assert.Equalf(t, len(data), numRead, "number of bytes parsed should match input")

	err = cleanup()
	require.NoError(t, err)
}

func TestParseResultJSONStreamFixedSizeKeepAlive(t *testing.T) {
	lmd := createTestLMDInstance()
	query := "GET hosts\nColumns: name hourly_value downtimes\nOutputFormat: json\nKeepAlive: on\nResponseHeader: fixed16\n"
	req, _, err := NewRequest(t.Context(), lmd, bufio.NewReader(bytes.NewBufferString(query)), ParseOptimize)
	require.NoError(t, err)

	bufferSizes := []int32{64, 1024, 1e6}
	testSizes := []int{5, 5000}

	if !testing.Short() {
		testSizes = append(testSizes, 2e6)
	}

	defer jsonReadBufferSize.Store(DefaultJSONBufferSize)

	for _, bs := range bufferSizes {
		for _, ts := range testSizes {
			testParseResultJSONStreamFixedSizeKeepAlive(t, lmd, req, ts, bs)
		}
	}
}

func testParseResultJSONStreamFixedSizeKeepAlive(t *testing.T, lmd *Daemon, req *Request, stringSize int, bufferSize int32) {
	t.Helper()

	jsonReadBufferSize.Store(bufferSize)

	data := []byte(`[
	 ["host1", 1, [1,2,3]],
	 ["host2` + strings.Repeat("x", stringSize) + `", 2, []],
	]`)

	results := ResultSet{}
	numRead := 0
	clb := func(row []any, numBytes int) error {
		results = append(results, row)
		numRead += numBytes

		return nil
	}

	listen := fmt.Sprintf("mock_0_%d.sock", time.Now().Nanosecond())

	defer os.Remove(listen)

	listenInit := make(chan bool)

	go func() {
		// listen on test socket, but do not close keepalive
		os.Remove(listen)
		listener, err := net.Listen("unix", listen)
		if err != nil {
			panic(fmt.Sprintf("failed to start listener: %s", err))
		}
		listenInit <- true

		for {
			conn, err := listener.Accept()
			if err != nil {
				return // probably closed
			}
			_checkErr2(fmt.Fprintf(conn, "200 %11d\n", len(data)))
			_checkErr2(conn.Write(data))

			// wait until connection is closed
			for {
				buf := make([]byte, 1024)
				_, err := conn.Read(buf)
				if err != nil {
					conn.Close()

					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	<-listenInit
	peer := NewPeer(lmd, &Connection{
		Source: []string{listen},
		Name:   "TestPeer",
	})

	_, err := peer.QueryCB(t.Context(), req, clb)

	require.NoErrorf(t, err, "parser worked")
	assert.Lenf(t, results, 2, "expect 2 result rows")
	assert.Equalf(t, len(data), numRead, "number of bytes parsed should match input")
}

func TestParseResultJSONStreamUnknownSize(t *testing.T) {
	data := []byte(`[
	 ["host1", 1, [1,2,3]],
	 ["host2", 2, []],
	]`)

	results := ResultSet{}
	numRead := 0
	clb := func(row []any, numBytes int) error {
		results = append(results, row)
		numRead += numBytes

		return nil
	}

	res, _, err := parseResponseUndefinedSize(io.NopCloser(bytes.NewReader(data)), clb)

	require.NoErrorf(t, err, "parser worked")
	require.Nilf(t, res, "did not expect result for callback reading")

	assert.Lenf(t, results, 2, "expect 2 result rows")
	assert.Equalf(t, len(data), numRead, "number of bytes parsed should match input")
}

func TestParseResultJSONStreamUnknownSizeMultiLine(t *testing.T) {
	data := []byte(`[
	 ["host1", 1, [1,2,3]],
	 ["host2", 2, []], ["host3", 2, []],
	 ["host2", 4, []]
	]`)

	results := ResultSet{}
	numRead := 0
	clb := func(row []any, numBytes int) error {
		results = append(results, row)
		numRead += numBytes

		return nil
	}

	res, _, err := parseResponseUndefinedSize(io.NopCloser(bytes.NewReader(data)), clb)

	require.NoErrorf(t, err, "parser worked")
	require.Nilf(t, res, "did not expect result for callback reading")

	assert.Lenf(t, results, 4, "expect 4 result rows")
	assert.Equalf(t, len(data), numRead, "number of bytes parsed should match input")
}

func TestParseResultJSONStreamUnknownSizeMultiLineII(t *testing.T) {
	data := []byte(`[
	 ["host1", 1, [1,2,3]],
	 ["host2",
	  2, []], ["host3", 2, []], ["host4", 4, []]
	]`)

	results := ResultSet{}
	numRead := 0
	clb := func(row []any, numBytes int) error {
		results = append(results, row)
		numRead += numBytes

		return nil
	}

	res, _, err := parseResponseUndefinedSize(io.NopCloser(bytes.NewReader(data)), clb)

	require.NoErrorf(t, err, "parser worked")
	require.Nilf(t, res, "did not expect result for callback reading")

	assert.Lenf(t, results, 4, "expect 4 result rows")
	assert.Equalf(t, len(data), numRead, "number of bytes parsed should match input")
}

func TestParseResultJSONStreamUnknownSizeMultiLineIII(t *testing.T) {
	data := []byte(`[["host1", 1, [1,2,3]],["host2",2, []], ["host3", 2, []], ["host4", 4, []]]`)

	results := ResultSet{}
	numRead := 0
	clb := func(row []any, numBytes int) error {
		results = append(results, row)
		numRead += numBytes

		return nil
	}

	res, _, err := parseResponseUndefinedSize(io.NopCloser(bytes.NewReader(data)), clb)

	require.NoErrorf(t, err, "parser worked")
	require.Nilf(t, res, "did not expect result for callback reading")

	assert.Lenf(t, results, 4, "expect 4 result rows")
	assert.Equalf(t, len(data), numRead, "number of bytes parsed should match input")
}

func TestParseResultJSONStreamUnknownSizeEmpty(t *testing.T) {
	data := []byte(`[]
`)

	results := ResultSet{}
	numRead := 0
	clb := func(row []any, numBytes int) error {
		results = append(results, row)
		numRead += numBytes

		return nil
	}

	res, _, err := parseResponseUndefinedSize(io.NopCloser(bytes.NewReader(data)), clb)

	require.NoErrorf(t, err, "parser worked")
	require.Nilf(t, res, "did not expect result for callback reading")

	assert.Emptyf(t, results, "expect 0 result rows")
	assert.Equalf(t, 0, numRead, "number of bytes parsed should match input")
}

func TestParseResultJSONStreamUnknownSizeRecoverError(t *testing.T) {
	data := []byte(`[["host1", 1, [1,2,3]],["host2",nan, []], ["host3", 2, []], ["host4", 4, []]]`)

	results := ResultSet{}
	numRead := 0
	clb := func(row []any, numBytes int) error {
		results = append(results, row)
		numRead += numBytes

		return nil
	}

	res, _, err := parseResponseUndefinedSize(io.NopCloser(bytes.NewReader(data)), clb)

	require.NoErrorf(t, err, "parser worked")
	require.Nilf(t, res, "did not expect result for callback reading")

	assert.Lenf(t, results, 4, "expect 4 result rows")
	assert.Equalf(t, len(data), numRead, "number of bytes parsed should match input")
}
