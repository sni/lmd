package lmd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerSource(t *testing.T) {
	lmd := createTestLMDInstance()
	connection := Connection{Name: "Test", Source: []string{"http://localhost/test/", "http://clusternode/test"}}
	peer := NewPeer(lmd, &connection)

	require.Len(t, peer.source, 2)
	assert.Equal(t, "http://localhost/test/", peer.source[0])
	assert.Equal(t, "http://clusternode/test", peer.source[1])
}

func TestPeerHTTPComplete(t *testing.T) {
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk/"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk/cgi-bin/remote.cgi"))
	assert.Equal(t, "http://localhost/sitename/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/sitename"))
	assert.Equal(t, "http://localhost/sitename/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/sitename/"))
}

func TestParseResultJSON(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[
	 ["host1", "desc1", 0, [1,2], {"a": 1}] , [
	 "host2", "desc2", 1, [1,2], {"a": 1}] , ]
	`)

	res, _, err := req.parseResult(data)
	require.NoError(t, err)

	assert.Len(t, res, 2)
	assert.Len(t, res[0], 5)
	assert.Equal(t, "host2", res[1][0])
	assert.InDelta(t, float64(1), res[1][2], 0)
}

func TestParseResultJSON2(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[["host1", "desc1", 0, [1,2], {"a": 1}],
["host2", "desc2", 1, [1,2], {"a": 1}],
]
	`)

	res, _, err := req.parseResult(data)
	require.NoError(t, err)

	assert.Len(t, res, 2)
	assert.Len(t, res[0], 5)
	assert.Equal(t, "host2", res[1][0])
	assert.InDelta(t, float64(1), res[1][2], 0)
}

func TestParseResultJSONEmpty(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	require.NoError(t, err)

	data := []byte(`[]`)

	res, _, err := req.parseResult(data)
	require.NoError(t, err)

	assert.Empty(t, res)
}

func TestParseResultWrappedJSON(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: wrapped_json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`{"data": [
	 ["host1", "desc1", 0, [1,2], {"a": 1}],
	 ["host2", "desc2", 1, [1,2], {"a": 1}],
	],
	"total_count": 2}`)

	res, meta, err := req.parseResult(data)
	require.NoError(t, err)
	assert.Len(t, res, 2)
	assert.Len(t, res[0], 5)
	assert.Equal(t, "host2", res[1][0])
	assert.InDelta(t, 1, res[1][2], 0)
	assert.Equal(t, int64(2), meta.Total)
}

func TestParseResultJSONBroken(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`[
	 ["host1", "desc1", 0, [1,2], {"a": 1}],
	 ["host2", "desc2", 1, [1
	]`)

	InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
	res, _, err := req.parseResult(data)
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
	req, _, err := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`[
	 ["host1", "desc1", 0, [1,2], {"a": 1}],
	 ["host2", "desc2", 1, [1,2], {"a" 1}],
	]`)

	InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
	res, _, err := req.parseResult(data)
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
	req, _, err := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name\nOutputFormat: json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	for _, s := range []string{"\x00", "\x01", "\x02", "\x02", "\x06", "a\xc5z"} {
		data := []byte(fmt.Sprintf(`[["null%s"] ,
			["xy%cz"],
			[null],
		]`, s, 0))

		InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
		res, _, err := req.parseResult(data)
		InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

		require.NoError(t, err)
		require.Len(t, res, 3)
		assert.Contains(t, res[0][0], "null")
	}
}

func TestParseResultJSONBrokenNaN(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name hourly_value\nOutputFormat: json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`[
	 ["host1", 1],
	 ["host2", nan],
	]`)

	InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
	res, _, err := req.parseResult(data)
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

	require.NoError(t, err)
	assert.Len(t, res, 2)
}

func TestParseResultJSONBrokenError(t *testing.T) {
	lmd := createTestLMDInstance()
	req, _, err := NewRequest(context.TODO(), lmd, bufio.NewReader(bytes.NewBufferString("GET hosts\nColumns: name hourly_value\nOutputFormat: json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`[
	 ["host1", 1],
	 ["host2", broken],
	]`)

	InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
	res, _, err := req.parseResult(data)
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

	require.Errorf(t, err, "got no error but expected broken peer")
	require.Nilf(t, res, "did not expect result for broken json")
	assert.ErrorContainsf(t, err, "json parse error at row 2 pos 10 (byte offset 31): invalid json array", "got error %v", err)
}

func TestPeerUpdate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	store := peer.data.Load()
	err := store.UpdateFull(context.TODO(), Objects.UpdateTables)
	require.NoError(t, err)

	// fake some last_update entries
	data, _ := peer.GetDataStoreSet()
	svcTbl, _ := peer.GetDataStore(TableServices)
	lastCheckCol := svcTbl.GetColumn("last_check")
	for _, row := range svcTbl.data {
		row.dataInt64[lastCheckCol.Index] = 2
	}
	ctx := context.TODO()
	err = data.UpdateDelta(ctx, float64(5), float64(time.Now().Unix()+5))
	require.NoError(t, err)

	peer.lastUpdate.Set(0)
	_, err = peer.periodicUpdate(context.TODO())
	require.NoError(t, err)

	peer.lastUpdate.Set(0)
	peer.peerState.Set(PeerStatusWarning)
	_, err = peer.periodicUpdate(context.TODO())
	require.NoError(t, err)

	peer.lastUpdate.Set(0)
	peer.peerState.Set(PeerStatusDown)
	_, err = peer.periodicUpdate(context.TODO())
	require.NoError(t, err)

	err = peer.periodicTimeperiodsUpdate(context.TODO(), store)
	require.NoError(t, err)

	cList := peer.data.Load().Get(TableComments).table.GetColumns([]string{"id", "host_name", "service_description", "entry_time", "author", "comment", "persistent"})
	err = peer.data.Load().Get(TableComments).AppendData(ResultSet([][]interface{}{{"666", "test", "svc", 123456, "author", "comment", 0}}), cList)
	require.NoError(t, err)
	err = peer.data.Load().UpdateDelta(context.TODO(), float64(time.Now().Unix())-60, float64(time.Now().Unix()))
	require.NoError(t, err)

	peer.lastUpdate.Set(0)
	peer.peerState.Set(PeerStatusBroken)
	_, err = peer.periodicUpdate(context.TODO())
	require.Errorf(t, err, "got no error but expected broken peer")
	assert.Contains(t, err.Error(), "waiting for peer to recover")

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerDeltaUpdate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	store := peer.data.Load()
	err := store.UpdateDelta(context.TODO(), 0, 0)
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerUpdateResume(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	ctx := context.TODO()
	err := peer.ResumeFromIdle(ctx)
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerInitSerial(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	store := peer.data.Load()
	err := peer.initAllTablesSerial(context.TODO(), store)
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestLMDPeerUpdate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(3, 10, 10)
	PauseTestPeers(peer)

	peer.lastUpdate.Set(0)
	peer.SetFlag(LMD)
	peer.SetFlag(MultiBackend)
	_, err := peer.periodicUpdateLMD(context.TODO(), nil, true)
	require.NoError(t, err)

	peer.lastUpdate.Set(0)
	peer.ResetFlags()
	peer.SetFlag(MultiBackend)
	_, err = peer.periodicUpdateMultiBackends(context.TODO(), nil, true)
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerLog(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	peer.setBroken("test")
	peer.logPeerStatus(log.Debugf)
	err := peer.initTablesIfRestartRequiredError(context.TODO(), fmt.Errorf("test"))
	require.Errorf(t, err, "got no error but expected broken peer")
	assert.Contains(t, err.Error(), "test")

	err = cleanup()
	require.NoError(t, err)
}
